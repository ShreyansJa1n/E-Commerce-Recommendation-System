from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession

from recsys import schemas
from recsys.clean.catalog import (
    BEGINNING_OF_TIME,
    build_catalog_latest,
    build_categories,
    build_item_properties_scd,
    property_as_of,
)

W1, W2, W3 = datetime(2015, 5, 10, 3), datetime(2015, 5, 17, 3), datetime(2015, 5, 24, 3)


def _ms(dt: datetime) -> int:
    return int((dt - datetime(1970, 1, 1)).total_seconds() * 1000)


def _props(spark: SparkSession, rows: list[tuple]):
    return spark.createDataFrame(
        [(_ms(ts), ts, item, p, val, "p.csv", ts.date()) for ts, item, p, val in rows],
        schemas.BRONZE_ITEM_PROPERTIES,
    )


@pytest.fixture
def props(spark: SparkSession):
    return _props(
        spark,
        [
            (W1, 1, "categoryid", "10"),
            (W2, 1, "categoryid", "10"),  # unchanged -> collapsed
            (W3, 1, "categoryid", "20"),
            (W1, 1, "available", "1"),
            (W2, 2, "categoryid", "20"),
        ],
    )


def test_scd_collapses_unchanged_snapshots(props) -> None:
    scd = build_item_properties_scd(props, backfill_first_version=False)
    rows = {
        (r.item_id, r.property, r.value): (r.valid_from, r.valid_to, r.is_current)
        for r in scd.collect()
    }
    assert rows == {
        (1, "categoryid", "10"): (W1, W3, False),
        (1, "categoryid", "20"): (W3, None, True),
        (1, "available", "1"): (W1, None, True),
        (2, "categoryid", "20"): (W2, None, True),
    }


def test_scd_backfills_only_first_version(props) -> None:
    scd = build_item_properties_scd(props, backfill_first_version=True)
    first = {(r.item_id, r.property, r.value): r for r in scd.collect()}
    epoch = BEGINNING_OF_TIME.replace(tzinfo=None)
    assert first[(1, "categoryid", "10")].valid_from == epoch
    assert first[(1, "categoryid", "10")].is_backfilled
    assert first[(1, "categoryid", "10")].first_seen_ts == W1
    assert first[(1, "categoryid", "20")].valid_from == W3
    assert not first[(1, "categoryid", "20")].is_backfilled


def test_property_as_of_has_no_lookahead(spark: SparkSession, props) -> None:
    scd = build_item_properties_scd(props, backfill_first_version=False)
    events = spark.createDataFrame(
        [
            (1, datetime(2015, 5, 9)),  # before any snapshot -> null
            (1, W1),  # boundary is inclusive
            (1, datetime(2015, 5, 23, 23)),  # just before the change -> old value
            (1, W3),  # at the change -> new value
            (2, datetime(2015, 5, 12)),  # before item 2's first snapshot -> null
            (3, W3),  # unknown item -> null
        ],
        "item_id int, event_ts timestamp",
    )
    out = property_as_of(events, scd, "categoryid", "event_ts", "category_id")
    got = [
        (r.item_id, r.event_ts, r.category_id) for r in out.orderBy("item_id", "event_ts").collect()
    ]
    assert got == [
        (1, datetime(2015, 5, 9), None),
        (1, W1, "10"),
        (1, datetime(2015, 5, 23, 23), "10"),
        (1, W3, "20"),
        (2, datetime(2015, 5, 12), None),
        (3, W3, None),
    ]
    assert out.count() == events.count()  # never fans out


def test_categories_root_level_dangling_and_cycles(spark: SparkSession) -> None:
    tree = spark.createDataFrame(
        [(1, None), (2, 1), (3, 2), (4, 999), (5, 6), (6, 5)], schemas.BRONZE_CATEGORY_TREE
    )
    cats = {r.category_id: r for r in build_categories(spark, tree).collect()}
    assert (cats[3].root_category_id, cats[3].category_level) == (1, 2)
    assert (cats[1].root_category_id, cats[1].category_level) == (1, 0)
    # Parent 999 is not in the tree: 4 is treated as its own root.
    assert (cats[4].parent_id, cats[4].root_category_id, cats[4].category_level) == (999, 4, 0)
    assert cats[5].has_cycle and cats[5].root_category_id is None
    assert not cats[3].has_cycle


def test_catalog_latest_uses_current_values(spark: SparkSession, props) -> None:
    scd = build_item_properties_scd(props, backfill_first_version=True)
    tree = spark.createDataFrame([(10, None), (20, 10)], schemas.BRONZE_CATEGORY_TREE)
    latest = {
        r.item_id: r for r in build_catalog_latest(scd, build_categories(spark, tree)).collect()
    }
    assert (latest[1].category_id, latest[1].available, latest[1].root_category_id) == (20, 1, 10)
    assert latest[1].category_level == 1
    assert (latest[2].category_id, latest[2].available) == (20, None)
    assert latest[1].last_updated_ts == W3
    assert date(2015, 5, 24) == latest[1].last_updated_ts.date()

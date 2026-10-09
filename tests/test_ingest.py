from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from recsys import schemas
from recsys.ingest.raw_to_bronze import (
    _read_csv,
    category_tree_to_bronze,
    events_to_bronze,
    item_properties_to_bronze,
)


def _raw_events(spark: SparkSession, rows):
    return spark.createDataFrame(rows, schemas.RAW_EVENTS).withColumn(
        "_source_file", F.lit("events.csv")
    )


def test_events_schema_is_enforced(spark: SparkSession) -> None:
    raw = _raw_events(spark, [("1433221332117", "257597", " View ", "355908", None)])
    bronze = events_to_bronze(raw)
    assert [(f.name, f.dataType) for f in bronze.schema] == [
        (f.name, f.dataType) for f in schemas.BRONZE_EVENTS
    ]
    row = bronze.collect()[0]
    assert row.event_ts == datetime(2015, 6, 2, 5, 2, 12, 117000)
    assert row.event_ts.replace(tzinfo=UTC).timestamp() * 1000 == 1433221332117
    assert row.event_type == "view"  # normalized
    assert row.event_date.isoformat() == "2015-06-02"


def test_malformed_values_become_null_not_errors(spark: SparkSession) -> None:
    # ANSI mode would raise on a plain cast; try_cast must yield nulls instead.
    raw = _raw_events(spark, [("abc", "x1", "view", "12.5", "nope")])
    row = events_to_bronze(raw).collect()[0]
    assert (row.ts_ms, row.event_ts, row.visitor_id, row.item_id, row.transaction_id) == (
        None,
        None,
        None,
        None,
        None,
    )


def test_item_properties_and_tree(spark: SparkSession) -> None:
    props = spark.createDataFrame(
        [("1431226800000", "7", " categoryid ", " 1338 ")], schemas.RAW_ITEM_PROPERTIES
    ).withColumn("_source_file", F.lit("p.csv"))
    row = item_properties_to_bronze(props).collect()[0]
    assert (row.item_id, row.property, row.value) == (7, "categoryid", "1338")
    assert row.snapshot_date.isoformat() == "2015-05-10"

    tree = spark.createDataFrame([("1", None), ("2", "1")], schemas.RAW_CATEGORY_TREE)
    assert [tuple(r) for r in category_tree_to_bronze(tree).collect()] == [(1, None), (2, 1)]


def test_header_mismatch_is_rejected(spark: SparkSession, tmp_path) -> None:
    bad = tmp_path / "events.csv"
    bad.write_text("visitorid,timestamp,event,itemid,transactionid\n1,1433221332117,view,2,\n")
    with pytest.raises(Exception, match=r"(?i)header|schema"):
        _read_csv(spark, [bad], schemas.RAW_EVENTS).collect()

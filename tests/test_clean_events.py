from datetime import date, datetime

from pyspark.sql import SparkSession

from recsys import schemas
from recsys.clean.events import clean_events
from recsys.config import CleanConfig

CFG = CleanConfig(min_event_date=date(2015, 5, 1), max_event_date=date(2015, 10, 1))
TS = datetime(2015, 6, 1, 12)


def _bronze(spark: SparkSession, rows: list[tuple]):
    full = [
        (ms, ts, vis, ev, item, txn, "events.csv", ts.date() if ts else None)
        for ms, ts, vis, ev, item, txn in rows
    ]
    return spark.createDataFrame(full, schemas.BRONZE_EVENTS)


def test_exact_duplicates_are_dropped(spark: SparkSession) -> None:
    row = (1, TS, 10, "view", 99, None)
    other = (2, TS, 10, "view", 99, None)  # different ts_ms -> not a duplicate
    out = clean_events(_bronze(spark, [row, row, row, other]), CFG)
    assert out.valid.count() == 2
    assert out.rejected.count() == 0


def test_duplicate_transactions_keep_the_row_with_an_id(spark: SparkSession) -> None:
    out = clean_events(
        _bronze(spark, [(1, TS, 10, "transaction", 99, 5), (1, TS, 10, "transaction", 99, 5)]),
        CFG,
    )
    assert [r.transaction_id for r in out.valid.collect()] == [5]


def test_rejections_carry_first_failing_reason(spark: SparkSession) -> None:
    rows = [
        (None, None, 10, "view", 99, None),  # null_key
        (1, TS, None, "view", 99, None),  # null_key
        (1, TS, 10, "click", 99, None),  # invalid_event_type
        (1, datetime(2014, 1, 1), 10, "view", 99, None),  # ts_out_of_range
        (1, datetime(2015, 10, 1), 10, "view", 99, None),  # upper bound is exclusive
        (1, TS, 10, "transaction", 99, None),  # transaction_id_mismatch
        (1, TS, 10, "view", 99, 7),  # transaction_id_mismatch
        (1, TS, None, "click", 99, None),  # null_key wins over invalid type
    ]
    out = clean_events(_bronze(spark, rows), CFG)
    reasons = sorted(r.reject_reason for r in out.rejected.collect())
    assert reasons == sorted(
        ["null_key"] * 3
        + ["invalid_event_type"]
        + ["ts_out_of_range"] * 2
        + ["transaction_id_mismatch"] * 2
    )
    assert out.valid.count() == 0


def test_silver_columns(spark: SparkSession) -> None:
    out = clean_events(_bronze(spark, [(1, TS, 10, "view", 99, None)]), CFG)
    assert out.valid.columns == [
        "event_ts",
        "ts_ms",
        "visitor_id",
        "item_id",
        "event_type",
        "transaction_id",
        "event_date",
    ]

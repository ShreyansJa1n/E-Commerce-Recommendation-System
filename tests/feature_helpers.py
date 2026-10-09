"""Builders for small enriched-event / catalog frames used by feature tests."""

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from recsys.config import FeatureConfig, load_config

ENRICHED_DDL = (
    "event_ts timestamp, ts_ms bigint, visitor_id int, item_id int, event_type string,"
    " transaction_id int, session_id string, category_id int, event_date date"
)
SCD_DDL = (
    "item_id int, property string, value string, valid_from timestamp, valid_to timestamp,"
    " first_seen_ts timestamp, is_current boolean, is_backfilled boolean"
)
CATEGORIES_DDL = (
    "category_id int, parent_id int, root_category_id int, category_level int, has_cycle boolean"
)


def feature_config() -> FeatureConfig:
    return load_config("base").features


def to_ms(ts: datetime) -> int:
    return int((ts - datetime(1970, 1, 1)).total_seconds() * 1000)


def enriched(
    spark: SparkSession,
    rows: list[tuple[datetime, int, int, str, int | None, str, int | None]],
) -> DataFrame:
    """rows: (event_ts, visitor_id, item_id, event_type, transaction_id, session_id, category_id)"""
    return spark.createDataFrame(
        [(ts, to_ms(ts), v, i, e, t, s, c, ts.date()) for ts, v, i, e, t, s, c in rows],
        ENRICHED_DDL,
    )


def scd(
    spark: SparkSession, rows: list[tuple[int, str, str, datetime, datetime | None]]
) -> DataFrame:
    """rows: (item_id, property, value, valid_from, valid_to)"""
    return spark.createDataFrame(
        [(i, p, v, f, t, f, t is None, False) for i, p, v, f, t in rows], SCD_DDL
    )


def categories(spark: SparkSession, rows: list[tuple[int, int | None, int, int]]) -> DataFrame:
    return spark.createDataFrame([(*r, False) for r in rows], CATEGORIES_DDL)

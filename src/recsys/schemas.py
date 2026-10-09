"""Explicit schemas for raw inputs and the typed bronze/silver tables."""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _strings(*names: str) -> StructType:
    return StructType([StructField(n, StringType(), True) for n in names])


# Raw CSVs are read as strings and cast with try_cast (Spark 4 runs in ANSI mode, so a
# plain cast of a malformed value would fail the job instead of yielding null).
RAW_EVENTS = _strings("timestamp", "visitorid", "event", "itemid", "transactionid")
RAW_ITEM_PROPERTIES = _strings("timestamp", "itemid", "property", "value")
RAW_CATEGORY_TREE = _strings("categoryid", "parentid")

BRONZE_EVENTS = StructType(
    [
        StructField("ts_ms", LongType()),
        StructField("event_ts", TimestampType()),
        StructField("visitor_id", IntegerType()),
        StructField("event_type", StringType()),
        StructField("item_id", IntegerType()),
        StructField("transaction_id", IntegerType()),
        StructField("_source_file", StringType()),
        StructField("event_date", DateType()),
    ]
)

BRONZE_ITEM_PROPERTIES = StructType(
    [
        StructField("ts_ms", LongType()),
        StructField("snapshot_ts", TimestampType()),
        StructField("item_id", IntegerType()),
        StructField("property", StringType()),
        StructField("value", StringType()),
        StructField("_source_file", StringType()),
        StructField("snapshot_date", DateType()),
    ]
)

BRONZE_CATEGORY_TREE = StructType(
    [
        StructField("category_id", IntegerType()),
        StructField("parent_id", IntegerType()),
    ]
)

"""Raw CSV -> bronze Parquet: explicit schemas, typed columns, epoch ms -> timestamps.

Bronze keeps every input row (including ones that fail to parse, as nulls) so that
silver can account for each rejected row.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from recsys import schemas
from recsys.config import Config
from recsys.io import StageReport, timed_stage, write_table


def _read_csv(spark: SparkSession, paths: Sequence[Path], schema: StructType) -> DataFrame:
    missing = [p for p in paths if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing raw input(s): {', '.join(map(str, missing))}")
    return (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        # Fail on a header that doesn't match the schema instead of mapping by position.
        .option("enforceSchema", "false")
        .schema(schema)
        .csv([str(p) for p in paths])
        .withColumn("_source_file", F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1))
    )


def _ms_to_ts(col: str) -> tuple[Column, Column]:
    ms = F.col(col).try_cast("long")
    return ms, F.timestamp_millis(ms)


def events_to_bronze(raw: DataFrame) -> DataFrame:
    ms, ts = _ms_to_ts("timestamp")
    out = raw.select(
        ms.alias("ts_ms"),
        ts.alias("event_ts"),
        F.col("visitorid").try_cast("int").alias("visitor_id"),
        F.lower(F.trim(F.col("event"))).alias("event_type"),
        F.col("itemid").try_cast("int").alias("item_id"),
        F.col("transactionid").try_cast("int").alias("transaction_id"),
        "_source_file",
    ).withColumn("event_date", F.to_date("event_ts"))
    return out.select(*schemas.BRONZE_EVENTS.fieldNames())


def item_properties_to_bronze(raw: DataFrame) -> DataFrame:
    ms, ts = _ms_to_ts("timestamp")
    out = raw.select(
        ms.alias("ts_ms"),
        ts.alias("snapshot_ts"),
        F.col("itemid").try_cast("int").alias("item_id"),
        F.trim(F.col("property")).alias("property"),
        F.trim(F.col("value")).alias("value"),
        "_source_file",
    ).withColumn("snapshot_date", F.to_date("snapshot_ts"))
    return out.select(*schemas.BRONZE_ITEM_PROPERTIES.fieldNames())


def category_tree_to_bronze(raw: DataFrame) -> DataFrame:
    return raw.select(
        F.col("categoryid").try_cast("int").alias("category_id"),
        F.col("parentid").try_cast("int").alias("parent_id"),
    )


def run(spark: SparkSession, cfg: Config) -> StageReport:
    paths = cfg.paths.resolved()
    with timed_stage("raw_to_bronze") as report:
        events = events_to_bronze(
            _read_csv(spark, [paths.raw / f for f in cfg.ingest.events_files], schemas.RAW_EVENTS)
        )
        write_table(events, paths.bronze / "events", partition_by=["event_date"])

        props = item_properties_to_bronze(
            _read_csv(
                spark,
                [paths.raw / f for f in cfg.ingest.item_properties_files],
                schemas.RAW_ITEM_PROPERTIES,
            )
        )
        write_table(props, paths.bronze / "item_properties", partition_by=["snapshot_date"])

        tree = category_tree_to_bronze(
            _read_csv(
                spark, [paths.raw / cfg.ingest.category_tree_file], schemas.RAW_CATEGORY_TREE
            ).drop("_source_file")
        )
        write_table(tree, paths.bronze / "category_tree")

        for name in ("events", "item_properties", "category_tree"):
            report.rows[name] = spark.read.parquet(str(paths.bronze / name)).count()
    report.write(paths.bronze / "_reports")
    return report

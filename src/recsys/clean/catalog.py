"""Bronze -> silver catalog tables.

- ``item_properties_scd``: one row per (item, property, value version) with
  ``[valid_from, valid_to)``. Use it for point-in-time joins (``property_as_of``).
- ``categories``: category tree with parent, root, and depth.
- ``catalog_latest``: current category/availability per item. For serving and
  reporting only; feature code must use the SCD table.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, StructField, StructType

from recsys.clean import validation as v
from recsys.config import Config
from recsys.io import StageReport, read_table, timed_stage, write_table

# Lower bound used for backfilled first versions (ADR-005).
BEGINNING_OF_TIME = datetime(1970, 1, 1, tzinfo=UTC)

CATEGORY_SCHEMA = StructType(
    [
        StructField("category_id", IntegerType(), False),
        StructField("parent_id", IntegerType(), True),
        StructField("root_category_id", IntegerType(), True),
        StructField("category_level", IntegerType(), True),
        StructField("has_cycle", BooleanType(), False),
    ]
)


def build_item_properties_scd(bronze: DataFrame, backfill_first_version: bool) -> DataFrame:
    """Collapse weekly snapshots into value-change intervals per (item, property)."""
    rows = bronze.where(
        F.col("item_id").isNotNull()
        & F.col("snapshot_ts").isNotNull()
        & F.col("property").isNotNull()
    ).dropDuplicates(["item_id", "property", "ts_ms"])
    by_key = Window.partitionBy("item_id", "property").orderBy("ts_ms")
    changes = (
        rows.withColumn("_prev", F.lag("value").over(by_key))
        .withColumn("_rn", F.row_number().over(by_key))
        .where((F.col("_rn") == 1) | ~F.col("value").eqNullSafe(F.col("_prev")))
    )
    scd = (
        changes.withColumn("is_first_version", F.row_number().over(by_key) == 1)
        .withColumn("valid_to", F.lead("snapshot_ts").over(by_key))
        .withColumn(
            "valid_from",
            F.when(
                F.lit(backfill_first_version) & F.col("is_first_version"),
                F.lit(BEGINNING_OF_TIME),
            ).otherwise(F.col("snapshot_ts")),
        )
        .withColumn("is_current", F.col("valid_to").isNull())
        .withColumn("is_backfilled", F.col("valid_from") != F.col("snapshot_ts"))
    )
    return scd.select(
        "item_id",
        "property",
        "value",
        "valid_from",
        "valid_to",
        F.col("snapshot_ts").alias("first_seen_ts"),
        "is_current",
        "is_backfilled",
    )


def property_as_of(
    df: DataFrame,
    scd: DataFrame,
    prop: str,
    ts_col: str,
    out_col: str | None = None,
    item_col: str = "item_id",
) -> DataFrame:
    """Attach the value of ``prop`` valid at ``df[ts_col]`` for each row (null if none).

    Interval is ``[valid_from, valid_to)``, so a value first seen at T is visible at T.
    """
    out_col = out_col or prop
    versions = scd.where(F.col("property") == prop).select(
        F.col("item_id").alias("_pit_item"),
        F.col("value").alias(out_col),
        F.col("valid_from").alias("_pit_from"),
        F.col("valid_to").alias("_pit_to"),
    )
    cond = (
        (F.col(item_col) == F.col("_pit_item"))
        & (F.col(ts_col) >= F.col("_pit_from"))
        & (F.col("_pit_to").isNull() | (F.col(ts_col) < F.col("_pit_to")))
    )
    return df.join(versions, cond, "left").drop("_pit_item", "_pit_from", "_pit_to")


def build_categories(spark: SparkSession, tree: DataFrame) -> DataFrame:
    """Resolve parent/root/level. The tree is tiny (~1.7k rows), so walk it on the driver."""
    parent: dict[int, int | None] = {}
    for r in tree.where(F.col("category_id").isNotNull()).collect():
        parent.setdefault(int(r["category_id"]), r["parent_id"])

    rows = []
    for cat in sorted(parent):
        seen = [cat]
        cycle = False
        nxt = parent.get(cat)
        while nxt is not None:
            if nxt in seen:
                cycle = True
                break
            seen.append(nxt)
            nxt = parent.get(nxt)
        # Root is the last ancestor present in the tree (a dangling parent id has no row).
        root = None if cycle else next(c for c in reversed(seen) if c in parent)
        level = None if cycle else len([c for c in seen if c in parent]) - 1
        rows.append((cat, parent[cat], root, level, cycle))
    return spark.createDataFrame(rows, CATEGORY_SCHEMA)


def build_catalog_latest(scd: DataFrame, categories: DataFrame) -> DataFrame:
    current = scd.where(F.col("is_current"))
    latest = current.groupBy("item_id").agg(
        F.max(F.when(F.col("property") == "categoryid", F.col("value")))
        .try_cast("int")
        .alias("category_id"),
        F.max(F.when(F.col("property") == "available", F.col("value")))
        .try_cast("int")
        .alias("available"),
        F.count(F.lit(1)).alias("n_properties"),
        F.max("first_seen_ts").alias("last_updated_ts"),
    )
    return latest.join(
        categories.select("category_id", "parent_id", "root_category_id", "category_level"),
        "category_id",
        "left",
    ).select(
        "item_id",
        "category_id",
        "parent_id",
        "root_category_id",
        "category_level",
        "available",
        "n_properties",
        "last_updated_ts",
    )


def scd_checks() -> list[v.Check]:
    return [
        *v.not_null("item_id", "property", "valid_from", "first_seen_ts"),
        v.unique("item_id", "property", "valid_from"),
        v.predicate(
            "valid_to_after_valid_from",
            F.col("valid_to").isNotNull() & (F.col("valid_to") <= F.col("valid_from")),
        ),
    ]


def catalog_checks() -> list[v.Check]:
    return [
        *v.not_null("item_id"),
        v.unique("item_id"),
        *v.not_null("category_id", severity=v.Severity.WARN),
        v.predicate(
            "category_in_tree",
            F.col("category_id").isNotNull() & F.col("category_level").isNull(),
            severity=v.Severity.WARN,
        ),
        v.predicate(
            "available_is_binary",
            F.col("available").isNotNull() & ~F.col("available").isin(0, 1),
        ),
    ]


def run(spark: SparkSession, cfg: Config) -> StageReport:
    paths = cfg.paths.resolved()
    reports: list[v.ValidationReport] = []
    with timed_stage("silver_catalog") as report:
        scd = build_item_properties_scd(
            read_table(spark, paths.bronze / "item_properties"),
            cfg.clean.backfill_first_property_version,
        )
        write_table(scd, paths.silver / "item_properties_scd")
        scd = read_table(spark, paths.silver / "item_properties_scd")

        categories = build_categories(spark, read_table(spark, paths.bronze / "category_tree"))
        write_table(categories, paths.silver / "categories")
        categories = read_table(spark, paths.silver / "categories")

        write_table(build_catalog_latest(scd, categories), paths.silver / "catalog_latest")
        catalog = read_table(spark, paths.silver / "catalog_latest")

        reports.append(v.run_checks("silver_item_properties_scd", scd, scd_checks()))
        reports.append(
            v.run_checks(
                "silver_item_properties_scd_current",
                scd.where(F.col("is_current")),
                [v.unique("item_id", "property")],
            )
        )
        reports.append(
            v.run_checks(
                "silver_categories",
                categories,
                [
                    v.unique("category_id"),
                    v.predicate("no_cycles", F.col("has_cycle")),
                ],
            )
        )
        reports.append(v.run_checks("silver_catalog_latest", catalog, catalog_checks()))
        for r in reports:
            r.write(paths.silver / "_validation")

        report.rows = {
            "item_properties_scd": reports[0].total_rows,
            "categories": reports[2].total_rows,
            "catalog_latest": reports[3].total_rows,
        }
        report.extra["validation_passed"] = all(not r.errors for r in reports)
        events_path = paths.silver / "events"
        if events_path.exists():
            event_items = read_table(spark, events_path).select("item_id").distinct()
            report.extra["event_items_without_catalog"] = event_items.join(
                catalog, "item_id", "left_anti"
            ).count()
    report.write(paths.silver / "_reports")
    for r in reports:
        r.raise_on_error()
    return report

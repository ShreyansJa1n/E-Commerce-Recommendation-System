"""Silver -> gold: enriched events, cutoffs, point-in-time features, labels."""

from __future__ import annotations

from collections.abc import Callable
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from recsys.clean import validation as v
from recsys.config import Config, FeatureConfig
from recsys.features import contract
from recsys.features.events import enrich_events
from recsys.features.item import build_item_features
from recsys.features.split import Cutoff, build_labels, cutoffs, cutoffs_frame
from recsys.features.user import build_user_category_affinity, build_user_features
from recsys.io import StageReport, read_table, timed_stage, write_table

KEYS = {
    "user_features": ["cutoff_date", "visitor_id"],
    "user_category_affinity": ["cutoff_date", "visitor_id", "category_id"],
    "item_features": ["cutoff_date", "item_id"],
    "labels": ["cutoff_date", "visitor_id", "item_id"],
}


def compute_snapshot(
    events: DataFrame,
    scd: DataFrame,
    categories: DataFrame,
    cut: Cutoff,
    fc: FeatureConfig,
) -> dict[str, DataFrame]:
    """All gold tables for one cutoff, conformed to the contract."""
    specs = contract.table_specs(fc)
    tables = {
        "user_features": build_user_features(events, cut, fc),
        "user_category_affinity": build_user_category_affinity(events, cut, fc),
        "item_features": build_item_features(events, scd, categories, cut, fc),
        "labels": build_labels(events, cut, fc.relevance),
    }
    return {name: contract.conform(df, specs[name], name) for name, df in tables.items()}


def table_checks(name: str, fc: FeatureConfig) -> list[v.Check]:
    never_null = [s.name for s in contract.table_specs(fc)[name] if s.nulls == contract.NEVER]
    checks = [*v.not_null(*never_null), v.unique(*KEYS[name])]
    if name == "labels":
        checks.append(v.between("relevance", 1, max(fc.relevance.values()) + 1))
    if name == "user_category_affinity":
        checks.append(
            v.predicate(
                "share_in_(0,1]",
                (F.col("affinity_share") <= 0) | (F.col("affinity_share") > 1 + 1e-9),
            )
        )
    return checks


def run(spark: SparkSession, cfg: Config) -> StageReport:
    paths = cfg.paths.resolved()
    fc = cfg.features
    cuts = cutoffs(cfg.split)
    gold = paths.gold
    with timed_stage("gold_features") as report:
        scd = read_table(spark, paths.silver / "item_properties_scd")
        categories = read_table(spark, paths.silver / "categories")
        enriched = enrich_events(
            read_table(spark, paths.silver / "events"), scd, fc.session_gap_minutes
        )
        write_table(enriched, gold / "events_enriched", partition_by=["event_date"])
        events = read_table(spark, gold / "events_enriched")
        write_table(cutoffs_frame(spark, cuts), gold / "cutoffs")

        snapshots = [compute_snapshot(events, scd, categories, c, fc) for c in cuts]
        union: Callable[[DataFrame, DataFrame], DataFrame] = DataFrame.unionByName
        reports: list[v.ValidationReport] = []
        for name in KEYS:
            write_table(
                reduce(union, (s[name] for s in snapshots)),
                gold / name,
                partition_by=["cutoff_date"],
            )
            table = read_table(spark, gold / name)
            reports.append(v.run_checks(f"gold_{name}", table, table_checks(name, fc)))
            report.rows[name] = reports[-1].total_rows
        report.rows["events_enriched"] = events.count()
        for r in reports:
            r.write(gold / "_validation")

        share = events.select(F.avg(F.col("category_id").isNotNull().cast("double"))).collect()
        report.extra["events_with_category_share"] = round(float(share[0][0]), 4)
        labels = read_table(spark, gold / "labels")
        per_split = (
            labels.groupBy("split", "cutoff_date")
            .agg(
                F.count(F.lit(1)).alias("label_pairs"),
                F.countDistinct("visitor_id").alias("label_visitors"),
                F.avg(F.col("visitor_has_history").cast("double")).alias("pair_share_warm"),
                F.avg(F.col("is_repeat").cast("double")).alias("pair_share_repeat"),
                F.sum(F.col("purchased").cast("int")).alias("purchased_pairs"),
            )
            .orderBy("cutoff_date")
            .collect()
        )
        warm = (
            labels.groupBy("cutoff_date", "visitor_id")
            .agg(F.max(F.col("visitor_has_history").cast("int")).alias("w"))
            .groupBy("cutoff_date")
            .agg(F.avg("w").alias("visitor_share_warm"))
        )
        warm_by_cut = {
            str(r["cutoff_date"]): round(r["visitor_share_warm"], 4) for r in warm.collect()
        }
        report.extra["labels"] = [
            {
                **{
                    k: (round(val, 4) if isinstance(val, float) else val)
                    for k, val in r.asDict().items()
                },
                "cutoff_date": str(r["cutoff_date"]),
                "visitor_share_warm": warm_by_cut[str(r["cutoff_date"])],
            }
            for r in per_split
        ]
        report.extra["validation_passed"] = all(not r.errors for r in reports)
    report.write(gold / "_reports")
    for r in reports:
        r.raise_on_error()
    return report

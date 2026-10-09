"""Bronze -> silver events: reject invalid rows (with a reason), dedup, validate."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from recsys.clean import validation as v
from recsys.config import CleanConfig, Config
from recsys.io import StageReport, read_table, timed_stage, write_table

EVENT_KEY = ["ts_ms", "visitor_id", "event_type", "item_id"]
SILVER_EVENT_COLUMNS = [
    "event_ts",
    "ts_ms",
    "visitor_id",
    "item_id",
    "event_type",
    "transaction_id",
    "event_date",
]


def _utc_midnight(d: date) -> datetime:
    return datetime.combine(d, time.min, tzinfo=UTC)


def _transaction_id_mismatch() -> Column:
    is_txn = F.col("event_type") == "transaction"
    has_id = F.col("transaction_id").isNotNull()
    return (is_txn & ~has_id) | (~is_txn & has_id)


def reject_reason(cfg: CleanConfig) -> Column:
    """First failing rule for a bronze event row, or null if the row is valid."""
    lo, hi = _utc_midnight(cfg.min_event_date), _utc_midnight(cfg.max_event_date)
    return (
        F.when(
            F.col("ts_ms").isNull() | F.col("visitor_id").isNull() | F.col("item_id").isNull(),
            "null_key",
        )
        .when(~F.col("event_type").isin(cfg.valid_event_types), "invalid_event_type")
        .when((F.col("event_ts") < F.lit(lo)) | (F.col("event_ts") >= F.lit(hi)), "ts_out_of_range")
        .when(_transaction_id_mismatch(), "transaction_id_mismatch")
    )


@dataclass
class CleanedEvents:
    valid: DataFrame
    rejected: DataFrame


def clean_events(bronze: DataFrame, cfg: CleanConfig) -> CleanedEvents:
    tagged = bronze.withColumn("reject_reason", reject_reason(cfg))
    rejected = tagged.where(F.col("reject_reason").isNotNull())
    valid = tagged.where(F.col("reject_reason").isNull())
    # Exact duplicates on the event key: keep one row deterministically.
    w = Window.partitionBy(*EVENT_KEY).orderBy(F.col("transaction_id").desc_nulls_last())
    deduped = (
        valid.withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .select(*SILVER_EVENT_COLUMNS)
    )
    return CleanedEvents(deduped, rejected)


def event_checks(cfg: CleanConfig) -> list[v.Check]:
    return [
        *v.not_null("event_ts", "visitor_id", "item_id", "event_type", "event_date"),
        v.is_in("event_type", cfg.valid_event_types),
        v.between("event_ts", _utc_midnight(cfg.min_event_date), _utc_midnight(cfg.max_event_date)),
        v.unique(*EVENT_KEY),
        v.predicate("transaction_id_consistent", _transaction_id_mismatch()),
    ]


def run(spark: SparkSession, cfg: Config) -> StageReport:
    paths = cfg.paths.resolved()
    with timed_stage("silver_events") as report:
        bronze = read_table(spark, paths.bronze / "events")
        cleaned = clean_events(bronze, cfg.clean)
        write_table(cleaned.valid, paths.silver / "events", partition_by=["event_date"])
        write_table(cleaned.rejected, paths.silver / "_rejected" / "events")

        silver = read_table(spark, paths.silver / "events")
        rejected = read_table(spark, paths.silver / "_rejected" / "events")
        bronze_rows = bronze.count()
        report.rows = {
            "bronze": bronze_rows,
            "silver": silver.count(),
            "rejected": rejected.count(),
        }
        report.rows["duplicates_dropped"] = (
            bronze_rows - report.rows["silver"] - report.rows["rejected"]
        )
        report.extra["rejected_by_reason"] = {
            r["reject_reason"]: r["count"]
            for r in rejected.groupBy("reject_reason").count().collect()
        }
        validation = v.run_checks("silver_events", silver, event_checks(cfg.clean))
        validation.write(paths.silver / "_validation")
        report.extra["validation_passed"] = not validation.errors
    report.write(paths.silver / "_reports")
    validation.raise_on_error()
    return report

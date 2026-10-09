"""Time-based split: cutoffs and future-interaction labels.

A cutoff T defines one point-in-time snapshot. Features for T use events with
``event_ts < T`` only (``history``). Labels for T come from ``[T, label_end)``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

from recsys.config import SplitConfig

CUTOFF_SCHEMA = StructType(
    [
        StructField("split", StringType(), False),
        StructField("cutoff_date", DateType(), False),
        StructField("cutoff_ts", TimestampType(), False),
        StructField("label_end_ts", TimestampType(), False),
    ]
)


def _midnight(d: date) -> datetime:
    return datetime.combine(d, time.min, tzinfo=UTC)


@dataclass(frozen=True)
class Cutoff:
    split: str
    cutoff_date: date
    label_end_date: date

    @property
    def ts(self) -> datetime:
        return _midnight(self.cutoff_date)

    @property
    def label_end_ts(self) -> datetime:
        return _midnight(self.label_end_date)


def cutoffs(cfg: SplitConfig) -> list[Cutoff]:
    horizon = timedelta(days=cfg.label_horizon_days)
    return [
        *(Cutoff("train", t, t + horizon) for t in cfg.train_cutoffs),
        Cutoff("val", cfg.val_start, cfg.test_start),
        Cutoff("test", cfg.test_start, cfg.end),
    ]


def cutoffs_frame(spark: SparkSession, cuts: list[Cutoff]) -> DataFrame:
    return spark.createDataFrame(
        [(c.split, c.cutoff_date, c.ts, c.label_end_ts) for c in cuts], CUTOFF_SCHEMA
    )


def history(events: DataFrame, cut: Cutoff) -> DataFrame:
    """Events strictly before the cutoff: the only input any feature may see."""
    return events.where(F.col("event_ts") < F.lit(cut.ts))


def label_window(events: DataFrame, cut: Cutoff) -> DataFrame:
    return events.where(
        (F.col("event_ts") >= F.lit(cut.ts)) & (F.col("event_ts") < F.lit(cut.label_end_ts))
    )


def build_labels(events: DataFrame, cut: Cutoff, relevance: Mapping[str, int]) -> DataFrame:
    """One row per (visitor, item) interacted with in the label window."""
    rel = F.coalesce(
        *[F.when(F.col("event_type") == e, F.lit(g)) for e, g in relevance.items()], F.lit(0)
    )
    future = label_window(events, cut)
    past = history(events, cut)
    labels = future.groupBy("visitor_id", "item_id").agg(
        F.max(rel).alias("relevance"),
        F.count(F.lit(1)).alias("n_label_events"),
        F.min("event_ts").alias("first_label_ts"),
        F.max((F.col("event_type") == "view").cast("int")).cast("boolean").alias("viewed"),
        F.max((F.col("event_type") == "addtocart").cast("int")).cast("boolean").alias("carted"),
        F.max((F.col("event_type") == "transaction").cast("int"))
        .cast("boolean")
        .alias("purchased"),
    )
    seen_pairs = (
        past.select("visitor_id", "item_id").distinct().withColumn("is_repeat", F.lit(True))
    )
    seen_visitors = (
        past.select("visitor_id").distinct().withColumn("visitor_has_history", F.lit(True))
    )
    return (
        labels.join(seen_pairs, ["visitor_id", "item_id"], "left")
        .join(seen_visitors, "visitor_id", "left")
        .select(
            F.lit(cut.cutoff_date).alias("cutoff_date"),
            F.lit(cut.split).alias("split"),
            "visitor_id",
            "item_id",
            F.col("relevance").cast("int").alias("relevance"),
            "n_label_events",
            "first_label_ts",
            "viewed",
            "carted",
            "purchased",
            F.coalesce("is_repeat", F.lit(False)).alias("is_repeat"),
            F.coalesce("visitor_has_history", F.lit(False)).alias("visitor_has_history"),
        )
    )

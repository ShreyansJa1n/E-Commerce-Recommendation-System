"""Non-learned baselines built from candidate sources."""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def priority_blend(candidates: DataFrame, priority: Sequence[str], n: int) -> DataFrame:
    """Concatenate sources in ``priority`` order (each in its own rank order), dedupe,
    keep the first ``n``. E.g. own recent items first, then category popularity, then
    global popularity: a strong hand-written baseline for the ranker to beat."""
    order = F.coalesce(*[F.when(F.col("source") == s, F.lit(p)) for p, s in enumerate(priority)])
    ranked = (
        candidates.where(F.col("source").isin(list(priority)))
        .withColumn("_key", order * F.lit(1_000_000) + F.col("rank"))
        .groupBy("cutoff_date", "split", "visitor_id", "item_id")
        .agg(F.min("_key").alias("_key"))
    )
    w = Window.partitionBy("cutoff_date", "visitor_id").orderBy("_key", "item_id")
    return (
        ranked.withColumn("rank", F.row_number().over(w))
        .where(F.col("rank") <= n)
        .select(
            "cutoff_date",
            "split",
            "visitor_id",
            "item_id",
            (-F.col("_key")).cast("double").alias("score"),
            "rank",
        )
    )

"""Shared helpers for candidate generators.

Every generator returns ``CANDIDATE_COLUMNS``: one row per (visitor, item) with a
source-specific ``score`` and ``rank`` = 1..top_n (ties broken by item_id).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from recsys.features.split import Cutoff
from recsys.features.user import days_before

CANDIDATE_COLUMNS = ["visitor_id", "item_id", "score", "rank"]


def decay(cut: Cutoff, half_life_days: float) -> Column:
    """Exponential time decay of an event: 1 at T, 0.5 after ``half_life_days``."""
    return F.pow(F.lit(0.5), days_before(cut, F.col("ts_ms")) / F.lit(half_life_days))


def top_n_per_user(scored: DataFrame, n: int, user_col: str = "visitor_id") -> DataFrame:
    """Keep the ``n`` best-scored items per user and assign rank 1..n."""
    w = Window.partitionBy(user_col).orderBy(F.col("score").desc(), F.col("item_id"))
    return (
        scored.withColumn("rank", F.row_number().over(w))
        .where(F.col("rank") <= n)
        .select(
            F.col(user_col).alias("visitor_id"), "item_id", F.col("score").cast("double"), "rank"
        )
    )


def ranked_list(scored_items: DataFrame, n: int) -> DataFrame:
    """Global top-n list (item_id, score, rank) from (item_id, score)."""
    w = Window.orderBy(F.col("score").desc(), F.col("item_id"))
    return scored_items.withColumn("rank", F.row_number().over(w)).where(F.col("rank") <= n)


def warm_targets(targets: DataFrame, history: DataFrame) -> DataFrame:
    return targets.join(history.select("visitor_id").distinct(), "visitor_id", "left_semi")

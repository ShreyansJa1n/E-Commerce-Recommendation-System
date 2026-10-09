"""Re-recommend a visitor's own recent items (repeat interactions are common in e-commerce)."""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from recsys.candidates.common import decay, top_n_per_user
from recsys.features.split import Cutoff
from recsys.features.user import weight_expr


def recent_items(
    history: DataFrame,
    targets: DataFrame,
    cut: Cutoff,
    half_life_days: float,
    weights: Mapping[str, float],
    n: int,
) -> DataFrame:
    """score = sum over the visitor's past events on the item of weight x decay."""
    scored = (
        history.join(targets.select("visitor_id"), "visitor_id", "left_semi")
        .groupBy("visitor_id", "item_id")
        .agg(F.sum(weight_expr(weights) * decay(cut, half_life_days)).alias("score"))
    )
    return top_n_per_user(scored, n)

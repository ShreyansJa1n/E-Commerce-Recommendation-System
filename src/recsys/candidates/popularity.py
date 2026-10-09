"""Non-personalized and category-level popularity baselines."""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from recsys.candidates.common import CANDIDATE_COLUMNS, decay, ranked_list, top_n_per_user
from recsys.config import CategoryCandidatesConfig
from recsys.features.split import Cutoff
from recsys.features.user import in_window, weight_expr


def item_popularity(
    history: DataFrame, cut: Cutoff, window_days: int, weights: Mapping[str, float]
) -> DataFrame:
    """(item_id, category_id, score): weighted events in the trailing window.

    ``category_id`` is the item's category at its most recent event (point-in-time).
    """
    recent = history.where(in_window(cut, window_days))
    latest = Window.partitionBy("item_id").orderBy(F.col("ts_ms").desc())
    return (
        recent.withColumn("_w", weight_expr(weights))
        .withColumn("_cat", F.first("category_id").over(latest))
        .groupBy("item_id")
        .agg(F.sum("_w").alias("score"), F.first("_cat").alias("category_id"))
    )


def global_popularity(
    history: DataFrame,
    targets: DataFrame,
    cut: Cutoff,
    window_days: int,
    weights: Mapping[str, float],
    n: int,
) -> DataFrame:
    """The same top-n list for every target visitor (works for cold-start visitors)."""
    top = ranked_list(
        item_popularity(history, cut, window_days, weights).select("item_id", "score"), n
    )
    return targets.select("visitor_id").crossJoin(F.broadcast(top)).select(*CANDIDATE_COLUMNS)


def category_popularity(
    history: DataFrame,
    targets: DataFrame,
    cut: Cutoff,
    cfg: CategoryCandidatesConfig,
    weights: Mapping[str, float],
    n: int,
) -> DataFrame:
    """Popular items from each warm visitor's top categories.

    score = (visitor's decayed share of interactions in the category)
          x (item's share of the category's recent popularity)
    """
    past = history.join(targets.select("visitor_id"), "visitor_id", "left_semi").where(
        F.col("category_id").isNotNull()
    )
    user_cat = past.groupBy("visitor_id", "category_id").agg(
        F.sum(weight_expr(weights) * decay(cut, cfg.half_life_days)).alias("_aff")
    )
    by_user = Window.partitionBy("visitor_id")
    top_cats = (
        user_cat.withColumn("_share", F.col("_aff") / F.sum("_aff").over(by_user))
        .withColumn(
            "_crank",
            F.row_number().over(by_user.orderBy(F.col("_aff").desc(), F.col("category_id"))),
        )
        .where(F.col("_crank") <= cfg.top_categories)
    )
    pop = item_popularity(history, cut, cfg.window_days, weights).where(
        F.col("category_id").isNotNull()
    )
    by_cat = Window.partitionBy("category_id")
    pop = (
        pop.withColumn("_item_share", F.col("score") / F.sum("score").over(by_cat))
        .withColumn(
            "_irank", F.row_number().over(by_cat.orderBy(F.col("score").desc(), F.col("item_id")))
        )
        .where(F.col("_irank") <= n)
    )
    scored = top_cats.join(pop, "category_id").select(
        "visitor_id", "item_id", (F.col("_share") * F.col("_item_share")).alias("score")
    )
    # An item belongs to one category, so (visitor, item) is already unique.
    return top_n_per_user(scored, n)

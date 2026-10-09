"""Item-item co-occurrence from sessions, and user recommendations from seed items.

Similarity (cosine over session incidence):
    sim(i, j) = sessions(i, j) / sqrt(sessions(i) * sessions(j))
keeping pairs seen in at least ``min_pair_sessions`` sessions and the top
``neighbors_per_item`` neighbors per item.

Skew: the pair self-join is bounded by ``max_session_items`` (quadratic in session size),
and the seed x neighbor join by ``neighbors_per_item``. Popular items are still hot keys in
the seed join, so it can be salted (``salt_buckets``) on top of AQE's skew-join handling.
"""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from recsys.candidates.common import decay, top_n_per_user
from recsys.config import CooccurrenceConfig
from recsys.features.split import Cutoff
from recsys.features.user import in_window, weight_expr


def item_neighbors(history: DataFrame, cut: Cutoff, cfg: CooccurrenceConfig) -> DataFrame:
    """(item_id, neighbor_id, similarity, neighbor_rank) from sessions in the window."""
    members = (
        history.where(in_window(cut, cfg.window_days)).select("session_id", "item_id").distinct()
    )
    size = members.groupBy("session_id").agg(F.count(F.lit(1)).alias("_n"))
    members = members.join(
        size.where((F.col("_n") >= 2) & (F.col("_n") <= cfg.max_session_items)),
        "session_id",
        "left_semi",
    )
    item_sessions = members.groupBy("item_id").agg(F.count(F.lit(1)).alias("_ni"))
    other = members.select("session_id", F.col("item_id").alias("neighbor_id"))
    pairs = (
        members.join(other, "session_id")
        .where(F.col("item_id") != F.col("neighbor_id"))
        .groupBy("item_id", "neighbor_id")
        .agg(F.count(F.lit(1)).alias("_nij"))
        .where(F.col("_nij") >= cfg.min_pair_sessions)
    )
    sims = (
        pairs.join(item_sessions, "item_id")
        .join(
            item_sessions.select(F.col("item_id").alias("neighbor_id"), F.col("_ni").alias("_nj")),
            "neighbor_id",
        )
        .withColumn("similarity", F.col("_nij") / F.sqrt(F.col("_ni") * F.col("_nj")))
    )
    w = Window.partitionBy("item_id").orderBy(F.col("similarity").desc(), F.col("neighbor_id"))
    return (
        sims.withColumn("neighbor_rank", F.row_number().over(w))
        .where(F.col("neighbor_rank") <= cfg.neighbors_per_item)
        .select("item_id", "neighbor_id", "similarity", "neighbor_rank")
    )


def salted_join(
    left: DataFrame, right: DataFrame, key: str, buckets: int, salt_from: str
) -> DataFrame:
    """Inner join on ``key`` with the key salted into ``buckets`` sub-keys.

    The left side's salt is a deterministic hash of ``salt_from``; the right side is
    replicated once per bucket. Results are identical to a plain join on ``key``.
    """
    if buckets <= 1:
        return left.join(right, key)
    salt = F.pmod(F.xxhash64(F.col(salt_from)), F.lit(buckets))
    salted_left = left.withColumn("_salt", salt)
    salted_right = right.crossJoin(
        left.sparkSession.range(buckets).select(F.col("id").cast("long").alias("_salt"))
    )
    return salted_left.join(salted_right, [key, "_salt"]).drop("_salt")


def seed_items(
    history: DataFrame,
    targets: DataFrame,
    cut: Cutoff,
    cfg: CooccurrenceConfig,
    weights: Mapping[str, float],
) -> DataFrame:
    """(visitor_id, item_id, seed_weight): each visitor's top recent items."""
    scored = (
        history.join(targets.select("visitor_id"), "visitor_id", "left_semi")
        .groupBy("visitor_id", "item_id")
        .agg(F.sum(weight_expr(weights) * decay(cut, cfg.seed_half_life_days)).alias("seed_weight"))
    )
    w = Window.partitionBy("visitor_id").orderBy(F.col("seed_weight").desc(), F.col("item_id"))
    return (
        scored.withColumn("_r", F.row_number().over(w))
        .where(F.col("_r") <= cfg.seed_items)
        .drop("_r")
    )


def recommend(seeds: DataFrame, neighbors: DataFrame, cfg: CooccurrenceConfig, n: int) -> DataFrame:
    """score(visitor, j) = sum over seeds s of seed_weight(s) x sim(s, j); seeds excluded."""
    joined = salted_join(
        seeds,
        neighbors.select("item_id", "neighbor_id", "similarity"),
        "item_id",
        cfg.salt_buckets,
        salt_from="visitor_id",
    )
    scored = (
        joined.groupBy("visitor_id", F.col("neighbor_id").alias("item_id"))
        .agg(F.sum(F.col("seed_weight") * F.col("similarity")).alias("score"))
        .join(seeds.select("visitor_id", "item_id"), ["visitor_id", "item_id"], "left_anti")
    )
    return top_n_per_user(scored, n)

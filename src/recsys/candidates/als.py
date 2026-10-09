"""Implicit-feedback ALS (Spark MLlib) trained on history before the cutoff.

Confidence input r(u, i) = sum of event weights over the visitor's history on the item.
MLlib's implicit ALS uses confidence 1 + alpha * r. Only visitors with at least
``min_user_items`` distinct items and items with at least ``min_item_users`` distinct
visitors are used for training; other target visitors get no ALS candidates.
"""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from recsys.candidates.common import CANDIDATE_COLUMNS
from recsys.config import ALSConfig
from recsys.features.user import weight_expr


def interactions(history: DataFrame, cfg: ALSConfig, weights: Mapping[str, float]) -> DataFrame:
    """(visitor_id, item_id, strength), after the min-interaction filters."""
    r = history.groupBy("visitor_id", "item_id").agg(F.sum(weight_expr(weights)).alias("strength"))
    items = (
        r.groupBy("item_id")
        .agg(F.count(F.lit(1)).alias("_n"))
        .where(F.col("_n") >= cfg.min_item_users)
    )
    r = r.join(items.select("item_id"), "item_id", "left_semi")
    users = (
        r.groupBy("visitor_id")
        .agg(F.count(F.lit(1)).alias("_n"))
        .where(F.col("_n") >= cfg.min_user_items)
    )
    return r.join(users.select("visitor_id"), "visitor_id", "left_semi")


def train(ratings: DataFrame, cfg: ALSConfig) -> ALSModel:
    als = ALS(
        userCol="visitor_id",
        itemCol="item_id",
        ratingCol="strength",
        implicitPrefs=True,
        rank=cfg.rank,
        regParam=cfg.reg_param,
        alpha=cfg.alpha,
        maxIter=cfg.max_iter,
        seed=cfg.seed,
        coldStartStrategy="drop",
        # Truncate lineage every 5 iterations (needs spark.checkpoint.dir); bounds the
        # shuffle data kept on disk for large ranks.
        checkpointInterval=5,
        nonnegative=False,
    )
    return als.fit(ratings)


def recommend(model: ALSModel, targets: DataFrame, n: int) -> DataFrame:
    """Top-n items for target visitors known to the model (others are skipped)."""
    known = targets.select("visitor_id").join(
        model.userFactors.select(F.col("id").alias("visitor_id")), "visitor_id", "left_semi"
    )
    recs = model.recommendForUserSubset(known, n)
    exploded = recs.select("visitor_id", F.posexplode("recommendations").alias("_pos", "_rec"))
    return exploded.select(
        "visitor_id",
        F.col("_rec.item_id").alias("item_id"),
        F.col("_rec.rating").cast("double").alias("score"),
        (F.col("_pos") + 1).cast("int").alias("rank"),
    ).select(*CANDIDATE_COLUMNS)

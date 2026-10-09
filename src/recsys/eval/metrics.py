"""Top-K ranking metrics: Precision, Recall, NDCG, MAP, hit rate.

Two implementations with identical definitions:
- pure-Python per-user functions (reference, used in tests and small analyses)
- ``ranking_metrics`` / ``per_user_metrics`` in Spark for full evaluation runs

Definitions, for one user with recommendations r_1..r_K and relevant set R
(truth items with relevance >= ``min_relevance``):
- precision@K = |top-K ∩ R| / K           (K, not len(recs): short lists are penalized)
- recall@K    = |top-K ∩ R| / |R|
- AP@K        = Σ_{i<=K, r_i ∈ R} precision@i / min(|R|, K)
- NDCG@K      = DCG@K / IDCG@K, gain(rel) = 2^rel - 1, discount log2(i + 1)
- hit@K       = 1 if |top-K ∩ R| > 0
Averages are over users with |R| > 0; users without recommendations score 0.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

METRICS = ("precision", "recall", "ndcg", "map", "hit_rate")


def _relevant[T](truth: Mapping[T, int], min_relevance: int) -> dict[T, int]:
    return {i: r for i, r in truth.items() if r >= min_relevance}


def precision_at_k[T](
    recs: Sequence[T], truth: Mapping[T, int], k: int, min_relevance: int = 1
) -> float:
    rel = _relevant(truth, min_relevance)
    return sum(1 for i in recs[:k] if i in rel) / k


def recall_at_k[T](
    recs: Sequence[T], truth: Mapping[T, int], k: int, min_relevance: int = 1
) -> float:
    rel = _relevant(truth, min_relevance)
    if not rel:
        return 0.0
    return sum(1 for i in recs[:k] if i in rel) / len(rel)


def average_precision_at_k[T](
    recs: Sequence[T], truth: Mapping[T, int], k: int, min_relevance: int = 1
) -> float:
    rel = _relevant(truth, min_relevance)
    if not rel:
        return 0.0
    hits, total = 0, 0.0
    for i, item in enumerate(recs[:k], start=1):
        if item in rel:
            hits += 1
            total += hits / i
    return total / min(len(rel), k)


def _gain(rel: int) -> float:
    return float(2**rel - 1)


def ndcg_at_k[T](
    recs: Sequence[T], truth: Mapping[T, int], k: int, min_relevance: int = 1
) -> float:
    rel = _relevant(truth, min_relevance)
    if not rel:
        return 0.0
    dcg = sum(_gain(rel[i]) / math.log2(n + 1) for n, i in enumerate(recs[:k], start=1) if i in rel)
    ideal = sorted(rel.values(), reverse=True)[:k]
    idcg = sum(_gain(r) / math.log2(n + 1) for n, r in enumerate(ideal, start=1))
    return dcg / idcg


def hit_at_k[T](recs: Sequence[T], truth: Mapping[T, int], k: int, min_relevance: int = 1) -> float:
    rel = _relevant(truth, min_relevance)
    return 1.0 if any(i in rel for i in recs[:k]) else 0.0


def _log2(c: Column) -> Column:
    return F.log2(c.cast("double"))


def per_user_metrics(
    recs: DataFrame,
    truth: DataFrame,
    ks: Sequence[int],
    user_col: str = "visitor_id",
    item_col: str = "item_id",
    rank_col: str = "rank",
    relevance_col: str = "relevance",
    min_relevance: int = 1,
) -> DataFrame:
    """One row per user with |R| > 0 and columns ``<metric>_at_<k>``.

    ``recs`` needs (user, item, rank) with rank = 1, 2, ... per user. If an item appears
    twice for a user, only its best rank counts. ``truth`` needs (user, item, relevance).
    """
    kmax = max(ks)
    rel = (
        truth.where(F.col(relevance_col) >= min_relevance)
        .groupBy(user_col, item_col)
        .agg(F.max(relevance_col).alias("_rel"))
    )
    n_rel = rel.groupBy(user_col).agg(F.count(F.lit(1)).alias("_n_rel"))

    by_rel = Window.partitionBy(user_col).orderBy(F.col("_rel").desc(), F.col(item_col))
    ideal = rel.withColumn("_irank", F.row_number().over(by_rel)).where(F.col("_irank") <= kmax)
    ideal_gain = (F.pow(F.lit(2.0), F.col("_rel")) - 1) / _log2(F.col("_irank") + 1)
    idcg = ideal.groupBy(user_col).agg(
        *[F.sum(F.when(F.col("_irank") <= k, ideal_gain)).alias(f"_idcg_{k}") for k in ks]
    )

    best = (
        recs.where(F.col(rank_col) <= kmax)
        .groupBy(user_col, item_col)
        .agg(F.min(rank_col).alias("_rank"))
        .join(F.broadcast(n_rel.select(user_col)), user_col, "left_semi")
    )
    scored = best.join(rel, [user_col, item_col], "left").withColumn(
        "_hit", F.col("_rel").isNotNull().cast("int")
    )
    cum = Window.partitionBy(user_col).orderBy("_rank").rowsBetween(Window.unboundedPreceding, 0)
    scored = scored.withColumn("_cum_hits", F.sum("_hit").over(cum))
    gain = F.when(
        F.col("_hit") == 1, (F.pow(F.lit(2.0), F.col("_rel")) - 1) / _log2(F.col("_rank") + 1)
    )
    prec_at_i = F.when(F.col("_hit") == 1, F.col("_cum_hits") / F.col("_rank"))
    aggs = []
    for k in ks:
        in_k = F.col("_rank") <= k
        aggs += [
            F.sum(F.when(in_k, F.col("_hit")).otherwise(0)).alias(f"_hits_{k}"),
            F.sum(F.when(in_k, gain)).alias(f"_dcg_{k}"),
            F.sum(F.when(in_k, prec_at_i)).alias(f"_ap_{k}"),
        ]
    per_user = scored.groupBy(user_col).agg(*aggs)

    out = n_rel.join(idcg, user_col, "left").join(per_user, user_col, "left")
    cols = [F.col(user_col), F.col("_n_rel").alias("n_relevant")]
    for k in ks:
        hits = F.coalesce(F.col(f"_hits_{k}"), F.lit(0))
        cols += [
            (hits / F.lit(float(k))).alias(f"precision_at_{k}"),
            (hits / F.col("_n_rel")).alias(f"recall_at_{k}"),
            (F.coalesce(F.col(f"_dcg_{k}"), F.lit(0.0)) / F.col(f"_idcg_{k}")).alias(
                f"ndcg_at_{k}"
            ),
            (F.coalesce(F.col(f"_ap_{k}"), F.lit(0.0)) / F.least(F.col("_n_rel"), F.lit(k))).alias(
                f"map_at_{k}"
            ),
            (hits > 0).cast("double").alias(f"hit_rate_at_{k}"),
        ]
    return out.select(*cols)


def summarize(per_user: DataFrame, ks: Sequence[int]) -> list[dict[str, float]]:
    """Average per-user metrics: one dict per k with keys ``k``, ``users``, and METRICS."""
    aggs = [F.count(F.lit(1)).alias("users")] + [
        F.avg(f"{m}_at_{k}").alias(f"{m}_at_{k}") for k in ks for m in METRICS
    ]
    row = per_user.agg(*aggs).collect()[0]
    users = int(row["users"])
    return [
        {"k": k, "users": users, **{m: float(row[f"{m}_at_{k}"] or 0.0) for m in METRICS}}
        for k in ks
    ]


def ranking_metrics(
    recs: DataFrame,
    truth: DataFrame,
    ks: Sequence[int],
    user_col: str = "visitor_id",
    item_col: str = "item_id",
    rank_col: str = "rank",
    relevance_col: str = "relevance",
    min_relevance: int = 1,
) -> list[dict[str, float]]:
    per_user = per_user_metrics(
        recs, truth, ks, user_col, item_col, rank_col, relevance_col, min_relevance
    )
    return summarize(per_user, ks)

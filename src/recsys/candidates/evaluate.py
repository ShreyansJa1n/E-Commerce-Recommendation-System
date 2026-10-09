"""Offline evaluation of candidate sources against future interactions (labels)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from recsys.eval.metrics import per_user_metrics, summarize


def segments() -> dict[str, Column]:
    """Visitor segments (built lazily: Column objects need an active SparkContext)."""
    return {
        "all": F.lit(True),
        "warm": F.col("visitor_has_history"),
        "cold": ~F.col("visitor_has_history"),
    }


def union_recall(candidates: DataFrame, truth: DataFrame, ks: Sequence[int]) -> dict[int, float]:
    """Mean recall of the union of every source's top-K, over users with truth."""
    n_rel = truth.groupBy("visitor_id").agg(F.count(F.lit(1)).alias("_n"))
    users = n_rel.count()
    out: dict[int, float] = {}
    for k in ks:
        hits = (
            candidates.where(F.col("rank") <= k)
            .select("visitor_id", "item_id")
            .distinct()
            .join(truth.select("visitor_id", "item_id"), ["visitor_id", "item_id"])
            .groupBy("visitor_id")
            .agg(F.count(F.lit(1)).alias("_h"))
        )
        total = (
            n_rel.join(hits, "visitor_id", "left")
            .agg(F.sum(F.coalesce(F.col("_h"), F.lit(0)) / F.col("_n")))
            .collect()[0][0]
        )
        out[k] = float(total or 0.0) / users if users else 0.0
    return out


def evaluate(
    candidates: DataFrame,
    labels: DataFrame,
    split: str,
    ks: Sequence[int],
    min_relevances: Sequence[int] = (1, 2),
) -> list[dict[str, Any]]:
    """Metrics per (source, segment, min_relevance, k) plus the union of all sources."""
    cands = candidates.where(F.col("split") == split).cache()
    lab = labels.where(F.col("split") == split).cache()
    sources = sorted(r.source for r in cands.select("source").distinct().collect())
    rows: list[dict[str, Any]] = []
    for segment, cond in segments().items():
        seg_labels = lab.where(cond)
        seg_visitors = seg_labels.select("visitor_id").distinct()
        n_visitors = seg_visitors.count()
        for min_rel in min_relevances:
            truth = seg_labels.where(F.col("relevance") >= min_rel)
            for source in [*sources, "union"]:
                src = cands if source == "union" else cands.where(F.col("source") == source)
                src = src.join(seg_visitors, "visitor_id", "left_semi")
                covered = src.select("visitor_id").distinct().count()
                base = {
                    "split": split,
                    "source": source,
                    "segment": segment,
                    "min_relevance": min_rel,
                    "coverage": covered / n_visitors if n_visitors else 0.0,
                }
                if source == "union":
                    for k, rec in union_recall(src, truth, ks).items():
                        rows.append({**base, "k": k, "recall": rec})
                    continue
                for m in summarize(per_user_metrics(src, truth, ks), ks):
                    rows.append({**base, **m})
    cands.unpersist()
    lab.unpersist()
    return rows


def leave_one_out(
    candidates: DataFrame, labels: DataFrame, split: str, ks: Sequence[int], min_relevance: int = 1
) -> list[dict[str, Any]]:
    """Union recall with all sources vs. without each one (its marginal contribution)."""
    cands = candidates.where(F.col("split") == split).cache()
    lab = labels.where((F.col("split") == split) & (F.col("relevance") >= min_relevance)).cache()
    sources = sorted(r.source for r in cands.select("source").distinct().collect())
    rows: list[dict[str, Any]] = []
    for segment, cond in segments().items():
        truth = lab.where(cond)
        full = union_recall(cands, truth, ks)
        for source in sources:
            rest = cands.where(F.col("source") != source)
            without = union_recall(rest, truth, ks)
            for k in ks:
                rows.append(
                    {
                        "split": split,
                        "segment": segment,
                        "min_relevance": min_relevance,
                        "removed": source,
                        "k": k,
                        "union_recall": full[k],
                        "union_recall_without": without[k],
                        "drop": full[k] - without[k],
                    }
                )
    cands.unpersist()
    lab.unpersist()
    return rows

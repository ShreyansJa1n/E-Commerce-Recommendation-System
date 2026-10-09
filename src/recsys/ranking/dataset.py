"""Ranking examples: merged candidates + point-in-time features + labels, per cutoff.

One row per (cutoff, visitor, item) in the union of all candidate sources. Features:
- ``src_*``: per-source rank and score (null when the source didn't propose the item),
  and how many sources proposed it
- ``u_*``: gold/user_features at T (null for cold-start visitors)
- ``i_*``: gold/item_features at T
- ``ui_*``: the visitor's own history with the item before T
- ``x_*``: cross features (visitor's affinity to the item's category at T)
Every input is a gold table keyed by the same cutoff, or ``history(events, T)``, so no
example can see anything at or after T. ``label`` is the label-window relevance (0 = none).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from recsys.features.split import Cutoff, history
from recsys.features.user import days_before, is_type, weight_expr

KEYS = ["cutoff_date", "split", "visitor_id", "item_id"]
LABEL = "label"
# Raw identifiers are not meaningful as numeric features.
ID_COLUMNS = {"i_category_id", "i_root_category_id", "u_top_category_id_30d"}


def _prefixed(df: DataFrame, prefix: str, keys: Sequence[str]) -> DataFrame:
    return df.select(
        *keys,
        *[F.col(c).alias(f"{prefix}{c}") for c in df.columns if c not in keys and c != "split"],
    )


def source_features(candidates: DataFrame, sources: Sequence[str]) -> DataFrame:
    """Pivot per-source rank/score onto the deduplicated (visitor, item) union."""
    aggs: list[Column] = []
    for s in sources:
        hit = F.col("source") == s
        aggs += [
            F.min(F.when(hit, F.col("rank"))).alias(f"src_{s}_rank"),
            F.max(F.when(hit, F.col("score"))).alias(f"src_{s}_score"),
        ]
    aggs.append(F.countDistinct("source").alias("src_n_sources"))
    return candidates.groupBy("cutoff_date", "split", "visitor_id", "item_id").agg(*aggs)


def user_item_history(
    events: DataFrame, pairs: DataFrame, cut: Cutoff, weights: Mapping[str, float]
) -> DataFrame:
    past = history(events, cut).join(
        pairs.select("visitor_id", "item_id"), ["visitor_id", "item_id"], "left_semi"
    )
    return past.groupBy("visitor_id", "item_id").agg(
        F.count(F.lit(1)).alias("ui_n_events"),
        F.sum(is_type("view").cast("int")).alias("ui_n_views"),
        F.sum(is_type("addtocart").cast("int")).alias("ui_n_addtocart"),
        F.sum(is_type("transaction").cast("int")).alias("ui_n_transactions"),
        F.min(days_before(cut, F.col("ts_ms"))).alias("ui_days_since_last"),
        F.max(days_before(cut, F.col("ts_ms"))).alias("ui_days_since_first"),
        F.sum(weight_expr(weights)).alias("ui_weighted_events"),
    )


def build_examples(
    cut: Cutoff,
    candidates: DataFrame,
    events: DataFrame,
    user_features: DataFrame,
    item_features: DataFrame,
    affinity: DataFrame,
    labels: DataFrame,
    sources: Sequence[str],
    weights: Mapping[str, float],
) -> DataFrame:
    at = F.col("cutoff_date") == F.lit(cut.cutoff_date)
    pairs = source_features(candidates.where(at), sources)
    users = _prefixed(user_features.where(at), "u_", ["cutoff_date", "visitor_id"])
    items = _prefixed(item_features.where(at), "i_", ["cutoff_date", "item_id"])
    aff = affinity.where(at).select(
        "visitor_id",
        F.col("category_id").alias("i_category_id"),
        F.col("affinity_share").alias("x_category_affinity_share"),
        F.col("affinity_score").alias("x_category_affinity_score"),
    )
    lab = labels.where(at).select("visitor_id", "item_id", F.col("relevance").alias(LABEL))
    ui = user_item_history(events, pairs, cut, weights)
    out = (
        pairs.join(users, ["cutoff_date", "visitor_id"], "left")
        .join(items, ["cutoff_date", "item_id"], "left")
        .join(aff, ["visitor_id", "i_category_id"], "left")
        .join(ui, ["visitor_id", "item_id"], "left")
        .join(lab, ["visitor_id", "item_id"], "left")
        .withColumn(LABEL, F.coalesce(F.col(LABEL), F.lit(0)).cast("int"))
        .withColumn("u_is_cold", F.col("u_n_events_total").isNull().cast("int"))
        .withColumn(
            "x_item_in_top_category",
            (F.col("i_category_id") == F.col("u_top_category_id_30d")).cast("int"),
        )
        .fillna(
            0,
            subset=[
                "ui_n_events",
                "ui_n_views",
                "ui_n_addtocart",
                "ui_n_transactions",
                "ui_weighted_events",
            ],
        )
    )
    return out


def feature_columns(df: DataFrame) -> list[str]:
    """Numeric model inputs: everything except keys, label, and raw identifiers."""
    numeric = {"int", "bigint", "double", "float", "smallint", "tinyint"}
    return [
        f.name
        for f in df.schema
        if f.name not in {*KEYS, LABEL, *ID_COLUMNS} and f.dataType.simpleString() in numeric
    ]


def sample_training_queries(examples: DataFrame, max_negatives: int, salt: str) -> DataFrame:
    """Keep queries with >= 1 positive; keep all positives and up to ``max_negatives``
    hash-sampled negatives per query (deterministic)."""
    q = ["cutoff_date", "visitor_id"]
    positive_queries = examples.where(F.col(LABEL) > 0).select(*q).distinct()
    kept = examples.join(positive_queries, q, "left_semi")
    order = F.xxhash64(F.lit(salt), F.col("cutoff_date"), F.col("visitor_id"), F.col("item_id"))
    w = Window.partitionBy(*q, F.col(LABEL) > 0).orderBy(order)
    return (
        kept.withColumn("_n", F.row_number().over(w))
        .where((F.col(LABEL) > 0) | (F.col("_n") <= max_negatives))
        .drop("_n")
    )

"""User features and user-category affinity as of a cutoff (see docs/feature_contract.md)."""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from recsys.config import FeatureConfig
from recsys.features.split import Cutoff, history

MS_PER_DAY = 86_400_000.0
EVENT_TYPES = ("view", "addtocart", "transaction")
_PLURAL = {"view": "views", "addtocart": "addtocart", "transaction": "transactions"}


def days_before(cut: Cutoff, ms_col: Column) -> Column:
    return (F.lit(int(cut.ts.timestamp() * 1000)) - ms_col) / F.lit(MS_PER_DAY)


def in_window(cut: Cutoff, days: int) -> Column:
    return days_before(cut, F.col("ts_ms")) <= F.lit(float(days))


def is_type(event: str) -> Column:
    return F.col("event_type") == event


def weight_expr(weights: Mapping[str, float]) -> Column:
    return F.coalesce(
        *[F.when(is_type(e), F.lit(float(w))) for e, w in weights.items()], F.lit(0.0)
    )


def _ratio(num: str, den: str) -> Column:
    """num/den clipped to [0, 1]; null when den == 0."""
    return F.when(F.col(den) > 0, F.least(F.col(num) / F.col(den), F.lit(1.0)))


def build_user_features(events: DataFrame, cut: Cutoff, cfg: FeatureConfig) -> DataFrame:
    past = history(events, cut)
    aggs: list[Column] = [
        F.max(days_before(cut, F.col("ts_ms"))).alias("days_since_first_event"),
        F.min(days_before(cut, F.col("ts_ms"))).alias("days_since_last_event"),
        *[
            F.min(F.when(is_type(e), days_before(cut, F.col("ts_ms")))).alias(
                f"days_since_last_{e}"
            )
            for e in EVENT_TYPES
        ],
        F.count(F.lit(1)).alias("n_events_total"),
        F.countDistinct("session_id").alias("n_sessions_total"),
    ]
    for w in cfg.windows_days:
        win = in_window(cut, w)
        aggs += [
            *[
                F.sum((win & is_type(e)).cast("int")).alias(f"n_{_PLURAL[e]}_{w}d")
                for e in EVENT_TYPES
            ],
            F.countDistinct(F.when(win, F.col("session_id"))).alias(f"n_sessions_{w}d"),
            F.countDistinct(F.when(win, F.col("item_id"))).alias(f"n_distinct_items_{w}d"),
        ]
    feats = past.groupBy("visitor_id").agg(*aggs)
    for w in cfg.windows_days:
        feats = feats.withColumn(
            f"view_to_cart_rate_{w}d", _ratio(f"n_addtocart_{w}d", f"n_views_{w}d")
        ).withColumn(
            f"cart_to_purchase_rate_{w}d", _ratio(f"n_transactions_{w}d", f"n_addtocart_{w}d")
        )

    aw = max(cfg.windows_days)
    affinity = build_user_category_affinity(events, cut, cfg)
    top = Window.partitionBy("visitor_id").orderBy(
        F.col("affinity_score").desc(), F.col("category_id")
    )
    cat_summary = (
        affinity.withColumn("_rn", F.row_number().over(top))
        .groupBy("visitor_id")
        .agg(
            F.count(F.lit(1)).alias(f"n_categories_{aw}d"),
            F.max(F.when(F.col("_rn") == 1, F.col("category_id"))).alias(f"top_category_id_{aw}d"),
            F.max(F.when(F.col("_rn") == 1, F.col("affinity_share"))).alias(
                f"top_category_share_{aw}d"
            ),
        )
    )
    return (
        feats.join(cat_summary, "visitor_id", "left")
        .fillna({f"n_categories_{aw}d": 0})
        .select(F.lit(cut.cutoff_date).alias("cutoff_date"), F.lit(cut.split).alias("split"), "*")
    )


def build_user_category_affinity(events: DataFrame, cut: Cutoff, cfg: FeatureConfig) -> DataFrame:
    """Weighted interaction score per (visitor, category) over the largest window."""
    aw = max(cfg.windows_days)
    recent = history(events, cut).where(in_window(cut, aw) & F.col("category_id").isNotNull())
    scores = recent.groupBy("visitor_id", "category_id").agg(
        F.sum(weight_expr(cfg.event_weights)).alias("affinity_score")
    )
    total = Window.partitionBy("visitor_id")
    return scores.where(F.col("affinity_score") > 0).select(
        F.lit(cut.cutoff_date).alias("cutoff_date"),
        F.lit(cut.split).alias("split"),
        "visitor_id",
        "category_id",
        "affinity_score",
        (F.col("affinity_score") / F.sum("affinity_score").over(total)).alias("affinity_share"),
    )

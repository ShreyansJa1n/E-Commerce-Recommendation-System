"""Item features as of a cutoff (see docs/feature_contract.md).

The item universe at T is every item with an event before T or a catalog version
valid at T, so catalog-only (cold) items still get category/availability features.
"""

from __future__ import annotations

from datetime import timedelta

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from recsys.clean.catalog import property_as_of
from recsys.config import FeatureConfig
from recsys.features.split import Cutoff, history
from recsys.features.user import _PLURAL, EVENT_TYPES, days_before, in_window, is_type, weight_expr


def catalog_as_of(scd: DataFrame, categories: DataFrame, cut: Cutoff) -> DataFrame:
    """Catalog state just before T: the last microsecond (Spark's timestamp precision)
    before ``cut.ts``. A version first observed exactly at T is not visible."""
    as_of = cut.ts - timedelta(microseconds=1)
    items = (
        scd.where(F.col("valid_from") <= F.lit(as_of))
        .select("item_id")
        .distinct()
        .withColumn("_t", F.lit(as_of))
    )
    cat = property_as_of(items, scd, "categoryid", "_t", "_category")
    cat = property_as_of(cat, scd, "available", "_t", "_available")
    return (
        cat.select(
            "item_id",
            F.col("_category").try_cast("int").alias("category_id"),
            F.col("_available").try_cast("int").alias("available"),
        )
        .join(
            categories.select("category_id", "root_category_id", "category_level"),
            "category_id",
            "left",
        )
        .select("item_id", "category_id", "root_category_id", "category_level", "available")
    )


def _cooccurrence_neighbors(
    pairs_input: DataFrame, group_col: str, out_col: str, max_items: int | None = None
) -> DataFrame:
    """Distinct partner items per item, where partners share ``group_col``."""
    members = pairs_input.select(group_col, "item_id").distinct()
    if max_items is not None:
        size = members.groupBy(group_col).agg(F.count(F.lit(1)).alias("_n"))
        members = members.join(
            size.where((F.col("_n") >= 2) & (F.col("_n") <= max_items)), group_col, "left_semi"
        )
    other = members.select(group_col, F.col("item_id").alias("_other"))
    return (
        members.join(other, group_col)
        .where(F.col("item_id") != F.col("_other"))
        .groupBy("item_id")
        .agg(F.countDistinct("_other").alias(out_col))
    )


def build_item_features(
    events: DataFrame,
    scd: DataFrame,
    categories: DataFrame,
    cut: Cutoff,
    cfg: FeatureConfig,
) -> DataFrame:
    past = history(events, cut)
    weight = weight_expr(cfg.event_weights)
    aggs: list[Column] = [
        F.max(days_before(cut, F.col("ts_ms"))).alias("days_since_first_event"),
        F.min(days_before(cut, F.col("ts_ms"))).alias("days_since_last_event"),
    ]
    for w in cfg.windows_days:
        win = in_window(cut, w)
        aggs += [
            *[
                F.sum((win & is_type(e)).cast("int")).alias(f"n_{_PLURAL[e]}_{w}d")
                for e in EVENT_TYPES
            ],
            F.countDistinct(F.when(win, F.col("visitor_id"))).alias(f"n_visitors_{w}d"),
            F.sum(F.when(win, weight).otherwise(0.0)).alias(f"_score_{w}d"),
        ]
    activity = past.groupBy("item_id").agg(*aggs)

    cw = cfg.cooccurrence_window_days
    coview = _cooccurrence_neighbors(
        past.where(in_window(cut, cw) & is_type("view")),
        "session_id",
        f"coview_neighbors_{cw}d",
        cfg.max_session_items_for_pairs,
    )
    copurchase = _cooccurrence_neighbors(
        past.where(F.col("transaction_id").isNotNull()),
        "transaction_id",
        "copurchase_neighbors_total",
    )

    catalog = catalog_as_of(scd, categories, cut)
    universe = catalog.select("item_id").union(activity.select("item_id")).distinct()
    feats = (
        universe.join(catalog, "item_id", "left")
        .join(activity, "item_id", "left")
        .join(coview, "item_id", "left")
        .join(copurchase, "item_id", "left")
    )
    count_cols = [f"n_{_PLURAL[e]}_{w}d" for w in cfg.windows_days for e in EVENT_TYPES] + [
        f"n_visitors_{w}d" for w in cfg.windows_days
    ]
    feats = feats.fillna(
        0, subset=[*count_cols, f"coview_neighbors_{cw}d", "copurchase_neighbors_total"]
    ).fillna(0.0, subset=[f"_score_{w}d" for w in cfg.windows_days])

    for w in cfg.windows_days:
        rank = Window.orderBy(F.col(f"_score_{w}d").desc())
        # A global window is a single partition; acceptable for ~400k items per cutoff.
        feats = feats.withColumn(
            f"popularity_rank_{w}d",
            F.when(F.col(f"_score_{w}d") > 0, F.dense_rank().over(rank)),
        )

    # Bayesian-smoothed conversion over the largest window, shrunk toward the global rate.
    aw = max(cfg.windows_days)
    views, carts, buys = (f"n_{_PLURAL[e]}_{aw}d" for e in EVENT_TYPES)
    totals = feats.agg(F.sum(views), F.sum(carts), F.sum(buys)).collect()[0]
    tv = float(totals[0] or 0)
    g_cart = min(float(totals[1] or 0) / tv, 1.0) if tv else 0.0
    g_buy = min(float(totals[2] or 0) / tv, 1.0) if tv else 0.0
    a = float(cfg.conversion_prior_views)

    def smoothed(num: str, prior: float) -> Column:
        den = F.col(views) + F.lit(a)
        rate = F.when(den > 0, (F.col(num) + F.lit(a * prior)) / den).otherwise(F.lit(prior))
        return F.least(rate, F.lit(1.0))

    feats = feats.withColumn(f"cart_rate_{aw}d", smoothed(carts, g_cart)).withColumn(
        f"purchase_rate_{aw}d", smoothed(buys, g_buy)
    )
    return feats.drop(*[f"_score_{w}d" for w in cfg.windows_days]).select(
        F.lit(cut.cutoff_date).alias("cutoff_date"), F.lit(cut.split).alias("split"), "*"
    )

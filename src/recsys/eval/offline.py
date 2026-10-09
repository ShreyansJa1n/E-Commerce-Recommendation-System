"""Offline evaluation of serving policies: accuracy, beyond-accuracy, and bootstrap
comparisons (paired and simulated A/B). Entry point: ``run`` (``make eval``)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from recsys.config import REPO_ROOT, Config
from recsys.eval import figures, report, stats
from recsys.eval.metrics import METRICS, per_user_metrics
from recsys.io import StageReport, read_table, timed_stage
from recsys.ranking import baselines
from recsys.ranking.pipeline import BLEND

SEGMENTS = ("all", "warm", "cold")


def policy_recs(spark: SparkSession, cfg: Config, split: str) -> DataFrame:
    """(source, cutoff_date, visitor_id, item_id, rank) for every evaluated policy."""
    gold = cfg.paths.resolved().gold
    cands = read_table(spark, gold / "candidates").where(F.col("split") == split)
    cols = ["source", "cutoff_date", "visitor_id", "item_id", "rank"]
    parts = [cands.select(*cols)]
    if "ranker" in cfg.evaluation.policies:
        ranked = read_table(spark, gold / "ranked").where(F.col("split") == split)
        parts.append(ranked.withColumn("source", F.lit("ranker")).select(*cols))
    if "blend" in cfg.evaluation.policies:
        blend = baselines.priority_blend(cands, BLEND, cfg.ranking.top_n)
        parts.append(blend.withColumn("source", F.lit("blend")).select(*cols))
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out.where(F.col("source").isin(cfg.evaluation.policies))


def per_user_table(
    recs: DataFrame, labels: DataFrame, policies: list[str], ks: list[int]
) -> pd.DataFrame:
    """Wide per-user frame: visitor_id, warm, and ``<policy>__<metric>_at_<k>`` columns,
    over every visitor with >= 1 relevant item (visitors without recs score 0)."""
    truth = labels.select("visitor_id", "item_id", "relevance")
    warm = labels.select("visitor_id", "visitor_has_history").distinct().toPandas()
    frames = []
    for p in policies:
        pu = per_user_metrics(recs.where(F.col("source") == p), truth, ks).toPandas()
        pu = pu.drop(columns=["n_relevant"]).set_index("visitor_id").add_prefix(f"{p}__")
        frames.append(pu)
    wide = pd.concat(frames, axis=1).fillna(0.0).reset_index()
    return wide.merge(warm, on="visitor_id", how="left").rename(
        columns={"visitor_has_history": "warm"}
    )


def beyond_accuracy(recs: DataFrame, item_features: DataFrame, k: int = 10) -> pd.DataFrame:
    """Per policy over top-k lists: catalog coverage, intra-list category diversity,
    novelty (mean self-information of 30-day item popularity), mean popularity rank."""
    items = item_features.select(
        "item_id",
        F.col("category_id").alias("_cat"),
        F.col("n_visitors_30d").alias("_pop"),
        F.col("popularity_rank_30d").alias("_poprank"),
    )
    n_catalog = items.count()
    n_users = item_features.agg(F.sum("n_visitors_30d")).collect()[0][0] or 1
    top = recs.where(F.col("rank") <= k).join(items, "item_id", "left")
    # Self-information with add-one smoothing so unseen items stay finite.
    info = -F.log2((F.coalesce(F.col("_pop"), F.lit(0)) + 1) / F.lit(float(n_users) + 1))
    per_list_cat = (
        top.where(F.col("_cat").isNotNull()).groupBy("source", "visitor_id", "_cat").count()
    )
    ild = (
        per_list_cat.groupBy("source", "visitor_id")
        .agg(
            F.sum("count").alias("_n"),
            F.sum(F.col("count") * (F.col("count") - 1) / 2).alias("_same"),
        )
        .where(F.col("_n") >= 2)
        .withColumn("_ild", 1 - F.col("_same") / (F.col("_n") * (F.col("_n") - 1) / 2))
        .groupBy("source")
        .agg(F.avg("_ild").alias(f"diversity_at_{k}"))
    )
    agg = top.groupBy("source").agg(
        (F.countDistinct("item_id") / F.lit(float(n_catalog))).alias(f"catalog_coverage_at_{k}"),
        F.countDistinct("item_id").alias(f"distinct_items_at_{k}"),
        F.avg(info).alias(f"novelty_at_{k}"),
        F.expr("percentile_approx(_poprank, 0.5)").alias(f"median_popularity_rank_at_{k}"),
    )
    return agg.join(ild, "source", "left").toPandas().set_index("source")


def _segment(df: pd.DataFrame, seg: str) -> pd.DataFrame:
    if seg == "warm":
        return df[df["warm"]]
    if seg == "cold":
        return df[~df["warm"]]
    return df


def _interval(i: stats.Interval) -> dict[str, float | None]:
    return {k: (None if v != v else v) for k, v in i.__dict__.items()}  # NaN -> null


def _cmp_dict(c: stats.Comparison) -> dict[str, Any]:
    return {
        "control_mean": c.control_mean,
        "treatment_mean": c.treatment_mean,
        "n_control": c.n_control,
        "n_treatment": c.n_treatment,
        "abs_diff": _interval(c.abs_diff),
        "rel_lift": _interval(c.rel_lift),
        "p_not_better": c.p_not_better,
        "significant": c.abs_diff.excludes_zero,
    }


def compare(per_user: pd.DataFrame, cfg: Config) -> list[dict[str, Any]]:
    ec = cfg.evaluation
    arms = stats.ab_arms(per_user["visitor_id"].tolist(), ec.ab_salt)
    per_user = per_user.assign(_treat=arms)
    rows = []
    for metric in [ec.primary_metric, *ec.secondary_metrics]:
        for control, treatment in ec.comparisons:
            for seg in SEGMENTS:
                d = _segment(per_user, seg)
                if d.empty:  # e.g. no cold visitors in a tiny dataset
                    continue
                c, t = (
                    d[f"{control}__{metric}"].to_numpy(float),
                    d[f"{treatment}__{metric}"].to_numpy(float),
                )
                seed = ec.seed
                pr = stats.paired(c, t, ec.bootstrap_samples, seed, ec.confidence)
                ab = stats.ab_simulation(
                    d.loc[~d["_treat"], f"{control}__{metric}"].to_numpy(float),
                    d.loc[d["_treat"], f"{treatment}__{metric}"].to_numpy(float),
                    ec.bootstrap_samples,
                    seed,
                    ec.confidence,
                )
                base = {
                    "metric": metric,
                    "control": control,
                    "treatment": treatment,
                    "segment": seg,
                }
                rows.append({**base, "design": "paired", **_cmp_dict(pr)})
                rows.append({**base, "design": "ab_simulation", **_cmp_dict(ab)})
    return rows


def policy_summary(per_user: pd.DataFrame, cfg: Config) -> list[dict[str, Any]]:
    """Mean of every metric per (policy, segment) with a bootstrap CI on the primary metric."""
    ec = cfg.evaluation
    rows = []
    for seg in SEGMENTS:
        d = _segment(per_user, seg)
        if d.empty:
            continue
        for p in ec.policies:
            row: dict[str, Any] = {"policy": p, "segment": seg, "users": len(d)}
            for k in ec.ks:
                for m in METRICS:
                    row[f"{m}_at_{k}"] = float(d[f"{p}__{m}_at_{k}"].mean())
            ci = stats.mean_ci(
                d[f"{p}__{ec.primary_metric}"].to_numpy(float),
                ec.bootstrap_samples,
                ec.seed,
                ec.confidence,
            )
            row[f"{ec.primary_metric}_ci"] = [ci.low, ci.high]
            rows.append(row)
    return rows


def run(spark: SparkSession, cfg: Config) -> StageReport:
    ec = cfg.evaluation
    gold = cfg.paths.resolved().gold
    with timed_stage("evaluation") as rep:
        recs = policy_recs(spark, cfg, ec.split).cache()
        labels = read_table(spark, gold / "labels").where(F.col("split") == ec.split)
        cutoff = labels.select("cutoff_date").distinct().collect()[0][0]
        per_user = per_user_table(recs, labels, ec.policies, ec.ks)
        items = read_table(spark, gold / "item_features").where(
            F.col("cutoff_date") == F.lit(cutoff)
        )
        beyond = beyond_accuracy(recs, items, 10)
        summary = policy_summary(per_user, cfg)
        comparisons = compare(per_user, cfg)
        recs.unpersist()
        result = {
            "split": ec.split,
            "cutoff_date": str(cutoff),
            "visitors_with_truth": len(per_user),
            "warm_visitors": int(per_user["warm"].sum()),
            "bootstrap_samples": ec.bootstrap_samples,
            "confidence": ec.confidence,
            "ab_salt": ec.ab_salt,
            "ab_treatment_share": float(
                stats.ab_arms(per_user["visitor_id"].tolist(), ec.ab_salt).mean()
            ),
            "summary": summary,
            "beyond_accuracy": beyond.reset_index().to_dict(orient="records"),
            "comparisons": comparisons,
        }
        out = gold / "_reports" / f"evaluation_{ec.split}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(result, indent=2, default=float) + "\n")
        figs_dir = _resolve(ec.figures_dir)
        paths = figures.render_all(result, cfg, figs_dir)
        report.write_markdown(result, cfg, paths, _resolve(ec.report_path))
        rep.rows = {"visitors": len(per_user)}
        rep.extra = {"figures": [str(p) for p in paths], "report": str(ec.report_path)}
    rep.write(gold / "_reports")
    return rep


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO_ROOT / path

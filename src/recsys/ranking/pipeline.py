"""Train the LambdaRank re-ranker and compare it with single sources and a blend.

Protocol: train on the train cutoffs, early-stop on the validation cutoff, and report
validation and the held-out test cutoff. Test is never used for any choice.
Validation and test are scored over *all* candidates of *all* label-window visitors (no
label-based filtering); only training queries are restricted to visitors with a positive.
"""

from __future__ import annotations

import json
import shutil
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from recsys.candidates.evaluate import evaluate
from recsys.config import Config
from recsys.features.split import Cutoff, cutoffs
from recsys.io import StageReport, read_table, timed_stage, write_table
from recsys.ranking import baselines, dataset, model

BLEND = ("recent_items", "popular_category", "popular_global")


def _examples(spark: SparkSession, cfg: Config, cut: Cutoff) -> DataFrame:
    gold = cfg.paths.resolved().gold
    t = {
        n: read_table(spark, gold / n)
        for n in (
            "candidates",
            "events_enriched",
            "user_features",
            "item_features",
            "user_category_affinity",
            "labels",
        )
    }
    return dataset.build_examples(
        cut,
        t["candidates"],
        t["events_enriched"],
        t["user_features"],
        t["item_features"],
        t["user_category_affinity"],
        t["labels"],
        cfg.candidates.sources,
        cfg.features.event_weights,
    )


def run(spark: SparkSession, cfg: Config) -> StageReport:
    gold = cfg.paths.resolved().gold
    rc = cfg.ranking
    cuts = cutoffs(cfg.split)
    with timed_stage("ranking") as report:
        # 1) training data: sampled queries from train cutoffs (fit) and val (early stopping)
        shutil.rmtree(gold / "ranking_train", ignore_errors=True)
        features: list[str] = []
        for cut in [c for c in cuts if c.split in ("train", "val")]:
            ex = _examples(spark, cfg, cut)
            features = features or dataset.feature_columns(ex)
            sampled = dataset.sample_training_queries(
                ex, rc.max_negatives_per_query, rc.sample_salt
            )
            # The partition directory carries cutoff_date; the files must not repeat it.
            write_table(
                sampled.drop("cutoff_date"),
                gold / "ranking_train" / f"cutoff_date={cut.cutoff_date}",
            )
        train_tbl = read_table(spark, gold / "ranking_train")
        cols = [*dataset.KEYS, dataset.LABEL, *features]
        train_pdf = train_tbl.where(F.col("split") == "train").select(*cols).toPandas()
        valid_pdf = train_tbl.where(F.col("split") == "val").select(*cols).toPandas()
        if train_pdf.empty or valid_pdf.empty:
            raise ValueError(
                f"ranking needs training queries with >= 1 positive candidate "
                f"(train rows={len(train_pdf)}, val rows={len(valid_pdf)})"
            )

        # 2) fit
        ranker = model.train(
            train_pdf,
            valid_pdf,
            features,
            rc.lgbm,
            rc.num_boost_round,
            rc.early_stopping_rounds,
            rc.eval_at,
        )
        del train_pdf, valid_pdf
        model_dir = gold / "models" / "ranker"
        model_dir.mkdir(parents=True, exist_ok=True)
        model_str = ranker.booster.model_to_string(num_iteration=ranker.best_iteration)
        (model_dir / "model.txt").write_text(model_str)
        importance = ranker.importance()
        meta = {
            "features": features,
            "best_iteration": ranker.best_iteration,
            "params": rc.lgbm,
            "stats": ranker.stats,
        }
        (model_dir / "meta.json").write_text(json.dumps(meta, indent=2) + "\n")

        # 3) score val + test over all candidates
        shutil.rmtree(gold / "ranked", ignore_errors=True)
        for cut in [c for c in cuts if c.split in ("val", "test")]:
            scored = model.score(
                _examples(spark, cfg, cut), model_str, features, ranker.best_iteration
            )
            write_table(
                model.top_n(scored, rc.top_n), gold / "ranked" / f"cutoff_date={cut.cutoff_date}"
            )

        report.rows = {
            "train_rows": ranker.stats["train_rows"],
            "valid_rows": ranker.stats["valid_rows"],
        }
        report.extra = {
            "model": ranker.stats,
            "n_features": len(features),
            "importance": importance,
        }
    report.write(gold / "_reports")
    evaluate_ranker(spark, cfg)
    return report


def evaluate_ranker(spark: SparkSession, cfg: Config) -> StageReport:
    """Ranker vs. single sources vs. priority blend on val and test (reads gold/ranked)."""
    gold = cfg.paths.resolved().gold
    rc = cfg.ranking
    with timed_stage("ranking_eval") as report:
        cands = read_table(spark, gold / "candidates").where(F.col("split").isin("val", "test"))
        ranked = read_table(spark, gold / "ranked").withColumn("source", F.lit("ranker"))
        blend = baselines.priority_blend(cands, BLEND, rc.top_n).withColumn(
            "source", F.lit("blend")
        )
        cols = ["cutoff_date", "split", "source", "visitor_id", "item_id", "score", "rank"]
        allrecs = (
            cands.select(*cols).unionByName(ranked.select(*cols)).unionByName(blend.select(*cols))
        )
        labels = read_table(spark, gold / "labels")
        metrics: list[dict[str, Any]] = []
        for split in ("val", "test"):
            metrics += [
                m for m in evaluate(allrecs, labels, split, rc.eval_ks) if m["source"] != "union"
            ]
        report.rows = {"metric_rows": len(metrics)}
    report.write(gold / "_reports")
    (gold / "_reports" / "ranking_metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
    return report

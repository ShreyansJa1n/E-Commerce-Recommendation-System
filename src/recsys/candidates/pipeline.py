"""Gold features -> ``gold/candidates`` (+ ``gold/item_neighbors``) and validation metrics."""

from __future__ import annotations

import json
import logging
import shutil

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from recsys.candidates import als, cooccurrence, history, popularity
from recsys.candidates.common import CANDIDATE_COLUMNS
from recsys.candidates.evaluate import evaluate
from recsys.config import Config
from recsys.features.split import Cutoff, cutoffs
from recsys.features.split import history as history_before
from recsys.io import StageReport, read_table, timed_stage, write_table

log = logging.getLogger(__name__)

SOURCES = ("popular_global", "popular_category", "recent_items", "cooccurrence", "als")


def generate(
    hist: DataFrame, targets: DataFrame, cut: Cutoff, cfg: Config
) -> tuple[dict[str, DataFrame], DataFrame]:
    """All candidate sources for one cutoff, plus the item-neighbor table."""
    cc = cfg.candidates
    w = cfg.features.event_weights
    n = cc.top_n
    neighbors = cooccurrence.item_neighbors(hist, cut, cc.cooccurrence).cache()
    seeds = cooccurrence.seed_items(hist, targets, cut, cc.cooccurrence, w)
    model = als.train(als.interactions(hist, cc.als, w), cc.als)
    sources = {
        "popular_global": popularity.global_popularity(
            hist, targets, cut, cc.popularity.window_days, w, n
        ),
        "popular_category": popularity.category_popularity(hist, targets, cut, cc.category, w, n),
        "recent_items": history.recent_items(hist, targets, cut, cc.history.half_life_days, w, n),
        "cooccurrence": cooccurrence.recommend(seeds, neighbors, cc.cooccurrence, n),
        "als": als.recommend(model, targets, n),
    }
    return sources, neighbors


def run(spark: SparkSession, cfg: Config) -> StageReport:
    paths = cfg.paths.resolved()
    gold = paths.gold
    cc = cfg.candidates
    with timed_stage("candidates") as report:
        events = read_table(spark, gold / "events_enriched")
        labels = read_table(spark, gold / "labels")
        for table in ("candidates", "item_neighbors"):
            shutil.rmtree(gold / table, ignore_errors=True)
        for cut in cutoffs(cfg.split):
            hist = history_before(events, cut).cache()
            targets = (
                labels.where(F.col("cutoff_date") == F.lit(cut.cutoff_date))
                .select("visitor_id")
                .distinct()
                .cache()
            )
            sources, neighbors = generate(hist, targets, cut, cfg)
            for name, df in sources.items():
                out = df.select(*CANDIDATE_COLUMNS).withColumn("split", F.lit(cut.split))
                path = gold / "candidates" / f"cutoff_date={cut.cutoff_date}" / f"source={name}"
                write_table(out, path)
                rows = read_table(spark, path).count()
                report.rows[f"{cut.cutoff_date}/{name}"] = rows
                if rows == 0:
                    log.warning("candidate source %s is empty at cutoff %s", name, cut.cutoff_date)
            write_table(neighbors, gold / "item_neighbors" / f"cutoff_date={cut.cutoff_date}")
            neighbors.unpersist()
            hist.unpersist()
            targets.unpersist()

        cands = read_table(spark, gold / "candidates")
        metrics = evaluate(cands, labels, "val", cc.eval_ks)
        report.extra["val_metrics"] = metrics
    report.write(gold / "_reports")
    (gold / "_reports" / "candidates_val_metrics.json").write_text(
        json.dumps(metrics, indent=2) + "\n"
    )
    return report

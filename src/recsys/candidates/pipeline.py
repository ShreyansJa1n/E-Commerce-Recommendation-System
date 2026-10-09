"""Gold features -> ``gold/candidates`` (+ ``gold/item_neighbors``) and validation metrics."""

from __future__ import annotations

import json
import logging
import shutil
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from recsys.candidates import als, cooccurrence, history, item2vec, popularity
from recsys.candidates.common import CANDIDATE_COLUMNS
from recsys.candidates.evaluate import evaluate, leave_one_out
from recsys.config import Config
from recsys.embeddings.item2vec import TrainedEmbeddings, from_frame
from recsys.features.split import Cutoff, cutoffs
from recsys.features.split import history as history_before
from recsys.io import StageReport, read_table, timed_stage, write_table

log = logging.getLogger(__name__)

SOURCES = (
    "popular_global",
    "popular_category",
    "recent_items",
    "cooccurrence",
    "als",
    "item2vec",
)


def generate(
    spark: SparkSession,
    hist: DataFrame,
    targets: DataFrame,
    cut: Cutoff,
    cfg: Config,
    embeddings: TrainedEmbeddings | None = None,
) -> tuple[dict[str, DataFrame], DataFrame | None]:
    """Requested candidate sources for one cutoff, plus the co-occurrence neighbor table
    (None unless ``cooccurrence`` is requested). Only requested sources are computed."""
    cc = cfg.candidates
    w = cfg.features.event_weights
    n = cc.top_n
    unknown = set(cc.sources) - set(SOURCES)
    if unknown:
        raise ValueError(f"unknown candidate sources: {sorted(unknown)}")
    neighbors = (
        cooccurrence.item_neighbors(hist, cut, cc.cooccurrence).cache()
        if "cooccurrence" in cc.sources
        else None
    )

    def _cooc() -> DataFrame:
        assert neighbors is not None
        seeds = cooccurrence.seed_items(hist, targets, cut, cc.cooccurrence, w)
        return cooccurrence.recommend(seeds, neighbors, cc.cooccurrence, n)

    def _item2vec() -> DataFrame:
        if embeddings is None:
            raise ValueError("item2vec requested but no embeddings for this cutoff")
        seed_cfg = cc.cooccurrence.model_copy(
            update={
                "seed_items": cc.item2vec.seed_items,
                "seed_half_life_days": cc.item2vec.seed_half_life_days,
            }
        )
        seeds = cooccurrence.seed_items(hist, targets, cut, seed_cfg, w)
        return item2vec.recommend(spark, seeds, embeddings, n, cc.item2vec.query_chunk)

    builders: dict[str, Callable[[], DataFrame]] = {
        "popular_global": lambda: popularity.global_popularity(
            hist, targets, cut, cc.popularity.window_days, w, n
        ),
        "popular_category": lambda: popularity.category_popularity(
            hist, targets, cut, cc.category, w, n
        ),
        "recent_items": lambda: history.recent_items(
            hist, targets, cut, cc.history.half_life_days, w, n
        ),
        "cooccurrence": _cooc,
        "als": lambda: als.recommend(
            als.train(als.interactions(hist, cc.als, w), cc.als), targets, n
        ),
        "item2vec": _item2vec,
    }
    return {name: builders[name]() for name in cc.sources}, neighbors


def run(spark: SparkSession, cfg: Config) -> StageReport:
    paths = cfg.paths.resolved()
    gold = paths.gold
    cc = cfg.candidates
    with timed_stage("candidates") as report:
        events = read_table(spark, gold / "events_enriched")
        labels = read_table(spark, gold / "labels")
        # Replace only the requested sources; other sources already on disk are kept.
        for cut in cutoffs(cfg.split):
            for name in cc.sources:
                shutil.rmtree(
                    gold / "candidates" / f"cutoff_date={cut.cutoff_date}" / f"source={name}",
                    ignore_errors=True,
                )
        if "cooccurrence" in cc.sources:
            shutil.rmtree(gold / "item_neighbors", ignore_errors=True)
        emb_path = gold / "item_embeddings"
        for cut in cutoffs(cfg.split):
            hist = history_before(events, cut).cache()
            targets = (
                labels.where(F.col("cutoff_date") == F.lit(cut.cutoff_date))
                .select("visitor_id")
                .distinct()
                .cache()
            )
            emb = (
                from_frame(read_table(spark, emb_path / f"cutoff_date={cut.cutoff_date}"))
                if "item2vec" in cc.sources
                else None
            )
            sources, neighbors = generate(spark, hist, targets, cut, cfg, emb)
            for name, df in sources.items():
                out = df.select(*CANDIDATE_COLUMNS).withColumn("split", F.lit(cut.split))
                path = gold / "candidates" / f"cutoff_date={cut.cutoff_date}" / f"source={name}"
                write_table(out, path)
                rows = read_table(spark, path).count()
                report.rows[f"{cut.cutoff_date}/{name}"] = rows
                if rows == 0:
                    log.warning("candidate source %s is empty at cutoff %s", name, cut.cutoff_date)
            if neighbors is not None:
                write_table(neighbors, gold / "item_neighbors" / f"cutoff_date={cut.cutoff_date}")
                neighbors.unpersist()
            hist.unpersist()
            targets.unpersist()

        cands = read_table(spark, gold / "candidates")
        metrics = evaluate(cands, labels, "val", cc.eval_ks)
        report.extra["val_metrics"] = metrics
        report.extra["val_ablation"] = leave_one_out(cands, labels, "val", cc.eval_ks)
    report.write(gold / "_reports")
    (gold / "_reports" / "candidates_val_metrics.json").write_text(
        json.dumps(metrics, indent=2) + "\n"
    )
    (gold / "_reports" / "candidates_val_ablation.json").write_text(
        json.dumps(report.extra["val_ablation"], indent=2) + "\n"
    )
    return report

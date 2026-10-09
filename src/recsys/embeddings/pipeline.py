"""Gold events -> ``gold/item_embeddings`` (item2vec per cutoff, history before T only)."""

from __future__ import annotations

import shutil

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from recsys.config import Config
from recsys.embeddings import item2vec
from recsys.embeddings.index import ExactIndex
from recsys.features.split import cutoffs, history
from recsys.io import StageReport, read_table, timed_stage, write_table


def neighbor_category_agreement(
    emb: item2vec.TrainedEmbeddings, item_category: dict[int, int], k: int, sample: int, seed: int
) -> float:
    """Share of an item's top-k exact neighbors that share its category (sanity metric)."""
    rng = np.random.default_rng(seed)
    with_cat = [n for n, i in enumerate(emb.item_ids) if int(i) in item_category]
    if not with_cat:
        return 0.0
    picks = rng.choice(with_cat, size=min(sample, len(with_cat)), replace=False)
    index = ExactIndex(emb.item_ids, emb.vectors)
    hits = index.search(emb.vectors[picks], k, [{int(emb.item_ids[p])} for p in picks])
    same = total = 0
    for p, hs in zip(picks, hits, strict=True):
        cat = item_category[int(emb.item_ids[p])]
        for h in hs:
            if h.item_id in item_category:
                total += 1
                same += item_category[h.item_id] == cat
    return same / total if total else 0.0


def run(spark: SparkSession, cfg: Config) -> StageReport:
    gold = cfg.paths.resolved().gold
    ec = cfg.embeddings
    with timed_stage("embeddings") as report:
        events = read_table(spark, gold / "events_enriched")
        items = read_table(spark, gold / "item_features")
        shutil.rmtree(gold / "item_embeddings", ignore_errors=True)
        per_cutoff = []
        for cut in cutoffs(cfg.split):
            sequences = item2vec.session_sequences(history(events, cut), ec.min_session_items)
            emb = item2vec.train(sequences, ec)
            write_table(
                item2vec.to_frame(spark, emb),
                gold / "item_embeddings" / f"cutoff_date={cut.cutoff_date}",
            )
            cats = {
                int(r.item_id): int(r.category_id)
                for r in items.where(
                    (F.col("cutoff_date") == F.lit(cut.cutoff_date))
                    & F.col("category_id").isNotNull()
                )
                .select("item_id", "category_id")
                .collect()
            }
            stats: dict[str, object] = dict(emb.stats)
            stats["cutoff_date"] = str(cut.cutoff_date)
            stats["neighbor_same_category@10"] = round(
                neighbor_category_agreement(emb, cats, 10, 2000, ec.seed), 4
            )
            per_cutoff.append(stats)
            report.rows[str(cut.cutoff_date)] = int(emb.stats["vocab"])
        report.extra["per_cutoff"] = per_cutoff
    report.write(gold / "_reports")
    return report

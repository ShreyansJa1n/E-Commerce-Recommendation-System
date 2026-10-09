"""Batch job: publish the serving snapshot to Redis (and its vectors to Qdrant).

Policy (docs/EXPERIMENT.md decision): visitors with history at the serving cutoff get the
ranker's list; everyone else (unknown or cold visitors) gets the global popularity list,
which is the same list `popular_global` serves (trailing ``candidates.popularity`` window).
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import redis
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from recsys.config import Config
from recsys.embeddings import vector_store
from recsys.embeddings.index import collection_name
from recsys.features.split import Cutoff, cutoffs
from recsys.io import StageReport, read_table, timed_stage
from recsys.serving.store import RecStore


def serving_cutoff(cfg: Config) -> Cutoff:
    (cut,) = [c for c in cutoffs(cfg.split) if c.split == cfg.serving.split]
    return cut


def model_id(cfg: Config) -> str:
    model = cfg.paths.resolved().gold / "models" / "ranker" / "model.txt"
    return hashlib.sha256(model.read_bytes()).hexdigest()[:12] if model.exists() else "none"


def popular_list(spark: SparkSession, cfg: Config, cut: Cutoff, n: int) -> list[tuple[int, float]]:
    w = cfg.candidates.popularity.window_days
    if w not in cfg.features.windows_days:
        raise ValueError(f"popularity window {w}d must be one of features.windows_days")
    items = read_table(spark, cfg.paths.resolved().gold / "item_features").where(
        (F.col("cutoff_date") == F.lit(cut.cutoff_date))
        & F.col(f"popularity_rank_{w}d").isNotNull()
    )
    rows = items.orderBy(f"popularity_rank_{w}d", "item_id").limit(n).collect()
    return [(int(r.item_id), 1.0 / float(r[f"popularity_rank_{w}d"])) for r in rows]


def run(
    spark: SparkSession, cfg: Config, client: redis.Redis | None = None, load_vectors: bool = True
) -> StageReport:
    sc = cfg.serving
    gold = cfg.paths.resolved().gold
    cut = serving_cutoff(cfg)
    with timed_stage("serving_load") as report:
        warm = (
            read_table(spark, gold / "user_features")
            .where(F.col("cutoff_date") == F.lit(cut.cutoff_date))
            .select("visitor_id")
        )
        ranked = (
            read_table(spark, gold / "ranked")
            .where(F.col("cutoff_date") == F.lit(cut.cutoff_date))
            .join(warm, "visitor_id", "left_semi")
            .where(F.col("rank") <= sc.top_n)
        )
        # sort_array on (rank, ...) structs restores rank order after collect_list.
        lists = ranked.groupBy("visitor_id").agg(
            F.sort_array(F.collect_list(F.struct("rank", "item_id", "score"))).alias("_recs")
        )
        user_recs = (
            (int(r.visitor_id), [(int(x.item_id), float(x.score)) for x in r._recs])
            for r in lists.toLocalIterator()
        )
        popular = popular_list(spark, cfg, cut, sc.top_n)
        collection = collection_name(cfg.vector_search.collection_prefix, str(cut.cutoff_date))
        version = f"{cut.cutoff_date}-{model_id(cfg)}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}"
        store = RecStore(client or redis.Redis.from_url(sc.redis_url), sc.key_prefix)
        n_users = store.write_version(
            version,
            user_recs,
            popular,
            {
                "cutoff_date": cut.cutoff_date,
                "model": model_id(cfg),
                "collection": collection,
                "top_n": sc.top_n,
                "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
                "policy": "ranker for visitors with history, global popularity otherwise",
            },
            sc.ttl_hours * 3600,
        )
        if load_vectors:
            vector_store.load(spark, cfg, sc.split)
        store.promote(version)
        report.rows = {"users": n_users, "popular": len(popular)}
        report.extra = {
            "version": version,
            "collection": collection,
            "previous": store.previous_version(),
        }
    report.write(gold / "_reports")
    return report

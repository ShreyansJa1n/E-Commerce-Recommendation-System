"""Load a cutoff's item embeddings (+ catalog payload) into Qdrant, and benchmark
Qdrant's approximate search against exact search: recall@k and client-side latency.
"""

from __future__ import annotations

import time
from typing import Any

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from qdrant_client import QdrantClient, models

from recsys.candidates.cooccurrence import seed_items
from recsys.candidates.item2vec import user_vectors
from recsys.config import Config
from recsys.embeddings.index import ExactIndex, Hit, QdrantIndex, Vector, collection_name
from recsys.embeddings.item2vec import TrainedEmbeddings, from_frame
from recsys.features.split import Cutoff, cutoffs, history
from recsys.io import StageReport, read_table, timed_stage


def _cutoff(cfg: Config) -> Cutoff:
    (cut,) = [c for c in cutoffs(cfg.split) if c.split == cfg.vector_search.benchmark.split]
    return cut


def _client(cfg: Config) -> QdrantClient:
    return QdrantClient(url=cfg.vector_search.qdrant_url, timeout=60)


def _payloads(
    spark: SparkSession, cfg: Config, cut: Cutoff, emb: TrainedEmbeddings
) -> list[dict[str, Any]]:
    gold = cfg.paths.resolved().gold
    feats = {
        int(r.item_id): r
        for r in read_table(spark, gold / "item_features")
        .where(F.col("cutoff_date") == F.lit(cut.cutoff_date))
        .select("item_id", "category_id", "root_category_id", "available")
        .collect()
    }
    out = []
    for item, count in zip(emb.item_ids, emb.counts, strict=True):
        r = feats.get(int(item))
        out.append(
            {
                "item_id": int(item),
                "category_id": None if r is None else r.category_id,
                "root_category_id": None if r is None else r.root_category_id,
                "available": None if r is None else r.available,
                "count": int(count),
            }
        )
    return out


def load(spark: SparkSession, cfg: Config) -> StageReport:
    vs = cfg.vector_search
    cut = _cutoff(cfg)
    gold = cfg.paths.resolved().gold
    name = collection_name(vs.collection_prefix, str(cut.cutoff_date))
    with timed_stage("vectors_load") as report:
        emb = from_frame(
            read_table(spark, gold / "item_embeddings" / f"cutoff_date={cut.cutoff_date}")
        )
        payloads = _payloads(spark, cfg, cut, emb)
        start = time.perf_counter()
        index = QdrantIndex.build(
            _client(cfg),
            name,
            emb.item_ids,
            emb.vectors,
            payloads,
            vs.hnsw_m,
            vs.hnsw_ef_construct,
            vs.search_ef,
            vs.upload_batch_size,
            vs.hnsw_full_scan_threshold_kb,
        )
        upload_s = round(time.perf_counter() - start, 2)
        indexed = index.wait_until_indexed()
        report.rows = {"points": indexed["points"]}
        report.extra = {
            "collection": name,
            "cutoff_date": str(cut.cutoff_date),
            "upload_seconds": upload_s,
            "index_wait_seconds": indexed["seconds"],
            "hnsw_m": vs.hnsw_m,
            "hnsw_ef_construct": vs.hnsw_ef_construct,
            "hnsw_full_scan_threshold_kb": vs.hnsw_full_scan_threshold_kb,
            "dim": int(emb.vectors.shape[1]),
        }
    report.write(gold / "_reports")
    return report


def _recall(ann: list[list[Hit]], exact: list[list[Hit]], k: int) -> float:
    per_query = [
        len({h.item_id for h in a[:k]} & {h.item_id for h in e[:k]}) / max(1, min(k, len(e)))
        for a, e in zip(ann, exact, strict=True)
    ]
    return float(np.mean(per_query)) if per_query else 0.0


def _latency_ms(fn: Any, queries: Vector, excl: list[set[int]]) -> dict[str, float]:
    times = []
    for q, ex in zip(queries, excl, strict=True):
        start = time.perf_counter()
        fn(q, ex)
        times.append((time.perf_counter() - start) * 1000)
    arr = np.array(times)
    return {
        "p50_ms": round(float(np.percentile(arr, 50)), 3),
        "p95_ms": round(float(np.percentile(arr, 95)), 3),
        "p99_ms": round(float(np.percentile(arr, 99)), 3),
        "mean_ms": round(float(arr.mean()), 3),
        "sequential_qps": round(1000 / float(arr.mean()), 1),
    }


def _scenario(
    name: str,
    queries: Vector,
    excl: list[set[int]],
    exact: ExactIndex,
    ann: QdrantIndex,
    k: int,
    batch_size: int,
    qfilter: models.Filter | None = None,
    exact_hits: list[list[Hit]] | None = None,
) -> dict[str, Any]:
    exact_hits = exact_hits if exact_hits is not None else exact.search(queries, k, excl)
    start = time.perf_counter()
    ann_hits = ann.search(queries, k, excl, qfilter, batch_size)
    batch_s = time.perf_counter() - start
    lat = _latency_ms(lambda q, ex: ann.search_one(q, k, ex, qfilter), queries, excl)
    start = time.perf_counter()
    exact.search(queries, k, excl)
    exact_s = time.perf_counter() - start
    return {
        "scenario": name,
        "queries": len(queries),
        "k": k,
        f"recall@{k}_vs_exact": round(_recall(ann_hits, exact_hits, k), 4),
        **lat,
        "batch_qps": round(len(queries) / batch_s, 1),
        "exact_numpy_qps": round(len(queries) / exact_s, 1),
    }


def benchmark(spark: SparkSession, cfg: Config) -> StageReport:
    vs = cfg.vector_search
    bc = vs.benchmark
    cut = _cutoff(cfg)
    gold = cfg.paths.resolved().gold
    name = collection_name(vs.collection_prefix, str(cut.cutoff_date))
    rng = np.random.default_rng(bc.seed)
    with timed_stage("vectors_benchmark") as report:
        emb = from_frame(
            read_table(spark, gold / "item_embeddings" / f"cutoff_date={cut.cutoff_date}")
        )
        client = _client(cfg)
        info = client.get_collection(name)
        if (info.points_count or 0) != len(emb.item_ids):
            raise RuntimeError(
                f"{name} has {info.points_count} points, expected {len(emb.item_ids)}:"
                " run make vectors-load"
            )
        ann = QdrantIndex(client, name, vs.search_ef)
        exact = ExactIndex(emb.item_ids, emb.vectors)
        k = bc.k
        scenarios = []

        # 1) similar items: query by an item's own vector, excluding itself
        picks = rng.choice(
            len(emb.item_ids), size=min(bc.n_queries, len(emb.item_ids)), replace=False
        )
        item_q = emb.vectors[picks]
        item_ex = [{int(emb.item_ids[p])} for p in picks]
        scenarios.append(_scenario("similar_items", item_q, item_ex, exact, ann, k, bc.batch_size))

        # 2) similar items, available items only (payload filter). Exact = filter, then search.
        payload_avail = {
            int(r.item_id)
            for r in read_table(spark, gold / "item_features")
            .where((F.col("cutoff_date") == F.lit(cut.cutoff_date)) & (F.col("available") == 1))
            .select("item_id")
            .collect()
        }
        mask = np.array([int(i) in payload_avail for i in emb.item_ids])
        exact_avail = ExactIndex(emb.item_ids[mask], emb.vectors[mask])
        avail_filter = models.Filter(
            must=[models.FieldCondition(key="available", match=models.MatchValue(value=1))]
        )
        scenarios.append(
            _scenario(
                "similar_items_available_only",
                item_q,
                item_ex,
                exact_avail,
                ann,
                k,
                bc.batch_size,
                avail_filter,
            )
        )

        # 3) user vectors: decayed mean of warm validation visitors' recent items
        events = read_table(spark, gold / "events_enriched")
        labels = read_table(spark, gold / "labels").where(
            (F.col("cutoff_date") == F.lit(cut.cutoff_date)) & F.col("visitor_has_history")
        )
        targets = labels.select("visitor_id").distinct()
        cc = cfg.candidates
        seed_cfg = cc.cooccurrence.model_copy(
            update={
                "seed_items": cc.item2vec.seed_items,
                "seed_half_life_days": cc.item2vec.seed_half_life_days,
            }
        )
        seeds = seed_items(history(events, cut), targets, cut, seed_cfg, cfg.features.event_weights)
        visitors, user_q, user_ex = user_vectors(seeds, emb)
        sel = rng.choice(len(visitors), size=min(bc.n_queries, len(visitors)), replace=False)
        scenarios.append(
            _scenario(
                "user_vector", user_q[sel], [user_ex[i] for i in sel], exact, ann, k, bc.batch_size
            )
        )

        # hnsw_ef trade-off: recall vs exact and single-query latency per ef value.
        user_sel_q, user_sel_ex = user_q[sel], [user_ex[i] for i in sel]
        exact_item = exact.search(item_q, k, item_ex)
        exact_user = exact.search(user_sel_q, k, user_sel_ex)
        ef_rows = []
        for ef in bc.ef_sweep:
            idx = QdrantIndex(client, name, ef)
            for scen, q, ex, truth in (
                ("similar_items", item_q, item_ex, exact_item),
                ("user_vector", user_sel_q, user_sel_ex, exact_user),
            ):
                hits = idx.search(q, k, ex, None, bc.batch_size)
                lat = _latency_ms(lambda v, e, i=idx: i.search_one(v, k, e), q, ex)
                ef_rows.append(
                    {
                        "scenario": scen,
                        "hnsw_ef": ef,
                        f"recall@{k}_vs_exact": round(_recall(hits, truth, k), 4),
                        "p50_ms": lat["p50_ms"],
                        "p99_ms": lat["p99_ms"],
                    }
                )
        report.rows = {"collection_points": int(info.points_count or 0)}
        report.extra = {
            "collection": name,
            "hnsw_m": vs.hnsw_m,
            "hnsw_ef_construct": vs.hnsw_ef_construct,
            "hnsw_full_scan_threshold_kb": vs.hnsw_full_scan_threshold_kb,
            "search_ef": vs.search_ef,
            "latency_note": (
                "client-side wall time per single query, HTTP to localhost (Docker/OrbStack)"
            ),
            "scenarios": scenarios,
            "ef_sweep": ef_rows,
        }
    report.write(gold / "_reports")
    return report

# Design notes

A short design doc for the main decisions. Every number comes from [RESULTS.md](RESULTS.md), and the decision records are in [DECISIONS.md](DECISIONS.md).

## 1. The problem the data actually poses
Retailrocket has 2.76M events from 1.4M visitors over 4.5 months. **71% of visitors have exactly one event**, and in every two-week evaluation window **about 90% of visitors have no history before the cutoff.** Product attributes are hashed except category and availability. Three things follow, and they shaped everything else:
- Personalization can only help the ~10% of returning visitors. The cold 90% need popularity or session context.
- Text or content embeddings aren't possible, so item similarity has to come from behavior (sessions, co-occurrence, ALS).
- A model's headline metric is dominated by how it treats cold visitors. Every result is therefore reported per segment (all / returning / new).

## 2. ALS vs. embeddings vs. heuristics
| | ALS (implicit, MLlib) | item2vec (session skip-gram) | Session co-occurrence | Recent items / category popularity |
|---|---|---|---|---|
| Signal | user × item matrix | item order within sessions | items in the same session | the user's own history |
| Cold users | none (needs ≥ 2 items) | needs ≥ 1 embedded seed | needs ≥ 1 seed | needs history |
| Returning-visitor R@100 (val) | 0.102 | 0.043 | 0.032 | 0.194 / 0.188 |
| Unique contribution (union R@100 drop when removed) | −0.008 | **−0.011** | −0.003 | −0.050 / −0.024 |
| Cost | dominant: rank 128, ~2.5 min per cutoff | 13–22 s per cutoff (driver) | cheap | cheap |

- **ALS** is the best learned *standalone* source, and it is strongest for carts and purchases (R@100 0.203 at relevance ≥ 2). It needs a large rank: the best recall@100 rose from 0.062 to 0.102 as rank went 32 → 128, and the best setting was still at the edge of the grid.
- **item2vec** is weak alone but adds the most *unique* recall after the two heuristics, because it covers 76% of returning visitors (ALS covers 42%) and finds different items. Its neighbors agree on category 62% of the time, vs 79% for co-occurrence neighbors, but there are many more of them.
- **The heuristics win on raw recall.** E-commerce users come back to items they've already seen, and 20% of returning visitors' future interactions are repeats.
- **Takeaway:** use them together. The sources are complementary, which is what the ranker and the union recall ceiling (0.307 at R@100) rely on.

## 3. Ranking: what the model learned
The LambdaRank model beats popularity 4.5× and ALS 6× on test NDCG@10, but it *ties* a hand-written blend (recent items → category popularity → global popularity). The paired bootstrap shows why:
- **Returning visitors:** +2.3% [+1.8%, +2.7%] for the ranker.
- **New visitors:** −9.1% [−11.5%, −6.6%]. For cold visitors it can only reorder popularity by item features, and that didn't transfer from August to September.

The top features are recent-items rank and score. The model mostly learned the blend, plus some smarter ordering for returning visitors. Hence the **hybrid serving policy**: ranker for visitors with history, popularity for everyone else ([EXPERIMENT.md](EXPERIMENT.md)).

## 4. Batch vs. online
**Chosen:** daily batch scoring with precomputed top-50 lists in Redis. Similar-items is online (Qdrant).
- **Why batch:** the ranker needs about 65 features computed from Spark tables (windows, as-of joins, per-source ranks). Computing them per request would mean a feature store and re-implementing every aggregation. Batch scoring keeps the request path at 1–2 Redis GETs (p99 11 ms at ~250 req/s, 2,839 req/s at saturation on a laptop).
- **What batch costs:** lists are up to a day stale, and a visitor's in-session behavior doesn't affect their recommendations until the next run. For a dataset where 90% of visitors are new, *in-session* recommendation is the biggest missing piece. The natural next step is a streaming job that updates session features (the Phase 10 stretch, not built), feeding a light online re-ranker on top of the batch candidates.
- **Why `/similar` is online:** it's a pure vector lookup with an optional payload filter (in stock only). Qdrant HNSW gives 0.996 recall@10 vs exact search at p95 4.8 ms. There's nothing to precompute that would be cheaper.

## 5. Offline/online parity
- **One code path for features.** Training examples, validation/test scoring and the serving snapshot are all produced by the same Spark functions at a cutoff T (`recsys.features`, `recsys.ranking.dataset`). Serving stores the outputs, so there is no second implementation that could drift.
- **Point-in-time correctness is tested.** `tests/test_leakage.py` injects future events and catalog changes and requires every feature row to stay identical. The test is proven to catch a one-day lookahead, and it found a real bug (catalog versions visible exactly at T).
- **The schema contract is enforced.** `recsys.features.contract` fails the pipeline on any column or type drift, and the documentation is generated from it.
- **The serving policy matches what was evaluated.** The loader writes exactly the ranker lists scored in Phase 5 for visitors with history, and `popular_global`'s list otherwise. Both are asserted in `tests/test_pipeline.py::test_serving_loader_publishes_policy`.
- **Versioned snapshots** (cutoff + model hash + timestamp) with an atomic pointer flip and rollback, so a served list can always be traced to the model and data that produced it.

## 6. Running it at scale
The code is plain PySpark plus Parquet paths, so moving off the laptop is configuration, not a rewrite:
- **Storage:** point `paths.*` at `s3://bucket/{bronze,silver,gold}` (S3A connector). Date-partitioned tables cut bytes read by 99% for windowed queries (Phase 9). That matters once tables exceed a node's page cache.
- **Compute (EMR or Kubernetes):** run each `python -m recsys.cli <stage> --env prod` as a step. `configs/prod.yaml` raises `spark.shuffle_partitions` (AQE coalesces overshoot cheaply) and `driver_memory`, and sets `master` via `spark-submit`.
  - Two driver-bound pieces need changes first: item2vec trains on the driver (switch to MLlib Word2Vec or sharded gensim), and LightGBM trains on pandas (use SynapseML/XGBoost-Spark, or keep the sampling).
  - Salting (`cooccurrence.salt_buckets`) becomes worthwhile once a hot key's partition is far above AQE's skew threshold. At local scale it cost 10%.
- **Scheduling:** an orchestrator (Airflow, Step Functions) runs the daily DAG `bronze → silver → gold → embeddings → candidates → ranking → eval → serve-load`. Gate `serve-load` on the evaluation guardrails (e.g. block if NDCG@10 drops beyond the previous snapshot's CI). Stage metrics already go to Pushgateway (Phase 8).
- **Serving:** the API image (629 MB, no Spark) runs as a Kubernetes Deployment with a readiness probe on `/health` `status == "ok"`. Redis would be a managed instance (ElastiCache) and Qdrant its own StatefulSet or Qdrant Cloud. The Prometheus rules and Grafana dashboard JSON carry over unchanged.
- **Candidate coverage:** today, candidates and ranker lists are generated for visitors in each evaluation window (14,839 returning visitors at the serving cutoff). A production run generates them for every recently active visitor, which is the same code with a different target set.

## 7. What I'd do next
1. **In-session recommendations for new visitors** (90% of traffic): streaming session features, plus item-to-item from item2vec and co-occurrence seeded by the current session.
2. **Re-evaluate the hybrid policy on a later period.** It came from the test split's result and hasn't been evaluated on unseen data.
3. **A LightGBM sweep and more training cutoffs** (validation only). Early stopping at iteration 16 suggests the model is data-limited.
4. **Online evaluation design:** a 50/50 A/B needs ~17× the traffic to detect the +2.3% lift. Use interleaving, or CUPED with pre-period NDCG as the covariate.

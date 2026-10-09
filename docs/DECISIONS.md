# Architecture Decision Records

Short ADRs: what, why, trade-offs.

## ADR-001: PySpark 4.0 on Java 21, Python 3.12
- **What:** Use `pyspark==4.0.*` instead of 3.5. Python is pinned to 3.12 via uv.
- **Why:** The dev machine has Java 21, which Spark 4.0 supports officially (3.5 only targets 8/11/17). 3.12 has the widest wheel coverage for lightgbm, gensim, and sentence-transformers.
- **Trade-offs:** ANSI SQL mode is on by default in 4.0, so invalid casts raise instead of returning null. Ingest and validation code must use `try_cast` explicitly. Some ecosystem libraries lag behind 4.0.

## ADR-002: Partitioned Parquet + custom validation module
- **What:** Store bronze/silver/gold as partitioned Parquet. Data-quality checks are a small Deequ-style module in `recsys.clean` instead of Great Expectations.
- **Why:** Fewer moving parts (no delta-spark jars, no GX Spark API churn). Checks stay plain, typed Python that is easy to test.
- **Trade-offs:** No ACID/time-travel. Idempotency comes from dynamic partition overwrite. Delta can be added later behind the same writer interface.

## ADR-003: Synthetic data for CI
- **What:** CI and unit tests use a synthetic generator (`tests/fixtures/synth.py`, Phase 1) that matches Retailrocket schemas. `make sample` subsamples the real data locally.
- **Why:** Real data needs Kaggle credentials and is CC BY-NC-SA, so it shouldn't be committed.
- **Trade-offs:** Synthetic distributions don't match the real ones. Metric numbers in RESULTS.md come only from real-data runs.

## ADR-004: Qdrant for vector search, locust for load testing
- **What:** Qdrant (not pgvector). Locust (not k6).
- **Why:** Qdrant runs as one container with payload filtering. Locust is Python-native, like the rest of the stack.
- **Trade-offs:** Qdrant adds a service to docker compose. Locust has lower max throughput than k6, which is fine at this scale.

## ADR-005: Versioned item properties with first-version backfill
- **What:** Silver keeps `item_properties_scd`: one row per (item, property, value) version with `[valid_from, valid_to)`. Consecutive identical weekly snapshots are collapsed. Features must join with `recsys.clean.catalog.property_as_of`. `catalog_latest` is for serving and reporting only.
- **Why:** 23,352 items change `categoryid` during the dataset, so a latest-value catalog would leak future category values into training features.
- **Backfill:** The first property snapshot (2015-05-10 03:00 UTC) comes a week after the first event, and 137,179 events (5%) happen before it. With `clean.backfill_first_property_version: true` (the default), each (item, property)'s first version is valid from 1970-01-01, which assumes the first observed value already held before the first snapshot. `first_seen_ts` and `is_backfilled` keep the real observation time. Set the flag to false for a strict no-assumption join (those events then get null properties).
- **Trade-offs:** A small, documented lookahead for items whose value changed before it was first observed.
- **Measured in Phase 2 (full data):** backfill raises the share of events with a point-in-time category from 76.2% to 90.7%. That's more than the 5% of events before the first snapshot, because many items' first recorded `categoryid` comes weeks after their events. 401,291 events rely on a backfilled value. For 390,483 of them, it's the only value the item ever had. The remaining 10,808 (0.39% of events) are on items whose category changed later, so their category could be wrong. Decision: keep backfill on.

## ADR-006: Bronze keeps every row; silver quarantines rejects
- **What:** Raw CSVs are read with string schemas and `try_cast` into bronze, so unparseable values become nulls instead of failing the job (Spark 4 ANSI mode). Header names are checked against the schema (`enforceSchema=false`). Silver tags each invalid row with its first failing rule and writes it to `silver/_rejected/events`. Exact duplicates on (ts_ms, visitor_id, event_type, item_id) are dropped and counted.
- **Why:** Bronze → silver row counts reconcile exactly (silver + rejected + duplicates = bronze), and bad input is visible instead of silently dropped.
- **Trade-offs:** Bronze stores some rows that are never used. Validation checks fail the stage on `error` severity (including an empty table) and only report on `warn`.

## ADR-007: Cutoff snapshots for point-in-time features
- **What:** Every gold table is keyed by a cutoff T: two weekly training cutoffs, then validation and test. Features at T come only from `history(events, T)` (`event_ts < T`). Item catalog features read the catalog just before T. Labels come from `[T, label_end)`. Event categories are resolved at each event's own timestamp before any aggregation.
- **Why:** This is the simplest scheme that is provably leakage-free. `tests/test_leakage.py` adds events at or after T (including exactly T, inside sessions that started before T, and for new visitors/items) and catalog changes at or after T, then requires every feature row to stay identical. A second test confirms it catches a one-day lookahead. Several training cutoffs give the Phase 5 ranker more examples.
- **Trade-offs:** Features are daily-granularity snapshots, not per-event, so a request at 3 pm gets midnight features. Serving (Phase 7) will compute the same snapshot features, keeping offline and online in parity. Per-event features would need a streaming path (Phase 10 stretch).
- **Schema contract:** `recsys.features.contract` is the single source of truth. The pipeline fails on any column or type drift, and `docs/feature_contract.md` is generated (`make contract`). A test fails if the doc is stale.

## ADR-008: Candidate generation design
- **What:** Five sources per cutoff, each trained or computed only on `history(events, T)` and each keeping its top 100 per visitor in `gold/candidates`:
  - `popular_global`: weighted events, last 7 days
  - `popular_category`: the visitor's top 3 decayed categories × each item's share of category popularity
  - `recent_items`: the visitor's own items, decayed
  - `cooccurrence`: session cosine similarity, top 50 neighbors per item, seeded by the visitor's top 20 recent items, seeds excluded
  - `als`: MLlib implicit ALS on summed event weights
- **Targets:** visitors in each cutoff's label window. Offline evaluation needs only those visitors, and it keeps the table small. Knowing *who* shows up isn't item-level leakage. The Phase 7 batch job will generate for all active visitors instead.
- **Added `recent_items` (not in the original plan):** for returning visitors, about 20% of label pairs are items they had already interacted with (Phase 2). Re-surfacing them is the cheapest strong signal, and the ranker can weigh it against discovery. ALS also doesn't filter seen items, for the same reason.
- **Tuning discipline:** the ALS sweep and any later candidate tuning use the validation cutoff only. The test split is first scored in Phase 5/6.
- **Skew:** session pairs are bounded by `max_session_items` (pairs grow with the square of session size). The seed × neighbor join is bounded by `neighbors_per_item`, with AQE skew-join handling on. Deterministic salting (`cooccurrence.salt_buckets`) is implemented and tested equal to the plain join. Its benefit is measured in Phase 9 rather than assumed.
- **ALS tuning:** rank 128, reg 0.01, alpha 40, chosen over two validation sweep rounds. The best is still on the grid edge, and larger ranks are deferred to Phase 9 for compute and disk reasons (RESULTS.md).
- **Trade-offs:** about 90% of target visitors are cold and only `popular_global` covers them. That cold segment needs session-context features or in-session recommendations, which this offline, cutoff-based setup doesn't model.

## ADR-009: item2vec with exact offline search and Qdrant for serving
- **What:** Item embeddings come from gensim skip-gram with negative sampling over session sequences (time-ordered, consecutive repeats collapsed), trained per cutoff on history before T. The offline candidate source scores with exact NumPy search (`ExactIndex`). Qdrant (`QdrantIndex`, same interface) holds the vectors plus catalog payload (category, availability) for serving `/similar` and user-vector queries. `make vectors-bench` measures Qdrant against exact search.
- **Why gensim over MLlib Word2Vec:** it offers negative sampling, subsampling, and `ns_exponent` control, and it trains in 13–22 s per cutoff on the driver (sessions fit in memory). Single-threaded training with a CRC32 `hashfxn` makes reruns bit-identical, which keeps the stage idempotent (tested).
- **Why exact offline:** results are deterministic, the pipeline doesn't depend on a running service, and brute force over about 50k × 64 vectors is fast (about 2,200 queries/s in NumPy). Serving needs filters, single-query latency, and updates, which is where Qdrant fits.
- **Qdrant full-scan threshold:** at this size, Qdrant's default planner brute-forces every segment, so a benchmark would silently measure exact search. `hnsw_full_scan_threshold_kb = 10` forces HNSW, and a test guards against a regression (RESULTS.md, Phase 4).
- **Known warning:** gensim 4.4.0 with numpy 2.5 / scipy 1.18 on macOS arm64 prints `Exception ignored in: 'gensim.models.word2vec_inner.our_dot_float'` 0–2 times per training run, without a traceback. It reproduces without Spark. Training stays deterministic, separates synthetic clusters, and gives 46–51% neighbor category agreement on real data, so at most a handful of dot products out of millions are affected. Revisit if gensim publishes a fix.
- **Trade-offs:** one embedding per item (no side information), so cold items without sessions have no vector. Text embeddings aren't possible because catalog values are hashed (ADR-005 context).

## ADR-010: LambdaRank re-ranker over merged candidates
- **What:** Merge every candidate source per (cutoff, visitor, item) into one deduped row with per-source rank/score features, and join point-in-time user, item, user × item, and user × category features (`recsys.ranking.dataset`). Train a LightGBM LambdaRank model on the train cutoffs, early-stopping on validation. Score validation and test in Spark with `mapInPandas` and keep the top 100.
- **Training sample:** only queries with ≥ 1 positive candidate (others contribute no LambdaRank gradient), all positives, ≤ 100 hash-sampled negatives per query. This brings about 2M candidate rows per cutoff down to about 1M, with the same queries.
- **Evaluation:** never filtered by labels. Every candidate of every label visitor is scored, and test is evaluated once with untuned parameters. A priority blend (recent items → category popularity → global popularity) is reported next to single sources as the strongest non-learned baseline.
- **Identifiers excluded:** raw category ids aren't used as numeric features. The category signal enters through affinity and "item in user's top category" features instead.
- **Trade-offs:** LightGBM trains on the driver (pandas). That's fine at about 2M rows, but more cutoffs or features would need distributed training (SynapseML/XGBoost-Spark) or more sampling. Spark scoring keeps evaluation scalable.

## ADR-011: Paired bootstrap + simulated A/B instead of replay
- **What:** Policies are compared on the same visitors with a paired bootstrap (resample visitors, recompute both means), and with a simulated 50/50 A/B (hash-assigned arms, each scored only with its own policy, independent bootstrap). Intervals are percentile intervals, and the decision rule is that the paired 95% CI of the absolute difference excludes 0.
- **Why not replay or IPS:** Retailrocket has no logged recommendations or propensities, so no unbiased counterfactual estimator exists. The offline metric is "agreement with what the visitor later did", which ignores the effect a list has on behavior. The simulated A/B makes the cost of online noise concrete: the +2.3% returning-visitor lift that the paired design detects needs about 17× the traffic to show up in a 50/50 test.
- **Absolute differences in plots:** relative lift explodes when a baseline is near zero (cold visitors, ALS), so figures show Δ NDCG@10 and print the relative lift as a label. Relative lift is reported as n/a when the control's mean is 0.
- **Trade-offs:** the per-visitor bootstrap treats visitors as independent, which ignores item-level correlation. That's acceptable at 142k visitors.

## ADR-012: Precomputed recommendations in Redis, versioned snapshots, hybrid policy
- **What:** A batch job (`make serve-load`) publishes the latest cutoff's ranker lists (top 50) for visitors with history, plus a global popularity list, to Redis as an immutable version with a TTL, then flips an atomic `current` pointer. FastAPI reads precomputed lists only, with no model inference on the request path. `/similar` queries Qdrant directly.
- **Why precompute:** the ranker needs about 65 features from Spark-built tables. Batch scoring keeps online and offline identical (no training/serving skew) and the request path down to one or two Redis GETs (p99 11 ms at steady load).
- **Policy:** ranker for visitors with history, popularity for everyone else, following the Phase 6 experiment (the ranker is significantly worse than popularity for cold visitors). The hybrid itself hasn't been evaluated on a later period yet.
- **Degraded mode:** the API keeps an in-process copy of the last popularity list and serves it (`popular_degraded`) if Redis is down. If Qdrant is down, `/similar` returns 503 rather than guessing.
- **Scope limit:** only visitors who appear in the test label window have ranker lists (14,839), because Phase 3–5 generated candidates for label-window visitors (ADR-008). A production job would run candidate generation and scoring for every recently active visitor at the snapshot cutoff. Unknown visitors correctly fall back to popularity either way.
- **Image:** the API image installs only the `serving` dependency group (no Spark or JVM) and runs as a non-root user with a healthcheck.

## ADR-013: Observability: Prometheus metrics, Pushgateway for batch, JSON logs
- **API:** `prometheus_client` in multiprocess mode, because 4 uvicorn workers each have a registry. `/metrics` aggregates their shared files, and the container clears the directory on start.
  - Latency histogram by route template, method and status (templates keep label cardinality bounded).
  - Recommendations by `source` (personalized share = cache hit rate; fallback and degraded rates).
  - Dependency errors and up gauges, plus snapshot age, users and version.
- **Batch pipeline:** stages can't be scraped, so `timed_stage` pushes duration, row counts and last-success time to a Pushgateway on success. Validation reports push failed error/warn check counts. Pushing is opt-in (`RECSYS_PUSHGATEWAY`) and never fails a stage.
- **Logs:** stdlib JSON formatter with a contextvar request id taken from `X-Request-ID` or generated, and one line per request with route, status and duration.
- **Alerts:** Prometheus rules for Redis/Qdrant down, degraded serving, fallback share > 90%, stale snapshot or pipeline (36 h), p99 > 100 ms, 5xx > 1%, and validation failures. No Alertmanager locally; each alert maps to a RUNBOOK section.
- **Resilience, from a failure drill:** per-worker startup warm-up of the popularity fallback, and a 5 s Redis circuit breaker (RESULTS.md, Phase 8).
- **Trade-offs:** Pushgateway keeps the *last* value per stage, so it has no history of failed runs beyond the missing "last success". A real deployment would also use an exporter for Redis, and a scheduler (Airflow etc.) for run history.

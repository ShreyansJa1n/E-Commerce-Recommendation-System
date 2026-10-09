# PLAN.md: Spark Recommendation Platform (Claude Code build plan)

## Goal
Build an end-to-end product recommendation system on Apache Spark: raw events and catalog data in, features, candidates, embeddings, ranking, offline evaluation, and a low-latency serving API out. Mirror a production recommendations platform (batch pipelines, vector search, observability, experimentation).

## Working agreements for Claude Code
- Work one phase at a time. At the end of each phase: run tests, update README, commit, then stop and summarize before starting the next.
- Write tests alongside code (pytest, pyspark local session). No phase is done without passing tests.
- Prefer small, typed, documented modules. Config lives in `configs/*.yaml`, never hardcoded.
- Every pipeline stage must be idempotent and runnable via `make <target>`.
- Record real measured numbers (runtime, metrics, latency) in `docs/RESULTS.md`. Never invent numbers.
- Keep a running `docs/DECISIONS.md` (short ADRs: what, why, trade-offs).

## Stack
- Python 3.12, PySpark 4.0 (Java 17/21), partitioned Parquet, pytest (ADR-001, ADR-002)
- Spark MLlib (ALS), LightGBM for ranking
- gensim/MLlib Word2Vec (item2vec), Qdrant (vector search) (ADR-004)
- Redis (precomputed recs), FastAPI (serving)
- Custom Deequ-style checks (data quality), Prometheus + Grafana (observability)
- Docker + docker compose, GitHub Actions, optional Kubernetes (kind)

## Dataset
Retailrocket e-commerce dataset (Kaggle): `events.csv` (view / addtocart / transaction), `item_properties_part*.csv`, `category_tree.csv`.
Alternative: H&M Personalized Fashion Recommendations.
Put raw data in `data/raw/` (gitignored). `make sample` builds a small local sample from the real data. CI uses a synthetic generator with the same schemas (`tests/fixtures/synth.py`) because the real data needs Kaggle credentials and is CC BY-NC-SA (ADR-003).

Dataset caveats:
- Item property values are hashed except `categoryid` and `available`, so meaningful text embeddings aren't possible.
- Most visitors have only 1–2 events. Use a config-driven min-interactions filter for ALS and ranker training, and report cold-start users separately.

## Repo layout
```
recsys-spark/
  configs/            # yaml configs
  data/               # raw/, bronze/, silver/, gold/ (gitignored)
  src/recsys/
    ingest/           # raw -> bronze
    clean/            # bronze -> silver, validation
    features/         # user/item/time-window features
    candidates/       # ALS, co-occurrence, vector retrieval
    embeddings/       # item2vec / text embeddings
    ranking/          # LightGBM ranker
    eval/             # offline metrics, replay evaluation
    serving/          # FastAPI, Redis loaders
    observability/    # metrics, logging
  tests/
  docs/               # DECISIONS.md, RESULTS.md, architecture diagram
  docker/, .github/workflows/
  Makefile, README.md
```

## Phase 0: Scaffold (30 min)
- Create repo layout, `pyproject.toml`, Makefile, pre-commit (ruff, mypy), GitHub Actions running lint + tests.
- Local Spark session helper with sane defaults (`spark.sql.shuffle.partitions`, AQE on).
- **Done when:** `make test` and CI pass on an empty smoke test.

## Phase 1: Ingest and clean (bronze to silver)
- Load events and item properties with explicit schemas. Convert epoch ms to timestamps.
- Dedup events, drop invalid rows. Keep item properties as a versioned table (`item_properties_scd`: valid_from/valid_to) so features can join as-of T, plus a `catalog_latest` view (latest value per property) for serving only.
- Spark 4.0 runs in ANSI mode: use `try_cast` for untrusted input.
- Write partitioned Parquet/Delta (partition by event date).
- Add Deequ-style checks (custom module): non-null keys, valid event types, timestamp ranges, uniqueness.
- **Done when:** silver tables exist, validation report is generated, tests cover dedup and schema enforcement.

## Phase 2: Feature engineering (silver to gold)
- User features: recency, frequency, category affinity, view-to-cart and cart-to-purchase rates, 7/30-day windows.
- Item features: popularity (windowed), conversion rate, co-view/co-purchase counts, category.
- Point-in-time correctness: features for an example at time T use only events before T. Add a test that proves no leakage.
- Time-based train/validation/test split (no random split).
- Build `eval/metrics.py` now (Precision/Recall/NDCG/MAP@K) so Phase 3 can use it.
- Document the feature contract in `docs/feature_contract.md` (name, type, window, null handling).
- **Done when:** gold feature tables are written and the leakage test passes.

## Phase 3: Candidate generation
- Baselines: global popularity, recent-popularity by category.
- Item-item co-occurrence (Spark joins, handle skew with salting or broadcast where appropriate).
- ALS (MLlib) with implicit feedback, weighted by event type. Simple hyperparameter sweep.
- Produce top-N candidates per user into `gold/candidates`.
- **Done when:** each generator outputs candidates and has Recall@K computed on the validation split.

## Phase 4: Embeddings and vector search
- Build item embeddings: item2vec on sessions (Spark + gensim or MLlib Word2Vec). Catalog text is hashed, so text embeddings are out of scope.
- Load into Qdrant with metadata; implement similar-item and user-vector (mean of recent item vectors) retrieval.
- Add as a third candidate source and measure its contribution (ablation).
- **Done when:** vector retrieval returns neighbors with a recall/latency benchmark in `docs/RESULTS.md`.

## Phase 5: Ranking
- Merge candidates from all sources, dedupe, attach features.
- Train LightGBM ranker (LambdaRank) with labels from future interactions in the training window.
- Log feature importance, compare to the best single candidate source.
- **Done when:** a measured NDCG@10 comparison against popularity and ALS on the held-out test split is in `docs/RESULTS.md`. If the ranker doesn't win, document why and the next iteration.

## Phase 6: Offline evaluation and experiment simulation
- Extend Phase 2 metrics with catalog coverage, diversity/novelty, and cold-start vs. warm-user breakdowns.
- Simulated A/B backtest: Retailrocket has no logged recommendations or propensities, so true replay/counterfactual evaluation isn't possible. Hash users into control/treatment, score each arm against future interactions, and state this limitation in the write-up.
- Report lift with bootstrap confidence intervals; include a short experiment write-up (hypothesis, metric, result, decision).
- **Done when:** `make eval` regenerates a metrics table and plots.

## Phase 7: Serving layer
- Batch job writes top-N recs per user to Redis (with TTL and versioning).
- FastAPI: `GET /recommendations/{user_id}` (Redis lookup, fallback to popularity), `GET /similar/{item_id}` (vector search).
- Define request/response schemas with Pydantic; document the API contract.
- Load test (locust, ADR-004); record p50/p95/p99 latency and throughput.
- **Done when:** load test results are in `docs/RESULTS.md`.

## Phase 8: Observability and ops
- Structured JSON logging with request IDs.
- Prometheus metrics: request latency, cache hit rate, fallback rate, pipeline stage durations, rows processed, validation failures.
- Grafana dashboard JSON committed to repo.
- Runbook in `docs/RUNBOOK.md` (failure modes: stale recs, Redis down, bad embeddings).
- **Done when:** `docker compose up` brings up API, Redis, Qdrant, Prometheus, Grafana with a working dashboard.

## Phase 9: Performance and scale write-up
- Benchmark pipeline on small vs. large sample. Show before/after for: partitioning, broadcast joins, caching, AQE, skew handling.
- Record Spark UI findings (shuffle size, spill, stage times).
- **Done when:** a performance section in `docs/RESULTS.md` has real before/after numbers.

## Phase 10: Packaging and polish
- Architecture diagram (Mermaid) in README.
- One-command reproducibility: `make all` runs ingest to eval on the sample dataset.
- Optional: Kubernetes manifests (kind) or an AWS EMR/S3 deployment guide.
- Optional stretch: Spark Structured Streaming job that updates user features in near real time.
- Short design doc: ALS vs. embeddings, batch vs. online, offline/online parity decisions.

## Definition of done
- `make all` runs end-to-end on the sample data; CI is green.
- README has an architecture diagram, quickstart, and a results table versus baselines.
- `docs/RESULTS.md` contains measured numbers only.

## Kickoff prompt to paste into Claude Code
> Read PLAN.md. Start with Phase 0 only. Create the repo scaffold, Makefile, CI, and a smoke test. When finished, run the tests, summarize what you did, and wait for my go-ahead before Phase 1.

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

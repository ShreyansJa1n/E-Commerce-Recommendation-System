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

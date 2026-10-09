# Runbook

Stack: `make up` starts Qdrant (:6333), Redis (:6379), the API (:8000), Pushgateway (:9091), Prometheus (:9090, alerts at `/alerts`) and Grafana (:3000, dashboard "recsys: serving & pipeline").
- API logs: `docker compose logs api` (JSON, one line per request, with `request_id`).
- Send `X-Request-ID` on a request to trace it. The API echoes it in the response header and the log line.

## Quick checks
```bash
curl -s localhost:8000/health        # status ok|degraded, redis, qdrant, live version + snapshot meta
curl -s localhost:9090/api/v1/alerts # firing alerts
docker compose ps                    # container health
```

## Stale recommendations (`RecsysSnapshotStale`, `RecsysPipelineStale`)
**Symptom:** snapshot age > 36 h on the dashboard, or `serving_load` hasn't succeeded in 36 h.
1. Check which stage failed: the "Batch pipeline" row (last durations), `data/gold/_reports/*.json`, and the job's logs.
2. A validation failure (`RecsysValidationFailures`) stops the pipeline on purpose. Read `data/silver/_validation/*.json` or `data/gold/_validation/*.json` for the failing check and row counts, and fix the input. Don't bypass the check.
3. Rerun from the failed stage (`make silver|gold|candidates|ranking ENV=base`), then `make serve-load ENV=base`.
4. The API keeps serving the last good snapshot the whole time. Keys have a 7-day TTL, so you have about 5 days after the alert before lists start expiring. If they do expire, unknown keys fall back to popularity.

## Redis down (`RecsysRedisDown`, `RecsysDegradedServing`)
**Symptom:** `/health` shows `redis: false`, and responses have `"source": "popular_degraded"` with `version: null`.
- **What the API does:** every worker warmed a copy of the popularity list at startup. After a Redis error, a 5 s circuit breaker serves that copy without retrying Redis on each request.
- **Drill (2026-10-09):** 200/200 requests succeeded during an outage (p95 2.1 ms), and serving went back to personalized within seconds of Redis returning.
1. `docker compose ps redis` / `docker compose logs redis`. Restart: `docker compose start redis`. Data persists in the `redis_data` volume (RDB snapshots every 60 s if ≥ 1,000 writes).
2. If the volume was lost, republish: `make serve-load ENV=base` (about 25 s).
3. A 503 with "no cached popularity list" means a worker started while Redis was already down. Restart the API after Redis is back: `docker compose restart api`.

## Qdrant down (`RecsysQdrantDown`)
**Symptom:** `/similar/*` returns 503. `/recommendations` is unaffected because it doesn't use Qdrant.
1. `docker compose start qdrant`. The collection persists in `qdrant_storage`.
2. If the collection is missing (`/similar` keeps returning 503 with Qdrant up): `make serve-load ENV=base` reloads the serving cutoff's vectors. Or run only the vectors: `uv run python -c "from recsys.embeddings import vector_store; ..."`.

## Bad embeddings or a bad ranker model
**Symptom:** a spike in `recsys_similar_not_found_total`, user complaints, or offline metrics dropping after a retrain.
1. Check the new run offline before publishing: `make eval ENV=base` (`docs/EVAL_REPORT.md`), `make vectors-bench ENV=base` (recall@10 vs exact should be ≥ 0.98), and the embeddings report (`neighbor_same_category@10` ≈ 0.5 historically).
2. **Rollback is instant:** the previous version stays loaded until its TTL expires.
   ```bash
   uv run python -c "import redis; from recsys.serving.store import RecStore; s=RecStore(redis.Redis(), 'recsys'); print(s.current_version(), s.previous_version()); s.promote(s.previous_version())"
   ```
   The API picks up the flip within 5 s. Its snapshot metadata includes the Qdrant collection name, so `/similar` switches back too, as long as that collection still exists.
3. Known benign warning: gensim may print `Exception ignored in: ...our_dot_float` during training (ADR-009).

## High latency (`RecsysHighLatencyP99`) or 5xx (`RecsysHigh5xxRate`)
1. Dashboard: p99 by route. If only `/similar` is slow, check Qdrant, since HNSW ef and collection size drive its cost. If everything is slow, check host CPU (Spark jobs on the same machine roughly double tail latency) and Redis latency (`redis-cli --latency`).
2. Logs: `docker compose logs api | grep '"status": 5'` with `request_id`, then follow one request.
3. Baseline (RESULTS.md, Phase 7): p99 11 ms personalized / 17 ms similar at about 250 req/s.

## Disk pressure (local dev)
The dev machine has little free disk (see the Phase 3 and Phase 8 notes in RESULTS.md). Spark shuffle and checkpoints, plus Docker images and build cache, are what grow.
- `rm -rf data/_checkpoints`
- `docker builder prune -f`
- Check `docker system df` before rebuilding images.

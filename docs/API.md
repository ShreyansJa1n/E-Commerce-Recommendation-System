# API contract

Service: `recsys-api` (FastAPI). Base URL locally: `http://localhost:8000`. The machine-readable schema is [openapi.json](openapi.json) (`make openapi`), and the interactive docs are at `/docs`. All responses are JSON.

## `GET /recommendations/{user_id}`
Precomputed recommendations for a visitor from the live snapshot.

| param | in | type | default | constraints |
|---|---|---|---|---|
| `user_id` | path | int | — | ≥ 0 |
| `n` | query | int | 10 | 1–50 |

```json
{"user_id": 568980,
 "items": [{"item_id": 195315, "score": 1.122481, "rank": 1}, ...],
 "source": "personalized",
 "version": "2015-09-04-6b5198912f88-20261009T200112"}
```

`source` (policy from [EXPERIMENT.md](EXPERIMENT.md)):
- `personalized`: the LambdaRank list for a visitor with history before the snapshot cutoff.
- `popular`: unknown or cold visitor. Global popularity (trailing 7 days); `score` = 1 / popularity rank.
- `popular_degraded`: Redis unreachable. The API serves its in-process copy of the last popularity list, and `version` is `null`.

Errors: `422` invalid parameters; `503` no live snapshot (and no cached fallback).

## `GET /similar/{item_id}`
Nearest items by item2vec embedding (cosine, Qdrant HNSW), excluding the item itself.

| param | in | type | default | constraints |
|---|---|---|---|---|
| `item_id` | path | int | — | ≥ 0 |
| `n` | query | int | 10 | 1–50 |
| `available_only` | query | bool | false | filter on catalog availability at the cutoff |

```json
{"item_id": 318282, "items": [{"item_id": 160793, "score": 0.9557, "rank": 1}, ...],
 "collection": "items_20150904", "available_only": true}
```

Errors: `404` item has no embedding (too few sessions before the cutoff); `503` Qdrant unreachable or no live collection.

## `GET /health`
`{"status": "ok" | "degraded", "redis": bool, "qdrant": bool, "version": str | null, "snapshot": {...} | null}`. Always returns HTTP 200 so it works as a liveness check. Readiness = `status == "ok"`.

## Snapshot versioning
`make serve-load` writes version `<cutoff>-<model sha256[:12]>-<UTC timestamp>` completely: per-user lists, the popularity list and metadata, with a 7-day TTL on every key. It then flips `recsys:current` in one atomic `SET`. The API re-reads the pointer at most every 5 s. The previous version stays live-able via `recsys:previous` until its TTL expires. Rollback is `RecStore.promote(previous)` (see [RUNBOOK.md](RUNBOOK.md)).

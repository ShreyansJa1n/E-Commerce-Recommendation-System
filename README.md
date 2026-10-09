# Spark Recommendation Platform

End-to-end product recommender on Apache Spark using the Retailrocket e-commerce dataset:
raw events → features → candidates (popularity, co-occurrence, ALS, item2vec) → LightGBM ranker →
offline evaluation → FastAPI/Redis serving with Prometheus/Grafana observability.

See [PLAN.md](PLAN.md) for the phased build plan, [docs/DECISIONS.md](docs/DECISIONS.md) for
architecture decisions, and [docs/RESULTS.md](docs/RESULTS.md) for measured results.

## Status

| Phase | Description | Status |
|---|---|---|
| 0 | Scaffold, Makefile, CI, smoke test | done |
| 1 | Ingest and clean (bronze → silver) | done |
| 2 | Feature engineering (silver → gold) | done |
| 3 | Candidate generation | done |
| 4 | Embeddings and vector search | done |
| 5 | Ranking | done |
| 6 | Offline evaluation and experiment simulation | not started |

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (installs Python 3.12 automatically)
- Java 17 or 21 on `PATH` (Spark 4.0 requires Java 17+)
- Docker (OrbStack or Docker Desktop) for Qdrant: `make up`
- macOS only: `brew install libomp` (LightGBM)

## Quickstart

```bash
make setup     # uv sync + pre-commit hooks
make check     # lint + typecheck + tests
make help      # list all targets
```

## Data

Download the [Retailrocket dataset](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset)
and put `events.csv`, `item_properties_part1.csv`, `item_properties_part2.csv`, and
`category_tree.csv` in `data/raw/` (gitignored). CI never uses the real data: tests run on a
synthetic generator with the same schemas (`tests/fixtures/synth.py`, ADR-003).

```bash
make sample            # data/raw -> data/sample/raw (deterministic 5% of visitors)
make data              # raw -> bronze -> silver -> gold -> candidates on the sample (ENV=sample is the default)
make data ENV=base     # same on the full dataset
make als-sweep ENV=base  # ALS hyperparameter sweep on the validation cutoff
make candidates ENV=base SOURCES=item2vec   # rebuild one source, keep the others
make ranking ENV=base  # LambdaRank re-ranker: train, score val/test, compare to baselines
make ranking-eval ENV=base  # re-evaluate without retraining
make up                # start Qdrant (docker compose)
make vectors-load ENV=base && make vectors-bench ENV=base   # Qdrant vs exact search
make contract          # regenerate docs/feature_contract.md
```

## Pipeline tables

| Layer | Table | Notes |
|---|---|---|
| bronze | `events`, `item_properties`, `category_tree` | Typed with `try_cast`, every input row kept. Events partitioned by `event_date`, properties by `snapshot_date` |
| silver | `events` | Valid, deduplicated events, partitioned by `event_date` |
| silver | `_rejected/events` | Invalid rows with a `reject_reason` |
| silver | `item_properties_scd` | Value versions with `[valid_from, valid_to)`. Join with `property_as_of` for point-in-time correctness (ADR-005) |
| silver | `categories` | Category tree with parent, root, and level |
| silver | `catalog_latest` | Current category/availability per item, for serving and reporting only |
| gold | `events_enriched` | Silver events + `session_id` (30-min gap) + point-in-time `category_id` |
| gold | `cutoffs` | Train/val/test cutoffs T and label windows |
| gold | `user_features`, `user_category_affinity`, `item_features` | Point-in-time features at each cutoff, from events before T only. See [feature contract](docs/feature_contract.md) |
| gold | `labels` | Future interactions in `[T, label_end)` with graded relevance and cold/repeat flags |
| gold | `candidates` | Top-100 per visitor per source (`popular_global`, `popular_category`, `recent_items`, `cooccurrence`, `als`, `item2vec`), partitioned by `cutoff_date`/`source` (ADR-008) |
| gold | `item_neighbors` | Session co-occurrence neighbors (cosine) per item and cutoff |
| gold | `ranking_train` | Sampled LambdaRank training/early-stopping examples (train + val cutoffs) |
| gold | `models/ranker` | LightGBM model (`model.txt`) and metadata (features, best iteration, params) |
| gold | `ranked` | Ranker top-100 per visitor for val and test (ADR-010) |
| gold | `item_embeddings` | item2vec vectors (64-d, L2-normalized) per item and cutoff (ADR-009) |
| Qdrant | `items_<cutoff>` | The benchmark cutoff's vectors + category/availability payload for similar-item and user-vector search |

Ranking metrics (Precision/Recall/NDCG/MAP/hit rate @K) live in `recsys.eval.metrics`, with a
Spark implementation that is cross-checked against a plain-Python reference.

Each stage writes a run report to `<layer>/_reports/<stage>.json`. Silver writes data-quality
reports to `silver/_validation/<table>.json`, and an `error`-severity failure fails the stage.

## Configuration

Configs live in `configs/`. `base.yaml` is always loaded, and an environment file is merged on top
(`load_config("sample")` or `RECSYS_ENV=sample`).

## Layout

```
configs/            yaml configs
data/               raw/, bronze/, silver/, gold/ (gitignored)
src/recsys/         ingest, clean, features, candidates, embeddings, ranking, eval, serving, observability
tests/              pytest (local SparkSession fixture in conftest.py)
docs/               DECISIONS.md, RESULTS.md
```

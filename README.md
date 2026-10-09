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
| 1 | Ingest and clean (bronze → silver) | not started |

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (installs Python 3.12 automatically)
- Java 17 or 21 on `PATH` (Spark 4.0 requires Java 17+)
- macOS only, for later phases: `brew install libomp` (LightGBM)

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
synthetic generator with the same schemas (see ADR-003).

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

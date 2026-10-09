.DEFAULT_GOAL := help
UV ?= uv
ENV ?= sample

.PHONY: help setup lint format typecheck test check sample bronze silver gold embeddings candidates ranking ranking-eval eval als-sweep up down vectors-load vectors-bench data contract clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

setup: ## Create the venv and install deps + pre-commit hooks
	$(UV) sync
	$(UV) run pre-commit install

lint: ## Ruff lint + format check
	$(UV) run ruff check src tests
	$(UV) run ruff format --check src tests

format: ## Auto-fix lint issues and format
	$(UV) run ruff check --fix src tests
	$(UV) run ruff format src tests

typecheck: ## mypy (strict) on src
	$(UV) run mypy

test: ## Run pytest
	$(UV) run pytest

check: lint typecheck test ## Everything CI runs

RUN = $(UV) run python -m recsys.cli

sample: ## Build data/sample/raw from data/raw (deterministic visitor sample)
	@test -f data/raw/events.csv || { echo "data/raw/events.csv not found. Download Retailrocket into data/raw/ (see README)."; exit 1; }
	$(RUN) sample --env sample

bronze: ## Raw CSV -> bronze Parquet (ENV=sample|base)
	$(RUN) bronze --env $(ENV)

silver: ## Bronze -> silver + validation reports (ENV=sample|base)
	$(RUN) silver --env $(ENV)

gold: ## Silver -> gold features, labels, cutoffs (ENV=sample|base)
	$(RUN) gold --env $(ENV)

embeddings: ## item2vec embeddings per cutoff (ENV=sample|base)
	$(RUN) embeddings --env $(ENV)

candidates: ## Candidate sources per cutoff + validation metrics (ENV=, SOURCES=a,b to rebuild a subset)
	$(RUN) candidates --env $(ENV) $(if $(SOURCES),--sources $(SOURCES))

ranking: ## Train the LambdaRank re-ranker, score val/test, compare to baselines (ENV=)
	$(RUN) ranking --env $(ENV)

ranking-eval: ## Re-evaluate the scored ranker vs baselines without retraining (ENV=)
	$(RUN) ranking-eval --env $(ENV)

eval: ## Offline evaluation: metrics, bootstrap/A-B comparisons, figures, report (ENV=)
	$(RUN) eval --env $(ENV)

up: ## Start local services (docker compose)
	docker compose up -d --wait

down: ## Stop local services
	docker compose down

vectors-load: ## Load the benchmark cutoff's item embeddings into Qdrant (needs `make up`)
	$(RUN) vectors-load --env $(ENV)

vectors-bench: ## Qdrant vs exact search: recall@k and latency (needs `make vectors-load`)
	$(RUN) vectors-bench --env $(ENV)

als-sweep: ## ALS hyperparameter sweep on the validation cutoff (ENV=sample|base)
	$(RUN) als-sweep --env $(ENV)

data: bronze silver gold embeddings candidates ranking ## Raw -> ranker end to end

contract: ## Regenerate docs/feature_contract.md from the contract specs
	$(RUN) contract

clean: ## Remove caches and Spark artifacts (keeps data/)
	rm -rf .pytest_cache .mypy_cache .ruff_cache spark-warehouse metastore_db derby.log
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

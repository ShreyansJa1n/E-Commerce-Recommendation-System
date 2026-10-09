.DEFAULT_GOAL := help
UV ?= uv
ENV ?= sample

.PHONY: help setup lint format typecheck test check sample bronze silver data clean

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

data: bronze silver ## Ingest and clean end to end

clean: ## Remove caches and Spark artifacts (keeps data/)
	rm -rf .pytest_cache .mypy_cache .ruff_cache spark-warehouse metastore_db derby.log
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

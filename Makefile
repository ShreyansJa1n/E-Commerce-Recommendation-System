.DEFAULT_GOAL := help
UV ?= uv
ENV ?= sample

.PHONY: help setup lint format typecheck test check sample clean

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

sample: ## Build a small sample from data/raw (implemented in Phase 1)
	@test -f data/raw/events.csv || { echo "data/raw/events.csv not found. Download Retailrocket into data/raw/ (see README)."; exit 1; }
	@echo "make sample is implemented in Phase 1."; exit 1

clean: ## Remove caches and Spark artifacts (keeps data/)
	rm -rf .pytest_cache .mypy_cache .ruff_cache spark-warehouse metastore_db derby.log
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

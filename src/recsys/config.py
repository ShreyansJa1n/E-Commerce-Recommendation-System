"""Typed configuration loaded from ``configs/*.yaml``.

``base.yaml`` is always loaded; an environment file (e.g. ``sample.yaml``) is
deep-merged on top. Select it with ``load_config("sample")`` or the
``RECSYS_ENV`` environment variable.
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, model_validator

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "configs"


class PathsConfig(BaseModel):
    raw: Path
    bronze: Path
    silver: Path
    gold: Path

    def resolved(self, root: Path = REPO_ROOT) -> PathsConfig:
        """Return a copy with relative paths anchored at ``root``."""
        return PathsConfig(
            **{k: (v if v.is_absolute() else root / v) for k, v in self.model_dump().items()}
        )


class SparkConfig(BaseModel):
    master: str = "local[*]"
    driver_memory: str = "4g"
    shuffle_partitions: int = Field(default=8, ge=1)
    extra_conf: dict[str, str] = Field(default_factory=dict)


class IngestConfig(BaseModel):
    events_files: list[str] = Field(default_factory=lambda: ["events.csv"])
    item_properties_files: list[str] = Field(
        default_factory=lambda: ["item_properties_part1.csv", "item_properties_part2.csv"]
    )
    category_tree_file: str = "category_tree.csv"


class CleanConfig(BaseModel):
    valid_event_types: list[str] = Field(
        default_factory=lambda: ["view", "addtocart", "transaction"]
    )
    # Inclusive lower / exclusive upper bound on event time (UTC, ISO dates).
    min_event_date: date
    max_event_date: date
    # Extend the first snapshot of each (item, property) back to -inf. Retailrocket's first
    # property snapshot is a week after the first event, so without this the first week of
    # events has no catalog. See ADR-005.
    backfill_first_property_version: bool = True


class SampleConfig(BaseModel):
    visitor_fraction: float = Field(gt=0, le=1)
    salt: str = "recsys-sample"


class SplitConfig(BaseModel):
    """Time-based split. All dates are UTC midnights; windows are ``[start, end)``.

    - train cutoffs: features from events before T, labels from ``[T, T + horizon)``
    - val: cutoff ``val_start``, labels ``[val_start, test_start)``
    - test: cutoff ``test_start``, labels ``[test_start, end)``
    """

    train_cutoffs: list[date] = Field(min_length=1)
    val_start: date
    test_start: date
    end: date
    label_horizon_days: int = Field(gt=0)

    @model_validator(mode="after")
    def _ordered(self) -> SplitConfig:
        if not self.val_start < self.test_start < self.end:
            raise ValueError("split dates must satisfy val_start < test_start < end")
        horizon = timedelta(days=self.label_horizon_days)
        for t in self.train_cutoffs:
            if t + horizon > self.val_start:
                raise ValueError(f"train cutoff {t} label window overlaps validation")
        if sorted(set(self.train_cutoffs)) != self.train_cutoffs:
            raise ValueError("train_cutoffs must be strictly increasing")
        return self


class FeatureConfig(BaseModel):
    windows_days: list[int] = Field(min_length=1)
    session_gap_minutes: int = Field(gt=0)
    # Interaction strength per event type (category affinity; reused by ALS in Phase 3).
    event_weights: dict[str, float]
    # Graded relevance of a future interaction, used for labels and NDCG.
    relevance: dict[str, int]
    # Pseudo-count for Bayesian-smoothed item conversion rates.
    conversion_prior_views: float = Field(ge=0)
    cooccurrence_window_days: int = Field(gt=0)
    # Sessions with more distinct items than this are skipped when counting co-views.
    max_session_items_for_pairs: int = Field(gt=1)


class PopularityConfig(BaseModel):
    window_days: int = Field(gt=0)


class CategoryCandidatesConfig(BaseModel):
    top_categories: int = Field(gt=0)
    half_life_days: float = Field(gt=0)
    window_days: int = Field(gt=0)


class HistoryCandidatesConfig(BaseModel):
    half_life_days: float = Field(gt=0)


class CooccurrenceConfig(BaseModel):
    window_days: int = Field(gt=0)
    max_session_items: int = Field(gt=1)
    min_pair_sessions: int = Field(ge=1)
    neighbors_per_item: int = Field(gt=0)
    seed_items: int = Field(gt=0)
    seed_half_life_days: float = Field(gt=0)
    salt_buckets: int = Field(ge=0)


class ALSConfig(BaseModel):
    rank: int = Field(gt=0)
    reg_param: float = Field(ge=0)
    alpha: float = Field(gt=0)
    max_iter: int = Field(gt=0)
    min_user_items: int = Field(ge=1)
    min_item_users: int = Field(ge=1)
    seed: int = 42


class ALSSweepConfig(BaseModel):
    rank: list[int] = Field(min_length=1)
    reg_param: list[float] = Field(min_length=1)
    alpha: list[float] = Field(min_length=1)


class Item2VecCandidatesConfig(BaseModel):
    seed_items: int = Field(gt=0)
    seed_half_life_days: float = Field(gt=0)
    query_chunk: int = Field(gt=0)


class CandidatesConfig(BaseModel):
    sources: list[str] = Field(min_length=1)
    top_n: int = Field(gt=0)
    eval_ks: list[int] = Field(min_length=1)
    popularity: PopularityConfig
    category: CategoryCandidatesConfig
    history: HistoryCandidatesConfig
    cooccurrence: CooccurrenceConfig
    item2vec: Item2VecCandidatesConfig
    als: ALSConfig
    als_sweep: ALSSweepConfig

    @model_validator(mode="after")
    def _ks_within_top_n(self) -> CandidatesConfig:
        if max(self.eval_ks) > self.top_n:
            raise ValueError("eval_ks must not exceed top_n")
        return self


class EmbeddingsConfig(BaseModel):
    vector_size: int = Field(gt=0)
    window: int = Field(gt=0)
    min_count: int = Field(ge=1)
    negative: int = Field(ge=1)
    ns_exponent: float
    sample: float = Field(ge=0)
    epochs: int = Field(gt=0)
    min_session_items: int = Field(ge=2)
    seed: int = 42


class VectorBenchmarkConfig(BaseModel):
    split: str
    n_queries: int = Field(gt=0)
    k: int = Field(gt=0)
    batch_size: int = Field(gt=0)
    seed: int = 7
    ef_sweep: list[int] = Field(default_factory=list)


class VectorSearchConfig(BaseModel):
    qdrant_url: str
    collection_prefix: str
    hnsw_m: int = Field(gt=0)
    hnsw_ef_construct: int = Field(gt=0)
    hnsw_full_scan_threshold_kb: int = Field(ge=10)  # Qdrant rejects < 10
    search_ef: int = Field(gt=0)
    upload_batch_size: int = Field(gt=0)
    benchmark: VectorBenchmarkConfig


class Config(BaseModel):
    env: str
    paths: PathsConfig
    spark: SparkConfig = Field(default_factory=SparkConfig)
    ingest: IngestConfig = Field(default_factory=IngestConfig)
    clean: CleanConfig
    sample: SampleConfig
    split: SplitConfig
    features: FeatureConfig
    candidates: CandidatesConfig
    embeddings: EmbeddingsConfig
    vector_search: VectorSearchConfig


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _read_yaml(path: Path) -> dict[str, Any]:
    with path.open() as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config file {path} must contain a mapping")
    return data


def load_config(env: str | None = None, config_dir: Path = CONFIG_DIR) -> Config:
    """Load ``base.yaml`` merged with ``<env>.yaml`` (if env is given and not 'base')."""
    env = env or os.environ.get("RECSYS_ENV", "base")
    data = _read_yaml(config_dir / "base.yaml")
    if env != "base":
        env_file = config_dir / f"{env}.yaml"
        if not env_file.exists():
            raise FileNotFoundError(f"No config for env '{env}' at {env_file}")
        data = _deep_merge(data, _read_yaml(env_file))
    data["env"] = env
    return Config.model_validate(data)

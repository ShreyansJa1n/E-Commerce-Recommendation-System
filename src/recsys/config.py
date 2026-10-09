"""Typed configuration loaded from ``configs/*.yaml``.

``base.yaml`` is always loaded; an environment file (e.g. ``sample.yaml``) is
deep-merged on top. Select it with ``load_config("sample")`` or the
``RECSYS_ENV`` environment variable.
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

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


class Config(BaseModel):
    env: str
    paths: PathsConfig
    spark: SparkConfig = Field(default_factory=SparkConfig)
    ingest: IngestConfig = Field(default_factory=IngestConfig)
    clean: CleanConfig
    sample: SampleConfig


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

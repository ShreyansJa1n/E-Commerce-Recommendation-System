import os
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from recsys.config import Config, SparkConfig, load_config
from recsys.spark import get_spark
from tests.fixtures import synth

# PySpark converts timestamps between Python and the JVM using the *driver process* TZ,
# not spark.sql.session.timeZone. Pin it so naive datetimes in tests mean UTC everywhere.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = get_spark(
        "recsys-tests",
        SparkConfig(master="local[2]", driver_memory="1g", shuffle_partitions=2),
        {"spark.ui.enabled": "false"},
    )
    yield session
    session.stop()


def make_config(root: Path) -> Config:
    """Base config with all data paths under ``root``."""
    data = load_config("base").model_dump()
    data["paths"] = {k: str(root / k) for k in ("raw", "bronze", "silver", "gold")}
    data["env"] = "test"
    # Synthetic events cover 2015-05-03 .. 2015-05-31 (see tests/fixtures/synth.py).
    data["split"] = {
        "train_cutoffs": ["2015-05-17"],
        "val_start": "2015-05-24",
        "test_start": "2015-05-27",
        "end": "2015-05-31",
        "label_horizon_days": 7,
    }
    return Config.model_validate(data)


@pytest.fixture(scope="session")
def synth_env(tmp_path_factory: pytest.TempPathFactory) -> tuple[Config, synth.SynthStats]:
    root = tmp_path_factory.mktemp("synth")
    cfg = make_config(root)
    stats = synth.generate(cfg.paths.raw)
    return cfg, stats

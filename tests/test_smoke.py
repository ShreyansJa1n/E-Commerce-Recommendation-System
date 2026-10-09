from pathlib import Path

import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession

from recsys.config import REPO_ROOT, load_config


def test_spark_session_defaults(spark: SparkSession) -> None:
    assert spark.conf.get("spark.sql.adaptive.enabled") == "true"
    assert spark.conf.get("spark.sql.shuffle.partitions") == "2"
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


def test_spark_roundtrip(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, "view"), (2, "addtocart"), (1, "view")], ["user", "event"])
    result = df.groupBy("user").count().orderBy("user")
    expected = spark.createDataFrame([(1, 2), (2, 1)], ["user", "count"])
    assert_df_equality(result, expected, ignore_nullable=True)


def test_load_base_config() -> None:
    cfg = load_config("base")
    assert cfg.env == "base"
    assert cfg.paths.raw == Path("data/raw")
    assert cfg.paths.resolved().raw == REPO_ROOT / "data/raw"


def test_load_sample_config_merges_over_base() -> None:
    cfg = load_config("sample")
    assert cfg.paths.raw == Path("data/sample/raw")
    assert cfg.spark.shuffle_partitions == 4
    assert cfg.spark.master == "local[*]"  # inherited from base


def test_unknown_env_raises() -> None:
    with pytest.raises(FileNotFoundError):
        load_config("does-not-exist")

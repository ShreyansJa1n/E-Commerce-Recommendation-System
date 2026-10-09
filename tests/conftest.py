from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from recsys.config import SparkConfig
from recsys.spark import get_spark


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = get_spark(
        "recsys-tests",
        SparkConfig(master="local[2]", driver_memory="1g", shuffle_partitions=2),
        {"spark.ui.enabled": "false"},
    )
    yield session
    session.stop()

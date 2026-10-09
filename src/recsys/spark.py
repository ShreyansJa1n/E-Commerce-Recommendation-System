"""Local SparkSession helper with sane defaults."""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import SparkSession

from recsys.config import SparkConfig

DEFAULT_CONF: dict[str, str] = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.ui.showConsoleProgress": "false",
}


def get_spark(
    app_name: str = "recsys",
    config: SparkConfig | None = None,
    conf_overrides: Mapping[str, str] | None = None,
) -> SparkSession:
    """Build (or reuse) a SparkSession.

    Precedence (lowest to highest): ``DEFAULT_CONF``, ``config`` fields,
    ``config.extra_conf``, ``conf_overrides``.
    """
    config = config or SparkConfig()
    conf = dict(DEFAULT_CONF)
    conf["spark.sql.shuffle.partitions"] = str(config.shuffle_partitions)
    conf["spark.driver.memory"] = config.driver_memory
    conf.update(config.extra_conf)
    conf.update(conf_overrides or {})

    builder = SparkSession.builder.appName(app_name).master(config.master)
    for key, value in conf.items():
        builder = builder.config(key, value)
    session: SparkSession = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session

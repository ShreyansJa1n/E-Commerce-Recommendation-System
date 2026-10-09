"""Table read/write helpers. Every pipeline write goes through ``write_table``."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from recsys.observability import pipeline_metrics

log = logging.getLogger(__name__)


def write_table(df: DataFrame, path: Path, partition_by: Sequence[str] = ()) -> None:
    """Fully replace the Parquet table at ``path``.

    Uses static partition overwrite so a rerun also drops partitions that no longer
    exist in the output. The session default stays dynamic for incremental jobs.
    """
    writer = df.write.mode("overwrite").option("partitionOverwriteMode", "static")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(str(path))


def read_table(spark: SparkSession, path: Path) -> DataFrame:
    return spark.read.parquet(str(path))


@dataclass
class StageReport:
    """Measured facts about one pipeline stage, written as JSON next to the outputs."""

    stage: str
    rows: dict[str, int] = field(default_factory=dict)
    seconds: float = 0.0
    extra: dict[str, Any] = field(default_factory=dict)

    def write(self, directory: Path) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        out = directory / f"{self.stage}.json"
        out.write_text(json.dumps(asdict(self), indent=2, default=str) + "\n")
        return out


@contextmanager
def timed_stage(stage: str) -> Iterator[StageReport]:
    report = StageReport(stage=stage)
    start = time.perf_counter()
    ok = False
    try:
        yield report
        ok = True
    finally:
        report.seconds = round(time.perf_counter() - start, 2)
        log.info("stage=%s seconds=%.2f rows=%s ok=%s", stage, report.seconds, report.rows, ok)
        if ok:  # only successful runs update the "last success" metrics
            pipeline_metrics.push_stage(stage, report.seconds, report.rows)

"""Pipeline entrypoint: ``python -m recsys.cli <stage> [--env ENV]``."""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Callable
from dataclasses import asdict

from pyspark.sql import SparkSession

from recsys.clean import catalog, events
from recsys.config import Config, load_config
from recsys.ingest import raw_to_bronze, sample
from recsys.io import StageReport
from recsys.spark import get_spark

Stage = Callable[[SparkSession, Config], StageReport]

STAGES: dict[str, list[Stage]] = {
    "bronze": [raw_to_bronze.run],
    "silver": [events.run, catalog.run],
}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="recsys")
    parser.add_argument("stage", choices=[*STAGES, "sample"])
    parser.add_argument("--env", default=None, help="config env (default: $RECSYS_ENV or base)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = load_config(args.env)
    spark = get_spark(f"recsys-{args.stage}", cfg.spark)
    try:
        if args.stage == "sample":
            # Always sample from the full raw data into this env's raw path.
            reports = [sample.run(spark, load_config("base"), cfg)]
        else:
            reports = [stage(spark, cfg) for stage in STAGES[args.stage]]
        for r in reports:
            print(json.dumps(asdict(r), default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

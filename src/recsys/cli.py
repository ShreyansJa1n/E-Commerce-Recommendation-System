"""Pipeline entrypoint: ``python -m recsys.cli <stage> [--env ENV]``."""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Callable
from dataclasses import asdict

from pyspark.sql import SparkSession

from recsys.candidates import pipeline as candidates
from recsys.candidates import sweep
from recsys.clean import catalog, events
from recsys.config import REPO_ROOT, Config, load_config
from recsys.embeddings import pipeline as embeddings
from recsys.embeddings import vector_store
from recsys.features import pipeline as features
from recsys.features.contract import render_markdown
from recsys.ingest import raw_to_bronze, sample
from recsys.io import StageReport
from recsys.ranking import pipeline as ranking
from recsys.spark import get_spark

Stage = Callable[[SparkSession, Config], StageReport]

STAGES: dict[str, list[Stage]] = {
    "bronze": [raw_to_bronze.run],
    "silver": [events.run, catalog.run],
    "gold": [features.run],
    "embeddings": [embeddings.run],
    "candidates": [candidates.run],
    "ranking": [ranking.run],
    "ranking-eval": [ranking.evaluate_ranker],
    "vectors-load": [vector_store.load],
    "vectors-bench": [vector_store.benchmark],
    "als-sweep": [sweep.run],
}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="recsys")
    parser.add_argument("stage", choices=[*STAGES, "sample", "contract"])
    parser.add_argument("--env", default=None, help="config env (default: $RECSYS_ENV or base)")
    parser.add_argument(
        "--sources", default=None, help="candidates: comma-separated subset of sources to rebuild"
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = load_config(args.env)
    if args.sources:
        sources = [x.strip() for x in args.sources.split(",") if x.strip()]
        cfg = cfg.model_copy(
            update={"candidates": cfg.candidates.model_copy(update={"sources": sources})}
        )
    if args.stage == "contract":
        out = REPO_ROOT / "docs" / "feature_contract.md"
        out.write_text(render_markdown(load_config("base")))
        print(f"wrote {out}")
        return
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

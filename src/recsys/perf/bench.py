"""Before/after Spark optimization benchmarks on real pipeline workloads (``make perf``).

Each experiment runs its variants with SQL confs set at runtime on one session; every
variant runs ``reps`` times (cache cleared in between) and reports the median wall time
plus stage metrics from the Spark REST API for its last repetition.
"""

from __future__ import annotations

import json
import shutil
import statistics
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from recsys.candidates import cooccurrence
from recsys.config import Config
from recsys.features.events import enrich_events
from recsys.features.split import cutoffs, history
from recsys.features.user import build_user_features
from recsys.io import StageReport, read_table, timed_stage
from recsys.perf.sparkmetrics import group_metrics

Workload = Callable[[], int]


@dataclass
class Variant:
    name: str
    conf: dict[str, str]
    run: Workload
    note: str = ""


@dataclass
class Experiment:
    name: str
    question: str
    variants: list[Variant]
    setup: Callable[[], None] | None = None
    results: list[dict[str, Any]] = field(default_factory=list)


BASELINE_CONF = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.autoBroadcastJoinThreshold": str(10 * 1024 * 1024),
    "spark.sql.shuffle.partitions": "8",
}


def _measure(spark: SparkSession, exp: str, v: Variant, reps: int) -> dict[str, Any]:
    for k, val in {**BASELINE_CONF, **v.conf}.items():
        spark.conf.set(k, val)
    times, rows = [], 0
    group = ""
    for i in range(reps):
        spark.catalog.clearCache()
        group = f"{exp}:{v.name}:{i}"
        spark.sparkContext.setJobGroup(group, group)
        start = time.perf_counter()
        rows = v.run()
        times.append(time.perf_counter() - start)
    spark.sparkContext.setJobGroup("", "")
    time.sleep(1.0)  # let the listener bus flush the last stage metrics to the REST API
    return {
        "variant": v.name,
        "note": v.note,
        "conf": v.conf,
        "result_rows": rows,
        "seconds_median": round(statistics.median(times), 3),
        "seconds_all": [round(t, 3) for t in times],
        "metrics_last_rep": group_metrics(spark, group),
    }


def experiments(spark: SparkSession, cfg: Config) -> list[Experiment]:
    p = cfg.paths.resolved()
    (cut,) = [c for c in cutoffs(cfg.split) if c.split == "val"]
    events_path = p.silver / "events"
    flat_path = p.gold.parent / "perf" / "events_flat"  # scratch copy, removed after the run
    week_lo, week_hi = cut.cutoff_date - timedelta(days=7), cut.cutoff_date

    # 1) Partition pruning -------------------------------------------------------------
    def _write_flat() -> None:
        if not flat_path.exists():
            read_table(spark, events_path).drop("event_date").coalesce(8).write.parquet(
                str(flat_path)
            )

    def pruned() -> int:
        df = read_table(spark, events_path).where(
            F.col("event_date").between(week_lo, week_hi - timedelta(days=1))
        )
        return len(df.groupBy("event_type").count().collect())

    def flat() -> int:
        lo, hi = cut.ts - timedelta(days=7), cut.ts
        df = spark.read.parquet(str(flat_path)).where(
            (F.col("event_ts") >= F.lit(lo)) & (F.col("event_ts") < F.lit(hi))
        )
        return len(df.groupBy("event_type").count().collect())

    # 2) Broadcast join -----------------------------------------------------------------
    def join_catalog(hint: bool) -> Workload:
        def run() -> int:
            ev = read_table(spark, p.gold / "events_enriched")
            cat = read_table(spark, p.silver / "catalog_latest").select(
                "item_id", "root_category_id"
            )
            j = ev.join(F.broadcast(cat) if hint else cat, "item_id")
            return len(j.groupBy("root_category_id").count().collect())

        return run

    # 3) Caching -------------------------------------------------------------------------
    def reuse(cache: bool) -> Workload:
        def run() -> int:
            ev = read_table(spark, events_path)
            scd = read_table(spark, p.silver / "item_properties_scd")
            enriched = history(enrich_events(ev, scd, cfg.features.session_gap_minutes), cut)
            if cache:
                enriched = enriched.cache()
            a = enriched.groupBy("visitor_id").count().count()
            b = enriched.groupBy("category_id").count().count()
            c = enriched.select("session_id").distinct().count()
            return a + b + c

        return run

    # 4) AQE -----------------------------------------------------------------------------
    def user_features() -> int:
        ev = read_table(spark, p.gold / "events_enriched")
        return build_user_features(ev, cut, cfg.features).count()

    # 5) Skew ----------------------------------------------------------------------------
    hist_cache: dict[str, DataFrame] = {}

    def seeds_neighbors(salt: int) -> Workload:
        def run() -> int:
            if "hist" not in hist_cache:
                ev = read_table(spark, p.gold / "events_enriched")
                hist_cache["hist"] = history(ev, cut)
            hist = hist_cache["hist"]
            labels = read_table(spark, p.gold / "labels").where(
                F.col("cutoff_date") == F.lit(cut.cutoff_date)
            )
            targets = labels.select("visitor_id").distinct()
            cc = cfg.candidates.cooccurrence.model_copy(update={"salt_buckets": salt})
            nbrs = (
                read_table(spark, p.gold / "item_neighbors")
                .where(F.col("cutoff_date") == F.lit(cut.cutoff_date))
                .drop("cutoff_date")
            )
            seeds = cooccurrence.seed_items(hist, targets, cut, cc, cfg.features.event_weights)
            return cooccurrence.recommend(seeds, nbrs, cc, cfg.candidates.top_n).count()

        return run

    no_bc = {"spark.sql.autoBroadcastJoinThreshold": "-1"}
    aqe_off = {"spark.sql.adaptive.enabled": "false"}
    return [
        Experiment(
            "partition_pruning",
            "7-day event aggregation: date-partitioned table vs. the same data unpartitioned",
            [
                Variant(
                    "unpartitioned (filter on event_ts)", {}, flat, "reads every file, filters rows"
                ),
                Variant(
                    "partitioned by event_date (pruned)", {}, pruned, "reads only 7 partitions"
                ),
            ],
            setup=_write_flat,
        ),
        Experiment(
            "broadcast_join",
            "2.76M events joined with the 417k-item catalog, then aggregated",
            [
                Variant(
                    "sort-merge join (AQE off, broadcast off)",
                    {**aqe_off, **no_bc},
                    join_catalog(False),
                ),
                Variant("broadcast hint (AQE off)", aqe_off, join_catalog(True)),
                Variant("default planner (AQE on, 10 MB threshold)", {}, join_catalog(False)),
            ],
        ),
        Experiment(
            "caching",
            "Point-in-time enrichment (as-of join + sessionization) reused by 3 aggregations",
            [
                Variant("recompute each time", {}, reuse(False)),
                Variant("cache() once", {}, reuse(True)),
            ],
        ),
        Experiment(
            "aqe",
            "User features for one cutoff with spark.sql.shuffle.partitions = 200",
            [
                Variant(
                    "AQE off, 200 partitions",
                    {**aqe_off, "spark.sql.shuffle.partitions": "200"},
                    user_features,
                ),
                Variant(
                    "AQE on, 200 partitions (coalesced)",
                    {"spark.sql.shuffle.partitions": "200"},
                    user_features,
                ),
                Variant("AQE on, 8 partitions (project default)", {}, user_features),
            ],
        ),
        Experiment(
            "skew",
            "Co-occurrence seed x neighbor join (hot items are hot keys), 64 shuffle partitions",
            [
                Variant(
                    "no skew handling (AQE off)",
                    {**aqe_off, **no_bc, "spark.sql.shuffle.partitions": "64"},
                    seeds_neighbors(0),
                ),
                Variant(
                    "AQE on, skew-join off",
                    {
                        **no_bc,
                        "spark.sql.shuffle.partitions": "64",
                        "spark.sql.adaptive.skewJoin.enabled": "false",
                        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "1MB",
                    },
                    seeds_neighbors(0),
                    "isolates AQE coalescing from skew-join splitting",
                ),
                Variant(
                    "AQE skew-join",
                    {
                        **no_bc,
                        "spark.sql.shuffle.partitions": "64",
                        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "1MB",
                        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "1MB",
                    },
                    seeds_neighbors(0),
                ),
                Variant(
                    "salted join, 8 buckets (AQE off)",
                    {**aqe_off, **no_bc, "spark.sql.shuffle.partitions": "64"},
                    seeds_neighbors(8),
                ),
            ],
        ),
    ]


def run(spark: SparkSession, cfg: Config, reps: int = 3) -> StageReport:
    gold = cfg.paths.resolved().gold
    out: list[dict[str, Any]] = []
    with timed_stage("perf") as report:
        for exp in experiments(spark, cfg):
            if exp.setup:
                exp.setup()
            for v in exp.variants:
                r = _measure(spark, exp.name, v, reps)
                print(
                    json.dumps(
                        {
                            "experiment": exp.name,
                            **{k: r[k] for k in ("variant", "seconds_median", "result_rows")},
                        }
                    )
                )
                exp.results.append(r)
            out.append({"experiment": exp.name, "question": exp.question, "results": exp.results})
        report.extra = {"reps": reps, "experiments": out, "spark_version": spark.version}
    report.write(gold / "_reports")
    shutil.rmtree(gold.parent / "perf", ignore_errors=True)
    return report

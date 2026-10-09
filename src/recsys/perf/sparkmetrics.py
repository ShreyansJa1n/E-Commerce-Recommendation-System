"""Read stage metrics for a job group from the Spark REST API (what the Spark UI shows)."""

from __future__ import annotations

import json
import urllib.request
from typing import Any

from pyspark.sql import SparkSession


def _get(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read())


def group_metrics(spark: SparkSession, group: str) -> dict[str, Any]:
    """Aggregate metrics over all stages of all succeeded jobs in ``group``.

    ``max_over_median_task_s`` is computed for the stage with the most shuffle read:
    the ratio of its slowest task to its median task (a direct skew measure).
    """
    sc = spark.sparkContext
    base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
    jobs = [
        j for j in _get(f"{base}/jobs") if j.get("jobGroup") == group and j["status"] == "SUCCEEDED"
    ]
    stage_ids = sorted({s for j in jobs for s in j["stageIds"]})
    totals = {
        k: 0
        for k in (
            "inputBytes",
            "shuffleReadBytes",
            "shuffleWriteBytes",
            "memoryBytesSpilled",
            "diskBytesSpilled",
            "executorRunTime",
            "numTasks",
        )
    }
    heaviest: tuple[int, int, int] | None = None  # (shuffleRead, stageId, attemptId)
    n_stages = 0
    for sid in stage_ids:
        for att in _get(f"{base}/stages/{sid}"):
            if att.get("status") != "COMPLETE":
                continue  # skipped (reused) stages did no work
            n_stages += 1
            for k in totals:
                totals[k] += int(att.get(k, 0))
            key = (int(att.get("shuffleReadBytes", 0)), sid, int(att["attemptId"]))
            if heaviest is None or key > heaviest:
                heaviest = key
    skew = None
    if heaviest and heaviest[0] > 0:
        q = _get(f"{base}/stages/{heaviest[1]}/{heaviest[2]}/taskSummary?quantiles=0.5,0.95,1.0")
        med, p95, mx = q["executorRunTime"]
        skew = {
            "stage": heaviest[1],
            "task_ms_p50": med,
            "task_ms_p95": p95,
            "task_ms_max": mx,
            "max_over_median": round(mx / med, 2) if med else None,
        }
    return {"jobs": len(jobs), "stages": n_stages, **totals, "heaviest_shuffle_stage": skew}

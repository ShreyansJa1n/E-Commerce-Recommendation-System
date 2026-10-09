from types import SimpleNamespace
from typing import Any

import pytest

from recsys.perf import sparkmetrics


def test_group_metrics_aggregates_completed_stages(monkeypatch: pytest.MonkeyPatch) -> None:
    base = "http://ui/api/v1/applications/app-1"
    responses: dict[str, Any] = {
        f"{base}/jobs": [
            {"jobGroup": "g", "status": "SUCCEEDED", "stageIds": [1, 2]},
            {"jobGroup": "g", "status": "FAILED", "stageIds": [9]},
            {"jobGroup": "other", "status": "SUCCEEDED", "stageIds": [3]},
        ],
        f"{base}/stages/1": [
            {
                "status": "COMPLETE",
                "attemptId": 0,
                "inputBytes": 100,
                "shuffleReadBytes": 0,
                "shuffleWriteBytes": 50,
                "executorRunTime": 10,
                "numTasks": 4,
            },
        ],
        f"{base}/stages/2": [
            {
                "status": "COMPLETE",
                "attemptId": 0,
                "shuffleReadBytes": 50,
                "shuffleWriteBytes": 0,
                "memoryBytesSpilled": 7,
                "executorRunTime": 30,
                "numTasks": 2,
            },
            {"status": "SKIPPED", "attemptId": 1, "shuffleReadBytes": 999},
        ],
        f"{base}/stages/2/0/taskSummary?quantiles=0.5,0.95,1.0": {
            "executorRunTime": [10.0, 18.0, 25.0]
        },
    }
    monkeypatch.setattr(sparkmetrics, "_get", lambda url: responses[url])
    spark = SimpleNamespace(
        sparkContext=SimpleNamespace(uiWebUrl="http://ui", applicationId="app-1")
    )
    m = sparkmetrics.group_metrics(spark, "g")  # type: ignore[arg-type]
    assert (m["jobs"], m["stages"], m["numTasks"]) == (
        1,
        2,
        6,
    )  # failed job + skipped attempt ignored
    assert (m["inputBytes"], m["shuffleReadBytes"], m["memoryBytesSpilled"]) == (100, 50, 7)
    assert m["heaviest_shuffle_stage"] == {
        "stage": 2,
        "task_ms_p50": 10.0,
        "task_ms_p95": 18.0,
        "task_ms_max": 25.0,
        "max_over_median": 2.5,
    }

"""Push batch-pipeline metrics to a Prometheus Pushgateway (if RECSYS_PUSHGATEWAY is set).

Batch jobs can't be scraped, so each stage pushes its duration, row counts and
validation results on completion, grouped by (job=recsys_pipeline, stage, env).
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Mapping

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

log = logging.getLogger(__name__)


def _gateway() -> str | None:
    return os.environ.get("RECSYS_PUSHGATEWAY")


def push_stage(stage: str, seconds: float, rows: Mapping[str, int], env: str | None = None) -> bool:
    gw = _gateway()
    if not gw:
        return False
    reg = CollectorRegistry()
    Gauge("recsys_pipeline_stage_duration_seconds", "Stage wall time.", registry=reg).set(seconds)
    g_rows = Gauge(
        "recsys_pipeline_rows", "Rows written/processed per table.", ["table"], registry=reg
    )
    for table, n in rows.items():
        g_rows.labels(table=table).set(n)
    Gauge(
        "recsys_pipeline_last_success_timestamp_seconds",
        "Unix time the stage last finished.",
        registry=reg,
    ).set(time.time())
    return _push(reg, stage, env)


def push_validation(
    table: str, failed_error: int, failed_warn: int, rows: int, env: str | None = None
) -> bool:
    gw = _gateway()
    if not gw:
        return False
    reg = CollectorRegistry()
    g = Gauge(
        "recsys_validation_failed_checks",
        "Failed data-quality checks.",
        ["table", "severity"],
        registry=reg,
    )
    g.labels(table=table, severity="error").set(failed_error)
    g.labels(table=table, severity="warn").set(failed_warn)
    Gauge("recsys_validation_rows", "Rows validated.", ["table"], registry=reg).labels(
        table=table
    ).set(rows)
    return _push(reg, f"validation_{table}", env)


def _push(reg: CollectorRegistry, stage: str, env: str | None) -> bool:
    try:
        push_to_gateway(
            _gateway() or "",
            job="recsys_pipeline",
            grouping_key={"stage": stage, "env": env or os.environ.get("RECSYS_ENV", "unknown")},
            registry=reg,
            timeout=3,
        )
        return True
    except Exception as e:  # never fail a pipeline stage because monitoring is down
        log.warning("pushgateway push failed for %s: %s", stage, e)
        return False

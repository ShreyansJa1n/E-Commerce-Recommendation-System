"""Prometheus metrics for the API (multi-process safe under uvicorn workers).

When PROMETHEUS_MULTIPROC_DIR is set (the container sets it), every worker writes to
shared files and /metrics aggregates them; otherwise the default registry is used.
"""

from __future__ import annotations

import os

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

LATENCY_BUCKETS = (0.001, 0.0025, 0.005, 0.0075, 0.01, 0.015, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0)

REQUEST_LATENCY = Histogram(
    "recsys_http_request_duration_seconds",
    "HTTP request latency by route template, method and status.",
    ["route", "method", "status"],
    buckets=LATENCY_BUCKETS,
)
RECS_SERVED = Counter(
    "recsys_recommendations_served_total",
    "Recommendation responses by source (personalized = Redis cache hit for the user).",
    ["source"],
)
DEPENDENCY_ERRORS = Counter(
    "recsys_dependency_errors_total", "Errors talking to a dependency.", ["dependency"]
)
# Pre-create label sets so dashboards show 0 instead of "no data" before the first event.
for _dep in ("redis", "qdrant"):
    DEPENDENCY_ERRORS.labels(_dep)
SIMILAR_NOT_FOUND = Counter(
    "recsys_similar_not_found_total", "/similar for items without an embedding."
)
DEPENDENCY_UP = Gauge(
    "recsys_dependency_up",
    "1 if the dependency answered the last check.",
    ["dependency"],
    multiprocess_mode="mostrecent",
)
for _src in ("personalized", "popular", "popular_degraded"):
    RECS_SERVED.labels(_src)
SNAPSHOT_AGE = Gauge(
    "recsys_snapshot_age_seconds",
    "Seconds since the live snapshot was published.",
    multiprocess_mode="mostrecent",
)
SNAPSHOT_USERS = Gauge(
    "recsys_snapshot_users",
    "Visitors with personalized lists in the live snapshot.",
    multiprocess_mode="mostrecent",
)
SNAPSHOT_INFO = Gauge(
    "recsys_snapshot_info",
    "Live snapshot (labels carry the version and model).",
    ["version", "model", "cutoff_date"],
    multiprocess_mode="mostrecent",
)


def render() -> tuple[bytes, str]:
    if os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
        from prometheus_client import multiprocess

        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)  # type: ignore[no-untyped-call]
        return generate_latest(registry), CONTENT_TYPE_LATEST
    return generate_latest(), CONTENT_TYPE_LATEST

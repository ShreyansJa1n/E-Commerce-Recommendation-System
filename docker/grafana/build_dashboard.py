"""Generate docker/grafana/dashboards/recsys.json (run: python docker/grafana/build_dashboard.py).

Panels follow the data-viz reference palette: blue for the main series, fixed
categorical order for multi-series, status colors only for health thresholds.
"""

import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "prometheus"}
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
GOOD, WARN, CRIT = "#0ca30c", "#fab219", "#d03b3b"
_id = 0


def _next() -> int:
    global _id
    _id += 1
    return _id


def target(expr: str, legend: str = "", instant: bool = False, fmt: str = "time_series") -> dict:
    return {
        "datasource": DS,
        "expr": expr,
        "legendFormat": legend,
        "refId": chr(64 + _next() % 26 or 1),
        "instant": instant,
        "range": not instant,
        "format": fmt,
    }


def ts(
    title: str,
    targets: list[dict],
    x: int,
    y: int,
    w: int = 12,
    h: int = 8,
    unit: str = "short",
    desc: str = "",
    overrides: list | None = None,
) -> dict:
    return {
        "id": _next(),
        "type": "timeseries",
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": targets,
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "color": {"mode": "palette-classic"},
                "custom": {"lineWidth": 2, "fillOpacity": 0, "showPoints": "never"},
            },
            "overrides": overrides or [],
        },
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom"},
            "tooltip": {"mode": "multi"},
        },
    }


def stat(
    title: str,
    expr: str,
    x: int,
    y: int,
    w: int = 4,
    h: int = 4,
    unit: str = "short",
    steps: list | None = None,
    desc: str = "",
    decimals: int | None = None,
    legend: str = "",
) -> dict:
    defaults = {
        "unit": unit,
        "color": {"mode": "thresholds"},
        "thresholds": {"mode": "absolute", "steps": steps or [{"color": SERIES[0], "value": None}]},
    }
    if decimals is not None:
        defaults["decimals"] = decimals
    return {
        "id": _next(),
        "type": "stat",
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [target(expr, legend, instant=True)],
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"]},
            "colorMode": "value",
            "graphMode": "none",
            "textMode": "value_and_name" if legend else "value",
        },
    }


def color(name: str, c: str) -> dict:
    return {
        "matcher": {"id": "byName", "options": name},
        "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": c}}],
    }


def row(title: str, y: int) -> dict:
    return {
        "id": _next(),
        "type": "row",
        "title": title,
        "collapsed": False,
        "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
        "panels": [],
    }


H = "recsys_http_request_duration_seconds"
ROUTE_COLORS = [
    color("/recommendations/{user_id}", SERIES[0]),
    color("/similar/{item_id}", SERIES[1]),
]
R = "recsys_recommendations_served_total"
panels = [
    row("API traffic and latency", 0),
    stat(
        "Request rate",
        f"sum(rate({H}_count{{route!='/health'}}[1m]))",
        0,
        1,
        unit="reqps",
        decimals=0,
    ),
    stat(
        "p95 latency",
        f"histogram_quantile(0.95, sum by (le) (rate({H}_bucket{{route!='/health'}}[1m])))",
        4,
        1,
        unit="s",
        steps=[
            {"color": GOOD, "value": None},
            {"color": WARN, "value": 0.05},
            {"color": CRIT, "value": 0.1},
        ],
    ),
    stat(
        "Personalized share (cache hit rate)",
        f"sum(rate({R}{{source='personalized'}}[5m])) / sum(rate({R}[5m]))",
        8,
        1,
        unit="percentunit",
        decimals=1,
        desc="Share of /recommendations answered from the visitor's own Redis list.",
    ),
    stat(
        "5xx rate",
        f"(sum(rate({H}_count{{status=~'5..'}}[5m])) or vector(0)) / sum(rate({H}_count[5m]))",
        12,
        1,
        unit="percentunit",
        decimals=2,
        steps=[{"color": GOOD, "value": None}, {"color": CRIT, "value": 0.01}],
    ),
    stat(
        "Snapshot age",
        "max(recsys_snapshot_age_seconds)",
        16,
        1,
        unit="s",
        steps=[
            {"color": GOOD, "value": None},
            {"color": WARN, "value": 86400},
            {"color": CRIT, "value": 129600},
        ],
        desc="Time since `make serve-load` published the live snapshot (alert at 36h).",
    ),
    stat("Users in snapshot", "max(recsys_snapshot_users)", 20, 1, decimals=0),
    ts(
        "Requests / s by route",
        [target(f"sum by (route) (rate({H}_count{{route!='/health'}}[1m]))", "{{route}}")],
        0,
        5,
        unit="reqps",
        overrides=ROUTE_COLORS,
    ),
    ts(
        "Latency percentiles (all routes)",
        [
            target(
                f"histogram_quantile(0.50, sum by (le) (rate({H}_bucket{{route!='/health'}}[1m])))",
                "p50",
            ),
            target(
                f"histogram_quantile(0.95, sum by (le) (rate({H}_bucket{{route!='/health'}}[1m])))",
                "p95",
            ),
            target(
                f"histogram_quantile(0.99, sum by (le) (rate({H}_bucket{{route!='/health'}}[1m])))",
                "p99",
            ),
        ],
        12,
        5,
        unit="s",
        overrides=[color("p50", SERIES[0]), color("p95", SERIES[1]), color("p99", SERIES[2])],
    ),
    ts(
        "p99 latency by route",
        [
            target(
                f"histogram_quantile(0.99, sum by (le, route) (rate({H}_bucket{{route!='/health'}}[1m])))",
                "{{route}}",
            )
        ],
        0,
        13,
        unit="s",
        overrides=ROUTE_COLORS,
    ),
    ts(
        "Recommendations by source (fallback rate)",
        [target(f"sum by (source) (rate({R}[1m]))", "{{source}}")],
        12,
        13,
        unit="reqps",
        overrides=[
            color("personalized", SERIES[0]),
            color("popular", SERIES[1]),
            color("popular_degraded", CRIT),
        ],
        desc="popular = unknown/cold visitor fallback; popular_degraded = Redis unavailable.",
    ),
    row("Dependencies", 21),
    stat(
        "Redis",
        "min(recsys_dependency_up{dependency='redis'})",
        0,
        22,
        w=3,
        steps=[{"color": CRIT, "value": None}, {"color": GOOD, "value": 1}],
    ),
    stat(
        "Qdrant",
        "min(recsys_dependency_up{dependency='qdrant'})",
        3,
        22,
        w=3,
        steps=[{"color": CRIT, "value": None}, {"color": GOOD, "value": 1}],
    ),
    ts(
        "Dependency errors / s",
        [
            target(
                "sum by (dependency) (rate(recsys_dependency_errors_total[1m]))", "{{dependency}}"
            )
        ],
        6,
        22,
        w=9,
        h=6,
        unit="short",
        overrides=[color("redis", SERIES[0]), color("qdrant", SERIES[1])],
    ),
    ts(
        "Status codes / s",
        [target(f"sum by (status) (rate({H}_count{{route!='/health'}}[1m]))", "{{status}}")],
        15,
        22,
        w=9,
        h=6,
        unit="reqps",
    ),
    row("Batch pipeline (Pushgateway)", 28),
    {
        "id": _next(),
        "type": "bargauge",
        "title": "Last run duration by stage",
        "datasource": DS,
        "gridPos": {"x": 0, "y": 29, "w": 12, "h": 9},
        "targets": [
            target(
                "max by (stage, env) (recsys_pipeline_stage_duration_seconds)",
                "{{stage}} ({{env}})",
                instant=True,
            )
        ],
        "fieldConfig": {
            "defaults": {"unit": "s", "color": {"mode": "fixed", "fixedColor": SERIES[0]}},
            "overrides": [],
        },
        "options": {
            "orientation": "horizontal",
            "displayMode": "basic",
            "reduceOptions": {"calcs": ["lastNotNull"]},
        },
    },
    {
        "id": _next(),
        "type": "table",
        "title": "Rows processed (last run)",
        "datasource": DS,
        "gridPos": {"x": 12, "y": 29, "w": 12, "h": 9},
        "targets": [
            target(
                "max by (stage, env, table) (recsys_pipeline_rows)", "", instant=True, fmt="table"
            )
        ],
        "fieldConfig": {"defaults": {}, "overrides": []},
        "transformations": [
            {
                "id": "organize",
                "options": {"excludeByName": {"Time": True}, "renameByName": {"Value": "rows"}},
            }
        ],
        "options": {"showHeader": True, "sortBy": [{"displayName": "rows", "desc": True}]},
    },
    stat(
        "Failed error-level checks",
        "sum(recsys_validation_failed_checks{severity='error'}) or vector(0)",
        0,
        38,
        w=6,
        steps=[{"color": GOOD, "value": None}, {"color": CRIT, "value": 1}],
        decimals=0,
    ),
    stat(
        "Failed warn-level checks",
        "sum(recsys_validation_failed_checks{severity='warn'}) or vector(0)",
        6,
        38,
        w=6,
        steps=[{"color": GOOD, "value": None}, {"color": WARN, "value": 1}],
        decimals=0,
    ),
    stat(
        "Since last serving_load",
        "time() - max(recsys_pipeline_last_success_timestamp_seconds{stage='serving_load'})",
        12,
        38,
        w=6,
        unit="s",
        steps=[
            {"color": GOOD, "value": None},
            {"color": WARN, "value": 86400},
            {"color": CRIT, "value": 129600},
        ],
    ),
    stat("Rows validated (all tables)", "sum(recsys_validation_rows)", 18, 38, w=6, decimals=0),
]

dashboard = {
    "uid": "recsys",
    "title": "recsys: serving & pipeline",
    "tags": ["recsys"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "10s",
    "time": {"from": "now-30m", "to": "now"},
    "panels": panels,
    "templating": {"list": []},
    "annotations": {"list": []},
    "editable": True,
}
out = Path(__file__).parent / "dashboards" / "recsys.json"
out.write_text(json.dumps(dashboard, indent=2) + "\n")
print(f"wrote {out} ({len(panels)} panels)")

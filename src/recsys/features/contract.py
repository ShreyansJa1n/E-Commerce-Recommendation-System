"""Feature contract: the single source of truth for gold table schemas.

The pipeline passes every gold table through ``conform`` (fails on a missing column or a
type mismatch, fixes column order), and ``docs/feature_contract.md`` is rendered from
these specs (``make contract``; a test fails if the doc is stale).
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame

from recsys.config import Config, FeatureConfig
from recsys.features.split import cutoffs


@dataclass(frozen=True)
class Spec:
    name: str
    dtype: str  # Spark simpleString
    window: str
    nulls: str
    description: str


NEVER = "never null"


def _key(name: str, dtype: str, description: str) -> Spec:
    return Spec(name, dtype, "-", NEVER, description)


CUTOFF_KEYS = [
    _key("cutoff_date", "date", "Snapshot time T (UTC midnight). Partition column."),
    _key("split", "string", "train / val / test."),
]


def user_feature_specs(fc: FeatureConfig) -> list[Spec]:
    aw = max(fc.windows_days)
    specs = [
        *CUTOFF_KEYS,
        _key("visitor_id", "int", "Visitor. Rows exist only for visitors with an event before T."),
        Spec(
            "days_since_first_event", "double", "all history", NEVER, "Days from first event to T."
        ),
        Spec("days_since_last_event", "double", "all history", NEVER, "Days from last event to T."),
        *[
            Spec(
                f"days_since_last_{e}",
                "double",
                "all history",
                f"null if the visitor never had a {e}",
                f"Days from last {e} to T.",
            )
            for e in ("view", "addtocart", "transaction")
        ],
        Spec("n_events_total", "bigint", "all history", NEVER, "Events before T."),
        Spec("n_sessions_total", "bigint", "all history", NEVER, "Sessions before T."),
    ]
    for w in fc.windows_days:
        win = f"{w}d"
        specs += [
            Spec(f"n_views_{win}", "bigint", win, "0 if none", "View events."),
            Spec(f"n_addtocart_{win}", "bigint", win, "0 if none", "Add-to-cart events."),
            Spec(f"n_transactions_{win}", "bigint", win, "0 if none", "Transaction events."),
            Spec(
                f"n_sessions_{win}",
                "bigint",
                win,
                "0 if none",
                "Sessions with an event in the window.",
            ),
            Spec(
                f"n_distinct_items_{win}",
                "bigint",
                win,
                "0 if none",
                "Distinct items interacted with.",
            ),
        ]
    for w in fc.windows_days:
        win = f"{w}d"
        specs += [
            Spec(
                f"view_to_cart_rate_{win}",
                "double",
                win,
                f"null if n_views_{win} = 0",
                "add-to-carts / views, clipped to [0, 1].",
            ),
            Spec(
                f"cart_to_purchase_rate_{win}",
                "double",
                win,
                f"null if n_addtocart_{win} = 0",
                "transactions / add-to-carts, clipped to [0, 1].",
            ),
        ]
    specs += [
        Spec(
            f"n_categories_{aw}d",
            "bigint",
            f"{aw}d",
            "0 if none",
            "Distinct categories (as of each event's time) interacted with.",
        ),
        Spec(
            f"top_category_id_{aw}d",
            "int",
            f"{aw}d",
            f"null if n_categories_{aw}d = 0",
            "Category with the highest affinity score (ties: lowest id).",
        ),
        Spec(
            f"top_category_share_{aw}d",
            "double",
            f"{aw}d",
            f"null if n_categories_{aw}d = 0",
            "Top category's share of the visitor's total affinity score.",
        ),
    ]
    return specs


def affinity_specs(fc: FeatureConfig) -> list[Spec]:
    aw = f"{max(fc.windows_days)}d"
    w = ", ".join(f"{k}={v:g}" for k, v in fc.event_weights.items())
    return [
        *CUTOFF_KEYS,
        _key("visitor_id", "int", "Visitor."),
        _key("category_id", "int", "Category of the item at event time (point-in-time)."),
        Spec("affinity_score", "double", aw, NEVER, f"Sum of event weights ({w}). Always > 0."),
        Spec("affinity_share", "double", aw, NEVER, "affinity_score / visitor's total score."),
    ]


def item_feature_specs(fc: FeatureConfig) -> list[Spec]:
    aw = max(fc.windows_days)
    cw = f"{fc.cooccurrence_window_days}d"
    no_catalog = "null if the item has no catalog version valid at T"
    specs = [
        *CUTOFF_KEYS,
        _key("item_id", "int", "Item with an event before T or a catalog version valid at T."),
        Spec("category_id", "int", "as of T", no_catalog, "categoryid valid at T."),
        Spec(
            "root_category_id",
            "int",
            "as of T",
            f"{no_catalog}, or category not in tree",
            "Root of category_id.",
        ),
        Spec(
            "category_level",
            "int",
            "as of T",
            f"{no_catalog}, or category not in tree",
            "Depth of category_id (root = 0).",
        ),
        Spec("available", "int", "as of T", no_catalog, "Availability flag (0/1) valid at T."),
        Spec(
            "days_since_first_event",
            "double",
            "all history",
            "null if no events before T",
            "Days from first event to T.",
        ),
        Spec(
            "days_since_last_event",
            "double",
            "all history",
            "null if no events before T",
            "Days from last event to T.",
        ),
    ]
    for w in fc.windows_days:
        win = f"{w}d"
        specs += [
            Spec(f"n_views_{win}", "bigint", win, "0 if none", "View events."),
            Spec(f"n_addtocart_{win}", "bigint", win, "0 if none", "Add-to-cart events."),
            Spec(f"n_transactions_{win}", "bigint", win, "0 if none", "Transaction events."),
            Spec(f"n_visitors_{win}", "bigint", win, "0 if none", "Distinct visitors."),
        ]
    specs += [
        Spec(
            f"coview_neighbors_{cw}",
            "bigint",
            cw,
            "0 if none",
            f"Distinct items viewed in the same session (sessions of 2..{fc.max_session_items_for_pairs} items).",
        ),
        Spec(
            "copurchase_neighbors_total",
            "bigint",
            "all history",
            "0 if none",
            "Distinct items bought in the same transaction.",
        ),
    ]
    for w in fc.windows_days:
        specs.append(
            Spec(
                f"popularity_rank_{w}d",
                "int",
                f"{w}d",
                "null if no weighted events in the window",
                "Dense rank by weighted event score (1 = most popular).",
            )
        )
    a = f"{fc.conversion_prior_views:g}"
    specs += [
        Spec(
            f"cart_rate_{aw}d",
            "double",
            f"{aw}d",
            NEVER,
            f"(carts + {a}·global_rate) / (views + {a}), clipped to [0, 1].",
        ),
        Spec(
            f"purchase_rate_{aw}d",
            "double",
            f"{aw}d",
            NEVER,
            f"(transactions + {a}·global_rate) / (views + {a}), clipped to [0, 1].",
        ),
    ]
    return specs


def label_specs(fc: FeatureConfig) -> list[Spec]:
    grades = ", ".join(f"{k}={v}" for k, v in fc.relevance.items())
    win = "label window"
    return [
        *CUTOFF_KEYS,
        _key("visitor_id", "int", "Visitor with an event in [T, label_end)."),
        _key("item_id", "int", "Item interacted with in the label window."),
        Spec("relevance", "int", win, NEVER, f"Max grade of the label-window events ({grades})."),
        Spec("n_label_events", "bigint", win, NEVER, "Events on this pair in the window."),
        Spec("first_label_ts", "timestamp", win, NEVER, "First event on this pair in the window."),
        Spec("viewed", "boolean", win, NEVER, "Any view in the window."),
        Spec("carted", "boolean", win, NEVER, "Any add-to-cart in the window."),
        Spec("purchased", "boolean", win, NEVER, "Any transaction in the window."),
        Spec(
            "is_repeat",
            "boolean",
            "all history",
            NEVER,
            "Visitor interacted with this item before T.",
        ),
        Spec(
            "visitor_has_history",
            "boolean",
            "all history",
            NEVER,
            "Visitor has any event before T (false = cold start).",
        ),
    ]


def table_specs(fc: FeatureConfig) -> dict[str, list[Spec]]:
    return {
        "user_features": user_feature_specs(fc),
        "user_category_affinity": affinity_specs(fc),
        "item_features": item_feature_specs(fc),
        "labels": label_specs(fc),
    }


class ContractError(ValueError):
    pass


def conform(df: DataFrame, specs: list[Spec], table: str) -> DataFrame:
    """Select columns in contract order; fail on missing, extra, or mistyped columns."""
    actual = {f.name: f.dataType.simpleString() for f in df.schema}
    expected = {s.name: s.dtype for s in specs}
    problems = [f"missing {n}" for n in expected if n not in actual]
    problems += [f"unexpected {n}" for n in actual if n not in expected]
    problems += [
        f"{n}: {actual[n]} != {t}" for n, t in expected.items() if n in actual and actual[n] != t
    ]
    if problems:
        raise ContractError(f"{table}: " + "; ".join(problems))
    return df.select(*[s.name for s in specs])


TABLE_NOTES = {
    "user_features": "One row per (cutoff, visitor) for visitors with at least one event before T.",
    "user_category_affinity": "One row per (cutoff, visitor, category) with a positive score.",
    "item_features": "One row per (cutoff, item) for items with an event before T or a catalog version valid at T.",
    "labels": "One row per (cutoff, visitor, item) with an interaction in the label window. Not a feature: never join it into model inputs.",
}


def render_markdown(cfg: Config) -> str:
    fc = cfg.features
    lines = [
        "# Feature contract",
        "",
        "<!-- Generated by `make contract` from src/recsys/features/contract.py. Do not edit. -->",
        "",
        "## Point-in-time rules",
        "",
        "- Every gold table is keyed by a cutoff T (`cutoff_date`, UTC midnight).",
        "- Features for T use only events with `event_ts < T`. Labels use `[T, label_end)`.",
        "- Event categories come from the catalog value valid at each event's own time"
        ' (`item_properties_scd`). Item catalog features ("as of T") use the value valid just'
        " before T, so a version first observed exactly at T is not visible (ADR-005).",
        f"- Windows are trailing: a `Nd` feature covers `[T - N days, T)`. Sessions break after"
        f" {fc.session_gap_minutes} minutes of inactivity.",
        "- Leakage is tested by `tests/test_leakage.py`: adding events at or after T, or catalog"
        " changes after T, must not change any feature row.",
        "",
        "## Cutoffs",
        "",
        "| split | cutoff T | label window |",
        "|---|---|---|",
    ]
    for c in cutoffs(cfg.split):
        lines.append(f"| {c.split} | {c.cutoff_date} | [{c.cutoff_date}, {c.label_end_date}) |")
    for table, specs in table_specs(fc).items():
        lines += [
            "",
            f"## `gold/{table}`",
            "",
            TABLE_NOTES[table],
            "",
            "| name | type | window | nulls | description |",
            "|---|---|---|---|---|",
        ]
        lines += [
            f"| `{s.name}` | {s.dtype} | {s.window} | {s.nulls} | {s.description} |" for s in specs
        ]
    return "\n".join(lines) + "\n"

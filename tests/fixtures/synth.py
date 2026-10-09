"""Synthetic Retailrocket-shaped raw data for tests and CI (ADR-003).

Generates CSVs with the real column names and value formats, plus a known set of
anomalies so tests can assert exact counts.
"""

from __future__ import annotations

import csv
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

START = datetime(2015, 5, 3, tzinfo=UTC)
FIRST_SNAPSHOT = datetime(2015, 5, 10, 3, tzinfo=UTC)  # a Sunday, like the real data
EVENT_TYPES = ("view", "addtocart", "transaction")


def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


@dataclass
class SynthStats:
    valid_events: int
    duplicate_events: int
    rejected: dict[str, int]
    items: int
    categories: int


def generate(
    out_dir: Path,
    n_visitors: int = 50,
    n_items: int = 30,
    n_events: int = 400,
    n_weeks: int = 4,
    seed: int = 7,
) -> SynthStats:
    rng = random.Random(seed)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Category tree: 3 roots, each with 2 children, each with 2 grandchildren.
    tree: list[tuple[int, int | None]] = []
    leaves: list[int] = []
    cid = 100
    for _ in range(3):
        root, cid = cid, cid + 1
        tree.append((root, None))
        for _ in range(2):
            child, cid = cid, cid + 1
            tree.append((child, root))
            for _ in range(2):
                leaf, cid = cid, cid + 1
                tree.append((leaf, child))
                leaves.append(leaf)
    with (out_dir / "category_tree.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["categoryid", "parentid"])
        w.writerows((c, "" if p is None else p) for c, p in tree)

    # Weekly property snapshots; each item changes category at most once.
    props: list[tuple[int, int, str, str]] = []
    for item in range(1, n_items + 1):
        cat = rng.choice(leaves)
        change_week = rng.randrange(1, n_weeks) if item % 3 == 0 else None
        for week in range(n_weeks):
            ts = ms(FIRST_SNAPSHOT + timedelta(weeks=week))
            if week == change_week:
                cat = rng.choice([c for c in leaves if c != cat])
            props.append((ts, item, "categoryid", str(cat)))
            props.append((ts, item, "available", str(int(rng.random() > 0.2))))
            if week == 0:
                props.append((ts, item, "888", f"{rng.randrange(10**6)} n{rng.randrange(999)}.000"))
    rng.shuffle(props)
    half = len(props) // 2
    for name, chunk in (
        ("item_properties_part1.csv", props[:half]),
        ("item_properties_part2.csv", props[half:]),
    ):
        with (out_dir / name).open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["timestamp", "itemid", "property", "value"])
            w.writerows(chunk)

    # Events: valid rows, then exact duplicates, then one row per rejection reason.
    rows: list[list[str]] = []
    txn_id = 1
    span_ms = int(timedelta(weeks=n_weeks).total_seconds() * 1000)
    seen_keys: set[tuple[str, ...]] = set()
    while len(rows) < n_events:
        ts = ms(START) + rng.randrange(span_ms)
        event = rng.choices(EVENT_TYPES, weights=(90, 7, 3))[0]
        row = [
            str(ts),
            str(rng.randrange(1, n_visitors + 1)),
            event,
            str(rng.randrange(1, n_items + 1)),
        ]
        key = tuple(row)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        if event == "transaction":
            row.append(str(txn_id))
            txn_id += 1
        else:
            row.append("")
        rows.append(row)
    duplicates = [list(r) for r in rows[:5]]
    bad = {
        "null_key": [str(ms(START)), "", "view", "3", ""],
        "invalid_event_type": [str(ms(START)), "1", "click", "3", ""],
        "ts_out_of_range": [str(ms(datetime(2014, 1, 1, tzinfo=UTC))), "1", "view", "3", ""],
        "transaction_id_mismatch": [str(ms(START)), "1", "transaction", "3", ""],
    }
    malformed_ts = ["not-a-number", "2", "view", "4", ""]  # parses to null -> null_key
    all_rows = rows + duplicates + list(bad.values()) + [malformed_ts]
    rng.shuffle(all_rows)
    with (out_dir / "events.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "visitorid", "event", "itemid", "transactionid"])
        w.writerows(all_rows)

    rejected = {k: 1 for k in bad}
    rejected["null_key"] += 1
    return SynthStats(
        valid_events=len(rows),
        duplicate_events=len(duplicates),
        rejected=rejected,
        items=n_items,
        categories=len(tree),
    )

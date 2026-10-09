"""Point-in-time correctness: nothing at or after the cutoff T may change a feature.

Strategy: compute every feature table at T on the real (synthetic) history, then add
"future" data -- new events at or after T (including exactly T, for existing and brand-new
visitors and items, and inside sessions that started before T) and catalog changes at or
after T -- and recompute from scratch, including sessionization and point-in-time
enrichment. Every feature row must be identical.
"""

import random
from datetime import UTC, date, datetime, timedelta

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from recsys.clean import catalog, events
from recsys.config import Config
from recsys.features import item as item_mod
from recsys.features import user as user_mod
from recsys.features.events import enrich_events
from recsys.features.pipeline import compute_snapshot
from recsys.features.split import Cutoff
from recsys.ingest import raw_to_bronze
from recsys.io import read_table
from tests.feature_helpers import to_ms
from tests.fixtures.synth import SynthStats

CUT = Cutoff("val", date(2015, 5, 20), date(2015, 5, 27))
T = datetime(2015, 5, 20)
FEATURE_TABLES = ("user_features", "user_category_affinity", "item_features")


@pytest.fixture(scope="module")
def silver(
    spark: SparkSession, synth_env: tuple[Config, SynthStats]
) -> tuple[Config, dict[str, DataFrame]]:
    cfg, _ = synth_env
    raw_to_bronze.run(spark, cfg)
    events.run(spark, cfg)
    catalog.run(spark, cfg)
    p = cfg.paths.resolved().silver
    tables = {
        n: read_table(spark, p / n).cache() for n in ("events", "item_properties_scd", "categories")
    }
    return cfg, tables


def _snapshot(cfg: Config, ev: DataFrame, scd: DataFrame, cats: DataFrame) -> dict[str, DataFrame]:
    enriched = enrich_events(ev, scd, cfg.features.session_gap_minutes)
    snap = compute_snapshot(enriched, scd, cats, CUT, cfg.features)
    return {n: snap[n].cache() for n in FEATURE_TABLES}


def _future_events(spark: SparkSession, ev: DataFrame) -> DataFrame:
    rng = random.Random(11)
    visitors = [r.visitor_id for r in ev.select("visitor_id").distinct().collect()] + [9001, 9002]
    items = [r.item_id for r in ev.select("item_id").distinct().collect()] + [8001]
    # Continue a pre-T session: last pre-T event per visitor, +1 minute past T if close to T.
    last_before = ev.where(F.col("event_ts") < F.lit(T)).agg(F.max("event_ts")).collect()[0][0]
    stamps = [T, T, T + timedelta(milliseconds=1), last_before + timedelta(minutes=5)]
    stamps += [T + timedelta(hours=rng.randrange(1, 24 * 10)) for _ in range(150)]
    rows = []
    for n, ts in enumerate(stamps):
        ts = max(ts, T)
        event = rng.choice(["view", "view", "addtocart", "transaction"])
        rows.append(
            (
                ts,
                to_ms(ts),
                rng.choice(visitors),
                rng.choice(items),
                event,
                90_000 + n // 2 if event == "transaction" else None,
                ts.date(),
            )
        )
    return spark.createDataFrame(rows, ev.schema)


def _future_catalog(spark: SparkSession, scd: DataFrame) -> DataFrame:
    """Close current versions of some items at T (or later) and open new ones."""
    changes = {i: T + timedelta(days=d) for i, d in ((3, 0), (6, 2), (9, 5))}
    rows = []
    for r in scd.collect():
        d = r.asDict()
        if r.item_id in changes and r.is_current and r.property in ("categoryid", "available"):
            at = changes[r.item_id]
            rows.append({**d, "valid_to": at, "is_current": False})
            new_val = "0" if r.property == "available" else "999"
            rows.append(
                {
                    **d,
                    "value": new_val,
                    "valid_from": at,
                    "first_seen_ts": at,
                    "valid_to": None,
                    "is_current": True,
                    "is_backfilled": False,
                }
            )
        else:
            rows.append(d)
    rows.append(
        {
            "item_id": 8001,
            "property": "categoryid",
            "value": "101",
            "valid_from": T,
            "valid_to": None,
            "first_seen_ts": T,
            "is_current": True,
            "is_backfilled": False,
        }
    )
    return spark.createDataFrame(rows, scd.schema)


def _diff(a: DataFrame, b: DataFrame) -> int:
    return a.exceptAll(b).count() + b.exceptAll(a).count()


def test_future_data_does_not_change_features(spark: SparkSession, silver) -> None:
    cfg, t = silver
    base = _snapshot(cfg, t["events"], t["item_properties_scd"], t["categories"])
    future_ev = _future_events(spark, t["events"])
    assert future_ev.where(F.col("event_ts") == F.lit(T)).count() >= 2
    perturbed = _snapshot(
        cfg,
        t["events"].unionByName(future_ev),
        _future_catalog(spark, t["item_properties_scd"]),
        t["categories"],
    )
    for name in FEATURE_TABLES:
        assert base[name].count() > 0, name
        assert _diff(base[name], perturbed[name]) == 0, (
            f"{name} changed when only future data was added"
        )


def test_leakage_check_detects_lookahead(spark: SparkSession, silver, monkeypatch) -> None:
    """Sanity check of the test itself: a one-day lookahead in history must be caught."""
    cfg, t = silver

    def leaky_history(ev: DataFrame, cut: Cutoff) -> DataFrame:
        return ev.where(F.col("event_ts") < F.lit(cut.ts + timedelta(days=1)))

    monkeypatch.setattr(user_mod, "history", leaky_history)
    monkeypatch.setattr(item_mod, "history", leaky_history)
    base = _snapshot(cfg, t["events"], t["item_properties_scd"], t["categories"])
    perturbed = _snapshot(
        cfg,
        t["events"].unionByName(_future_events(spark, t["events"])),
        t["item_properties_scd"],
        t["categories"],
    )
    assert _diff(base["user_features"], perturbed["user_features"]) > 0


def test_cutoff_is_utc_midnight() -> None:
    assert CUT.ts == datetime(2015, 5, 20, tzinfo=UTC)

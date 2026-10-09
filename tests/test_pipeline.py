"""End-to-end raw -> bronze -> silver -> gold on synthetic data."""

import json

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from recsys.candidates import pipeline as candidates
from recsys.clean import catalog, events
from recsys.config import Config
from recsys.embeddings import pipeline as embeddings
from recsys.features import contract
from recsys.features import pipeline as features
from recsys.ingest import raw_to_bronze, sample
from recsys.io import read_table
from tests.conftest import make_config
from tests.fixtures.synth import SynthStats


@pytest.fixture(scope="module")
def built(spark: SparkSession, synth_env: tuple[Config, SynthStats]) -> tuple[Config, SynthStats]:
    cfg, stats = synth_env
    raw_to_bronze.run(spark, cfg)
    events.run(spark, cfg)
    catalog.run(spark, cfg)
    features.run(spark, cfg)
    embeddings.run(spark, cfg)
    candidates.run(spark, cfg)
    return cfg, stats


def test_event_counts_reconcile(spark: SparkSession, built: tuple[Config, SynthStats]) -> None:
    cfg, stats = built
    p = cfg.paths.resolved()
    report = json.loads((p.silver / "_reports" / "silver_events.json").read_text())
    assert report["rows"]["silver"] == stats.valid_events
    assert report["rows"]["duplicates_dropped"] == stats.duplicate_events
    assert report["extra"]["rejected_by_reason"] == stats.rejected
    assert report["rows"]["bronze"] == (
        stats.valid_events + stats.duplicate_events + sum(stats.rejected.values())
    )


def test_silver_is_partitioned_by_date(built: tuple[Config, SynthStats]) -> None:
    cfg, _ = built
    parts = sorted(d.name for d in (cfg.paths.resolved().silver / "events").glob("event_date=*"))
    assert parts and all(p.startswith("event_date=2015-") for p in parts)


def test_validation_reports_written_and_pass(built: tuple[Config, SynthStats]) -> None:
    cfg, _ = built
    vdir = cfg.paths.resolved().silver / "_validation"
    names = sorted(p.stem for p in vdir.glob("*.json"))
    assert names == [
        "silver_catalog_latest",
        "silver_categories",
        "silver_events",
        "silver_item_properties_scd",
        "silver_item_properties_scd_current",
    ]
    assert all(json.loads((vdir / f"{n}.json").read_text())["passed"] for n in names)


def test_catalog_tables(spark: SparkSession, built: tuple[Config, SynthStats]) -> None:
    cfg, stats = built
    silver = cfg.paths.resolved().silver
    latest = read_table(spark, silver / "catalog_latest")
    assert latest.count() == stats.items
    assert latest.where(F.col("category_level") != 2).count() == 0  # synth assigns leaves
    assert read_table(spark, silver / "categories").count() == stats.categories
    scd = read_table(spark, silver / "item_properties_scd")
    # Items 3, 6, 9, ... change category once -> two versions.
    versions = scd.where(F.col("property") == "categoryid").groupBy("item_id").count()
    assert versions.where(F.col("count") > 2).count() == 0
    assert versions.where(F.col("count") == 2).count() > 0


def test_rerun_is_idempotent(spark: SparkSession, built: tuple[Config, SynthStats]) -> None:
    cfg, _ = built
    silver = cfg.paths.resolved().silver
    before = {t: read_table(spark, silver / t).count() for t in ("events", "item_properties_scd")}
    files_before = sorted(p.name for p in (silver / "events").glob("event_date=*"))
    raw_to_bronze.run(spark, cfg)
    events.run(spark, cfg)
    catalog.run(spark, cfg)
    after = {t: read_table(spark, silver / t).count() for t in ("events", "item_properties_scd")}
    assert before == after
    assert files_before == sorted(p.name for p in (silver / "events").glob("event_date=*"))


def test_sample_is_deterministic_and_ingestible(
    spark: SparkSession,
    synth_env: tuple[Config, SynthStats],
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    source, _ = synth_env
    source = source.model_copy(
        update={"sample": source.sample.model_copy(update={"visitor_fraction": 0.5})}
    )
    a = make_config(tmp_path_factory.mktemp("sample_a"))
    b = make_config(tmp_path_factory.mktemp("sample_b"))
    sample.run(spark, source, a)
    sample.run(spark, source, b)
    for name in (
        "events.csv",
        "item_properties_part1.csv",
        "item_properties_part2.csv",
        "category_tree.csv",
    ):
        assert (a.paths.raw / name).read_text() == (b.paths.raw / name).read_text()
    n_sampled = len((a.paths.raw / "events.csv").read_text().splitlines()) - 1
    n_full = len((source.paths.raw / "events.csv").read_text().splitlines()) - 1
    assert 0 < n_sampled < n_full
    # The sample must round-trip through ingest with every column in place.
    raw_to_bronze.run(spark, a)
    full = read_table(spark, source.paths.resolved().bronze / "item_properties")
    sampled = read_table(spark, a.paths.resolved().bronze / "item_properties")
    assert sampled.count() > 0
    assert sampled.where(F.col("item_id").isNull() | F.col("snapshot_ts").isNull()).count() == 0
    assert sampled.drop("_source_file").exceptAll(full.drop("_source_file")).count() == 0


def test_gold_tables_match_contract(spark: SparkSession, built: tuple[Config, SynthStats]) -> None:
    cfg, _ = built
    gold = cfg.paths.resolved().gold
    for name, specs in contract.table_specs(cfg.features).items():
        df = read_table(spark, gold / name)
        # Partition column comes back last on read; compare as name -> type.
        got = {f.name: f.dataType.simpleString() for f in df.schema}
        assert got == {s.name: s.dtype for s in specs}, name
        cutoffs = {r.cutoff_date.isoformat() for r in df.select("cutoff_date").distinct().collect()}
        assert cutoffs <= {"2015-05-17", "2015-05-24", "2015-05-27"}, name
    vdir = gold / "_validation"
    for name in contract.table_specs(cfg.features):
        assert json.loads((vdir / f"gold_{name}.json").read_text())["passed"], name


def test_gold_split_is_time_ordered(spark: SparkSession, built: tuple[Config, SynthStats]) -> None:
    cfg, _ = built
    gold = cfg.paths.resolved().gold
    cuts = {r.split: r for r in read_table(spark, gold / "cutoffs").collect()}
    labels = read_table(spark, gold / "labels")
    for split, cut in cuts.items():
        window = (
            labels.where(F.col("split") == split)
            .agg(F.min("first_label_ts").alias("lo"), F.max("first_label_ts").alias("hi"))
            .collect()[0]
        )
        assert window.lo is not None, split
        assert cut.cutoff_ts <= window.lo and window.hi < cut.label_end_ts, split
    assert cuts["train"].label_end_ts <= cuts["val"].cutoff_ts
    assert cuts["val"].label_end_ts <= cuts["test"].cutoff_ts


def test_gold_features_only_see_history(
    spark: SparkSession, built: tuple[Config, SynthStats]
) -> None:
    cfg, _ = built
    gold = cfg.paths.resolved().gold
    users = read_table(spark, gold / "user_features")
    # Recency is measured back from T, so it can never be negative.
    assert users.where(F.col("days_since_last_event") < 0).count() == 0
    assert (
        read_table(spark, gold / "item_features").where(F.col("days_since_last_event") < 0).count()
        == 0
    )
    report = json.loads((gold / "_reports" / "gold_features.json").read_text())
    assert [r["split"] for r in report["extra"]["labels"]] == ["train", "val", "test"]
    assert 0 < report["extra"]["events_with_category_share"] <= 1


def test_candidates_written_per_cutoff_and_source(
    spark: SparkSession, built: tuple[Config, SynthStats]
) -> None:
    cfg, _ = built
    gold = cfg.paths.resolved().gold
    cands = read_table(spark, gold / "candidates")
    assert set(cands.columns) == {
        "visitor_id",
        "item_id",
        "score",
        "rank",
        "split",
        "cutoff_date",
        "source",
    }
    sources = {r.source for r in cands.select("source").distinct().collect()}
    # Co-occurrence can be empty on tiny synthetic data (no pair repeats across sessions);
    # the run report must still account for every source at every cutoff.
    # item2vec too: synthetic sessions are short and min_count may leave no vocabulary.
    sparse = {"cooccurrence", "item2vec"}
    assert set(candidates.SOURCES) - sparse <= sources <= set(candidates.SOURCES)
    report = json.loads((gold / "_reports" / "candidates.json").read_text())
    expected_keys = {
        f"{c}/{s}" for c in ("2015-05-17", "2015-05-24", "2015-05-27") for s in candidates.SOURCES
    }
    assert set(report["rows"]) == expected_keys
    n = cfg.candidates.top_n
    assert cands.where((F.col("rank") < 1) | (F.col("rank") > n)).count() == 0
    key = ["cutoff_date", "source", "visitor_id", "item_id"]
    assert cands.groupBy(*key).count().where(F.col("count") > 1).count() == 0
    # Only visitors from that cutoff's label window get candidates.
    labels = read_table(spark, gold / "labels").select("cutoff_date", "visitor_id").distinct()
    assert cands.join(labels, ["cutoff_date", "visitor_id"], "left_anti").count() == 0
    metrics = json.loads((gold / "_reports" / "candidates_val_metrics.json").read_text())
    assert {m["source"] for m in metrics} == {*sources, "union"}
    assert all(m["split"] == "val" for m in metrics)


def test_embeddings_written_per_cutoff(
    spark: SparkSession, built: tuple[Config, SynthStats]
) -> None:
    cfg, _ = built
    gold = cfg.paths.resolved().gold
    report = json.loads((gold / "_reports" / "embeddings.json").read_text())
    assert [c["cutoff_date"] for c in report["extra"]["per_cutoff"]] == [
        "2015-05-17",
        "2015-05-24",
        "2015-05-27",
    ]
    emb = read_table(spark, gold / "item_embeddings")
    if emb.count():
        sizes = {r.n for r in emb.select(F.size("vector").alias("n")).distinct().collect()}
        assert sizes == {cfg.embeddings.vector_size}


def test_rebuilding_a_source_subset_keeps_the_others(
    spark: SparkSession, built: tuple[Config, SynthStats]
) -> None:
    cfg, _ = built
    gold = cfg.paths.resolved().gold
    before = read_table(spark, gold / "candidates").where(F.col("source") != "recent_items")
    before_rows = sorted(
        map(
            tuple, before.select("cutoff_date", "source", "visitor_id", "item_id", "rank").collect()
        )
    )
    subset = cfg.model_copy(
        update={"candidates": cfg.candidates.model_copy(update={"sources": ["recent_items"]})}
    )
    candidates.run(spark, subset)
    after = read_table(spark, gold / "candidates")
    assert {r.source for r in after.select("source").distinct().collect()} >= {
        "recent_items",
        "als",
    }
    after_rows = sorted(
        map(
            tuple,
            after.where(F.col("source") != "recent_items")
            .select("cutoff_date", "source", "visitor_id", "item_id", "rank")
            .collect(),
        )
    )
    assert after_rows == before_rows
    ablation = json.loads((gold / "_reports" / "candidates_val_ablation.json").read_text())
    assert ablation and all(
        r["union_recall_without"] <= r["union_recall"] + 1e-12 for r in ablation
    )

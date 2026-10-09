from datetime import date, datetime

import lightgbm as lgb
import numpy as np
import pandas as pd
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from recsys.features.split import Cutoff
from recsys.ranking import baselines, dataset, model
from tests.feature_helpers import enriched

CUT = Cutoff("train", date(2015, 6, 30), date(2015, 7, 14))
CAND_DDL = (
    "cutoff_date date, split string, source string, visitor_id int, item_id int,"
    " score double, rank int"
)
W = {"view": 1.0, "addtocart": 3.0, "transaction": 5.0}


def _cands(spark: SparkSession, rows: list[tuple[str, int, int, float, int]]):
    return spark.createDataFrame([(CUT.cutoff_date, "train", *r) for r in rows], CAND_DDL)


def test_source_features_pivot_and_dedupe(spark: SparkSession) -> None:
    c = _cands(spark, [("a", 1, 10, 0.9, 1), ("b", 1, 10, 0.4, 3), ("b", 1, 11, 0.5, 2)])
    rows = {r.item_id: r for r in dataset.source_features(c, ["a", "b"]).collect()}
    assert set(rows) == {10, 11}  # one row per (visitor, item)
    assert (rows[10].src_a_rank, rows[10].src_b_rank, rows[10].src_n_sources) == (1, 3, 2)
    assert rows[11].src_a_rank is None and rows[11].src_b_score == 0.5


def test_user_item_history_ignores_future(spark: SparkSession) -> None:
    ev = enriched(
        spark,
        [
            (datetime(2015, 6, 20), 1, 10, "view", None, "s", 1),
            (datetime(2015, 6, 29), 1, 10, "addtocart", None, "s", 1),
            (datetime(2015, 6, 30), 1, 10, "transaction", 9, "t", 1),  # at T: excluded
            (datetime(2015, 6, 25), 1, 99, "view", None, "s", 1),  # not a candidate pair
        ],
    )
    pairs = spark.createDataFrame([(1, 10), (1, 11)], "visitor_id int, item_id int")
    (r,) = dataset.user_item_history(ev, pairs, CUT, W).collect()
    assert (r.item_id, r.ui_n_events, r.ui_n_transactions, r.ui_weighted_events) == (10, 2, 0, 4.0)
    assert r.ui_days_since_last == pytest.approx(1.0)


def test_sample_training_queries(spark: SparkSession) -> None:
    rows = [(CUT.cutoff_date, "train", 1, i, 1 if i in (3, 7) else 0) for i in range(50)]
    rows += [(CUT.cutoff_date, "train", 2, i, 0) for i in range(20)]  # no positive: dropped
    ex = spark.createDataFrame(
        rows, "cutoff_date date, split string, visitor_id int, item_id int, label int"
    )
    a = dataset.sample_training_queries(ex, 10, "salt")
    b = dataset.sample_training_queries(ex, 10, "salt")
    got = sorted((r.visitor_id, r.item_id, r.label) for r in a.collect())
    assert got == sorted((r.visitor_id, r.item_id, r.label) for r in b.collect())  # deterministic
    assert {v for v, _, _ in got} == {1}
    assert sum(1 for *_, lab in got if lab > 0) == 2 and len(got) == 12
    other = {
        i
        for _, i, _ in (
            tuple(r)
            for r in dataset.sample_training_queries(ex, 10, "other")
            .select("visitor_id", "item_id", "label")
            .collect()
        )
    }
    assert other != {i for _, i, _ in got}  # salt changes the negative sample


def test_feature_columns_exclude_keys_ids_and_label(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(date(2015, 1, 1), "train", 1, 2, 0, 5, 1.0, "x", 3)],
        "cutoff_date date, split string, visitor_id int, item_id int, label int,"
        " i_category_id int, u_x double, s string, src_a_rank int",
    )
    assert dataset.feature_columns(df) == ["u_x", "src_a_rank"]


def _toy(n_queries: int = 60, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    for q in range(n_queries):
        for i in range(20):
            f1 = rng.normal()
            rows.append(
                {
                    "cutoff_date": date(2015, 6, 1),
                    "split": "train",
                    "visitor_id": q,
                    "item_id": i,
                    "f1": f1,
                    "f2": rng.normal(),
                    "label": int(f1 > 1.0) + int(f1 > 1.8),
                }
            )
    return pd.DataFrame(rows)


def test_to_lgb_frame_groups_and_order() -> None:
    pdf = _toy(3).sample(frac=1.0, random_state=1)
    x, _, groups = model.to_lgb_frame(pdf, ["f1", "f2"])
    assert (
        list(groups) == [20, 20, 20] and len(x) == 60 and x.dtypes.unique().tolist() == [np.float32]
    )


def test_train_learns_signal_and_scores_in_spark(spark: SparkSession) -> None:
    params = {
        "objective": "lambdarank",
        "metric": "ndcg",
        "num_leaves": 7,
        "learning_rate": 0.1,
        "min_data_in_leaf": 5,
        "verbosity": -1,
        "deterministic": True,
        "force_row_wise": True,
        "num_threads": 1,
        "seed": 1,
    }
    ranker = model.train(_toy(60, 0), _toy(20, 1), ["f1", "f2"], params, 200, 20, 5)
    assert ranker.stats["valid_ndcg@5_best"] > 0.9
    assert ranker.importance()[0]["feature"] == "f1"
    pdf = _toy(4, 2)
    sdf = spark.createDataFrame(pdf)
    model_str = ranker.booster.model_to_string(num_iteration=ranker.best_iteration)
    scored = model.score(sdf, model_str, ["f1", "f2"], ranker.best_iteration).toPandas()
    expected = lgb.Booster(model_str=model_str).predict(pdf[["f1", "f2"]].astype("float32"))
    merged = pdf.assign(expected=expected).merge(
        scored[["visitor_id", "item_id", "ranker_score"]], on=["visitor_id", "item_id"]
    )
    assert np.allclose(merged.expected, merged.ranker_score)
    top = model.top_n(spark.createDataFrame(scored), 3).collect()
    assert len(top) == 12 and {r.rank for r in top} == {1, 2, 3}


def test_priority_blend(spark: SparkSession) -> None:
    c = _cands(
        spark,
        [
            ("recent_items", 1, 10, 1.0, 1),
            ("popular_category", 1, 20, 1.0, 1),
            ("popular_category", 1, 10, 0.5, 2),  # already taken by recent_items
            ("popular_global", 1, 30, 1.0, 1),
            ("popular_global", 2, 30, 1.0, 1),
            ("als", 1, 40, 1.0, 1),  # not in priority list
        ],
    )
    got = sorted(
        (r.visitor_id, r.rank, r.item_id)
        for r in baselines.priority_blend(
            c, ["recent_items", "popular_category", "popular_global"], 10
        ).collect()
    )
    assert got == [(1, 1, 10), (1, 2, 20), (1, 3, 30), (2, 1, 30)]
    assert (
        baselines.priority_blend(c, ["recent_items", "popular_category", "popular_global"], 2)
        .where(F.col("visitor_id") == 1)
        .count()
        == 2
    )

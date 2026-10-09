import math
import random

import pytest
from pyspark.sql import SparkSession

from recsys.eval import metrics as m


def test_reference_metrics_hand_computed() -> None:
    recs = ["a", "x", "b", "y", "c"]
    truth = {"a": 1, "b": 3, "z": 1}
    assert m.precision_at_k(recs, truth, 5) == pytest.approx(2 / 5)
    assert m.recall_at_k(recs, truth, 5) == pytest.approx(2 / 3)
    assert m.average_precision_at_k(recs, truth, 5) == pytest.approx((1 / 1 + 2 / 3) / 3)
    dcg = 1 / math.log2(2) + 7 / math.log2(4)
    idcg = 7 / math.log2(2) + 1 / math.log2(3) + 1 / math.log2(4)
    assert m.ndcg_at_k(recs, truth, 5) == pytest.approx(dcg / idcg)
    assert m.hit_at_k(recs, truth, 1) == 1.0
    assert m.hit_at_k(["x"], truth, 1) == 0.0


def test_min_relevance_filters_truth() -> None:
    recs = ["a", "b"]
    truth = {"a": 1, "b": 3}
    assert m.precision_at_k(recs, truth, 2, min_relevance=2) == pytest.approx(0.5)
    assert m.recall_at_k(recs, truth, 2, min_relevance=2) == 1.0
    assert m.ndcg_at_k(["a"], {"a": 1}, 1, min_relevance=2) == 0.0


def test_short_lists_and_perfect_ranking() -> None:
    assert m.precision_at_k(["a"], {"a": 1}, 10) == pytest.approx(0.1)
    assert m.ndcg_at_k(["b", "a"], {"a": 1, "b": 2}, 2) == pytest.approx(1.0)
    assert m.average_precision_at_k(["a", "b"], {"a": 1, "b": 1, "c": 1}, 2) == pytest.approx(1.0)


def test_spark_matches_reference_on_random_data(spark: SparkSession) -> None:
    rng = random.Random(3)
    ks = [1, 3, 10]
    recs_rows, truth_rows = [], []
    recs_by_user: dict[int, list[int]] = {}
    truth_by_user: dict[int, dict[int, int]] = {}
    for u in range(60):
        n_recs = rng.choice([0, 1, 5, 12])  # some users have no recs
        items = rng.sample(range(40), n_recs)
        recs_by_user[u] = items
        recs_rows += [(u, i, r) for r, i in enumerate(items, start=1)]
        truth = {i: rng.choice([1, 1, 2, 3]) for i in rng.sample(range(40), rng.randint(0, 6))}
        truth_by_user[u] = truth
        truth_rows += [(u, i, rel) for i, rel in truth.items()]
    recs = spark.createDataFrame(recs_rows, "visitor_id int, item_id int, rank int")
    truth = spark.createDataFrame(truth_rows, "visitor_id int, item_id int, relevance int")

    for min_rel in (1, 2):
        got = {row["k"]: row for row in m.ranking_metrics(recs, truth, ks, min_relevance=min_rel)}
        users = [u for u, t in truth_by_user.items() if any(r >= min_rel for r in t.values())]
        for k in ks:
            ref = {
                "precision": m.precision_at_k,
                "recall": m.recall_at_k,
                "ndcg": m.ndcg_at_k,
                "map": m.average_precision_at_k,
                "hit_rate": m.hit_at_k,
            }
            assert got[k]["users"] == len(users)
            for name, fn in ref.items():
                expected = sum(
                    fn(recs_by_user[u], truth_by_user[u], k, min_rel) for u in users
                ) / len(users)
                assert got[k][name] == pytest.approx(expected), (name, k, min_rel)


def test_duplicate_recs_count_once_at_best_rank(spark: SparkSession) -> None:
    recs = spark.createDataFrame([(1, 7, 1), (1, 7, 2)], "visitor_id int, item_id int, rank int")
    truth = spark.createDataFrame([(1, 7, 1)], "visitor_id int, item_id int, relevance int")
    (row,) = m.ranking_metrics(recs, truth, [2])
    assert row["precision"] == pytest.approx(0.5)
    assert row["recall"] == 1.0

import math
from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from recsys.candidates import als, cooccurrence, history, popularity
from recsys.candidates.common import top_n_per_user
from recsys.candidates.evaluate import evaluate, union_recall
from recsys.config import load_config
from recsys.features.split import Cutoff
from tests.feature_helpers import enriched

CUT = Cutoff("val", date(2015, 6, 30), date(2015, 7, 14))
CC = load_config("base").candidates
W = {"view": 1.0, "addtocart": 3.0, "transaction": 5.0}


def d(day: int, hour: int = 12) -> datetime:
    return datetime(2015, 6, day, hour)


def _targets(spark: SparkSession, ids: list[int]):
    return spark.createDataFrame([(i,) for i in ids], "visitor_id int")


def test_top_n_per_user_breaks_ties_by_item(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(1, 30, 1.0), (1, 10, 1.0), (1, 20, 2.0), (2, 5, 0.5)],
        "visitor_id int, item_id int, score double",
    )
    got = [
        (r.visitor_id, r.item_id, r.rank)
        for r in top_n_per_user(df, 2).orderBy("visitor_id", "rank").collect()
    ]
    assert got == [(1, 20, 1), (1, 10, 2), (2, 5, 1)]


def test_global_popularity_window_and_weights(spark: SparkSession) -> None:
    ev = enriched(
        spark,
        [
            (d(29), 1, 100, "view", None, "1:1", 10),
            (d(29), 2, 100, "view", None, "2:1", 10),
            (d(28), 1, 101, "transaction", 1, "1:0", 10),  # weight 5 beats 2 views
            (d(1), 1, 102, "transaction", 2, "1:x", 10),  # outside 7d window
        ],
    )
    cands = popularity.global_popularity(ev, _targets(spark, [1, 99]), CUT, 7, W, 10)
    rows = sorted((r.visitor_id, r.rank, r.item_id) for r in cands.collect())
    assert rows == [(1, 1, 101), (1, 2, 100), (99, 1, 101), (99, 2, 100)]  # cold visitor 99 too


def test_category_popularity_scores(spark: SparkSession) -> None:
    ev = enriched(
        spark,
        [
            # visitor 1 history: 3 views in cat 10, 1 view in cat 20 (same day -> same decay)
            (d(29), 1, 100, "view", None, "1:1", 10),
            (d(29), 1, 100, "view", None, "1:1", 10),
            (d(29), 1, 101, "view", None, "1:1", 10),
            (d(29), 1, 200, "view", None, "1:1", 20),
            # popularity: cat 10 -> item 100 x3 (incl. above), 101 x1; cat 20 -> 200 x1
            (d(29), 2, 100, "view", None, "2:1", 10),
        ],
    )
    cfg = CC.category.model_copy(update={"top_categories": 2})
    got = {
        r.item_id: r.score
        for r in popularity.category_popularity(
            ev, _targets(spark, [1, 3]), CUT, cfg, W, 10
        ).collect()
    }
    assert got == pytest.approx({100: 0.75 * 0.75, 101: 0.75 * 0.25, 200: 0.25 * 1.0})


def test_recent_items_decay(spark: SparkSession) -> None:
    ev = enriched(
        spark,
        [
            (datetime(2015, 6, 23), 1, 100, "view", None, "1:1", 10),  # 7 days -> 0.5
            (datetime(2015, 6, 29), 1, 101, "view", None, "1:2", 10),  # 1 day
            (datetime(2015, 6, 16), 1, 102, "transaction", 1, "1:0", 10),  # 14 days: 5 * 0.25
        ],
    )
    got = {
        r.item_id: (r.rank, r.score)
        for r in history.recent_items(ev, _targets(spark, [1]), CUT, 7, W, 10).collect()
    }
    assert got[102][0] == 1 and got[102][1] == pytest.approx(1.25)
    assert got[101][1] == pytest.approx(0.5 ** (1 / 7))
    assert got[100] == (3, pytest.approx(0.5))


@pytest.fixture
def session_events(spark: SparkSession):
    rows = []
    # sessions: {1,2} x3, {1,3} x1, {2,3} x2, and one oversized session (excluded)
    for s, items in enumerate([[1, 2], [1, 2], [1, 2], [1, 3], [2, 3], [2, 3], list(range(1, 60))]):
        rows += [(d(20), 100 + s, i, "view", None, f"s{s}", 10) for i in items]
    return enriched(spark, rows)


def test_item_neighbors_cosine(session_events) -> None:
    cfg = CC.cooccurrence.model_copy(update={"min_pair_sessions": 1, "max_session_items": 50})
    sims = {
        (r.item_id, r.neighbor_id): r.similarity
        for r in cooccurrence.item_neighbors(session_events, CUT, cfg).collect()
    }
    # sessions: item1 in 4, item2 in 5, item3 in 3
    assert sims[(1, 2)] == pytest.approx(3 / math.sqrt(4 * 5))
    assert sims[(2, 3)] == pytest.approx(2 / math.sqrt(5 * 3))
    assert sims[(1, 3)] == pytest.approx(1 / math.sqrt(4 * 3))
    assert sims[(1, 2)] == sims[(2, 1)]
    assert not any(i > 3 or j > 3 for i, j in sims)  # oversized session ignored
    strict = cfg.model_copy(update={"min_pair_sessions": 2})
    assert (1, 3) not in {
        (r.item_id, r.neighbor_id)
        for r in cooccurrence.item_neighbors(session_events, CUT, strict).collect()
    }


def test_cooccurrence_recommend_excludes_seeds(spark: SparkSession) -> None:
    seeds = spark.createDataFrame(
        [(7, 1, 2.0), (7, 2, 1.0)], "visitor_id int, item_id int, seed_weight double"
    )
    nbrs = spark.createDataFrame(
        [(1, 2, 0.9, 1), (1, 3, 0.5, 2), (2, 3, 0.4, 1), (2, 4, 0.1, 2)],
        "item_id int, neighbor_id int, similarity double, neighbor_rank int",
    )
    got = {
        r.item_id: r.score
        for r in cooccurrence.recommend(seeds, nbrs, CC.cooccurrence, 10).collect()
    }
    assert got == pytest.approx({3: 2.0 * 0.5 + 1.0 * 0.4, 4: 0.1})  # item 2 is a seed


def test_salted_join_matches_plain_join(spark: SparkSession) -> None:
    left = spark.createDataFrame(
        [(u, k) for u in range(40) for k in (1, 1, 2)], "visitor_id int, item_id int"
    )
    right = spark.createDataFrame([(1, "a"), (1, "b"), (2, "c")], "item_id int, v string")
    plain = cooccurrence.salted_join(left, right, "item_id", 0, "visitor_id")
    salted = cooccurrence.salted_join(left, right, "item_id", 8, "visitor_id")
    assert salted.columns == plain.columns
    assert plain.count() == 40 * (2 + 2 + 1)
    assert plain.exceptAll(salted).count() == 0 and salted.exceptAll(plain).count() == 0


def test_als_learns_block_structure(spark: SparkSession) -> None:
    # Users 0-19 view items 0-9, users 20-39 view items 10-19; each user skips one item.
    rows = []
    for u in range(40):
        block = range(0, 10) if u < 20 else range(10, 20)
        rows += [(d(20), u, i, "view", None, f"{u}:1", 10) for i in block if i != block[u % 10]]
    ev = enriched(spark, rows)
    cfg = CC.als.model_copy(update={"rank": 4, "max_iter": 10, "reg_param": 0.01, "alpha": 10.0})
    ratings = als.interactions(ev, cfg, W)
    assert ratings.count() == 40 * 9
    model = als.train(ratings, cfg)
    recs = als.recommend(model, _targets(spark, [0, 25, 999]), 10).collect()
    by_user: dict[int, list[int]] = {}
    for r in sorted(recs, key=lambda r: r.rank):
        by_user.setdefault(r.visitor_id, []).append(r.item_id)
    assert set(by_user) == {0, 25}  # unknown visitor 999 skipped
    assert all(i < 10 for i in by_user[0]) and all(i >= 10 for i in by_user[25])
    # ALS does not filter seen items; the unseen in-block item must still be recommended.
    assert 0 in by_user[0] and 15 in by_user[25]


def test_als_interaction_filters(spark: SparkSession) -> None:
    ev = enriched(
        spark,
        [
            (d(20), 1, 10, "view", None, "a", 1),
            (d(20), 1, 11, "view", None, "a", 1),
            (d(20), 2, 10, "addtocart", None, "b", 1),
            (d(20), 2, 11, "view", None, "b", 1),
            (d(20), 3, 10, "view", None, "c", 1),  # user with 1 item -> dropped
            (d(20), 4, 12, "view", None, "d", 1),  # item with 1 user -> dropped (then user 4 too)
            (d(20), 4, 10, "view", None, "d", 1),
        ],
    )
    got = {(r.visitor_id, r.item_id): r.strength for r in als.interactions(ev, CC.als, W).collect()}
    assert got == {(1, 10): 1.0, (1, 11): 1.0, (2, 10): 3.0, (2, 11): 1.0}


def test_union_recall_and_evaluate(spark: SparkSession) -> None:
    cands = spark.createDataFrame(
        [
            ("val", "a", 1, 10, 1.0, 1),
            ("val", "a", 1, 11, 0.5, 2),
            ("val", "b", 1, 12, 1.0, 1),
            ("val", "b", 2, 20, 1.0, 1),
        ],
        "split string, source string, visitor_id int, item_id int, score double, rank int",
    )
    labels = spark.createDataFrame(
        [
            ("val", 1, 11, 1, True),
            ("val", 1, 12, 3, True),
            ("val", 2, 21, 1, False),
            ("val", 3, 30, 1, False),
        ],
        "split string, visitor_id int, item_id int, relevance int, visitor_has_history boolean",
    )
    truth = labels.where(F.col("split") == "val")
    assert union_recall(cands, truth, [1, 2]) == pytest.approx(
        {1: (0.5 + 0 + 0) / 3, 2: (1 + 0 + 0) / 3}
    )
    rows = evaluate(cands, labels, "val", [1, 2], min_relevances=(1,))
    get = {(r["source"], r["segment"], r["k"]): r for r in rows}
    assert get[("a", "warm", 2)]["recall"] == pytest.approx(0.5)
    assert get[("a", "all", 2)]["coverage"] == pytest.approx(1 / 3)
    assert get[("b", "all", 1)]["coverage"] == pytest.approx(2 / 3)
    assert get[("union", "warm", 2)]["recall"] == pytest.approx(1.0)


def test_leave_one_out(spark: SparkSession) -> None:
    from recsys.candidates.evaluate import leave_one_out

    cands = spark.createDataFrame(
        [("val", "a", 1, 10, 1.0, 1), ("val", "b", 1, 11, 1.0, 1), ("val", "b", 1, 10, 0.5, 2)],
        "split string, source string, visitor_id int, item_id int, score double, rank int",
    )
    labels = spark.createDataFrame(
        [("val", 1, 10, 1, True), ("val", 1, 11, 1, True)],
        "split string, visitor_id int, item_id int, relevance int, visitor_has_history boolean",
    )
    rows = {
        (r["segment"], r["removed"], r["k"]): r for r in leave_one_out(cands, labels, "val", [1, 2])
    }
    assert rows[("all", "a", 1)]["union_recall"] == pytest.approx(1.0)
    assert rows[("all", "a", 1)]["union_recall_without"] == pytest.approx(0.5)  # b@1 = item 11
    assert rows[("all", "b", 2)]["drop"] == pytest.approx(0.5)
    assert rows[("all", "a", 2)]["drop"] == pytest.approx(0.0)  # b covers item 10 at rank 2

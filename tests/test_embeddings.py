import random
from datetime import datetime

import numpy as np
import pytest
from pyspark.sql import SparkSession

from recsys.candidates import item2vec as i2v_candidates
from recsys.config import load_config
from recsys.embeddings import item2vec
from recsys.embeddings.index import ExactIndex, normalize
from tests.feature_helpers import enriched

EC = load_config("base").embeddings


def test_session_sequences_collapse_and_filter(spark: SparkSession) -> None:
    t = lambda m: datetime(2015, 6, 1, 10, m)  # noqa: E731
    ev = enriched(
        spark,
        [
            (t(3), 1, 30, "view", None, "1:1", 1),
            (t(1), 1, 10, "view", None, "1:1", 1),
            (t(2), 1, 10, "view", None, "1:1", 1),  # consecutive repeat collapsed
            (t(4), 1, 10, "addtocart", None, "1:1", 1),  # not consecutive: kept
            (t(1), 2, 50, "view", None, "2:1", 1),
            (t(2), 2, 50, "view", None, "2:1", 1),  # single distinct item: dropped
            (t(1), 3, 60, "view", None, "3:1", 1),
            (t(2), 3, 61, "view", None, "3:1", 1),
        ],
    )
    assert item2vec.session_sequences(ev, 2) == [["10", "30", "10"], ["60", "61"]]


def _clustered_corpus(seed: int = 0) -> list[list[str]]:
    rng = random.Random(seed)
    a = [str(i) for i in range(100, 120)]
    b = [str(i) for i in range(200, 220)]
    return [rng.sample(a, 6) for _ in range(400)] + [rng.sample(b, 6) for _ in range(400)]


def test_train_learns_clusters_and_is_deterministic() -> None:
    # A 40-token toy vocabulary needs more epochs than real data to separate clusters
    # (with 5 epochs all vectors stay nearly parallel); 20 is plenty.
    cfg = EC.model_copy(update={"vector_size": 16, "epochs": 20, "min_count": 1})
    emb = item2vec.train(_clustered_corpus(), cfg)
    again = item2vec.train(_clustered_corpus(), cfg)
    assert np.array_equal(emb.vectors, again.vectors)
    assert list(emb.item_ids) == sorted(emb.item_ids)
    assert np.allclose(np.linalg.norm(emb.vectors, axis=1), 1.0, atol=1e-5)
    pos = {int(i): n for n, i in enumerate(emb.item_ids)}
    sim = lambda x, y: float(emb.vectors[pos[x]] @ emb.vectors[pos[y]])  # noqa: E731
    within = np.mean([sim(100, j) for j in range(101, 120)])
    across = np.mean([sim(100, j) for j in range(200, 220)])
    assert within > across + 0.3
    assert emb.stats["vocab"] == 40 and emb.stats["sessions"] == 800


def test_frame_roundtrip(spark: SparkSession) -> None:
    cfg = EC.model_copy(update={"vector_size": 8, "epochs": 1, "min_count": 1})
    emb = item2vec.train(_clustered_corpus(), cfg)
    back = item2vec.from_frame(item2vec.to_frame(spark, emb))
    assert np.array_equal(back.item_ids, emb.item_ids)
    assert np.allclose(back.vectors, emb.vectors)
    assert np.array_equal(back.counts, emb.counts)


def test_exact_index_matches_brute_force_with_exclusions() -> None:
    rng = np.random.default_rng(0)
    ids = np.arange(1000, 1200, dtype=np.int64)
    vecs = normalize(rng.normal(size=(200, 12)).astype(np.float32))
    queries = rng.normal(size=(30, 12)).astype(np.float32)
    exclude = [{int(ids[i])} for i in range(30)]
    idx = ExactIndex(ids, vecs, chunk=7)
    got = idx.search(queries, 5, exclude)
    sims = normalize(queries) @ vecs.T
    for q in range(30):
        order = [int(ids[i]) for i in np.argsort(-sims[q]) if int(ids[i]) not in exclude[q]][:5]
        assert [h.item_id for h in got[q]] == order
        assert got[q][0].score == pytest.approx(
            float(
                sims[q].max()
                if int(ids[sims[q].argmax()]) not in exclude[q]
                else np.sort(sims[q])[-2]
            ),
            abs=1e-5,
        )
    assert len(idx.search(queries[:1], 500)[0]) == 200  # k > n returns everything


def test_exact_index_tie_break_by_item_id() -> None:
    vecs = np.array([[1, 0], [1, 0], [0, 1]], dtype=np.float32)
    idx = ExactIndex(np.array([30, 10, 20], dtype=np.int64), vecs)
    assert [h.item_id for h in idx.search(np.array([[1, 0]], np.float32), 2)[0]] == [10, 30]


def test_item2vec_candidates_user_vector(spark: SparkSession) -> None:
    emb = item2vec.TrainedEmbeddings(
        np.array([1, 2, 3, 4], dtype=np.int64),
        normalize(np.array([[1, 0], [0, 1], [0.9, 0.1], [0.1, 0.9]], dtype=np.float32)),
        np.ones(4, dtype=np.int64),
    )
    seeds = spark.createDataFrame(
        [(7, 1, 3.0), (7, 2, 1.0), (8, 99, 1.0)], "visitor_id int, item_id int, seed_weight double"
    )
    visitors, queries, excl = i2v_candidates.user_vectors(seeds, emb)
    assert visitors == [7]  # visitor 8's only seed has no embedding
    assert np.allclose(queries[0], [3.0, 1.0])
    assert excl == [{1, 2}]
    recs = i2v_candidates.recommend(spark, seeds, emb, 5, 16).orderBy("rank").collect()
    assert [(r.visitor_id, r.item_id, r.rank) for r in recs] == [(7, 3, 1), (7, 4, 2)]

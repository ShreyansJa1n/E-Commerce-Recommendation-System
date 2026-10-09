"""Integration tests against a real Qdrant (docker compose locally, a service container in CI).

Skipped when Qdrant isn't reachable at $QDRANT_URL (default http://localhost:6333).
"""

import os

import numpy as np
import pytest
from qdrant_client import QdrantClient, models

from recsys.embeddings.index import ExactIndex, QdrantIndex, normalize

URL = os.environ.get("QDRANT_URL", "http://localhost:6333")


def _client() -> QdrantClient | None:
    try:
        c = QdrantClient(url=URL, timeout=5)
        c.get_collections()
        return c
    except Exception:
        return None


client = _client()
pytestmark = pytest.mark.skipif(client is None, reason=f"Qdrant not reachable at {URL}")


@pytest.fixture(scope="module")
def built() -> tuple[QdrantIndex, np.ndarray, np.ndarray]:
    assert client is not None
    rng = np.random.default_rng(1)
    ids = np.arange(1, 2001, dtype=np.int64)
    vecs = normalize(rng.normal(size=(2000, 16)).astype(np.float32))
    payloads = [
        {"item_id": int(i), "available": int(i % 2), "category_id": int(i % 5)} for i in ids
    ]
    # Low full-scan threshold forces HNSW even for this small collection.
    idx = QdrantIndex.build(client, "test_items", ids, vecs, payloads, 16, 100, 256, 500, 10)
    idx.wait_until_indexed(timeout_s=120)
    yield idx, ids, vecs
    client.delete_collection("test_items")


def test_build_is_idempotent_and_indexed(built) -> None:
    _, ids, _ = built
    info = client.get_collection("test_items")  # type: ignore[union-attr]
    assert info.points_count == len(ids)
    assert (info.indexed_vectors_count or 0) >= len(ids)


def test_ann_recall_vs_exact(built) -> None:
    idx, ids, vecs = built
    queries = vecs[:100]
    excl = [{int(i)} for i in ids[:100]]
    exact = ExactIndex(ids, vecs).search(queries, 10, excl)
    ann = idx.search(queries, 10, excl, batch_size=32)
    recall = np.mean(
        [
            len({h.item_id for h in a} & {h.item_id for h in e}) / 10
            for a, e in zip(ann, exact, strict=True)
        ]
    )
    assert recall >= 0.95
    assert all(int(ids[q]) not in {h.item_id for h in ann[q]} for q in range(100))  # self excluded
    one = idx.search_one(queries[0], 10, excl[0])
    assert [h.item_id for h in one] == [h.item_id for h in ann[0]]


def test_payload_filter(built) -> None:
    idx, _, vecs = built
    flt = models.Filter(
        must=[models.FieldCondition(key="available", match=models.MatchValue(value=1))]
    )
    hits = idx.search(vecs[:20], 10, None, flt)
    assert all(h.item_id % 2 == 1 for hs in hits for h in hs)


def test_low_ef_hnsw_is_approximate(built) -> None:
    """Guard against silently benchmarking a full scan: tiny ef must lose some recall."""
    _, ids, vecs = built
    assert client is not None
    low = QdrantIndex(client, "test_items", search_ef=1)
    exact = ExactIndex(ids, vecs).search(vecs[:200], 10, [{int(i)} for i in ids[:200]])
    ann = low.search(vecs[:200], 10, [{int(i)} for i in ids[:200]])
    recall = np.mean(
        [
            len({h.item_id for h in a} & {h.item_id for h in e}) / 10
            for a, e in zip(ann, exact, strict=True)
        ]
    )
    assert recall < 1.0

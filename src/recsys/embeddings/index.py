"""Vector indexes with one interface: exact (NumPy) and approximate (Qdrant HNSW).

The offline pipeline uses ``ExactIndex`` (deterministic, no service needed). Serving uses
Qdrant. ``recsys.embeddings.benchmark`` measures how closely Qdrant matches exact search.
All vectors are L2-normalized, so the inner product is cosine similarity.
"""

from __future__ import annotations

import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import numpy as np
import numpy.typing as npt
from qdrant_client import QdrantClient, models

Vector = npt.NDArray[np.float32]


@dataclass(frozen=True)
class Hit:
    item_id: int
    score: float


class VectorIndex(Protocol):
    def search(
        self, queries: Vector, k: int, exclude: Sequence[set[int]] | None = None
    ) -> list[list[Hit]]: ...


def normalize(v: Vector) -> Vector:
    norms = np.linalg.norm(v, axis=-1, keepdims=True)
    return (v / np.where(norms == 0, 1.0, norms)).astype(np.float32)


class ExactIndex:
    """Brute-force cosine search, chunked over queries."""

    def __init__(self, item_ids: npt.NDArray[np.int64], vectors: Vector, chunk: int = 2048):
        self.item_ids = item_ids
        self.vectors = normalize(vectors)
        self.chunk = chunk

    def search(
        self, queries: Vector, k: int, exclude: Sequence[set[int]] | None = None
    ) -> list[list[Hit]]:
        results: list[list[Hit]] = []
        n_items = len(self.item_ids)
        for start in range(0, len(queries), self.chunk):
            q = normalize(queries[start : start + self.chunk])
            scores = q @ self.vectors.T
            excl = exclude[start : start + self.chunk] if exclude else [set()] * len(q)
            for row, ex in zip(scores, excl, strict=True):
                take = min(n_items, k + len(ex))
                top = (
                    np.argpartition(-row, take - 1)[:take] if take < n_items else np.arange(n_items)
                )
                # Sort by score desc, then item id asc, for deterministic ties.
                top = top[np.lexsort((self.item_ids[top], -row[top]))]
                hits = [
                    Hit(int(self.item_ids[i]), float(row[i]))
                    for i in top
                    if int(self.item_ids[i]) not in ex
                ]
                results.append(hits[:k])
        return results


def collection_name(prefix: str, cutoff: str) -> str:
    return f"{prefix}_{cutoff.replace('-', '')}"


def _batches(n: int, size: int) -> Iterator[tuple[int, int]]:
    for start in range(0, n, size):
        yield start, min(start + size, n)


class QdrantIndex:
    def __init__(self, client: QdrantClient, collection: str, search_ef: int):
        self.client = client
        self.collection = collection
        self.search_ef = search_ef

    @classmethod
    def build(
        cls,
        client: QdrantClient,
        collection: str,
        item_ids: npt.NDArray[np.int64],
        vectors: Vector,
        payloads: Sequence[dict[str, Any]],
        hnsw_m: int,
        hnsw_ef_construct: int,
        search_ef: int,
        batch_size: int,
        full_scan_threshold_kb: int = 10_000,
    ) -> QdrantIndex:
        """(Re)create the collection and upload all points. Idempotent: drops it first."""
        if client.collection_exists(collection):
            client.delete_collection(collection)
        client.create_collection(
            collection,
            vectors_config=models.VectorParams(
                size=vectors.shape[1], distance=models.Distance.COSINE
            ),
            hnsw_config=models.HnswConfigDiff(
                m=hnsw_m,
                ef_construct=hnsw_ef_construct,
                full_scan_threshold=full_scan_threshold_kb,
            ),
            # Build the HNSW graph regardless of collection size (default threshold would
            # leave small collections on brute-force search and skew the benchmark).
            optimizers_config=models.OptimizersConfigDiff(indexing_threshold=1),
        )
        client.create_payload_index(collection, "category_id", models.PayloadSchemaType.INTEGER)
        client.create_payload_index(collection, "available", models.PayloadSchemaType.INTEGER)
        for lo, hi in _batches(len(item_ids), batch_size):
            client.upsert(
                collection,
                points=[
                    models.PointStruct(
                        id=int(item_ids[i]), vector=vectors[i].tolist(), payload=payloads[i]
                    )
                    for i in range(lo, hi)
                ],
                wait=True,
            )
        return cls(client, collection, search_ef)

    def wait_until_indexed(self, timeout_s: float = 600.0, poll_s: float = 1.0) -> dict[str, Any]:
        """Block until the optimizer has finished building the HNSW index."""
        start = time.perf_counter()
        while True:
            info = self.client.get_collection(self.collection)
            indexed = info.indexed_vectors_count or 0
            points = info.points_count or 0
            if info.status == models.CollectionStatus.GREEN and indexed >= points:
                return {
                    "points": points,
                    "indexed_vectors": indexed,
                    "seconds": round(time.perf_counter() - start, 2),
                }
            if time.perf_counter() - start > timeout_s:
                raise TimeoutError(
                    f"{self.collection} not indexed after {timeout_s}s: {info.status}"
                )
            time.sleep(poll_s)

    def _request(
        self, query: Vector, k: int, ex: set[int], flt: models.Filter | None
    ) -> models.QueryRequest:
        must_not = [models.HasIdCondition(has_id=sorted(ex))] if ex else []
        conds = models.Filter(must=flt.must if flt else None, must_not=must_not or None)
        return models.QueryRequest(
            query=query.tolist(),
            limit=k,
            filter=conds if (must_not or flt) else None,
            params=models.SearchParams(hnsw_ef=self.search_ef),
            with_payload=False,
        )

    def search(
        self,
        queries: Vector,
        k: int,
        exclude: Sequence[set[int]] | None = None,
        query_filter: models.Filter | None = None,
        batch_size: int = 64,
    ) -> list[list[Hit]]:
        excl = exclude or [set()] * len(queries)
        out: list[list[Hit]] = []
        for lo, hi in _batches(len(queries), batch_size):
            reqs = [self._request(queries[i], k, excl[i], query_filter) for i in range(lo, hi)]
            for resp in self.client.query_batch_points(self.collection, requests=reqs):
                out.append([Hit(int(p.id), float(p.score)) for p in resp.points])
        return out

    def search_one(
        self,
        query: Vector,
        k: int,
        exclude: set[int] | None = None,
        query_filter: models.Filter | None = None,
    ) -> list[Hit]:
        req = self._request(query, k, exclude or set(), query_filter)
        resp = self.client.query_points(
            self.collection,
            query=req.query,
            limit=k,
            query_filter=req.filter,
            search_params=req.params,
            with_payload=False,
        )
        return [Hit(int(p.id), float(p.score)) for p in resp.points]

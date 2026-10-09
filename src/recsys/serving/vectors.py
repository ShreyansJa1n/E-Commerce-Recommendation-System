"""Similar-item lookups for the API (Qdrant), behind a small interface for testing."""

from __future__ import annotations

from typing import Protocol

from qdrant_client import QdrantClient, models


class SimilarItems(Protocol):
    def similar(
        self, collection: str, item_id: int, n: int, available_only: bool
    ) -> list[tuple[int, float]] | None:
        """Neighbors of ``item_id`` (itself excluded), or None if the item is unknown."""

    def ping(self) -> bool: ...


class QdrantSimilarItems:
    def __init__(self, client: QdrantClient, search_ef: int = 128):
        self.client = client
        self.search_ef = search_ef

    def similar(
        self, collection: str, item_id: int, n: int, available_only: bool
    ) -> list[tuple[int, float]] | None:
        points = self.client.retrieve(collection, ids=[item_id], with_vectors=True)
        if not points or points[0].vector is None:
            return None
        must = (
            [models.FieldCondition(key="available", match=models.MatchValue(value=1))]
            if available_only
            else None
        )
        resp = self.client.query_points(
            collection,
            query=points[0].vector,  # type: ignore[arg-type]
            limit=n,
            query_filter=models.Filter(
                must=must, must_not=[models.HasIdCondition(has_id=[item_id])]
            ),
            search_params=models.SearchParams(hnsw_ef=self.search_ef),
            with_payload=False,
        )
        return [(int(p.id), float(p.score)) for p in resp.points]

    def ping(self) -> bool:
        try:
            self.client.get_collections()
            return True
        except Exception:
            return False

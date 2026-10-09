"""FastAPI service: precomputed recommendations from Redis, similar items from Qdrant.

Run: ``uvicorn recsys.serving.app:app`` (settings from RECSYS_* environment variables).
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field

import redis
from fastapi import FastAPI, HTTPException, Path, Query
from qdrant_client import QdrantClient

from recsys.serving.schemas import (
    Health,
    RecommendationsResponse,
    ScoredItem,
    SimilarItemsResponse,
    Source,
)
from recsys.serving.store import Recs, RecStore
from recsys.serving.vectors import QdrantSimilarItems, SimilarItems

MAX_N = 50


@dataclass
class Settings:
    redis_url: str = field(
        default_factory=lambda: os.environ.get("RECSYS_REDIS_URL", "redis://localhost:6379/0")
    )
    qdrant_url: str = field(
        default_factory=lambda: os.environ.get("RECSYS_QDRANT_URL", "http://localhost:6333")
    )
    key_prefix: str = field(default_factory=lambda: os.environ.get("RECSYS_KEY_PREFIX", "recsys"))
    # How long the live version pointer is cached in-process before re-reading Redis.
    version_ttl_s: float = field(
        default_factory=lambda: float(os.environ.get("RECSYS_VERSION_TTL_S", "5"))
    )


class Recommender:
    """Request logic, independent of FastAPI (unit-testable)."""

    def __init__(self, store: RecStore, vectors: SimilarItems, version_ttl_s: float = 5.0):
        self.store = store
        self.vectors = vectors
        self.version_ttl_s = version_ttl_s
        self._version: str | None = None
        self._version_at = 0.0
        self._collection: str | None = None
        self._popular: Recs = []  # in-process copy for when Redis is unavailable

    def version(self) -> str | None:
        now = time.monotonic()
        if self._version is None or now - self._version_at > self.version_ttl_s:
            v = self.store.current_version()
            if v != self._version and v is not None:
                snap = self.store.snapshot(v)
                self._collection = snap.meta.get("collection") if snap else None
                self._popular = self.store.popular(v) or self._popular
            self._version, self._version_at = v, now
        return self._version

    def recommendations(self, user_id: int, n: int) -> tuple[Recs, Source, str | None]:
        try:
            v = self.version()
            if v is None:
                raise HTTPException(503, "no recommendation snapshot is live")
            recs = self.store.user_recs(v, user_id)
            if recs:
                return recs[:n], Source.personalized, v
            popular = self.store.popular(v) or self._popular
            return popular[:n], Source.popular, v
        except redis.RedisError:
            if not self._popular:
                raise HTTPException(
                    503, "Redis unavailable and no cached popularity list"
                ) from None
            return self._popular[:n], Source.popular_degraded, None

    def similar(self, item_id: int, n: int, available_only: bool) -> tuple[Recs, str]:
        self.version()  # refresh the collection name with the live snapshot
        if self._collection is None:
            raise HTTPException(503, "no vector collection is live")
        try:
            hits = self.vectors.similar(self._collection, item_id, n, available_only)
        except Exception as e:  # Qdrant unreachable
            raise HTTPException(503, f"vector search unavailable: {type(e).__name__}") from None
        if hits is None:
            raise HTTPException(404, f"item {item_id} has no embedding")
        return hits, self._collection


def _items(recs: Recs) -> list[ScoredItem]:
    return [ScoredItem(item_id=i, score=s, rank=r) for r, (i, s) in enumerate(recs, start=1)]


def create_app(rec: Recommender) -> FastAPI:
    app = FastAPI(
        title="recsys",
        version="1.0",
        description="Precomputed e-commerce recommendations (Retailrocket). See docs/API.md.",
    )

    @app.get("/recommendations/{user_id}", response_model=RecommendationsResponse)
    def recommendations(
        user_id: int = Path(ge=0), n: int = Query(10, ge=1, le=MAX_N)
    ) -> RecommendationsResponse:
        recs, source, version = rec.recommendations(user_id, n)
        return RecommendationsResponse(
            user_id=user_id, items=_items(recs), source=source, version=version
        )

    @app.get("/similar/{item_id}", response_model=SimilarItemsResponse)
    def similar(
        item_id: int = Path(ge=0),
        n: int = Query(10, ge=1, le=MAX_N),
        available_only: bool = Query(False),
    ) -> SimilarItemsResponse:
        hits, collection = rec.similar(item_id, n, available_only)
        return SimilarItemsResponse(
            item_id=item_id,
            items=_items(hits),
            collection=collection,
            available_only=available_only,
        )

    @app.get("/health", response_model=Health)
    def health() -> Health:
        try:
            v = rec.store.current_version()
            snap = rec.store.snapshot(v) if v else None
            redis_ok = True
        except redis.RedisError:
            v, snap, redis_ok = None, None, False
        qdrant_ok = rec.vectors.ping()
        status = "ok" if redis_ok and qdrant_ok and v else "degraded"
        return Health(
            status=status,
            redis=redis_ok,
            qdrant=qdrant_ok,
            version=v,
            snapshot=snap.meta if snap else None,
        )

    return app


def build_app(settings: Settings | None = None) -> FastAPI:
    s = settings or Settings()
    store = RecStore(
        redis.Redis.from_url(s.redis_url, socket_timeout=0.5, socket_connect_timeout=0.5),
        s.key_prefix,
    )
    vectors = QdrantSimilarItems(QdrantClient(url=s.qdrant_url, timeout=2))
    return create_app(Recommender(store, vectors, s.version_ttl_s))


def __getattr__(name: str) -> FastAPI:
    # `uvicorn recsys.serving.app:app` builds the app lazily (importing doesn't connect).
    if name == "app":
        return build_app()
    raise AttributeError(name)

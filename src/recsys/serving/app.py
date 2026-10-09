"""FastAPI service: precomputed recommendations from Redis, similar items from Qdrant.

Run: ``uvicorn recsys.serving.app:app`` (settings from RECSYS_* environment variables).
Observability: JSON request logs with X-Request-ID, Prometheus metrics on /metrics.
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime

import redis
from fastapi import FastAPI, HTTPException, Path, Query, Request, Response
from qdrant_client import QdrantClient

from recsys.observability import api_metrics as m
from recsys.observability.logging import configure as configure_logging
from recsys.observability.logging import request_id
from recsys.serving.schemas import (
    Health,
    RecommendationsResponse,
    ScoredItem,
    SimilarItemsResponse,
    Source,
)
from recsys.serving.store import Recs, RecStore, Snapshot
from recsys.serving.vectors import QdrantSimilarItems, SimilarItems

MAX_N = 50
log = logging.getLogger("recsys.api")


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


@dataclass
class Status:
    version: str | None
    snapshot: Snapshot | None
    redis_ok: bool
    qdrant_ok: bool


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
        self._info_labels: tuple[str, str, str] | None = None
        self.breaker_s = 5.0
        self._redis_open_until = 0.0

    def version(self) -> str | None:
        now = time.monotonic()
        if self._version is None or now - self._version_at > self.version_ttl_s:
            v = self.store.current_version()
            if v != self._version and v is not None:
                snap = self.store.snapshot(v)
                self._collection = snap.meta.get("collection") if snap else None
                self._popular = self.store.popular(v) or self._popular
                log.info("live snapshot changed", extra={"version": v, "previous": self._version})
            self._version, self._version_at = v, now
        return self._version

    def recommendations(self, user_id: int, n: int) -> tuple[Recs, Source, str | None]:
        # Circuit breaker: right after a Redis failure, don't pay a socket timeout per
        # request; serve the cached popularity list until the breaker re-closes.
        if self._popular and time.monotonic() < self._redis_open_until:
            m.RECS_SERVED.labels(Source.popular_degraded.value).inc()
            return self._popular[:n], Source.popular_degraded, None
        try:
            v = self.version()
            if v is None:
                raise HTTPException(503, "no recommendation snapshot is live")
            recs = self.store.user_recs(v, user_id)
            if recs:
                out, source = recs[:n], Source.personalized
            else:
                out, source = (self.store.popular(v) or self._popular)[:n], Source.popular
        except redis.RedisError as e:
            m.DEPENDENCY_ERRORS.labels("redis").inc()
            self._redis_open_until = time.monotonic() + self.breaker_s
            if not self._popular:
                raise HTTPException(
                    503, "Redis unavailable and no cached popularity list"
                ) from None
            log.warning("redis unavailable, serving cached popularity", extra={"error": str(e)})
            out, source, v = self._popular[:n], Source.popular_degraded, None
        m.RECS_SERVED.labels(source.value).inc()
        return out, source, v

    def similar(self, item_id: int, n: int, available_only: bool) -> tuple[Recs, str]:
        try:
            self.version()  # refresh the collection name with the live snapshot
        except redis.RedisError:
            m.DEPENDENCY_ERRORS.labels("redis").inc()  # keep the last known collection
        if self._collection is None:
            raise HTTPException(503, "no vector collection is live")
        try:
            hits = self.vectors.similar(self._collection, item_id, n, available_only)
        except Exception as e:  # Qdrant unreachable
            m.DEPENDENCY_ERRORS.labels("qdrant").inc()
            raise HTTPException(503, f"vector search unavailable: {type(e).__name__}") from None
        if hits is None:
            m.SIMILAR_NOT_FOUND.inc()
            raise HTTPException(404, f"item {item_id} has no embedding")
        return hits, self._collection

    def status(self) -> Status:
        """Check dependencies and refresh the health gauges (called by /health and /metrics)."""
        try:
            v = self.store.current_version()
            snap = self.store.snapshot(v) if v else None
            redis_ok = True
        except redis.RedisError:
            m.DEPENDENCY_ERRORS.labels("redis").inc()
            v, snap, redis_ok = None, None, False
        qdrant_ok = self.vectors.ping()
        m.DEPENDENCY_UP.labels("redis").set(int(redis_ok))
        m.DEPENDENCY_UP.labels("qdrant").set(int(qdrant_ok))
        if snap:
            created = snap.meta.get("created_at")
            if created:
                age = (datetime.now(UTC) - datetime.fromisoformat(created)).total_seconds()
                m.SNAPSHOT_AGE.set(age)
            m.SNAPSHOT_USERS.set(float(snap.meta.get("users", 0)))
            info = (snap.version, snap.meta.get("model", ""), snap.meta.get("cutoff_date", ""))
            if self._info_labels and self._info_labels != info:
                m.SNAPSHOT_INFO.labels(*self._info_labels).set(0)  # retire the old version
            m.SNAPSHOT_INFO.labels(*info).set(1)
            self._info_labels = info
        return Status(v, snap, redis_ok, qdrant_ok)


def _items(recs: Recs) -> list[ScoredItem]:
    return [ScoredItem(item_id=i, score=s, rank=r) for r, (i, s) in enumerate(recs, start=1)]


def create_app(rec: Recommender) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        # Warm the live version + popularity copy in *every* worker at startup, so a worker
        # that has served no traffic yet can still fall back if Redis goes down.
        try:
            rec.version()
        except redis.RedisError:
            log.warning("redis unavailable at startup; no popularity copy cached yet")
        yield

    app = FastAPI(
        title="recsys",
        version="1.0",
        description="Precomputed e-commerce recommendations (Retailrocket). See docs/API.md.",
        lifespan=lifespan,
    )

    @app.middleware("http")
    async def observe(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        rid = request.headers.get("x-request-id") or uuid.uuid4().hex
        token = request_id.set(rid)
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            response.headers["x-request-id"] = rid
            return response
        finally:
            elapsed = time.perf_counter() - start
            route = request.scope.get("route")
            template = getattr(route, "path", "unmatched")
            if template != "/metrics":
                m.REQUEST_LATENCY.labels(template, request.method, str(status)).observe(elapsed)
                log.info(
                    "request",
                    extra={
                        "method": request.method,
                        "route": template,
                        "path": request.url.path,
                        "status": status,
                        "duration_ms": round(elapsed * 1000, 3),
                    },
                )
            request_id.reset(token)

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
        s = rec.status()
        ok = s.redis_ok and s.qdrant_ok and s.version
        return Health(
            status="ok" if ok else "degraded",
            redis=s.redis_ok,
            qdrant=s.qdrant_ok,
            version=s.version,
            snapshot=s.snapshot.meta if s.snapshot else None,
        )

    @app.get("/metrics", include_in_schema=False)
    def metrics() -> Response:
        rec.status()
        body, content_type = m.render()
        return Response(body, media_type=content_type)

    return app


def build_app(settings: Settings | None = None) -> FastAPI:
    configure_logging()
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

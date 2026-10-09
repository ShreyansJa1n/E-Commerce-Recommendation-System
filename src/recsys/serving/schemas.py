"""API request/response contract (documented in docs/API.md, served as OpenAPI)."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class Source(StrEnum):
    personalized = "personalized"  # ranker list for a visitor with history
    popular = "popular"  # unknown / cold visitor: global popularity
    popular_degraded = "popular_degraded"  # Redis unavailable: in-process popularity copy


class ScoredItem(BaseModel):
    item_id: int
    score: float
    rank: int = Field(ge=1)


class RecommendationsResponse(BaseModel):
    user_id: int
    items: list[ScoredItem]
    source: Source
    version: str | None = Field(description="Snapshot version served (null when degraded)")


class SimilarItemsResponse(BaseModel):
    item_id: int
    items: list[ScoredItem]
    collection: str
    available_only: bool


class Health(BaseModel):
    status: str
    redis: bool
    qdrant: bool
    version: str | None
    snapshot: dict[str, str] | None = None

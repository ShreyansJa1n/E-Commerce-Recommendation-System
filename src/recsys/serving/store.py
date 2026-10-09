"""Versioned recommendation snapshots in Redis (no Spark imports: used by the API).

Keys (``p`` = key prefix, ``v`` = version):
    p:current, p:previous          -> version strings (no TTL)
    p:v:meta                       -> hash: cutoff, model, collection, created_at, counts
    p:v:user:<visitor_id>          -> JSON [[item_id, score], ...] in rank order
    p:v:popular                    -> JSON [[item_id, score], ...]
Every ``p:v:*`` key gets a TTL, so abandoned versions expire on their own. A new version is
written completely before ``p:current`` flips (one atomic SET), and the old version stays
reachable via ``p:previous`` until its TTL ends (rollback = ``promote(previous)``).
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import redis

Recs = list[tuple[int, float]]


@dataclass(frozen=True)
class Snapshot:
    version: str
    meta: dict[str, str]


class RecStore:
    def __init__(self, client: redis.Redis, prefix: str = "recsys"):
        self.r = client
        self.p = prefix

    # --- keys -------------------------------------------------------------------------
    def _k(self, *parts: str) -> str:
        return ":".join([self.p, *parts])

    def user_key(self, version: str, visitor_id: int) -> str:
        return self._k(version, "user", str(visitor_id))

    # --- write path (batch loader) ----------------------------------------------------
    def write_version(
        self,
        version: str,
        user_recs: Iterable[tuple[int, Sequence[tuple[int, float]]]],
        popular: Sequence[tuple[int, float]],
        meta: Mapping[str, Any],
        ttl_seconds: int,
        batch_size: int = 1000,
    ) -> int:
        """Write a complete version (not yet live). Returns the number of users written."""
        n = 0
        pipe = self.r.pipeline(transaction=False)
        for visitor_id, recs in user_recs:
            pipe.set(self.user_key(version, visitor_id), _dump(recs), ex=ttl_seconds)
            n += 1
            if n % batch_size == 0:
                pipe.execute()
        pipe.set(self._k(version, "popular"), _dump(popular), ex=ttl_seconds)
        pipe.hset(
            self._k(version, "meta"),
            mapping={**{k: str(v) for k, v in meta.items()}, "users": str(n)},
        )
        pipe.expire(self._k(version, "meta"), ttl_seconds)
        pipe.execute()
        return n

    def promote(self, version: str) -> None:
        """Make ``version`` live; the previously live version becomes ``previous``."""
        if not self.r.exists(self._k(version, "meta")):
            raise KeyError(f"version {version!r} is not loaded (or expired)")
        old = self.current_version()
        pipe = self.r.pipeline(transaction=True)
        if old and old != version:
            pipe.set(self._k("previous"), old)
        pipe.set(self._k("current"), version)
        pipe.execute()

    # --- read path (API) --------------------------------------------------------------
    def current_version(self) -> str | None:
        v = self.r.get(self._k("current"))
        return v.decode() if isinstance(v, bytes) else v

    def previous_version(self) -> str | None:
        v = self.r.get(self._k("previous"))
        return v.decode() if isinstance(v, bytes) else v

    def snapshot(self, version: str) -> Snapshot | None:
        raw = self.r.hgetall(self._k(version, "meta"))
        if not raw:
            return None
        return Snapshot(version, {_s(k): _s(v) for k, v in raw.items()})

    def user_recs(self, version: str, visitor_id: int) -> Recs | None:
        return _load(self.r.get(self.user_key(version, visitor_id)))

    def popular(self, version: str) -> Recs | None:
        return _load(self.r.get(self._k(version, "popular")))


def _s(x: bytes | str) -> str:
    return x.decode() if isinstance(x, bytes) else x


def _dump(recs: Sequence[tuple[int, float]]) -> str:
    return json.dumps([[int(i), round(float(s), 6)] for i, s in recs], separators=(",", ":"))


def _load(raw: bytes | str | None) -> Recs | None:
    if raw is None:
        return None
    return [(int(i), float(s)) for i, s in json.loads(raw)]

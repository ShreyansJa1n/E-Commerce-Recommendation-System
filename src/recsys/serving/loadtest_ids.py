"""Write data/loadtest/ids.json: sampled live user ids (Redis) and item ids (Qdrant)."""

from __future__ import annotations

import json
import random
import sys
from pathlib import Path

import redis
from qdrant_client import QdrantClient

from recsys.serving.store import RecStore


def main(out: str = "data/loadtest/ids.json", n: int = 5000, seed: int = 7) -> None:
    store = RecStore(redis.Redis.from_url("redis://localhost:6379/0"), "recsys")
    v = store.current_version()
    snap = store.snapshot(v) if v else None
    if not v or not snap:
        raise SystemExit("no live snapshot: run `make serve-load ENV=base` first")
    prefix = f"recsys:{v}:user:"
    users = [int(k.decode()[len(prefix) :]) for k in store.r.scan_iter(f"{prefix}*", count=10_000)]
    items: list[int] = []
    client = QdrantClient(url="http://localhost:6333")
    offset = None
    while True:
        pts, offset = client.scroll(
            snap.meta["collection"], limit=10_000, offset=offset, with_payload=False
        )
        items += [int(p.id) for p in pts]
        if offset is None:
            break
    rng = random.Random(seed)
    data = {
        "version": v,
        "warm_users": rng.sample(users, min(n, len(users))),
        "items": rng.sample(items, min(n, len(items))),
    }
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    Path(out).write_text(json.dumps(data))
    n_u, n_i = len(data["warm_users"]), len(data["items"])
    print(f"wrote {out}: {n_u} of {len(users)} users, {n_i} of {len(items)} items")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "data/loadtest/ids.json")

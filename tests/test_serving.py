import fakeredis
import pytest
import redis
from fastapi.testclient import TestClient

from recsys.serving.app import Recommender, create_app
from recsys.serving.store import RecStore


class FakeVectors:
    def __init__(self) -> None:
        self.up = True

    def similar(
        self, collection: str, item_id: int, n: int, available_only: bool
    ) -> list[tuple[int, float]] | None:
        if not self.up:
            raise ConnectionError("down")
        if item_id == 404:
            return None
        hits = [(item_id + k, 1.0 - k / 10) for k in range(1, 20) if not (available_only and k % 2)]
        return hits[:n]

    def ping(self) -> bool:
        return self.up


@pytest.fixture
def store() -> RecStore:
    s = RecStore(fakeredis.FakeRedis(), "t")
    s.write_version(
        "v1",
        [(1, [(10, 0.9), (11, 0.8), (12, 0.7)])],
        [(100, 1.0), (101, 0.5)],
        {"collection": "items_1"},
        3600,
    )
    s.promote("v1")
    return s


@pytest.fixture
def client(store: RecStore) -> tuple[TestClient, Recommender, FakeVectors]:
    vec = FakeVectors()
    rec = Recommender(store, vec, version_ttl_s=0)
    return TestClient(create_app(rec)), rec, vec


def test_store_versions_ttl_and_rollback() -> None:
    r = fakeredis.FakeRedis()
    s = RecStore(r, "p")
    s.write_version("a", [(1, [(5, 0.5)])], [(9, 1.0)], {"cutoff_date": "x"}, 60)
    assert s.current_version() is None  # written but not live
    s.promote("a")
    s.write_version("b", [(1, [(6, 0.6)])], [(9, 1.0)], {}, 60)
    s.promote("b")
    assert (s.current_version(), s.previous_version()) == ("b", "a")
    assert s.user_recs("b", 1) == [(6, 0.6)] and s.user_recs("a", 1) == [(5, 0.5)]
    assert 0 < r.ttl("p:b:user:1") <= 60 and r.ttl("p:current") == -1
    assert s.snapshot("a").meta["users"] == "1"
    with pytest.raises(KeyError):
        s.promote("never-loaded")
    s.promote("a")  # rollback
    assert s.current_version() == "a"


def test_personalized_and_fallback(client) -> None:
    c, _, _ = client
    r = c.get("/recommendations/1", params={"n": 2}).json()
    assert r["source"] == "personalized" and r["version"] == "v1"
    assert [(i["item_id"], i["rank"]) for i in r["items"]] == [(10, 1), (11, 2)]
    r = c.get("/recommendations/999").json()
    assert r["source"] == "popular" and [i["item_id"] for i in r["items"]] == [100, 101]


def test_redis_down_serves_cached_popularity(client, monkeypatch) -> None:
    c, rec, _ = client
    c.get("/recommendations/1")  # warms the in-process popularity copy

    def boom(*_a, **_k):
        raise redis.ConnectionError("down")

    monkeypatch.setattr(rec.store, "current_version", boom)
    r = c.get("/recommendations/1").json()
    assert r["source"] == "popular_degraded" and r["version"] is None
    assert [i["item_id"] for i in r["items"]] == [100, 101]
    h = c.get("/health").json()
    assert h["status"] == "degraded" and not h["redis"]


def test_no_snapshot_is_503() -> None:
    rec = Recommender(RecStore(fakeredis.FakeRedis(), "empty"), FakeVectors(), 0)
    c = TestClient(create_app(rec))
    assert c.get("/recommendations/1").status_code == 503
    assert c.get("/similar/5").status_code == 503


def test_similar_items(client) -> None:
    c, _, vec = client
    r = c.get("/similar/5", params={"n": 3}).json()
    assert r["collection"] == "items_1" and [i["item_id"] for i in r["items"]] == [6, 7, 8]
    assert all(
        i["item_id"] % 2 == 1
        for i in c.get("/similar/5", params={"available_only": True}).json()["items"]
    )
    assert c.get("/similar/404").status_code == 404
    vec.up = False
    assert c.get("/similar/5").status_code == 503


def test_validation_and_health(client) -> None:
    c, _, _ = client
    assert c.get("/recommendations/1", params={"n": 0}).status_code == 422
    assert c.get("/recommendations/1", params={"n": 51}).status_code == 422
    assert c.get("/recommendations/-3").status_code == 422
    h = c.get("/health").json()
    assert h["status"] == "ok" and h["version"] == "v1" and h["snapshot"]["collection"] == "items_1"


def test_version_flip_is_picked_up(client, store: RecStore) -> None:
    c, _, _ = client
    store.write_version("v2", [(1, [(77, 0.1)])], [(100, 1.0)], {"collection": "items_2"}, 3600)
    store.promote("v2")
    r = c.get("/recommendations/1").json()
    assert r["version"] == "v2" and r["items"][0]["item_id"] == 77
    assert c.get("/similar/5").json()["collection"] == "items_2"


def test_startup_warms_popularity_cache(store: RecStore, monkeypatch) -> None:
    """A worker that never served a request must still have the fallback after startup."""
    rec = Recommender(store, FakeVectors(), version_ttl_s=0)
    with TestClient(create_app(rec)) as c:  # runs the lifespan startup
        assert rec._popular  # warmed without any request

        def boom(*_a, **_k):
            raise redis.ConnectionError("down")

        monkeypatch.setattr(rec.store, "current_version", boom)
        r = c.get("/recommendations/1").json()
        assert r["source"] == "popular_degraded" and len(r["items"]) == 2


def test_circuit_breaker_skips_redis_after_failure(client, monkeypatch) -> None:
    c, rec, _ = client
    c.get("/recommendations/1")
    calls = {"n": 0}

    def boom(*_a, **_k):
        calls["n"] += 1
        raise redis.ConnectionError("down")

    monkeypatch.setattr(rec.store, "current_version", boom)
    for _ in range(5):
        assert c.get("/recommendations/1").json()["source"] == "popular_degraded"
    assert calls["n"] == 1  # only the first request touched Redis
    rec._redis_open_until = 0.0  # breaker re-closes after breaker_s
    monkeypatch.undo()
    assert c.get("/recommendations/1").json()["source"] == "personalized"

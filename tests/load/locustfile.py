"""Locust load test: mixed traffic against the recsys API.

Traffic mix: 75% /recommendations (half known returning visitors from the live snapshot,
half unknown ids -> popularity fallback), 25% /similar for items in the live collection.
IDs are sampled from data/loadtest/ids.json (`make loadtest-ids`, from the live snapshot).
"""

from __future__ import annotations

import json
import os
import random
from pathlib import Path

from locust import HttpUser, between, task

IDS = Path(os.environ.get("RECSYS_LOADTEST_IDS", "data/loadtest/ids.json"))


def _ids() -> dict[str, list[int]]:
    if IDS.exists():
        data: dict[str, list[int]] = json.loads(IDS.read_text())
        return data
    raise SystemExit(f"{IDS} not found: run `make loadtest-ids` (needs the live snapshot)")


DATA = _ids()


# Think time per simulated user, seconds. Default ~0 = saturation (throughput) run; set
# RECSYS_LOADTEST_WAIT=0.1 for a steady-rate latency run that doesn't saturate the client.
_WAIT = float(os.environ.get("RECSYS_LOADTEST_WAIT", "0.01"))


class Visitor(HttpUser):
    wait_time = between(_WAIT * 0.5, _WAIT)

    @task(3)
    def recommendations(self) -> None:
        if random.random() < 0.5:
            uid = random.choice(DATA["warm_users"])
            name = "/recommendations/{user_id} [personalized]"
        else:
            uid = random.randint(5_000_000, 9_000_000)
            name = "/recommendations/{user_id} [fallback]"
        self.client.get(f"/recommendations/{uid}?n=10", name=name)

    @task(1)
    def similar(self) -> None:
        self.client.get(f"/similar/{random.choice(DATA['items'])}?n=10", name="/similar/{item_id}")

"""Structured JSON logging with a per-request id (contextvar), stdlib only."""

from __future__ import annotations

import json
import logging
import os
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

request_id: ContextVar[str | None] = ContextVar("request_id", default=None)

_STD = set(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        rid = request_id.get()
        if rid:
            out["request_id"] = rid
        out.update({k: v for k, v in vars(record).items() if k not in _STD})
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


def configure(level: str | None = None, fmt: str | None = None) -> None:
    """``RECSYS_LOG_FORMAT=json`` (default for the API container) or ``text``."""
    fmt = fmt or os.environ.get("RECSYS_LOG_FORMAT", "text")
    handler = logging.StreamHandler()
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level or os.environ.get("RECSYS_LOG_LEVEL", "INFO"))

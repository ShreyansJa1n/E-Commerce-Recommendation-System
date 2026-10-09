"""item2vec: skip-gram with negative sampling over session item sequences (gensim).

A session's sequence is its items in time order with consecutive repeats collapsed
(repeated views of the same item add no context). Sessions with fewer than
``min_session_items`` distinct items are dropped. Training is single-threaded with a
deterministic hash function, so reruns produce identical vectors.
"""

from __future__ import annotations

import logging
import time
import zlib
from collections import Counter
from dataclasses import dataclass, field

import numpy as np
import numpy.typing as npt
from gensim.models import Word2Vec
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, FloatType, IntegerType, LongType, StructField, StructType

from recsys.config import EmbeddingsConfig

log = logging.getLogger(__name__)

EMBEDDING_SCHEMA = StructType(
    [
        StructField("item_id", IntegerType(), False),
        StructField("vector", ArrayType(FloatType(), False), False),
        StructField("count", LongType(), False),
    ]
)


def _stable_hash(s: str) -> int:
    """Deterministic replacement for Python's per-process randomized ``hash``."""
    return zlib.crc32(s.encode())


def session_sequences(history: DataFrame, min_session_items: int) -> list[list[str]]:
    """Collapsed item sequences per session, collected to the driver.

    Sorted by session id so the corpus order (and hence training) is deterministic.
    """
    rows = (
        history.groupBy("session_id")
        .agg(F.sort_array(F.collect_list(F.struct("ts_ms", "item_id"))).alias("_ev"))
        .where(F.size(F.array_distinct(F.col("_ev.item_id"))) >= min_session_items)
        .select("session_id", F.col("_ev.item_id").alias("items"))
        .orderBy("session_id")
        .collect()
    )
    sequences = []
    for r in rows:
        seq: list[str] = []
        for item in r["items"]:
            token = str(item)
            if not seq or seq[-1] != token:
                seq.append(token)
        sequences.append(seq)
    return sequences


@dataclass
class TrainedEmbeddings:
    item_ids: npt.NDArray[np.int64]
    vectors: npt.NDArray[np.float32]  # L2-normalized rows
    counts: npt.NDArray[np.int64]
    stats: dict[str, float] = field(default_factory=dict)


def train(sequences: list[list[str]], cfg: EmbeddingsConfig) -> TrainedEmbeddings:
    start = time.perf_counter()
    stats: dict[str, float] = {
        "sessions": len(sequences),
        "tokens": sum(len(s) for s in sequences),
    }
    token_counts = Counter(t for s in sequences for t in s)
    if not any(c >= cfg.min_count for c in token_counts.values()):
        log.warning("item2vec: no item reaches min_count=%d; no embeddings", cfg.min_count)
        return TrainedEmbeddings(
            np.zeros(0, np.int64),
            np.zeros((0, cfg.vector_size), np.float32),
            np.zeros(0, np.int64),
            stats={**stats, "vocab": 0, "train_seconds": 0.0},
        )
    model = Word2Vec(
        sentences=sequences,
        vector_size=cfg.vector_size,
        window=cfg.window,
        min_count=cfg.min_count,
        sg=1,
        hs=0,
        negative=cfg.negative,
        ns_exponent=cfg.ns_exponent,
        sample=cfg.sample,
        epochs=cfg.epochs,
        seed=cfg.seed,
        workers=1,
        hashfxn=_stable_hash,
    )
    wv = model.wv
    ids = np.array([int(t) for t in wv.index_to_key], dtype=np.int64)
    vectors = wv.get_normed_vectors().astype(np.float32)
    counts = np.array([wv.get_vecattr(t, "count") for t in wv.index_to_key], dtype=np.int64)
    order = np.argsort(ids)
    return TrainedEmbeddings(
        ids[order],
        vectors[order],
        counts[order],
        stats={
            **stats,
            "vocab": len(ids),
            "train_seconds": round(time.perf_counter() - start, 2),
        },
    )


def to_frame(spark: SparkSession, emb: TrainedEmbeddings) -> DataFrame:
    rows = [
        (int(i), v.tolist(), int(c))
        for i, v, c in zip(emb.item_ids, emb.vectors, emb.counts, strict=True)
    ]
    return spark.createDataFrame(rows, EMBEDDING_SCHEMA)


def from_frame(df: DataFrame) -> TrainedEmbeddings:
    rows = df.select("item_id", "vector", "count").orderBy("item_id").collect()
    if not rows:
        return TrainedEmbeddings(
            np.zeros(0, np.int64), np.zeros((0, 0), np.float32), np.zeros(0, np.int64)
        )
    return TrainedEmbeddings(
        np.array([r["item_id"] for r in rows], dtype=np.int64),
        np.array([r["vector"] for r in rows], dtype=np.float32),
        np.array([r["count"] for r in rows], dtype=np.int64),
    )

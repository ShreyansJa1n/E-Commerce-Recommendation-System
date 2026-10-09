"""item2vec candidates: nearest items to a visitor's decayed mean of recent item vectors.

user_vector = normalize(sum_s seed_weight(s) * v(s)) over the visitor's top recent
seed items that have an embedding. Seeds are excluded from the results, as in the
co-occurrence source (repeats come from ``recent_items``).
"""

from __future__ import annotations

from collections import defaultdict

import numpy as np
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType

from recsys.candidates.common import CANDIDATE_COLUMNS
from recsys.embeddings.index import ExactIndex, VectorIndex
from recsys.embeddings.item2vec import TrainedEmbeddings

_SCHEMA = StructType(
    [
        StructField("visitor_id", IntegerType(), False),
        StructField("item_id", IntegerType(), False),
        StructField("score", DoubleType(), False),
        StructField("rank", IntegerType(), False),
    ]
)


def user_vectors(
    seeds: DataFrame, emb: TrainedEmbeddings
) -> tuple[list[int], np.ndarray, list[set[int]]]:
    """(visitor ids, query matrix, seed sets) for visitors with >= 1 embedded seed."""
    position = {int(i): n for n, i in enumerate(emb.item_ids)}
    acc: dict[int, np.ndarray] = {}
    seed_sets: dict[int, set[int]] = defaultdict(set)
    for r in seeds.select("visitor_id", "item_id", "seed_weight").collect():
        v, i = int(r["visitor_id"]), int(r["item_id"])
        seed_sets[v].add(i)
        if i in position:
            vec = emb.vectors[position[i]] * float(r["seed_weight"])
            acc[v] = acc[v] + vec if v in acc else vec.astype(np.float32)
    visitors = sorted(acc)
    queries = (
        np.stack([acc[v] for v in visitors])
        if visitors
        else np.zeros((0, emb.vectors.shape[1] if emb.vectors.ndim == 2 else 0), np.float32)
    )
    return visitors, queries, [seed_sets[v] for v in visitors]


def recommend(
    spark: SparkSession,
    seeds: DataFrame,
    emb: TrainedEmbeddings,
    n: int,
    chunk: int,
    index: VectorIndex | None = None,
) -> DataFrame:
    visitors, queries, exclude = user_vectors(seeds, emb)
    if not visitors:
        return spark.createDataFrame([], _SCHEMA)
    index = index or ExactIndex(emb.item_ids, emb.vectors, chunk=chunk)
    results = index.search(queries, n, exclude)
    rows = [
        (v, h.item_id, h.score, rank)
        for v, hits in zip(visitors, results, strict=True)
        for rank, h in enumerate(hits, start=1)
    ]
    return spark.createDataFrame(rows, _SCHEMA).select(*CANDIDATE_COLUMNS)

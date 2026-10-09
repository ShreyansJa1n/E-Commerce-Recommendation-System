"""LightGBM LambdaRank training and Spark-side scoring."""

from __future__ import annotations

import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any

import lightgbm as lgb
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructField, StructType

from recsys.ranking.dataset import LABEL


def to_lgb_frame(
    pdf: pd.DataFrame, features: Sequence[str]
) -> tuple[pd.DataFrame, np.ndarray, np.ndarray]:
    """Sort by query and return (X, y, group sizes) in LightGBM's expected layout."""
    pdf = pdf.sort_values(["cutoff_date", "visitor_id", "item_id"], kind="mergesort")
    groups = pdf.groupby(["cutoff_date", "visitor_id"], sort=False).size().to_numpy()
    x = pdf[list(features)].astype("float32")
    y = pdf[LABEL].to_numpy()
    return x, y, groups


@dataclass
class TrainedRanker:
    booster: lgb.Booster
    features: list[str]
    best_iteration: int
    stats: dict[str, Any] = field(default_factory=dict)

    def importance(self) -> list[dict[str, Any]]:
        gain = self.booster.feature_importance("gain", iteration=self.best_iteration)
        split = self.booster.feature_importance("split", iteration=self.best_iteration)
        total = float(gain.sum()) or 1.0
        rows = [
            {"feature": f, "gain": float(g), "gain_share": float(g) / total, "splits": int(s)}
            for f, g, s in zip(self.features, gain, split, strict=True)
        ]
        return sorted(rows, key=lambda r: -r["gain"])


def train(
    train_pdf: pd.DataFrame,
    valid_pdf: pd.DataFrame,
    features: Sequence[str],
    params: dict[str, Any],
    num_boost_round: int,
    early_stopping_rounds: int,
    eval_at: int,
) -> TrainedRanker:
    xt, yt, gt = to_lgb_frame(train_pdf, features)
    xv, yv, gv = to_lgb_frame(valid_pdf, features)
    dtrain = lgb.Dataset(xt, yt, group=gt, feature_name=list(features), free_raw_data=True)
    dvalid = lgb.Dataset(xv, yv, group=gv, reference=dtrain)
    history: dict[str, Any] = {}
    start = time.perf_counter()
    booster = lgb.train(
        {**params, "eval_at": [eval_at]},
        dtrain,
        num_boost_round=num_boost_round,
        valid_sets=[dvalid],
        valid_names=["val"],
        callbacks=[
            lgb.early_stopping(early_stopping_rounds, verbose=False),
            lgb.record_evaluation(history),
        ],
    )
    curve = history["val"][f"ndcg@{eval_at}"]
    return TrainedRanker(
        booster,
        list(features),
        booster.best_iteration,
        stats={
            "train_rows": len(yt),
            "train_queries": len(gt),
            "train_positive_rows": int((yt > 0).sum()),
            "valid_rows": len(yv),
            "valid_queries": len(gv),
            "best_iteration": int(booster.best_iteration),
            f"valid_ndcg@{eval_at}_best": float(curve[booster.best_iteration - 1]),
            f"valid_ndcg@{eval_at}_first_iter": float(curve[0]),
            "train_seconds": round(time.perf_counter() - start, 2),
        },
    )


def score(
    examples: DataFrame, model_str: str, features: Sequence[str], best_iteration: int
) -> DataFrame:
    """Add ``ranker_score`` using the model inside Spark (mapInPandas, batched)."""
    feats = list(features)
    out_schema = StructType(
        [*examples.schema.fields, StructField("ranker_score", DoubleType(), False)]
    )

    def predict(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        booster = lgb.Booster(model_str=model_str)
        for pdf in batches:
            x = pdf[feats].astype("float32")
            pdf["ranker_score"] = booster.predict(x, num_iteration=best_iteration, num_threads=1)
            yield pdf

    return examples.mapInPandas(predict, out_schema)  # type: ignore[arg-type]


def top_n(scored: DataFrame, n: int, score_col: str = "ranker_score") -> DataFrame:
    w = Window.partitionBy("cutoff_date", "visitor_id").orderBy(
        F.col(score_col).desc(), F.col("item_id")
    )
    return (
        scored.withColumn("rank", F.row_number().over(w))
        .where(F.col("rank") <= n)
        .select(
            "cutoff_date", "split", "visitor_id", "item_id", F.col(score_col).alias("score"), "rank"
        )
    )

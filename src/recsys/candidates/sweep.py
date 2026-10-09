"""ALS hyperparameter sweep on the validation cutoff (warm visitors, any interaction).

Selection uses validation only; the test split is never touched here.
"""

from __future__ import annotations

import itertools
import json
import time
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from recsys.candidates import als
from recsys.config import Config
from recsys.eval.metrics import per_user_metrics, summarize
from recsys.features.split import cutoffs, history
from recsys.io import StageReport, read_table, timed_stage


def run(spark: SparkSession, cfg: Config) -> StageReport:
    gold = cfg.paths.resolved().gold
    cc = cfg.candidates
    (cut,) = [c for c in cutoffs(cfg.split) if c.split == "val"]
    with timed_stage("als_sweep") as report:
        events = read_table(spark, gold / "events_enriched")
        labels = read_table(spark, gold / "labels").where(
            (F.col("split") == "val") & F.col("visitor_has_history")
        )
        hist = history(events, cut)
        ratings = als.interactions(hist, cc.als, cfg.features.event_weights).cache()
        targets = labels.select("visitor_id").distinct().cache()
        results: list[dict[str, Any]] = []
        grid = itertools.product(cc.als_sweep.rank, cc.als_sweep.reg_param, cc.als_sweep.alpha)
        for rank, reg, alpha in grid:
            params = cc.als.model_copy(update={"rank": rank, "reg_param": reg, "alpha": alpha})
            start = time.perf_counter()
            model = als.train(ratings, params)
            recs = als.recommend(model, targets, cc.top_n).cache()
            metrics = summarize(per_user_metrics(recs, labels, cc.eval_ks), cc.eval_ks)
            seconds = round(time.perf_counter() - start, 2)
            recs.unpersist()
            row = {"rank": rank, "reg_param": reg, "alpha": alpha, "seconds": seconds}
            row |= {f"recall@{m['k']}": round(m["recall"], 5) for m in metrics}
            row |= {f"ndcg@{m['k']}": round(m["ndcg"], 5) for m in metrics}
            results.append(row)
            print(json.dumps(row))
        best_key = f"recall@{max(cc.eval_ks)}"
        report.extra = {
            "selection_metric": best_key,
            "segment": "warm visitors, relevance >= 1, val",
            "best": max(results, key=lambda r: r[best_key]),
            "results": results,
        }
        report.rows = {"ratings": ratings.count(), "targets": targets.count()}
    report.write(gold / "_reports")
    return report

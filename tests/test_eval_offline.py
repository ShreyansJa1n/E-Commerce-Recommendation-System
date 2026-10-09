from datetime import date

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from recsys.eval.offline import beyond_accuracy, per_user_table


def test_per_user_table_scores_missing_recs_as_zero(spark: SparkSession) -> None:
    recs = spark.createDataFrame(
        [
            ("a", date(2015, 9, 4), 1, 10, 1),
            ("a", date(2015, 9, 4), 1, 11, 2),
            ("b", date(2015, 9, 4), 2, 20, 1),
        ],
        "source string, cutoff_date date, visitor_id int, item_id int, rank int",
    )
    labels = spark.createDataFrame(
        [(1, 11, 1, True), (2, 20, 3, False), (3, 30, 1, False)],
        "visitor_id int, item_id int, relevance int, visitor_has_history boolean",
    )
    pu = per_user_table(recs, labels, ["a", "b"], [1, 2]).set_index("visitor_id")
    assert list(pu.index.sort_values()) == [1, 2, 3]
    assert pu.loc[1, "a__recall_at_2"] == 1.0 and pu.loc[1, "a__recall_at_1"] == 0.0
    assert pu.loc[2, "a__ndcg_at_10" if "a__ndcg_at_10" in pu else "a__ndcg_at_2"] == 0.0
    assert pu.loc[2, "b__hit_rate_at_1"] == 1.0 and pu.loc[3, "b__hit_rate_at_1"] == 0.0
    assert bool(pu.loc[1, "warm"]) and not bool(pu.loc[3, "warm"])


def test_beyond_accuracy(spark: SparkSession) -> None:
    recs = spark.createDataFrame(
        [("p", 1, 10, 1), ("p", 1, 11, 2), ("p", 2, 10, 1), ("p", 2, 12, 2), ("q", 1, 10, 1)],
        "source string, visitor_id int, item_id int, rank int",
    )
    items = spark.createDataFrame(
        [(10, 1, 50, 1), (11, 1, 10, 2), (12, 2, 0, None), (13, 2, 5, 3)],
        "item_id int, category_id int, n_visitors_30d bigint, popularity_rank_30d int",
    )
    got = beyond_accuracy(recs, items, 10)
    assert got.loc["p", "catalog_coverage_at_10"] == pytest.approx(3 / 4)
    # visitor 1: same category (0), visitor 2: different (1) -> mean 0.5
    assert got.loc["p", "diversity_at_10"] == pytest.approx(0.5)
    assert pd.isna(got.loc["q", "diversity_at_10"])  # single-item list
    assert (
        got.loc["p", "novelty_at_10"] > got.loc["q", "novelty_at_10"]
    )  # q only recommends the top item

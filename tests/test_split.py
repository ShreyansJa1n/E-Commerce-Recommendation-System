from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError
from pyspark.sql import SparkSession

from recsys.config import SplitConfig
from recsys.features.split import Cutoff, build_labels, cutoffs, history

REL = {"view": 1, "addtocart": 2, "transaction": 3}


def test_cutoffs_from_config() -> None:
    cfg = SplitConfig(
        train_cutoffs=[date(2015, 7, 24), date(2015, 8, 7)],
        val_start=date(2015, 8, 21),
        test_start=date(2015, 9, 4),
        end=date(2015, 9, 18),
        label_horizon_days=14,
    )
    assert [(c.split, c.cutoff_date, c.label_end_date) for c in cutoffs(cfg)] == [
        ("train", date(2015, 7, 24), date(2015, 8, 7)),
        ("train", date(2015, 8, 7), date(2015, 8, 21)),
        ("val", date(2015, 8, 21), date(2015, 9, 4)),
        ("test", date(2015, 9, 4), date(2015, 9, 18)),
    ]
    assert cutoffs(cfg)[0].ts == datetime(2015, 7, 24, tzinfo=UTC)


@pytest.mark.parametrize(
    "overrides",
    [
        {"train_cutoffs": [date(2015, 8, 14)]},  # label window runs into validation
        {"test_start": date(2015, 8, 1)},  # test before val
        {"train_cutoffs": [date(2015, 8, 1), date(2015, 7, 1)]},  # not increasing
    ],
)
def test_invalid_split_rejected(overrides: dict[str, object]) -> None:
    base: dict[str, object] = {
        "train_cutoffs": [date(2015, 7, 24)],
        "val_start": date(2015, 8, 21),
        "test_start": date(2015, 9, 4),
        "end": date(2015, 9, 18),
        "label_horizon_days": 14,
    }
    with pytest.raises(ValidationError):
        SplitConfig.model_validate(base | overrides)


CUT = Cutoff("val", date(2015, 6, 10), date(2015, 6, 17))


def _events(spark: SparkSession, rows: list[tuple[int, int, str, datetime]]):
    return spark.createDataFrame(
        rows, "visitor_id int, item_id int, event_type string, event_ts timestamp"
    )


def test_history_is_strictly_before_cutoff(spark: SparkSession) -> None:
    ev = _events(
        spark,
        [
            (1, 1, "view", datetime(2015, 6, 9, 23, 59, 59)),
            (1, 1, "view", datetime(2015, 6, 10)),  # exactly T -> not history
        ],
    )
    assert history(ev, CUT).count() == 1


def test_labels_window_grades_and_flags(spark: SparkSession) -> None:
    ev = _events(
        spark,
        [
            (1, 10, "view", datetime(2015, 6, 1)),  # history: makes (1,10) a repeat
            (1, 10, "view", datetime(2015, 6, 10)),  # window start inclusive
            (1, 10, "transaction", datetime(2015, 6, 12)),
            (1, 11, "addtocart", datetime(2015, 6, 16, 23)),
            (2, 10, "view", datetime(2015, 6, 11)),  # cold-start visitor
            (3, 12, "view", datetime(2015, 6, 17)),  # window end exclusive
        ],
    )
    got = {(r.visitor_id, r.item_id): r for r in build_labels(ev, CUT, REL).collect()}
    assert set(got) == {(1, 10), (1, 11), (2, 10)}
    r = got[(1, 10)]
    assert (r.relevance, r.n_label_events, r.viewed, r.purchased, r.carted) == (
        3,
        2,
        True,
        True,
        False,
    )
    assert r.is_repeat and r.visitor_has_history
    assert (got[(1, 11)].relevance, got[(1, 11)].is_repeat) == (2, False)
    assert not got[(2, 10)].visitor_has_history
    assert got[(1, 10)].first_label_ts == datetime(2015, 6, 10)

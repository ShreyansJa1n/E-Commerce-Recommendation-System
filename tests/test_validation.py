import json

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from recsys.clean import validation as v


@pytest.fixture
def df(spark: SparkSession):
    return spark.createDataFrame(
        [(1, "view", 5), (2, "view", 50), (2, "bogus", None), (None, "view", 7)],
        "id int, event string, n int",
    )


def test_row_checks_count_failures(df) -> None:
    report = v.run_checks(
        "t",
        df,
        [
            *v.not_null("id"),
            v.is_in("event", ["view"]),
            v.between("n", 0, 10),
            v.predicate("n_small", F.col("n") > 10, severity=v.Severity.WARN),
        ],
    )
    failed = {r.name: r.failed_rows for r in report.results if r.name != "min_rows(1)"}
    assert report.total_rows == 4
    assert failed == {"not_null(id)": 1, "is_in(event)": 1, "between(n, 0, 10)": 2, "n_small": 1}
    assert len(report.errors) == 3
    assert [r.name for r in report.warnings] == ["n_small"]


def test_unique_counts_extra_rows(df) -> None:
    report = v.run_checks("t", df, [v.unique("id"), v.unique("id", "event")])
    assert [r.failed_rows for r in report.results[1:]] == [1, 0]


def test_tolerance(df) -> None:
    report = v.run_checks("t", df, [v.predicate("id_null", F.col("id").isNull(), tolerance=0.25)])
    assert not report.errors


def test_raise_and_write(df, tmp_path) -> None:
    report = v.run_checks("t", df, v.not_null("id"))
    out = json.loads(report.write(tmp_path).read_text())
    assert out["passed"] is False
    with pytest.raises(v.ValidationError, match="not_null"):
        report.raise_on_error()


def test_empty_frame_passes(spark: SparkSession) -> None:
    empty = spark.createDataFrame([], "id int")
    report = v.run_checks("t", empty, [*v.not_null("id"), v.unique("id")], min_rows=0)
    assert report.total_rows == 0
    assert not report.errors


def test_empty_frame_fails_min_rows_by_default(spark: SparkSession) -> None:
    report = v.run_checks("t", spark.createDataFrame([], "id int"), v.not_null("id"))
    assert [r.name for r in report.errors] == ["min_rows(1)"]

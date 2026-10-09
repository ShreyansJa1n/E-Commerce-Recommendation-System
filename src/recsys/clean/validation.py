"""Small Deequ-style data-quality checks (ADR-002).

Each check is evaluated as one aggregate expression, so a whole suite costs one
pass over the data (plus one grouped pass per uniqueness check).
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


class Severity(StrEnum):
    ERROR = "error"
    WARN = "warn"


@dataclass(frozen=True)
class Check:
    """A row-level predicate (``failing`` marks bad rows) or a uniqueness constraint."""

    name: str
    failing: Column | None = None
    unique_on: tuple[str, ...] = ()
    severity: Severity = Severity.ERROR
    # Fraction of rows allowed to fail before the check fails.
    tolerance: float = 0.0


def not_null(*cols: str, severity: Severity = Severity.ERROR) -> list[Check]:
    return [Check(f"not_null({c})", F.col(c).isNull(), severity=severity) for c in cols]


def is_in(col: str, allowed: Sequence[str], severity: Severity = Severity.ERROR) -> Check:
    return Check(
        f"is_in({col})", ~F.col(col).isin(list(allowed)) | F.col(col).isNull(), severity=severity
    )


def between(col: str, lo: Any, hi: Any, severity: Severity = Severity.ERROR) -> Check:
    """``lo <= col < hi``; nulls fail."""
    return Check(
        f"between({col}, {lo}, {hi})",
        ~((F.col(col) >= F.lit(lo)) & (F.col(col) < F.lit(hi))) | F.col(col).isNull(),
        severity=severity,
    )


def unique(*cols: str, severity: Severity = Severity.ERROR) -> Check:
    return Check(f"unique({', '.join(cols)})", unique_on=tuple(cols), severity=severity)


def predicate(
    name: str, failing: Column, severity: Severity = Severity.ERROR, tolerance: float = 0.0
) -> Check:
    return Check(name, failing, severity=severity, tolerance=tolerance)


@dataclass
class CheckResult:
    name: str
    severity: str
    failed_rows: int
    total_rows: int
    tolerance: float

    @property
    def passed(self) -> bool:
        if self.total_rows == 0:
            return self.failed_rows == 0
        return self.failed_rows / self.total_rows <= self.tolerance


@dataclass
class ValidationReport:
    table: str
    total_rows: int
    results: list[CheckResult]

    @property
    def errors(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity == Severity.ERROR]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity == Severity.WARN]

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "total_rows": self.total_rows,
            "passed": not self.errors,
            "results": [asdict(r) | {"passed": r.passed} for r in self.results],
        }

    def write(self, directory: Path) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        out = directory / f"{self.table}.json"
        out.write_text(json.dumps(self.to_dict(), indent=2) + "\n")
        return out

    def raise_on_error(self) -> None:
        if self.errors:
            names = ", ".join(f"{r.name} ({r.failed_rows} rows)" for r in self.errors)
            raise ValidationError(f"{self.table}: failed checks: {names}")


class ValidationError(RuntimeError):
    pass


def run_checks(
    table: str, df: DataFrame, checks: Sequence[Check], min_rows: int = 1
) -> ValidationReport:
    """Evaluate ``checks``. A table with fewer than ``min_rows`` rows fails (error)."""
    row_checks = [c for c in checks if c.failing is not None]
    aggs = [F.count(F.lit(1)).alias("__total")] + [
        F.sum(c.failing.cast("long")).alias(f"__c{i}")  # type: ignore[union-attr]
        for i, c in enumerate(row_checks)
    ]
    row = df.agg(*aggs).collect()[0]
    total = int(row["__total"])

    results = [
        CheckResult(
            f"min_rows({min_rows})", Severity.ERROR.value, max(0, min_rows - total), total, 0.0
        )
    ]
    for i, c in enumerate(row_checks):
        failed = int(row[f"__c{i}"] or 0)
        results.append(CheckResult(c.name, c.severity.value, failed, total, c.tolerance))
    for c in checks:
        if c.unique_on:
            # Rows beyond the first per key count as failures.
            dupes = (
                df.groupBy(*c.unique_on)
                .count()
                .where(F.col("count") > 1)
                .agg(F.sum(F.col("count") - 1).alias("extra"))
                .collect()[0]["extra"]
            )
            results.append(
                CheckResult(c.name, c.severity.value, int(dupes or 0), total, c.tolerance)
            )
    return ValidationReport(table, total, results)

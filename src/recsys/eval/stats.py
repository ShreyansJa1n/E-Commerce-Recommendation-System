"""Bootstrap confidence intervals for offline policy comparisons (pure NumPy).

Two designs over per-user metric values:
- **paired**: both policies scored on the same users; resample users, compare means.
  This is the offline counterfactual: maximal power, no assignment noise.
- **A/B simulation**: users hashed into control/treatment arms (50/50); each arm is
  scored only with its own policy and resampled independently, which is what an
  online A/B test of the same size would observe.
Relative lift = mean(treatment) / mean(control) - 1. Intervals are percentile intervals.
"""

from __future__ import annotations

import zlib
from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
import numpy.typing as npt

Array = npt.NDArray[np.float64]


@dataclass(frozen=True)
class Interval:
    estimate: float
    low: float
    high: float

    @property
    def excludes_zero(self) -> bool:
        return self.low > 0 or self.high < 0


@dataclass(frozen=True)
class Comparison:
    control_mean: float
    treatment_mean: float
    n_control: int
    n_treatment: int
    abs_diff: Interval
    rel_lift: Interval
    # Share of bootstrap replicates with treatment - control <= 0 (one-sided p-value).
    p_not_better: float


def _ci(samples: Array, estimate: float, level: float) -> Interval:
    """Percentile interval; NaN when undefined (e.g. relative lift over a zero control)."""
    if len(samples) == 0 or np.isnan(estimate):
        return Interval(float("nan"), float("nan"), float("nan"))
    lo, hi = np.quantile(samples, [(1 - level) / 2, 1 - (1 - level) / 2])
    return Interval(float(estimate), float(lo), float(hi))


def _safe_lift(t: Array | float, c: Array | float) -> Array | float:
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(np.asarray(c) > 0, np.asarray(t) / np.asarray(c) - 1.0, np.nan)


def mean_ci(values: Array, n_boot: int, seed: int, level: float = 0.95) -> Interval:
    rng = np.random.default_rng(seed)
    n = len(values)
    boots = np.array([values[rng.integers(0, n, n)].mean() for _ in range(n_boot)])
    return _ci(boots, float(values.mean()), level)


def paired(
    control: Array, treatment: Array, n_boot: int, seed: int, level: float = 0.95
) -> Comparison:
    if control.shape != treatment.shape:
        raise ValueError("paired comparison needs per-user values for the same users")
    rng = np.random.default_rng(seed)
    n = len(control)
    diffs, lifts = np.empty(n_boot), np.empty(n_boot)
    for b in range(n_boot):
        idx = rng.integers(0, n, n)
        c, t = control[idx].mean(), treatment[idx].mean()
        diffs[b], lifts[b] = t - c, float(_safe_lift(t, c))
    c0, t0 = float(control.mean()), float(treatment.mean())
    return Comparison(
        c0,
        t0,
        n,
        n,
        _ci(diffs, t0 - c0, level),
        _ci(lifts[~np.isnan(lifts)], float(_safe_lift(t0, c0)), level),
        _share_not_better(diffs),
    )


def _share_not_better(diffs: Array) -> float:
    """One-sided bootstrap p-value on the absolute difference (defined even if control = 0)."""
    return float(np.mean(diffs <= 0))


def ab_arms(user_ids: Sequence[int], salt: str) -> npt.NDArray[np.bool_]:
    """Deterministic 50/50 assignment: True = treatment."""
    return np.array([zlib.crc32(f"{salt}:{u}".encode()) % 2 == 1 for u in user_ids])


def ab_simulation(
    control_arm: Array, treatment_arm: Array, n_boot: int, seed: int, level: float = 0.95
) -> Comparison:
    rng = np.random.default_rng(seed)
    nc, nt = len(control_arm), len(treatment_arm)
    diffs, lifts = np.empty(n_boot), np.empty(n_boot)
    for b in range(n_boot):
        c = control_arm[rng.integers(0, nc, nc)].mean()
        t = treatment_arm[rng.integers(0, nt, nt)].mean()
        diffs[b], lifts[b] = t - c, float(_safe_lift(t, c))
    c0, t0 = float(control_arm.mean()), float(treatment_arm.mean())
    ok = ~np.isnan(lifts)
    return Comparison(
        c0,
        t0,
        nc,
        nt,
        _ci(diffs, t0 - c0, level),
        _ci(lifts[ok], float(_safe_lift(t0, c0)), level),
        _share_not_better(diffs),
    )

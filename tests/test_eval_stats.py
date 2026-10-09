import numpy as np
import pytest

from recsys.eval import stats


def test_paired_detects_a_real_difference_and_not_a_null() -> None:
    rng = np.random.default_rng(0)
    control = rng.binomial(1, 0.10, 4000).astype(float)
    better = np.clip(control + rng.binomial(1, 0.05, 4000), 0, 1)
    real = stats.paired(control, better, 500, 1)
    assert real.abs_diff.low > 0 and real.abs_diff.excludes_zero
    assert real.rel_lift.estimate == pytest.approx(better.mean() / control.mean() - 1)
    assert real.p_not_better < 0.01
    null = stats.paired(control, control.copy(), 500, 1)
    assert null.abs_diff.estimate == 0 and not null.abs_diff.excludes_zero


def test_paired_is_tighter_than_ab_for_correlated_policies() -> None:
    rng = np.random.default_rng(1)
    base = rng.binomial(1, 0.2, 6000).astype(float)
    treat = np.clip(base + rng.binomial(1, 0.02, 6000), 0, 1)
    arms = stats.ab_arms(range(6000), "s")
    pr = stats.paired(base, treat, 400, 2)
    ab = stats.ab_simulation(base[~arms], treat[arms], 400, 2)
    assert (pr.abs_diff.high - pr.abs_diff.low) < (ab.abs_diff.high - ab.abs_diff.low)


def test_ab_arms_deterministic_and_balanced() -> None:
    a = stats.ab_arms(range(20000), "salt-1")
    assert np.array_equal(a, stats.ab_arms(range(20000), "salt-1"))
    assert abs(a.mean() - 0.5) < 0.02
    assert not np.array_equal(a, stats.ab_arms(range(20000), "salt-2"))


def test_zero_control_gives_undefined_lift_not_a_crash() -> None:
    c = np.zeros(100)
    t = np.r_[np.ones(10), np.zeros(90)]
    out = stats.paired(c, t, 200, 3)
    assert np.isnan(out.rel_lift.estimate)
    assert out.abs_diff.estimate == pytest.approx(0.1)


def test_mean_ci_covers_mean() -> None:
    v = np.random.default_rng(4).normal(1.0, 1.0, 2000)
    ci = stats.mean_ci(v, 500, 5)
    assert ci.low < v.mean() < ci.high
    with pytest.raises(ValueError):
        stats.paired(np.zeros(3), np.zeros(4), 10, 0)

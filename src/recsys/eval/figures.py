"""Static figures for the evaluation report (matplotlib, light theme).

Palette and chrome follow the reference data-viz palette: one hue for single-series
charts, fixed categorical order for multi-series charts (validated for CVD separation),
recessive grid, text in ink colors, direct labels where colors fall below 3:1.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import matplotlib
import matplotlib.ticker

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

from recsys.config import Config

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
FOCUS, DIM = "#2a78d6", "#b7d3f6"
SEG_TITLE = {
    "all": "All test visitors",
    "warm": "Returning visitors",
    "cold": "New (cold-start) visitors",
}


def _style(ax: Any) -> None:
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(AXIS)
    ax.tick_params(colors=INK_2, labelsize=9)
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def _fig(ncols: int, width: float, height: float) -> tuple[Any, Any]:
    fig, axes = plt.subplots(1, ncols, figsize=(width, height), facecolor=SURFACE)
    return fig, axes


def ndcg_by_policy(result: dict[str, Any], cfg: Config, out: Path) -> Path:
    metric = cfg.evaluation.primary_metric
    fig, axes = _fig(3, 13, 4.2)
    for ax, seg in zip(axes, ("all", "warm", "cold"), strict=True):
        rows = [r for r in result["summary"] if r["segment"] == seg]
        if not rows:
            ax.set_visible(False)
            continue
        rows = sorted(rows, key=lambda r: r[metric])
        names = [r["policy"] for r in rows]
        vals = [r[metric] for r in rows]
        err = [
            [v - r[f"{metric}_ci"][0] for v, r in zip(vals, rows, strict=True)],
            [r[f"{metric}_ci"][1] - v for v, r in zip(vals, rows, strict=True)],
        ]
        colors = [FOCUS if n == "ranker" else DIM for n in names]
        ax.barh(names, vals, color=colors, height=0.62, edgecolor=SURFACE, linewidth=2)
        ax.errorbar(vals, names, xerr=err, fmt="none", ecolor=INK_2, elinewidth=1, capsize=2)
        for y, (v, r) in enumerate(zip(vals, rows, strict=True)):
            ax.text(
                r[f"{metric}_ci"][1],
                y,
                f"  {v:.4f}",
                va="center",
                ha="left",
                fontsize=8,
                color=INK_2,
            )
        _style(ax)
        ax.set_title(
            f"{SEG_TITLE[seg]} (n={rows[0]['users']:,})", fontsize=10, color=INK, loc="left"
        )
        top = max(r[f"{metric}_ci"][1] for r in rows)
        ax.set_xlim(0, top * 1.4 if top > 0 else 1)
        ax.set_xlabel("NDCG@10 (95% bootstrap CI)", fontsize=9, color=MUTED)
    fig.suptitle(
        f"NDCG@10 by policy, {result['split']} split (cutoff {result['cutoff_date']})",
        x=0.01,
        ha="left",
        fontsize=12,
        color=INK,
    )
    fig.tight_layout()
    path = out / "ndcg_by_policy.png"
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)
    return path


def lift_forest(result: dict[str, Any], cfg: Config, out: Path) -> Path:
    """Absolute NDCG@10 difference (treatment - control) with 95% CI; relative lift in the
    label. Absolute differences stay readable when a control is near zero."""
    metric = cfg.evaluation.primary_metric
    rows = [r for r in result["comparisons"] if r["metric"] == metric]
    fig, axes = _fig(3, 14, 4.0)
    for ax, seg in zip(axes, ("all", "warm", "cold"), strict=True):
        seg_rows = [r for r in rows if r["segment"] == seg]
        labels, ys, highs = [], [], []
        for i, (control, treatment) in enumerate(cfg.evaluation.comparisons):
            for j, design in enumerate(("paired", "ab_simulation")):
                found = [x for x in seg_rows if x["control"] == control and x["design"] == design]
                if not found or found[0]["abs_diff"]["estimate"] is None:
                    continue
                r = found[0]
                y = i * 2.6 + j
                d, lift = r["abs_diff"], r["rel_lift"]
                color = SERIES[0] if design == "paired" else SERIES[1]
                ax.plot(
                    [d["low"], d["high"]], [y, y], color=color, linewidth=2, solid_capstyle="round"
                )
                ax.plot(
                    d["estimate"],
                    y,
                    "o",
                    color=color,
                    markersize=6,
                    markeredgecolor=SURFACE,
                    markeredgewidth=1.5,
                )
                rel = "n/a" if lift["estimate"] is None else f"{lift['estimate'] * 100:+.0f}%"
                ax.text(
                    d["high"],
                    y,
                    f"  {d['estimate']:+.4f} ({rel})",
                    va="center",
                    fontsize=8,
                    color=INK_2,
                )
                highs.append(d["high"])
                if j == 0:
                    labels.append(f"{treatment} vs {control}")
                    ys.append(y + 0.5)
        lows = [
            r["abs_diff"]["low"]
            for r in seg_rows
            if r["control"] in {c for c, _ in cfg.evaluation.comparisons}
            and r["abs_diff"]["low"] is not None
        ]
        if not lows:
            ax.set_visible(False)
            continue
        lo, hi = min(0.0, *lows), max(0.0, *highs)
        span = hi - lo or 1.0
        ax.set_xlim(lo - 0.05 * span, hi + 0.75 * span)
        ax.axvline(0, color=AXIS, linewidth=1)
        ax.set_yticks(ys, labels)
        ax.invert_yaxis()
        ax.xaxis.set_major_locator(matplotlib.ticker.MaxNLocator(4))
        _style(ax)
        ax.set_title(SEG_TITLE[seg], fontsize=10, color=INK, loc="left")
        ax.set_xlabel("Δ NDCG@10, treatment − control (95% CI)", fontsize=9, color=MUTED)
    handles = [
        Line2D([], [], color=SERIES[0], marker="o", linewidth=2, label="paired (same visitors)"),
        Line2D([], [], color=SERIES[1], marker="o", linewidth=2, label="simulated 50/50 A/B"),
    ]
    fig.legend(
        handles=handles, loc="upper right", frameon=False, fontsize=9, labelcolor=INK_2, ncols=2
    )
    fig.suptitle(
        "Ranker vs. baselines: NDCG@10 difference", x=0.01, ha="left", fontsize=12, color=INK
    )
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    path = out / "lift_forest.png"
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)
    return path


def recall_at_k(result: dict[str, Any], cfg: Config, out: Path) -> Path:
    policies = [
        p
        for p in ("ranker", "blend", "recent_items", "popular_category", "als")
        if p in cfg.evaluation.policies
    ]
    ks = cfg.evaluation.ks
    fig, axes = _fig(2, 11, 4.2)
    for ax, seg in zip(axes, ("warm", "all"), strict=True):
        ends: list[tuple[float, str]] = []
        for color, p in zip(SERIES, policies, strict=False):
            found = [x for x in result["summary"] if x["policy"] == p and x["segment"] == seg]
            if not found:
                continue
            r = found[0]
            vals = [r[f"recall_at_{k}"] for k in ks]
            ax.plot(
                ks,
                vals,
                color=color,
                linewidth=2,
                marker="o",
                markersize=7,
                markeredgecolor=SURFACE,
                markeredgewidth=1.5,
                label=p,
            )
            ax.text(ks[-1], vals[-1], f"  {p}", va="center", fontsize=8, color=INK_2)
        _end_labels(ax, ends, ks[-1])
        ax.set_xscale("log")
        ax.set_xticks(ks, [str(k) for k in ks])
        ax.minorticks_off()
        _style(ax)
        ax.grid(axis="y", color=GRID, linewidth=0.8)
        ax.set_xlim(ks[0] * 0.85, ks[-1] * 2.6)
        ax.set_title(SEG_TITLE[seg], fontsize=10, color=INK, loc="left")
        ax.set_xlabel("K (list length)", fontsize=9, color=MUTED)
        ax.set_ylabel("Recall@K", fontsize=9, color=MUTED)
    handles, names = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        names,
        loc="lower center",
        ncols=len(names),
        frameon=False,
        fontsize=9,
        labelcolor=INK_2,
    )
    fig.suptitle(f"Recall@K, {result['split']} split", x=0.01, ha="left", fontsize=12, color=INK)
    fig.tight_layout(rect=(0, 0.07, 1, 1))
    path = out / "recall_at_k.png"
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)
    return path


def _end_labels(ax: Any, ends: list[tuple[float, str]], x: float) -> None:
    """Direct labels at line ends, nudged apart so they never overlap."""
    ax.figure.canvas.draw()
    lo, hi = ax.get_ylim()
    gap = (hi - lo) * 0.065
    placed: list[float] = []
    for y, name in sorted(ends):
        y_lab = max(y, placed[-1] + gap) if placed else y
        placed.append(y_lab)
        ax.text(x * 1.08, y_lab, name, va="center", fontsize=8, color=INK_2)


def render_all(result: dict[str, Any], cfg: Config, out: Path) -> list[Path]:
    out.mkdir(parents=True, exist_ok=True)
    return [
        ndcg_by_policy(result, cfg, out),
        lift_forest(result, cfg, out),
        recall_at_k(result, cfg, out),
    ]

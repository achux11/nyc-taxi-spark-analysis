"""Charts for the three questions, saved as PNG files for the README."""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.ticker  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

# Dashboard-style look: one strong blue, grey text, light gridlines, no chart borders.
BLUE, LIGHT = "#118DFF", "#9BCBFF"
TEXT, SUBTLE, GRID = "#252423", "#605E5C", "#E1DFDD"

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 10,
    "axes.edgecolor": GRID, "axes.labelcolor": SUBTLE,
    "xtick.color": SUBTLE, "ytick.color": SUBTLE,
    "axes.grid": True, "grid.color": GRID, "grid.linestyle": "-", "grid.linewidth": 0.8,
    "axes.axisbelow": True,
})


def _finish(ax, title: str, path: Path, grid_axis: str = "y") -> None:
    ax.set_title(title, fontsize=13, color=TEXT, loc="left", pad=14)
    ax.grid(axis="x" if grid_axis == "y" else "y", visible=False)
    for side in ["top", "right", "left", "bottom"]:
        ax.spines[side].set_visible(False)
    ax.tick_params(length=0)
    fig = ax.figure
    fig.patch.set_facecolor("white")
    fig.patch.set_edgecolor(GRID)
    fig.patch.set_linewidth(1.5)
    plt.tight_layout(pad=1.2)
    plt.savefig(path, dpi=150, edgecolor=fig.get_edgecolor())
    plt.close()


def rain_chart(rain: pd.DataFrame, path: Path) -> None:
    labels = [s.split(". ", 1)[-1] for s in rain["rain_level"]]
    fig, ax = plt.subplots(figsize=(7, 4))
    bars = ax.bar(labels, rain["avg_daily_trips"], color=[LIGHT] + [BLUE] * (len(labels) - 1), width=0.6)
    ax.bar_label(bars, labels=[f"{v / 1000:,.1f}K" for v in rain["avg_daily_trips"]], padding=4, color=TEXT)
    ax.set_ylabel("Average trips per day")
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1000:,.0f}K"))
    ax.set_ylim(0, rain["avg_daily_trips"].max() * 1.15)
    _finish(ax, "Taxi demand rises on rainy days", path)


def borough_chart(speed: pd.DataFrame, path: Path) -> None:
    df = speed.sort_values("speed_cost_corr")
    fig, ax = plt.subplots(figsize=(7, 4))
    bars = ax.barh(df["borough"], df["speed_cost_corr"], color=BLUE, height=0.6)
    ax.bar_label(bars, labels=[f"{v:+.3f}" for v in df["speed_cost_corr"]], padding=4, color=TEXT)
    ax.axvline(0, color=SUBTLE, linewidth=0.8)
    lim = max(0.05, df["speed_cost_corr"].abs().max() * 1.4)
    ax.set_xlim(-lim, lim)
    ax.set_xlabel("Correlation: trip speed vs. total cost")
    _finish(ax, "Trip speed barely changes the fare, in every borough", path, grid_axis="x")


def tip_chart(tips: pd.DataFrame, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.scatter(tips["tmax_c"], tips["avg_tip_pct"], color=BLUE, alpha=0.6)
    ax.set_xlabel("Daily high temperature (°C)")
    ax.set_ylabel("Average tip % (card trips)")
    _finish(ax, "Tip % stays flat whatever the temperature", path, grid_axis="both")

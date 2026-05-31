"""Backtest artifact export helpers."""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from app.backtest.runner import BacktestResult


def plot_equity_curve(
    equity_curve_df,
    output_path: str | Path,
    *,
    title: str,
    subtitle: str | None = None,
) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    if equity_curve_df.is_empty():
        raise ValueError("equity_curve_df 为空，无法绘图")

    x = equity_curve_df.get_column("time").to_list()
    y = equity_curve_df.get_column("equity_curve").cast(float).to_list()

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(x, y, linewidth=2.0, color="#1f77b4")
    ax.set_title(title)
    if subtitle:
        ax.text(0.5, 1.02, subtitle, transform=ax.transAxes, ha="center", va="bottom", fontsize=10)
    ax.set_xlabel("Date")
    ax.set_ylabel("Equity Curve")
    ax.grid(alpha=0.3)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(output, dpi=160)
    plt.close(fig)
    return output


def export_backtest_artifacts(
    result: BacktestResult,
    *,
    artifacts_dir: str | Path,
    chart_title: str,
    chart_subtitle: str | None = None,
    save_chart: bool = True,
) -> BacktestResult:
    base_dir = Path(artifacts_dir)
    base_dir.mkdir(parents=True, exist_ok=True)

    returns_path = base_dir / "returns.csv"
    holdings_path = base_dir / "holdings.csv"
    trades_path = base_dir / "trades.csv"
    equity_curve_path = base_dir / "equity_curve.csv"

    result.returns_df.write_csv(returns_path)
    result.holdings_df.write_csv(holdings_path)
    result.trades_df.write_csv(trades_path)
    result.equity_curve_df.write_csv(equity_curve_path)

    chart_path = None
    if save_chart and not result.equity_curve_df.is_empty():
        chart_path = str(
            plot_equity_curve(
                result.equity_curve_df,
                base_dir / "equity.png",
                title=chart_title,
                subtitle=chart_subtitle,
            )
        )

    artifact_paths = {
        "returns_csv": str(returns_path),
        "holdings_csv": str(holdings_path),
        "trades_csv": str(trades_path),
        "equity_curve_csv": str(equity_curve_path),
    }
    if chart_path:
        artifact_paths["equity_chart_png"] = chart_path

    return replace(
        result,
        artifacts_dir=str(base_dir),
        equity_chart_path=chart_path,
        artifact_paths=artifact_paths,
    )

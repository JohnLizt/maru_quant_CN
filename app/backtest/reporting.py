"""Backtest artifact export helpers."""
from __future__ import annotations

import json
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import polars as pl
from loguru import logger

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


def _empty_periods_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "period_index": pl.Int64,
            "signal_date": pl.Date,
            "effective_date": pl.Date,
            "period_end_date": pl.Date,
            "holding_days": pl.Int64,
            "selected_symbols": pl.Utf8,
            "selected_tags": pl.Utf8,
            "start_nav": pl.Float64,
            "end_nav": pl.Float64,
            "period_return": pl.Float64,
            "gross_return_sum": pl.Float64,
            "net_return_sum": pl.Float64,
            "turnover_sum": pl.Float64,
            "trade_count": pl.Int64,
            "fee_cost_sum": pl.Float64,
            "top_contributor_symbol": pl.Utf8,
            "top_contributor_pnl": pl.Float64,
            "worst_contributor_symbol": pl.Utf8,
            "worst_contributor_pnl": pl.Float64,
        }
    )


def _empty_period_holdings_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "period_index": pl.Int64,
            "signal_date": pl.Date,
            "effective_date": pl.Date,
            "period_end_date": pl.Date,
            "asset_type": pl.Utf8,
            "symbol": pl.Utf8,
            "tag": pl.Utf8,
            "rank": pl.Int64,
            "target_weight": pl.Float64,
            "entry_price": pl.Float64,
            "exit_price_or_last_price": pl.Float64,
            "start_weight": pl.Float64,
            "end_weight": pl.Float64,
            "period_pnl": pl.Float64,
            "period_return_contribution": pl.Float64,
            "held_through_period": pl.Boolean,
            "exit_reason": pl.Utf8,
        }
    )


def _compact_rows(df: pl.DataFrame, limit: int) -> list[dict[str, Any]]:
    if df.is_empty():
        return []
    rows: list[dict[str, Any]] = []
    for row in df.head(limit).iter_rows(named=True):
        current: dict[str, Any] = {}
        for key, value in row.items():
            if hasattr(value, "isoformat"):
                current[key] = value.isoformat()
            else:
                current[key] = value
        rows.append(current)
    return rows


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def _latest_symbol_leaderboard(period_holdings_df: pl.DataFrame, descending: bool) -> list[dict[str, Any]]:
    if period_holdings_df.is_empty():
        return []
    grouped = (
        period_holdings_df.group_by("symbol")
        .agg(
            [
                pl.col("tag").drop_nulls().first().alias("tag"),
                pl.col("period_pnl").sum().alias("total_period_pnl"),
                pl.col("period_return_contribution").sum().alias("total_return_contribution"),
                pl.len().alias("period_count"),
            ]
        )
        .sort(["total_period_pnl", "symbol"], descending=[descending, False])
    )
    return _compact_rows(grouped, 5)


def build_rebalance_period_analysis(result: BacktestResult) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, Any]]:
    effective_decisions = result.effective_decisions_df
    if (
        effective_decisions is None
        or effective_decisions.is_empty()
        or result.returns_df.is_empty()
    ):
        return _empty_periods_frame(), _empty_period_holdings_frame(), {
            "best_periods": [],
            "worst_periods": [],
            "recent_period": None,
            "recent_period_holdings": [],
            "top_symbols": [],
            "bottom_symbols": [],
        }

    returns_rows = result.returns_df.sort("time").to_dicts()
    trading_dates = [row["time"] for row in returns_rows]
    nav_by_date = {row["time"]: float(row["nav"]) for row in returns_rows}
    returns_by_date = {row["time"]: row for row in returns_rows}
    trading_index = {current: idx for idx, current in enumerate(trading_dates)}
    holdings_lookup = {
        (row["time"], row["asset_type"], row["symbol"]): row
        for row in result.holdings_df.to_dicts()
    }
    trades_rows = result.trades_df.to_dicts()
    initial_capital = float(result.metrics.get("initial_capital", nav_by_date.get(trading_dates[0], 0.0)))

    effective_rows = sorted(
        effective_decisions.to_dicts(),
        key=lambda row: (row["effective_date"], row["rank"], row["symbol"]),
    )
    by_period: dict[date, list[dict[str, Any]]] = {}
    signal_date_by_effective: dict[date, date] = {}
    for row in effective_rows:
        effective_date = row["effective_date"]
        by_period.setdefault(effective_date, []).append(row)
        signal_date_by_effective.setdefault(effective_date, row["signal_date"])

    effective_dates = sorted(by_period)
    periods_rows: list[dict[str, Any]] = []
    period_holdings_rows: list[dict[str, Any]] = []

    for index, effective_date in enumerate(effective_dates, start=1):
        signal_date = signal_date_by_effective[effective_date]
        current_rows = by_period[effective_date]
        next_effective_date = effective_dates[index] if index < len(effective_dates) else None
        if next_effective_date is not None:
            next_idx = trading_index[next_effective_date]
            period_end_date = trading_dates[max(0, next_idx - 1)]
        else:
            period_end_date = trading_dates[-1]

        start_idx = trading_index[effective_date]
        prev_date = trading_dates[start_idx - 1] if start_idx > 0 else None
        start_nav = float(nav_by_date[prev_date]) if prev_date is not None else initial_capital
        end_nav = float(nav_by_date[period_end_date])
        period_dates = [current for current in trading_dates if effective_date <= current <= period_end_date]
        period_returns = [returns_by_date[current] for current in period_dates]
        period_trades = [
            row
            for row in trades_rows
            if effective_date <= row["time"] <= period_end_date
        ]

        next_symbols = (
            {(str(row["asset_type"]), str(row["symbol"])) for row in by_period[next_effective_date]}
            if next_effective_date is not None
            else set()
        )

        selected_symbols = [str(row["symbol"]) for row in current_rows]
        selected_tags = _dedupe_keep_order([str(row.get("tag", "") or "") for row in current_rows])

        for decision_row in current_rows:
            asset_type = str(decision_row["asset_type"])
            symbol = str(decision_row["symbol"])
            symbol_trades = [
                row
                for row in period_trades
                if str(row["asset_type"]) == asset_type and str(row["symbol"]) == symbol
            ]
            buy_trade = next((row for row in symbol_trades if row["side"] == "buy" and row["time"] == effective_date), None)
            sell_trades = [row for row in symbol_trades if row["side"] == "sell"]
            last_sell_trade = sell_trades[-1] if sell_trades else None
            start_holding = holdings_lookup.get((effective_date, asset_type, symbol))
            end_holding = holdings_lookup.get((period_end_date, asset_type, symbol))

            start_value = float(start_holding["market_value"]) if start_holding is not None else 0.0
            end_value = float(end_holding["market_value"]) if end_holding is not None else 0.0
            realized_sell_value = sum(float(row["notional"]) - float(row["fee"]) for row in sell_trades)
            period_pnl = end_value + realized_sell_value - start_value

            if any(str(row["action"]) == "止损" for row in sell_trades):
                exit_reason = "stop_loss"
            elif any(str(row["action"]) == "风险半仓" for row in sell_trades):
                exit_reason = "risk_half_sell"
            elif next_effective_date is not None and (asset_type, symbol) not in next_symbols:
                exit_reason = "rebalance_out"
            else:
                exit_reason = "period_end"

            entry_price = None
            if buy_trade is not None:
                entry_price = float(buy_trade["price"])
            elif start_holding is not None:
                entry_price = float(start_holding["close"])

            exit_price = None
            if last_sell_trade is not None:
                exit_price = float(last_sell_trade["price"])
            elif end_holding is not None:
                exit_price = float(end_holding["close"])

            period_holdings_rows.append(
                {
                    "period_index": index,
                    "signal_date": signal_date,
                    "effective_date": effective_date,
                    "period_end_date": period_end_date,
                    "asset_type": asset_type,
                    "symbol": symbol,
                    "tag": str(decision_row.get("tag", "") or ""),
                    "rank": int(decision_row["rank"]),
                    "target_weight": float(decision_row["target_weight"]),
                    "entry_price": entry_price,
                    "exit_price_or_last_price": exit_price,
                    "start_weight": float(start_holding["weight"]) if start_holding is not None else 0.0,
                    "end_weight": float(end_holding["weight"]) if end_holding is not None else 0.0,
                    "period_pnl": period_pnl,
                    "period_return_contribution": period_pnl / start_nav if start_nav > 0 else 0.0,
                    "held_through_period": end_holding is not None,
                    "exit_reason": exit_reason,
                }
            )

        current_holdings_rows = [row for row in period_holdings_rows if row["period_index"] == index]
        top_row = max(current_holdings_rows, key=lambda row: row["period_pnl"]) if current_holdings_rows else None
        worst_row = min(current_holdings_rows, key=lambda row: row["period_pnl"]) if current_holdings_rows else None

        periods_rows.append(
            {
                "period_index": index,
                "signal_date": signal_date,
                "effective_date": effective_date,
                "period_end_date": period_end_date,
                "holding_days": len(period_dates),
                "selected_symbols": json.dumps(selected_symbols, ensure_ascii=False),
                "selected_tags": json.dumps(selected_tags, ensure_ascii=False),
                "start_nav": start_nav,
                "end_nav": end_nav,
                "period_return": end_nav / start_nav - 1.0 if start_nav > 0 else 0.0,
                "gross_return_sum": float(sum(float(row["gross_return"]) for row in period_returns)),
                "net_return_sum": float(sum(float(row["net_return"]) for row in period_returns)),
                "turnover_sum": float(sum(float(row["turnover"]) for row in period_returns)),
                "trade_count": len(period_trades),
                "fee_cost_sum": float(sum(float(row["fee"]) for row in period_trades)),
                "top_contributor_symbol": str(top_row["symbol"]) if top_row is not None else "",
                "top_contributor_pnl": float(top_row["period_pnl"]) if top_row is not None else 0.0,
                "worst_contributor_symbol": str(worst_row["symbol"]) if worst_row is not None else "",
                "worst_contributor_pnl": float(worst_row["period_pnl"]) if worst_row is not None else 0.0,
            }
        )

    periods_df = pl.DataFrame(periods_rows) if periods_rows else _empty_periods_frame()
    period_holdings_df = pl.DataFrame(period_holdings_rows) if period_holdings_rows else _empty_period_holdings_frame()
    if not periods_df.is_empty():
        periods_df = periods_df.sort("period_index")
    if not period_holdings_df.is_empty():
        period_holdings_df = period_holdings_df.sort(["period_index", "rank", "symbol"])

    recent_period = periods_df.tail(1) if not periods_df.is_empty() else _empty_periods_frame()
    recent_period_index = int(recent_period.get_column("period_index").item()) if not recent_period.is_empty() else None
    recent_holdings = (
        period_holdings_df.filter(pl.col("period_index") == recent_period_index)
        if recent_period_index is not None
        else _empty_period_holdings_frame()
    )

    summary = {
        "best_periods": _compact_rows(periods_df.sort(["period_return", "period_index"], descending=[True, False]), 5),
        "worst_periods": _compact_rows(periods_df.sort(["period_return", "period_index"], descending=[False, False]), 5),
        "recent_period": _compact_rows(recent_period, 1)[0] if not recent_period.is_empty() else None,
        "recent_period_holdings": _compact_rows(recent_holdings, 10),
        "top_symbols": _latest_symbol_leaderboard(period_holdings_df, True),
        "bottom_symbols": _latest_symbol_leaderboard(period_holdings_df, False),
    }
    return periods_df, period_holdings_df, summary


def _format_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.2f}%"


def _log_backtest_analysis(
    result: BacktestResult,
    periods_df: pl.DataFrame,
    period_holdings_df: pl.DataFrame,
    summary: dict[str, Any],
    *,
    context: dict[str, Any] | None = None,
) -> None:
    context = context or {}
    logger.info("=== 回测摘要 ===")
    logger.info(
        "  区间: {} -> {} | universe={} | profile={} | top_n={} | max_per_tag={} | fees={}bps | risk_control={}",
        context.get("start_date", "n/a"),
        context.get("end_date", "n/a"),
        context.get("universe", "n/a"),
        context.get("profile_name", "n/a"),
        context.get("top_n", "n/a"),
        context.get("max_per_tag", "n/a"),
        context.get("total_fee_bps", "n/a"),
        context.get("risk_control", "n/a"),
    )
    logger.info(
        "  期末NAV={:.4f} | 总收益={} | 年化收益={} | 最大回撤={} | Sharpe={:.4f}",
        float(result.metrics.get("end_nav", 0.0)),
        _format_pct(float(result.metrics.get("total_return", 0.0))),
        _format_pct(float(result.metrics.get("annualized_return", 0.0))),
        _format_pct(float(result.metrics.get("max_drawdown", 0.0))),
        float(result.metrics.get("sharpe", 0.0)),
    )

    logger.info("=== 核心利润来源（按调仓周期） ===")
    if periods_df.is_empty():
        logger.info("  无可分析调仓周期")
    else:
        avg_period_return = float(periods_df.get_column("period_return").mean())
        win_count = int(periods_df.filter(pl.col("period_return") > 0).height)
        loss_count = int(periods_df.filter(pl.col("period_return") <= 0).height)
        total_fee_cost = float(periods_df.get_column("fee_cost_sum").sum())
        avg_turnover = float(periods_df.get_column("turnover_sum").mean())
        logger.info(
            "  周期数={} | 平均周期收益={} | 盈利周期={} | 亏损周期={} | 总费用={:.4f} | 平均周期换手={:.4f}",
            periods_df.height,
            _format_pct(avg_period_return),
            win_count,
            loss_count,
            total_fee_cost,
            avg_turnover,
        )
        logger.info("  最佳5个周期:")
        for row in summary.get("best_periods", []):
            logger.info(
                "    #{} {} -> {} | return={} | top={} ({:.4f})",
                row["period_index"],
                row["effective_date"],
                row["period_end_date"],
                _format_pct(float(row["period_return"])),
                row["top_contributor_symbol"],
                float(row["top_contributor_pnl"]),
            )
        logger.info("  最差5个周期:")
        for row in summary.get("worst_periods", []):
            logger.info(
                "    #{} {} -> {} | return={} | worst={} ({:.4f})",
                row["period_index"],
                row["effective_date"],
                row["period_end_date"],
                _format_pct(float(row["period_return"])),
                row["worst_contributor_symbol"],
                float(row["worst_contributor_pnl"]),
            )

    logger.info("=== 最近调仓周期详情 ===")
    recent_period = summary.get("recent_period")
    if recent_period is None:
        logger.info("  无最近调仓周期")
    else:
        recent_period_index = int(recent_period["period_index"])
        recent_trades = (
            result.trades_df.filter(
                (pl.col("time") >= pl.lit(date.fromisoformat(recent_period["effective_date"])))
                & (pl.col("time") <= pl.lit(date.fromisoformat(recent_period["period_end_date"])))
            )
            if not result.trades_df.is_empty()
            else result.trades_df
        )
        logger.info(
            "  周期 #{} | signal={} | effective={} | end={} | return={} | trade_count={}",
            recent_period_index,
            recent_period["signal_date"],
            recent_period["effective_date"],
            recent_period["period_end_date"],
            _format_pct(float(recent_period["period_return"])),
            int(recent_period["trade_count"]),
        )
        for row in summary.get("recent_period_holdings", []):
            logger.info(
                "    {} | rank={} | tag={} | target_weight={:.2%} | pnl={:.4f} | contrib={} | exit_reason={}",
                row["symbol"],
                int(row["rank"]),
                row["tag"],
                float(row["target_weight"]),
                float(row["period_pnl"]),
                _format_pct(float(row["period_return_contribution"])),
                row["exit_reason"],
            )
        logger.info(
            "  stop_loss_events={} | risk_half_sell_events={}",
            int(recent_trades.filter(pl.col("action") == "止损").height) if not recent_trades.is_empty() else 0,
            int(recent_trades.filter(pl.col("action") == "风险半仓").height) if not recent_trades.is_empty() else 0,
        )

    logger.info("=== 全年主要贡献标的 ===")
    logger.info("  正贡献 Top5:")
    for row in summary.get("top_symbols", []):
        logger.info(
            "    {} | tag={} | pnl={:.4f} | contrib={} | periods={}",
            row["symbol"],
            row["tag"],
            float(row["total_period_pnl"]),
            _format_pct(float(row["total_return_contribution"])),
            int(row["period_count"]),
        )
    logger.info("  负贡献 Top5:")
    for row in summary.get("bottom_symbols", []):
        logger.info(
            "    {} | tag={} | pnl={:.4f} | contrib={} | periods={}",
            row["symbol"],
            row["tag"],
            float(row["total_period_pnl"]),
            _format_pct(float(row["total_return_contribution"])),
            int(row["period_count"]),
        )


def export_backtest_artifacts(
    result: BacktestResult,
    *,
    artifacts_dir: str | Path,
    chart_title: str,
    chart_subtitle: str | None = None,
    save_chart: bool = True,
    write_artifacts: bool = True,
    summary_context: dict[str, Any] | None = None,
) -> BacktestResult:
    base_dir = Path(artifacts_dir)
    base_dir.mkdir(parents=True, exist_ok=True)

    periods_df, period_holdings_df, analysis_summary = build_rebalance_period_analysis(result)
    _log_backtest_analysis(result, periods_df, period_holdings_df, analysis_summary, context=summary_context)

    artifact_paths = dict(result.artifact_paths or {})
    chart_path = result.equity_chart_path
    if write_artifacts:
        returns_path = base_dir / "returns.csv"
        holdings_path = base_dir / "holdings.csv"
        trades_path = base_dir / "trades.csv"
        equity_curve_path = base_dir / "equity_curve.csv"
        periods_path = base_dir / "rebalance_periods.csv"
        period_holdings_path = base_dir / "rebalance_period_holdings.csv"

        result.returns_df.write_csv(returns_path)
        result.holdings_df.write_csv(holdings_path)
        result.trades_df.write_csv(trades_path)
        result.equity_curve_df.write_csv(equity_curve_path)
        periods_df.write_csv(periods_path)
        period_holdings_df.write_csv(period_holdings_path)

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
            "rebalance_periods_csv": str(periods_path),
            "rebalance_period_holdings_csv": str(period_holdings_path),
        }
        if chart_path:
            artifact_paths["equity_chart_png"] = chart_path

    return replace(
        result,
        artifacts_dir=str(base_dir),
        equity_chart_path=chart_path,
        artifact_paths=artifact_paths or None,
        rebalance_periods_df=periods_df,
        rebalance_period_holdings_df=period_holdings_df,
        analysis_summary=analysis_summary,
    )

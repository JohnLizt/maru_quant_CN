"""Backtest pure QQQ with an in-strategy trailing stop."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl
from loguru import logger
from sqlalchemy import text


REPO_ROOT = Path(__file__).resolve().parents[1]
for candidate in ["/app", str(REPO_ROOT)]:
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from app.backtest.costs import DEFAULT_COMMISSION_BPS, DEFAULT_SLIPPAGE_BPS
from app.backtest.reporting import export_backtest_artifacts
from app.backtest.runner import run_backtest
from app.utils.price_adjustment import apply_price_adjustment
from app.strategy.qqq_enhanced import QQQOnlyTrailingStopStrategy
from app.utils.db import get_engine
from app.utils.logging import build_timestamped_prefix, configure_task_logger, ensure_log_directories


def _normalize_date(value: str | date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def _load_qqq_snapshot(start_date: date, end_date: date) -> pl.DataFrame:
    sql = text(
        """
        SELECT time, asset_type, symbol, close, adj_factor
        FROM market.daily
        WHERE asset_type = 'etf_US'
          AND symbol = 'QQQ'
          AND time >= :start_date
          AND time <= :end_date
        ORDER BY time
        """
    )
    with get_engine().connect() as conn:
        rows = conn.execute(
            sql,
            {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
        ).fetchall()
    if not rows:
        raise RuntimeError("未加载到 QQQ 行情，无法生成 trailing stop decision")

    return (
        apply_price_adjustment(
            pl.DataFrame(
                rows,
                schema=["time", "asset_type", "symbol", "close", "adj_factor"],
                orient="row",
            )
        )
        .with_columns(
            [
                pl.col("time").cast(pl.Datetime("us", "UTC")),
                pl.col("asset_type").cast(pl.Utf8),
                pl.col("symbol").cast(pl.Utf8),
                pl.col("close").cast(pl.Float64),
                pl.lit("cross_sectional").alias("signal_mode"),
                pl.lit("nasdaq_100").alias("tag"),
                pl.lit(1.0).alias("composite_score"),
            ]
        )
        .select(["time", "asset_type", "signal_mode", "symbol", "tag", "composite_score", "close"])
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Backtest pure QQQ plus trailing stop")
    parser.add_argument("--start-date", default="2016-07-13")
    parser.add_argument("--end-date", default="2026-07-12")
    parser.add_argument("--trailing-stop-rate", type=float, default=0.10)
    parser.add_argument("--trailing-peak-window", type=int, default=252)
    parser.add_argument("--rebalance-weekday", type=int, default=2)
    parser.add_argument("--execution-lag", type=int, default=1)
    parser.add_argument("--commission-bps", type=float, default=DEFAULT_COMMISSION_BPS)
    parser.add_argument("--slippage-bps", type=float, default=DEFAULT_SLIPPAGE_BPS)
    parser.add_argument("--initial-capital", type=float, default=40000.0)
    parser.add_argument("--commission-min", type=float, default=0.01)
    parser.add_argument("--cash-interest-rate", type=float, default=0.01)
    parser.add_argument("--output-dir", default="logs/backtest/qqq_only_trailing_stop")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    start_date = _normalize_date(args.start_date)
    end_date = _normalize_date(args.end_date)

    ensure_log_directories("logs")
    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    run_dir = output_root / build_timestamped_prefix("qqq_only_trailing_stop")
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = configure_task_logger(
        log_path=run_dir / "backtest.log",
        file_level=args.log_level,
        console_level="INFO",
        enable_console=True,
    )

    logger.info(
        "QQQ-only trailing stop 回测启动 | start={} | end={} | trailing_stop={} | peak_window={} | rebalance_weekday={} | execution_lag={}",
        start_date,
        end_date,
        args.trailing_stop_rate,
        args.trailing_peak_window,
        args.rebalance_weekday,
        args.execution_lag,
    )

    snapshot = _load_qqq_snapshot(start_date, end_date)
    strategy = QQQOnlyTrailingStopStrategy(
        trailing_stop_rate=args.trailing_stop_rate,
        trailing_peak_window=args.trailing_peak_window,
    )
    decisions = strategy.build_decisions(snapshot)
    result = run_backtest(
        decisions,
        asset_type="etf_US",
        start=start_date,
        end=end_date,
        rebalance_frequency="weekly",
        rebalance_weekday=args.rebalance_weekday,
        execution_lag=args.execution_lag,
        commission_bps=args.commission_bps,
        slippage_bps=args.slippage_bps,
        risk_config=None,
        initial_capital=args.initial_capital,
        commission_min=args.commission_min,
        cash_interest_rate=args.cash_interest_rate,
    )
    result = export_backtest_artifacts(
        result,
        artifacts_dir=run_dir,
        chart_title="QQQ Only + Trailing Stop Equity Curve",
        chart_subtitle=(
            f"stop={args.trailing_stop_rate:.0%} | peak_window={args.trailing_peak_window}d | "
            f"weekly weekday={args.rebalance_weekday} | fees={args.commission_bps + args.slippage_bps}bps"
        ),
        save_chart=True,
        write_artifacts=True,
        summary_context={
            "strategy": strategy.strategy_name,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "universe": "QQQ only",
            "profile_name": strategy.profile_name,
            "top_n": 1,
            "max_per_tag": 1,
            "risk_control": "in_strategy_trailing_stop",
            "trailing_stop_rate": args.trailing_stop_rate,
            "trailing_peak_window": args.trailing_peak_window,
            "rebalance_weekday": args.rebalance_weekday,
            "execution_lag": args.execution_lag,
            "total_fee_bps": args.commission_bps + args.slippage_bps,
        },
    )

    payload = {
        "run_dir": str(run_dir),
        "log_path": str(log_path),
        "artifact_paths": result.artifact_paths or {},
        "snapshot_rows": snapshot.height,
        "decision_rows": decisions.height,
        "metrics": result.metrics,
        "summary_preview": result.analysis_summary or {},
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

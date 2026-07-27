"""Backtest the first QQQ enhanced fixed-core strategy variant."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from datetime import timedelta
from pathlib import Path

import polars as pl
from loguru import logger
from sqlalchemy import text


REPO_ROOT = Path(__file__).resolve().parents[1]
for candidate in ["/app", str(REPO_ROOT)]:
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from app.backtest.reporting import export_backtest_artifacts
from app.backtest.runner import run_backtest
from app.strategy.qqq_enhanced import QQQEnhancedFixedCoreStrategy
from app.utils.db import get_engine
from app.utils.logging import build_timestamped_prefix, configure_task_logger, ensure_log_directories


SYMBOLS = ("QQQ", "XLK", "SMH", "GLD", "IEF", "UUP")


def _normalize_date(value: str | date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def _load_prices(start_date: date, end_date: date, *, lookback_days: int = 320) -> pl.DataFrame:
    query_start = start_date - timedelta(days=lookback_days)
    sql = text(
        """
        SELECT time, asset_type, symbol, close
        FROM market.daily
        WHERE asset_type = 'etf_US'
          AND symbol = ANY(:symbols)
          AND time >= :start_date
          AND time <= :end_date
        ORDER BY time, symbol
        """
    )
    with get_engine().connect() as conn:
        rows = conn.execute(
            sql,
            {
                "symbols": list(SYMBOLS),
                "start_date": str(query_start),
                "end_date": end_date.isoformat(),
            },
        ).fetchall()

    if not rows:
        raise RuntimeError("未加载到 QQQ enhanced 回测所需行情")

    return (
        pl.DataFrame(rows, schema=["time", "asset_type", "symbol", "close"], orient="row")
        .with_columns(
            [
                pl.col("time").cast(pl.Datetime("us", "UTC")),
                pl.col("asset_type").cast(pl.Utf8),
                pl.col("symbol").cast(pl.Utf8),
                pl.col("close").cast(pl.Float64),
            ]
        )
        .sort(["symbol", "time"])
    )


def _build_signal_snapshot(prices: pl.DataFrame, start_date: date, end_date: date) -> pl.DataFrame:
    scored = (
        prices.sort(["symbol", "time"])
        .with_columns(
            [
                pl.col("close").rolling_mean(window_size=200, min_samples=200).over("symbol").alias("ma200"),
                (pl.col("close") / pl.col("close").shift(60).over("symbol") - 1.0).alias("ret_60d"),
            ]
        )
        .with_columns(
            pl.when(pl.col("symbol") == "QQQ")
            .then(
                pl.when((pl.col("close") > pl.col("ma200")) & (pl.col("ret_60d") > 0.0))
                .then(pl.lit("risk_on"))
                .when((pl.col("close") < pl.col("ma200")) & (pl.col("ret_60d") < 0.0))
                .then(pl.lit("risk_off"))
                .otherwise(pl.lit("neutral"))
            )
            .otherwise(None)
            .alias("_qqq_regime")
        )
        .with_columns(pl.col("_qqq_regime").forward_fill().over("symbol"))
    )

    regimes = (
        scored.filter(pl.col("symbol") == "QQQ")
        .select(["time", pl.col("_qqq_regime").fill_null("neutral").alias("regime")])
        .sort("time")
    )
    tag_map = {
        "QQQ": "nasdaq_100",
        "XLK": "tech",
        "SMH": "semiconductor",
        "GLD": "gold",
        "IEF": "treasury_mid",
        "UUP": "usd",
    }
    snapshot = (
        scored.join(regimes, on="time", how="left")
        .filter((pl.col("time").dt.date() >= pl.lit(start_date)) & (pl.col("time").dt.date() <= pl.lit(end_date)))
        .with_columns(
            [
                pl.lit("cross_sectional").alias("signal_mode"),
                pl.col("ret_60d").fill_null(0.0).alias("composite_score"),
                pl.col("symbol").replace(tag_map).alias("tag"),
            ]
        )
        .select(["time", "asset_type", "signal_mode", "symbol", "tag", "composite_score", "regime"])
        .sort(["time", "symbol"])
    )
    if snapshot.is_empty():
        raise RuntimeError("生成 signal snapshot 为空")
    return snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description="Backtest QQQ enhanced fixed-core strategy")
    parser.add_argument("--start-date", default="2016-07-13")
    parser.add_argument("--end-date", default="2026-07-12")
    parser.add_argument("--rebalance-weekday", type=int, default=2)
    parser.add_argument("--execution-lag", type=int, default=1)
    parser.add_argument("--commission-bps", type=float, default=5.0)
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    parser.add_argument("--initial-capital", type=float, default=40000.0)
    parser.add_argument("--commission-min", type=float, default=0.01)
    parser.add_argument("--cash-interest-rate", type=float, default=0.01)
    parser.add_argument("--output-dir", default="logs/backtest/qqq_enhanced")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    start_date = _normalize_date(args.start_date)
    end_date = _normalize_date(args.end_date)

    ensure_log_directories("logs")
    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    run_dir = output_root / build_timestamped_prefix("qqq_enhanced_fixed_core")
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = configure_task_logger(
        log_path=run_dir / "backtest.log",
        file_level=args.log_level,
        console_level="INFO",
        enable_console=True,
    )

    logger.info(
        "QQQ Enhanced fixed-core 回测启动 | start={} | end={} | symbols={} | rebalance_weekday={} | execution_lag={}",
        start_date,
        end_date,
        ",".join(SYMBOLS),
        args.rebalance_weekday,
        args.execution_lag,
    )

    prices = _load_prices(start_date, end_date)
    snapshot = _build_signal_snapshot(prices, start_date, end_date)
    strategy = QQQEnhancedFixedCoreStrategy()
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
        initial_capital=args.initial_capital,
        commission_min=args.commission_min,
        cash_interest_rate=args.cash_interest_rate,
    )
    result = export_backtest_artifacts(
        result,
        artifacts_dir=run_dir,
        chart_title="QQQ Enhanced Fixed Core Equity Curve",
        chart_subtitle=(
            f"R1 | W1 | weekly weekday={args.rebalance_weekday} | "
            f"fees={args.commission_bps + args.slippage_bps}bps"
        ),
        save_chart=True,
        write_artifacts=True,
        summary_context={
            "strategy": strategy.strategy_name,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "regime_rule": "R1: QQQ > MA200 and 60d momentum > 0 => risk_on; inverse => risk_off",
            "weight_template": "W1 / section 6.1 fixed QQQ core",
            "symbols": list(SYMBOLS),
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

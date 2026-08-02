"""Run the faithful ETF-proxy Global Equities Momentum backtest."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl
from loguru import logger


REPO_ROOT = Path(__file__).resolve().parents[1]
for candidate in ["/app", str(REPO_ROOT)]:
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from app.backtest.baselines import build_cash_return_series
from app.backtest.costs import DEFAULT_COMMISSION_BPS, DEFAULT_SLIPPAGE_BPS
from app.backtest.reporting import export_backtest_artifacts
from app.backtest.runner import _load_market_data, run_backtest
from app.strategy.dual_momentum_gem import DualMomentumGEMStrategy
from app.utils.logging import build_timestamped_prefix, configure_task_logger, ensure_log_directories


US_SYMBOL = "SPY"
EX_US_SYMBOL = "ACWX"
DEFENSIVE_SYMBOL = "AGG"
T_BILL_SYMBOL = "BIL"
LOOKBACK_MONTHS = 12


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _warmup_start(start_date: date) -> date:
    return date(start_date.year - 2, 1, 1)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backtest faithful Dual Momentum GEM")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--execution-lag", type=int, default=1)
    parser.add_argument("--commission-bps", type=float, default=DEFAULT_COMMISSION_BPS)
    parser.add_argument("--slippage-bps", type=float, default=DEFAULT_SLIPPAGE_BPS)
    parser.add_argument("--initial-capital", type=float, default=40000.0)
    parser.add_argument("--commission-min", type=float, default=0.01)
    parser.add_argument("--cash-interest-rate", type=float, default=0.01)
    parser.add_argument("--output-dir", default="logs/experiments/dual_momentum_gem")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    start_date = _parse_date(args.start_date)
    end_date = _parse_date(args.end_date)
    if start_date > end_date:
        raise ValueError("start-date 不能晚于 end-date")

    ensure_log_directories("logs")
    run_dir = Path(args.output_dir) / build_timestamped_prefix("gem")
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = configure_task_logger(
        log_path=run_dir / "backtest.log",
        file_level=args.log_level,
        console_level="INFO",
        enable_console=True,
    )

    strategy = DualMomentumGEMStrategy()
    data_start = _warmup_start(start_date)
    logger.info(
        "GEM 回测启动 | start={} | end={} | data_start={} | assets={}/{}/{}/{} | "
        "lookback={}m | execution_lag={} | fee={}bps",
        start_date,
        end_date,
        data_start,
        US_SYMBOL,
        EX_US_SYMBOL,
        DEFENSIVE_SYMBOL,
        T_BILL_SYMBOL,
        LOOKBACK_MONTHS,
        args.execution_lag,
        args.commission_bps + args.slippage_bps,
    )

    market_data = _load_market_data(["etf_US"], data_start, end_date).filter(
        pl.col("symbol").is_in([US_SYMBOL, EX_US_SYMBOL, DEFENSIVE_SYMBOL, T_BILL_SYMBOL])
    )
    signal_snapshot = strategy.build_signal_snapshot(market_data).filter(
        (pl.col("time").dt.date() >= pl.lit(start_date))
        & (pl.col("time").dt.date() <= pl.lit(end_date))
    )
    if signal_snapshot.is_empty():
        raise RuntimeError("没有可用 GEM 月末信号，请检查四个 ETF 的历史数据和预热期")
    decisions = strategy.build_decisions(signal_snapshot)
    cash_returns = build_cash_return_series(market_data, symbol=T_BILL_SYMBOL)

    result = run_backtest(
        decisions,
        asset_type="etf_US",
        start=start_date,
        end=end_date,
        rebalance_frequency="monthly",
        execution_lag=args.execution_lag,
        commission_bps=args.commission_bps,
        slippage_bps=args.slippage_bps,
        initial_capital=args.initial_capital,
        commission_min=args.commission_min,
        cash_interest_rate=args.cash_interest_rate,
        cash_return_series=cash_returns,
        market_data_override=market_data,
    )
    result.log_path = str(log_path)
    result = export_backtest_artifacts(
        result,
        artifacts_dir=run_dir,
        chart_title="Dual Momentum GEM Equity Curve",
        chart_subtitle=(
            f"SPY/ACWX/AGG | 12m vs BIL | monthly | "
            f"fees={args.commission_bps + args.slippage_bps}bps"
        ),
        save_chart=True,
        write_artifacts=True,
        summary_context={
            "strategy": strategy.strategy_name,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "universe": "SPY/ACWX/AGG/BIL",
            "lookback_months": LOOKBACK_MONTHS,
            "rebalance_frequency": "monthly",
            "execution_lag": args.execution_lag,
            "total_fee_bps": args.commission_bps + args.slippage_bps,
        },
    )

    config = {
        "strategy": strategy.strategy_name,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "data_start": data_start.isoformat(),
        "us_symbol": US_SYMBOL,
        "ex_us_symbol": EX_US_SYMBOL,
        "defensive_symbol": DEFENSIVE_SYMBOL,
        "t_bill_symbol": T_BILL_SYMBOL,
        "lookback_months": LOOKBACK_MONTHS,
        "rebalance_frequency": "monthly",
        "execution_lag": args.execution_lag,
        "commission_bps": args.commission_bps,
        "slippage_bps": args.slippage_bps,
        "initial_capital": args.initial_capital,
    }
    (run_dir / "experiment_config.json").write_text(
        json.dumps(config, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    signal_snapshot.write_csv(run_dir / "monthly_signals.csv")
    decisions.write_csv(run_dir / "decisions.csv")

    payload = {
        "run_dir": str(run_dir),
        "log_path": str(log_path),
        "signal_rows": signal_snapshot.height,
        "decision_rows": decisions.height,
        "metrics": result.metrics,
        "artifact_paths": result.artifact_paths or {},
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

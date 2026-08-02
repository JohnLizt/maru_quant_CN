"""Run a single-asset, fully invested buy-and-hold backtest."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from loguru import logger
from sqlalchemy import text


REPO_ROOT = Path(__file__).resolve().parents[2]
for candidate in ["/app", str(REPO_ROOT)]:
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from app.backtest.baselines import build_buy_and_hold_decisions
from app.backtest.costs import DEFAULT_COMMISSION_BPS, DEFAULT_SLIPPAGE_BPS
from app.backtest.reporting import export_backtest_artifacts
from app.backtest.runner import run_backtest
from app.utils.db import get_engine
from app.utils.logging import build_timestamped_prefix, configure_task_logger, ensure_log_directories


def _normalize_date(value: str | date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _find_first_trading_date(
    engine,
    *,
    asset_type: str,
    symbol: str,
    start_date: date,
    end_date: date,
) -> date:
    with engine.connect() as conn:
        first_date = conn.execute(
            text(
                """
                SELECT MIN(time)::date
                FROM market.daily
                WHERE asset_type = :asset_type
                  AND symbol = :symbol
                  AND time::date BETWEEN :start_date AND :end_date
                """
            ),
            {
                "asset_type": asset_type,
                "symbol": symbol,
                "start_date": start_date,
                "end_date": end_date,
            },
        ).scalar_one_or_none()
    if first_date is None:
        raise RuntimeError(
            f"market.daily 没有 {asset_type}/{symbol} 在 {start_date}~{end_date} 的行情"
        )
    return first_date if isinstance(first_date, date) else first_date.date()


def main(args: argparse.Namespace) -> int:
    symbol = args.symbol.strip().upper()
    asset_type = args.asset_type.strip()
    start_date = _normalize_date(args.start_date)
    end_date = _normalize_date(args.end_date)
    if not symbol:
        raise ValueError("--symbol 不能为空")
    if start_date > end_date:
        raise ValueError("--start-date 不能晚于 --end-date")

    engine = get_engine()
    signal_date = _find_first_trading_date(
        engine,
        asset_type=asset_type,
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
    )

    ensure_log_directories("logs")
    output_root = Path(args.output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    run_dir = output_root / build_timestamped_prefix(f"{symbol}_buy_and_hold")
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = configure_task_logger(
        log_path=run_dir / "backtest.log",
        file_level=args.log_level,
        console_level="INFO",
        enable_console=True,
    )

    logger.info(
        "买入持有回测启动 | asset_type={} | symbol={} | requested={}~{} | signal_date={} | "
        "execution_lag={} | commission_bps={} | slippage_bps={} | commission_min={} | cash_interest_rate={}",
        asset_type,
        symbol,
        start_date,
        end_date,
        signal_date,
        args.execution_lag,
        args.commission_bps,
        args.slippage_bps,
        args.commission_min,
        args.cash_interest_rate,
    )

    strategy_name = f"buy_and_hold_{symbol.lower()}"
    decisions = build_buy_and_hold_decisions(
        symbol,
        signal_date,
        asset_type=asset_type,
        tag=args.tag,
        strategy_name=strategy_name,
    )
    result = run_backtest(
        decisions,
        asset_type=asset_type,
        start=start_date,
        end=end_date,
        rebalance_frequency="daily",
        execution_lag=args.execution_lag,
        commission_bps=args.commission_bps,
        slippage_bps=args.slippage_bps,
        risk_config=None,
        initial_capital=args.initial_capital,
        commission_min=args.commission_min,
        cash_interest_rate=args.cash_interest_rate,
    )
    result.log_path = str(log_path)
    result = export_backtest_artifacts(
        result,
        artifacts_dir=run_dir,
        chart_title=f"{symbol} Buy and Hold Equity Curve",
        chart_subtitle=(
            f"100% {symbol} | no risk control | "
            f"fees={args.commission_bps + args.slippage_bps:g}bps"
        ),
        save_chart=args.save_artifacts and args.save_chart,
        write_artifacts=args.save_artifacts,
        summary_context={
            "strategy": strategy_name,
            "asset_type": asset_type,
            "symbol": symbol,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "signal_date": signal_date.isoformat(),
            "execution_lag": args.execution_lag,
            "initial_capital": args.initial_capital,
            "commission_bps": args.commission_bps,
            "slippage_bps": args.slippage_bps,
            "total_fee_bps": args.commission_bps + args.slippage_bps,
            "commission_min": args.commission_min,
            "cash_interest_rate": args.cash_interest_rate,
            "risk_control": False,
        },
    )

    payload = {
        "strategy": strategy_name,
        "asset_type": asset_type,
        "symbol": symbol,
        "requested_start_date": start_date.isoformat(),
        "signal_date": signal_date.isoformat(),
        "end_date": end_date.isoformat(),
        "decision_rows": decisions.height,
        "trade_rows": result.trades_df.height,
        "return_rows": result.returns_df.height,
        "metrics": result.metrics,
        "run_dir": str(run_dir),
        "log_path": str(log_path),
        "artifact_paths": result.artifact_paths or {},
    }
    if args.format == "json":
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    print(f"{symbol} Buy and Hold Backtest")
    print(f"window: {start_date} -> {end_date} | first signal: {signal_date}")
    print(
        f"rows: decisions={decisions.height} "
        f"trades={result.trades_df.height} returns={result.returns_df.height}"
    )
    print(f"run_dir: {run_dir}")
    print("metrics:")
    for key, value in result.metrics.items():
        print(f"  {key}: {value}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    default_end = datetime.now(timezone.utc).date().isoformat()
    parser = argparse.ArgumentParser(description="Run a fully invested buy-and-hold backtest")
    parser.add_argument("--symbol", default="VTI", help="持有标的，默认 VTI")
    parser.add_argument("--asset-type", default="etf_US", help="资产类型，默认 etf_US")
    parser.add_argument("--tag", default="benchmark", help="报告中的资产 tag")
    parser.add_argument("--start-date", default="2016-08-24", help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end-date", default=default_end, help="结束日期 YYYY-MM-DD")
    parser.add_argument("--execution-lag", type=int, default=0, help="首次买入延迟交易日数，默认 0")
    parser.add_argument(
        "--commission-bps", type=float, default=DEFAULT_COMMISSION_BPS,
        help=f"单边手续费 bps，默认 {DEFAULT_COMMISSION_BPS:g}",
    )
    parser.add_argument(
        "--slippage-bps", type=float, default=DEFAULT_SLIPPAGE_BPS,
        help=f"单边滑点 bps，默认 {DEFAULT_SLIPPAGE_BPS:g}",
    )
    parser.add_argument("--commission-min", type=float, default=0.0, help="单笔最低佣金，默认 0")
    parser.add_argument("--cash-interest-rate", type=float, default=0.0, help="现金年化利率，默认 0")
    parser.add_argument("--initial-capital", type=float, default=40000.0, help="初始资金，默认 40000")
    parser.add_argument("--output-dir", default="logs/backtest", help="输出根目录，默认 logs/backtest")
    parser.add_argument("--log-level", default="DEBUG", help="文件日志级别，默认 DEBUG")
    parser.add_argument("--format", choices=["table", "json"], default="table")
    parser.add_argument("--save-artifacts", dest="save_artifacts", action="store_true")
    parser.add_argument("--no-save-artifacts", dest="save_artifacts", action="store_false")
    parser.set_defaults(save_artifacts=True)
    parser.add_argument("--save-chart", dest="save_chart", action="store_true")
    parser.add_argument("--no-save-chart", dest="save_chart", action="store_false")
    parser.set_defaults(save_chart=True)
    return parser


if __name__ == "__main__":
    raise SystemExit(main(build_parser().parse_args()))

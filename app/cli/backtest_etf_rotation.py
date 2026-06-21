"""Run ETF rotation backtests from the strategy layer."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
for candidate in ["/app", str(REPO_ROOT)]:
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from app.backtest.reporting import export_backtest_artifacts
from app.backtest.risk_overlay import RiskOverlayConfig
from app.backtest.runner import run_strategy_backtest
from app.strategy.etf_rotation import ETFUniverseRotationStrategy
from app.utils.logging import build_timestamped_prefix, configure_task_logger, ensure_log_directories
from loguru import logger


def _to_compact_rows(df, limit: int) -> list[dict]:
    if df.is_empty():
        return []

    rows: list[dict] = []
    for row in df.head(limit).iter_rows(named=True):
        item: dict[str, object] = {}
        for key, value in row.items():
            if hasattr(value, "isoformat"):
                item[key] = value.isoformat()
            else:
                item[key] = value
        rows.append(item)
    return rows


def main(
    start_date: str,
    end_date: str,
    profile_name: str,
    universe: str,
    top_n: int,
    max_per_tag: int,
    rebalance_weekday: int,
    execution_lag: int,
    commission_bps: float,
    slippage_bps: float,
    output_format: str,
    log_level: str,
    output_dir: str,
    save_artifacts: bool,
    save_chart: bool,
    risk_control: bool = False,
    risk_std_threshold: float = 0.03,
    risk_cv_threshold: float = 0.5,
    stop_loss_rate: float = 0.10,
    risk_half_weight: float = 0.5,
    initial_capital: float = 40000.0,
    commission_min: float = 0.01,
    cash_interest_rate: float = 0.01,
) -> int:
    ensure_log_directories("logs")
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    file_prefix = build_timestamped_prefix("etf_rotation")
    run_dir = output_root / file_prefix
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = configure_task_logger(
        log_path=run_dir / "backtest.log",
        file_level=log_level,
        console_level="INFO",
        enable_console=True,
    )
    logger.info(
        "回测启动 | profile={} | universe={} | start={} | end={} | top_n={} | max_per_tag={} | rebalance_frequency=weekly | rebalance_weekday={} | execution_lag={} | commission_bps={} | slippage_bps={} | risk_control={} | initial_capital={} | commission_min={} | cash_interest_rate={}",
        profile_name,
        universe,
        start_date,
        end_date,
        top_n,
        max_per_tag,
        rebalance_weekday,
        execution_lag,
        commission_bps,
        slippage_bps,
        risk_control,
        initial_capital,
        commission_min,
        cash_interest_rate,
    )

    strategy = ETFUniverseRotationStrategy(
        top_n=top_n,
        profile_name=profile_name,
        max_per_tag=max_per_tag,
    )
    risk_config = (
        RiskOverlayConfig(
            std_threshold=risk_std_threshold,
            cv_threshold=risk_cv_threshold,
            stop_loss_rate=stop_loss_rate,
            half_weight=risk_half_weight,
        )
        if risk_control
        else None
    )
    bundle = run_strategy_backtest(
        strategy,
        asset_type=None,
        universe=universe,
        profile_name=profile_name,
        start=start_date,
        end=end_date,
        rebalance_frequency="weekly",
        rebalance_weekday=rebalance_weekday,
        execution_lag=execution_lag,
        commission_bps=commission_bps,
        slippage_bps=slippage_bps,
        log_path=str(log_path),
        artifacts_dir=str(run_dir),
        risk_config=risk_config,
        initial_capital=initial_capital,
        commission_min=commission_min,
        cash_interest_rate=cash_interest_rate,
    )

    backtest_result = bundle.backtest_result

    if save_artifacts:
        backtest_result = export_backtest_artifacts(
            backtest_result,
            artifacts_dir=run_dir,
            chart_title="ETF Rotation Equity Curve",
            chart_subtitle=(
                f"weekly | weekday={rebalance_weekday} | top_n={top_n} | "
                f"max_per_tag={max_per_tag} | fees={commission_bps + slippage_bps}bps"
            ),
            save_chart=save_chart,
        )

    payload = {
        "query": {
            "universe": universe,
            "profile": profile_name,
            "strategy": strategy.strategy_name,
            "strategy_mode": strategy.strategy_mode,
            "start_date": start_date,
            "end_date": end_date,
            "top_n": top_n,
            "max_per_tag": max_per_tag,
            "rebalance_frequency": "weekly",
            "rebalance_weekday": rebalance_weekday,
            "execution_lag": execution_lag,
            "commission_bps": commission_bps,
            "slippage_bps": slippage_bps,
            "log_level": log_level.upper(),
            "risk_control": risk_control,
            "risk_std_threshold": risk_std_threshold,
            "risk_cv_threshold": risk_cv_threshold,
            "stop_loss_rate": stop_loss_rate,
            "risk_half_weight": risk_half_weight,
            "initial_capital": initial_capital,
            "commission_min": commission_min,
            "cash_interest_rate": cash_interest_rate,
        },
        "snapshot_rows": bundle.signal_snapshot.height,
        "decision_rows": bundle.decisions_df.height,
        "holding_rows": backtest_result.holdings_df.height,
        "trade_rows": backtest_result.trades_df.height,
        "return_rows": backtest_result.returns_df.height,
        "metrics": backtest_result.metrics,
        "run_dir": str(run_dir),
        "log_path": backtest_result.log_path,
        "artifacts_dir": backtest_result.artifacts_dir,
        "equity_chart_path": backtest_result.equity_chart_path,
        "artifact_paths": backtest_result.artifact_paths or {},
        "sample_decisions": _to_compact_rows(bundle.decisions_df, 5),
        "sample_returns": _to_compact_rows(backtest_result.returns_df, 10),
    }

    if output_format == "json":
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    print("ETF Rotation Backtest")
    print(f"universe: {universe}")
    print(f"window: {start_date} -> {end_date}")
    print(f"strategy: {strategy.strategy_name} | profile: {profile_name}")
    print(
        "weekly rebalance: weekday={} | top_n={} | max_per_tag={} | execution_lag={} | commission_bps={} | slippage_bps={} | risk_control={} | initial_capital={} | commission_min={} | cash_interest_rate={}".format(
            rebalance_weekday,
            top_n,
            max_per_tag,
            execution_lag,
            commission_bps,
            slippage_bps,
            risk_control,
            initial_capital,
            commission_min,
            cash_interest_rate,
        )
    )
    print(
        "rows: snapshot={} decisions={} holdings={} trades={} returns={}".format(
            payload["snapshot_rows"],
            payload["decision_rows"],
            payload["holding_rows"],
            payload["trade_rows"],
            payload["return_rows"],
        )
    )
    if backtest_result.log_path:
        print(f"log_path: {backtest_result.log_path}")
    print(f"run_dir: {run_dir}")
    if backtest_result.equity_chart_path:
        print(f"equity_chart_path: {backtest_result.equity_chart_path}")
    print("metrics:")
    for key, value in backtest_result.metrics.items():
        print(f"  {key}: {value}")
    return 0


if __name__ == "__main__":
    default_end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    parser = argparse.ArgumentParser(description="Run ETF weekly rotation backtest")
    parser.add_argument("--start-date", default="2023-06-03", help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end-date", default=default_end, help="结束日期 YYYY-MM-DD")
    parser.add_argument(
        "--profile",
        default="trend_etf_momentum_reg20",
        help="ETF signal profile，默认 trend_etf_momentum_reg20",
    )
    parser.add_argument("--universe", default="etf_mixed", help="策略池，默认 etf_mixed")
    parser.add_argument("--top-n", type=int, default=4, help="持仓数量，默认 4")
    parser.add_argument("--max-per-tag", type=int, default=1, help="同 tag 最大持仓数，默认 1")
    parser.add_argument("--rebalance-weekday", type=int, default=2, help="周调仓日，Python weekday 语义，周一=0，默认周三=2")
    parser.add_argument("--execution-lag", type=int, default=1, help="信号到执行的交易日延迟，默认 1")
    parser.add_argument("--commission-bps", type=float, default=5.0, help="单边手续费 bps，默认 5")
    parser.add_argument("--slippage-bps", type=float, default=5.0, help="单边滑点 bps，默认 5")
    parser.add_argument("--risk-control", action="store_true", help="启用 ETF 风险过滤/半仓/止损 overlay，默认关闭")
    parser.add_argument("--risk-std-threshold", type=float, default=0.03, help="风险过滤波动率阈值，默认 0.03")
    parser.add_argument("--risk-cv-threshold", type=float, default=0.5, help="成交额 CV 阈值，默认 0.5")
    parser.add_argument("--stop-loss-rate", type=float, default=0.10, help="持仓周期止损阈值，默认 0.10")
    parser.add_argument("--risk-half-weight", type=float, default=0.5, help="触发风险过滤后的权重乘数，默认 0.5")
    parser.add_argument("--initial-capital", type=float, default=40000.0, help="初始资金，默认 40000")
    parser.add_argument("--commission-min", type=float, default=0.01, help="单笔最低佣金，默认 0.01")
    parser.add_argument("--cash-interest-rate", type=float, default=0.01, help="现金年化利率，默认 0.01")
    parser.add_argument("--log-level", default="DEBUG", help="文件日志级别，默认 DEBUG")
    parser.add_argument("--output-dir", default="logs/backtest", help="回测产物输出目录，默认 logs/backtest")
    parser.add_argument("--save-artifacts", dest="save_artifacts", action="store_true", help="保存 CSV/图表产物（默认开启）")
    parser.add_argument("--no-save-artifacts", dest="save_artifacts", action="store_false", help="不保存 CSV/图表产物")
    parser.set_defaults(save_artifacts=True)
    parser.add_argument("--save-chart", dest="save_chart", action="store_true", help="保存账户价值曲线 PNG（默认开启）")
    parser.add_argument("--no-save-chart", dest="save_chart", action="store_false", help="不保存账户价值曲线 PNG")
    parser.set_defaults(save_chart=True)
    parser.add_argument("--format", choices=["table", "json"], default="table", help="输出格式")
    args = parser.parse_args()

    raise SystemExit(
        main(
            args.start_date,
            args.end_date,
            args.profile,
            args.universe,
            args.top_n,
            args.max_per_tag,
            args.rebalance_weekday,
            args.execution_lag,
            args.commission_bps,
            args.slippage_bps,
            args.format,
            args.log_level,
            args.output_dir,
            args.save_artifacts,
            args.save_chart,
            args.risk_control,
            args.risk_std_threshold,
            args.risk_cv_threshold,
            args.stop_loss_rate,
            args.risk_half_weight,
            args.initial_capital,
            args.commission_min,
            args.cash_interest_rate,
        )
    )

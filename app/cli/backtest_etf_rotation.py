"""Run ETF rotation backtests from the strategy layer."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl


REPO_ROOT = Path(__file__).resolve().parents[2]
for candidate in ["/app", str(REPO_ROOT)]:
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from app.backtest.baselines import build_buy_and_hold_decisions
from app.backtest.costs import DEFAULT_COMMISSION_BPS, DEFAULT_SLIPPAGE_BPS
from app.backtest.reporting import export_backtest_artifacts
from app.backtest.risk_overlay import RiskOverlayConfig
from app.backtest.runner import _load_market_data, run_backtest, run_strategy_backtest
from app.strategy.etf_rotation import ETFRotationCNStrategy, resolve_etf_rotation_strategy
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


def _build_vti_benchmark_curve(
    start_date: date,
    end_date: date,
    *,
    commission_bps: float,
    slippage_bps: float,
    initial_capital: float,
    commission_min: float,
    cash_interest_rate: float,
) -> pl.DataFrame:
    market_data = _load_market_data(
        ["etf_US"],
        start_date,
        end_date,
        symbols=["VTI"],
    )
    if market_data.is_empty():
        logger.warning("同期 VTI 行情为空，资金曲线将只显示 ETF Rotation")
        return pl.DataFrame()

    signal_date = market_data.get_column("time").min()
    decisions = build_buy_and_hold_decisions("VTI", signal_date)
    benchmark_result = run_backtest(
        decisions,
        asset_type="etf_US",
        start=start_date,
        end=end_date,
        rebalance_frequency="daily",
        execution_lag=0,
        commission_bps=commission_bps,
        slippage_bps=slippage_bps,
        risk_config=None,
        initial_capital=initial_capital,
        commission_min=commission_min,
        cash_interest_rate=cash_interest_rate,
        market_data_override=market_data,
    )
    return benchmark_result.equity_curve_df


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
    risk_std_threshold: float | None = None,
    risk_cv_threshold: float | None = None,
    stop_loss_rate: float | None = None,
    risk_half_weight: float | None = None,
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

    strategy = resolve_etf_rotation_strategy(
        universe,
        top_n=top_n,
        profile_name=profile_name,
        max_per_tag=max_per_tag,
    )
    if risk_control:
        strategy_risk_config = strategy.default_risk_config()
        risk_config = RiskOverlayConfig(
            std_threshold=risk_std_threshold if risk_std_threshold is not None else strategy_risk_config.std_threshold,
            cv_threshold=risk_cv_threshold if risk_cv_threshold is not None else strategy_risk_config.cv_threshold,
            stop_loss_rate=stop_loss_rate if stop_loss_rate is not None else strategy_risk_config.stop_loss_rate,
            half_weight=risk_half_weight if risk_half_weight is not None else strategy_risk_config.half_weight,
            std_long_window=strategy_risk_config.std_long_window,
            std_short_window=strategy_risk_config.std_short_window,
            cv_window=strategy_risk_config.cv_window,
        )
    else:
        risk_config = None
    if risk_config is not None:
        logger.info(
            "风控参数 | std_threshold={} | cv_threshold={} | stop_loss_rate={} | half_weight={}",
            risk_config.std_threshold,
            risk_config.cv_threshold,
            risk_config.stop_loss_rate,
            risk_config.half_weight,
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

    benchmark_equity_curve_df = None
    decision_asset_types = (
        set(bundle.decisions_df.get_column("asset_type").unique().to_list())
        if "asset_type" in bundle.decisions_df.columns
        else set()
    )
    if (
        save_artifacts
        and save_chart
        and decision_asset_types == {"etf_US"}
        and not backtest_result.returns_df.is_empty()
    ):
        benchmark_equity_curve_df = _build_vti_benchmark_curve(
            backtest_result.returns_df.get_column("time").min(),
            backtest_result.returns_df.get_column("time").max(),
            commission_bps=commission_bps,
            slippage_bps=slippage_bps,
            initial_capital=initial_capital,
            commission_min=commission_min,
            cash_interest_rate=cash_interest_rate,
        )
    benchmark_available = (
        benchmark_equity_curve_df is not None and not benchmark_equity_curve_df.is_empty()
    )

    backtest_result = export_backtest_artifacts(
        backtest_result,
        artifacts_dir=run_dir,
        chart_title="ETF Rotation vs VTI Equity Curve",
        chart_subtitle=(
            f"weekly | weekday={rebalance_weekday} | top_n={top_n} | "
            f"max_per_tag={max_per_tag} | fees={commission_bps + slippage_bps}bps"
        ),
        save_chart=save_artifacts and save_chart,
        write_artifacts=save_artifacts,
        strategy_label="ETF Rotation",
        benchmark_equity_curve_df=benchmark_equity_curve_df,
        benchmark_label="VTI Buy & Hold",
        summary_context={
            "start_date": start_date,
            "end_date": end_date,
            "universe": universe,
            "profile_name": profile_name,
            "top_n": top_n,
            "max_per_tag": max_per_tag,
            "total_fee_bps": commission_bps + slippage_bps,
            "risk_control": risk_control,
            "benchmark_symbol": "VTI" if benchmark_available else None,
            "benchmark_total_fee_bps": (
                commission_bps + slippage_bps if benchmark_available else None
            ),
        },
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
            "risk_std_threshold": risk_config.std_threshold if risk_config is not None else None,
            "risk_cv_threshold": risk_config.cv_threshold if risk_config is not None else None,
            "stop_loss_rate": risk_config.stop_loss_rate if risk_config is not None else None,
            "risk_half_weight": risk_config.half_weight if risk_config is not None else None,
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
        "summary_preview": backtest_result.analysis_summary or {},
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
        default=ETFRotationCNStrategy.default_profile_name,
        help="ETF signal profile，默认 trend_etf_momentum_reg20",
    )
    parser.add_argument(
        "--universe",
        default=ETFRotationCNStrategy.default_universe,
        help="策略池，默认 etf_rotation_CN",
    )
    parser.add_argument("--top-n", type=int, default=4, help="持仓数量，默认 4")
    parser.add_argument("--max-per-tag", type=int, default=1, help="同 tag 最大持仓数，默认 1")
    parser.add_argument("--rebalance-weekday", type=int, default=2, help="周调仓日，Python weekday 语义，周一=0，默认周三=2")
    parser.add_argument("--execution-lag", type=int, default=1, help="信号到执行的交易日延迟，默认 1")
    parser.add_argument(
        "--commission-bps", type=float, default=DEFAULT_COMMISSION_BPS,
        help=f"单边手续费 bps，默认 {DEFAULT_COMMISSION_BPS:g}",
    )
    parser.add_argument(
        "--slippage-bps", type=float, default=DEFAULT_SLIPPAGE_BPS,
        help=f"单边滑点 bps，默认 {DEFAULT_SLIPPAGE_BPS:g}",
    )
    parser.add_argument("--risk-control", action="store_true", help="启用 ETF 风险过滤/半仓/止损 overlay，默认关闭")
    parser.add_argument(
        "--risk-std-threshold",
        type=float,
        default=None,
        help="风险过滤波动率阈值，默认使用策略/市场配置",
    )
    parser.add_argument(
        "--risk-cv-threshold",
        type=float,
        default=None,
        help="成交额 CV 阈值，默认使用策略/市场配置",
    )
    parser.add_argument(
        "--stop-loss-rate",
        type=float,
        default=None,
        help="持仓周期止损阈值，默认使用策略/市场配置",
    )
    parser.add_argument(
        "--risk-half-weight",
        type=float,
        default=None,
        help="触发风险过滤后的权重乘数，默认使用策略/市场配置",
    )
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

"""Run ETF CN rotation backtests from the strategy layer."""
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
        "回测启动 | profile={} | asset_type=etf_CN | start={} | end={} | top_n={} | max_per_tag={} | rebalance_frequency=weekly | rebalance_weekday={} | execution_lag={} | commission_bps={} | slippage_bps={}",
        profile_name,
        start_date,
        end_date,
        top_n,
        max_per_tag,
        rebalance_weekday,
        execution_lag,
        commission_bps,
        slippage_bps,
    )

    strategy = ETFUniverseRotationStrategy(
        top_n=top_n,
        profile_name=profile_name,
        max_per_tag=max_per_tag,
    )
    bundle = run_strategy_backtest(
        strategy,
        asset_type="etf_CN",
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
            "asset_type": "etf_CN",
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
    print(f"asset_type: etf_CN")
    print(f"window: {start_date} -> {end_date}")
    print(f"strategy: {strategy.strategy_name} | profile: {profile_name}")
    print(
        "weekly rebalance: weekday={} | top_n={} | max_per_tag={} | execution_lag={} | commission_bps={} | slippage_bps={}".format(
            rebalance_weekday,
            top_n,
            max_per_tag,
            execution_lag,
            commission_bps,
            slippage_bps,
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
    parser = argparse.ArgumentParser(description="Run ETF CN weekly rotation backtest")
    parser.add_argument("--start-date", default="2023-06-03", help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end-date", default=default_end, help="结束日期 YYYY-MM-DD")
    parser.add_argument("--profile", default="trend_etf_v1", help="ETF signal profile，默认 trend_etf_v1")
    parser.add_argument("--top-n", type=int, default=5, help="持仓数量，默认 5")
    parser.add_argument("--max-per-tag", type=int, default=1, help="同 tag 最大持仓数，默认 1")
    parser.add_argument("--rebalance-weekday", type=int, default=2, help="周调仓日，Python weekday 语义，周一=0，默认周三=2")
    parser.add_argument("--execution-lag", type=int, default=1, help="信号到执行的交易日延迟，默认 1")
    parser.add_argument("--commission-bps", type=float, default=5.0, help="单边手续费 bps，默认 5")
    parser.add_argument("--slippage-bps", type=float, default=5.0, help="单边滑点 bps，默认 5")
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
        )
    )

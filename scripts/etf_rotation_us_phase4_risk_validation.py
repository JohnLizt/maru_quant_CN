"""Run phase-4 ETF rotation US risk overlay validation experiments."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl


REPO_ROOT = Path(__file__).resolve().parents[1]
for candidate in ["/app", str(REPO_ROOT)]:
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from app.backtest.costs import DEFAULT_COMMISSION_BPS, DEFAULT_SLIPPAGE_BPS
from app.backtest.reporting import build_rebalance_period_analysis
from app.backtest.runner import _load_market_data, run_backtest
from app.backtest.risk_overlay import RiskOverlayConfig, build_risk_features
from app.strategy.etf_rotation import ETFRotationUSStrategy
from scripts.etf_rotation_us_phase3_sensitivity import _pool_corr_metrics, _variant_symbol_map
from scripts.etf_rotation_us_universe_experiment import (
    DEFAULT_END_DATE,
    DEFAULT_MAX_PER_TAG,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_PROFILE,
    DEFAULT_START_DATE,
    _candidate_rows,
    _contribution_rows,
    _load_full_momentum_snapshot,
    _metrics_row,
    _normalize_date,
    _pairwise_correlation_map,
    _selection_stability,
    _variant_decisions,
)


DEFAULT_PHASE2_EXPERIMENT_ID = "phase2_full_20260712"
DEFAULT_PHASE3_EXPERIMENT_ID = "phase3_sensitivity_8x4_20260712"
DEFAULT_OUTPUT_SUBDIR = "phase4_risk_validation"
DEFAULT_POOL_SIZE = 8
DEFAULT_TOP_K = 4


@dataclass(frozen=True)
class Phase4Config:
    experiment_id: str
    phase2_experiment_id: str
    phase3_experiment_id: str
    candidate_id: str | None
    start_date: str
    end_date: str
    pool_size: int
    top_k: int
    max_per_tag: int
    profile_name: str
    rebalance_weekday: int
    execution_lag: int
    commission_bps: float
    slippage_bps: float
    initial_capital: float
    commission_min: float
    cash_interest_rate: float
    feature_quantile: float
    drawdown_quantile: float


def _now_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _phase_dir(experiment_id: str) -> Path:
    return DEFAULT_OUTPUT_ROOT / experiment_id


def _load_phase3_winner(phase3_experiment_id: str, candidate_id: str | None) -> str:
    candidate_summary = pl.read_csv(_phase_dir(phase3_experiment_id) / "candidate_summary.csv")
    if candidate_id:
        matched = candidate_summary.filter(pl.col("parent_candidate_id") == candidate_id)
        if matched.is_empty():
            raise ValueError(f"phase3 实验 {phase3_experiment_id} 中不存在候选池 {candidate_id}")
        return candidate_id
    winner = candidate_summary.sort(["phase3_robustness_score", "base_sharpe"], descending=[True, True]).row(
        0, named=True
    )
    return str(winner["parent_candidate_id"])


def _load_candidate_symbols(phase2_experiment_id: str, candidate_id: str) -> list[str]:
    variants_df = pl.read_csv(_phase_dir(phase2_experiment_id) / "universe_variants.csv")
    variant_map = _variant_symbol_map(variants_df)
    if candidate_id not in variant_map:
        raise ValueError(f"phase2 实验 {phase2_experiment_id} 中不存在候选池 {candidate_id}")
    return variant_map[candidate_id]


def _candidate_snapshot_hash(symbols: list[str]) -> str:
    return hashlib.sha1("|".join(sorted(symbols)).encode("utf-8")).hexdigest()[:12]


def _quantile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    series = pl.Series("value", values)
    result = series.quantile(quantile, interpolation="linear")
    return float(result) if result is not None else 0.0


def _rolling_drawdown_values(price_df: pl.DataFrame, window_size: int = 20) -> list[float]:
    values: list[float] = []
    for symbol_df in price_df.sort(["symbol", "time"]).partition_by("symbol", maintain_order=True):
        drawdown_df = symbol_df.with_columns(
            pl.col("close")
            .cast(pl.Float64)
            .rolling_max(window_size=window_size, min_samples=window_size)
            .alias("_rolling_peak")
        ).with_columns(((pl.col("close") / pl.col("_rolling_peak")) - 1.0).alias("_rolling_drawdown"))
        series = drawdown_df.get_column("_rolling_drawdown").drop_nulls()
        values.extend([-float(value) for value in series.to_list() if value is not None and float(value) < 0.0])
    return values


def _derive_data_driven_risk_config(
    market_data: pl.DataFrame,
    pool_symbols: list[str],
    *,
    feature_quantile: float,
    drawdown_quantile: float,
) -> tuple[RiskOverlayConfig, dict[str, float]]:
    default_config = ETFRotationUSStrategy().default_risk_config()
    pool_market = market_data.filter(pl.col("symbol").is_in(pool_symbols))
    feature_df = build_risk_features(pool_market, default_config)
    std_values = [
        float(value)
        for value in feature_df.get_column("std_score").drop_nulls().to_list()
        if value is not None and float(value) > 0.0
    ]
    cv_values = [
        float(value)
        for value in feature_df.get_column("cv").drop_nulls().to_list()
        if value is not None and float(value) > 0.0
    ]
    drawdown_values = _rolling_drawdown_values(pool_market)

    std_threshold = max(0.005, _quantile(std_values, feature_quantile))
    cv_threshold = max(0.10, _quantile(cv_values, feature_quantile))
    stop_loss_rate = min(0.20, max(0.06, _quantile(drawdown_values, drawdown_quantile)))

    config = RiskOverlayConfig(
        std_threshold=round(std_threshold, 6),
        cv_threshold=round(cv_threshold, 6),
        stop_loss_rate=round(stop_loss_rate, 6),
        half_weight=default_config.half_weight,
        std_long_window=default_config.std_long_window,
        std_short_window=default_config.std_short_window,
        cv_window=default_config.cv_window,
    )
    details = {
        "feature_quantile": feature_quantile,
        "drawdown_quantile": drawdown_quantile,
        "derived_std_threshold": config.std_threshold,
        "derived_cv_threshold": config.cv_threshold,
        "derived_stop_loss_rate": config.stop_loss_rate,
        "sample_std_count": len(std_values),
        "sample_cv_count": len(cv_values),
        "sample_drawdown_count": len(drawdown_values),
    }
    return config, details


def _scenario_defs(data_driven: RiskOverlayConfig) -> list[dict[str, Any]]:
    default_risk = ETFRotationUSStrategy().default_risk_config()
    return [
        {
            "scenario_id": "risk_off",
            "scenario_name": "risk_disabled",
            "risk_mode": "disabled",
            "risk_config": None,
        },
        {
            "scenario_id": "risk_default",
            "scenario_name": "risk_default_us",
            "risk_mode": "default",
            "risk_config": default_risk,
        },
        {
            "scenario_id": "risk_data_driven",
            "scenario_name": "risk_data_driven_quantile",
            "risk_mode": "data_driven",
            "risk_config": data_driven,
        },
    ]


def _report_text(
    config: Phase4Config,
    candidate_id: str,
    pool_symbols: list[str],
    thresholds: dict[str, float],
    run_metrics_df: pl.DataFrame,
) -> str:
    def _fmt(value: Any, digits: int = 4) -> str:
        if value is None:
            return "N/A"
        return f"{float(value):.{digits}f}"

    sorted_df = run_metrics_df.sort("phase4_score", descending=True)
    winner = sorted_df.row(0, named=True)
    sharpe_winner = run_metrics_df.sort("sharpe", descending=True).row(0, named=True)
    drawdown_winner = run_metrics_df.sort("max_drawdown", descending=True).row(0, named=True)

    lines = [
        "# ETF Rotation US Universe Phase-4 Risk Validation",
        "",
        "## Scope",
        f"- experiment_id: `{config.experiment_id}`",
        f"- phase3_source: `{config.phase3_experiment_id}`",
        f"- phase2_source: `{config.phase2_experiment_id}`",
        f"- candidate_id: `{candidate_id}`",
        f"- fixed grid: `N={config.pool_size}, TopK={config.top_k}`",
        f"- symbols: `{'|'.join(pool_symbols)}`",
        f"- profile: `{config.profile_name}`",
        "",
        "## Data-Driven Thresholds",
        f"- std_threshold quantile={_fmt(thresholds['feature_quantile'], 2)} -> `{_fmt(thresholds['derived_std_threshold'], 6)}`",
        f"- cv_threshold quantile={_fmt(thresholds['feature_quantile'], 2)} -> `{_fmt(thresholds['derived_cv_threshold'], 6)}`",
        f"- stop_loss quantile={_fmt(thresholds['drawdown_quantile'], 2)} -> `{_fmt(thresholds['derived_stop_loss_rate'], 6)}`",
        "",
        "## Scenario Comparison",
    ]
    for row in run_metrics_df.sort("scenario_name").iter_rows(named=True):
        lines.append(
            f"- `{row['scenario_name']}`: score={_fmt(row['phase4_score'])}, sharpe={_fmt(row['sharpe'])}, ann_return={_fmt(row['annualized_return'])}, max_dd={_fmt(row['max_drawdown'])}, risk_half={int(row['risk_half_events'])}, stop_loss={int(row['stop_loss_events'])}, turnover={_fmt(row['cum_turnover'])}"
        )
    lines.extend(
        [
            "",
            "## Summary",
            f"- recommended scenario by combined score: `{winner['scenario_name']}`",
            f"- highest sharpe scenario: `{sharpe_winner['scenario_name']}` with `{_fmt(sharpe_winner['sharpe'])}`",
            f"- lowest drawdown scenario: `{drawdown_winner['scenario_name']}` with `{_fmt(drawdown_winner['max_drawdown'])}`",
            "- phase4_score = sharpe + 0.8 * annualized_return + 1.2 * max_drawdown - 0.0005 * risk_half_events - 0.01 * stop_loss_events",
            "",
            "## Notes",
            "- 数据驱动阈值方案使用候选池成分的历史滚动波动、成交额 CV 与 20 日滚动回撤分位数生成阈值。",
            "- 第四阶段用于确认收益优势是否依赖风控，以及默认风控是否已经接近合适的阈值区间。",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run ETF rotation US phase-4 risk validation")
    parser.add_argument("--phase2-experiment-id", default=DEFAULT_PHASE2_EXPERIMENT_ID)
    parser.add_argument("--phase3-experiment-id", default=DEFAULT_PHASE3_EXPERIMENT_ID)
    parser.add_argument("--candidate-id", default=None)
    parser.add_argument("--experiment-id", default=f"{DEFAULT_OUTPUT_SUBDIR}_{_now_id()}")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--pool-size", type=int, default=DEFAULT_POOL_SIZE)
    parser.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    parser.add_argument("--max-per-tag", type=int, default=DEFAULT_MAX_PER_TAG)
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--rebalance-weekday", type=int, default=2)
    parser.add_argument("--execution-lag", type=int, default=1)
    parser.add_argument("--commission-bps", type=float, default=DEFAULT_COMMISSION_BPS)
    parser.add_argument("--slippage-bps", type=float, default=DEFAULT_SLIPPAGE_BPS)
    parser.add_argument("--initial-capital", type=float, default=40000.0)
    parser.add_argument("--commission-min", type=float, default=0.01)
    parser.add_argument("--cash-interest-rate", type=float, default=0.01)
    parser.add_argument("--feature-quantile", type=float, default=0.80)
    parser.add_argument("--drawdown-quantile", type=float, default=0.90)
    args = parser.parse_args()

    config = Phase4Config(
        experiment_id=args.experiment_id,
        phase2_experiment_id=args.phase2_experiment_id,
        phase3_experiment_id=args.phase3_experiment_id,
        candidate_id=args.candidate_id,
        start_date=args.start_date,
        end_date=args.end_date,
        pool_size=args.pool_size,
        top_k=args.top_k,
        max_per_tag=args.max_per_tag,
        profile_name=args.profile,
        rebalance_weekday=args.rebalance_weekday,
        execution_lag=args.execution_lag,
        commission_bps=args.commission_bps,
        slippage_bps=args.slippage_bps,
        initial_capital=args.initial_capital,
        commission_min=args.commission_min,
        cash_interest_rate=args.cash_interest_rate,
        feature_quantile=args.feature_quantile,
        drawdown_quantile=args.drawdown_quantile,
    )

    candidate_id = _load_phase3_winner(config.phase3_experiment_id, config.candidate_id)
    pool_symbols = _load_candidate_symbols(config.phase2_experiment_id, candidate_id)
    output_dir = DEFAULT_OUTPUT_ROOT / config.experiment_id
    output_dir.mkdir(parents=True, exist_ok=True)

    candidate_rows = _candidate_rows()
    start_date = _normalize_date(config.start_date)
    end_date = _normalize_date(config.end_date)
    signal_source = _load_full_momentum_snapshot(candidate_rows, start_date, end_date)
    market_data = _load_market_data(["etf_US"], start_date, end_date).filter(pl.col("symbol").is_in(sorted({
        row["symbol"] for row in candidate_rows
    })))
    corr_map = _pairwise_correlation_map(market_data, sorted({row["symbol"] for row in candidate_rows}))
    avg_pair_corr, max_pair_corr = _pool_corr_metrics(pool_symbols, corr_map)
    thresholds_config, thresholds = _derive_data_driven_risk_config(
        market_data,
        pool_symbols,
        feature_quantile=config.feature_quantile,
        drawdown_quantile=config.drawdown_quantile,
    )

    decisions_df = _variant_decisions(signal_source, pool_symbols, config.top_k, config.max_per_tag)
    effective_counts = (
        signal_source.filter(pl.col("symbol").is_in(pool_symbols)).group_by("time").len().get_column("len").to_list()
    )
    base_variant = {
        "variant_id": candidate_id,
        "variant_type": "phase4_candidate",
        "pool_size": len(pool_symbols),
        "symbols": sorted(pool_symbols),
        "avg_pair_corr": round(avg_pair_corr, 6),
        "max_pair_corr": round(max_pair_corr, 6),
    }
    snapshot_hash = _candidate_snapshot_hash(pool_symbols)

    scenario_rows: list[dict[str, Any]] = []
    run_metrics_rows: list[dict[str, Any]] = []
    contribution_rows: list[dict[str, Any]] = []
    period_rows: list[dict[str, Any]] = []
    selection_rows: list[dict[str, Any]] = []

    for scenario in _scenario_defs(thresholds_config):
        run_id = f"{candidate_id}__{scenario['scenario_id']}"
        result = run_backtest(
            decisions_df,
            asset_type="etf_US",
            start=config.start_date,
            end=config.end_date,
            rebalance_frequency="weekly",
            rebalance_weekday=config.rebalance_weekday,
            execution_lag=config.execution_lag,
            commission_bps=config.commission_bps,
            slippage_bps=config.slippage_bps,
            risk_config=scenario["risk_config"],
            initial_capital=config.initial_capital,
            commission_min=config.commission_min,
            cash_interest_rate=config.cash_interest_rate,
            market_data_override=market_data,
        )
        periods_df, period_holdings_df, _ = build_rebalance_period_analysis(result)
        contribution_data, contribution_summary = _contribution_rows(run_id, period_holdings_df)
        metrics_row = _metrics_row(
            experiment_id=config.experiment_id,
            candidate_snapshot_hash=snapshot_hash,
            run_id=run_id,
            variant=base_variant,
            top_k=config.top_k,
            decisions_df=decisions_df,
            result=result,
            periods_df=periods_df,
            period_holdings_df=period_holdings_df,
            contributions=contribution_summary,
            effective_candidate_counts=[int(value) for value in effective_counts],
        )
        overlap_ratio, jaccard_ratio = _selection_stability(periods_df)
        metrics_row.update(
            {
                "scenario_id": scenario["scenario_id"],
                "scenario_name": scenario["scenario_name"],
                "risk_mode": scenario["risk_mode"],
                "risk_std_threshold": scenario["risk_config"].std_threshold if scenario["risk_config"] else None,
                "risk_cv_threshold": scenario["risk_config"].cv_threshold if scenario["risk_config"] else None,
                "stop_loss_rate": scenario["risk_config"].stop_loss_rate if scenario["risk_config"] else None,
                "risk_half_weight": scenario["risk_config"].half_weight if scenario["risk_config"] else None,
            }
        )
        run_metrics_rows.append(metrics_row)
        contribution_rows.extend(contribution_data)
        selection_rows.append(
            {
                "variant_id": run_id,
                "scenario_id": scenario["scenario_id"],
                "scenario_name": scenario["scenario_name"],
                "avg_holding_overlap_ratio": round(overlap_ratio, 6),
                "avg_holding_jaccard": round(jaccard_ratio, 6),
            }
        )
        for row in periods_df.iter_rows(named=True):
            period_rows.append(
                {
                    "variant_id": run_id,
                    "scenario_id": scenario["scenario_id"],
                    **{key: (value.isoformat() if hasattr(value, "isoformat") else value) for key, value in row.items()},
                }
            )
        scenario_rows.append(
            {
                "scenario_id": scenario["scenario_id"],
                "scenario_name": scenario["scenario_name"],
                "risk_mode": scenario["risk_mode"],
                "risk_std_threshold": scenario["risk_config"].std_threshold if scenario["risk_config"] else None,
                "risk_cv_threshold": scenario["risk_config"].cv_threshold if scenario["risk_config"] else None,
                "stop_loss_rate": scenario["risk_config"].stop_loss_rate if scenario["risk_config"] else None,
                "risk_half_weight": scenario["risk_config"].half_weight if scenario["risk_config"] else None,
            }
        )

    run_metrics_df = (
        pl.DataFrame(run_metrics_rows)
        .with_columns(
            (
                pl.col("sharpe")
                + pl.col("annualized_return") * 0.8
                + pl.col("max_drawdown") * 1.2
                - pl.col("risk_half_events") * 0.0005
                - pl.col("stop_loss_events") * 0.01
            ).alias("phase4_score")
        )
        .sort("phase4_score", descending=True)
    )

    (output_dir / "experiment_config.json").write_text(
        json.dumps(
            {
                **asdict(config),
                "resolved_candidate_id": candidate_id,
                "pool_symbols": pool_symbols,
                "data_driven_thresholds": thresholds,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    pl.DataFrame(scenario_rows).write_csv(output_dir / "scenario_definitions.csv")
    run_metrics_df.write_csv(output_dir / "run_metrics.csv")
    pl.DataFrame(contribution_rows).write_csv(output_dir / "symbol_contributions.csv")
    pl.DataFrame(period_rows).write_csv(output_dir / "period_metrics.csv")
    pl.DataFrame(selection_rows).write_csv(output_dir / "selection_stability.csv")
    (output_dir / "experiment_report.md").write_text(
        _report_text(config, candidate_id, pool_symbols, thresholds, run_metrics_df),
        encoding="utf-8",
    )
    print(json.dumps({"experiment_id": config.experiment_id, "output_dir": str(output_dir)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

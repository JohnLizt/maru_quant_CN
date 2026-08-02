"""Run Best-universe optimization experiments for ETF Rotation US."""
from __future__ import annotations

import argparse
import json
import math
import statistics
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
from app.backtest.risk_overlay import RiskOverlayConfig
from app.services.asset_universe import load_universe
from app.strategy.etf_rotation import ETFRotationUSStrategy
from scripts.etf_rotation_us_phase3_sensitivity import _choose_replacement_symbol, _pool_corr_metrics
from scripts.etf_rotation_us_universe_experiment import (
    ASSET_CLASSES,
    DEFAULT_END_DATE,
    DEFAULT_MAX_PER_TAG,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_PROFILE,
    DEFAULT_START_DATE,
    EXPOSURE_GROUPS,
    _candidate_rows,
    _contribution_rows,
    _load_full_momentum_snapshot,
    _metrics_row,
    _normalize_date,
    _pairwise_correlation_map,
    _selection_stability,
    _variant_decisions,
)


DEFAULT_OUTPUT_SUBDIR = "etf_rotation_US_best_optimization"


@dataclass(frozen=True)
class ExperimentConfig:
    experiment_id: str
    universe: str
    start_date: str
    end_date: str
    top_k_values: tuple[int, ...]
    max_per_tag: int
    profile_name: str
    rebalance_weekday: int
    execution_lag: int
    commission_bps: float
    slippage_bps: float
    initial_capital: float
    commission_min: float
    cash_interest_rate: float


def _now_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _weekly_nav(periods_df: pl.DataFrame) -> list[tuple[str, float]]:
    if periods_df.is_empty():
        return []
    nav = 1.0
    rows: list[tuple[str, float]] = []
    for row in periods_df.sort("period_index").iter_rows(named=True):
        nav *= 1.0 + float(row.get("period_return", 0.0) or 0.0)
        period_end = row.get("period_end")
        label = period_end.isoformat() if hasattr(period_end, "isoformat") else str(period_end)
        rows.append((label, nav))
    return rows


def _linear_regression_slope(values: list[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    xs = list(range(n))
    mean_x = sum(xs) / n
    mean_y = sum(values) / n
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, values))
    denominator = sum((x - mean_x) ** 2 for x in xs)
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _rolling_slope_rows(variant_id: str, nav_rows: list[tuple[str, float]], windows: tuple[int, ...] = (13, 26)) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not nav_rows:
        return rows
    labels = [label for label, _ in nav_rows]
    log_nav = [math.log(max(nav, 1e-12)) for _, nav in nav_rows]
    for window in windows:
        if len(log_nav) < window:
            continue
        for index in range(window - 1, len(log_nav)):
            subset = log_nav[index - window + 1:index + 1]
            slope = _linear_regression_slope(subset)
            rows.append(
                {
                    "variant_id": variant_id,
                    "window_weeks": window,
                    "window_end": labels[index],
                    "weekly_log_nav_slope": round(slope, 8),
                    "annualized_log_nav_slope": round(slope * 52.0, 8),
                }
            )
    return rows


def _drawdown_segment_rows(variant_id: str, nav_rows: list[tuple[str, float]]) -> list[dict[str, Any]]:
    if not nav_rows:
        return []
    rows: list[dict[str, Any]] = []
    peak_value = nav_rows[0][1]
    peak_label = nav_rows[0][0]
    segment_start = None
    trough_label = None
    trough_dd = 0.0
    trough_value = peak_value
    segment_index = 0

    for label, nav in nav_rows[1:]:
        if nav >= peak_value:
            if segment_start is not None:
                segment_index += 1
                rows.append(
                    {
                        "variant_id": variant_id,
                        "segment_index": segment_index,
                        "drawdown_start": segment_start,
                        "drawdown_trough": trough_label,
                        "recovery_date": label,
                        "max_drawdown": round(trough_dd, 6),
                        "duration_weeks": None,
                        "recovery_weeks": None,
                    }
                )
                segment_start = None
                trough_label = None
                trough_dd = 0.0
            peak_value = nav
            peak_label = label
            trough_value = nav
            continue

        dd = nav / peak_value - 1.0
        if segment_start is None:
            segment_start = peak_label
            trough_label = label
            trough_dd = dd
            trough_value = nav
        elif dd < trough_dd:
            trough_dd = dd
            trough_label = label
            trough_value = nav

    if segment_start is not None:
        segment_index += 1
        rows.append(
            {
                "variant_id": variant_id,
                "segment_index": segment_index,
                "drawdown_start": segment_start,
                "drawdown_trough": trough_label,
                "recovery_date": "",
                "max_drawdown": round(trough_dd, 6),
                "duration_weeks": None,
                "recovery_weeks": None,
            }
        )
    return rows


def _smoothness_metrics(periods_df: pl.DataFrame, rolling_slope_rows: list[dict[str, Any]], analysis_summary: dict[str, Any]) -> dict[str, Any]:
    period_returns = [float(value or 0.0) for value in periods_df.get_column("period_return").to_list()] if not periods_df.is_empty() else []
    slope_rows_13 = [row for row in rolling_slope_rows if int(row["window_weeks"]) == 13]
    slope_rows_26 = [row for row in rolling_slope_rows if int(row["window_weeks"]) == 26]

    def _slope_stats(rows: list[dict[str, Any]]) -> tuple[float, float, float]:
        if not rows:
            return 0.0, 0.0, 0.0
        values = [float(row["annualized_log_nav_slope"]) for row in rows]
        median_value = statistics.median(values)
        neg_share = sum(1 for value in values if value < 0.0) / len(values)
        volatility = statistics.pstdev(values) if len(values) > 1 else 0.0
        return median_value, neg_share, volatility

    slope13_median, slope13_neg_share, slope13_vol = _slope_stats(slope_rows_13)
    slope26_median, slope26_neg_share, slope26_vol = _slope_stats(slope_rows_26)
    period_structure = analysis_summary.get("period_structure", {})
    asset_concentration = analysis_summary.get("asset_concentration", {})

    return {
        "weekly_return_std": round(float(period_structure.get("weekly_return_std", 0.0)), 6),
        "worst_6w_return": round(float(period_structure.get("worst_6w_return", 0.0)), 6),
        "worst_12w_return": round(float(period_structure.get("worst_12w_return", 0.0)), 6),
        "longest_loss_streak": int(period_structure.get("longest_loss_streak", 0)),
        "slope13_median": round(slope13_median, 6),
        "slope13_negative_share": round(slope13_neg_share, 6),
        "slope13_volatility": round(slope13_vol, 6),
        "slope26_median": round(slope26_median, 6),
        "slope26_negative_share": round(slope26_neg_share, 6),
        "slope26_volatility": round(slope26_vol, 6),
        "best10_period_positive_return_share": round(float(period_structure.get("best10_period_positive_return_share", 0.0)), 6),
        "top1_profit_share": round(float(asset_concentration.get("top1_profit_share", 0.0)), 6),
        "top3_profit_share": round(float(asset_concentration.get("top3_profit_share", 0.0)), 6),
    }


def _variant(symbols: list[str], variant_id: str, variant_type: str, corr_map: dict[tuple[str, str], float]) -> dict[str, Any]:
    avg_pair_corr, max_pair_corr = _pool_corr_metrics(symbols, corr_map)
    return {
        "variant_id": variant_id,
        "variant_type": variant_type,
        "pool_size": len(symbols),
        "symbols": sorted(symbols),
        "avg_pair_corr": round(avg_pair_corr, 6),
        "max_pair_corr": round(max_pair_corr, 6),
    }


def _base_risk_scenarios() -> list[tuple[str, str, RiskOverlayConfig | None]]:
    default_cfg = ETFRotationUSStrategy().default_risk_config()
    return [
        ("risk_off", "risk", None),
        ("risk_default", "risk", default_cfg),
        (
            "risk_mild",
            "risk",
            RiskOverlayConfig(
                std_threshold=0.025,
                cv_threshold=0.80,
                stop_loss_rate=0.12,
                half_weight=default_cfg.half_weight,
                std_long_window=default_cfg.std_long_window,
                std_short_window=default_cfg.std_short_window,
                cv_window=default_cfg.cv_window,
            ),
        ),
        (
            "risk_strict",
            "risk",
            RiskOverlayConfig(
                std_threshold=0.015,
                cv_threshold=0.60,
                stop_loss_rate=0.08,
                half_weight=default_cfg.half_weight,
                std_long_window=default_cfg.std_long_window,
                std_short_window=default_cfg.std_short_window,
                cv_window=default_cfg.cv_window,
            ),
        ),
    ]


def _controlled_replacements(base_symbols: list[str]) -> list[tuple[str, list[str]]]:
    variants: list[tuple[str, list[str]]] = []
    if "XLK" in base_symbols and "SMH" not in base_symbols:
        variants.append(("swap_XLK_to_SMH", sorted(["SMH" if symbol == "XLK" else symbol for symbol in base_symbols])))
    if "VNQ" in base_symbols and "TLT" not in base_symbols:
        variants.append(("swap_VNQ_to_TLT", sorted(["TLT" if symbol == "VNQ" else symbol for symbol in base_symbols])))
    if "IEF" in base_symbols and "TLT" not in base_symbols:
        variants.append(("swap_IEF_to_TLT", sorted(["TLT" if symbol == "IEF" else symbol for symbol in base_symbols])))
    if "GLD" in base_symbols and "DBC" not in base_symbols:
        variants.append(("swap_GLD_to_DBC", sorted(["DBC" if symbol == "GLD" else symbol for symbol in base_symbols])))
    return variants


def _report_text(config: ExperimentConfig, base_symbols: list[str], run_metrics_df: pl.DataFrame, sensitivity_summary_df: pl.DataFrame) -> str:
    def _fmt(value: Any, digits: int = 4) -> str:
        if value is None:
            return "N/A"
        return f"{float(value):.{digits}f}"

    lines = [
        "# ETF Rotation US Best Optimization Experiment",
        "",
        "## Scope",
        f"- experiment_id: `{config.experiment_id}`",
        f"- universe: `{config.universe}`",
        f"- date window: `{config.start_date}` -> `{config.end_date}`",
        f"- base symbols: `{'|'.join(base_symbols)}`",
        "",
        "## TopK Comparison",
    ]
    topk_rows = run_metrics_df.filter(pl.col("scenario_group") == "topk").sort("scenario_name")
    for row in topk_rows.iter_rows(named=True):
        lines.append(
            f"- `{row['scenario_name']}`: sharpe={_fmt(row['sharpe'])}, ann_return={_fmt(row['annualized_return'])}, max_dd={_fmt(row['max_drawdown'])}, weekly_std={_fmt(row['weekly_return_std'])}, slope13_neg={_fmt(row['slope13_negative_share'])}"
        )

    lines.extend(["", "## Risk Comparison"])
    risk_rows = run_metrics_df.filter(pl.col("scenario_group") == "risk").sort("scenario_name")
    for row in risk_rows.iter_rows(named=True):
        lines.append(
            f"- `{row['scenario_name']}`: sharpe={_fmt(row['sharpe'])}, ann_return={_fmt(row['annualized_return'])}, max_dd={_fmt(row['max_drawdown'])}, weekly_std={_fmt(row['weekly_return_std'])}, risk_half={int(row['risk_half_events'])}, stop_loss={int(row['stop_loss_events'])}"
        )

    lines.extend(["", "## Controlled Replacements"])
    replacement_rows = run_metrics_df.filter(pl.col("scenario_group") == "controlled_replacement").sort("scenario_name")
    if replacement_rows.is_empty():
        lines.append("- no controlled replacements run")
    else:
        for row in replacement_rows.iter_rows(named=True):
            lines.append(
                f"- `{row['scenario_name']}`: sharpe={_fmt(row['sharpe'])}, ann_return={_fmt(row['annualized_return'])}, max_dd={_fmt(row['max_drawdown'])}, weekly_std={_fmt(row['weekly_return_std'])}"
            )

    lines.extend(["", "## Sensitivity Summary"])
    for row in sensitivity_summary_df.sort(["scenario_type", "delta_sharpe_median"]).iter_rows(named=True):
        lines.append(
            f"- `{row['scenario_type']}`: median_delta_sharpe={_fmt(row['delta_sharpe_median'])}, worst_delta_sharpe={_fmt(row['delta_sharpe_worst'])}, key_symbol=`{row['worst_symbol'] or 'N/A'}`, replacement=`{row['worst_replacement'] or 'N/A'}`"
        )

    lines.extend(["", "## Recommendation"])
    best_topk = topk_rows.sort("sharpe", descending=True).row(0, named=True) if not topk_rows.is_empty() else None
    best_risk = risk_rows.sort("sharpe", descending=True).row(0, named=True) if not risk_rows.is_empty() else None
    if best_topk:
        lines.append(f"- topk winner by sharpe: `{best_topk['scenario_name']}`")
    if best_risk:
        lines.append(f"- risk winner by sharpe: `{best_risk['scenario_name']}`")
    lines.append("- use smoothness metrics and sensitivity results together; do not rank solely by Sharpe.")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run ETF Rotation US Best optimization experiments")
    parser.add_argument("--experiment-id", default=f"{DEFAULT_OUTPUT_SUBDIR}_{_now_id()}")
    parser.add_argument("--universe", default="etf_rotation_US")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--top-k-values", default="3,4,5")
    parser.add_argument("--max-per-tag", type=int, default=DEFAULT_MAX_PER_TAG)
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--rebalance-weekday", type=int, default=2)
    parser.add_argument("--execution-lag", type=int, default=1)
    parser.add_argument("--commission-bps", type=float, default=DEFAULT_COMMISSION_BPS)
    parser.add_argument("--slippage-bps", type=float, default=DEFAULT_SLIPPAGE_BPS)
    parser.add_argument("--initial-capital", type=float, default=40000.0)
    parser.add_argument("--commission-min", type=float, default=0.01)
    parser.add_argument("--cash-interest-rate", type=float, default=0.01)
    args = parser.parse_args()

    top_k_values = tuple(sorted({int(part.strip()) for part in str(args.top_k_values).split(",") if part.strip()}))
    config = ExperimentConfig(
        experiment_id=args.experiment_id,
        universe=args.universe,
        start_date=args.start_date,
        end_date=args.end_date,
        top_k_values=top_k_values,
        max_per_tag=args.max_per_tag,
        profile_name=args.profile,
        rebalance_weekday=args.rebalance_weekday,
        execution_lag=args.execution_lag,
        commission_bps=args.commission_bps,
        slippage_bps=args.slippage_bps,
        initial_capital=args.initial_capital,
        commission_min=args.commission_min,
        cash_interest_rate=args.cash_interest_rate,
    )

    output_dir = DEFAULT_OUTPUT_ROOT.parent / DEFAULT_OUTPUT_SUBDIR / config.experiment_id
    output_dir.mkdir(parents=True, exist_ok=True)

    base_rows = load_universe(config.universe, default_asset_type="etf_US")
    base_symbols = [row["symbol"] for row in base_rows]
    all_rows = _candidate_rows()
    rows_by_symbol = {
        row["symbol"]: {
            **row,
            "exposure_group": EXPOSURE_GROUPS.get(row["symbol"], row.get("tag", "other")),
            "asset_class": ASSET_CLASSES.get(row["symbol"], "other"),
        }
        for row in all_rows
    }

    start_date = _normalize_date(config.start_date)
    end_date = _normalize_date(config.end_date)
    signal_source = _load_full_momentum_snapshot(all_rows, start_date, end_date)
    market_data = _load_market_data(["etf_US"], start_date, end_date).filter(pl.col("symbol").is_in(sorted(rows_by_symbol)))
    corr_map = _pairwise_correlation_map(market_data, sorted(rows_by_symbol))
    default_risk = ETFRotationUSStrategy().default_risk_config()

    run_metrics_rows: list[dict[str, Any]] = []
    period_rows: list[dict[str, Any]] = []
    contribution_rows: list[dict[str, Any]] = []
    rolling_rows: list[dict[str, Any]] = []
    drawdown_rows: list[dict[str, Any]] = []
    scenario_rows: list[dict[str, Any]] = []
    sensitivity_rows: list[dict[str, Any]] = []

    base_variant = _variant(base_symbols, "best_base", "best_base", corr_map)
    base_counts = (
        signal_source.filter(pl.col("symbol").is_in(base_symbols)).group_by("time").len().get_column("len").to_list()
    )

    def _record_run(
        *,
        variant: dict[str, Any],
        top_k: int,
        risk_config: RiskOverlayConfig | None,
        scenario_group: str,
        scenario_name: str,
    ) -> dict[str, Any]:
        decisions_df = _variant_decisions(signal_source, variant["symbols"], top_k, config.max_per_tag)
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
            risk_config=risk_config,
            initial_capital=config.initial_capital,
            commission_min=config.commission_min,
            cash_interest_rate=config.cash_interest_rate,
            market_data_override=market_data,
        )
        periods_df, period_holdings_df, analysis_summary = build_rebalance_period_analysis(result)
        contribution_data, contribution_summary = _contribution_rows(variant["variant_id"], period_holdings_df)
        rolling_slope_data = _rolling_slope_rows(variant["variant_id"], _weekly_nav(periods_df))
        drawdown_data = _drawdown_segment_rows(variant["variant_id"], _weekly_nav(periods_df))
        metrics_row = _metrics_row(
            experiment_id=config.experiment_id,
            candidate_snapshot_hash="best_universe",
            run_id=variant["variant_id"],
            variant=variant,
            top_k=top_k,
            decisions_df=decisions_df,
            result=result,
            periods_df=periods_df,
            period_holdings_df=period_holdings_df,
            contributions=contribution_summary,
            effective_candidate_counts=[int(value) for value in base_counts],
        )
        metrics_row.update(_smoothness_metrics(periods_df, rolling_slope_data, analysis_summary))
        metrics_row.update(
            {
                "scenario_group": scenario_group,
                "scenario_name": scenario_name,
                "risk_std_threshold": risk_config.std_threshold if risk_config else None,
                "risk_cv_threshold": risk_config.cv_threshold if risk_config else None,
                "stop_loss_rate": risk_config.stop_loss_rate if risk_config else None,
            }
        )
        run_metrics_rows.append(metrics_row)
        contribution_rows.extend(contribution_data)
        rolling_rows.extend(rolling_slope_data)
        drawdown_rows.extend(drawdown_data)
        for row in periods_df.iter_rows(named=True):
            period_rows.append(
                {
                    "variant_id": variant["variant_id"],
                    "scenario_group": scenario_group,
                    "scenario_name": scenario_name,
                    **{key: (value.isoformat() if hasattr(value, "isoformat") else value) for key, value in row.items()},
                }
            )
        scenario_rows.append(
            {
                "variant_id": variant["variant_id"],
                "scenario_group": scenario_group,
                "scenario_name": scenario_name,
                "top_k": top_k,
                "symbols": "|".join(variant["symbols"]),
            }
        )
        return metrics_row

    for top_k in config.top_k_values:
        _record_run(
            variant={**base_variant, "variant_id": f"best_topk_{top_k}", "variant_type": "topk"},
            top_k=top_k,
            risk_config=default_risk,
            scenario_group="topk",
            scenario_name=f"topk_{top_k}_default_risk",
        )

    for scenario_id, group_name, risk_cfg in _base_risk_scenarios():
        _record_run(
            variant={**base_variant, "variant_id": f"best_{scenario_id}", "variant_type": "risk"},
            top_k=4,
            risk_config=risk_cfg,
            scenario_group=group_name,
            scenario_name=scenario_id,
        )

    for scenario_name, symbols in _controlled_replacements(base_symbols):
        _record_run(
            variant=_variant(symbols, f"best_{scenario_name}", "controlled_replacement", corr_map),
            top_k=4,
            risk_config=default_risk,
            scenario_group="controlled_replacement",
            scenario_name=scenario_name,
        )

    base_symbols_sorted = sorted(base_symbols)
    for symbol in base_symbols_sorted:
        remove_symbols = sorted(member for member in base_symbols_sorted if member != symbol)
        remove_variant = _variant(remove_symbols, f"best_remove_{symbol}", "sensitivity_remove", corr_map)
        remove_metrics = _record_run(
            variant=remove_variant,
            top_k=min(4, len(remove_symbols) - 1),
            risk_config=default_risk,
            scenario_group="sensitivity_remove",
            scenario_name=f"remove_{symbol}",
        )
        sensitivity_rows.append(
            {
                "scenario_type": "remove",
                "modified_symbol": symbol,
                "replacement_symbol": "",
                "variant_id": remove_variant["variant_id"],
                "delta_sharpe": round(float(remove_metrics["sharpe"]) - 1.6557, 6),
                "delta_annualized_return": round(float(remove_metrics["annualized_return"]) - 0.1626, 6),
                "drawdown_worsening": round(abs(float(remove_metrics["max_drawdown"])) - abs(-0.0797), 6),
            }
        )
        replacement_symbol, _ = _choose_replacement_symbol(symbol, base_symbols_sorted, rows_by_symbol, corr_map)
        if replacement_symbol:
            replace_symbols = sorted([member for member in base_symbols_sorted if member != symbol] + [replacement_symbol])
            replace_variant = _variant(replace_symbols, f"best_swap_{symbol}_to_{replacement_symbol}", "sensitivity_replace", corr_map)
            replace_metrics = _record_run(
                variant=replace_variant,
                top_k=4,
                risk_config=default_risk,
                scenario_group="sensitivity_replace",
                scenario_name=f"replace_{symbol}_to_{replacement_symbol}",
            )
            sensitivity_rows.append(
                {
                    "scenario_type": "replace",
                    "modified_symbol": symbol,
                    "replacement_symbol": replacement_symbol,
                    "variant_id": replace_variant["variant_id"],
                    "delta_sharpe": round(float(replace_metrics["sharpe"]) - 1.6557, 6),
                    "delta_annualized_return": round(float(replace_metrics["annualized_return"]) - 0.1626, 6),
                    "drawdown_worsening": round(abs(float(replace_metrics["max_drawdown"])) - abs(-0.0797), 6),
                }
            )

    sensitivity_df = pl.DataFrame(sensitivity_rows)
    sensitivity_summary_df = (
        sensitivity_df.group_by("scenario_type")
        .agg(
            [
                pl.col("delta_sharpe").median().alias("delta_sharpe_median"),
                pl.col("delta_sharpe").min().alias("delta_sharpe_worst"),
                pl.col("modified_symbol").sort_by("delta_sharpe").first().alias("worst_symbol"),
                pl.col("replacement_symbol").sort_by("delta_sharpe").first().alias("worst_replacement"),
            ]
        )
        if not sensitivity_df.is_empty()
        else pl.DataFrame()
    )

    run_metrics_df = pl.DataFrame(run_metrics_rows).sort(["scenario_group", "scenario_name"])
    pl.DataFrame(period_rows).write_csv(output_dir / "period_metrics.csv")
    run_metrics_df.write_csv(output_dir / "run_metrics.csv")
    pl.DataFrame(contribution_rows).write_csv(output_dir / "symbol_contributions.csv")
    pl.DataFrame(rolling_rows).write_csv(output_dir / "rolling_slope_metrics.csv")
    pl.DataFrame(drawdown_rows).write_csv(output_dir / "drawdown_segments.csv")
    sensitivity_df.write_csv(output_dir / "sensitivity_details.csv")
    sensitivity_summary_df.write_csv(output_dir / "sensitivity_summary.csv")
    pl.DataFrame(scenario_rows).write_csv(output_dir / "scenario_definitions.csv")
    (output_dir / "experiment_config.json").write_text(json.dumps(asdict(config), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output_dir / "experiment_report.md").write_text(
        _report_text(config, base_symbols_sorted, run_metrics_df, sensitivity_summary_df),
        encoding="utf-8",
    )
    print(json.dumps({"experiment_id": config.experiment_id, "output_dir": str(output_dir)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Run phase-3 ETF rotation US single-name sensitivity experiments."""
from __future__ import annotations

import argparse
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

from app.backtest.reporting import build_rebalance_period_analysis
from app.backtest.runner import _load_market_data, run_backtest
from app.backtest.risk_overlay import RiskOverlayConfig
from app.strategy.etf_rotation import ETFRotationUSStrategy
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
    _run_id,
    _run_selection_snapshot,
    _selection_stability,
    _variant_decisions,
)


DEFAULT_PHASE2_EXPERIMENT_ID = "phase2_full_20260712"
DEFAULT_OUTPUT_SUBDIR = "phase3_sensitivity"
DEFAULT_POOL_SIZE = 12
DEFAULT_TOP_K = 5
DEFAULT_CANDIDATE_COUNT = 5


@dataclass(frozen=True)
class Phase3Config:
    experiment_id: str
    phase2_experiment_id: str
    start_date: str
    end_date: str
    pool_size: int
    top_k: int
    shortlist_count: int
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


def _phase2_dir(experiment_id: str) -> Path:
    return DEFAULT_OUTPUT_ROOT / experiment_id


def _load_phase2_inputs(experiment_id: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    phase2_dir = _phase2_dir(experiment_id)
    return (
        pl.read_csv(phase2_dir / "run_metrics.csv"),
        pl.read_csv(phase2_dir / "universe_variants.csv"),
    )


def _candidate_score_expr() -> pl.Expr:
    return (
        pl.col("sharpe") * 0.45
        + pl.col("annualized_return") * 1.1
        + pl.col("avg_holding_overlap_ratio") * 0.7
        - pl.col("top1_profit_share") * 0.65
        + pl.col("max_drawdown") * 0.45
    )


def _select_phase2_shortlist(
    run_metrics_df: pl.DataFrame,
    pool_size: int,
    top_k: int,
    shortlist_count: int,
) -> pl.DataFrame:
    return (
        run_metrics_df
        .filter(
            (pl.col("variant_type") == "phase1_random_low_corr")
            & (pl.col("pool_size") == pool_size)
            & (pl.col("top_k") == top_k)
        )
        .with_columns(_candidate_score_expr().alias("phase3_selection_score"))
        .sort(
            ["phase3_selection_score", "sharpe", "annualized_return", "max_drawdown"],
            descending=[True, True, True, True],
        )
        .head(shortlist_count)
    )


def _variant_symbol_map(universe_variants_df: pl.DataFrame) -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {}
    for row in universe_variants_df.iter_rows(named=True):
        mapping[str(row["variant_id"])] = [symbol for symbol in str(row["symbols"]).split("|") if symbol]
    return mapping


def _avg_corr_to_pool(symbol: str, pool_symbols: list[str], corr_map: dict[tuple[str, str], float]) -> float:
    values = [float(corr_map.get((symbol, member), 0.0)) for member in pool_symbols if member != symbol]
    return sum(values) / len(values) if values else 0.0


def _pool_corr_metrics(symbols: list[str], corr_map: dict[tuple[str, str], float]) -> tuple[float, float]:
    values: list[float] = []
    for index, left in enumerate(symbols):
        for right in symbols[index + 1:]:
            values.append(float(corr_map.get((left, right), 0.0)))
    if not values:
        return 0.0, 0.0
    return sum(values) / len(values), max(values)


def _choose_addition_symbol(
    pool_symbols: list[str],
    rows_by_symbol: dict[str, dict[str, str]],
    corr_map: dict[tuple[str, str], float],
) -> tuple[str | None, str]:
    pool_exposures = {rows_by_symbol[symbol]["exposure_group"] for symbol in pool_symbols}
    candidates: list[tuple[tuple[int, float, str], str]] = []
    for symbol, row in rows_by_symbol.items():
        if symbol in pool_symbols:
            continue
        unique_exposure = 1 if row["exposure_group"] not in pool_exposures else 0
        avg_corr = _avg_corr_to_pool(symbol, pool_symbols, corr_map)
        candidates.append(((-unique_exposure, avg_corr, symbol), symbol))
    if not candidates:
        return None, "no_candidate"
    _, symbol = min(candidates)
    mode = "unique_exposure_low_corr" if rows_by_symbol[symbol]["exposure_group"] not in pool_exposures else "low_corr"
    return symbol, mode


def _choose_replacement_symbol(
    removed_symbol: str,
    pool_symbols: list[str],
    rows_by_symbol: dict[str, dict[str, str]],
    corr_map: dict[tuple[str, str], float],
) -> tuple[str | None, str]:
    removed_row = rows_by_symbol[removed_symbol]
    remaining = [symbol for symbol in pool_symbols if symbol != removed_symbol]
    remaining_exposures = {rows_by_symbol[symbol]["exposure_group"] for symbol in remaining}
    excluded = [symbol for symbol in rows_by_symbol if symbol not in pool_symbols]

    same_exposure = [
        symbol for symbol in excluded if rows_by_symbol[symbol]["exposure_group"] == removed_row["exposure_group"]
    ]
    if same_exposure:
        chosen = min(same_exposure, key=lambda symbol: (_avg_corr_to_pool(symbol, remaining, corr_map), symbol))
        return chosen, "same_exposure"

    same_asset = [
        symbol
        for symbol in excluded
        if rows_by_symbol[symbol]["asset_class"] == removed_row["asset_class"]
        and rows_by_symbol[symbol]["exposure_group"] not in remaining_exposures
    ]
    if same_asset:
        chosen = min(same_asset, key=lambda symbol: (_avg_corr_to_pool(symbol, remaining, corr_map), symbol))
        return chosen, "same_asset_low_corr"

    unique_exposure = [
        symbol for symbol in excluded if rows_by_symbol[symbol]["exposure_group"] not in remaining_exposures
    ]
    if unique_exposure:
        chosen = min(unique_exposure, key=lambda symbol: (_avg_corr_to_pool(symbol, remaining, corr_map), symbol))
        return chosen, "low_corr_fallback"

    if not excluded:
        return None, "no_candidate"
    chosen = min(excluded, key=lambda symbol: (_avg_corr_to_pool(symbol, remaining, corr_map), symbol))
    return chosen, "any_fallback"


def _phase3_variants(
    candidate_id: str,
    pool_symbols: list[str],
    rows_by_symbol: dict[str, dict[str, str]],
    corr_map: dict[tuple[str, str], float],
) -> list[dict[str, Any]]:
    variants: list[dict[str, Any]] = []
    base_avg_corr, base_max_corr = _pool_corr_metrics(pool_symbols, corr_map)
    variants.append(
        {
            "variant_id": candidate_id,
            "variant_type": "phase3_base",
            "parent_candidate_id": candidate_id,
            "scenario_type": "base",
            "modified_symbol": "",
            "replacement_symbol": "",
            "replacement_mode": "",
            "pool_size": len(pool_symbols),
            "symbols": sorted(pool_symbols),
            "avg_pair_corr": round(base_avg_corr, 6),
            "max_pair_corr": round(base_max_corr, 6),
        }
    )

    add_symbol, add_mode = _choose_addition_symbol(pool_symbols, rows_by_symbol, corr_map)
    if add_symbol:
        added_symbols = sorted([*pool_symbols, add_symbol])
        add_avg_corr, add_max_corr = _pool_corr_metrics(added_symbols, corr_map)
        variants.append(
            {
                "variant_id": f"{candidate_id}_add_{add_symbol}",
                "variant_type": "phase3_add",
                "parent_candidate_id": candidate_id,
                "scenario_type": "add",
                "modified_symbol": add_symbol,
                "replacement_symbol": add_symbol,
                "replacement_mode": add_mode,
                "pool_size": len(pool_symbols) + 1,
                "symbols": added_symbols,
                "avg_pair_corr": round(add_avg_corr, 6),
                "max_pair_corr": round(add_max_corr, 6),
            }
        )

    for symbol in pool_symbols:
        reduced = sorted([member for member in pool_symbols if member != symbol])
        remove_avg_corr, remove_max_corr = _pool_corr_metrics(reduced, corr_map)
        variants.append(
            {
                "variant_id": f"{candidate_id}_drop_{symbol}",
                "variant_type": "phase3_remove",
                "parent_candidate_id": candidate_id,
                "scenario_type": "remove",
                "modified_symbol": symbol,
                "replacement_symbol": "",
                "replacement_mode": "remove_only",
                "pool_size": len(reduced),
                "symbols": reduced,
                "avg_pair_corr": round(remove_avg_corr, 6),
                "max_pair_corr": round(remove_max_corr, 6),
            }
        )
        replacement_symbol, mode = _choose_replacement_symbol(symbol, pool_symbols, rows_by_symbol, corr_map)
        if replacement_symbol:
            replaced = sorted([member for member in pool_symbols if member != symbol] + [replacement_symbol])
            replace_avg_corr, replace_max_corr = _pool_corr_metrics(replaced, corr_map)
            variants.append(
                {
                    "variant_id": f"{candidate_id}_swap_{symbol}_to_{replacement_symbol}",
                    "variant_type": "phase3_replace",
                    "parent_candidate_id": candidate_id,
                    "scenario_type": "replace",
                    "modified_symbol": symbol,
                    "replacement_symbol": replacement_symbol,
                    "replacement_mode": mode,
                    "pool_size": len(replaced),
                    "symbols": replaced,
                    "avg_pair_corr": round(replace_avg_corr, 6),
                    "max_pair_corr": round(replace_max_corr, 6),
                }
            )
    return variants


def _dd_worsening_expr() -> pl.Expr:
    return (pl.col("max_drawdown").abs() - pl.col("base_max_drawdown").abs()).alias("drawdown_worsening")


def _summarize_phase3(run_metrics_df: pl.DataFrame) -> pl.DataFrame:
    base_df = (
        run_metrics_df
        .filter(pl.col("scenario_type") == "base")
        .select(
            [
                "parent_candidate_id",
                pl.col("sharpe").alias("base_sharpe"),
                pl.col("annualized_return").alias("base_annualized_return"),
                pl.col("max_drawdown").alias("base_max_drawdown"),
                pl.col("avg_holding_overlap_ratio").alias("base_overlap"),
                pl.col("top1_profit_share").alias("base_top1_profit_share"),
                pl.col("symbols").alias("base_symbols"),
            ]
        )
    )
    scenario_df = (
        run_metrics_df
        .filter(pl.col("scenario_type") != "base")
        .join(base_df, on="parent_candidate_id", how="left")
        .with_columns(
            [
                (pl.col("sharpe") - pl.col("base_sharpe")).alias("delta_sharpe"),
                (pl.col("annualized_return") - pl.col("base_annualized_return")).alias("delta_annualized_return"),
                (pl.col("avg_holding_overlap_ratio") - pl.col("base_overlap")).alias("delta_overlap"),
                (pl.col("top1_profit_share") - pl.col("base_top1_profit_share")).alias("delta_top1_profit_share"),
                _dd_worsening_expr(),
            ]
        )
    )
    scenario_summary = (
        scenario_df.group_by(["parent_candidate_id", "scenario_type"])
        .agg(
            [
                pl.len().alias("run_count"),
                pl.col("delta_sharpe").median().alias("median_delta_sharpe"),
                pl.col("delta_sharpe").min().alias("worst_delta_sharpe"),
                pl.col("delta_annualized_return").median().alias("median_delta_return"),
                pl.col("drawdown_worsening").median().alias("median_drawdown_worsening"),
                pl.col("drawdown_worsening").max().alias("worst_drawdown_worsening"),
            ]
        )
    )
    worst_case = (
        scenario_df.sort(["parent_candidate_id", "scenario_type", "delta_sharpe"])
        .group_by(["parent_candidate_id", "scenario_type"])
        .agg(
            [
                pl.col("variant_id").first().alias("worst_case_variant_id"),
                pl.col("modified_symbol").first().alias("worst_case_symbol"),
                pl.col("replacement_symbol").first().alias("worst_case_replacement"),
            ]
        )
    )
    summary = scenario_summary.join(worst_case, on=["parent_candidate_id", "scenario_type"], how="left")

    def _scenario_slice(name: str, prefix: str) -> pl.DataFrame:
        return (
            summary.filter(pl.col("scenario_type") == name)
            .select(
                [
                    "parent_candidate_id",
                    pl.col("run_count").alias(f"{prefix}_run_count"),
                    pl.col("median_delta_sharpe").alias(f"{prefix}_median_delta_sharpe"),
                    pl.col("worst_delta_sharpe").alias(f"{prefix}_worst_delta_sharpe"),
                    pl.col("median_delta_return").alias(f"{prefix}_median_delta_return"),
                    pl.col("median_drawdown_worsening").alias(f"{prefix}_median_drawdown_worsening"),
                    pl.col("worst_drawdown_worsening").alias(f"{prefix}_worst_drawdown_worsening"),
                    pl.col("worst_case_symbol").alias(f"{prefix}_worst_case_symbol"),
                    pl.col("worst_case_replacement").alias(f"{prefix}_worst_case_replacement"),
                ]
            )
        )

    candidate_summary = (
        base_df
        .join(_scenario_slice("remove", "remove"), on="parent_candidate_id", how="left")
        .join(_scenario_slice("replace", "replace"), on="parent_candidate_id", how="left")
        .join(_scenario_slice("add", "add"), on="parent_candidate_id", how="left")
        .with_columns(
            (
                pl.col("base_sharpe")
                + pl.col("remove_median_delta_sharpe").fill_null(0.0) * 0.9
                + pl.col("replace_median_delta_sharpe").fill_null(0.0) * 0.7
                + pl.col("add_median_delta_sharpe").fill_null(0.0) * 0.4
                - pl.col("remove_worst_drawdown_worsening").fill_null(0.0) * 0.8
                - pl.col("base_top1_profit_share").fill_null(0.0) * 0.6
            ).alias("phase3_robustness_score")
        )
        .sort(["phase3_robustness_score", "base_sharpe"], descending=[True, True])
    )
    return scenario_df, candidate_summary


def _report_text(
    config: Phase3Config,
    shortlist_df: pl.DataFrame,
    candidate_summary_df: pl.DataFrame,
) -> str:
    def _fmt(value: Any, digits: int = 4) -> str:
        if value is None:
            return "N/A"
        return f"{float(value):.{digits}f}"

    winner = candidate_summary_df.row(0, named=True)
    lines = [
        "# ETF Rotation US Universe Phase-3 Sensitivity Experiment",
        "",
        "## Scope",
        f"- experiment_id: `{config.experiment_id}`",
        f"- phase2_source: `{config.phase2_experiment_id}`",
        f"- fixed grid: `N={config.pool_size}, TopK={config.top_k}`",
        f"- shortlist_count: `{config.shortlist_count}`",
        f"- profile: `{config.profile_name}`",
        "",
        "## Shortlist",
    ]
    for row in shortlist_df.iter_rows(named=True):
        lines.append(
            f"- `{row['universe_variant_id']}`: score={row['phase3_selection_score']:.4f}, sharpe={row['sharpe']:.4f}, max_dd={row['max_drawdown']:.4f}, top1_share={row['top1_profit_share']:.4f}"
        )
    lines.extend(
        [
            "",
            "## Winner",
            f"- selected candidate: `{winner['parent_candidate_id']}`",
            f"- base sharpe={_fmt(winner['base_sharpe'])}, ann_return={_fmt(winner['base_annualized_return'])}, max_dd={_fmt(winner['base_max_drawdown'])}",
            f"- remove median delta sharpe={_fmt(winner['remove_median_delta_sharpe'])}, worst={_fmt(winner['remove_worst_delta_sharpe'])}, worst symbol=`{winner['remove_worst_case_symbol'] or 'N/A'}`",
            f"- replace median delta sharpe={_fmt(winner['replace_median_delta_sharpe'])}, worst={_fmt(winner['replace_worst_delta_sharpe'])}, most difficult replacement=`{winner['replace_worst_case_symbol'] or 'N/A'}` -> `{winner['replace_worst_case_replacement'] or 'N/A'}`",
            f"- add-one delta sharpe={_fmt(winner['add_median_delta_sharpe'])}",
            "",
            "## Candidate Summary",
        ]
    )
    for row in candidate_summary_df.iter_rows(named=True):
        lines.append(
            f"- `{row['parent_candidate_id']}`: robustness={_fmt(row['phase3_robustness_score'])}, base_sharpe={_fmt(row['base_sharpe'])}, remove_worst={_fmt(row['remove_worst_delta_sharpe'])}, replace_worst={_fmt(row['replace_worst_delta_sharpe'])}, add_delta={_fmt(row['add_median_delta_sharpe'])}"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "- 同经济暴露替代在当前 23 只冻结候选中经常不存在，因此替换实验优先使用同资产大类、与剩余组合平均相关性更低的替代 ETF。",
            "- 第三阶段用于识别稳健候选池和关键敏感 ETF，不用于重新搜索新的全局最优池。",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run ETF rotation US phase-3 sensitivity experiments")
    parser.add_argument("--phase2-experiment-id", default=DEFAULT_PHASE2_EXPERIMENT_ID)
    parser.add_argument("--experiment-id", default=f"{DEFAULT_OUTPUT_SUBDIR}_{_now_id()}")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--pool-size", type=int, default=DEFAULT_POOL_SIZE)
    parser.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    parser.add_argument("--shortlist-count", type=int, default=DEFAULT_CANDIDATE_COUNT)
    parser.add_argument("--max-per-tag", type=int, default=DEFAULT_MAX_PER_TAG)
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--rebalance-weekday", type=int, default=2)
    parser.add_argument("--execution-lag", type=int, default=1)
    parser.add_argument("--commission-bps", type=float, default=5.0)
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    parser.add_argument("--initial-capital", type=float, default=40000.0)
    parser.add_argument("--commission-min", type=float, default=0.01)
    parser.add_argument("--cash-interest-rate", type=float, default=0.01)
    args = parser.parse_args()

    config = Phase3Config(
        experiment_id=args.experiment_id,
        phase2_experiment_id=args.phase2_experiment_id,
        start_date=args.start_date,
        end_date=args.end_date,
        pool_size=args.pool_size,
        top_k=args.top_k,
        shortlist_count=args.shortlist_count,
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

    phase2_run_metrics_df, phase2_variants_df = _load_phase2_inputs(config.phase2_experiment_id)
    shortlist_df = _select_phase2_shortlist(
        phase2_run_metrics_df,
        config.pool_size,
        config.top_k,
        config.shortlist_count,
    )
    variant_map = _variant_symbol_map(phase2_variants_df)

    output_dir = DEFAULT_OUTPUT_ROOT / config.experiment_id
    output_dir.mkdir(parents=True, exist_ok=True)

    candidate_rows = _candidate_rows()
    rows_by_symbol = {
        row["symbol"]: {
            **row,
            "exposure_group": EXPOSURE_GROUPS.get(row["symbol"], row.get("tag", "other")),
            "asset_class": ASSET_CLASSES.get(row["symbol"], "other"),
        }
        for row in candidate_rows
    }
    symbols = sorted(rows_by_symbol)
    start_date = _normalize_date(config.start_date)
    end_date = _normalize_date(config.end_date)
    signal_source = _load_full_momentum_snapshot(candidate_rows, start_date, end_date)
    market_data = _load_market_data(["etf_US"], start_date, end_date).filter(pl.col("symbol").is_in(symbols))
    corr_map = _pairwise_correlation_map(market_data, symbols)
    risk_config = ETFRotationUSStrategy().default_risk_config()

    candidate_selection_rows: list[dict[str, Any]] = []
    scenario_rows: list[dict[str, Any]] = []
    run_metrics_rows: list[dict[str, Any]] = []
    contribution_rows: list[dict[str, Any]] = []

    for row in shortlist_df.iter_rows(named=True):
        parent_id = str(row["universe_variant_id"])
        pool_symbols = variant_map[parent_id]
        candidate_selection_rows.append(
            {
                "candidate_id": parent_id,
                "phase3_selection_score": float(row["phase3_selection_score"]),
                "phase2_sharpe": float(row["sharpe"]),
                "phase2_annualized_return": float(row["annualized_return"]),
                "phase2_max_drawdown": float(row["max_drawdown"]),
                "phase2_overlap": float(row["avg_holding_overlap_ratio"]),
                "phase2_top1_profit_share": float(row["top1_profit_share"]),
                "symbols": "|".join(pool_symbols),
            }
        )
        for variant in _phase3_variants(parent_id, pool_symbols, rows_by_symbol, corr_map):
            decisions_df = _variant_decisions(signal_source, variant["symbols"], config.top_k, config.max_per_tag)
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
                risk_config=RiskOverlayConfig(
                    std_threshold=risk_config.std_threshold,
                    cv_threshold=risk_config.cv_threshold,
                    stop_loss_rate=risk_config.stop_loss_rate,
                    half_weight=risk_config.half_weight,
                    std_long_window=risk_config.std_long_window,
                    std_short_window=risk_config.std_short_window,
                    cv_window=risk_config.cv_window,
                ),
                initial_capital=config.initial_capital,
                commission_min=config.commission_min,
                cash_interest_rate=config.cash_interest_rate,
                market_data_override=market_data,
            )
            periods_df, period_holdings_df, _ = build_rebalance_period_analysis(result)
            snapshot_for_variant = _run_selection_snapshot(signal_source, variant["symbols"])
            effective_counts = (
                snapshot_for_variant.group_by("time").len().get_column("len").to_list()
                if not snapshot_for_variant.is_empty()
                else []
            )
            metrics_row = _metrics_row(
                experiment_id=config.experiment_id,
                candidate_snapshot_hash="",
                run_id=_run_id(variant["variant_id"], config.top_k),
                variant=variant,
                top_k=config.top_k,
                decisions_df=decisions_df,
                result=result,
                periods_df=periods_df,
                period_holdings_df=period_holdings_df,
                contributions=_contribution_rows(_run_id(variant["variant_id"], config.top_k), period_holdings_df)[1],
                effective_candidate_counts=[int(value) for value in effective_counts],
            )
            metrics_row.update(
                {
                    "parent_candidate_id": variant["parent_candidate_id"],
                    "scenario_type": variant["scenario_type"],
                    "modified_symbol": variant["modified_symbol"],
                    "replacement_symbol": variant["replacement_symbol"],
                    "replacement_mode": variant["replacement_mode"],
                }
            )
            run_metrics_rows.append(metrics_row)
            scenario_rows.append(
                {
                    "variant_id": _run_id(variant["variant_id"], config.top_k),
                    "parent_candidate_id": variant["parent_candidate_id"],
                    "scenario_type": variant["scenario_type"],
                    "modified_symbol": variant["modified_symbol"],
                    "replacement_symbol": variant["replacement_symbol"],
                    "replacement_mode": variant["replacement_mode"],
                    "pool_size": variant["pool_size"],
                    "symbols": "|".join(variant["symbols"]),
                }
            )
            contribution_rows.extend(_contribution_rows(_run_id(variant["variant_id"], config.top_k), period_holdings_df)[0])

    run_metrics_df = pl.DataFrame(run_metrics_rows).sort(["parent_candidate_id", "scenario_type", "variant_id"])
    scenario_df, candidate_summary_df = _summarize_phase3(run_metrics_df)

    pl.DataFrame(candidate_selection_rows).write_csv(output_dir / "candidate_selection.csv")
    pl.DataFrame(scenario_rows).write_csv(output_dir / "scenario_definitions.csv")
    run_metrics_df.write_csv(output_dir / "sensitivity_run_metrics.csv")
    scenario_df.write_csv(output_dir / "scenario_deltas.csv")
    candidate_summary_df.write_csv(output_dir / "candidate_summary.csv")
    pl.DataFrame(contribution_rows).write_csv(output_dir / "symbol_contributions.csv")
    (output_dir / "experiment_config.json").write_text(
        json.dumps(asdict(config), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (output_dir / "experiment_report.md").write_text(
        _report_text(config, shortlist_df, candidate_summary_df),
        encoding="utf-8",
    )
    print(json.dumps({"experiment_id": config.experiment_id, "output_dir": str(output_dir)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

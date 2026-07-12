"""Run data prep and phase-1 ETF rotation US universe experiments."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

import polars as pl
from sqlalchemy import text


REPO_ROOT = Path(__file__).resolve().parents[1]
for candidate in ["/app", str(REPO_ROOT)]:
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from app.backtest.reporting import build_rebalance_period_analysis
from app.backtest.runner import _load_market_data, run_backtest
from app.backtest.risk_overlay import RiskOverlayConfig
from app.services.asset_universe import load_etl_universe
from app.services.signal_score import _attach_symbol_names, _query_universe_factors
from app.signals.composite import apply_composite_score
from app.signals.normalization import apply_signal_profile
from app.signals.profiles import get_signal_profile
from app.strategy.etf_rotation import ETFRotationUSStrategy
from app.utils.db import get_engine


DEFAULT_START_DATE = "2021-07-12"
DEFAULT_END_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
DEFAULT_POOL_SIZES = (6, 8, 10, 12, 15, 23)
DEFAULT_SAMPLES_PER_SIZE = 50
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "logs" / "experiments" / "etf_rotation_US_universe"
DEFAULT_PROFILE = "trend_etf_momentum_reg20"
DEFAULT_SEED = 20260712
DEFAULT_TOP_K = 4
DEFAULT_MAX_PER_TAG = 1

EXPOSURE_GROUPS = {
    "VTI": "us_broad_market",
    "QQQ": "growth_technology",
    "VTV": "value",
    "SMH": "semiconductor",
    "IWM": "small_cap",
    "VT": "global_equity",
    "VEA": "developed_ex_us",
    "EEM": "emerging_market",
    "VNQ": "real_estate",
    "XLK": "technology_sector",
    "XLF": "financial_sector",
    "XLE": "energy_sector",
    "XLI": "industrial_sector",
    "XLV": "defensive_healthcare",
    "TLT": "treasury_long",
    "IEF": "treasury_mid",
    "BND": "aggregate_bond",
    "LQD": "investment_grade_credit",
    "HYG": "high_yield_credit",
    "GLD": "gold",
    "USO": "oil",
    "UUP": "usd",
    "DBC": "broad_commodity",
}

ASSET_CLASSES = {
    "VTI": "equity",
    "QQQ": "equity",
    "VTV": "equity",
    "SMH": "equity",
    "IWM": "equity",
    "VT": "equity",
    "VEA": "equity",
    "EEM": "equity",
    "VNQ": "real_asset",
    "XLK": "equity",
    "XLF": "equity",
    "XLE": "equity",
    "XLI": "equity",
    "XLV": "equity",
    "TLT": "bond",
    "IEF": "bond",
    "BND": "bond",
    "LQD": "bond",
    "HYG": "bond",
    "GLD": "commodity",
    "USO": "commodity",
    "UUP": "currency",
    "DBC": "commodity",
}

BOND_SYMBOLS = {symbol for symbol, group in ASSET_CLASSES.items() if group == "bond"}
COMMODITY_SYMBOLS = {symbol for symbol, group in ASSET_CLASSES.items() if group == "commodity"}
EQUITY_SYMBOLS = {symbol for symbol, group in ASSET_CLASSES.items() if group == "equity"}
DIVERSIFIER_SYMBOLS = {"UUP", "VNQ", "VT", "VEA", "EEM"}
BASELINE_VARIANTS = {
    "baseline_best8": ["EEM", "GLD", "IEF", "UUP", "VNQ", "XLF", "XLK", "XLV"],
    "baseline_current13": ["DBC", "EEM", "GLD", "IWM", "QQQ", "SMH", "UUP", "VEA", "VTI", "VTV", "XLE", "XLF", "XLV"],
}


@dataclass(frozen=True)
class ExperimentConfig:
    experiment_id: str
    start_date: str
    end_date: str
    pool_sizes: tuple[int, ...]
    samples_per_size: int
    top_k: int
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
    random_seed: int


def _now_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _parse_sizes(raw: str) -> tuple[int, ...]:
    values = tuple(sorted({int(part.strip()) for part in raw.split(",") if part.strip()}))
    if not values:
        raise ValueError("pool_sizes 不能为空")
    return values


def _parse_top_k_values(raw: str) -> tuple[int, ...]:
    values = tuple(sorted({int(part.strip()) for part in raw.split(",") if part.strip()}))
    if not values:
        raise ValueError("top_k_values 不能为空")
    return values


def _normalize_date(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def _candidate_rows() -> list[dict[str, str]]:
    rows = load_etl_universe("etf_US")
    enriched: list[dict[str, str]] = []
    for row in rows:
        symbol = row["symbol"]
        enriched.append(
            {
                **row,
                "exposure_group": EXPOSURE_GROUPS.get(symbol, row.get("tag", "other")),
                "asset_class": ASSET_CLASSES.get(symbol, "other"),
            }
        )
    return enriched


def _query_market_and_factor_metadata(symbols: list[str], start_date: str, end_date: str) -> tuple[dict[str, dict[str, Any]], dict[str, int]]:
    market_sql = text(
        """
        SELECT symbol,
               MIN(time) AS market_start_date,
               MAX(time) AS market_end_date,
               COUNT(*) AS market_rows,
               AVG(amount) AS avg_amount,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) AS median_amount
        FROM market.daily
        WHERE asset_type = 'etf_US'
          AND symbol = ANY(:symbols)
          AND time >= :start_date
          AND time <= :end_date
        GROUP BY symbol
        """
    )
    factor_sql = text(
        """
        SELECT symbol,
               factor_name,
               MIN(time) AS factor_start_date,
               MAX(time) AS factor_end_date,
               COUNT(*) AS factor_rows
        FROM factors.daily_factors
        WHERE asset_type = 'etf_US'
          AND symbol = ANY(:symbols)
          AND factor_name = ANY(:factor_names)
          AND time >= :start_date
          AND time < (CAST(:end_date AS date) + INTERVAL '1 day')
        GROUP BY symbol, factor_name
        """
    )
    calendar_sql = text(
        """
        SELECT COUNT(DISTINCT time) AS trading_days
        FROM market.daily
        WHERE asset_type = 'etf_US'
          AND time >= :start_date
          AND time <= :end_date
        """
    )
    metadata: dict[str, dict[str, Any]] = {symbol: {} for symbol in symbols}
    with get_engine().connect() as conn:
        for row in conn.execute(
            market_sql,
            {"symbols": symbols, "start_date": start_date, "end_date": end_date},
        ).mappings():
            metadata[row["symbol"]].update(
                {
                    "market_start_date": row["market_start_date"].isoformat() if row["market_start_date"] else "",
                    "market_end_date": row["market_end_date"].isoformat() if row["market_end_date"] else "",
                    "market_rows": int(row["market_rows"] or 0),
                    "avg_amount": float(row["avg_amount"] or 0.0),
                    "median_amount": float(row["median_amount"] or 0.0),
                }
            )
        for row in conn.execute(
            factor_sql,
            {
                "symbols": symbols,
                "factor_names": ["momentum_reg_20", "std_score", "cv"],
                "start_date": start_date,
                "end_date": end_date,
            },
        ).mappings():
            prefix = str(row["factor_name"])
            start_value = row["factor_start_date"]
            end_value = row["factor_end_date"]
            metadata[row["symbol"]].update(
                {
                    f"{prefix}_start_date": start_value.isoformat() if start_value else "",
                    f"{prefix}_end_date": end_value.isoformat() if end_value else "",
                    f"{prefix}_rows": int(row["factor_rows"] or 0),
                }
            )
        trading_days = int(
            conn.execute(calendar_sql, {"start_date": start_date, "end_date": end_date}).scalar_one()
            or 0
        )
    counts = {"trading_days": trading_days}
    return metadata, counts


def _write_candidate_snapshot(
    rows: list[dict[str, str]],
    metadata: dict[str, dict[str, Any]],
    trading_days: int,
    output_path: Path,
) -> str:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "asset_type",
        "symbol",
        "name",
        "tag",
        "exposure_group",
        "asset_class",
        "is_active",
        "market_start_date",
        "market_end_date",
        "market_rows",
        "market_coverage_ratio",
        "momentum_reg_20_start_date",
        "momentum_reg_20_end_date",
        "momentum_reg_20_rows",
        "std_score_start_date",
        "std_score_end_date",
        "std_score_rows",
        "cv_start_date",
        "cv_end_date",
        "cv_rows",
        "avg_amount",
        "median_amount",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in sorted(rows, key=lambda item: item["symbol"]):
            meta = metadata.get(row["symbol"], {})
            writer.writerow(
                {
                    **{key: row.get(key, "") for key in ["asset_type", "symbol", "name", "tag", "exposure_group", "asset_class", "is_active"]},
                    "market_start_date": meta.get("market_start_date", ""),
                    "market_end_date": meta.get("market_end_date", ""),
                    "market_rows": meta.get("market_rows", 0),
                    "market_coverage_ratio": round((meta.get("market_rows", 0) or 0) / trading_days, 6) if trading_days else 0.0,
                    "momentum_reg_20_start_date": meta.get("momentum_reg_20_start_date", ""),
                    "momentum_reg_20_end_date": meta.get("momentum_reg_20_end_date", ""),
                    "momentum_reg_20_rows": meta.get("momentum_reg_20_rows", 0),
                    "std_score_start_date": meta.get("std_score_start_date", ""),
                    "std_score_end_date": meta.get("std_score_end_date", ""),
                    "std_score_rows": meta.get("std_score_rows", 0),
                    "cv_start_date": meta.get("cv_start_date", ""),
                    "cv_end_date": meta.get("cv_end_date", ""),
                    "cv_rows": meta.get("cv_rows", 0),
                    "avg_amount": round(float(meta.get("avg_amount", 0.0) or 0.0), 4),
                    "median_amount": round(float(meta.get("median_amount", 0.0) or 0.0), 4),
                }
            )
    return hashlib.sha256(output_path.read_bytes()).hexdigest()


def _coverage_summary(rows: list[dict[str, str]], metadata: dict[str, dict[str, Any]]) -> dict[str, Any]:
    market_starts = [meta["market_start_date"] for meta in metadata.values() if meta.get("market_start_date")]
    momentum_starts = [meta["momentum_reg_20_start_date"] for meta in metadata.values() if meta.get("momentum_reg_20_start_date")]
    std_starts = [meta["std_score_start_date"] for meta in metadata.values() if meta.get("std_score_start_date")]
    cv_starts = [meta["cv_start_date"] for meta in metadata.values() if meta.get("cv_start_date")]
    common_start_candidates = market_starts + momentum_starts + std_starts + cv_starts
    common_complete_start = max(common_start_candidates) if common_start_candidates else ""
    return {
        "candidate_count": len(rows),
        "common_complete_start": common_complete_start,
        "symbols_missing_market": sorted([symbol for symbol, meta in metadata.items() if not meta.get("market_rows")]),
        "symbols_missing_momentum": sorted([symbol for symbol, meta in metadata.items() if not meta.get("momentum_reg_20_rows")]),
        "symbols_missing_std": sorted([symbol for symbol, meta in metadata.items() if not meta.get("std_score_rows")]),
        "symbols_missing_cv": sorted([symbol for symbol, meta in metadata.items() if not meta.get("cv_rows")]),
    }


def _load_full_momentum_snapshot(rows: list[dict[str, str]], start_date: date, end_date: date) -> pl.DataFrame:
    profile = get_signal_profile(DEFAULT_PROFILE)
    long_df = _query_universe_factors(["momentum_reg_20"], rows, start_date, end_date)
    if long_df.is_empty():
        return pl.DataFrame()
    wide = (
        long_df.pivot(values="factor_value", index=["time", "asset_type", "symbol"], on="factor_name")
        .sort(["time", "symbol"])
        .filter(pl.col("momentum_reg_20").is_not_null() & pl.col("momentum_reg_20").is_finite())
    )
    base = _attach_symbol_names(wide, rows).with_columns(pl.lit(profile.signal_mode).alias("signal_mode"))
    return base.select(["time", "asset_type", "signal_mode", "symbol", "symbol_name", "tag", "momentum_reg_20"])


def _run_selection_snapshot(source_df: pl.DataFrame, symbols: list[str]) -> pl.DataFrame:
    profile = get_signal_profile(DEFAULT_PROFILE)
    subset = source_df.filter(pl.col("symbol").is_in(symbols))
    if subset.is_empty():
        return subset
    scored = apply_signal_profile(subset, profile)
    scored = apply_composite_score(scored, profile)
    ranked = (
        scored.with_columns(
            pl.col("composite_score")
            .rank(method="ordinal", descending=True)
            .over("time")
            .cast(pl.UInt32)
            .alias("rank")
        )
        .sort(["time", "composite_score", "symbol"], descending=[False, True, False])
    )
    return ranked


def _pool_daily_returns(market_data: pl.DataFrame) -> pl.DataFrame:
    return (
        market_data.select(["time", "symbol", "daily_return"])
        .pivot(values="daily_return", index="time", on="symbol")
        .sort("time")
    )


def _pairwise_correlation_map(market_data: pl.DataFrame, symbols: list[str]) -> dict[tuple[str, str], float]:
    pivot = _pool_daily_returns(market_data).select(["time", *symbols]) if symbols else pl.DataFrame()
    corr_map: dict[tuple[str, str], float] = {}
    if pivot.is_empty():
        return corr_map
    for idx, left in enumerate(symbols):
        for right in symbols[idx + 1:]:
            pair = (
                pivot.select([left, right])
                .drop_nulls()
            )
            if pair.height < 2:
                corr = 0.0
            else:
                corr = float(pair.select(pl.corr(left, right)).item() or 0.0)
            corr_map[(left, right)] = corr
            corr_map[(right, left)] = corr
    return corr_map


def _combo_corr_metrics(symbols: list[str], corr_map: dict[tuple[str, str], float]) -> tuple[float, float]:
    values: list[float] = []
    for idx, left in enumerate(symbols):
        for right in symbols[idx + 1:]:
            values.append(float(corr_map.get((left, right), 0.0)))
    if not values:
        return 0.0, 0.0
    return float(sum(values) / len(values)), float(max(values))


def _is_valid_pool(symbols: list[str], rows_by_symbol: dict[str, dict[str, str]], pool_size: int) -> bool:
    row_values = [rows_by_symbol[symbol] for symbol in symbols]
    equity_count = sum(1 for row in row_values if row["asset_class"] == "equity")
    has_bond = any(symbol in BOND_SYMBOLS for symbol in symbols)
    has_gold_or_commodity = any(symbol in COMMODITY_SYMBOLS or symbol == "GLD" for symbol in symbols)
    has_diversifier = any(symbol in DIVERSIFIER_SYMBOLS for symbol in symbols)
    exposure_groups = {row["exposure_group"] for row in row_values}
    if len(exposure_groups) != len(symbols):
        return False
    if equity_count > pool_size // 2:
        return False
    if not has_bond or not has_gold_or_commodity:
        return False
    if pool_size >= 8 and not has_diversifier:
        return False
    return True


def _generate_variants(
    rows: list[dict[str, str]],
    pool_sizes: tuple[int, ...],
    samples_per_size: int,
    corr_map: dict[tuple[str, str], float],
    random_seed: int,
) -> list[dict[str, Any]]:
    rows_by_symbol = {row["symbol"]: row for row in rows}
    all_symbols = sorted(rows_by_symbol)
    rng = random.Random(random_seed)
    variants: list[dict[str, Any]] = []
    for name, symbols in BASELINE_VARIANTS.items():
        avg_corr, max_corr = _combo_corr_metrics(symbols, corr_map)
        variants.append(
            {
                "variant_id": name,
                "variant_type": "baseline",
                "pool_size": len(symbols),
                "symbols": symbols,
                "avg_pair_corr": round(avg_corr, 6),
                "max_pair_corr": round(max_corr, 6),
            }
        )
    full_symbols = sorted(all_symbols)
    avg_corr, max_corr = _combo_corr_metrics(full_symbols, corr_map)
    variants.append(
        {
            "variant_id": "baseline_full23",
            "variant_type": "baseline",
            "pool_size": len(full_symbols),
            "symbols": full_symbols,
            "avg_pair_corr": round(avg_corr, 6),
            "max_pair_corr": round(max_corr, 6),
        }
    )

    for pool_size in pool_sizes:
        sample_target = samples_per_size
        candidate_pool: dict[tuple[str, ...], tuple[float, float]] = {}
        max_attempts = max(2000, samples_per_size * 400)
        attempts = 0
        while attempts < max_attempts and len(candidate_pool) < sample_target * 6:
            attempts += 1
            combo = tuple(sorted(rng.sample(all_symbols, pool_size)))
            if combo in candidate_pool:
                continue
            if not _is_valid_pool(list(combo), rows_by_symbol, pool_size):
                continue
            candidate_pool[combo] = _combo_corr_metrics(list(combo), corr_map)
        ranked = sorted(candidate_pool.items(), key=lambda item: (item[1][0], item[1][1], item[0]))
        for index, (combo, metrics) in enumerate(ranked[:sample_target], start=1):
            variants.append(
                {
                    "variant_id": f"phase1_n{pool_size:02d}_{index:03d}",
                    "variant_type": "phase1_random_low_corr",
                    "pool_size": pool_size,
                    "symbols": list(combo),
                    "avg_pair_corr": round(metrics[0], 6),
                    "max_pair_corr": round(metrics[1], 6),
                }
            )
    return variants


def _variant_decisions(source_df: pl.DataFrame, symbols: list[str], top_k: int, max_per_tag: int) -> pl.DataFrame:
    strategy = ETFRotationUSStrategy(top_n=top_k, profile_name=DEFAULT_PROFILE, max_per_tag=max_per_tag)
    snapshot = _run_selection_snapshot(source_df, symbols)
    return strategy.build_decisions(snapshot)


def _run_id(variant_id: str, top_k: int) -> str:
    return f"{variant_id}_k{top_k}"


def _selection_stability(periods_df: pl.DataFrame) -> tuple[float, float]:
    if periods_df.is_empty() or periods_df.height < 2:
        return 0.0, 0.0
    symbol_lists: list[list[str]] = []
    for raw in periods_df.get_column("selected_symbols").to_list():
        if raw in (None, ""):
            symbol_lists.append([])
            continue
        try:
            symbol_lists.append([str(part) for part in json.loads(str(raw))])
        except json.JSONDecodeError:
            symbol_lists.append([part for part in str(raw).split("|") if part])
    overlaps: list[float] = []
    jaccards: list[float] = []
    for left, right in zip(symbol_lists, symbol_lists[1:]):
        left_set = set(left)
        right_set = set(right)
        if not left_set and not right_set:
            continue
        overlaps.append(len(left_set & right_set) / max(1, min(len(left_set), len(right_set))))
        jaccards.append(len(left_set & right_set) / max(1, len(left_set | right_set)))
    return (sum(overlaps) / len(overlaps) if overlaps else 0.0, sum(jaccards) / len(jaccards) if jaccards else 0.0)


def _contribution_rows(variant_id: str, period_holdings_df: pl.DataFrame) -> tuple[list[dict[str, Any]], dict[str, float]]:
    if period_holdings_df.is_empty():
        return [], {"top1_profit_share": 0.0, "top3_profit_share": 0.0}
    grouped = (
        period_holdings_df.group_by("symbol")
        .agg(
            [
                pl.col("tag").drop_nulls().first().alias("tag"),
                pl.col("period_pnl").sum().alias("total_period_pnl"),
                pl.col("period_return_contribution").sum().alias("total_return_contribution"),
                pl.len().alias("period_count"),
            ]
        )
        .sort(["total_period_pnl", "symbol"], descending=[True, False])
    )
    positive_total = float(sum(value for value in grouped.get_column("total_period_pnl").to_list() if value and value > 0.0))
    pnl_values = [float(value or 0.0) for value in grouped.get_column("total_period_pnl").to_list()]
    top1_share = max(pnl_values[0], 0.0) / positive_total if positive_total > 0 else 0.0
    top3_share = sum(max(value, 0.0) for value in pnl_values[:3]) / positive_total if positive_total > 0 else 0.0
    rows = []
    for row in grouped.iter_rows(named=True):
        rows.append(
            {
                "variant_id": variant_id,
                "symbol": row["symbol"],
                "tag": row["tag"],
                "total_period_pnl": round(float(row["total_period_pnl"] or 0.0), 6),
                "total_return_contribution": round(float(row["total_return_contribution"] or 0.0), 6),
                "period_count": int(row["period_count"]),
            }
        )
    return rows, {"top1_profit_share": top1_share, "top3_profit_share": top3_share}


def _metrics_row(
    *,
    experiment_id: str,
    candidate_snapshot_hash: str,
    run_id: str,
    variant: dict[str, Any],
    top_k: int,
    decisions_df: pl.DataFrame,
    result,
    periods_df: pl.DataFrame,
    period_holdings_df: pl.DataFrame,
    contributions: dict[str, float],
    effective_candidate_counts: list[int],
) -> dict[str, Any]:
    metrics = dict(result.metrics)
    avg_period_return = (
        float(periods_df.get_column("period_return").mean()) if not periods_df.is_empty() else 0.0
    )
    win_rate = (
        float((periods_df.get_column("period_return") > 0).mean()) if not periods_df.is_empty() else 0.0
    )
    avg_turnover = float(result.returns_df.get_column("turnover").mean()) if not result.returns_df.is_empty() else 0.0
    cum_turnover = float(result.returns_df.get_column("turnover").sum()) if not result.returns_df.is_empty() else 0.0
    total_cost = float(result.returns_df.get_column("cost").sum()) if not result.returns_df.is_empty() else 0.0
    overlap_ratio, jaccard_ratio = _selection_stability(periods_df)
    best10_period_share = 0.0
    if not periods_df.is_empty():
        positive_sum = float(sum(max(value, 0.0) for value in periods_df.get_column("period_return").to_list()))
        best10_sum = float(sum(sorted([max(value, 0.0) for value in periods_df.get_column("period_return").to_list()], reverse=True)[:10]))
        best10_period_share = best10_sum / positive_sum if positive_sum > 0 else 0.0
    return {
        "experiment_id": experiment_id,
        "variant_id": run_id,
        "universe_variant_id": variant["variant_id"],
        "variant_type": variant["variant_type"],
        "candidate_snapshot_hash": candidate_snapshot_hash,
        "pool_size": int(variant["pool_size"]),
        "top_k": top_k,
        "selection_ratio": round(top_k / int(variant["pool_size"]), 6),
        "symbols": "|".join(variant["symbols"]),
        "avg_pair_corr": variant["avg_pair_corr"],
        "max_pair_corr": variant["max_pair_corr"],
        "decision_rows": int(decisions_df.height),
        "effective_candidate_count_min": min(effective_candidate_counts) if effective_candidate_counts else 0,
        "effective_candidate_count_median": float(median(effective_candidate_counts)) if effective_candidate_counts else 0.0,
        "effective_candidate_count_max": max(effective_candidate_counts) if effective_candidate_counts else 0,
        "period_count": int(periods_df.height),
        "annualized_return": round(float(metrics.get("annualized_return", 0.0) or 0.0), 6),
        "annualized_vol": round(float(metrics.get("annualized_vol", 0.0) or 0.0), 6),
        "sharpe": round(float(metrics.get("sharpe", 0.0) or 0.0), 6),
        "max_drawdown": round(float(metrics.get("max_drawdown", 0.0) or 0.0), 6),
        "calmar": round(float(metrics.get("calmar", 0.0) or 0.0), 6),
        "total_return": round(float(metrics.get("total_return", 0.0) or 0.0), 6),
        "end_nav": round(float(metrics.get("end_nav", 0.0) or 0.0), 6),
        "avg_period_return": round(avg_period_return, 6),
        "period_win_rate": round(win_rate, 6),
        "avg_turnover": round(avg_turnover, 6),
        "cum_turnover": round(cum_turnover, 6),
        "total_cost": round(total_cost, 6),
        "risk_half_events": int(metrics.get("risk_half_events", 0.0) or 0.0),
        "stop_loss_events": int(metrics.get("stop_loss_events", 0.0) or 0.0),
        "rebalance_trade_events": int(metrics.get("rebalance_trade_events", 0.0) or 0.0),
        "avg_holding_overlap_ratio": round(overlap_ratio, 6),
        "avg_holding_jaccard": round(jaccard_ratio, 6),
        "top1_profit_share": round(contributions["top1_profit_share"], 6),
        "top3_profit_share": round(contributions["top3_profit_share"], 6),
        "best10_period_positive_return_share": round(best10_period_share, 6),
    }


def _leaderboard(run_metrics_df: pl.DataFrame) -> pl.DataFrame:
    experiment_runs = run_metrics_df.filter(pl.col("variant_type") == "phase1_random_low_corr")
    if experiment_runs.is_empty():
        return pl.DataFrame()
    return (
        experiment_runs.group_by(["pool_size", "top_k"])
        .agg(
            [
                pl.len().alias("run_count"),
                pl.col("sharpe").median().alias("sharpe_median"),
                pl.col("sharpe").quantile(0.25).alias("sharpe_q25"),
                pl.col("sharpe").quantile(0.75).alias("sharpe_q75"),
                pl.col("sharpe").quantile(0.10).alias("sharpe_q10"),
                pl.col("sharpe").max().alias("sharpe_best"),
                pl.col("annualized_return").median().alias("ann_return_median"),
                pl.col("max_drawdown").median().alias("max_drawdown_median"),
                pl.col("cum_turnover").median().alias("cum_turnover_median"),
            ]
        )
        .sort(["pool_size", "top_k"])
    )


def _experiment_stage_name(config: ExperimentConfig) -> str:
    return "Phase-2" if len(config.top_k_values) > 1 else "Phase-1"


def _report_text(
    config: ExperimentConfig,
    coverage: dict[str, Any],
    leaderboard_df: pl.DataFrame,
    baselines_df: pl.DataFrame,
) -> str:
    stage_name = _experiment_stage_name(config)
    lines = [
        f"# ETF Rotation US Universe {stage_name} Experiment",
        "",
        "## Scope",
        f"- experiment_id: `{config.experiment_id}`",
        f"- date window: `{config.start_date}` -> `{config.end_date}`",
        f"- pool_sizes: `{', '.join(str(value) for value in config.pool_sizes)}`",
        f"- samples_per_size: `{config.samples_per_size}`",
        f"- top_k_values: `{', '.join(str(value) for value in config.top_k_values)}`",
        f"- profile: `{config.profile_name}`",
        "",
        "## Data Prep",
        f"- frozen candidate count: `{coverage['candidate_count']}`",
        f"- common complete start (market + momentum_reg_20 + std_score + cv): `{coverage['common_complete_start'] or 'N/A'}`",
        "- survivorship-bias note: 当前结果是“当前存续 ETF 的历史回放”，不代表 point-in-time 可交易全集。",
        "",
        "## Baselines",
    ]
    if baselines_df.is_empty():
        lines.append("- no baseline rows")
    else:
        for row in baselines_df.sort(["universe_variant_id", "top_k"]).iter_rows(named=True):
            lines.append(
                f"- `{row['universe_variant_id']}` / top_k={row['top_k']}: size={row['pool_size']}, sharpe={row['sharpe']:.4f}, ann_return={row['annualized_return']:.4f}, max_dd={row['max_drawdown']:.4f}"
            )
    lines.extend(["", f"## {stage_name} Leaderboard"])
    if leaderboard_df.is_empty():
        lines.append(f"- no {stage_name.lower()} runs")
    else:
        best_row = leaderboard_df.sort("sharpe_median", descending=True).row(0, named=True)
        lines.append(
            f"- best median sharpe grid: `N={best_row['pool_size']}, TopK={best_row['top_k']}` with median `{best_row['sharpe_median']:.4f}`"
        )
        for row in leaderboard_df.iter_rows(named=True):
            lines.append(
                f"- `N={row['pool_size']}, TopK={row['top_k']}`: median_sharpe={row['sharpe_median']:.4f}, q25={row['sharpe_q25']:.4f}, q75={row['sharpe_q75']:.4f}, median_max_dd={row['max_drawdown_median']:.4f}"
            )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run ETF rotation US universe experiments")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--pool-sizes", default=",".join(str(value) for value in DEFAULT_POOL_SIZES))
    parser.add_argument("--samples-per-size", type=int, default=DEFAULT_SAMPLES_PER_SIZE)
    parser.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    parser.add_argument("--top-k-grid", default=None, help="逗号分隔的 TopK 网格，例如 3,4,5")
    parser.add_argument("--max-per-tag", type=int, default=DEFAULT_MAX_PER_TAG)
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--rebalance-weekday", type=int, default=2)
    parser.add_argument("--execution-lag", type=int, default=1)
    parser.add_argument("--commission-bps", type=float, default=5.0)
    parser.add_argument("--slippage-bps", type=float, default=5.0)
    parser.add_argument("--initial-capital", type=float, default=40000.0)
    parser.add_argument("--commission-min", type=float, default=0.01)
    parser.add_argument("--cash-interest-rate", type=float, default=0.01)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--experiment-id", default=f"exp_{_now_id()}")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()

    config = ExperimentConfig(
        experiment_id=args.experiment_id,
        start_date=args.start_date,
        end_date=args.end_date,
        pool_sizes=_parse_sizes(args.pool_sizes),
        samples_per_size=args.samples_per_size,
        top_k=args.top_k,
        top_k_values=_parse_top_k_values(args.top_k_grid) if args.top_k_grid else (args.top_k,),
        max_per_tag=args.max_per_tag,
        profile_name=args.profile,
        rebalance_weekday=args.rebalance_weekday,
        execution_lag=args.execution_lag,
        commission_bps=args.commission_bps,
        slippage_bps=args.slippage_bps,
        initial_capital=args.initial_capital,
        commission_min=args.commission_min,
        cash_interest_rate=args.cash_interest_rate,
        random_seed=args.seed,
    )

    output_dir = Path(args.output_root) / config.experiment_id
    output_dir.mkdir(parents=True, exist_ok=True)

    candidate_rows = _candidate_rows()
    symbols = sorted(row["symbol"] for row in candidate_rows)
    metadata, coverage_counts = _query_market_and_factor_metadata(symbols, config.start_date, config.end_date)
    snapshot_hash = _write_candidate_snapshot(
        candidate_rows,
        metadata,
        coverage_counts["trading_days"],
        output_dir / "candidate_snapshot.csv",
    )
    coverage = _coverage_summary(candidate_rows, metadata)

    start_date = _normalize_date(config.start_date)
    end_date = _normalize_date(config.end_date)
    signal_source = _load_full_momentum_snapshot(candidate_rows, start_date, end_date)
    market_data = _load_market_data(["etf_US"], start_date, end_date)
    market_data = market_data.filter(pl.col("symbol").is_in(symbols))
    corr_map = _pairwise_correlation_map(market_data, symbols)

    variants = _generate_variants(
        candidate_rows,
        config.pool_sizes,
        config.samples_per_size,
        corr_map,
        config.random_seed,
    )

    (output_dir / "experiment_config.json").write_text(
        json.dumps(
            {
                **asdict(config),
                "candidate_snapshot_hash": snapshot_hash,
                "coverage": coverage,
            },
            ensure_ascii=False,
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )

    universe_variants_rows: list[dict[str, Any]] = []
    run_metrics_rows: list[dict[str, Any]] = []
    period_metrics_rows: list[dict[str, Any]] = []
    contribution_rows: list[dict[str, Any]] = []
    selection_rows: list[dict[str, Any]] = []
    correlation_rows: list[dict[str, Any]] = []
    risk_config = ETFRotationUSStrategy().default_risk_config()

    for variant in variants:
        snapshot_for_variant = _run_selection_snapshot(signal_source, variant["symbols"])
        effective_counts = (
            snapshot_for_variant.group_by("time").len().get_column("len").to_list()
            if not snapshot_for_variant.is_empty()
            else []
        )
        universe_variants_rows.append(
            {
                "variant_id": variant["variant_id"],
                "variant_type": variant["variant_type"],
                "pool_size": variant["pool_size"],
                "symbol_count": len(variant["symbols"]),
                "symbols": "|".join(variant["symbols"]),
                "avg_pair_corr": variant["avg_pair_corr"],
                "max_pair_corr": variant["max_pair_corr"],
            }
        )
        correlation_rows.append(
            {
                "variant_id": variant["variant_id"],
                "pool_size": variant["pool_size"],
                "avg_pair_corr": variant["avg_pair_corr"],
                "max_pair_corr": variant["max_pair_corr"],
                "symbols": "|".join(variant["symbols"]),
            }
        )
        for top_k in config.top_k_values:
            if top_k >= int(variant["pool_size"]):
                continue
            run_id = _run_id(variant["variant_id"], top_k)
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
            contribution_data, contribution_summary = _contribution_rows(run_id, period_holdings_df)
            metrics_row = _metrics_row(
                experiment_id=config.experiment_id,
                candidate_snapshot_hash=snapshot_hash,
                run_id=run_id,
                variant=variant,
                top_k=top_k,
                decisions_df=decisions_df,
                result=result,
                periods_df=periods_df,
                period_holdings_df=period_holdings_df,
                contributions=contribution_summary,
                effective_candidate_counts=[int(value) for value in effective_counts],
            )
            run_metrics_rows.append(metrics_row)
            contribution_rows.extend(contribution_data)
            overlap_ratio, jaccard_ratio = _selection_stability(periods_df)
            selection_rows.append(
                {
                    "variant_id": run_id,
                    "universe_variant_id": variant["variant_id"],
                    "pool_size": variant["pool_size"],
                    "top_k": top_k,
                    "avg_holding_overlap_ratio": round(overlap_ratio, 6),
                    "avg_holding_jaccard": round(jaccard_ratio, 6),
                }
            )
            for row in periods_df.iter_rows(named=True):
                period_metrics_rows.append(
                    {
                        "variant_id": run_id,
                        "universe_variant_id": variant["variant_id"],
                        "pool_size": variant["pool_size"],
                        "top_k": top_k,
                        **{
                            key: (value.isoformat() if hasattr(value, "isoformat") else value)
                            for key, value in row.items()
                        },
                    }
                )

    run_metrics_df = pl.DataFrame(run_metrics_rows).sort(["variant_type", "pool_size", "top_k", "variant_id"])
    baselines_df = run_metrics_df.filter(pl.col("variant_type") == "baseline")
    leaderboard_df = _leaderboard(run_metrics_df)

    pl.DataFrame(universe_variants_rows).write_csv(output_dir / "universe_variants.csv")
    run_metrics_df.write_csv(output_dir / "run_metrics.csv")
    pl.DataFrame(period_metrics_rows).write_csv(output_dir / "period_metrics.csv")
    pl.DataFrame(contribution_rows).write_csv(output_dir / "symbol_contributions.csv")
    pl.DataFrame(selection_rows).write_csv(output_dir / "selection_stability.csv")
    pl.DataFrame(correlation_rows).write_csv(output_dir / "correlation_summary.csv")
    leaderboard_df.write_csv(output_dir / "leaderboard.csv")

    report = _report_text(config, coverage, leaderboard_df, baselines_df)
    (output_dir / "experiment_report.md").write_text(report, encoding="utf-8")
    print(json.dumps({"experiment_id": config.experiment_id, "output_dir": str(output_dir)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

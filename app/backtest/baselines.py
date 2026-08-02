"""Reusable benchmark decisions and cash-return helpers for allocation research."""
from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date, datetime, time, timezone

import polars as pl

from app.strategy.base import STRATEGY_DECISION_SCHEMA


GEM_STATIC_GAA_WEIGHTS: dict[str, float] = {
    "SPY": 0.45,
    "ACWX": 0.28,
    "AGG": 0.27,
}


def _signal_time(value: str | date | datetime) -> datetime:
    if isinstance(value, str):
        value = date.fromisoformat(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return datetime.combine(value, time.min, tzinfo=timezone.utc)


def build_buy_and_hold_decisions(
    symbol: str,
    signal_date: str | date | datetime,
    *,
    asset_type: str = "etf_US",
    tag: str = "benchmark",
    strategy_name: str | None = None,
) -> pl.DataFrame:
    """Create one 100% target-weight decision for a buy-and-hold benchmark."""
    normalized_symbol = symbol.strip().upper()
    if not normalized_symbol:
        raise ValueError("symbol 不能为空")
    return build_static_allocation_decisions(
        {normalized_symbol: 1.0},
        signal_date,
        asset_type=asset_type,
        tags={normalized_symbol: tag},
        strategy_name=strategy_name or f"buy_and_hold_{normalized_symbol.lower()}",
    )


def build_static_allocation_decisions(
    weights: Mapping[str, float],
    signal_date: str | date | datetime,
    *,
    asset_type: str = "etf_US",
    tags: Mapping[str, str] | None = None,
    strategy_name: str = "static_allocation",
) -> pl.DataFrame:
    """Create an initial target-weight table for a static allocation benchmark."""
    normalized_weights = {str(symbol).strip().upper(): float(weight) for symbol, weight in weights.items()}
    if not normalized_weights or any(not symbol for symbol in normalized_weights):
        raise ValueError("weights 必须包含至少一个有效 symbol")
    if any(weight <= 0 for weight in normalized_weights.values()):
        raise ValueError("weights 必须全部大于 0")
    total_weight = sum(normalized_weights.values())
    if abs(total_weight - 1.0) > 1e-8:
        raise ValueError(f"weights 合计必须为 1，当前为 {total_weight}")

    timestamp = _signal_time(signal_date)
    rows = []
    for rank, (symbol, weight) in enumerate(normalized_weights.items(), start=1):
        tag = (tags or {}).get(symbol, "benchmark")
        rows.append(
            {
                "time": timestamp,
                "asset_type": asset_type,
                "strategy": strategy_name,
                "strategy_mode": "cross_sectional",
                "symbol": symbol,
                "decision_type": "target_weight",
                "signal": 1,
                "target_weight": weight,
                "score": weight,
                "rank": rank,
                "tag": tag,
                "metadata": json.dumps(
                    {"benchmark": strategy_name, "static_weight": weight},
                    ensure_ascii=True,
                    sort_keys=True,
                ),
            }
        )
    return pl.DataFrame(rows, schema=STRATEGY_DECISION_SCHEMA)


def build_gem_baseline_decisions(
    baseline: str,
    signal_date: str | date | datetime,
) -> pl.DataFrame:
    """Build one of the Stage-A GEM baselines: VTI, SPY, or static GAA."""
    normalized = baseline.strip().lower()
    if normalized in {"vti", "spy"}:
        return build_buy_and_hold_decisions(normalized.upper(), signal_date)
    if normalized == "static_gaa":
        return build_static_allocation_decisions(
            GEM_STATIC_GAA_WEIGHTS,
            signal_date,
            tags={"SPY": "us_equity", "ACWX": "ex_us_equity", "AGG": "bond"},
            strategy_name="static_gaa",
        )
    raise ValueError(f"不支持的 GEM baseline: {baseline}")


def build_cash_return_series(
    market_data: pl.DataFrame,
    *,
    symbol: str = "BIL",
    price_column: str = "close",
) -> pl.DataFrame:
    """Convert a T-Bill ETF total-return price series to daily simple returns."""
    required_columns = {"time", "symbol", price_column}
    missing_columns = required_columns - set(market_data.columns)
    if missing_columns:
        raise ValueError(f"market_data 缺少列: {sorted(missing_columns)}")

    selected = market_data.filter(pl.col("symbol") == symbol.upper()).sort("time")
    if selected.is_empty():
        return pl.DataFrame(schema={"time": pl.Date, "cash_return": pl.Float64})

    price = pl.col(price_column).cast(pl.Float64)
    if "adj_factor" in selected.columns:
        valid_factor = (
            pl.when(
                pl.col("adj_factor").cast(pl.Float64).is_finite()
                & (pl.col("adj_factor").cast(pl.Float64) > 0)
            )
            .then(pl.col("adj_factor").cast(pl.Float64))
            .otherwise(1.0)
        )
        price = price * valid_factor
    return (
        selected.select(
            pl.col("time").cast(pl.Date),
            price.alias("_total_return_price"),
        )
        .with_columns(pl.col("_total_return_price").pct_change().alias("cash_return"))
        .drop_nulls("cash_return")
        .select("time", "cash_return")
    )

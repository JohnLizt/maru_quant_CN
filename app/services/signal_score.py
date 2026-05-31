"""Composite signal score query service."""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import polars as pl
from sqlalchemy import text

from app.services.asset_universe import (
    get_pipeline_symbol_name_map,
    get_pipeline_symbol_tag_map,
    normalize_symbol,
)
from app.signals.composite import apply_composite_score
from app.signals.normalization import apply_signal_profile
from app.signals.profiles import SignalProfile, get_signal_profile
from app.utils.db import get_engine


SIGNAL_SCORE_SCHEMA: dict[str, pl.DataType] = {
    "time": pl.Datetime("us", "UTC"),
    "asset_type": pl.Utf8,
    "symbol": pl.Utf8,
    "symbol_name": pl.Utf8,
    "tag": pl.Utf8,
    "ma_cross": pl.Float64,
    "price_to_ma20": pl.Float64,
    "rsi14": pl.Float64,
    "ma_cross_score": pl.Float64,
    "price_to_ma20_score": pl.Float64,
    "rsi14_score": pl.Float64,
    "composite_score": pl.Float64,
    "label": pl.Utf8,
    "contributors": pl.List(pl.Utf8),
}


def _today_utc_date() -> date:
    return datetime.now(timezone.utc).date()


def _normalize_date(value: str | date | datetime | None, *, default: date) -> date:
    if value is None:
        return default
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def _normalize_date_range(
    start_date: str | date | datetime | None,
    end_date: str | date | datetime | None,
) -> tuple[date, date]:
    today = _today_utc_date()
    start = _normalize_date(start_date, default=today)
    end = _normalize_date(end_date, default=today)
    if start > end:
        raise ValueError(f"start_date 不能晚于 end_date: {start} > {end}")
    return start, end


def _normalize_symbols(symbols: list[str] | None) -> list[str]:
    if not symbols:
        return []

    normalized: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        current = normalize_symbol(symbol)
        if current in seen:
            continue
        seen.add(current)
        normalized.append(current)
    return normalized


def _empty_result() -> pl.DataFrame:
    return pl.DataFrame(schema=SIGNAL_SCORE_SCHEMA)


def _query_universe_factors(profile: SignalProfile, asset_type: str, start: date, end: date) -> pl.DataFrame:
    sql = text("""
        SELECT time, asset_type, symbol, factor_name, factor_value
        FROM factors.daily_factors
        WHERE asset_type = :asset_type
          AND factor_name = ANY(:factor_names)
          AND time >= :start_date
          AND time < (CAST(:end_date AS date) + INTERVAL '1 day')
        ORDER BY time, symbol, factor_name
    """)

    params: dict[str, Any] = {
        "asset_type": asset_type,
        "factor_names": profile.factor_names,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
    }

    with get_engine().connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    if not rows:
        return pl.DataFrame(schema={
            "time": pl.Datetime("us", "UTC"),
            "asset_type": pl.Utf8,
            "symbol": pl.Utf8,
            "factor_name": pl.Utf8,
            "factor_value": pl.Float64,
        })

    return pl.DataFrame(
        rows,
        schema=["time", "asset_type", "symbol", "factor_name", "factor_value"],
        orient="row",
    ).with_columns(pl.col("factor_value").cast(pl.Float64))


def _pivot_factors(df: pl.DataFrame, profile: SignalProfile) -> pl.DataFrame:
    wide = df.pivot(values="factor_value", index=["time", "asset_type", "symbol"], on="factor_name").sort(["time", "symbol"])

    missing_columns = [factor_name for factor_name in profile.factor_names if factor_name not in wide.columns]
    for factor_name in missing_columns:
        wide = wide.with_columns(pl.lit(None).cast(pl.Float64).alias(factor_name))

    validity_checks = [pl.col(name).is_not_null() & pl.col(name).is_finite() for name in profile.factor_names]
    return wide.filter(pl.all_horizontal(validity_checks))


def _attach_symbol_names(df: pl.DataFrame, asset_type: str) -> pl.DataFrame:
    stock_pool_map = get_pipeline_symbol_name_map(asset_type)
    tag_map = get_pipeline_symbol_tag_map(asset_type)
    mapping_rows = [
        {"symbol": symbol, "symbol_name": name, "tag": tag_map.get(symbol, "")}
        for symbol, name in stock_pool_map.items()
    ]
    if not mapping_rows:
        return df.with_columns([pl.lit("").alias("symbol_name"), pl.lit("").alias("tag")])

    mapping_df = pl.DataFrame(mapping_rows, schema={"symbol": pl.Utf8, "symbol_name": pl.Utf8, "tag": pl.Utf8})
    return (
        df.join(mapping_df, on="symbol", how="left")
        .with_columns([pl.col("symbol_name").fill_null(""), pl.col("tag").fill_null("")])
    )


def query_signal_scores(
    symbols: list[str] | None = None,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    *,
    asset_type: str = "stock_CN",
    profile_name: str = "trend_v1",
    top_n: int | None = None,
) -> tuple[SignalProfile, pl.DataFrame]:
    """Query composite signal scores over the full factor universe."""

    profile = get_signal_profile(profile_name)
    normalized_symbols = _normalize_symbols(symbols)
    start, end = _normalize_date_range(start_date, end_date)

    raw_factors = _query_universe_factors(profile, asset_type, start, end)
    if raw_factors.is_empty():
        return profile, _empty_result()

    scored = (
        raw_factors
        .pipe(_pivot_factors, profile=profile)
        .pipe(apply_signal_profile, profile=profile)
        .pipe(apply_composite_score, profile=profile)
        .pipe(_attach_symbol_names, asset_type=asset_type)
        .select([
            "time",
            "asset_type",
            "symbol",
            "symbol_name",
            "tag",
            *profile.factor_names,
            *[f"{factor_name}_score" for factor_name in profile.factor_names],
            "composite_score",
            "label",
            "contributors",
        ])
        .sort(["time", "composite_score", "symbol"], descending=[False, True, False])
    )

    if normalized_symbols:
        scored = scored.filter(pl.col("symbol").is_in(normalized_symbols))

    if scored.is_empty():
        return profile, _empty_result()

    if top_n is not None:
        if top_n <= 0:
            raise ValueError("top_n 必须大于 0")
        scored = (
            scored
            .with_columns(pl.col("composite_score").rank(method="ordinal", descending=True).over("time").alias("_rank"))
            .filter(pl.col("_rank") <= top_n)
            .drop("_rank")
            .sort(["time", "composite_score", "symbol"], descending=[False, True, False])
        )

    return profile, scored.cast(SIGNAL_SCORE_SCHEMA)

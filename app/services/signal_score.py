"""Composite signal score query service."""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import polars as pl
from sqlalchemy import text

from app.services.asset_universe import (
    load_etl_universe,
    normalize_symbol,
    resolve_universe_rows,
)
from app.signals.composite import apply_composite_score
from app.signals.normalization import apply_signal_profile
from app.signals.profiles import SignalProfile, get_signal_profile
from app.utils.db import get_engine


BASE_SIGNAL_SCHEMA: dict[str, pl.DataType] = {
    "time": pl.Datetime("us", "UTC"),
    "asset_type": pl.Utf8,
    "signal_mode": pl.Utf8,
    "symbol": pl.Utf8,
    "symbol_name": pl.Utf8,
    "tag": pl.Utf8,
    "composite_score": pl.Float64,
    "label": pl.Utf8,
    "contributors": pl.List(pl.Utf8),
    "rank": pl.UInt32,
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


def _result_schema(profile: SignalProfile) -> dict[str, pl.DataType]:
    schema = dict(BASE_SIGNAL_SCHEMA)
    for factor_name in profile.factor_names:
        schema[factor_name] = pl.Float64
        schema[f"{factor_name}_score"] = pl.Float64
    ordered_keys = ["time", "asset_type", "signal_mode", "symbol", "symbol_name", "tag", *profile.factor_names, *[f"{name}_score" for name in profile.factor_names], "composite_score", "label", "contributors", "rank"]
    return {key: schema[key] for key in ordered_keys}


def _ordered_factor_names(
    profile: SignalProfile,
    extra_factor_names: list[str] | None = None,
) -> list[str]:
    ordered = list(profile.factor_names)
    for factor_name in extra_factor_names or []:
        if factor_name not in ordered:
            ordered.append(factor_name)
    return ordered


def _result_schema_with_extras(
    profile: SignalProfile,
    extra_factor_names: list[str] | None = None,
) -> dict[str, pl.DataType]:
    schema = dict(BASE_SIGNAL_SCHEMA)
    ordered_factor_names = _ordered_factor_names(profile, extra_factor_names)
    for factor_name in ordered_factor_names:
        schema[factor_name] = pl.Float64
    for factor_name in profile.factor_names:
        schema[f"{factor_name}_score"] = pl.Float64
    ordered_keys = ["time", "asset_type", "signal_mode", "symbol", "symbol_name", "tag", *ordered_factor_names, *[f"{name}_score" for name in profile.factor_names], "composite_score", "label", "contributors", "rank"]
    return {key: schema[key] for key in ordered_keys}


def _empty_result(profile: SignalProfile, extra_factor_names: list[str] | None = None) -> pl.DataFrame:
    return pl.DataFrame(schema=_result_schema_with_extras(profile, extra_factor_names))


def _validate_profile_asset_types(profile: SignalProfile, asset_types: set[str]) -> None:
    if "*" in profile.supported_asset_types:
        return
    unsupported_asset_types = sorted(asset_types - set(profile.supported_asset_types))
    if unsupported_asset_types:
        raise ValueError(
            f"profile={profile.name} 不支持 asset_type={unsupported_asset_types}，可用: {list(profile.supported_asset_types)}"
        )


def _query_universe_factors(
    factor_names: list[str],
    universe_rows: list[dict[str, str]],
    start: date,
    end: date,
) -> pl.DataFrame:
    sql = text("""
        SELECT time, asset_type, symbol, factor_name, factor_value
        FROM factors.daily_factors
        WHERE asset_type = :asset_type
          AND symbol = ANY(:symbols)
          AND factor_name = ANY(:factor_names)
          AND time >= :start_date
          AND time < (CAST(:end_date AS date) + INTERVAL '1 day')
        ORDER BY time, symbol, factor_name
    """)

    with get_engine().connect() as conn:
        rows: list[Any] = []
        rows_by_asset_type: dict[str, list[str]] = {}
        for row in universe_rows:
            rows_by_asset_type.setdefault(row["asset_type"], []).append(row["symbol"])
        for asset_type, symbols in rows_by_asset_type.items():
            rows.extend(
                conn.execute(
                    sql,
                    {
                        "asset_type": asset_type,
                        "symbols": sorted(set(symbols)),
                        "factor_names": factor_names,
                        "start_date": start.isoformat(),
                        "end_date": end.isoformat(),
                    },
                ).fetchall()
            )

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


def _pivot_factors(
    df: pl.DataFrame,
    *,
    query_factor_names: list[str],
    required_factor_names: list[str],
) -> pl.DataFrame:
    wide = df.pivot(values="factor_value", index=["time", "asset_type", "symbol"], on="factor_name").sort(["time", "symbol"])

    missing_columns = [factor_name for factor_name in query_factor_names if factor_name not in wide.columns]
    for factor_name in missing_columns:
        wide = wide.with_columns(pl.lit(None).cast(pl.Float64).alias(factor_name))

    validity_checks = [pl.col(name).is_not_null() & pl.col(name).is_finite() for name in required_factor_names]
    return wide.filter(pl.all_horizontal(validity_checks))


def _attach_symbol_names(df: pl.DataFrame, universe_rows: list[dict[str, str]]) -> pl.DataFrame:
    mapping_rows = [
        {
            "asset_type": row["asset_type"],
            "symbol": row["symbol"],
            "symbol_name": str(row.get("name", "")).strip(),
            "tag": str(row.get("tag", "")).strip(),
        }
        for row in universe_rows
    ]
    if not mapping_rows:
        return df.with_columns([pl.lit("").alias("symbol_name"), pl.lit("").alias("tag")])

    mapping_df = pl.DataFrame(
        mapping_rows,
        schema={"asset_type": pl.Utf8, "symbol": pl.Utf8, "symbol_name": pl.Utf8, "tag": pl.Utf8},
    )
    return (
        df.join(mapping_df, on=["asset_type", "symbol"], how="left")
        .with_columns([pl.col("symbol_name").fill_null(""), pl.col("tag").fill_null("")])
    )


def _filter_to_universe(df: pl.DataFrame, universe_rows: list[dict[str, str]]) -> pl.DataFrame:
    universe_df = pl.DataFrame(
        [{"asset_type": row["asset_type"], "symbol": row["symbol"]} for row in universe_rows],
        schema={"asset_type": pl.Utf8, "symbol": pl.Utf8},
    )
    return df.join(universe_df, on=["asset_type", "symbol"], how="inner")


def _smooth_factor_scores(df: pl.DataFrame, profile: SignalProfile) -> pl.DataFrame:
    window = profile.score_smoothing_window
    if window is None or window <= 1:
        return df

    score_columns = [f"{factor_name}_score" for factor_name in profile.factor_names]
    smoothed = (
        df.sort(["symbol", "time"])
        .with_columns([
            pl.col(column).rolling_mean(window_size=window, min_samples=1).over("symbol").alias(column)
            for column in score_columns
        ])
    )
    return smoothed.sort(["time", "symbol"])


def _finalize_signal_frame(
    df: pl.DataFrame,
    *,
    profile: SignalProfile,
) -> pl.DataFrame:
    if profile.signal_mode == "cross_sectional":
        ranked = df.with_columns(
            pl.col("composite_score")
            .rank(method="ordinal", descending=True)
            .over("time")
            .cast(pl.UInt32)
            .alias("rank")
        )
        return ranked.sort(["time", "composite_score", "symbol"], descending=[False, True, False])

    scored = df.with_columns(pl.lit(None).cast(pl.UInt32).alias("rank"))
    return scored.sort(["time", "composite_score", "symbol"], descending=[False, True, False])


def build_signal_snapshot(
    symbols: list[str] | None = None,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    *,
    asset_type: str | None = "stock_CN",
    universe: str | None = None,
    profile_name: str = "trend_etf_momentum_reg20",
    extra_factor_names: list[str] | None = None,
) -> tuple[SignalProfile, pl.DataFrame]:
    """Return full-universe signal ranking table for the requested profile."""

    profile = get_signal_profile(profile_name)
    normalized_symbols = _normalize_symbols(symbols)
    start, end = _normalize_date_range(start_date, end_date)
    if universe:
        target_universe = str(universe).strip()
        if not target_universe:
            raise ValueError("universe 不能为空")
        default_asset_type = asset_type if asset_type else None
        universe_rows = resolve_universe_rows(target_universe, default_asset_type=default_asset_type)
    elif asset_type:
        target_universe = asset_type
        universe_rows = load_etl_universe(asset_type)
    else:
        raise ValueError("asset_type 与 universe 不能同时为空")
    universe_asset_types = {row["asset_type"] for row in universe_rows}
    _validate_profile_asset_types(profile, universe_asset_types)
    query_factor_names = _ordered_factor_names(profile, extra_factor_names)

    raw_factors = _query_universe_factors(query_factor_names, universe_rows, start, end)
    if raw_factors.is_empty():
        return profile, _empty_result(profile, extra_factor_names)

    scored = (
        raw_factors
        .pipe(
            _pivot_factors,
            query_factor_names=query_factor_names,
            required_factor_names=list(profile.factor_names),
        )
        .pipe(_filter_to_universe, universe_rows=universe_rows)
        .pipe(apply_signal_profile, profile=profile)
        .pipe(_smooth_factor_scores, profile=profile)
        .pipe(apply_composite_score, profile=profile)
        .pipe(_attach_symbol_names, universe_rows=universe_rows)
        .with_columns(pl.lit(profile.signal_mode).alias("signal_mode"))
        .pipe(_finalize_signal_frame, profile=profile)
        .select([
            "time",
            "asset_type",
            "signal_mode",
            "symbol",
            "symbol_name",
            "tag",
            *query_factor_names,
            *[f"{factor_name}_score" for factor_name in profile.factor_names],
            "composite_score",
            "label",
            "contributors",
            "rank",
        ])
    )

    if normalized_symbols:
        scored = scored.filter(pl.col("symbol").is_in(normalized_symbols))

    if scored.is_empty():
        return profile, _empty_result(profile, extra_factor_names)

    return profile, scored.cast(_result_schema_with_extras(profile, extra_factor_names))


def query_signal_rankings(
    symbols: list[str] | None = None,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    *,
    asset_type: str | None = "stock_CN",
    universe: str | None = None,
    profile_name: str = "trend_etf_momentum_reg20",
    extra_factor_names: list[str] | None = None,
) -> tuple[SignalProfile, pl.DataFrame]:
    """Backward-compatible alias for build_signal_snapshot."""

    return build_signal_snapshot(
        symbols,
        start_date,
        end_date,
        asset_type=asset_type,
        universe=universe,
        profile_name=profile_name,
        extra_factor_names=extra_factor_names,
    )


def query_signal_scores(
    symbols: list[str] | None = None,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    *,
    asset_type: str | None = "stock_CN",
    universe: str | None = None,
    profile_name: str = "trend_etf_momentum_reg20",
    top_n: int | None = None,
    extra_factor_names: list[str] | None = None,
) -> tuple[SignalProfile, pl.DataFrame]:
    """Query composite signal scores over the full factor universe."""

    profile, scored = build_signal_snapshot(
        symbols,
        start_date,
        end_date,
        asset_type=asset_type,
        universe=universe,
        profile_name=profile_name,
        extra_factor_names=extra_factor_names,
    )

    if scored.is_empty():
        return profile, scored

    if top_n is not None:
        if top_n <= 0:
            raise ValueError("top_n 必须大于 0")
        scored = scored.filter(pl.col("rank") <= top_n).sort(["time", "composite_score", "symbol"], descending=[False, True, False])

    return profile, scored

"""Shared factor IC computation helpers."""
from __future__ import annotations

from datetime import date, datetime, timedelta

import polars as pl
from sqlalchemy import text


DEFAULT_LAGS = [1, 2, 5, 10, 20]
DEFAULT_WINDOW_DAYS = 126
CALC_VERSION = "v1"


def load_factors(
    engine,
    start: str,
    end: str,
    asset_type: str,
    factor_names: list[str] | None = None,
) -> pl.DataFrame:
    """Load long-format factor rows from factors.daily_factors."""
    where_factor = ""
    params: dict[str, object] = {"start": start, "end": end, "asset_type": asset_type}
    if factor_names:
        placeholders = ", ".join(f":f{i}" for i in range(len(factor_names)))
        where_factor = f"AND factor_name IN ({placeholders})"
        params |= {f"f{i}": factor_name for i, factor_name in enumerate(factor_names)}

    sql = text(
        f"""
        SELECT time, symbol, factor_name, factor_value
        FROM factors.daily_factors
        WHERE time >= :start
          AND time <= :end
          AND asset_type = :asset_type
          {where_factor}
        ORDER BY time, symbol, factor_name
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return pl.DataFrame(
        rows,
        schema=["time", "symbol", "factor_name", "factor_value"],
        orient="row",
    )


def load_returns(engine, start: str, end: str, asset_type: str, max_lag: int) -> pl.DataFrame:
    """Load market daily returns with enough future buffer for the largest lag."""
    end_ext = (datetime.strptime(end, "%Y-%m-%d") + timedelta(days=max_lag * 2 + 5)).strftime("%Y-%m-%d")
    sql = text(
        """
        SELECT time, symbol, pct_change
        FROM market.daily
        WHERE time >= :start
          AND time <= :end
          AND asset_type = :asset_type
        ORDER BY symbol, time
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(
            sql,
            {"start": start, "end": end_ext, "asset_type": asset_type},
        ).fetchall()

    df = pl.DataFrame(rows, schema=["time", "symbol", "pct_change"], orient="row")
    if df.is_empty():
        return df
    return df.with_columns(pl.col("pct_change").cast(pl.Float64))


def _empty_daily_ic_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "factor_name": pl.Utf8,
            "time": pl.Datetime("us", "UTC"),
            "lag": pl.Int16,
            "ic": pl.Float64,
            "rank_ic": pl.Float64,
            "n_stocks": pl.UInt32,
        }
    )


def compute_daily_ic(
    df_factors: pl.DataFrame,
    df_ret: pl.DataFrame,
    lag: int,
    factor_min_cross_section: dict[str, int | None],
) -> pl.DataFrame:
    """Compute daily cross-sectional IC / RankIC for a given forward lag."""
    if df_factors.is_empty() or df_ret.is_empty():
        return _empty_daily_ic_frame()

    df_next = (
        df_ret.sort(["symbol", "time"])
        .with_columns(((pl.col("pct_change") / 100.0) + 1.0).alias("gross_ret"))
        .with_columns([
            pl.col("gross_ret").shift(-offset).over("symbol").alias(f"gross_ret_t{offset}")
            for offset in range(1, lag + 1)
        ])
        .with_columns(
            (
                pl.fold(
                    acc=pl.lit(1.0),
                    function=lambda acc, x: acc * x,
                    exprs=[pl.col(f"gross_ret_t{offset}") for offset in range(1, lag + 1)],
                )
                - 1.0
            ).alias("fwd_ret")
        )
        .drop(["pct_change", "gross_ret"] + [f"gross_ret_t{offset}" for offset in range(1, lag + 1)])
        .drop_nulls("fwd_ret")
    )

    joined = df_factors.join(df_next, on=["time", "symbol"], how="inner")
    joined = joined.filter(pl.col("factor_value").is_finite() & pl.col("fwd_ret").is_finite())
    if joined.is_empty():
        return _empty_daily_ic_frame()

    return (
        joined.group_by(["factor_name", "time"])
        .agg([
            pl.corr("factor_value", "fwd_ret", method="pearson").alias("ic"),
            pl.corr("factor_value", "fwd_ret", method="spearman").alias("rank_ic"),
            pl.len().cast(pl.UInt32).alias("n_stocks"),
        ])
        .sort(["factor_name", "time"])
        .with_columns(
            pl.col("factor_name").replace_strict(
                factor_min_cross_section,
                default=None,
                return_dtype=pl.Int64,
            ).alias("min_cross_section")
        )
        .filter(
            pl.col("min_cross_section").is_null() | (pl.col("n_stocks") >= pl.col("min_cross_section"))
        )
        .with_columns([
            pl.col("ic").fill_nan(None),
            pl.col("rank_ic").fill_nan(None),
            pl.lit(lag).cast(pl.Int16).alias("lag"),
        ])
        .drop("min_cross_section")
        .select(["time", "factor_name", "lag", "ic", "rank_ic", "n_stocks"])
    )


def summarize_daily_ic(daily_ic: pl.DataFrame, lag: int) -> pl.DataFrame:
    """Summarize a daily IC series into aggregate stats."""
    if daily_ic.is_empty():
        return pl.DataFrame(
            schema={
                "lag": pl.Int16,
                "factor_name": pl.Utf8,
                "mean_rank_ic": pl.Float64,
                "rank_ic_std": pl.Float64,
                "rank_ic_ir": pl.Float64,
                "mean_ic": pl.Float64,
                "ic_std": pl.Float64,
                "ic_ir": pl.Float64,
                "t_stat": pl.Float64,
                "win_rate": pl.Float64,
                "n_days": pl.UInt32,
            }
        )

    return (
        daily_ic.group_by("factor_name")
        .agg([
            pl.col("ic").mean().alias("mean_ic"),
            pl.col("ic").std().alias("ic_std"),
            pl.col("rank_ic").mean().alias("mean_rank_ic"),
            pl.col("rank_ic").std().alias("rank_ic_std"),
            (pl.col("ic") > 0).mean().alias("win_rate"),
            pl.col("ic").count().cast(pl.UInt32).alias("n_days"),
        ])
        .with_columns([
            (pl.col("mean_ic") / pl.col("ic_std")).alias("ic_ir"),
            (pl.col("mean_rank_ic") / pl.col("rank_ic_std")).alias("rank_ic_ir"),
        ])
        .with_columns((pl.col("ic_ir") * pl.col("n_days").sqrt()).alias("t_stat"))
        .with_columns(pl.lit(lag).cast(pl.Int16).alias("lag"))
        .select([
            "lag",
            "factor_name",
            "mean_rank_ic",
            "rank_ic_std",
            "rank_ic_ir",
            "mean_ic",
            "ic_std",
            "ic_ir",
            "t_stat",
            "win_rate",
            "n_days",
        ])
        .sort("ic_ir", descending=True)
    )


def load_daily_ic_rows(
    engine,
    start: str,
    end: str,
    asset_type: str,
    lags: list[int] | None = None,
    factor_names: list[str] | None = None,
) -> pl.DataFrame:
    """Load daily IC rows from analytics.factor_daily_ic."""
    lag_filter = ""
    factor_filter = ""
    params: dict[str, object] = {"start": start, "end": end, "asset_type": asset_type}

    if lags:
        placeholders = ", ".join(f":lag_{index}" for index in range(len(lags)))
        lag_filter = f"AND lag IN ({placeholders})"
        params |= {f"lag_{index}": lag for index, lag in enumerate(lags)}

    if factor_names:
        placeholders = ", ".join(f":f_{index}" for index in range(len(factor_names)))
        factor_filter = f"AND factor_name IN ({placeholders})"
        params |= {f"f_{index}": factor_name for index, factor_name in enumerate(factor_names)}

    sql = text(
        f"""
        SELECT time, factor_name, lag, ic, rank_ic, n_stocks
        FROM analytics.factor_daily_ic
        WHERE time >= :start
          AND time <= :end
          AND asset_type = :asset_type
          {lag_filter}
          {factor_filter}
        ORDER BY factor_name, lag, time
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return pl.DataFrame(
        rows,
        schema=["time", "factor_name", "lag", "ic", "rank_ic", "n_stocks"],
        orient="row",
    )


def summarize_ic_window(
    daily_ic: pl.DataFrame,
    *,
    asset_type: str,
    as_of_dates: list[date],
    window_days: int,
) -> pl.DataFrame:
    """Build rolling IC summary rows for the requested as_of dates."""
    schema = {
        "as_of_date": pl.Date,
        "asset_type": pl.Utf8,
        "factor_name": pl.Utf8,
        "lag": pl.Int16,
        "window_days": pl.Int16,
        "mean_ic": pl.Float64,
        "ic_std": pl.Float64,
        "ic_ir": pl.Float64,
        "t_stat": pl.Float64,
        "win_rate": pl.Float64,
        "mean_rank_ic": pl.Float64,
        "rank_ic_std": pl.Float64,
        "rank_ic_ir": pl.Float64,
        "n_days": pl.UInt32,
        "start_date": pl.Date,
        "end_date": pl.Date,
        "calc_version": pl.Utf8,
    }
    if daily_ic.is_empty() or not as_of_dates:
        return pl.DataFrame(schema=schema)

    rows: list[dict[str, object]] = []
    target_dates = sorted(as_of_dates)

    for keys, frame in daily_ic.group_by(["factor_name", "lag"], maintain_order=True):
        factor_name, lag = keys
        ordered = frame.sort("time")
        times = ordered.get_column("time").dt.date().to_list()
        ic_values = ordered.get_column("ic").to_list()
        rank_ic_values = ordered.get_column("rank_ic").to_list()

        for as_of_date in target_dates:
            valid_indices = [idx for idx, current in enumerate(times) if current is not None and current <= as_of_date]
            if not valid_indices:
                continue
            selected_indices = valid_indices[-window_days:]
            selected_ic = [ic_values[idx] for idx in selected_indices if ic_values[idx] is not None]
            selected_rank_ic = [rank_ic_values[idx] for idx in selected_indices if rank_ic_values[idx] is not None]
            if not selected_ic or not selected_rank_ic:
                continue

            ic_series = pl.Series("ic", selected_ic, dtype=pl.Float64)
            rank_ic_series = pl.Series("rank_ic", selected_rank_ic, dtype=pl.Float64)
            n_days = len(selected_ic)
            mean_ic = float(ic_series.mean())
            ic_std = float(ic_series.std()) if n_days > 1 else None
            mean_rank_ic = float(rank_ic_series.mean())
            rank_ic_std = float(rank_ic_series.std()) if len(selected_rank_ic) > 1 else None
            ic_ir = (mean_ic / ic_std) if ic_std not in (None, 0.0) else None
            rank_ic_ir = (mean_rank_ic / rank_ic_std) if rank_ic_std not in (None, 0.0) else None
            t_stat = (ic_ir * (n_days ** 0.5)) if ic_ir is not None else None

            rows.append(
                {
                    "as_of_date": as_of_date,
                    "asset_type": asset_type,
                    "factor_name": factor_name,
                    "lag": lag,
                    "window_days": window_days,
                    "mean_ic": mean_ic,
                    "ic_std": ic_std,
                    "ic_ir": ic_ir,
                    "t_stat": t_stat,
                    "win_rate": float(sum(1 for value in selected_ic if value > 0) / n_days),
                    "mean_rank_ic": mean_rank_ic,
                    "rank_ic_std": rank_ic_std,
                    "rank_ic_ir": rank_ic_ir,
                    "n_days": n_days,
                    "start_date": times[selected_indices[0]],
                    "end_date": times[selected_indices[-1]],
                    "calc_version": CALC_VERSION,
                }
            )

    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(rows).cast(schema)

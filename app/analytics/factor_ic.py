"""Shared factor validation computation helpers."""
from __future__ import annotations

from datetime import date, datetime, timedelta

import polars as pl
from sqlalchemy import text


DEFAULT_LAGS = [1, 2, 5, 10, 20]
DEFAULT_WINDOW_DAYS = 126
DEFAULT_QUANTILE_GROUPS = 10
DEFAULT_TOP_KS = [5, 10, 20]
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


def _empty_daily_quantile_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "factor_name": pl.Utf8,
            "time": pl.Datetime("us", "UTC"),
            "lag": pl.Int16,
            "quantile_n": pl.Int16,
            "quantile_id": pl.Int16,
            "avg_fwd_ret": pl.Float64,
            "n_stocks": pl.UInt32,
        }
    )


def _empty_daily_topk_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "factor_name": pl.Utf8,
            "time": pl.Datetime("us", "UTC"),
            "lag": pl.Int16,
            "top_k": pl.Int16,
            "topk_ret": pl.Float64,
            "universe_ret": pl.Float64,
            "excess_ret": pl.Float64,
            "n_stocks": pl.UInt32,
        }
    )


def _empty_quantile_summary_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "as_of_date": pl.Date,
            "asset_type": pl.Utf8,
            "factor_name": pl.Utf8,
            "lag": pl.Int16,
            "quantile_n": pl.Int16,
            "quantile_id": pl.Int16,
            "window_days": pl.Int16,
            "mean_ret": pl.Float64,
            "ret_std": pl.Float64,
            "ret_ir": pl.Float64,
            "win_rate": pl.Float64,
            "n_days": pl.UInt32,
            "start_date": pl.Date,
            "end_date": pl.Date,
            "calc_version": pl.Utf8,
        }
    )


def _empty_topk_summary_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "as_of_date": pl.Date,
            "asset_type": pl.Utf8,
            "factor_name": pl.Utf8,
            "lag": pl.Int16,
            "top_k": pl.Int16,
            "window_days": pl.Int16,
            "mean_topk_ret": pl.Float64,
            "topk_ret_std": pl.Float64,
            "topk_ret_ir": pl.Float64,
            "topk_win_rate": pl.Float64,
            "mean_excess_ret": pl.Float64,
            "excess_ret_std": pl.Float64,
            "excess_ret_ir": pl.Float64,
            "excess_win_rate": pl.Float64,
            "n_days": pl.UInt32,
            "start_date": pl.Date,
            "end_date": pl.Date,
            "calc_version": pl.Utf8,
        }
    )


def _compute_fwd_returns(df_ret: pl.DataFrame, lag: int) -> pl.DataFrame:
    if df_ret.is_empty():
        return pl.DataFrame(
            schema={
                "time": pl.Datetime("us", "UTC"),
                "symbol": pl.Utf8,
                "fwd_ret": pl.Float64,
            }
        )

    return (
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


def _prepare_joined_factor_returns(
    df_factors: pl.DataFrame,
    df_ret: pl.DataFrame,
    lag: int,
) -> pl.DataFrame:
    if df_factors.is_empty() or df_ret.is_empty():
        return pl.DataFrame(
            schema={
                "time": pl.Datetime("us", "UTC"),
                "symbol": pl.Utf8,
                "factor_name": pl.Utf8,
                "factor_value": pl.Float64,
                "fwd_ret": pl.Float64,
            }
        )

    df_next = _compute_fwd_returns(df_ret, lag)
    joined = df_factors.join(df_next, on=["time", "symbol"], how="inner")
    if joined.is_empty():
        return joined

    return (
        joined.filter(pl.col("factor_value").is_finite() & pl.col("fwd_ret").is_finite())
        .with_columns(pl.len().over(["factor_name", "time"]).cast(pl.UInt32).alias("_cross_n"))
        .sort(["factor_name", "time", "factor_value", "symbol"])
    )


def _factor_required_sample_expr(
    factor_min_cross_section: dict[str, int | None],
    minimum_size: int,
) -> pl.Expr:
    return (
        pl.col("factor_name")
        .replace_strict(
            factor_min_cross_section,
            default=None,
            return_dtype=pl.Int64,
        )
        .fill_null(minimum_size)
        .clip(lower_bound=minimum_size)
        .alias("_required_n")
    )


def compute_daily_ic(
    df_factors: pl.DataFrame,
    df_ret: pl.DataFrame,
    lag: int,
    factor_min_cross_section: dict[str, int | None],
) -> pl.DataFrame:
    """Compute daily cross-sectional IC / RankIC for a given forward lag."""
    joined = _prepare_joined_factor_returns(df_factors, df_ret, lag)
    if joined.is_empty():
        return _empty_daily_ic_frame()

    return (
        joined.group_by(["factor_name", "time"])
        .agg([
            pl.corr("factor_value", "fwd_ret", method="pearson").alias("ic"),
            pl.corr("factor_value", "fwd_ret", method="spearman").alias("rank_ic"),
            pl.first("_cross_n").alias("n_stocks"),
        ])
        .sort(["factor_name", "time"])
        .with_columns(_factor_required_sample_expr(factor_min_cross_section, 1))
        .filter(pl.col("n_stocks") >= pl.col("_required_n"))
        .with_columns([
            pl.col("ic").fill_nan(None),
            pl.col("rank_ic").fill_nan(None),
            pl.lit(lag).cast(pl.Int16).alias("lag"),
        ])
        .drop("_required_n")
        .select(["time", "factor_name", "lag", "ic", "rank_ic", "n_stocks"])
    )


def compute_daily_quantile_return(
    df_factors: pl.DataFrame,
    df_ret: pl.DataFrame,
    lag: int,
    quantile_n: int,
    factor_min_cross_section: dict[str, int | None],
) -> pl.DataFrame:
    """Compute daily equal-weight forward returns for each factor quantile."""
    joined = _prepare_joined_factor_returns(df_factors, df_ret, lag)
    if joined.is_empty():
        return _empty_daily_quantile_frame()

    filtered = (
        joined.with_columns(_factor_required_sample_expr(factor_min_cross_section, quantile_n))
        .filter(pl.col("_cross_n") >= pl.col("_required_n"))
        .with_columns([
            pl.col("factor_value")
            .rank(method="ordinal")
            .over(["factor_name", "time"])
            .cast(pl.Float64)
            .alias("_rank"),
            pl.lit(quantile_n).cast(pl.Int16).alias("quantile_n"),
        ])
        .with_columns(
            (
                (((pl.col("_rank") - 1.0) * quantile_n) / pl.col("_cross_n"))
                .floor()
                .cast(pl.Int16)
                + 1
            ).clip(lower_bound=1, upper_bound=quantile_n).alias("quantile_id")
        )
    )
    if filtered.is_empty():
        return _empty_daily_quantile_frame()

    return (
        filtered.group_by(["factor_name", "time", "quantile_n", "quantile_id"])
        .agg([
            pl.col("fwd_ret").mean().alias("avg_fwd_ret"),
            pl.len().cast(pl.UInt32).alias("n_stocks"),
        ])
        .sort(["factor_name", "time", "quantile_id"])
        .with_columns(pl.lit(lag).cast(pl.Int16).alias("lag"))
        .select(["time", "factor_name", "lag", "quantile_n", "quantile_id", "avg_fwd_ret", "n_stocks"])
    )


def compute_daily_topk_return(
    df_factors: pl.DataFrame,
    df_ret: pl.DataFrame,
    lag: int,
    top_ks: list[int],
    factor_min_cross_section: dict[str, int | None],
) -> pl.DataFrame:
    """Compute daily equal-weight forward returns for Top-K baskets."""
    if not top_ks:
        return _empty_daily_topk_frame()

    joined = _prepare_joined_factor_returns(df_factors, df_ret, lag)
    if joined.is_empty():
        return _empty_daily_topk_frame()

    max_top_k = max(top_ks)
    filtered = (
        joined.with_columns(_factor_required_sample_expr(factor_min_cross_section, max_top_k))
        .filter(pl.col("_cross_n") >= pl.col("_required_n"))
        .sort(["factor_name", "time", "factor_value", "symbol"], descending=[False, False, True, False])
        .with_columns(
            pl.col("symbol")
            .cum_count()
            .over(["factor_name", "time"])
            .cast(pl.Int16)
            .alias("_rank")
        )
    )
    if filtered.is_empty():
        return _empty_daily_topk_frame()

    universe = (
        filtered.group_by(["factor_name", "time"])
        .agg(pl.col("fwd_ret").mean().alias("universe_ret"))
    )

    frames: list[pl.DataFrame] = []
    for top_k in sorted(set(top_ks)):
        current = (
            filtered.filter(pl.col("_rank") <= top_k)
            .group_by(["factor_name", "time"])
            .agg([
                pl.col("fwd_ret").mean().alias("topk_ret"),
                pl.len().cast(pl.UInt32).alias("n_stocks"),
            ])
            .join(universe, on=["factor_name", "time"], how="left")
            .with_columns([
                (pl.col("topk_ret") - pl.col("universe_ret")).alias("excess_ret"),
                pl.lit(lag).cast(pl.Int16).alias("lag"),
                pl.lit(top_k).cast(pl.Int16).alias("top_k"),
            ])
            .select(["time", "factor_name", "lag", "top_k", "topk_ret", "universe_ret", "excess_ret", "n_stocks"])
        )
        if not current.is_empty():
            frames.append(current)

    return pl.concat(frames) if frames else _empty_daily_topk_frame()


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


def load_daily_quantile_rows(
    engine,
    start: str,
    end: str,
    asset_type: str,
    lags: list[int] | None = None,
    factor_names: list[str] | None = None,
    quantile_n: int | None = None,
) -> pl.DataFrame:
    lag_filter = ""
    factor_filter = ""
    quantile_filter = ""
    params: dict[str, object] = {"start": start, "end": end, "asset_type": asset_type}

    if lags:
        placeholders = ", ".join(f":lag_{index}" for index in range(len(lags)))
        lag_filter = f"AND lag IN ({placeholders})"
        params |= {f"lag_{index}": lag for index, lag in enumerate(lags)}

    if factor_names:
        placeholders = ", ".join(f":f_{index}" for index in range(len(factor_names)))
        factor_filter = f"AND factor_name IN ({placeholders})"
        params |= {f"f_{index}": factor_name for index, factor_name in enumerate(factor_names)}

    if quantile_n is not None:
        quantile_filter = "AND quantile_n = :quantile_n"
        params["quantile_n"] = quantile_n

    sql = text(
        f"""
        SELECT time, factor_name, lag, quantile_n, quantile_id, avg_fwd_ret, n_stocks
        FROM analytics.factor_daily_quantile_return
        WHERE time >= :start
          AND time <= :end
          AND asset_type = :asset_type
          {lag_filter}
          {factor_filter}
          {quantile_filter}
        ORDER BY factor_name, lag, quantile_n, quantile_id, time
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return pl.DataFrame(
        rows,
        schema=["time", "factor_name", "lag", "quantile_n", "quantile_id", "avg_fwd_ret", "n_stocks"],
        orient="row",
    )


def load_daily_topk_rows(
    engine,
    start: str,
    end: str,
    asset_type: str,
    lags: list[int] | None = None,
    factor_names: list[str] | None = None,
    top_ks: list[int] | None = None,
) -> pl.DataFrame:
    lag_filter = ""
    factor_filter = ""
    topk_filter = ""
    params: dict[str, object] = {"start": start, "end": end, "asset_type": asset_type}

    if lags:
        placeholders = ", ".join(f":lag_{index}" for index in range(len(lags)))
        lag_filter = f"AND lag IN ({placeholders})"
        params |= {f"lag_{index}": lag for index, lag in enumerate(lags)}

    if factor_names:
        placeholders = ", ".join(f":f_{index}" for index in range(len(factor_names)))
        factor_filter = f"AND factor_name IN ({placeholders})"
        params |= {f"f_{index}": factor_name for index, factor_name in enumerate(factor_names)}

    if top_ks:
        placeholders = ", ".join(f":topk_{index}" for index in range(len(top_ks)))
        topk_filter = f"AND top_k IN ({placeholders})"
        params |= {f"topk_{index}": top_k for index, top_k in enumerate(top_ks)}

    sql = text(
        f"""
        SELECT time, factor_name, lag, top_k, topk_ret, universe_ret, excess_ret, n_stocks
        FROM analytics.factor_daily_topk_return
        WHERE time >= :start
          AND time <= :end
          AND asset_type = :asset_type
          {lag_filter}
          {factor_filter}
          {topk_filter}
        ORDER BY factor_name, lag, top_k, time
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return pl.DataFrame(
        rows,
        schema=["time", "factor_name", "lag", "top_k", "topk_ret", "universe_ret", "excess_ret", "n_stocks"],
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


def summarize_quantile_window(
    daily_quantile: pl.DataFrame,
    *,
    asset_type: str,
    as_of_dates: list[date],
    window_days: int,
) -> pl.DataFrame:
    if daily_quantile.is_empty() or not as_of_dates:
        return _empty_quantile_summary_frame()

    rows: list[dict[str, object]] = []
    target_dates = sorted(as_of_dates)

    for keys, frame in daily_quantile.group_by(["factor_name", "lag", "quantile_n", "quantile_id"], maintain_order=True):
        factor_name, lag, quantile_n, quantile_id = keys
        ordered = frame.sort("time")
        times = ordered.get_column("time").dt.date().to_list()
        ret_values = ordered.get_column("avg_fwd_ret").to_list()

        for as_of_date in target_dates:
            valid_indices = [idx for idx, current in enumerate(times) if current is not None and current <= as_of_date]
            if not valid_indices:
                continue
            selected_indices = valid_indices[-window_days:]
            selected_ret = [ret_values[idx] for idx in selected_indices if ret_values[idx] is not None]
            if not selected_ret:
                continue

            ret_series = pl.Series("ret", selected_ret, dtype=pl.Float64)
            n_days = len(selected_ret)
            mean_ret = float(ret_series.mean())
            ret_std = float(ret_series.std()) if n_days > 1 else None
            ret_ir = (mean_ret / ret_std) if ret_std not in (None, 0.0) else None

            rows.append(
                {
                    "as_of_date": as_of_date,
                    "asset_type": asset_type,
                    "factor_name": factor_name,
                    "lag": lag,
                    "quantile_n": quantile_n,
                    "quantile_id": quantile_id,
                    "window_days": window_days,
                    "mean_ret": mean_ret,
                    "ret_std": ret_std,
                    "ret_ir": ret_ir,
                    "win_rate": float(sum(1 for value in selected_ret if value > 0) / n_days),
                    "n_days": n_days,
                    "start_date": times[selected_indices[0]],
                    "end_date": times[selected_indices[-1]],
                    "calc_version": CALC_VERSION,
                }
            )

    return pl.DataFrame(rows).cast(_empty_quantile_summary_frame().schema) if rows else _empty_quantile_summary_frame()


def summarize_topk_window(
    daily_topk: pl.DataFrame,
    *,
    asset_type: str,
    as_of_dates: list[date],
    window_days: int,
) -> pl.DataFrame:
    if daily_topk.is_empty() or not as_of_dates:
        return _empty_topk_summary_frame()

    rows: list[dict[str, object]] = []
    target_dates = sorted(as_of_dates)

    for keys, frame in daily_topk.group_by(["factor_name", "lag", "top_k"], maintain_order=True):
        factor_name, lag, top_k = keys
        ordered = frame.sort("time")
        times = ordered.get_column("time").dt.date().to_list()
        topk_values = ordered.get_column("topk_ret").to_list()
        excess_values = ordered.get_column("excess_ret").to_list()

        for as_of_date in target_dates:
            valid_indices = [idx for idx, current in enumerate(times) if current is not None and current <= as_of_date]
            if not valid_indices:
                continue
            selected_indices = valid_indices[-window_days:]
            selected_topk = [topk_values[idx] for idx in selected_indices if topk_values[idx] is not None]
            selected_excess = [excess_values[idx] for idx in selected_indices if excess_values[idx] is not None]
            if not selected_topk or not selected_excess:
                continue

            topk_series = pl.Series("topk_ret", selected_topk, dtype=pl.Float64)
            excess_series = pl.Series("excess_ret", selected_excess, dtype=pl.Float64)
            n_days = len(selected_topk)
            mean_topk_ret = float(topk_series.mean())
            topk_ret_std = float(topk_series.std()) if n_days > 1 else None
            topk_ret_ir = (mean_topk_ret / topk_ret_std) if topk_ret_std not in (None, 0.0) else None
            mean_excess_ret = float(excess_series.mean())
            excess_ret_std = float(excess_series.std()) if len(selected_excess) > 1 else None
            excess_ret_ir = (mean_excess_ret / excess_ret_std) if excess_ret_std not in (None, 0.0) else None

            rows.append(
                {
                    "as_of_date": as_of_date,
                    "asset_type": asset_type,
                    "factor_name": factor_name,
                    "lag": lag,
                    "top_k": top_k,
                    "window_days": window_days,
                    "mean_topk_ret": mean_topk_ret,
                    "topk_ret_std": topk_ret_std,
                    "topk_ret_ir": topk_ret_ir,
                    "topk_win_rate": float(sum(1 for value in selected_topk if value > 0) / n_days),
                    "mean_excess_ret": mean_excess_ret,
                    "excess_ret_std": excess_ret_std,
                    "excess_ret_ir": excess_ret_ir,
                    "excess_win_rate": float(sum(1 for value in selected_excess if value > 0) / len(selected_excess)),
                    "n_days": n_days,
                    "start_date": times[selected_indices[0]],
                    "end_date": times[selected_indices[-1]],
                    "calc_version": CALC_VERSION,
                }
            )

    return pl.DataFrame(rows).cast(_empty_topk_summary_frame().schema) if rows else _empty_topk_summary_frame()

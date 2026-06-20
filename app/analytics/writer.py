"""Persistence helpers for factor validation analytics tables."""
from __future__ import annotations

import polars as pl
from sqlalchemy import text


FACTOR_DAILY_IC_DATA_TYPE = "factor_daily_ic"
FACTOR_IC_SUMMARY_DATA_TYPE = "factor_ic_summary"
FACTOR_DAILY_QUANTILE_RETURN_DATA_TYPE = "factor_daily_quantile_return"
FACTOR_QUANTILE_SUMMARY_DATA_TYPE = "factor_quantile_summary"
FACTOR_DAILY_TOPK_RETURN_DATA_TYPE = "factor_daily_topk_return"
FACTOR_TOPK_SUMMARY_DATA_TYPE = "factor_topk_summary"


def upsert_factor_daily_ic(engine, df: pl.DataFrame) -> int:
    if df.is_empty():
        return 0

    rows = df.to_dicts()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO analytics.factor_daily_ic
                    (time, asset_type, factor_name, lag, ic, rank_ic, n_stocks, calc_version, created_at, updated_at)
                VALUES
                    (:time, :asset_type, :factor_name, :lag, :ic, :rank_ic, :n_stocks, :calc_version, NOW(), NOW())
                ON CONFLICT (time, asset_type, factor_name, lag) DO UPDATE SET
                    ic = EXCLUDED.ic,
                    rank_ic = EXCLUDED.rank_ic,
                    n_stocks = EXCLUDED.n_stocks,
                    calc_version = EXCLUDED.calc_version,
                    updated_at = NOW()
                """
            ),
            rows,
        )
    return len(rows)


def upsert_factor_ic_summary(engine, df: pl.DataFrame) -> int:
    if df.is_empty():
        return 0

    rows = df.to_dicts()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO analytics.factor_ic_summary
                    (
                        as_of_date, asset_type, factor_name, lag, window_days,
                        mean_ic, ic_std, ic_ir, t_stat, win_rate,
                        mean_rank_ic, rank_ic_std, rank_ic_ir, n_days,
                        start_date, end_date, calc_version, created_at, updated_at
                    )
                VALUES
                    (
                        :as_of_date, :asset_type, :factor_name, :lag, :window_days,
                        :mean_ic, :ic_std, :ic_ir, :t_stat, :win_rate,
                        :mean_rank_ic, :rank_ic_std, :rank_ic_ir, :n_days,
                        :start_date, :end_date, :calc_version, NOW(), NOW()
                    )
                ON CONFLICT (as_of_date, asset_type, factor_name, lag, window_days) DO UPDATE SET
                    mean_ic = EXCLUDED.mean_ic,
                    ic_std = EXCLUDED.ic_std,
                    ic_ir = EXCLUDED.ic_ir,
                    t_stat = EXCLUDED.t_stat,
                    win_rate = EXCLUDED.win_rate,
                    mean_rank_ic = EXCLUDED.mean_rank_ic,
                    rank_ic_std = EXCLUDED.rank_ic_std,
                    rank_ic_ir = EXCLUDED.rank_ic_ir,
                    n_days = EXCLUDED.n_days,
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    calc_version = EXCLUDED.calc_version,
                    updated_at = NOW()
                """
            ),
            rows,
        )
    return len(rows)


def upsert_factor_daily_quantile_return(engine, df: pl.DataFrame) -> int:
    if df.is_empty():
        return 0

    rows = df.to_dicts()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO analytics.factor_daily_quantile_return
                    (
                        time, asset_type, factor_name, lag, quantile_n, quantile_id,
                        avg_fwd_ret, n_stocks, calc_version, created_at, updated_at
                    )
                VALUES
                    (
                        :time, :asset_type, :factor_name, :lag, :quantile_n, :quantile_id,
                        :avg_fwd_ret, :n_stocks, :calc_version, NOW(), NOW()
                    )
                ON CONFLICT (time, asset_type, factor_name, lag, quantile_n, quantile_id) DO UPDATE SET
                    avg_fwd_ret = EXCLUDED.avg_fwd_ret,
                    n_stocks = EXCLUDED.n_stocks,
                    calc_version = EXCLUDED.calc_version,
                    updated_at = NOW()
                """
            ),
            rows,
        )
    return len(rows)


def upsert_factor_quantile_summary(engine, df: pl.DataFrame) -> int:
    if df.is_empty():
        return 0

    rows = df.to_dicts()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO analytics.factor_quantile_summary
                    (
                        as_of_date, asset_type, factor_name, lag, quantile_n, quantile_id, window_days,
                        mean_ret, ret_std, ret_ir, win_rate, n_days,
                        start_date, end_date, calc_version, created_at, updated_at
                    )
                VALUES
                    (
                        :as_of_date, :asset_type, :factor_name, :lag, :quantile_n, :quantile_id, :window_days,
                        :mean_ret, :ret_std, :ret_ir, :win_rate, :n_days,
                        :start_date, :end_date, :calc_version, NOW(), NOW()
                    )
                ON CONFLICT (as_of_date, asset_type, factor_name, lag, quantile_n, quantile_id, window_days) DO UPDATE SET
                    mean_ret = EXCLUDED.mean_ret,
                    ret_std = EXCLUDED.ret_std,
                    ret_ir = EXCLUDED.ret_ir,
                    win_rate = EXCLUDED.win_rate,
                    n_days = EXCLUDED.n_days,
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    calc_version = EXCLUDED.calc_version,
                    updated_at = NOW()
                """
            ),
            rows,
        )
    return len(rows)


def upsert_factor_daily_topk_return(engine, df: pl.DataFrame) -> int:
    if df.is_empty():
        return 0

    rows = df.to_dicts()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO analytics.factor_daily_topk_return
                    (
                        time, asset_type, factor_name, lag, top_k,
                        topk_ret, universe_ret, excess_ret, n_stocks, calc_version, created_at, updated_at
                    )
                VALUES
                    (
                        :time, :asset_type, :factor_name, :lag, :top_k,
                        :topk_ret, :universe_ret, :excess_ret, :n_stocks, :calc_version, NOW(), NOW()
                    )
                ON CONFLICT (time, asset_type, factor_name, lag, top_k) DO UPDATE SET
                    topk_ret = EXCLUDED.topk_ret,
                    universe_ret = EXCLUDED.universe_ret,
                    excess_ret = EXCLUDED.excess_ret,
                    n_stocks = EXCLUDED.n_stocks,
                    calc_version = EXCLUDED.calc_version,
                    updated_at = NOW()
                """
            ),
            rows,
        )
    return len(rows)


def upsert_factor_topk_summary(engine, df: pl.DataFrame) -> int:
    if df.is_empty():
        return 0

    rows = df.to_dicts()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO analytics.factor_topk_summary
                    (
                        as_of_date, asset_type, factor_name, lag, top_k, window_days,
                        mean_topk_ret, topk_ret_std, topk_ret_ir, topk_win_rate,
                        mean_excess_ret, excess_ret_std, excess_ret_ir, excess_win_rate,
                        n_days, start_date, end_date, calc_version, created_at, updated_at
                    )
                VALUES
                    (
                        :as_of_date, :asset_type, :factor_name, :lag, :top_k, :window_days,
                        :mean_topk_ret, :topk_ret_std, :topk_ret_ir, :topk_win_rate,
                        :mean_excess_ret, :excess_ret_std, :excess_ret_ir, :excess_win_rate,
                        :n_days, :start_date, :end_date, :calc_version, NOW(), NOW()
                    )
                ON CONFLICT (as_of_date, asset_type, factor_name, lag, top_k, window_days) DO UPDATE SET
                    mean_topk_ret = EXCLUDED.mean_topk_ret,
                    topk_ret_std = EXCLUDED.topk_ret_std,
                    topk_ret_ir = EXCLUDED.topk_ret_ir,
                    topk_win_rate = EXCLUDED.topk_win_rate,
                    mean_excess_ret = EXCLUDED.mean_excess_ret,
                    excess_ret_std = EXCLUDED.excess_ret_std,
                    excess_ret_ir = EXCLUDED.excess_ret_ir,
                    excess_win_rate = EXCLUDED.excess_win_rate,
                    n_days = EXCLUDED.n_days,
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    calc_version = EXCLUDED.calc_version,
                    updated_at = NOW()
                """
            ),
            rows,
        )
    return len(rows)


def get_complete_validation_dates(
    engine,
    *,
    table_name: str,
    date_column: str,
    asset_type: str,
    factor_names: list[str],
    lags: list[int],
    expected: int,
    start: str,
    end: str,
    extra_filters: str = "",
    extra_params: dict[str, object] | None = None,
) -> list[str]:
    if expected <= 0:
        return []

    factor_placeholders = ", ".join(f":f_{index}" for index in range(len(factor_names)))
    lag_placeholders = ", ".join(f":lag_{index}" for index in range(len(lags)))
    params: dict[str, object] = {
        "asset_type": asset_type,
        "start": start,
        "end": end,
        "expected": expected,
        **{f"f_{index}": factor_name for index, factor_name in enumerate(factor_names)},
        **{f"lag_{index}": lag for index, lag in enumerate(lags)},
    }
    if extra_params:
        params |= extra_params

    sql = text(
        f"""
        SELECT TO_CHAR({date_column} AT TIME ZONE 'UTC', 'YYYYMMDD') AS trade_date
        FROM {table_name}
        WHERE asset_type = :asset_type
          AND {date_column} >= :start
          AND {date_column} <= :end
          AND factor_name IN ({factor_placeholders})
          AND lag IN ({lag_placeholders})
          {extra_filters}
        GROUP BY {date_column}
        HAVING COUNT(*) = :expected
        ORDER BY trade_date
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [row[0] for row in rows]


def get_complete_ic_dates(
    engine,
    *,
    asset_type: str,
    factor_names: list[str],
    lags: list[int],
    start: str,
    end: str,
) -> list[str]:
    return get_complete_validation_dates(
        engine,
        table_name="analytics.factor_daily_ic",
        date_column="time",
        asset_type=asset_type,
        factor_names=factor_names,
        lags=lags,
        expected=len(factor_names) * len(lags),
        start=start,
        end=end,
    )


def get_complete_quantile_dates(
    engine,
    *,
    asset_type: str,
    factor_names: list[str],
    lags: list[int],
    quantile_n: int,
    start: str,
    end: str,
) -> list[str]:
    return get_complete_validation_dates(
        engine,
        table_name="analytics.factor_daily_quantile_return",
        date_column="time",
        asset_type=asset_type,
        factor_names=factor_names,
        lags=lags,
        expected=len(factor_names) * len(lags) * quantile_n,
        start=start,
        end=end,
        extra_filters="AND quantile_n = :quantile_n",
        extra_params={"quantile_n": quantile_n},
    )


def get_complete_topk_dates(
    engine,
    *,
    asset_type: str,
    factor_names: list[str],
    lags: list[int],
    top_ks: list[int],
    start: str,
    end: str,
) -> list[str]:
    topk_placeholders = ", ".join(f":topk_{index}" for index in range(len(top_ks)))
    return get_complete_validation_dates(
        engine,
        table_name="analytics.factor_daily_topk_return",
        date_column="time",
        asset_type=asset_type,
        factor_names=factor_names,
        lags=lags,
        expected=len(factor_names) * len(lags) * len(top_ks),
        start=start,
        end=end,
        extra_filters=f"AND top_k IN ({topk_placeholders})",
        extra_params={f"topk_{index}": top_k for index, top_k in enumerate(top_ks)},
    )


def update_ic_sync_status(
    engine,
    *,
    data_type: str,
    asset_type: str,
    status: str,
    last_date: str | None,
    error_msg: str | None = None,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO meta.sync_status
                    (data_type, asset_type, symbol, data_source, last_sync_time, last_date, status, error_msg, updated_at)
                VALUES
                    (:data_type, :asset_type, '', 'analytics', NOW(), :last_date, :status, :error_msg, NOW())
                ON CONFLICT (data_type, asset_type, symbol, data_source) DO UPDATE SET
                    last_sync_time = NOW(),
                    last_date = EXCLUDED.last_date,
                    status = EXCLUDED.status,
                    error_msg = EXCLUDED.error_msg,
                    updated_at = NOW()
                """
            ),
            {
                "data_type": data_type,
                "asset_type": asset_type,
                "last_date": last_date,
                "status": status,
                "error_msg": error_msg,
            },
        )

"""Persistence helpers for factor IC analytics tables."""
from __future__ import annotations

import polars as pl
from sqlalchemy import text


FACTOR_DAILY_IC_DATA_TYPE = "factor_daily_ic"
FACTOR_IC_SUMMARY_DATA_TYPE = "factor_ic_summary"


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


def get_complete_ic_dates(
    engine,
    *,
    asset_type: str,
    factor_names: list[str],
    lags: list[int],
    start: str,
    end: str,
) -> list[str]:
    expected = len(factor_names) * len(lags)
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

    sql = text(
        f"""
        SELECT TO_CHAR(time AT TIME ZONE 'UTC', 'YYYYMMDD') AS trade_date
        FROM analytics.factor_daily_ic
        WHERE asset_type = :asset_type
          AND time >= :start
          AND time <= :end
          AND factor_name IN ({factor_placeholders})
          AND lag IN ({lag_placeholders})
        GROUP BY time
        HAVING COUNT(*) = :expected
        ORDER BY trade_date
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [row[0] for row in rows]


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

"""
因子流水线写库与状态更新。
"""
from __future__ import annotations

import polars as pl
from sqlalchemy import text


DATA_TYPE = "daily_factors"


def upsert_factors(engine, df: pl.DataFrame) -> int:
    """将长格式因子数据写入 factors.daily_factors。"""
    if df.is_empty():
        return 0

    rows = df.to_dicts()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO factors.daily_factors (time, symbol, factor_name, factor_value)
            VALUES (:time, :symbol, :factor_name, :factor_value)
            ON CONFLICT (time, symbol, factor_name) DO UPDATE SET
                factor_value = EXCLUDED.factor_value
        """), rows)
    return len(rows)


def update_sync_status(engine, status: str, last_date: str | None,
                       error_msg: str | None = None) -> None:
    """更新 daily_factors 同步状态。"""
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO meta.sync_status
                (data_type, symbol, last_sync_time, last_date, status, error_msg, updated_at)
            VALUES
                (:data_type, NULL, NOW(), :last_date, :status, :error_msg, NOW())
            ON CONFLICT (data_type, symbol) DO UPDATE SET
                last_sync_time = NOW(),
                last_date      = EXCLUDED.last_date,
                status         = EXCLUDED.status,
                error_msg      = EXCLUDED.error_msg,
                updated_at     = NOW()
        """), {
            "data_type": DATA_TYPE,
            "last_date": last_date,
            "status": status,
            "error_msg": error_msg,
        })

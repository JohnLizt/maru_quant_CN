"""
因子流水线写库与状态更新。
"""
from __future__ import annotations

import csv
import io

import polars as pl
from sqlalchemy import text


DATA_TYPE = "daily_factors"


def upsert_factors(engine, df: pl.DataFrame, *, asset_type: str | None = None) -> int:
    """将长格式因子数据写入 factors.daily_factors。"""
    if df.is_empty():
        return 0

    if "asset_type" not in df.columns:
        if not asset_type:
            asset_type = "stock_CN"
        df = df.with_columns(pl.lit(asset_type).alias("asset_type"))

    ordered_columns = ["time", "asset_type", "symbol", "factor_name", "factor_value"]
    rows = df.select(ordered_columns).to_dicts()
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0"))
        conn.execute(
            text(
                """
                CREATE TEMP TABLE _daily_factors_upsert (
                    time TIMESTAMPTZ NOT NULL,
                    asset_type VARCHAR(32) NOT NULL,
                    symbol VARCHAR(32) NOT NULL,
                    factor_name VARCHAR(64) NOT NULL,
                    factor_value DOUBLE PRECISION
                ) ON COMMIT DROP
                """
            )
        )

        buffer = io.StringIO()
        writer = csv.writer(buffer, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        for row in rows:
            writer.writerow(["\\N" if row[column] is None else row[column] for column in ordered_columns])
        buffer.seek(0)
        with conn.connection.driver_connection.cursor() as cursor:
            cursor.copy_expert(
                """
                COPY _daily_factors_upsert (time, asset_type, symbol, factor_name, factor_value)
                FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')
                """,
                buffer,
            )

        result = conn.execute(
            text(
                """
                INSERT INTO factors.daily_factors (time, asset_type, symbol, factor_name, factor_value)
                SELECT time, asset_type, symbol, factor_name, factor_value
                FROM _daily_factors_upsert
                ON CONFLICT (time, asset_type, symbol, factor_name) DO UPDATE SET
                    factor_value = EXCLUDED.factor_value
                """
            )
        )
        if result.rowcount != len(rows):
            raise RuntimeError(f"因子更新行数不一致: expected={len(rows)}, actual={result.rowcount}")
    return len(rows)


def update_sync_status(
    engine,
    asset_type: str,
    status: str,
    last_date: str | None,
    error_msg: str | None = None,
) -> None:
    """更新 daily_factors 同步状态。"""
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO meta.sync_status
                    (data_type, asset_type, symbol, data_source, last_sync_time, last_date, status, error_msg, updated_at)
                VALUES
                    (:data_type, :asset_type, '', '', NOW(), :last_date, :status, :error_msg, NOW())
                ON CONFLICT (data_type, asset_type, symbol, data_source) DO UPDATE SET
                    last_sync_time = NOW(),
                    last_date      = EXCLUDED.last_date,
                    status         = EXCLUDED.status,
                    error_msg      = EXCLUDED.error_msg,
                    updated_at     = NOW()
                """
            ),
            {
                "data_type": DATA_TYPE,
                "asset_type": asset_type,
                "last_date": last_date,
                "status": status,
                "error_msg": error_msg,
            },
        )

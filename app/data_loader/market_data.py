"""High-level market data loader helpers and shared persistence."""
from __future__ import annotations

import csv
import io

import polars as pl
from sqlalchemy import text

from app.data_loader.registry import get_market_data_loader as _get_loader
from app.services.asset_universe import get_asset_type_config
from app.utils.db import get_engine


def get_market_data_loader(asset_type: str):
    config = get_asset_type_config(asset_type)
    loader = _get_loader(config.loader_key)
    if not loader.supports(config.asset_type):
        raise ValueError(f"loader={config.loader_key} 不支持 asset_type={config.asset_type}")
    return loader


def fetch_daily_by_symbol(asset_type: str, symbol: str, start: str, end: str) -> pl.DataFrame:
    loader = get_market_data_loader(asset_type)
    return loader.fetch_daily_by_symbol(asset_type, symbol, start, end)


def upsert_daily(df: pl.DataFrame, *, asset_type: str | None = None) -> int:
    """将日线数据写入 market.daily（冲突时更新）"""
    if df.is_empty():
        return 0

    if "asset_type" not in df.columns:
        if not asset_type:
            asset_type = "stock_CN"
        df = df.with_columns(pl.lit(asset_type).alias("asset_type"))

    if "data_source" not in df.columns:
        source_name = get_asset_type_config(asset_type or "stock_CN").data_source
        df = df.with_columns(pl.lit(source_name).alias("data_source"))

    if "is_suspended" not in df.columns:
        df = df.with_columns(pl.lit(False).alias("is_suspended"))

    if "adj_factor" not in df.columns:
        df = df.with_columns(pl.lit(1.0).alias("adj_factor"))

    rows = df.to_dicts()
    sql = text(
        """
        INSERT INTO market.daily
            (time, asset_type, symbol, open, high, low, close, volume, amount, adj_factor, pct_change, is_suspended, data_source)
        VALUES
            (:time, :asset_type, :symbol, :open, :high, :low, :close, :volume, :amount, :adj_factor, :pct_change, :is_suspended, :data_source)
        ON CONFLICT (time, asset_type, symbol) DO UPDATE SET
            open         = EXCLUDED.open,
            high         = EXCLUDED.high,
            low          = EXCLUDED.low,
            close        = EXCLUDED.close,
            volume       = EXCLUDED.volume,
            amount       = EXCLUDED.amount,
            adj_factor   = EXCLUDED.adj_factor,
            pct_change   = EXCLUDED.pct_change,
            is_suspended = EXCLUDED.is_suspended,
            data_source  = EXCLUDED.data_source
        """
    )

    with get_engine().begin() as conn:
        conn.execute(sql, rows)

    return len(rows)


def update_daily_adjustments(df: pl.DataFrame, *, engine=None) -> int:
    """Update only adjustment-related columns on existing market.daily rows."""
    if df.is_empty():
        return 0

    required_columns = {"time", "asset_type", "symbol", "adj_factor", "pct_change"}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"复权数据缺少列: {sorted(missing_columns)}")

    ordered_columns = ["time", "asset_type", "symbol", "adj_factor", "pct_change"]
    rows = df.select(ordered_columns).to_dicts()
    update_sql = text(
        """
        UPDATE market.daily AS daily
        SET adj_factor = adjustments.adj_factor,
            pct_change = adjustments.pct_change
        FROM _daily_adjustments AS adjustments
        WHERE daily.time = adjustments.time
          AND daily.asset_type = adjustments.asset_type
          AND daily.symbol = adjustments.symbol
        """
    )
    resolved_engine = engine or get_engine()
    with resolved_engine.begin() as conn:
        conn.execute(text("SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0"))
        conn.execute(
            text(
                """
                CREATE TEMP TABLE _daily_adjustments (
                    time TIMESTAMPTZ NOT NULL,
                    asset_type VARCHAR(32) NOT NULL,
                    symbol VARCHAR(32) NOT NULL,
                    adj_factor DOUBLE PRECISION NOT NULL,
                    pct_change DOUBLE PRECISION
                ) ON COMMIT DROP
                """
            )
        )
        buffer = io.StringIO()
        writer = csv.writer(buffer, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        for row in rows:
            writer.writerow(["\\N" if row[column] is None else row[column] for column in ordered_columns])
        buffer.seek(0)

        driver_connection = conn.connection.driver_connection
        with driver_connection.cursor() as cursor:
            cursor.copy_expert(
                """
                COPY _daily_adjustments (time, asset_type, symbol, adj_factor, pct_change)
                FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')
                """,
                buffer,
            )

        result = conn.execute(update_sql)
        rowcount = getattr(result, "rowcount", -1)
        if rowcount != len(rows):
            raise RuntimeError(f"复权更新行数不一致: expected={len(rows)}, actual={rowcount}")
    return len(rows)

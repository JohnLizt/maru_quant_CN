"""High-level market data loader helpers and shared persistence."""
from __future__ import annotations

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

    rows = df.to_dicts()
    sql = text(
        """
        INSERT INTO market.daily
            (time, asset_type, symbol, open, high, low, close, volume, amount, pct_change, is_suspended, data_source)
        VALUES
            (:time, :asset_type, :symbol, :open, :high, :low, :close, :volume, :amount, :pct_change, :is_suspended, :data_source)
        ON CONFLICT (time, asset_type, symbol) DO UPDATE SET
            open         = EXCLUDED.open,
            high         = EXCLUDED.high,
            low          = EXCLUDED.low,
            close        = EXCLUDED.close,
            volume       = EXCLUDED.volume,
            amount       = EXCLUDED.amount,
            pct_change   = EXCLUDED.pct_change,
            is_suspended = EXCLUDED.is_suspended,
            data_source  = EXCLUDED.data_source
        """
    )

    with get_engine().begin() as conn:
        conn.execute(sql, rows)

    return len(rows)

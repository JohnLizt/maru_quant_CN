"""
日线行情数据拉取
使用 Tushare Pro 获取数据，Polars 处理，写入 TimescaleDB
"""
from __future__ import annotations

import os

import polars as pl
import tushare as ts
from loguru import logger
from sqlalchemy import text

from app.utils.db import get_engine


def _get_pro():
    token = os.environ.get("TUSHARE_TOKEN", "")
    ts.set_token(token)
    return ts.pro_api()


def fetch_stock_daily(symbol: str, start: str, end: str) -> pl.DataFrame:
    """
    拉取单只股票日线数据

    Args:
        symbol: 股票代码，如 "000001.SZ"
        start:  开始日期 "YYYYMMDD"
        end:    结束日期 "YYYYMMDD"

    Returns:
        Polars DataFrame
    """
    logger.info(f"拉取 {symbol} 日线数据: {start} ~ {end}")
    pro = _get_pro()
    df_pd = pro.daily(ts_code=symbol, start_date=start, end_date=end)

    df = pl.from_pandas(df_pd).rename({
        "ts_code": "symbol",
        "trade_date": "time",
        "vol": "volume",
        "pct_chg": "pct_change",
    }).with_columns([
        pl.col("time").str.strptime(pl.Date, "%Y%m%d").cast(pl.Datetime("us", "UTC")),
    ]).select(["time", "symbol", "open", "high", "low", "close",
               "volume", "amount", "pct_change"])

    logger.success(f"获取 {len(df)} 条记录")
    return df


def upsert_daily(df: pl.DataFrame) -> int:
    """将日线数据写入 market.daily（冲突时更新）"""
    if df.is_empty():
        return 0

    engine = get_engine()
    rows = df.to_dicts()

    sql = text("""
        INSERT INTO market.daily
            (time, symbol, open, high, low, close, volume, amount, pct_change)
        VALUES
            (:time, :symbol, :open, :high, :low, :close, :volume, :amount, :pct_change)
        ON CONFLICT (time, symbol) DO UPDATE SET
            open       = EXCLUDED.open,
            high       = EXCLUDED.high,
            low        = EXCLUDED.low,
            close      = EXCLUDED.close,
            volume     = EXCLUDED.volume,
            amount     = EXCLUDED.amount,
            pct_change = EXCLUDED.pct_change
    """)

    with engine.begin() as conn:
        conn.execute(sql, rows)

    logger.success(f"写入/更新 {len(rows)} 条日线记录")
    return len(rows)

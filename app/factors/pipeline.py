"""
因子批量计算流水线
从 market.daily 读取行情数据，计算各因子，写入 factors.daily_factors
"""
from __future__ import annotations

import polars as pl
from loguru import logger
from sqlalchemy import text

from app.factors.base import BaseFactor
from app.factors.cross_sectional.cross_sectional import LimitUpFactor
from app.factors.technical import MA20Factor, MA60Factor, RSIFactor, MACDFactor
from app.utils.db import get_engine

DEFAULT_FACTORS: list[BaseFactor] = [
    MA20Factor(),
    MA60Factor(),
    RSIFactor(),
    MACDFactor(),
    LimitUpFactor(),
]


def _load_daily(symbol: str, start: str, end: str) -> pl.DataFrame:
    """从 market.daily 读取指定股票的行情数据"""
    sql = text("""
        SELECT time, symbol, open, high, low, close, volume
        FROM market.daily
        WHERE symbol = :symbol
          AND time BETWEEN :start AND :end
        ORDER BY time
    """)
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(sql, {"symbol": symbol, "start": start, "end": end}).fetchall()
    return pl.DataFrame(rows, schema=["time", "symbol", "open", "high", "low", "close", "volume"])


def _upsert_factors(df: pl.DataFrame) -> int:
    """将长格式因子数据写入 factors.daily_factors"""
    if df.is_empty():
        return 0
    rows = df.to_dicts()
    sql = text("""
        INSERT INTO factors.daily_factors (time, symbol, factor_name, factor_value)
        VALUES (:time, :symbol, :factor_name, :factor_value)
        ON CONFLICT (time, symbol, factor_name) DO UPDATE SET
            factor_value = EXCLUDED.factor_value
    """)
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def run_factor_pipeline(
    symbols: list[str],
    start: str,
    end: str,
    factors: list[BaseFactor] | None = None,
) -> int:
    """
    对给定股票列表运行全部因子计算并写入数据库

    Args:
        symbols: 股票代码列表，如 ["000001.SZ", "600000.SH"]
        start:   开始日期 "YYYY-MM-DD"
        end:     结束日期 "YYYY-MM-DD"
        factors: 因子列表，默认使用 DEFAULT_FACTORS

    Returns:
        总写入记录数
    """
    if factors is None:
        factors = DEFAULT_FACTORS

    total = 0
    for symbol in symbols:
        df = _load_daily(symbol, start, end)
        if df.is_empty():
            logger.warning(f"{symbol} 无行情数据，跳过")
            continue
        for factor in factors:
            long_df = factor.compute(df)
            n = _upsert_factors(long_df)
            total += n
            logger.info(f"{symbol} {factor.name}: 写入 {n} 条")

    logger.success(f"因子流水线完成，共写入 {total} 条记录")
    return total

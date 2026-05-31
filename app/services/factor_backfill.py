"""
个股因子缺失时的自动补算服务。
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import polars as pl
from loguru import logger

from app.data_loader.market_data import fetch_daily_by_symbol, upsert_daily
from app.factors.pipeline.loader import load_ohlcv
from app.factors.pipeline.writer import upsert_factors
from app.factors.registry import DEFAULT_FACTORS, max_warmup_days, required_market_fields
from app.utils.db import get_engine


BACKFILL_WINDOW_DAYS = 365


def _yyyymmdd(value: date) -> str:
    return value.strftime("%Y%m%d")


def _factor_date_strings(start: date, end: date) -> set[str]:
    current = start
    dates: set[str] = set()
    while current <= end:
        dates.add(_yyyymmdd(current))
        current += timedelta(days=1)
    return dates


def _compute_symbol_factors(asset_type: str, symbol: str, start: date, end: date) -> int:
    factors = DEFAULT_FACTORS
    engine = get_engine()
    warmup_days = max_warmup_days(factors)
    warmup_start = _yyyymmdd(start - timedelta(days=warmup_days))
    end_str = _yyyymmdd(end)
    target_dates = _factor_date_strings(start, end)
    market_fields = required_market_fields(factors)

    df = load_ohlcv(engine, asset_type, symbol, warmup_start, end_str, market_fields)
    if df.is_empty():
        return 0

    written = 0
    for factor in factors:
        long_df = factor.compute(df).filter(
            pl.col("time").dt.strftime("%Y%m%d").is_in(target_dates)
        )
        written += upsert_factors(engine, long_df, asset_type=asset_type)
    return written


def backfill_symbol_factors(symbol: str, *, asset_type: str = "stock_CN", end_date: date | None = None) -> bool:
    """补齐单只股票过去一年行情与因子；成功后加入全局股票池。"""
    normalized_symbol = symbol.strip().upper()
    if not normalized_symbol:
        raise ValueError("symbol 不能为空")

    end = end_date or datetime.now(timezone.utc).date()
    start = end - timedelta(days=BACKFILL_WINDOW_DAYS)
    start_str = _yyyymmdd(start)
    end_str = _yyyymmdd(end)

    logger.info(f"触发自动补算 | asset_type={asset_type} | symbol={normalized_symbol} | window={start_str}~{end_str}")
    daily_df = fetch_daily_by_symbol(asset_type, normalized_symbol, start_str, end_str)
    if daily_df.is_empty():
        logger.warning(f"自动补算失败：{normalized_symbol} 无可用日线数据")
        return False

    daily_written = upsert_daily(daily_df, asset_type=asset_type)
    factor_written = _compute_symbol_factors(asset_type, normalized_symbol, start, end)
    if factor_written <= 0:
        logger.warning(
            f"自动补算未产出因子 | symbol={normalized_symbol} | market_rows={daily_written}"
        )
        return False

    logger.success(
        f"自动补算完成 | symbol={normalized_symbol} | market_rows={daily_written} | factor_rows={factor_written}"
    )
    return True

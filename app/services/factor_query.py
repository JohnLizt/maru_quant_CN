"""
个股因子查询服务。

提供面向调用方的服务层函数，封装：
- 参数校验
- 默认日期处理
- 因子数据查询
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import polars as pl
from loguru import logger
from sqlalchemy import text

from app.factors.registry import FACTOR_REGISTRY
from app.services.factor_backfill import backfill_symbol_factors
from app.utils.db import get_engine


FACTOR_QUERY_SCHEMA: dict[str, pl.DataType] = {
    "time": pl.Datetime("us", "UTC"),
    "asset_type": pl.Utf8,
    "symbol": pl.Utf8,
    "factor_name": pl.Utf8,
    "factor_value": pl.Float64,
}

MARKET_SYNC_DATA_TYPE = "daily_market"
FACTOR_SYNC_DATA_TYPE = "daily_factors"


@dataclass(frozen=True)
class SyncStatus:
    data_type: str
    status: str | None
    last_date: date | None
    error_msg: str | None


def _today_utc_date() -> date:
    """返回当前 UTC 日期。"""
    return datetime.now(timezone.utc).date()


def _normalize_date(value: str | date | datetime | None, *, default: date) -> date:
    """将输入统一规范为 date。"""
    if value is None:
        return default
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def _compact_date(value: date) -> str:
    return value.strftime("%Y%m%d")


def _parse_sync_last_date(value: str | date | datetime | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not value:
        return None
    return datetime.strptime(value, "%Y%m%d").date()


def _empty_result() -> pl.DataFrame:
    """返回空结果 DataFrame。"""
    return pl.DataFrame(schema=FACTOR_QUERY_SCHEMA)


def _normalize_symbol(symbol: str) -> str:
    """规范化单个股票代码。"""
    normalized_symbol = symbol.strip().upper()
    if not normalized_symbol:
        raise ValueError("symbol 不能为空")
    return normalized_symbol


def _normalize_symbols(symbols: list[str]) -> list[str]:
    """规范化股票代码列表并去重。"""
    normalized = []
    seen: set[str] = set()
    for symbol in symbols:
        current = _normalize_symbol(symbol)
        if current not in seen:
            seen.add(current)
            normalized.append(current)

    if not normalized:
        raise ValueError("symbols 不能为空")
    return normalized


def _normalize_factor_name(factor_name: str) -> str:
    """规范化单个因子名称并校验。"""
    normalized_factor = factor_name.strip()
    if not normalized_factor:
        raise ValueError("factor_name 不能为空")
    if normalized_factor not in FACTOR_REGISTRY:
        raise ValueError(f"未知因子: {normalized_factor}，可用: {list(FACTOR_REGISTRY)}")
    return normalized_factor


def _normalize_factor_names(factor_names: list[str]) -> list[str]:
    """规范化因子名称列表并去重。"""
    normalized = []
    seen: set[str] = set()
    for factor_name in factor_names:
        current = _normalize_factor_name(factor_name)
        if current not in seen:
            seen.add(current)
            normalized.append(current)

    if not normalized:
        raise ValueError("factor_names 不能为空")
    return normalized


def _normalize_date_range(
    start_date: str | date | datetime | None,
    end_date: str | date | datetime | None,
) -> tuple[date, date]:
    """规范化日期范围，默认当天（UTC）。"""
    today = _today_utc_date()
    start = _normalize_date(start_date, default=today)
    end = _normalize_date(end_date, default=today)
    if start > end:
        raise ValueError(f"start_date 不能晚于 end_date: {start} > {end}")
    return start, end


def _query_factors(asset_type: str, symbols: list[str], factor_names: list[str], start: date, end: date) -> pl.DataFrame:
    """执行因子范围查询。"""
    sql = text("""
        SELECT time, asset_type, symbol, factor_name, factor_value
        FROM factors.daily_factors
        WHERE asset_type = :asset_type
          AND symbol = ANY(:symbols)
          AND factor_name = ANY(:factor_names)
          AND time >= :start_date
          AND time < (CAST(:end_date AS date) + INTERVAL '1 day')
        ORDER BY time, symbol, factor_name
    """)

    params: dict[str, Any] = {
        "asset_type": asset_type,
        "symbols": symbols,
        "factor_names": factor_names,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
    }

    with get_engine().connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    if not rows:
        return _empty_result()

    return pl.DataFrame(
        rows,
        schema=["time", "asset_type", "symbol", "factor_name", "factor_value"],
        orient="row",
    ).cast(FACTOR_QUERY_SCHEMA)


def _get_sync_status(data_type: str, asset_type: str) -> SyncStatus | None:
    sql = text("""
        SELECT data_type, status, last_date, error_msg
        FROM meta.sync_status
        WHERE data_type = :data_type
          AND asset_type = :asset_type
          AND (symbol IS NULL OR symbol = '')
        ORDER BY last_date DESC NULLS LAST, updated_at DESC NULLS LAST, id DESC
        LIMIT 1
    """)

    with get_engine().connect() as conn:
        row = conn.execute(sql, {"data_type": data_type, "asset_type": asset_type}).fetchone()

    if row is None:
        return None

    return SyncStatus(
        data_type=row[0],
        status=row[1],
        last_date=_parse_sync_last_date(row[2]),
        error_msg=row[3],
    )


def _get_market_symbol_count(asset_type: str, target: date) -> int:
    sql = text("""
        SELECT COUNT(DISTINCT symbol)
        FROM market.daily
        WHERE asset_type = :asset_type
          AND time >= :start_date
          AND time < (CAST(:start_date AS date) + INTERVAL '1 day')
    """)

    with get_engine().connect() as conn:
        value = conn.execute(sql, {"asset_type": asset_type, "start_date": target.isoformat()}).scalar()

    return int(value or 0)


def _can_auto_backfill(
    asset_type: str,
    symbols: list[str],
    start: date,
    end: date,
    result: pl.DataFrame,
    missing_symbols: list[str],
) -> bool:
    if not missing_symbols:
        return False

    if start != end:
        logger.info(
            "区间查询，跳过自动补算 | start={} | end={} | missing_symbols={}",
            start,
            end,
            missing_symbols,
        )
        return False

    if result.is_empty():
        logger.info(
            "查询结果为空，跳过自动补算 | date={} | symbols={}",
            start,
            symbols,
        )
        return False

    if len(missing_symbols) == len(symbols):
        logger.info(
            "全部请求 symbol 缺失，跳过自动补算 | date={} | symbols={}",
            start,
            symbols,
        )
        return False

    market_symbol_count = _get_market_symbol_count(asset_type, start)
    if market_symbol_count <= 0:
        logger.info(
            "market.daily 当天无记录，跳过自动补算 | date={} | missing_symbols={}",
            start,
            missing_symbols,
        )
        return False

    market_sync = _get_sync_status(MARKET_SYNC_DATA_TYPE, asset_type)
    if market_sync is None or market_sync.last_date is None:
        logger.info(
            "market.daily sync_status 缺失，跳过自动补算 | date={} | missing_symbols={}",
            start,
            missing_symbols,
        )
        return False

    if market_sync.last_date < start:
        logger.info(
            "market.daily sync_status 未覆盖目标日期，跳过自动补算 | date={} | market_last_date={} | status={}",
            start,
            market_sync.last_date,
            market_sync.status,
        )
        return False

    factor_sync = _get_sync_status(FACTOR_SYNC_DATA_TYPE, asset_type)
    if factor_sync is None or factor_sync.last_date is None:
        logger.info(
            "daily_factors sync_status 缺失，跳过自动补算 | date={} | missing_symbols={}",
            start,
            missing_symbols,
        )
        return False

    if factor_sync.last_date < start:
        logger.info(
            "daily_factors sync_status 未覆盖目标日期，跳过自动补算 | date={} | factor_last_date={} | status={}",
            start,
            factor_sync.last_date,
            factor_sync.status,
        )
        return False

    logger.info(
        "检测到部分 symbol 因子缺失，允许自动补算 | date={} | market_symbol_count={} | missing_symbols={} | market_sync={}({}) | factor_sync={}({})",
        start,
        market_symbol_count,
        missing_symbols,
        _compact_date(market_sync.last_date),
        market_sync.status,
        _compact_date(factor_sync.last_date),
        factor_sync.status,
    )
    return True


def _query_with_auto_backfill(asset_type: str, symbols: list[str], factor_names: list[str], start: date, end: date) -> pl.DataFrame:
    """查询因子；若存在缺失股票则自动补算并重试。"""
    result = _query_factors(asset_type, symbols, factor_names, start, end)

    found_symbols = set(result.get_column("symbol").to_list()) if not result.is_empty() else set()
    missing_symbols = [symbol for symbol in symbols if symbol not in found_symbols]
    if not missing_symbols:
        return result

    if not _can_auto_backfill(asset_type, symbols, start, end, result, missing_symbols):
        return result

    backfilled = False
    for symbol in missing_symbols:
        logger.info("开始补算缺失 symbol | date={} | symbol={}", start, symbol)
        if backfill_symbol_factors(symbol, asset_type=asset_type, end_date=end):
            backfilled = True

    if backfilled:
        logger.info("自动补算完成，重试因子查询 | date={} | symbols={}", start, symbols)
        return _query_factors(asset_type, symbols, factor_names, start, end)

    logger.info("自动补算未产出新数据，返回原始查询结果 | date={} | missing_symbols={}", start, missing_symbols)
    return result


def query_stock_factor(
    symbol: str,
    factor_name: str,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    *,
    asset_type: str = "stock_CN",
) -> pl.DataFrame:
    """
    查询单个股票单个因子在指定时间范围内的因子值。

    Args:
        symbol: 股票代码，例如 ``603019.SH``
        factor_name: 因子名称，例如 ``price_to_ma20``
        start_date: 开始日期，支持 ``YYYY-MM-DD`` / ``date`` / ``datetime``，默认当天（UTC）
        end_date: 结束日期，支持 ``YYYY-MM-DD`` / ``date`` / ``datetime``，默认当天（UTC）

    Returns:
        Polars DataFrame，列：``time, symbol, factor_name, factor_value``

    Raises:
        ValueError: 当参数为空、因子不存在或日期范围非法时抛出
    """
    normalized_symbol = _normalize_symbol(symbol)
    normalized_factor = _normalize_factor_name(factor_name)
    start, end = _normalize_date_range(start_date, end_date)
    return _query_with_auto_backfill(asset_type, [normalized_symbol], [normalized_factor], start, end)


def query_stock_factors(
    symbol: str,
    factor_names: list[str],
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    *,
    asset_type: str = "stock_CN",
) -> pl.DataFrame:
    """
    查询单个股票多个因子在指定时间范围内的因子值。

    Args:
        symbol: 股票代码，例如 ``603019.SH``
        factor_names: 因子名称列表，例如 ``["price_to_ma20", "limit_up"]``
        start_date: 开始日期，默认当天（UTC）
        end_date: 结束日期，默认当天（UTC）

    Returns:
        Polars DataFrame，列：``time, symbol, factor_name, factor_value``
    """
    normalized_symbol = _normalize_symbol(symbol)
    normalized_factors = _normalize_factor_names(factor_names)
    start, end = _normalize_date_range(start_date, end_date)
    return _query_with_auto_backfill(asset_type, [normalized_symbol], normalized_factors, start, end)


def query_stocks_factors(
    symbols: list[str],
    factor_names: list[str],
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    *,
    asset_type: str = "stock_CN",
) -> pl.DataFrame:
    """
    查询多个股票多个因子在指定时间范围内的因子值。

    Args:
        symbols: 股票代码列表，例如 ``["603019.SH", "300059.SZ"]``
        factor_names: 因子名称列表，例如 ``["price_to_ma20", "limit_up"]``
        start_date: 开始日期，默认当天（UTC）
        end_date: 结束日期，默认当天（UTC）

    Returns:
        Polars DataFrame，列：``time, symbol, factor_name, factor_value``
    """
    normalized_symbols = _normalize_symbols(symbols)
    normalized_factors = _normalize_factor_names(factor_names)
    start, end = _normalize_date_range(start_date, end_date)
    return _query_with_auto_backfill(asset_type, normalized_symbols, normalized_factors, start, end)

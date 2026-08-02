"""
因子流水线数据加载工具。
"""
from __future__ import annotations

import polars as pl
from sqlalchemy import text

from app.utils.price_adjustment import DEFAULT_PRICE_COLUMNS, apply_price_adjustment


DEFAULT_SCHEMA = {
    "time": pl.Datetime("us", "UTC"),
    "asset_type": pl.Utf8,
    "symbol": pl.Utf8,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
    "amount": pl.Float64,
    "is_suspended": pl.Boolean,
    "adj_factor": pl.Float64,
}


def _iso(yyyymmdd: str) -> str:
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"


def get_all_symbols(engine, asset_type: str) -> list[str]:
    """从 market.daily 获取指定 asset_type 的全量代码。"""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT DISTINCT symbol
                FROM market.daily
                WHERE asset_type = :asset_type
                ORDER BY symbol
                """
            ),
            {"asset_type": asset_type},
        ).fetchall()
    return [r[0] for r in rows]


def get_market_dates(engine, start: str, end: str, asset_type: str) -> list[str]:
    """market.daily 中指定 asset_type 的 [start, end] 交易日列表（YYYYMMDD）。"""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT DISTINCT TO_CHAR(time AT TIME ZONE 'UTC', 'YYYYMMDD')
                FROM market.daily
                WHERE asset_type = :asset_type
                  AND time >= :start
                  AND time <= :end
                ORDER BY 1
                """
            ),
            {"asset_type": asset_type, "start": _iso(start), "end": _iso(end)},
        ).fetchall()
    return [r[0] for r in rows]


def load_ohlcv(engine, asset_type: str, symbol: str, start: str, end: str, fields: set[str] | None = None) -> pl.DataFrame:
    """加载单个 asset_type / symbol 的行情数据（含 warm-up 历史和停牌标记）。"""
    requested = set(fields or DEFAULT_SCHEMA.keys())
    requested.update({"time", "asset_type", "symbol", "is_suspended"})

    ordered_columns = [
        column
        for column in ["time", "asset_type", "symbol", "open", "high", "low", "close", "volume", "amount", "is_suspended"]
        if column in requested
    ]
    schema = {column: DEFAULT_SCHEMA[column] for column in ordered_columns}
    should_adjust = bool(set(DEFAULT_PRICE_COLUMNS) & set(ordered_columns))
    query_columns = [*ordered_columns, "adj_factor"] if should_adjust else ordered_columns
    query_schema = {column: DEFAULT_SCHEMA[column] for column in query_columns}

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT {', '.join(query_columns)}
                FROM market.daily
                WHERE asset_type = :asset_type
                  AND symbol = :symbol
                  AND time BETWEEN :start AND :end
                ORDER BY time
                """
            ),
            {"asset_type": asset_type, "symbol": symbol, "start": _iso(start), "end": _iso(end)},
        ).fetchall()
    if not rows:
        return pl.DataFrame(schema=schema)
    loaded = pl.DataFrame(rows, schema=query_columns, orient="row").cast(query_schema)
    return apply_price_adjustment(loaded).cast(schema)


def load_ohlcv_panel(
    engine,
    asset_type: str,
    symbols: list[str],
    start: str,
    end: str,
    fields: set[str] | None = None,
) -> pl.DataFrame:
    """加载多 symbol 的行情面板数据。"""
    requested = set(fields or DEFAULT_SCHEMA.keys())
    requested.update({"time", "asset_type", "symbol", "is_suspended"})

    ordered_columns = [
        column
        for column in ["time", "asset_type", "symbol", "open", "high", "low", "close", "volume", "amount", "is_suspended"]
        if column in requested
    ]
    schema = {column: DEFAULT_SCHEMA[column] for column in ordered_columns}
    should_adjust = bool(set(DEFAULT_PRICE_COLUMNS) & set(ordered_columns))
    query_columns = [*ordered_columns, "adj_factor"] if should_adjust else ordered_columns
    query_schema = {column: DEFAULT_SCHEMA[column] for column in query_columns}

    if not symbols:
        return pl.DataFrame(schema=schema)

    placeholders = ", ".join(f":symbol_{index}" for index in range(len(symbols)))
    params = {
        "asset_type": asset_type,
        "start": _iso(start),
        "end": _iso(end),
        **{f"symbol_{index}": symbol for index, symbol in enumerate(symbols)},
    }

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT {', '.join(query_columns)}
                FROM market.daily
                WHERE asset_type = :asset_type
                  AND symbol IN ({placeholders})
                  AND time BETWEEN :start AND :end
                ORDER BY symbol, time
                """
            ),
            params,
        ).fetchall()
    if not rows:
        return pl.DataFrame(schema=schema)
    loaded = pl.DataFrame(rows, schema=query_columns, orient="row").cast(query_schema)
    return apply_price_adjustment(loaded).cast(schema)

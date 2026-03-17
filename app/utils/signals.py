"""
信号写入工具
将策略生成的信号写入 signals.trading_signals
"""
from __future__ import annotations

import json

import polars as pl
from loguru import logger
from sqlalchemy import text

from app.utils.db import get_engine


def upsert_signals(df: pl.DataFrame) -> int:
    """
    将交易信号写入 signals.trading_signals（冲突时更新）

    Args:
        df: DataFrame，列：time, symbol, strategy, signal, score, metadata
            metadata 列为 dict（可选），将序列化为 JSONB

    Returns:
        写入/更新的记录数
    """
    if df.is_empty():
        return 0

    rows = df.to_dicts()
    for row in rows:
        if "metadata" not in row or row["metadata"] is None:
            row["metadata"] = "{}"
        elif isinstance(row["metadata"], dict):
            row["metadata"] = json.dumps(row["metadata"], ensure_ascii=False)

    sql = text("""
        INSERT INTO signals.trading_signals
            (time, symbol, strategy, signal, score, metadata)
        VALUES
            (:time, :symbol, :strategy, :signal, :score, CAST(:metadata AS jsonb))
        ON CONFLICT (time, symbol, strategy) DO UPDATE SET
            signal   = EXCLUDED.signal,
            score    = EXCLUDED.score,
            metadata = EXCLUDED.metadata
    """)

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(sql, rows)

    logger.success(f"写入/更新 {len(rows)} 条信号记录 (strategy={rows[0]['strategy']})")
    return len(rows)

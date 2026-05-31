"""
因子流水线执行器。
"""
from __future__ import annotations

import time
from dataclasses import dataclass

import polars as pl
from loguru import logger

from app.factors.base import BaseFactor
from app.factors.pipeline.loader import load_ohlcv
from app.factors.pipeline.writer import upsert_factors


@dataclass(frozen=True)
class RunResult:
    total_written: int
    errors: list[str]


def run_time_series_factors(
    engine,
    asset_type: str,
    symbols: list[str],
    factors: list[BaseFactor],
    warmup_start: str,
    end_str: str,
    target_dates: set[str],
    market_fields: set[str],
    rate_limit: float = 0.05,
) -> RunResult:
    """按 symbol 逐一执行时序因子并写入结果。"""
    total_written = 0
    errors: list[str] = []

    for i, symbol in enumerate(symbols, 1):
        try:
            df = load_ohlcv(engine, asset_type, symbol, warmup_start, end_str, market_fields)
            if df.is_empty():
                continue

            written = 0
            for factor in factors:
                long_df = factor.compute(df)
                long_df = long_df.filter(
                    pl.col("time").dt.strftime("%Y%m%d").is_in(target_dates)
                )
                written += upsert_factors(engine, long_df, asset_type=asset_type)

            total_written += written
            if i % 100 == 0:
                logger.info(f"  [{asset_type}] 进度 {i}/{len(symbols)} | 累计写入 {total_written} 条")

        except Exception as exc:
            logger.error(f"  [{asset_type}] {symbol}: 失败 — {exc}")
            errors.append(f"{symbol}: {exc}")

        time.sleep(rate_limit)

    return RunResult(total_written=total_written, errors=errors)

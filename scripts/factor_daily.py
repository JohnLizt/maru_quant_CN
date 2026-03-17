"""
每日增量因子计算：gap 检测 + warm-up 窗口 + 写入 factors.daily_factors

流程：
  1. 从 market.daily 获取全量股票代码
  2. 对比 factors.daily_factors 找出缺失日期（gap 检测）
  3. 对每只股票，加载 (gap_start - WARMUP_DAYS) 至今的行情（保证 MA60 等指标正确）
  4. 计算所有因子，过滤只保留缺失日期，upsert 写入
  5. 更新 meta.sync_status

用法：
  python scripts/factor_daily.py                               # 默认 7 日回溯，全部因子
  python scripts/factor_daily.py --lookback-days 30            # 每周对账用
  python scripts/factor_daily.py --force-update                # 强制重算窗口内所有交易日
  python scripts/factor_daily.py --factors ma20,rsi14          # 只计算指定因子
  python scripts/factor_daily.py --factors limit_up --force-update
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/app")

import polars as pl
from loguru import logger
from sqlalchemy import text

from app.factors.base import BaseFactor
from app.factors.cross_sectional.cross_sectional import LimitUpFactor
from app.factors.technical import PriceToMA20Factor, MACrossGactor, RSIFactor, MACDNormFactor
from app.utils.db import get_engine

# MA60 是最长指标窗口；额外加 5 个交易日作为缓冲
WARMUP_DAYS = 90          # 日历天，换算约 60 个交易日 + buffer
RATE_LIMIT  = 0.05        # 每只股票写入后短暂休眠（秒），避免 DB 连接堆积
DATA_TYPE   = "daily_factors"

DEFAULT_FACTORS: list[BaseFactor] = [
    PriceToMA20Factor(),
    MACrossGactor(),
    RSIFactor(),
    MACDNormFactor(),
    LimitUpFactor(),
]

FACTOR_REGISTRY: dict[str, BaseFactor] = {f.name: f for f in DEFAULT_FACTORS}


# ── DB 工具 ───────────────────────────────────────────────────

def _yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def _iso(yyyymmdd: str) -> str:
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"


def get_all_symbols(engine) -> list[str]:
    """从 market.daily 获取全量股票代码"""
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT DISTINCT symbol FROM market.daily ORDER BY symbol"
        )).fetchall()
    return [r[0] for r in rows]


def get_market_dates(engine, start: str, end: str) -> list[str]:
    """market.daily 中 [start, end] 的交易日列表（YYYYMMDD）"""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT TO_CHAR(time AT TIME ZONE 'UTC', 'YYYYMMDD')
            FROM market.daily
            WHERE time >= :start AND time <= :end
            ORDER BY 1
        """), {"start": _iso(start), "end": _iso(end)}).fetchall()
    return [r[0] for r in rows]


def get_factor_dates(engine, start: str, end: str) -> set[str]:
    """factors.daily_factors 中 [start, end] 已有的日期集合（YYYYMMDD）"""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT TO_CHAR(time AT TIME ZONE 'UTC', 'YYYYMMDD')
            FROM factors.daily_factors
            WHERE time >= :start AND time <= :end
        """), {"start": _iso(start), "end": _iso(end)}).fetchall()
    return {r[0] for r in rows}


def load_ohlcv(engine, symbol: str, start: str, end: str) -> pl.DataFrame:
    """加载单只股票的行情数据（含 warm-up 历史）"""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT time, symbol, open, high, low, close, volume
            FROM market.daily
            WHERE symbol = :symbol
              AND time BETWEEN :start AND :end
            ORDER BY time
        """), {"symbol": symbol, "start": _iso(start), "end": _iso(end)}).fetchall()
    if not rows:
        return pl.DataFrame(schema={
            "time": pl.Datetime("us", "UTC"),
            "symbol": pl.Utf8,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Int64,
        })
    return pl.DataFrame(rows, schema=["time", "symbol", "open", "high", "low", "close", "volume"],
                        orient="row")


def upsert_factors(engine, df: pl.DataFrame) -> int:
    """将长格式因子数据写入 factors.daily_factors"""
    if df.is_empty():
        return 0
    rows = df.to_dicts()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO factors.daily_factors (time, symbol, factor_name, factor_value)
            VALUES (:time, :symbol, :factor_name, :factor_value)
            ON CONFLICT (time, symbol, factor_name) DO UPDATE SET
                factor_value = EXCLUDED.factor_value
        """), rows)
    return len(rows)


def update_sync_status(engine, status: str, last_date: str | None,
                       error_msg: str | None = None) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO meta.sync_status
                (data_type, symbol, last_sync_time, last_date, status, error_msg, updated_at)
            VALUES
                (:data_type, NULL, NOW(), :last_date, :status, :error_msg, NOW())
            ON CONFLICT (data_type, symbol) DO UPDATE SET
                last_sync_time = NOW(),
                last_date      = EXCLUDED.last_date,
                status         = EXCLUDED.status,
                error_msg      = EXCLUDED.error_msg,
                updated_at     = NOW()
        """), {
            "data_type": DATA_TYPE,
            "last_date":  last_date,
            "status":     status,
            "error_msg":  error_msg,
        })


# ── 主流程 ────────────────────────────────────────────────────

def main(lookback_days: int, force_update: bool = False,
         factor_names: list[str] | None = None) -> None:
    engine = get_engine()

    today     = datetime.now(timezone.utc)
    end_str   = _yyyymmdd(today)
    start_str = _yyyymmdd(today - timedelta(days=lookback_days))

    # ── 解析因子列表 ─────────────────────────────────────────
    if factor_names:
        unknown = [n for n in factor_names if n not in FACTOR_REGISTRY]
        if unknown:
            logger.error(f"未知因子: {unknown}，可用: {list(FACTOR_REGISTRY)}")
            sys.exit(1)
        factors = [FACTOR_REGISTRY[n] for n in factor_names]
    else:
        factors = DEFAULT_FACTORS

    logger.info(f"因子流水线 | lookback={lookback_days}d | {start_str} ~ {end_str}"
                + (f" | factors={[f.name for f in factors]}" if factor_names else "")
                + (" | FORCE" if force_update else ""))

    # ── 1. 交易日 gap 检测 ───────────────────────────────────
    market_dates = get_market_dates(engine, start_str, end_str)
    if not market_dates:
        logger.warning("market.daily 无数据，退出")
        return
    logger.info(f"market.daily 交易日: {len(market_dates)} 个 "
                f"({market_dates[0]} ~ {market_dates[-1]})")

    if force_update:
        missing = market_dates
        logger.info(f"强制模式：重新计算全部 {len(missing)} 个交易日")
    else:
        factor_dates = get_factor_dates(engine, start_str, end_str)
        missing      = [d for d in market_dates if d not in factor_dates]
        logger.info(f"factors.daily_factors 已有 {len(factor_dates)} 个交易日，缺失 {len(missing)} 个")

    if not missing:
        logger.success("因子数据完整，无需补全")
        update_sync_status(engine, "ok", market_dates[-1])
        return

    # ── 2. 确定加载窗口（含 warm-up）───────────────────────
    gap_start    = missing[0]                                    # 最早缺失日
    warmup_start = _yyyymmdd(
        datetime.strptime(gap_start, "%Y%m%d") - timedelta(days=WARMUP_DAYS)
    )
    logger.info(f"缺失日期: {missing[0]} ~ {missing[-1]}，"
                f"行情加载窗口: {warmup_start} ~ {end_str}")

    missing_set = set(missing)

    # ── 3. 获取股票列表 ──────────────────────────────────────
    symbols = get_all_symbols(engine)
    logger.info(f"股票数量: {len(symbols)}")

    # ── 4. 逐股票计算 ────────────────────────────────────────
    total_written = 0
    errors: list[str] = []

    for i, symbol in enumerate(symbols, 1):
        try:
            df = load_ohlcv(engine, symbol, warmup_start, end_str)
            if df.is_empty():
                continue

            written = 0
            for factor in factors:
                long_df = factor.compute(df)
                # 只写入缺失日期
                long_df = long_df.filter(
                    pl.col("time").dt.strftime("%Y%m%d").is_in(missing_set)
                )
                written += upsert_factors(engine, long_df)

            total_written += written
            if i % 100 == 0:
                logger.info(f"  进度 {i}/{len(symbols)} | 累计写入 {total_written} 条")

        except Exception as exc:
            logger.error(f"  {symbol}: 失败 — {exc}")
            errors.append(f"{symbol}: {exc}")

        time.sleep(RATE_LIMIT)

    # ── 5. 更新 sync_status ──────────────────────────────────
    if errors:
        update_sync_status(engine, "error", missing[-1], "; ".join(errors[:5]))
        logger.warning(f"因子流水线完成（含错误）| 写入 {total_written} 条 | "
                       f"失败 {len(errors)} 只")
        sys.exit(1)
    else:
        update_sync_status(engine, "ok", missing[-1])
        logger.success(f"因子流水线完成 | 补齐 {len(missing)} 个交易日 | "
                       f"共写入 {total_written} 条")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily incremental factor computation")
    parser.add_argument(
        "--lookback-days", type=int, default=7,
        help="回溯天数（默认 7；每周对账用 30）",
    )
    parser.add_argument(
        "--force-update", action="store_true",
        help="强制重新计算窗口内所有交易日，忽略已有数据",
    )
    parser.add_argument(
        "--factors", type=str, default=None,
        help=f"逗号分隔的因子名称，默认全部。可选: {','.join(FACTOR_REGISTRY)}",
    )
    args = parser.parse_args()
    factor_names = [f.strip() for f in args.factors.split(",")] if args.factors else None
    main(args.lookback_days, args.force_update, factor_names)

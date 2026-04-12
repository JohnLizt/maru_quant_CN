"""
每日增量 ETL：market.daily 数据补全 + 孔洞检测

流程：
  1. 从 000001.SZ 日线推导交易日历（无需 trade_cal 权限）
  2. 对比 market.daily 中已有日期 → 找出缺失孔洞
  3. 按日期逐一拉取全市场行情并写入 DB
  4. 更新 meta.sync_status

用法：
  python scripts/etl_daily.py                          # 默认 7 日回溯
  python scripts/etl_daily.py --lookback-days 30       # 每周对账用
  python scripts/etl_daily.py --force-update           # 强制重新拉取窗口内所有交易日
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, "/app")

import polars as pl
import tushare as ts
from loguru import logger
from sqlalchemy import text

from app.data_pipeline.fetch_daily import upsert_daily
from app.utils.db import get_engine

RATE_LIMIT     = 1.25          # Tushare 调用间隔（秒）
DATA_TYPE      = "daily_market"
CAL_SYMBOL     = "000001.SZ"   # 用于推导交易日历的参考股票
FETCH_FIELDS   = "ts_code,trade_date,open,high,low,close,pct_chg,vol,amount"
STOCK_POOL_CSV = Path("/app/config/stock_pool.csv")


# ── 工具函数 ──────────────────────────────────────────────────

def _yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def _iso(yyyymmdd: str) -> str:
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"


def get_trading_dates(pro, start: str, end: str) -> list[str]:
    """从参考股票日线推导交易日列表（YYYYMMDD 字符串）"""
    df = pro.daily(ts_code=CAL_SYMBOL, start_date=start, end_date=end, fields="trade_date")
    time.sleep(RATE_LIMIT)
    return sorted(df["trade_date"].tolist())


def load_stock_pool(path: Path = STOCK_POOL_CSV) -> pl.DataFrame:
    """加载股票池配置，至少包含 symbol 列。"""
    if not path.exists():
        raise FileNotFoundError(f"股票池文件不存在: {path}")

    pool = pl.read_csv(path)
    if "symbol" not in pool.columns:
        raise ValueError(f"股票池文件缺少 symbol 列: {path}")

    pool = (
        pool
        .with_columns(pl.col("symbol").cast(pl.Utf8).str.strip_chars())
        .filter(pl.col("symbol") != "")
        .unique(subset=["symbol"], keep="first")
        .sort("symbol")
    )
    if pool.is_empty():
        raise ValueError(f"股票池为空: {path}")
    return pool


def get_existing_dates(engine, start: str, end: str, symbols: list[str]) -> set[str]:
    """查询股票池内已完整写入的交易日集合。"""
    if not symbols:
        return set()

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT TO_CHAR(time AT TIME ZONE 'UTC', 'YYYYMMDD') AS trade_date
            FROM market.daily
            WHERE time >= :start AND time <= :end
              AND symbol = ANY(:symbols)
            GROUP BY trade_date
            HAVING COUNT(DISTINCT symbol) = :symbol_count
        """), {
            "start": _iso(start),
            "end": _iso(end),
            "symbols": symbols,
            "symbol_count": len(symbols),
        }).fetchall()
    return {r[0] for r in rows}


def get_incomplete_dates(engine, start: str, end: str, symbols: list[str]) -> list[tuple[str, int]]:
    """返回股票池内不完整的交易日及当日已写入 symbol 数。"""
    if not symbols:
        return []

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
                TO_CHAR(time AT TIME ZONE 'UTC', 'YYYYMMDD') AS trade_date,
                COUNT(DISTINCT symbol) AS symbol_count
            FROM market.daily
            WHERE time >= :start AND time <= :end
              AND symbol = ANY(:symbols)
            GROUP BY trade_date
            HAVING COUNT(DISTINCT symbol) < :symbol_count
            ORDER BY trade_date
        """), {
            "start": _iso(start),
            "end": _iso(end),
            "symbols": symbols,
            "symbol_count": len(symbols),
        }).fetchall()
    return [(r[0], r[1]) for r in rows]


def fetch_one_date(pro, date: str, symbols: list[str]) -> int:
    """拉取单个交易日股票池行情并写入 market.daily，返回写入行数"""
    df_pd = pro.daily(trade_date=date, fields=FETCH_FIELDS)
    if df_pd is None or df_pd.empty:
        logger.warning(f"{date}: Tushare 返回空数据，跳过")
        return 0

    df_pd = df_pd[df_pd["ts_code"].isin(symbols)]
    if df_pd.empty:
        logger.warning(f"{date}: 股票池无数据，跳过")
        return 0

    df = (
        pl.from_pandas(df_pd)
        .rename({"ts_code": "symbol", "trade_date": "time",
                 "pct_chg": "pct_change", "vol": "volume"})
        .with_columns(
            pl.col("time").str.strptime(pl.Date, "%Y%m%d").cast(pl.Datetime("us", "UTC"))
        )
        .with_columns([
            pl.col("open").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("pct_change").cast(pl.Float64),
            pl.col("volume").cast(pl.Int64),
            pl.col("amount").cast(pl.Float64),
        ])
        .select(["time", "symbol", "open", "high", "low", "close",
                 "volume", "amount", "pct_change"])
    )
    return upsert_daily(df)


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

def main(lookback_days: int, force_update: bool = False) -> None:
    ts.set_token(os.environ["TUSHARE_TOKEN"])
    pro    = ts.pro_api()
    engine = get_engine()
    stock_pool = load_stock_pool()
    symbols = stock_pool["symbol"].to_list()

    today     = datetime.now(timezone.utc)
    start_str = _yyyymmdd(today - timedelta(days=lookback_days))
    end_str   = _yyyymmdd(today)

    logger.info(f"ETL daily | lookback={lookback_days}d | {start_str} ~ {end_str}"
                + (" | FORCE" if force_update else ""))
    logger.info(f"股票池: {len(symbols)} 只 | 文件={STOCK_POOL_CSV}")

    # ── 1. 交易日历 ──────────────────────────────────────────
    trade_dates = get_trading_dates(pro, start_str, end_str)
    if not trade_dates:
        logger.warning("交易日历为空，退出")
        return
    logger.info(f"交易日: {len(trade_dates)} 个 ({trade_dates[0]} ~ {trade_dates[-1]})")

    # ── 2. 孔洞检测 ──────────────────────────────────────────
    if force_update:
        missing = trade_dates
        logger.info(f"强制模式：重新拉取全部 {len(missing)} 个交易日")
    else:
        existing = get_existing_dates(engine, start_str, end_str, symbols)
        missing  = [d for d in trade_dates if d not in existing]
        logger.info(f"股票池已完整写入 {len(existing)} 个交易日，缺失 {len(missing)} 个")

    if not missing:
        logger.success("数据完整，无需补全")
        update_sync_status(engine, "ok", trade_dates[-1])
        return

    # ── 3. 拉取 & 写入缺失日期 ───────────────────────────────
    last_success: str | None = None
    errors: list[str] = []

    for date in missing:
        try:
            n = fetch_one_date(pro, date, symbols)
            logger.info(f"  {date}: 写入 {n} 条")
            last_success = date
        except Exception as exc:
            logger.error(f"  {date}: 失败 — {exc}")
            errors.append(f"{date}: {exc}")
        time.sleep(RATE_LIMIT)

    # ── 4. 更新 sync_status ──────────────────────────────────
    existing = get_existing_dates(engine, start_str, end_str, symbols)
    incomplete = get_incomplete_dates(engine, start_str, end_str, symbols)
    still_missing = [d for d in trade_dates if d not in existing]
    if still_missing:
        preview = ", ".join(
            f"{trade_date}({count}/{len(symbols)})" for trade_date, count in incomplete[:5]
        )
        errors.append(
            f"incomplete dates remain: {len(still_missing)}"
            + (f" | sample: {preview}" if preview else "")
        )

    failed_dates = len([e for e in errors if not e.startswith("incomplete dates remain:")])

    if errors:
        update_sync_status(engine, "error", last_success, "; ".join(errors))
        logger.warning(
            f"ETL 完成（含错误）| 成功 {len(missing)-failed_dates} | 失败日期 {failed_dates}"
            + (f" | 不完整交易日 {len(still_missing)}" if still_missing else "")
        )
        sys.exit(1)
    else:
        update_sync_status(engine, "ok", missing[-1])
        logger.success(f"ETL 完成 | 补齐 {len(missing)} 个交易日")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily market.daily incremental ETL")
    parser.add_argument(
        "--lookback-days", type=int, default=7,
        help="回溯天数（默认 7；每周对账用 30）",
    )
    parser.add_argument(
        "--force-update", action="store_true",
        help="强制重新拉取窗口内所有交易日，忽略已有数据",
    )
    args = parser.parse_args()
    main(args.lookback_days, args.force_update)

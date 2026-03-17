"""
每日增量 ETL：market.daily 数据补全 + 孔洞检测

流程：
  1. 从 000001.SZ 日线推导交易日历（无需 trade_cal 权限）
  2. 对比 market.daily 中已有日期 → 找出缺失孔洞
  3. 按日期逐一拉取全市场行情并写入 DB
  4. 更新 meta.sync_status

用法：
  python scripts/etl_daily.py                     # 默认 7 日回溯
  python scripts/etl_daily.py --lookback-days 30  # 每周对账用
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone

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


def get_existing_dates(engine, start: str, end: str) -> set[str]:
    """查询 market.daily 中已有的交易日集合"""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT TO_CHAR(time AT TIME ZONE 'UTC', 'YYYYMMDD')
            FROM market.daily
            WHERE time >= :start AND time <= :end
        """), {"start": _iso(start), "end": _iso(end)}).fetchall()
    return {r[0] for r in rows}


def fetch_one_date(pro, date: str) -> int:
    """拉取单个交易日全市场行情并写入 market.daily，返回写入行数"""
    df_pd = pro.daily(trade_date=date, fields=FETCH_FIELDS)
    if df_pd is None or df_pd.empty:
        logger.warning(f"{date}: Tushare 返回空数据，跳过")
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

def main(lookback_days: int) -> None:
    ts.set_token(os.environ["TUSHARE_TOKEN"])
    pro    = ts.pro_api()
    engine = get_engine()

    today     = datetime.now(timezone.utc)
    start_str = _yyyymmdd(today - timedelta(days=lookback_days))
    end_str   = _yyyymmdd(today)

    logger.info(f"ETL daily | lookback={lookback_days}d | {start_str} ~ {end_str}")

    # ── 1. 交易日历 ──────────────────────────────────────────
    trade_dates = get_trading_dates(pro, start_str, end_str)
    if not trade_dates:
        logger.warning("交易日历为空，退出")
        return
    logger.info(f"交易日: {len(trade_dates)} 个 ({trade_dates[0]} ~ {trade_dates[-1]})")

    # ── 2. 孔洞检测 ──────────────────────────────────────────
    existing = get_existing_dates(engine, start_str, end_str)
    missing  = [d for d in trade_dates if d not in existing]
    logger.info(f"DB 已有 {len(existing)} 个交易日，缺失 {len(missing)} 个")

    if not missing:
        logger.success("数据完整，无需补全")
        update_sync_status(engine, "ok", trade_dates[-1])
        return

    # ── 3. 拉取 & 写入缺失日期 ───────────────────────────────
    last_success: str | None = None
    errors: list[str] = []

    for date in missing:
        try:
            n = fetch_one_date(pro, date)
            logger.info(f"  {date}: 写入 {n} 条")
            last_success = date
        except Exception as exc:
            logger.error(f"  {date}: 失败 — {exc}")
            errors.append(f"{date}: {exc}")
        time.sleep(RATE_LIMIT)

    # ── 4. 更新 sync_status ──────────────────────────────────
    if errors:
        update_sync_status(engine, "error", last_success, "; ".join(errors))
        logger.warning(f"ETL 完成（含错误）| 成功 {len(missing)-len(errors)} | 失败 {len(errors)}")
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
    args = parser.parse_args()
    main(args.lookback_days)

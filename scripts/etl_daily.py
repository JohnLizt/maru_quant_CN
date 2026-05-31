"""
每日增量 ETL：market.daily 数据补全 + 孔洞检测

流程：
  1. 遍历启用的 asset_type
  2. 读取该 asset_type 的 pipeline universe
  3. 推导交易日历并做孔洞检测
  4. 按日期拉取对应 universe 行情并写入 DB
  5. 更新 meta.sync_status
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/app")

import polars as pl
from loguru import logger
from sqlalchemy import text

from app.data_loader.market_data import get_market_data_loader, upsert_daily
from app.services.asset_universe import get_asset_type_config, list_asset_types, resolve_pipeline_symbols
from app.utils.db import get_engine

DATA_TYPE = "daily_market"


def _yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def _iso(yyyymmdd: str) -> str:
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"


def _resolve_asset_types(selected_asset_types: list[str] | None) -> list[str]:
    if selected_asset_types:
        seen: set[str] = set()
        ordered: list[str] = []
        for asset_type in selected_asset_types:
            normalized = asset_type.strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            ordered.append(normalized)
        if not ordered:
            raise ValueError("--asset-type 不能为空")
        return ordered
    return [config.asset_type for config in list_asset_types(enabled_only=True)]


def get_existing_dates(engine, asset_type: str, start: str, end: str, symbols: list[str]) -> set[str]:
    if not symbols:
        return set()

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT TO_CHAR(time AT TIME ZONE 'UTC', 'YYYYMMDD') AS trade_date
                FROM market.daily
                WHERE asset_type = :asset_type
                  AND time >= :start AND time <= :end
                  AND symbol = ANY(:symbols)
                GROUP BY trade_date
                HAVING COUNT(DISTINCT symbol) = :symbol_count
                """
            ),
            {
                "asset_type": asset_type,
                "start": _iso(start),
                "end": _iso(end),
                "symbols": symbols,
                "symbol_count": len(symbols),
            },
        ).fetchall()
    return {r[0] for r in rows}


def get_incomplete_dates(engine, asset_type: str, start: str, end: str, symbols: list[str]) -> list[tuple[str, int]]:
    if not symbols:
        return []

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    TO_CHAR(time AT TIME ZONE 'UTC', 'YYYYMMDD') AS trade_date,
                    COUNT(DISTINCT symbol) AS symbol_count
                FROM market.daily
                WHERE asset_type = :asset_type
                  AND time >= :start AND time <= :end
                  AND symbol = ANY(:symbols)
                GROUP BY trade_date
                HAVING COUNT(DISTINCT symbol) < :symbol_count
                ORDER BY trade_date
                """
            ),
            {
                "asset_type": asset_type,
                "start": _iso(start),
                "end": _iso(end),
                "symbols": symbols,
                "symbol_count": len(symbols),
            },
        ).fetchall()
    return [(r[0], r[1]) for r in rows]


def get_previous_closes(engine, asset_type: str, date: str, symbols: list[str]) -> dict[str, float]:
    if not symbols:
        return {}

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT DISTINCT ON (symbol)
                    symbol,
                    close::double precision AS close
                FROM market.daily
                WHERE asset_type = :asset_type
                  AND symbol = ANY(:symbols)
                  AND time < :date
                ORDER BY symbol, time DESC
                """
            ),
            {"asset_type": asset_type, "symbols": symbols, "date": _iso(date)},
        ).fetchall()
    return {r[0]: float(r[1]) for r in rows if r[1] is not None}


def build_suspended_rows(engine, asset_type: str, date: str, suspended_symbols: list[str], data_source: str) -> pl.DataFrame:
    prev_close_map = get_previous_closes(engine, asset_type, date, suspended_symbols)
    missing_prev_close = [symbol for symbol in suspended_symbols if symbol not in prev_close_map]
    if missing_prev_close:
        raise ValueError(f"停牌股票缺少前收盘价，无法填充: {missing_prev_close[:5]}")

    trade_time = datetime.strptime(date, "%Y%m%d").replace(tzinfo=timezone.utc)
    rows = [
        {
            "time": trade_time,
            "asset_type": asset_type,
            "symbol": symbol,
            "open": prev_close,
            "high": prev_close,
            "low": prev_close,
            "close": prev_close,
            "volume": 0,
            "amount": 0.0,
            "pct_change": 0.0,
            "is_suspended": True,
            "data_source": data_source,
        }
        for symbol, prev_close in sorted(prev_close_map.items())
    ]
    return pl.DataFrame(
        rows,
        schema={
            "time": pl.Datetime("us", "UTC"),
            "asset_type": pl.Utf8,
            "symbol": pl.Utf8,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Int64,
            "amount": pl.Float64,
            "pct_change": pl.Float64,
            "is_suspended": pl.Boolean,
            "data_source": pl.Utf8,
        },
    )


def _empty_daily_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "time": pl.Datetime("us", "UTC"),
            "asset_type": pl.Utf8,
            "symbol": pl.Utf8,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Int64,
            "amount": pl.Float64,
            "pct_change": pl.Float64,
            "is_suspended": pl.Boolean,
            "data_source": pl.Utf8,
        }
    )


def fetch_one_date(loader, engine, asset_type: str, date: str, symbols: list[str], data_source: str) -> int:
    df = loader.fetch_daily_by_date(asset_type, date, symbols)
    actual_symbols = set(df.get_column("symbol").to_list()) if not df.is_empty() else set()

    if asset_type != "stock_CN":
        if df.is_empty():
            logger.warning("[{}] {}: universe 无数据，跳过", asset_type, date)
            return 0
        return upsert_daily(df, asset_type=asset_type)

    missing_symbols = sorted(set(symbols) - actual_symbols)
    if missing_symbols:
        suspended_symbols = loader.get_suspended_symbols(date, symbols)
        unresolved = sorted(set(missing_symbols) - suspended_symbols)
        if unresolved:
            raise ValueError(
                f"非停牌股票缺少日线数据: {unresolved[:5]}" + (" ..." if len(unresolved) > 5 else "")
            )
        suspended_df = build_suspended_rows(engine, asset_type, date, sorted(suspended_symbols), data_source)
        logger.info("[{}] {}: 检测到停牌股票 {} 只，已按前收补齐", asset_type, date, len(suspended_symbols))
    else:
        suspended_df = _empty_daily_frame()

    frames: list[pl.DataFrame] = []
    if not df.is_empty():
        frames.append(df)
    if not suspended_df.is_empty():
        frames.append(suspended_df)

    if not frames:
        logger.warning("[{}] {}: pipeline universe 无数据，跳过", asset_type, date)
        return 0

    return upsert_daily(pl.concat(frames, how="vertical"), asset_type=asset_type)


def update_sync_status(
    engine,
    asset_type: str,
    data_source: str,
    status: str,
    last_date: str | None,
    error_msg: str | None = None,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO meta.sync_status
                    (data_type, asset_type, symbol, data_source, last_sync_time, last_date, status, error_msg, updated_at)
                VALUES
                    (:data_type, :asset_type, '', :data_source, NOW(), :last_date, :status, :error_msg, NOW())
                ON CONFLICT (data_type, asset_type, symbol, data_source) DO UPDATE SET
                    last_sync_time = NOW(),
                    last_date      = EXCLUDED.last_date,
                    status         = EXCLUDED.status,
                    error_msg      = EXCLUDED.error_msg,
                    updated_at     = NOW()
                """
            ),
            {
                "data_type": DATA_TYPE,
                "asset_type": asset_type,
                "data_source": data_source,
                "last_date": last_date,
                "status": status,
                "error_msg": error_msg,
            },
        )


def _run_for_asset_type(engine, asset_type: str, lookback_days: int, force_update: bool) -> list[str]:
    config = get_asset_type_config(asset_type)
    loader = get_market_data_loader(asset_type)
    symbols = resolve_pipeline_symbols(asset_type)

    today = datetime.now(timezone.utc)
    start_str = _yyyymmdd(today - timedelta(days=lookback_days))
    end_str = _yyyymmdd(today)

    logger.info(
        "ETL daily | asset_type={} | lookback={}d | {} ~ {}{}",
        asset_type,
        lookback_days,
        start_str,
        end_str,
        " | FORCE" if force_update else "",
    )
    logger.info(
        "[{}] pipeline universe: {} 只 | loader={} | data_source={}",
        asset_type,
        len(symbols),
        config.loader_key,
        config.data_source,
    )

    trade_dates = loader.get_trading_dates(asset_type, start_str, end_str)
    if not trade_dates:
        logger.warning("[{}] 交易日历为空，退出", asset_type)
        return []
    logger.info("[{}] 交易日: {} 个 ({} ~ {})", asset_type, len(trade_dates), trade_dates[0], trade_dates[-1])

    if force_update:
        missing = trade_dates
        logger.info("[{}] 强制模式：重新拉取全部 {} 个交易日", asset_type, len(missing))
    else:
        existing = get_existing_dates(engine, asset_type, start_str, end_str, symbols)
        missing = [d for d in trade_dates if d not in existing]
        logger.info("[{}] 已完整写入 {} 个交易日，缺失 {}", asset_type, len(existing), len(missing))

    if not missing:
        logger.success("[{}] 数据完整，无需补全", asset_type)
        update_sync_status(engine, asset_type, config.data_source, "ok", trade_dates[-1])
        return []

    last_success: str | None = None
    errors: list[str] = []

    for date in missing:
        try:
            n = fetch_one_date(loader, engine, asset_type, date, symbols, config.data_source)
            logger.info("[{}] {}: 写入 {} 条", asset_type, date, n)
            last_success = date
        except Exception as exc:
            logger.error("[{}] {}: 失败 — {}", asset_type, date, exc)
            errors.append(f"{date}: {exc}")

    existing = get_existing_dates(engine, asset_type, start_str, end_str, symbols)
    incomplete = get_incomplete_dates(engine, asset_type, start_str, end_str, symbols)
    still_missing = [d for d in trade_dates if d not in existing]
    if still_missing:
        preview = ", ".join(f"{trade_date}({count}/{len(symbols)})" for trade_date, count in incomplete[:5])
        errors.append(f"incomplete dates remain: {len(still_missing)}" + (f" | sample: {preview}" if preview else ""))

    failed_dates = len([e for e in errors if not e.startswith("incomplete dates remain:")])

    if errors:
        update_sync_status(engine, asset_type, config.data_source, "error", last_success, "; ".join(errors))
        logger.warning(
            "[{}] ETL 完成（含错误）| 成功 {} | 失败日期 {}{}",
            asset_type,
            len(missing) - failed_dates,
            failed_dates,
            f" | 不完整交易日 {len(still_missing)}" if still_missing else "",
        )
    else:
        update_sync_status(engine, asset_type, config.data_source, "ok", missing[-1])
        logger.success("[{}] ETL 完成 | 补齐 {} 个交易日", asset_type, len(missing))

    return errors


def main(lookback_days: int, force_update: bool = False, asset_types: list[str] | None = None) -> None:
    engine = get_engine()
    try:
        resolved_asset_types = _resolve_asset_types(asset_types)
    except ValueError as exc:
        logger.error(str(exc))
        sys.exit(1)

    aggregated_errors: list[str] = []
    for asset_type in resolved_asset_types:
        try:
            errors = _run_for_asset_type(engine, asset_type, lookback_days, force_update)
            aggregated_errors.extend(f"[{asset_type}] {error}" for error in errors)
        except Exception as exc:
            logger.error("[{}] ETL 失败 — {}", asset_type, exc)
            aggregated_errors.append(f"[{asset_type}] {exc}")

    if aggregated_errors:
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily market.daily incremental ETL")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="回溯天数（默认 7；每周对账用 30）",
    )
    parser.add_argument(
        "--force-update",
        action="store_true",
        help="强制重新拉取窗口内所有交易日，忽略已有数据",
    )
    parser.add_argument(
        "--asset-type",
        action="append",
        dest="asset_types",
        default=[],
        help="可重复传入，仅执行指定 asset_type；不传则遍历全部 enabled asset_type",
    )
    args = parser.parse_args()
    main(args.lookback_days, args.force_update, args.asset_types or None)

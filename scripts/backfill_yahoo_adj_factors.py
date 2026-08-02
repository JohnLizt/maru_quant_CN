"""One-time Yahoo adjustment-factor backfill for an ETL universe."""
from __future__ import annotations

import argparse
import math
import sys
from dataclasses import dataclass
from datetime import date, timedelta

import polars as pl
from loguru import logger
from sqlalchemy import text

sys.path.insert(0, "/app")

from app.data_loader.market_data import get_market_data_loader, update_daily_adjustments
from app.services.asset_universe import resolve_etl_symbols
from app.utils.db import get_engine


@dataclass(frozen=True)
class BackfillWindow:
    start: date
    end: date


def _subtract_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(year=value.year - years, day=28)


def _load_expected_dates(
    engine,
    asset_type: str,
    symbols: list[str],
    lookback_years: int,
) -> tuple[BackfillWindow, dict[str, set[date]]]:
    if lookback_years <= 0:
        raise ValueError("--lookback-years 必须大于 0")
    if not symbols:
        raise ValueError(f"asset_type={asset_type} 没有 active ETL symbols")

    with engine.connect() as conn:
        latest = conn.execute(
            text(
                """
                SELECT MAX(time)::date
                FROM market.daily
                WHERE asset_type = :asset_type
                  AND symbol = ANY(:symbols)
                """
            ),
            {"asset_type": asset_type, "symbols": symbols},
        ).scalar_one_or_none()
    if latest is None:
        raise RuntimeError(f"market.daily 没有 asset_type={asset_type} 行情")

    end = latest if isinstance(latest, date) else latest.date()
    start = _subtract_years(end, lookback_years)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT time::date, symbol
                FROM market.daily
                WHERE asset_type = :asset_type
                  AND symbol = ANY(:symbols)
                  AND time::date BETWEEN :start_date AND :end_date
                ORDER BY symbol, time
                """
            ),
            {
                "asset_type": asset_type,
                "symbols": symbols,
                "start_date": start,
                "end_date": end,
            },
        ).fetchall()

    expected = {symbol: set() for symbol in symbols}
    for trade_date, symbol in rows:
        expected[str(symbol)].add(trade_date)
    empty_symbols = [symbol for symbol, dates in expected.items() if not dates]
    if empty_symbols:
        raise RuntimeError(f"以下标的在回填窗口没有数据库行情: {empty_symbols}")
    return BackfillWindow(start=start, end=end), expected


def _validate_and_select(
    daily: pl.DataFrame,
    *,
    asset_type: str,
    symbol: str,
    expected_dates: set[date],
) -> pl.DataFrame:
    required = {"time", "asset_type", "symbol", "adj_factor", "pct_change"}
    missing_columns = required - set(daily.columns)
    if missing_columns:
        raise ValueError(f"{symbol}: Yahoo 数据缺少列 {sorted(missing_columns)}")
    if daily.is_empty():
        raise ValueError(f"{symbol}: Yahoo 返回空数据")
    returned_asset_types = set(daily.get_column("asset_type").drop_nulls().to_list())
    returned_symbols = set(daily.get_column("symbol").drop_nulls().to_list())
    if returned_asset_types != {asset_type}:
        raise ValueError(f"{symbol}: Yahoo asset_type 不一致: {sorted(returned_asset_types)}")
    if returned_symbols != {symbol}:
        raise ValueError(f"{symbol}: Yahoo symbol 不一致: {sorted(returned_symbols)}")

    normalized = daily.with_columns(pl.col("time").dt.date().alias("_trade_date"))
    if normalized.select(["symbol", "_trade_date"]).n_unique() != normalized.height:
        raise ValueError(f"{symbol}: Yahoo 返回重复交易日")

    available_dates = set(normalized.get_column("_trade_date").to_list())
    missing_dates = sorted(expected_dates - available_dates)
    if missing_dates:
        preview = ",".join(current.isoformat() for current in missing_dates[:5])
        raise ValueError(f"{symbol}: Yahoo 缺少 {len(missing_dates)} 个数据库交易日，示例={preview}")

    selected = normalized.filter(pl.col("_trade_date").is_in(sorted(expected_dates))).drop("_trade_date")
    factors = selected.get_column("adj_factor").cast(pl.Float64).to_list()
    if any(value is None or not math.isfinite(value) or value <= 0 for value in factors):
        raise ValueError(f"{symbol}: adj_factor 存在空值、非有限值或非正值")
    if selected.height != len(expected_dates):
        raise ValueError(f"{symbol}: 复权记录数不一致 expected={len(expected_dates)}, actual={selected.height}")

    return selected.select(["time", "asset_type", "symbol", "adj_factor", "pct_change"])


def build_adjustment_backfill(
    engine,
    *,
    asset_type: str,
    lookback_years: int,
) -> tuple[BackfillWindow, pl.DataFrame]:
    symbols = resolve_etl_symbols(asset_type)
    window, expected = _load_expected_dates(engine, asset_type, symbols, lookback_years)
    loader = get_market_data_loader(asset_type)
    fetch_start = window.start - timedelta(days=10)

    logger.info(
        "Yahoo 复权回填准备 | asset_type={} | symbols={} | window={}~{}",
        asset_type,
        len(symbols),
        window.start,
        window.end,
    )
    frames: list[pl.DataFrame] = []
    for index, symbol in enumerate(symbols, start=1):
        daily = loader.fetch_daily_by_symbol(
            asset_type,
            symbol,
            fetch_start.strftime("%Y%m%d"),
            window.end.strftime("%Y%m%d"),
        )
        selected = _validate_and_select(
            daily,
            asset_type=asset_type,
            symbol=symbol,
            expected_dates=expected[symbol],
        )
        frames.append(selected)
        latest_factor = selected.sort("time").get_column("adj_factor")[-1]
        logger.info(
            "[{}/{}] {} 校验完成 | rows={} | latest_factor={:.6f}",
            index,
            len(symbols),
            symbol,
            selected.height,
            latest_factor,
        )

    combined = pl.concat(frames, how="vertical").sort(["symbol", "time"])
    return window, combined


def main(asset_type: str, lookback_years: int, dry_run: bool) -> int:
    engine = get_engine()
    window, adjustments = build_adjustment_backfill(
        engine,
        asset_type=asset_type,
        lookback_years=lookback_years,
    )
    logger.success(
        "全部 Yahoo 数据校验通过 | asset_type={} | rows={} | window={}~{}",
        asset_type,
        adjustments.height,
        window.start,
        window.end,
    )
    if dry_run:
        logger.success("DRY RUN 完成，数据库未写入")
        return 0

    written = update_daily_adjustments(adjustments, engine=engine)
    logger.success("复权字段更新完成 | rows={}", written)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill Yahoo Adj Close / Close factors")
    parser.add_argument("--asset-type", default="etf_US", help="目标 ETL asset_type（默认 etf_US）")
    parser.add_argument("--lookback-years", type=int, default=10, help="回填年数（默认 10）")
    parser.add_argument("--dry-run", action="store_true", help="只下载和校验，不更新数据库")
    args = parser.parse_args()
    raise SystemExit(main(args.asset_type, args.lookback_years, args.dry_run))

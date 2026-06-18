"""
Daily incremental factor IC pipeline.

Flow:
  1. Resolve enabled asset types and target factors
  2. Find daily IC dates that are fully computable and still missing
  3. Compute daily IC / RankIC for lags and upsert to analytics.factor_daily_ic
  4. Rebuild 126-day rolling summaries for affected as_of dates
  5. Update sync status for both raw IC and summary layers
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone

import polars as pl
from loguru import logger

sys.path.insert(0, "/app")

from app.analytics import (
    CALC_VERSION,
    DEFAULT_LAGS,
    DEFAULT_WINDOW_DAYS,
    FACTOR_DAILY_IC_DATA_TYPE,
    FACTOR_IC_SUMMARY_DATA_TYPE,
    compute_daily_ic,
    get_complete_ic_dates,
    load_daily_ic_rows,
    load_factors,
    load_returns,
    summarize_ic_window,
    update_ic_sync_status,
    upsert_factor_daily_ic,
    upsert_factor_ic_summary,
)
from app.factors.pipeline.loader import get_market_dates
from app.factors.registry import FACTOR_REGISTRY, resolve_factors
from app.services.asset_universe import list_asset_types
from app.utils.db import get_engine


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


def _window_start(yyyymmdd: str, trading_days: int) -> str:
    dt = datetime.strptime(yyyymmdd, "%Y%m%d")
    return _yyyymmdd(dt - timedelta(days=trading_days * 2 + 10))


def _summarize_target_dates(missing: list[str], market_dates: list[str], window_days: int) -> list[str]:
    if not missing:
        return []
    first_missing = missing[0]
    anchor_index = next((idx for idx, value in enumerate(market_dates) if value == first_missing), None)
    if anchor_index is None:
        return missing
    start_index = max(0, anchor_index - window_days + 1)
    return market_dates[start_index : market_dates.index(missing[-1]) + 1]


def _run_for_asset_type(
    engine,
    *,
    asset_type: str,
    lookback_days: int,
    lags: list[int],
    window_days: int,
    force_update: bool,
    factor_names: list[str] | None,
) -> tuple[int, int, list[str]]:
    today = datetime.now(timezone.utc)
    end_str = _yyyymmdd(today)
    start_str = _yyyymmdd(today - timedelta(days=lookback_days))
    factors = resolve_factors(factor_names, asset_type=asset_type)
    factor_name_list = [factor.name for factor in factors]
    factor_min_cross_section = {factor.name: factor.ic_min_cross_section for factor in factors}
    max_lag = max(lags)

    market_dates = get_market_dates(engine, start_str, end_str, asset_type)
    if len(market_dates) <= max_lag:
        logger.warning("[{}] market.daily 交易日不足以计算 IC，跳过", asset_type)
        update_ic_sync_status(
            engine,
            data_type=FACTOR_DAILY_IC_DATA_TYPE,
            asset_type=asset_type,
            status="ok",
            last_date=None,
        )
        update_ic_sync_status(
            engine,
            data_type=FACTOR_IC_SUMMARY_DATA_TYPE,
            asset_type=asset_type,
            status="ok",
            last_date=None,
        )
        return 0, 0, []

    target_dates = market_dates[: len(market_dates) - max_lag]
    if not target_dates:
        logger.warning("[{}] 无可计算的 IC 目标日期，跳过", asset_type)
        return 0, 0, []

    if force_update:
        missing = target_dates
    else:
        complete_dates = get_complete_ic_dates(
            engine,
            asset_type=asset_type,
            factor_names=factor_name_list,
            lags=lags,
            start=_iso(target_dates[0]),
            end=_iso(target_dates[-1]),
        )
        missing = [trade_date for trade_date in target_dates if trade_date not in complete_dates]

    logger.info(
        "[{}] IC 流水线 | dates={} ~ {} | total={} | missing={} | lags={}{}",
        asset_type,
        target_dates[0],
        target_dates[-1],
        len(target_dates),
        len(missing),
        lags,
        " | FORCE" if force_update else "",
    )

    if not missing:
        update_ic_sync_status(
            engine,
            data_type=FACTOR_DAILY_IC_DATA_TYPE,
            asset_type=asset_type,
            status="ok",
            last_date=target_dates[-1],
        )
        update_ic_sync_status(
            engine,
            data_type=FACTOR_IC_SUMMARY_DATA_TYPE,
            asset_type=asset_type,
            status="ok",
            last_date=target_dates[-1],
        )
        logger.success("[{}] factor IC 数据完整，无需补全", asset_type)
        return 0, 0, []

    factor_start = _iso(missing[0])
    factor_end = _iso(target_dates[-1])
    df_factors = load_factors(engine, factor_start, factor_end, asset_type, factor_name_list)
    df_ret = load_returns(engine, factor_start, factor_end, asset_type, max_lag)
    if df_factors.is_empty() or df_ret.is_empty():
        error = "IC 输入数据为空，请先确认 daily_factors 与 market.daily 完整"
        update_ic_sync_status(
            engine,
            data_type=FACTOR_DAILY_IC_DATA_TYPE,
            asset_type=asset_type,
            status="error",
            last_date=missing[-1],
            error_msg=error,
        )
        update_ic_sync_status(
            engine,
            data_type=FACTOR_IC_SUMMARY_DATA_TYPE,
            asset_type=asset_type,
            status="error",
            last_date=missing[-1],
            error_msg=error,
        )
        return 0, 0, [error]

    target_date_set = set(missing)
    daily_frames: list[pl.DataFrame] = []
    for lag in lags:
        daily_ic = compute_daily_ic(df_factors, df_ret, lag, factor_min_cross_section)
        if daily_ic.is_empty():
            continue
        filtered = (
            daily_ic.filter(pl.col("time").dt.strftime("%Y%m%d").is_in(target_date_set))
            .with_columns([
                pl.lit(asset_type).alias("asset_type"),
                pl.lit(CALC_VERSION).alias("calc_version"),
            ])
            .select(["time", "asset_type", "factor_name", "lag", "ic", "rank_ic", "n_stocks", "calc_version"])
        )
        if not filtered.is_empty():
            daily_frames.append(filtered)

    daily_rows = upsert_factor_daily_ic(engine, pl.concat(daily_frames) if daily_frames else pl.DataFrame())

    summary_target_dates = _summarize_target_dates(missing, target_dates, window_days)
    summary_start = _window_start(summary_target_dates[0], window_days) if summary_target_dates else factor_start
    loaded_daily_ic = load_daily_ic_rows(
        engine,
        summary_start,
        _iso(summary_target_dates[-1]) if summary_target_dates else factor_end,
        asset_type,
        lags=lags,
        factor_names=factor_name_list,
    )
    summary_rows = 0
    if not loaded_daily_ic.is_empty() and summary_target_dates:
        summary_df = summarize_ic_window(
            loaded_daily_ic,
            asset_type=asset_type,
            as_of_dates=[datetime.strptime(value, "%Y%m%d").date() for value in summary_target_dates],
            window_days=window_days,
        )
        summary_rows = upsert_factor_ic_summary(engine, summary_df)

    update_ic_sync_status(
        engine,
        data_type=FACTOR_DAILY_IC_DATA_TYPE,
        asset_type=asset_type,
        status="ok",
        last_date=missing[-1],
    )
    update_ic_sync_status(
        engine,
        data_type=FACTOR_IC_SUMMARY_DATA_TYPE,
        asset_type=asset_type,
        status="ok",
        last_date=summary_target_dates[-1] if summary_target_dates else missing[-1],
    )
    logger.success(
        "[{}] factor IC 完成 | daily_rows={} | summary_rows={} | missing_dates={}",
        asset_type,
        daily_rows,
        summary_rows,
        len(missing),
    )
    return daily_rows, summary_rows, []


def main(
    lookback_days: int,
    lags: list[int],
    window_days: int,
    force_update: bool = False,
    factor_names: list[str] | None = None,
    asset_types: list[str] | None = None,
) -> None:
    engine = get_engine()

    try:
        resolved_asset_types = _resolve_asset_types(asset_types)
        for asset_type in resolved_asset_types:
            resolve_factors(factor_names, asset_type=asset_type)
    except ValueError as exc:
        logger.error(str(exc))
        sys.exit(1)

    total_daily_rows = 0
    total_summary_rows = 0
    aggregated_errors: list[str] = []

    for asset_type in resolved_asset_types:
        try:
            daily_rows, summary_rows, errors = _run_for_asset_type(
                engine,
                asset_type=asset_type,
                lookback_days=lookback_days,
                lags=lags,
                window_days=window_days,
                force_update=force_update,
                factor_names=factor_names,
            )
            total_daily_rows += daily_rows
            total_summary_rows += summary_rows
            aggregated_errors.extend(f"[{asset_type}] {error}" for error in errors)
        except Exception as exc:
            logger.error("[{}] factor IC 流水线失败 — {}", asset_type, exc)
            update_ic_sync_status(
                engine,
                data_type=FACTOR_DAILY_IC_DATA_TYPE,
                asset_type=asset_type,
                status="error",
                last_date=None,
                error_msg=str(exc),
            )
            update_ic_sync_status(
                engine,
                data_type=FACTOR_IC_SUMMARY_DATA_TYPE,
                asset_type=asset_type,
                status="error",
                last_date=None,
                error_msg=str(exc),
            )
            aggregated_errors.append(f"[{asset_type}] {exc}")

    if aggregated_errors:
        logger.warning(
            "factor IC 流水线完成（含错误）| asset_types={} | daily_rows={} | summary_rows={}",
            resolved_asset_types,
            total_daily_rows,
            total_summary_rows,
        )
        sys.exit(1)

    logger.success(
        "factor IC 流水线完成 | asset_types={} | daily_rows={} | summary_rows={}",
        resolved_asset_types,
        total_daily_rows,
        total_summary_rows,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily incremental factor IC pipeline")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=365,
        help="回溯天数（默认 365）",
    )
    parser.add_argument(
        "--lags",
        default="1,2,5,10,20",
        help=f"逗号分隔的 forward lag，默认 {','.join(str(value) for value in DEFAULT_LAGS)}",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=DEFAULT_WINDOW_DAYS,
        help=f"滚动汇总窗口交易日数，默认 {DEFAULT_WINDOW_DAYS}",
    )
    parser.add_argument(
        "--force-update",
        action="store_true",
        help="强制重算窗口内所有目标日期",
    )
    parser.add_argument(
        "--factors",
        type=str,
        default=None,
        help=f"逗号分隔的因子名称，默认全部。可选: {','.join(FACTOR_REGISTRY)}",
    )
    parser.add_argument(
        "--asset-type",
        action="append",
        dest="asset_types",
        default=[],
        help="可重复传入，仅计算指定 asset_type；不传则遍历全部 enabled asset_type",
    )
    args = parser.parse_args()
    factor_names = [factor.strip() for factor in args.factors.split(",")] if args.factors else None
    lags = [int(value.strip()) for value in args.lags.split(",") if value.strip()]
    main(
        args.lookback_days,
        lags,
        args.window_days,
        args.force_update,
        factor_names,
        args.asset_types or None,
    )

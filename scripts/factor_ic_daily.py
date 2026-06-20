"""
Daily incremental factor validation pipeline.

Flow:
  1. Resolve enabled asset types and target factors
  2. Find missing IC / quantile / top-k dates that are fully computable
  3. Compute daily validations for lags and upsert raw analytics tables
  4. Rebuild 126-day rolling summaries for affected as_of dates
  5. Update sync status for all validation layers
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
    DEFAULT_QUANTILE_GROUPS,
    DEFAULT_TOP_KS,
    DEFAULT_WINDOW_DAYS,
    FACTOR_DAILY_IC_DATA_TYPE,
    FACTOR_DAILY_QUANTILE_RETURN_DATA_TYPE,
    FACTOR_DAILY_TOPK_RETURN_DATA_TYPE,
    FACTOR_IC_SUMMARY_DATA_TYPE,
    FACTOR_QUANTILE_SUMMARY_DATA_TYPE,
    FACTOR_TOPK_SUMMARY_DATA_TYPE,
    compute_daily_ic,
    compute_daily_quantile_return,
    compute_daily_topk_return,
    get_complete_ic_dates,
    get_complete_quantile_dates,
    get_complete_topk_dates,
    load_daily_ic_rows,
    load_daily_quantile_rows,
    load_daily_topk_rows,
    load_factors,
    load_returns,
    summarize_ic_window,
    summarize_quantile_window,
    summarize_topk_window,
    update_ic_sync_status,
    upsert_factor_daily_ic,
    upsert_factor_daily_quantile_return,
    upsert_factor_daily_topk_return,
    upsert_factor_ic_summary,
    upsert_factor_quantile_summary,
    upsert_factor_topk_summary,
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


def _update_status_group(
    engine,
    *,
    asset_type: str,
    status: str,
    error_msg: str | None,
    ic_last_date: str | None,
    ic_summary_last_date: str | None,
    quantile_last_date: str | None,
    quantile_summary_last_date: str | None,
    topk_last_date: str | None,
    topk_summary_last_date: str | None,
) -> None:
    mappings = [
        (FACTOR_DAILY_IC_DATA_TYPE, ic_last_date),
        (FACTOR_IC_SUMMARY_DATA_TYPE, ic_summary_last_date),
        (FACTOR_DAILY_QUANTILE_RETURN_DATA_TYPE, quantile_last_date),
        (FACTOR_QUANTILE_SUMMARY_DATA_TYPE, quantile_summary_last_date),
        (FACTOR_DAILY_TOPK_RETURN_DATA_TYPE, topk_last_date),
        (FACTOR_TOPK_SUMMARY_DATA_TYPE, topk_summary_last_date),
    ]
    for data_type, last_date in mappings:
        update_ic_sync_status(
            engine,
            data_type=data_type,
            asset_type=asset_type,
            status=status,
            last_date=last_date,
            error_msg=error_msg,
        )


def _run_for_asset_type(
    engine,
    *,
    asset_type: str,
    lookback_days: int,
    lags: list[int],
    quantile_groups: int,
    top_ks: list[int],
    window_days: int,
    force_update: bool,
    factor_names: list[str] | None,
) -> tuple[dict[str, int], list[str]]:
    today = datetime.now(timezone.utc)
    end_str = _yyyymmdd(today)
    start_str = _yyyymmdd(today - timedelta(days=lookback_days))
    factors = resolve_factors(factor_names, asset_type=asset_type)
    factor_name_list = [factor.name for factor in factors]
    factor_min_cross_section = {factor.name: factor.ic_min_cross_section for factor in factors}
    max_lag = max(lags)

    market_dates = get_market_dates(engine, start_str, end_str, asset_type)
    if len(market_dates) <= max_lag:
        logger.warning("[{}] market.daily 交易日不足以计算因子有效性，跳过", asset_type)
        _update_status_group(
            engine,
            asset_type=asset_type,
            status="ok",
            error_msg=None,
            ic_last_date=None,
            ic_summary_last_date=None,
            quantile_last_date=None,
            quantile_summary_last_date=None,
            topk_last_date=None,
            topk_summary_last_date=None,
        )
        return {
            "ic_daily_rows": 0,
            "ic_summary_rows": 0,
            "quantile_daily_rows": 0,
            "quantile_summary_rows": 0,
            "topk_daily_rows": 0,
            "topk_summary_rows": 0,
        }, []

    target_dates = market_dates[: len(market_dates) - max_lag]
    if not target_dates:
        logger.warning("[{}] 无可计算的目标日期，跳过", asset_type)
        return {
            "ic_daily_rows": 0,
            "ic_summary_rows": 0,
            "quantile_daily_rows": 0,
            "quantile_summary_rows": 0,
            "topk_daily_rows": 0,
            "topk_summary_rows": 0,
        }, []

    if force_update:
        missing_ic = target_dates
        missing_quantile = target_dates
        missing_topk = target_dates
    else:
        complete_ic = get_complete_ic_dates(
            engine,
            asset_type=asset_type,
            factor_names=factor_name_list,
            lags=lags,
            start=_iso(target_dates[0]),
            end=_iso(target_dates[-1]),
        )
        complete_quantile = get_complete_quantile_dates(
            engine,
            asset_type=asset_type,
            factor_names=factor_name_list,
            lags=lags,
            quantile_n=quantile_groups,
            start=_iso(target_dates[0]),
            end=_iso(target_dates[-1]),
        )
        complete_topk = get_complete_topk_dates(
            engine,
            asset_type=asset_type,
            factor_names=factor_name_list,
            lags=lags,
            top_ks=top_ks,
            start=_iso(target_dates[0]),
            end=_iso(target_dates[-1]),
        )
        missing_ic = [trade_date for trade_date in target_dates if trade_date not in complete_ic]
        missing_quantile = [trade_date for trade_date in target_dates if trade_date not in complete_quantile]
        missing_topk = [trade_date for trade_date in target_dates if trade_date not in complete_topk]

    logger.info(
        "[{}] validation 流水线 | dates={} ~ {} | total={} | missing_ic={} | missing_quantile={} | missing_topk={} | lags={}{}",
        asset_type,
        target_dates[0],
        target_dates[-1],
        len(target_dates),
        len(missing_ic),
        len(missing_quantile),
        len(missing_topk),
        lags,
        " | FORCE" if force_update else "",
    )

    if not missing_ic and not missing_quantile and not missing_topk:
        _update_status_group(
            engine,
            asset_type=asset_type,
            status="ok",
            error_msg=None,
            ic_last_date=target_dates[-1],
            ic_summary_last_date=target_dates[-1],
            quantile_last_date=target_dates[-1],
            quantile_summary_last_date=target_dates[-1],
            topk_last_date=target_dates[-1],
            topk_summary_last_date=target_dates[-1],
        )
        logger.success("[{}] 因子有效性数据完整，无需补全", asset_type)
        return {
            "ic_daily_rows": 0,
            "ic_summary_rows": 0,
            "quantile_daily_rows": 0,
            "quantile_summary_rows": 0,
            "topk_daily_rows": 0,
            "topk_summary_rows": 0,
        }, []

    load_dates = sorted(set(missing_ic) | set(missing_quantile) | set(missing_topk))
    factor_start = _iso(load_dates[0])
    factor_end = _iso(target_dates[-1])
    df_factors = load_factors(engine, factor_start, factor_end, asset_type, factor_name_list)
    df_ret = load_returns(engine, factor_start, factor_end, asset_type, max_lag)
    if df_factors.is_empty() or df_ret.is_empty():
        error = "因子有效性输入数据为空，请先确认 daily_factors 与 market.daily 完整"
        _update_status_group(
            engine,
            asset_type=asset_type,
            status="error",
            error_msg=error,
            ic_last_date=load_dates[-1],
            ic_summary_last_date=load_dates[-1],
            quantile_last_date=load_dates[-1],
            quantile_summary_last_date=load_dates[-1],
            topk_last_date=load_dates[-1],
            topk_summary_last_date=load_dates[-1],
        )
        return {
            "ic_daily_rows": 0,
            "ic_summary_rows": 0,
            "quantile_daily_rows": 0,
            "quantile_summary_rows": 0,
            "topk_daily_rows": 0,
            "topk_summary_rows": 0,
        }, [error]

    ic_date_set = set(missing_ic)
    quantile_date_set = set(missing_quantile)
    topk_date_set = set(missing_topk)

    ic_frames: list[pl.DataFrame] = []
    quantile_frames: list[pl.DataFrame] = []
    topk_frames: list[pl.DataFrame] = []

    for lag in lags:
        if ic_date_set:
            daily_ic = compute_daily_ic(df_factors, df_ret, lag, factor_min_cross_section)
            if not daily_ic.is_empty():
                current_ic = (
                    daily_ic.filter(pl.col("time").dt.strftime("%Y%m%d").is_in(ic_date_set))
                    .with_columns([
                        pl.lit(asset_type).alias("asset_type"),
                        pl.lit(CALC_VERSION).alias("calc_version"),
                    ])
                    .select(["time", "asset_type", "factor_name", "lag", "ic", "rank_ic", "n_stocks", "calc_version"])
                )
                if not current_ic.is_empty():
                    ic_frames.append(current_ic)

        if quantile_date_set:
            daily_quantile = compute_daily_quantile_return(df_factors, df_ret, lag, quantile_groups, factor_min_cross_section)
            if not daily_quantile.is_empty():
                current_quantile = (
                    daily_quantile.filter(pl.col("time").dt.strftime("%Y%m%d").is_in(quantile_date_set))
                    .with_columns([
                        pl.lit(asset_type).alias("asset_type"),
                        pl.lit(CALC_VERSION).alias("calc_version"),
                    ])
                    .select([
                        "time",
                        "asset_type",
                        "factor_name",
                        "lag",
                        "quantile_n",
                        "quantile_id",
                        "avg_fwd_ret",
                        "n_stocks",
                        "calc_version",
                    ])
                )
                if not current_quantile.is_empty():
                    quantile_frames.append(current_quantile)

        if topk_date_set:
            daily_topk = compute_daily_topk_return(df_factors, df_ret, lag, top_ks, factor_min_cross_section)
            if not daily_topk.is_empty():
                current_topk = (
                    daily_topk.filter(pl.col("time").dt.strftime("%Y%m%d").is_in(topk_date_set))
                    .with_columns([
                        pl.lit(asset_type).alias("asset_type"),
                        pl.lit(CALC_VERSION).alias("calc_version"),
                    ])
                    .select([
                        "time",
                        "asset_type",
                        "factor_name",
                        "lag",
                        "top_k",
                        "topk_ret",
                        "universe_ret",
                        "excess_ret",
                        "n_stocks",
                        "calc_version",
                    ])
                )
                if not current_topk.is_empty():
                    topk_frames.append(current_topk)

    ic_daily_rows = upsert_factor_daily_ic(engine, pl.concat(ic_frames) if ic_frames else pl.DataFrame())
    quantile_daily_rows = upsert_factor_daily_quantile_return(engine, pl.concat(quantile_frames) if quantile_frames else pl.DataFrame())
    topk_daily_rows = upsert_factor_daily_topk_return(engine, pl.concat(topk_frames) if topk_frames else pl.DataFrame())

    ic_summary_rows = 0
    quantile_summary_rows = 0
    topk_summary_rows = 0

    ic_summary_dates = _summarize_target_dates(missing_ic, target_dates, window_days)
    if ic_summary_dates:
        ic_summary_start = _window_start(ic_summary_dates[0], window_days)
        loaded_daily_ic = load_daily_ic_rows(
            engine,
            ic_summary_start,
            _iso(ic_summary_dates[-1]),
            asset_type,
            lags=lags,
            factor_names=factor_name_list,
        )
        if not loaded_daily_ic.is_empty():
            ic_summary_rows = upsert_factor_ic_summary(
                engine,
                summarize_ic_window(
                    loaded_daily_ic,
                    asset_type=asset_type,
                    as_of_dates=[datetime.strptime(value, "%Y%m%d").date() for value in ic_summary_dates],
                    window_days=window_days,
                ),
            )

    quantile_summary_dates = _summarize_target_dates(missing_quantile, target_dates, window_days)
    if quantile_summary_dates:
        quantile_summary_start = _window_start(quantile_summary_dates[0], window_days)
        loaded_daily_quantile = load_daily_quantile_rows(
            engine,
            quantile_summary_start,
            _iso(quantile_summary_dates[-1]),
            asset_type,
            lags=lags,
            factor_names=factor_name_list,
            quantile_n=quantile_groups,
        )
        if not loaded_daily_quantile.is_empty():
            quantile_summary_rows = upsert_factor_quantile_summary(
                engine,
                summarize_quantile_window(
                    loaded_daily_quantile,
                    asset_type=asset_type,
                    as_of_dates=[datetime.strptime(value, "%Y%m%d").date() for value in quantile_summary_dates],
                    window_days=window_days,
                ),
            )

    topk_summary_dates = _summarize_target_dates(missing_topk, target_dates, window_days)
    if topk_summary_dates:
        topk_summary_start = _window_start(topk_summary_dates[0], window_days)
        loaded_daily_topk = load_daily_topk_rows(
            engine,
            topk_summary_start,
            _iso(topk_summary_dates[-1]),
            asset_type,
            lags=lags,
            factor_names=factor_name_list,
            top_ks=top_ks,
        )
        if not loaded_daily_topk.is_empty():
            topk_summary_rows = upsert_factor_topk_summary(
                engine,
                summarize_topk_window(
                    loaded_daily_topk,
                    asset_type=asset_type,
                    as_of_dates=[datetime.strptime(value, "%Y%m%d").date() for value in topk_summary_dates],
                    window_days=window_days,
                ),
            )

    _update_status_group(
        engine,
        asset_type=asset_type,
        status="ok",
        error_msg=None,
        ic_last_date=missing_ic[-1] if missing_ic else target_dates[-1],
        ic_summary_last_date=ic_summary_dates[-1] if ic_summary_dates else (missing_ic[-1] if missing_ic else target_dates[-1]),
        quantile_last_date=missing_quantile[-1] if missing_quantile else target_dates[-1],
        quantile_summary_last_date=quantile_summary_dates[-1] if quantile_summary_dates else (missing_quantile[-1] if missing_quantile else target_dates[-1]),
        topk_last_date=missing_topk[-1] if missing_topk else target_dates[-1],
        topk_summary_last_date=topk_summary_dates[-1] if topk_summary_dates else (missing_topk[-1] if missing_topk else target_dates[-1]),
    )

    counts = {
        "ic_daily_rows": ic_daily_rows,
        "ic_summary_rows": ic_summary_rows,
        "quantile_daily_rows": quantile_daily_rows,
        "quantile_summary_rows": quantile_summary_rows,
        "topk_daily_rows": topk_daily_rows,
        "topk_summary_rows": topk_summary_rows,
    }
    logger.success(
        "[{}] 因子有效性完成 | ic=({}/{}) | quantile=({}/{}) | topk=({}/{})",
        asset_type,
        ic_daily_rows,
        ic_summary_rows,
        quantile_daily_rows,
        quantile_summary_rows,
        topk_daily_rows,
        topk_summary_rows,
    )
    return counts, []


def main(
    lookback_days: int,
    lags: list[int],
    quantile_groups: int,
    top_ks: list[int],
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

    totals = {
        "ic_daily_rows": 0,
        "ic_summary_rows": 0,
        "quantile_daily_rows": 0,
        "quantile_summary_rows": 0,
        "topk_daily_rows": 0,
        "topk_summary_rows": 0,
    }
    aggregated_errors: list[str] = []

    for asset_type in resolved_asset_types:
        try:
            counts, errors = _run_for_asset_type(
                engine,
                asset_type=asset_type,
                lookback_days=lookback_days,
                lags=lags,
                quantile_groups=quantile_groups,
                top_ks=top_ks,
                window_days=window_days,
                force_update=force_update,
                factor_names=factor_names,
            )
            for key, value in counts.items():
                totals[key] += value
            aggregated_errors.extend(f"[{asset_type}] {error}" for error in errors)
        except Exception as exc:
            logger.error("[{}] 因子有效性流水线失败 — {}", asset_type, exc)
            _update_status_group(
                engine,
                asset_type=asset_type,
                status="error",
                error_msg=str(exc),
                ic_last_date=None,
                ic_summary_last_date=None,
                quantile_last_date=None,
                quantile_summary_last_date=None,
                topk_last_date=None,
                topk_summary_last_date=None,
            )
            aggregated_errors.append(f"[{asset_type}] {exc}")

    if aggregated_errors:
        logger.warning(
            "因子有效性流水线完成（含错误）| asset_types={} | counts={}",
            resolved_asset_types,
            totals,
        )
        sys.exit(1)

    logger.success(
        "因子有效性流水线完成 | asset_types={} | counts={}",
        resolved_asset_types,
        totals,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily incremental factor validation pipeline")
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
        "--quantile-groups",
        type=int,
        default=DEFAULT_QUANTILE_GROUPS,
        help=f"分组收益组数，默认 {DEFAULT_QUANTILE_GROUPS}",
    )
    parser.add_argument(
        "--topk",
        default="5,10,20",
        help=f"逗号分隔的 Top-K 集合，默认 {','.join(str(value) for value in DEFAULT_TOP_KS)}",
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
    top_ks = [int(value.strip()) for value in args.topk.split(",") if value.strip()]
    main(
        args.lookback_days,
        lags,
        args.quantile_groups,
        top_ks,
        args.window_days,
        args.force_update,
        factor_names,
        args.asset_types or None,
    )

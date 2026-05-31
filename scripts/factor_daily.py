"""
每日增量因子计算：gap 检测 + warm-up 窗口 + 写入 factors.daily_factors

流程：
  1. 遍历启用的 asset_type
  2. 读取该 asset_type 的 pipeline universe
  3. 对比 factors.daily_factors 找出缺失日期（gap 检测）
  4. 对每个 symbol，加载 (gap_start - WARMUP_DAYS) 至今的行情
  5. 计算所有因子，过滤只保留缺失日期，upsert 写入
  6. 更新 meta.sync_status
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/app")

from loguru import logger

from app.factors.pipeline.loader import get_market_dates
from app.factors.pipeline.runner import run_time_series_factors
from app.factors.pipeline.validator import (
    get_complete_factor_dates,
    get_missing_factor_symbols,
    validate_factor_completeness,
)
from app.factors.pipeline.writer import update_sync_status
from app.factors.registry import FACTOR_REGISTRY, max_warmup_days, required_market_fields, resolve_factors
from app.services.asset_universe import list_asset_types, resolve_pipeline_symbols
from app.utils.db import get_engine

RATE_LIMIT = 0.05


def _yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


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


def _run_for_asset_type(
    engine,
    asset_type: str,
    lookback_days: int,
    force_update: bool,
    factor_names: list[str] | None,
) -> tuple[int, list[str]]:
    today = datetime.now(timezone.utc)
    end_str = _yyyymmdd(today)
    start_str = _yyyymmdd(today - timedelta(days=lookback_days))

    factors = resolve_factors(factor_names, asset_type=asset_type)
    symbols = resolve_pipeline_symbols(asset_type)
    market_dates = get_market_dates(engine, start_str, end_str, asset_type)

    logger.info(
        "因子流水线 | asset_type={} | lookback={}d | {} ~ {}{}{}",
        asset_type,
        lookback_days,
        start_str,
        end_str,
        f" | factors={[f.name for f in factors]}" if factor_names else "",
        " | FORCE" if force_update else "",
    )
    logger.info("[{}] pipeline universe 标的数: {}", asset_type, len(symbols))

    if not market_dates:
        logger.warning("[{}] market.daily 无数据，跳过", asset_type)
        return 0, []
    logger.info("[{}] market.daily 交易日: {} 个 ({} ~ {})", asset_type, len(market_dates), market_dates[0], market_dates[-1])

    if force_update:
        missing = market_dates
        logger.info("[{}] 强制模式：重新计算全部 {} 个交易日", asset_type, len(missing))
    else:
        complete_dates = get_complete_factor_dates(engine, asset_type, market_dates, symbols, factors)
        missing = [d for d in market_dates if d not in complete_dates]
        logger.info("[{}] 因子完整交易日 {} 个，缺失 {}", asset_type, len(complete_dates), len(missing))

    if not missing:
        logger.success("[{}] 因子数据完整，无需补全", asset_type)
        update_sync_status(engine, asset_type, "ok", market_dates[-1])
        return 0, []

    gap_start = missing[0]
    warmup_days = max_warmup_days(factors)
    warmup_start = _yyyymmdd(datetime.strptime(gap_start, "%Y%m%d") - timedelta(days=warmup_days))
    market_fields = required_market_fields(factors)

    logger.info(
        "[{}] 缺失日期: {} ~ {}，行情加载窗口: {} ~ {} | warmup={}d",
        asset_type,
        missing[0],
        missing[-1],
        warmup_start,
        end_str,
        warmup_days,
    )

    run_result = run_time_series_factors(
        engine=engine,
        asset_type=asset_type,
        symbols=symbols,
        factors=factors,
        warmup_start=warmup_start,
        end_str=end_str,
        target_dates=set(missing),
        market_fields=market_fields,
        rate_limit=RATE_LIMIT,
    )
    total_written = run_result.total_written
    errors = list(run_result.errors)

    validation = validate_factor_completeness(engine, asset_type, missing, symbols, factors)
    if not validation.ok:
        preview_parts: list[str] = []
        for issue in validation.issues[:5]:
            missing_symbols = get_missing_factor_symbols(
                engine,
                asset_type,
                issue.trade_date,
                symbols,
                issue.factor_name,
            )
            symbol_preview = f" missing={missing_symbols}" if missing_symbols else ""
            preview_parts.append(
                f"{issue.trade_date}:{issue.factor_name}({issue.actual_count}/{issue.expected_count}){symbol_preview}"
            )
        preview = "; ".join(preview_parts)
        errors.append(
            f"factor coverage incomplete: {len(validation.issues)} issues" + (f" | sample: {preview}" if preview else "")
        )

    if errors:
        update_sync_status(engine, asset_type, "error", missing[-1], "; ".join(errors[:5]))
    else:
        update_sync_status(engine, asset_type, "ok", missing[-1])
        logger.success("[{}] 因子流水线完成 | 补齐 {} 个交易日 | 共写入 {} 条", asset_type, len(missing), total_written)

    return total_written, errors


def main(
    lookback_days: int,
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

    total_written = 0
    aggregated_errors: list[str] = []

    for asset_type in resolved_asset_types:
        try:
            written, errors = _run_for_asset_type(engine, asset_type, lookback_days, force_update, factor_names)
            total_written += written
            aggregated_errors.extend(f"[{asset_type}] {error}" for error in errors)
        except Exception as exc:
            logger.error("[{}] 因子流水线失败 — {}", asset_type, exc)
            aggregated_errors.append(f"[{asset_type}] {exc}")

    if aggregated_errors:
        logger.warning("因子流水线完成（含错误）| asset_types={} | 写入 {} 条", resolved_asset_types, total_written)
        sys.exit(1)

    logger.success("因子流水线完成 | asset_types={} | 共写入 {} 条", resolved_asset_types, total_written)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily incremental factor computation")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=365,
        help="回溯天数（默认 365，覆盖 ETF/MA60 所需历史窗口）",
    )
    parser.add_argument(
        "--force-update",
        action="store_true",
        help="强制重新计算窗口内所有交易日，忽略已有数据",
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
    factor_names = [f.strip() for f in args.factors.split(",")] if args.factors else None
    main(args.lookback_days, args.force_update, factor_names, args.asset_types or None)

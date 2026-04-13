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
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/app")

from loguru import logger

from app.factors.pipeline.loader import get_all_symbols, get_market_dates
from app.factors.pipeline.runner import run_time_series_factors
from app.factors.pipeline.validator import get_complete_factor_dates, validate_factor_completeness
from app.factors.pipeline.writer import update_sync_status
from app.factors.registry import FACTOR_REGISTRY, max_warmup_days, required_market_fields, resolve_factors
from app.utils.db import get_engine

RATE_LIMIT  = 0.05        # 每只股票写入后短暂休眠（秒），避免 DB 连接堆积


def _yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


# ── 主流程 ────────────────────────────────────────────────────

def main(lookback_days: int, force_update: bool = False,
         factor_names: list[str] | None = None) -> None:
    engine = get_engine()

    today     = datetime.now(timezone.utc)
    end_str   = _yyyymmdd(today)
    start_str = _yyyymmdd(today - timedelta(days=lookback_days))

    # ── 解析因子列表 ─────────────────────────────────────────
    try:
        factors = resolve_factors(factor_names)
    except ValueError as exc:
        logger.error(str(exc))
        sys.exit(1)

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

    symbols = get_all_symbols(engine)
    logger.info(f"股票数量: {len(symbols)}")

    if force_update:
        missing = market_dates
        logger.info(f"强制模式：重新计算全部 {len(missing)} 个交易日")
    else:
        complete_dates = get_complete_factor_dates(engine, market_dates, symbols, factors)
        missing = [d for d in market_dates if d not in complete_dates]
        logger.info(f"因子完整交易日 {len(complete_dates)} 个，缺失 {len(missing)} 个")

    if not missing:
        logger.success("因子数据完整，无需补全")
        update_sync_status(engine, "ok", market_dates[-1])
        return

    # ── 2. 确定加载窗口（含 warm-up）───────────────────────
    gap_start    = missing[0]                                    # 最早缺失日
    warmup_days  = max_warmup_days(factors)
    warmup_start = _yyyymmdd(
        datetime.strptime(gap_start, "%Y%m%d") - timedelta(days=warmup_days)
    )
    logger.info(f"缺失日期: {missing[0]} ~ {missing[-1]}，"
                f"行情加载窗口: {warmup_start} ~ {end_str} | warmup={warmup_days}d")

    missing_set = set(missing)
    market_fields = required_market_fields(factors)

    # ── 4. 执行因子计算 ────────────────────────────────────────
    run_result = run_time_series_factors(
        engine=engine,
        symbols=symbols,
        factors=factors,
        warmup_start=warmup_start,
        end_str=end_str,
        target_dates=missing_set,
        market_fields=market_fields,
        rate_limit=RATE_LIMIT,
    )
    total_written = run_result.total_written
    errors = list(run_result.errors)

    # ── 5. 更新 sync_status ──────────────────────────────────
    validation = validate_factor_completeness(engine, missing, symbols, factors)
    if not validation.ok:
        preview = "; ".join(
            f"{issue.trade_date}:{issue.factor_name}({issue.actual_count}/{issue.expected_count})"
            for issue in validation.issues[:5]
        )
        errors.append(f"factor coverage incomplete: {len(validation.issues)} issues" + (f" | sample: {preview}" if preview else ""))

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

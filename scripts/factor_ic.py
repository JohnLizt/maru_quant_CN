"""
因子 IC / RankIC 统计分析

方法：截面 IC（cross-sectional）
  - 对每个交易日，计算全市场各股票因子值与第 lag 期后收益率的横截面相关系数
  - Pearson IC：线性相关；Spearman RankIC：排名相关（更鲁棒）
  - 汇总统计：均值 IC、IC_IR（= mean/std）、t 统计量、胜率

输出列说明：
  mean_ic      均值 IC（绝对值 > 0.02 有参考意义）
  ic_std       IC 标准差
  ic_ir        IC_IR = mean_ic / ic_std（> 0.5 较强，> 1.0 优秀）
  t_stat       t 统计量（检验 IC 均值是否显著异于 0）
  win_rate     IC > 0 的交易日占比
  mean_rank_ic 均值 RankIC
  rank_ic_ir   RankIC_IR
  n_days       有效交易日数

用法：
  python scripts/factor_ic.py                                   # 近 1 年，lags=1,2,5,10,20，全部因子
  python scripts/factor_ic.py --start 2023-01-01 --end 2024-12-31
  python scripts/factor_ic.py --factors ma_cross,rsi14
  python scripts/factor_ic.py --lags 1,2,5,10,20               # IC 衰减分析
  python scripts/factor_ic.py --lags 1,2,5 --output logs/ic_report.csv
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/app")

import polars as pl
from loguru import logger

from app.analytics.factor_ic import (
    compute_daily_ic,
    load_factors,
    load_returns,
    summarize_daily_ic,
)
from app.factors.registry import FACTOR_REGISTRY, resolve_factors
from app.utils.db import get_engine


# ── 打印 ──────────────────────────────────────────────────────

def print_full_table(summary: pl.DataFrame, lag: int) -> None:
    def _fmt_float(value: float | None, width: int, precision: int = 4) -> str:
        if value is None:
            return f"{'—':>{width}}"
        return f"{value:>{width}.{precision}f}"

    def _fmt_pct(value: float | None, width: int) -> str:
        if value is None:
            return f"{'—':>{width}}"
        return f"{value:>{width}.1%}"

    def _fmt_int(value: int | None, width: int) -> str:
        if value is None:
            return f"{'—':>{width}}"
        return f"{int(value):>{width}d}"

    print(f"\n── lag={lag}d {'─' * 60}")
    header = (
        f"  {'factor':<16} {'mean_RankIC':>12} {'RankIC_std':>11} {'RankIC_IR':>10} "
        f"{'mean_IC':>8} {'IC_std':>8} {'IC_IR':>7} "
        f"{'t_stat':>7} {'win_rate':>9} {'n_days':>7}"
    )
    print(header)
    print("  " + "─" * (len(header) - 2))
    for row in summary.iter_rows(named=True):
        print(
            f"  {row['factor_name']:<16} "
            f"{_fmt_float(row['mean_rank_ic'], 12)} "
            f"{_fmt_float(row['rank_ic_std'], 11)} "
            f"{_fmt_float(row['rank_ic_ir'], 10, 3)} "
            f"{_fmt_float(row['mean_ic'], 8)} "
            f"{_fmt_float(row['ic_std'], 8)} "
            f"{_fmt_float(row['ic_ir'], 7, 3)} "
            f"{_fmt_float(row['t_stat'], 7, 2)} "
            f"{_fmt_pct(row['win_rate'], 8)} "
            f"{_fmt_int(row['n_days'], 7)}"
        )


def print_decay_grid(all_summaries: list[pl.DataFrame], lags: list[int]) -> None:
    """多 lag 时打印 IC_IR 衰减矩阵"""
    combined = pl.concat(all_summaries)
    factors = combined["factor_name"].unique().sort().to_list()

    col_w = 9
    header = f"\n── IC_IR 衰减矩阵 {'─' * 40}\n"
    header += f"  {'factor':<18}" + "".join(f"{'lag='+str(l)+'d':>{col_w}}" for l in lags)
    print(header)
    print("  " + "─" * (18 + col_w * len(lags)))

    for factor in factors:
        row_str = f"  {factor:<18}"
        for lag in lags:
            match = combined.filter(
                (pl.col("factor_name") == factor) & (pl.col("lag") == lag)
            )
            val = match["ic_ir"][0] if len(match) > 0 else None
            row_str += f"{val:>{col_w}.3f}" if val is not None else f"{'—':>{col_w}}"
        print(row_str)
    print()


# ── 主流程 ────────────────────────────────────────────────────

def main(
    start: str,
    end: str,
    lags: list[int],
    asset_type: str,
    factor_names: list[str] | None,
    output: str | None,
) -> None:
    engine = get_engine()
    max_lag = max(lags)

    try:
        factors = resolve_factors(
            factor_names,
            asset_type=asset_type,
            production_only=factor_names is None,
        )
    except ValueError as exc:
        logger.error(str(exc))
        sys.exit(1)

    factor_min_cross_section = {factor.name: factor.ic_min_cross_section for factor in factors}

    logger.info(
        f"IC 分析 | asset_type={asset_type} | {start} ~ {end} | lags={lags}"
        + (f" | factors={[factor.name for factor in factors]}" if factor_names else " | 全部因子")
    )

    df_factors = load_factors(engine, start, end, asset_type, [factor.name for factor in factors])
    if df_factors.is_empty():
        logger.error(
            f"factors.daily_factors 无数据 | asset_type={asset_type}，请先运行 python scripts/factor_daily.py"
        )
        sys.exit(1)

    available = df_factors["factor_name"].unique().sort().to_list()
    logger.info(f"因子: {available} | 记录数: {len(df_factors)}")

    df_ret = load_returns(engine, start, end, asset_type, max_lag)
    if df_ret.is_empty():
        logger.error(f"market.daily 无数据 | asset_type={asset_type}")
        sys.exit(1)

    all_summaries: list[pl.DataFrame] = []
    for lag in lags:
        daily_ic = compute_daily_ic(df_factors, df_ret, lag, factor_min_cross_section)
        if daily_ic.is_empty():
            logger.warning(f"lag={lag}: IC 结果为空，跳过")
            continue
        summary = summarize_daily_ic(daily_ic, lag)
        all_summaries.append(summary)
        print_full_table(summary, lag)

    if len(lags) > 1 and all_summaries:
        print_decay_grid(all_summaries, lags)

    if output and all_summaries:
        combined = pl.concat(all_summaries)
        os.makedirs(os.path.dirname(output) or ".", exist_ok=True)
        combined.write_csv(output)
        logger.info(f"IC 报告已保存至 {output}")

    logger.success("IC 分析完成")


if __name__ == "__main__":
    _default_end   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    _default_start = (datetime.now(timezone.utc) - timedelta(days=365)).strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser(description="Cross-sectional factor IC analysis")
    parser.add_argument("--start", default=_default_start,
                        help=f"开始日期 YYYY-MM-DD（默认 {_default_start}）")
    parser.add_argument("--end",   default=_default_end,
                        help=f"结束日期 YYYY-MM-DD（默认 {_default_end}）")
    parser.add_argument("--lags",  default="1,2,5,10,20",
                        help="逗号分隔的 forward lag（默认 1,2,5,10,20）。例：--lags 1,2,5,10,20")
    parser.add_argument("--asset-type", default="stock_CN",
                        help="资产类型，默认 stock_CN，例如 etf_CN")
    parser.add_argument("--factors", default=None,
                        help="逗号分隔的因子名称，默认全部")
    parser.add_argument("--output", default=None,
                        help="可选：结果输出 CSV 路径，如 logs/ic_report.csv")
    args = parser.parse_args()

    lags         = [int(x.strip()) for x in args.lags.split(",")]
    factor_names = [f.strip() for f in args.factors.split(",")] if args.factors else None
    main(args.start, args.end, lags, args.asset_type, factor_names, args.output)

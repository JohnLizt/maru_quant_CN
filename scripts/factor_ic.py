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
from sqlalchemy import text

from app.factors.registry import FACTOR_REGISTRY, resolve_factors
from app.utils.db import get_engine


# ── DB 加载 ───────────────────────────────────────────────────

def load_factors(
    engine,
    start: str,
    end: str,
    asset_type: str,
    factor_names: list[str] | None = None,
) -> pl.DataFrame:
    """从 factors.daily_factors 加载长格式因子数据"""
    where_factor = ""
    params: dict = {"start": start, "end": end, "asset_type": asset_type}
    if factor_names:
        placeholders = ", ".join(f":f{i}" for i in range(len(factor_names)))
        where_factor = f"AND factor_name IN ({placeholders})"
        params |= {f"f{i}": n for i, n in enumerate(factor_names)}

    sql = text(f"""
        SELECT time, symbol, factor_name, factor_value
        FROM factors.daily_factors
        WHERE time >= :start AND time <= :end
          AND asset_type = :asset_type
          {where_factor}
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return pl.DataFrame(rows, schema=["time", "symbol", "factor_name", "factor_value"],
                        orient="row")


def load_returns(engine, start: str, end: str, asset_type: str, max_lag: int) -> pl.DataFrame:
    """从 market.daily 加载收益率（尾部多取 max_lag 个交易周的缓冲）"""
    # 用日历天估算：max_lag 交易日 ≈ max_lag * 2 日历天（含节假日缓冲）
    end_ext = (datetime.strptime(end, "%Y-%m-%d") + timedelta(days=max_lag * 2 + 5)).strftime("%Y-%m-%d")
    sql = text("""
        SELECT time, symbol, pct_change
        FROM market.daily
        WHERE time >= :start AND time <= :end
          AND asset_type = :asset_type
        ORDER BY symbol, time
    """)
    with engine.connect() as conn:
        rows = conn.execute(
            sql,
            {"start": start, "end": end_ext, "asset_type": asset_type},
        ).fetchall()
    df = pl.DataFrame(rows, schema=["time", "symbol", "pct_change"], orient="row")
    return df.with_columns(pl.col("pct_change").cast(pl.Float64))


# ── IC 计算 ───────────────────────────────────────────────────

def compute_daily_ic(df_factors: pl.DataFrame, df_ret: pl.DataFrame,
                     lag: int, factor_min_cross_section: dict[str, int | None]) -> pl.DataFrame:
    """
    计算每日截面 IC / RankIC（对应未来 lag 期累计收益）

    Returns: DataFrame[factor_name, time, ic, rank_ic, n_stocks]
    """
    df_next = (
        df_ret
        .sort(["symbol", "time"])
        .with_columns(
            ((pl.col("pct_change") / 100.0) + 1.0).alias("gross_ret")
        )
        .with_columns([
            pl.col("gross_ret")
            .shift(-offset)
            .over("symbol")
            .alias(f"gross_ret_t{offset}")
            for offset in range(1, lag + 1)
        ])
        .with_columns(
            (
                pl.fold(
                    acc=pl.lit(1.0),
                    function=lambda acc, x: acc * x,
                    exprs=[pl.col(f"gross_ret_t{offset}") for offset in range(1, lag + 1)],
                ) - 1.0
            ).alias("fwd_ret")
        )
        .drop(["pct_change", "gross_ret"] + [f"gross_ret_t{offset}" for offset in range(1, lag + 1)])
        .drop_nulls("fwd_ret")
    )

    df = df_factors.join(df_next, on=["time", "symbol"], how="inner")
    df = df.filter(pl.col("factor_value").is_finite() & pl.col("fwd_ret").is_finite())

    if df.is_empty():
        return pl.DataFrame(schema={
            "factor_name": pl.Utf8, "time": pl.Datetime("us", "UTC"),
            "ic": pl.Float64, "rank_ic": pl.Float64, "n_stocks": pl.UInt32,
        })

    daily_ic = (
        df.group_by(["factor_name", "time"])
        .agg([
            pl.corr("factor_value", "fwd_ret", method="pearson").alias("ic"),
            pl.corr("factor_value", "fwd_ret", method="spearman").alias("rank_ic"),
            pl.len().alias("n_stocks"),
        ])
        .sort(["factor_name", "time"])
        .with_columns(
            pl.col("factor_name").replace_strict(
                factor_min_cross_section,
                default=None,
                return_dtype=pl.Int64,
            ).alias("min_cross_section")
        )
        .filter(
            pl.col("min_cross_section").is_null() | (pl.col("n_stocks") >= pl.col("min_cross_section"))
        )
        .with_columns([
            pl.col("ic").fill_nan(None),
            pl.col("rank_ic").fill_nan(None),
        ])
        .drop("min_cross_section")
    )

    nan_days = daily_ic.filter(pl.col("ic").is_null()).group_by("factor_name").len()
    for row in nan_days.iter_rows(named=True):
        logger.debug(f"  lag={lag} {row['factor_name']}: {row['len']} 天截面零方差，已跳过")

    return daily_ic


def summarize_ic(daily_ic: pl.DataFrame, lag: int) -> pl.DataFrame:
    """从每日 IC 序列汇总统计指标"""
    return (
        daily_ic
        .group_by("factor_name")
        .agg([
            pl.col("ic").mean().alias("mean_ic"),
            pl.col("ic").std().alias("ic_std"),
            pl.col("rank_ic").mean().alias("mean_rank_ic"),
            pl.col("rank_ic").std().alias("rank_ic_std"),
            (pl.col("ic") > 0).mean().alias("win_rate"),
            pl.col("ic").count().alias("n_days"),
        ])
        .with_columns([
            (pl.col("mean_ic") / pl.col("ic_std")).alias("ic_ir"),
            (pl.col("mean_rank_ic") / pl.col("rank_ic_std")).alias("rank_ic_ir"),
        ])
        .with_columns(
            (pl.col("ic_ir") * pl.col("n_days").sqrt()).alias("t_stat"),
        )
        .with_columns(pl.lit(lag).alias("lag"))
        .select([
            "lag", "factor_name",
            "mean_rank_ic", "rank_ic_std", "rank_ic_ir",
            "mean_ic", "ic_std", "ic_ir", "t_stat", "win_rate", "n_days",
        ])
        .sort("ic_ir", descending=True)
    )


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
        factors = resolve_factors(factor_names, asset_type=asset_type)
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
        summary = summarize_ic(daily_ic, lag)
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

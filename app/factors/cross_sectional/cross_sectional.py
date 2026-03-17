"""
截面因子
基于当日行情与前一交易日行情的关系计算，适用于跨股票比较
"""
from __future__ import annotations

import polars as pl

from app.factors.base import BaseFactor


class LimitUpFactor(BaseFactor):
    """
    涨停触及因子

    定义：当天最高价 >= 理论涨停价（前收盘价 × 1.1，四舍五入到分）
    值：1.0 = 触及涨停，0.0 = 未触及

    注意：
    - 仅适用于普通 A 股（非 ST，涨跌幅限制 ±10%）
    - 新股上市首日 prev_close 为 null，对应行输出 NaN 并在写入时被过滤
    """

    name = "limit_up"

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        result = (
            df
            # df 已按 time 升序排列（pipeline._load_daily 保证），shift(1) 即前一交易日
            .with_columns(
                pl.col("close").shift(1).alias("prev_close")
            )
            .with_columns(
                limit_up_price=(pl.col("prev_close") * 1.1).round(2)
            )
            .with_columns(
                factor_value=(pl.col("high") >= pl.col("limit_up_price"))
                .cast(pl.Float64)
            )
            # prev_close 为 null 的行（首日）→ factor_value 为 null，_to_long 后被 drop_nulls 过滤
            .with_columns(
                pl.when(pl.col("prev_close").is_null())
                .then(None)
                .otherwise(pl.col("factor_value"))
                .alias("factor_value")
            )
        )
        return self._to_long(result, "factor_value").drop_nulls("factor_value")

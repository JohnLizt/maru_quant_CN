"""
技术因子
所有因子值均为无量纲、跨股票可比的归一化数值

因子说明：
  price_to_ma20  (close - MA20) / MA20   价格偏离 20 日均线的比率（短期动量/均值回归）
  ma_cross       (MA20 - MA60) / MA60    均线斜率，金叉为正，死叉为负（趋势强度）
  rsi14          RSI(14)，0~100，已归一化，无需处理
  macd_norm      macd_diff / close        MACD 差离值除以股价，消除价格量纲
"""
from __future__ import annotations

import polars as pl
import ta

from app.factors.base import TimeSeriesFactor
from app.factors.specs import get_factor_spec


def _clean(result: pl.DataFrame) -> pl.DataFrame:
    """统一过滤 null 和 NaN"""
    return result.drop_nulls("factor_value").filter(pl.col("factor_value").is_not_nan())


class PriceToMA20Factor(TimeSeriesFactor):
    """价格偏离 20 日均线比率：(close - MA20) / MA20"""

    spec = get_factor_spec("price_to_ma20")

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        close = df["close"].to_pandas()
        ma20 = ta.trend.sma_indicator(close, window=20)
        result = (
            df.with_columns([
                pl.Series("_ma20", ma20.values),
            ])
            .with_columns(
                ((pl.col("close") - pl.col("_ma20")) / pl.col("_ma20")).alias("factor_value")
            )
        )
        return _clean(self._to_long(result, "factor_value"))


class MACrossGactor(TimeSeriesFactor):
    """均线斜率：(MA20 - MA60) / MA60，正值为多头排列（金叉），负值为空头排列（死叉）"""

    spec = get_factor_spec("ma_cross")

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        close = df["close"].to_pandas()
        ma20 = ta.trend.sma_indicator(close, window=20)
        ma60 = ta.trend.sma_indicator(close, window=60)
        result = (
            df.with_columns([
                pl.Series("_ma20", ma20.values),
                pl.Series("_ma60", ma60.values),
            ])
            .with_columns(
                ((pl.col("_ma20") - pl.col("_ma60")) / pl.col("_ma60")).alias("factor_value")
            )
        )
        return _clean(self._to_long(result, "factor_value"))


class RSIFactor(TimeSeriesFactor):
    """14 日 RSI（0~100），已归一化，跨股票可比"""

    spec = get_factor_spec("rsi14")

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        close = df["close"].to_pandas()
        values = ta.momentum.rsi(close, window=14)
        result = (
            df.with_columns(pl.Series("factor_value", values.values))
        )
        return _clean(self._to_long(result, "factor_value"))


class MACDNormFactor(TimeSeriesFactor):
    """MACD 差离值 / 收盘价，消除价格量纲后跨股票可比"""

    spec = get_factor_spec("macd_norm")

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        close = df["close"].to_pandas()
        macd_diff = ta.trend.macd_diff(close)
        result = (
            df.with_columns(pl.Series("_macd_diff", macd_diff.values))
            .with_columns(
                (pl.col("_macd_diff") / pl.col("close")).alias("factor_value")
            )
        )
        return _clean(self._to_long(result, "factor_value"))


class LimitUpFactor(TimeSeriesFactor):
    """
    涨停触及因子

    定义：当天最高价 >= 理论涨停价（前收盘价 × 1.1，四舍五入到分）
    值：1.0 = 触及涨停，0.0 = 未触及

    注意：
    - 仅适用于普通 A 股（非 ST，涨跌幅限制 ±10%）
    - 新股上市首日 prev_close 为 null，对应行输出 null 并在写入时被过滤
    """

    spec = get_factor_spec("limit_up")

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        result = (
            df.with_columns(
                pl.col("close").shift(1).alias("prev_close")
            )
            .with_columns(
                limit_up_price=(pl.col("prev_close") * 1.1).round(2)
            )
            .with_columns(
                factor_value=(pl.col("high") >= pl.col("limit_up_price")).cast(pl.Float64)
            )
            .with_columns(
                pl.when(pl.col("prev_close").is_null())
                .then(None)
                .otherwise(pl.col("factor_value"))
                .alias("factor_value")
            )
        )
        return self._to_long(result, "factor_value").drop_nulls("factor_value")

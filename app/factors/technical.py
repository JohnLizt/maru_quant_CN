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

from app.factors.base import BaseFactor


def _clean(result: pl.DataFrame) -> pl.DataFrame:
    """统一过滤 null 和 NaN"""
    return result.drop_nulls("factor_value").filter(pl.col("factor_value").is_not_nan())


class PriceToMA20Factor(BaseFactor):
    """价格偏离 20 日均线比率：(close - MA20) / MA20"""

    name = "price_to_ma20"

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
            .select(["time", "symbol", "factor_value"])
            .with_columns(pl.lit(self.name).alias("factor_name"))
        )
        return _clean(result)


class MACrossGactor(BaseFactor):
    """均线斜率：(MA20 - MA60) / MA60，正值为多头排列（金叉），负值为空头排列（死叉）"""

    name = "ma_cross"

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
            .select(["time", "symbol", "factor_value"])
            .with_columns(pl.lit(self.name).alias("factor_name"))
        )
        return _clean(result)


class RSIFactor(BaseFactor):
    """14 日 RSI（0~100），已归一化，跨股票可比"""

    name = "rsi14"

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        close = df["close"].to_pandas()
        values = ta.momentum.rsi(close, window=14)
        result = (
            df.with_columns(pl.Series("factor_value", values.values))
            .select(["time", "symbol", "factor_value"])
            .with_columns(pl.lit(self.name).alias("factor_name"))
        )
        return _clean(result)


class MACDNormFactor(BaseFactor):
    """MACD 差离值 / 收盘价，消除价格量纲后跨股票可比"""

    name = "macd_norm"

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        close = df["close"].to_pandas()
        macd_diff = ta.trend.macd_diff(close)
        result = (
            df.with_columns(pl.Series("_macd_diff", macd_diff.values))
            .with_columns(
                (pl.col("_macd_diff") / pl.col("close")).alias("factor_value")
            )
            .select(["time", "symbol", "factor_value"])
            .with_columns(pl.lit(self.name).alias("factor_name"))
        )
        return _clean(result)

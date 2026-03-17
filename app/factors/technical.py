"""
技术因子
使用 `ta` 库计算常用技术指标，存入 factors.daily_factors
"""
from __future__ import annotations

import pandas as pd
import polars as pl
import ta

from app.factors.base import BaseFactor


class MA20Factor(BaseFactor):
    """20 日收盘价移动均线"""

    name = "ma20"

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        close = df["close"].to_pandas()
        values = ta.trend.sma_indicator(close, window=20)
        result = df.with_columns(
            pl.Series("factor_value", values.values)
        ).select(["time", "symbol", "factor_value"]).with_columns(
            pl.lit(self.name).alias("factor_name")
        )
        return result.drop_nulls("factor_value")


class MA60Factor(BaseFactor):
    """60 日收盘价移动均线"""

    name = "ma60"

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        close = df["close"].to_pandas()
        values = ta.trend.sma_indicator(close, window=60)
        result = df.with_columns(
            pl.Series("factor_value", values.values)
        ).select(["time", "symbol", "factor_value"]).with_columns(
            pl.lit(self.name).alias("factor_name")
        )
        return result.drop_nulls("factor_value")


class RSIFactor(BaseFactor):
    """14 日 RSI 相对强弱指数"""

    name = "rsi14"

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        close = df["close"].to_pandas()
        values = ta.momentum.rsi(close, window=14)
        result = df.with_columns(
            pl.Series("factor_value", values.values)
        ).select(["time", "symbol", "factor_value"]).with_columns(
            pl.lit(self.name).alias("factor_name")
        )
        return result.drop_nulls("factor_value")


class MACDFactor(BaseFactor):
    """MACD 差离值（DIF - DEA）"""

    name = "macd"

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        close = df["close"].to_pandas()
        values = ta.trend.macd_diff(close)
        result = df.with_columns(
            pl.Series("factor_value", values.values)
        ).select(["time", "symbol", "factor_value"]).with_columns(
            pl.lit(self.name).alias("factor_name")
        )
        return result.drop_nulls("factor_value")

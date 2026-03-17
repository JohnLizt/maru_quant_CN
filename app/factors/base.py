"""
因子基类
所有因子继承 BaseFactor，实现 compute() 方法
"""
from __future__ import annotations

from abc import ABC, abstractmethod

import polars as pl


class BaseFactor(ABC):
    """
    因子基类

    子类须实现 compute()，接收包含 OHLCV 列的 DataFrame，
    返回新增因子列的 DataFrame。
    """

    name: str  # 因子名称，用作 factors.daily_factors.factor_name

    @abstractmethod
    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        计算因子值

        Args:
            df: 包含 time, symbol, open, high, low, close, volume 列，按 time 升序排列

        Returns:
            长格式 DataFrame，列：time, symbol, factor_name, factor_value
        """
        ...

    def _to_long(self, df: pl.DataFrame, value_col: str) -> pl.DataFrame:
        """将宽格式因子列转为 factors.daily_factors 所需的长格式"""
        return df.select(["time", "symbol", value_col]).rename(
            {value_col: "factor_value"}
        ).with_columns(pl.lit(self.name).alias("factor_name"))

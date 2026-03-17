"""
策略基类
所有策略继承 BaseStrategy，实现 generate_signals() 方法
"""
from __future__ import annotations

from abc import ABC, abstractmethod

import polars as pl


class BaseStrategy(ABC):
    """
    策略基类

    子类须实现 generate_signals()，读取因子数据并产生交易信号。
    信号值约定：1 = 买入，-1 = 卖出，0 = 持有
    """

    name: str  # 策略唯一标识，写入 signals.trading_signals.strategy

    @abstractmethod
    def generate_signals(
        self,
        factors: pl.DataFrame,
        universe: list[str] | None = None,
    ) -> pl.DataFrame:
        """
        根据因子数据生成交易信号

        Args:
            factors: 长格式因子 DataFrame (time, symbol, factor_name, factor_value)
            universe: 股票池，None 表示全部

        Returns:
            DataFrame，列：time, symbol, strategy, signal, score, metadata
        """
        ...

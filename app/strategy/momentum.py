"""
动量策略示例
基于 RSI + MA20/MA60 金叉产生买卖信号
"""
from __future__ import annotations

import polars as pl
from loguru import logger

from app.strategy.base import BaseStrategy


class MomentumStrategy(BaseStrategy):
    """
    简单动量策略

    规则：
    - 买入 (1)：MA20 上穿 MA60（金叉）且 RSI < 70（未超买）
    - 卖出 (-1)：MA20 下穿 MA60（死叉）或 RSI > 80（超买）
    - 其余持有 (0)
    """

    name = "momentum_v1"

    def __init__(self, rsi_overbought: float = 70.0, rsi_oversold: float = 30.0):
        self.rsi_overbought = rsi_overbought
        self.rsi_oversold = rsi_oversold

    def generate_signals(
        self,
        factors: pl.DataFrame,
        universe: list[str] | None = None,
    ) -> pl.DataFrame:
        """
        Args:
            factors: 长格式 DataFrame (time, symbol, factor_name, factor_value)
            universe: 股票池过滤

        Returns:
            DataFrame 列：time, symbol, strategy, signal, score, metadata
        """
        if universe:
            factors = factors.filter(pl.col("symbol").is_in(universe))

        # 转宽格式
        wide = factors.pivot(
            values="factor_value",
            index=["time", "symbol"],
            on="factor_name",
        )

        required = {"ma20", "ma60", "rsi14"}
        missing = required - set(wide.columns)
        if missing:
            raise ValueError(f"缺少因子列: {missing}，请先运行因子流水线")

        signals = (
            wide
            .sort(["symbol", "time"])
            # Drop warmup rows where any required factor is NaN
            .filter(~pl.any_horizontal(pl.col(c).is_nan() for c in required))
            .with_columns([
                pl.col("ma20").shift(1).over("symbol").alias("ma20_prev"),
                pl.col("ma60").shift(1).over("symbol").alias("ma60_prev"),
            ])
            .with_columns(
                pl.when(
                    (pl.col("ma20") > pl.col("ma60")) &
                    (pl.col("ma20_prev") <= pl.col("ma60_prev")) &
                    (pl.col("rsi14") < self.rsi_overbought)
                ).then(1)
                .when(
                    (pl.col("ma20") < pl.col("ma60")) &
                    (pl.col("ma20_prev") >= pl.col("ma60_prev"))
                ).then(-1)
                .when(pl.col("rsi14") > 80)
                .then(-1)
                .otherwise(0)
                .alias("signal")
            )
            .with_columns([
                pl.lit(self.name).alias("strategy"),
                (pl.col("ma20") - pl.col("ma60")).alias("score"),
                pl.lit(None).cast(pl.Utf8).alias("metadata"),
            ])
            .select(["time", "symbol", "strategy", "signal", "score", "metadata"])
        )

        logger.info(
            f"[{self.name}] 生成信号 {len(signals)} 条，"
            f"买入={signals.filter(pl.col('signal')==1).height}，"
            f"卖出={signals.filter(pl.col('signal')==-1).height}"
        )
        return signals

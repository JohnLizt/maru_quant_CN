"""因子基类与执行形态定义。"""
from __future__ import annotations

from abc import ABC, abstractmethod

import polars as pl

from app.factors.specs import FactorSpec, SuspendedPolicy


class BaseFactor(ABC):
    """因子公共元信息与输出工具。"""

    spec: FactorSpec

    @property
    def name(self) -> str:
        return self.spec.name

    @property
    def suspended_policy(self) -> SuspendedPolicy:
        return self.spec.suspended_policy

    @property
    def warmup_days(self) -> int:
        return self.spec.warmup_days

    @property
    def required_fields(self) -> tuple[str, ...]:
        return self.spec.required_fields

    @property
    def ic_min_cross_section(self) -> int | None:
        return self.spec.ic_min_cross_section

    @property
    def production_enabled(self) -> bool:
        return self.spec.production_enabled

    def supports_asset_type(self, asset_type: str) -> bool:
        return asset_type in self.spec.supported_asset_types

    def _apply_suspended_policy(self, df: pl.DataFrame, value_col: str) -> pl.DataFrame:
        """按因子策略处理停牌日因子值。"""
        if "is_suspended" not in df.columns or self.suspended_policy == "allow":
            return df

        if self.suspended_policy == "mask":
            return df.with_columns(
                pl.when(pl.col("is_suspended"))
                .then(None)
                .otherwise(pl.col(value_col))
                .alias(value_col)
            )

        raise ValueError(f"Unsupported suspended_policy: {self.suspended_policy}")

    def _to_long(self, df: pl.DataFrame, value_col: str) -> pl.DataFrame:
        """将宽格式因子列转为 factors.daily_factors 所需的长格式"""
        df = self._apply_suspended_policy(df, value_col)
        columns = ["time", "symbol", value_col]
        if "asset_type" in df.columns:
            columns.insert(1, "asset_type")
        return df.select(columns).rename({value_col: "factor_value"}).with_columns(
            pl.lit(self.name).alias("factor_name")
        )


class TimeSeriesFactor(BaseFactor, ABC):
    """按单个 symbol 时序独立计算的因子。"""

    @abstractmethod
    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        计算单个 symbol 的时序因子。

        Args:
            df: 单个 symbol 的 OHLCV 序列，按 time 升序排列。

        Returns:
            长格式 DataFrame，列：time, symbol, factor_name, factor_value
        """
        ...


class CrossSectionalFactor(BaseFactor, ABC):
    """需要同一交易日全池面板数据的截面因子。"""

    @abstractmethod
    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        计算全池截面因子。

        Args:
            df: 多个 symbol 的 OHLCV 面板数据，至少包含 time/symbol。

        Returns:
            长格式 DataFrame，列：time, symbol, factor_name, factor_value
        """
        ...

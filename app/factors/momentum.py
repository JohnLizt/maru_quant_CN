"""Momentum factors spanning time-series and cross-sectional forms."""
from __future__ import annotations

import polars as pl

from app.factors.base import CrossSectionalFactor, TimeSeriesFactor
from app.factors.specs import FactorSpec


def _clean(result: pl.DataFrame) -> pl.DataFrame:
    return result.drop_nulls("factor_value").filter(pl.col("factor_value").is_not_nan())


def _rank_to_unit(df: pl.DataFrame, raw_col: str) -> pl.DataFrame:
    group_cols = ["time"]
    if "asset_type" in df.columns:
        group_cols.insert(0, "asset_type")

    return (
        df.with_columns([
            pl.col(raw_col).rank(method="average").over(group_cols).alias("_rank"),
            pl.col(raw_col).count().over(group_cols).alias("_cross_count"),
        ])
        .with_columns(
            pl.when(pl.col(raw_col).is_null())
            .then(None)
            .when(pl.col("_cross_count") <= 1)
            .then(0.5)
            .otherwise((pl.col("_rank") - 1.0) / (pl.col("_cross_count") - 1.0))
            .alias("factor_value")
        )
    )


class _BaseReturnFactor(TimeSeriesFactor):
    window: int

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        result = df.with_columns(
            (pl.col("close") / pl.col("close").shift(self.window) - 1.0).alias("factor_value")
        )
        return _clean(self._to_long(result, "factor_value"))


class Ret10Factor(_BaseReturnFactor):
    """10-day simple return."""

    window = 10

    spec = FactorSpec(
        name="ret_10",
        category="time_series",
        warmup_days=20,
        suspended_policy="allow",
        required_fields=("close",),
        ic_min_cross_section=20,
        description="close / close.shift(10) - 1",
        supported_asset_types=("stock_CN", "etf_CN"),
    )


class Ret20Factor(_BaseReturnFactor):
    """20-day simple return."""

    window = 20

    spec = FactorSpec(
        name="ret_20",
        category="time_series",
        warmup_days=30,
        suspended_policy="allow",
        required_fields=("close",),
        ic_min_cross_section=20,
        description="close / close.shift(20) - 1",
        supported_asset_types=("stock_CN", "etf_CN"),
    )


class Ret30Factor(_BaseReturnFactor):
    """30-day simple return."""

    window = 30

    spec = FactorSpec(
        name="ret_30",
        category="time_series",
        warmup_days=50,
        suspended_policy="allow",
        required_fields=("close",),
        ic_min_cross_section=20,
        description="close / close.shift(30) - 1",
        supported_asset_types=("stock_CN", "etf_CN"),
    )


class Ret60Factor(_BaseReturnFactor):
    """60-day simple return."""

    window = 60

    spec = FactorSpec(
        name="ret_60",
        category="time_series",
        warmup_days=100,
        suspended_policy="allow",
        required_fields=("close",),
        ic_min_cross_section=20,
        description="close / close.shift(60) - 1",
        supported_asset_types=("stock_CN", "etf_CN"),
    )


class Ret10RankFactor(CrossSectionalFactor):
    """10-day return percentile rank within each trading day."""

    spec = FactorSpec(
        name="ret_10_rank",
        category="cross_sectional",
        warmup_days=20,
        suspended_policy="allow",
        required_fields=("close",),
        ic_min_cross_section=20,
        description="cross-sectional percentile rank of ret_10 mapped to [0, 1]",
        supported_asset_types=("stock_CN", "etf_CN"),
    )

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        result = (
            df.sort(["symbol", "time"])
            .with_columns(
                (pl.col("close") / pl.col("close").shift(10).over("symbol") - 1.0).alias("_ret10")
            )
            .pipe(_rank_to_unit, raw_col="_ret10")
        )
        return _clean(self._to_long(result, "factor_value"))


class Ret20RankFactor(CrossSectionalFactor):
    """20-day return percentile rank within each trading day."""

    spec = FactorSpec(
        name="ret_20_rank",
        category="cross_sectional",
        warmup_days=30,
        suspended_policy="allow",
        required_fields=("close",),
        ic_min_cross_section=20,
        description="cross-sectional percentile rank of ret_20 mapped to [0, 1]",
        supported_asset_types=("stock_CN", "etf_CN"),
    )

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        result = (
            df.sort(["symbol", "time"])
            .with_columns(
                (pl.col("close") / pl.col("close").shift(20).over("symbol") - 1.0).alias("_ret20")
            )
            .pipe(_rank_to_unit, raw_col="_ret20")
        )
        return _clean(self._to_long(result, "factor_value"))


class Ret30RankFactor(CrossSectionalFactor):
    """30-day return percentile rank within each trading day."""

    spec = FactorSpec(
        name="ret_30_rank",
        category="cross_sectional",
        warmup_days=50,
        suspended_policy="allow",
        required_fields=("close",),
        ic_min_cross_section=20,
        description="cross-sectional percentile rank of ret_30 mapped to [0, 1]",
        supported_asset_types=("stock_CN", "etf_CN"),
    )

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        result = (
            df.sort(["symbol", "time"])
            .with_columns(
                (pl.col("close") / pl.col("close").shift(30).over("symbol") - 1.0).alias("_ret30")
            )
            .pipe(_rank_to_unit, raw_col="_ret30")
        )
        return _clean(self._to_long(result, "factor_value"))


class Ret60RankFactor(CrossSectionalFactor):
    """60-day return percentile rank within each trading day."""

    spec = FactorSpec(
        name="ret_60_rank",
        category="cross_sectional",
        warmup_days=100,
        suspended_policy="allow",
        required_fields=("close",),
        ic_min_cross_section=20,
        description="cross-sectional percentile rank of ret_60 mapped to [0, 1]",
        supported_asset_types=("stock_CN", "etf_CN"),
    )

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        result = (
            df.sort(["symbol", "time"])
            .with_columns(
                (pl.col("close") / pl.col("close").shift(60).over("symbol") - 1.0).alias("_ret60")
            )
            .pipe(_rank_to_unit, raw_col="_ret60")
        )
        return _clean(self._to_long(result, "factor_value"))

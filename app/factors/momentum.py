"""Momentum factors spanning time-series and cross-sectional forms."""
from __future__ import annotations

import math

import numpy as np
import polars as pl

from app.factors.base import CrossSectionalFactor, TimeSeriesFactor
from app.factors.specs import get_factor_spec


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


def _weighted_regression_momentum_scores(close_values: list[float], lookback_days: int) -> list[float | None]:
    need_len = lookback_days + 1
    scores: list[float | None] = [None] * len(close_values)

    x = np.arange(need_len, dtype=float)
    weights = np.linspace(1.0, 2.0, need_len)

    for idx in range(need_len - 1, len(close_values)):
        window = np.asarray(close_values[idx - need_len + 1 : idx + 1], dtype=float)
        if np.any(~np.isfinite(window)) or np.any(window <= 0.0):
            continue

        y = np.log(window)
        slope, intercept = np.polyfit(x, y, 1, w=weights)
        annualized_return = math.exp(slope * 250.0) - 1.0
        y_hat = slope * x + intercept
        weighted_mean = np.average(y, weights=weights)
        ss_res = np.sum(weights * (y - y_hat) ** 2)
        ss_tot = np.sum(weights * (y - weighted_mean) ** 2)
        r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0.0 else 0.0
        scores[idx] = annualized_return * r_squared

    return scores


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

    spec = get_factor_spec("ret_10")


class Ret20Factor(_BaseReturnFactor):
    """20-day simple return."""

    window = 20

    spec = get_factor_spec("ret_20")


class Ret30Factor(_BaseReturnFactor):
    """30-day simple return."""

    window = 30

    spec = get_factor_spec("ret_30")


class Ret60Factor(_BaseReturnFactor):
    """60-day simple return."""

    window = 60

    spec = get_factor_spec("ret_60")


class MomentumReg20Factor(TimeSeriesFactor):
    """20-day weighted log-price regression momentum: annualized return * R^2."""

    lookback_days = 20

    spec = get_factor_spec("momentum_reg_20")

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        close_values = df.get_column("close").cast(pl.Float64).to_list()
        factor_values = _weighted_regression_momentum_scores(close_values, self.lookback_days)
        result = df.with_columns(pl.Series(name="factor_value", values=factor_values, dtype=pl.Float64))
        return _clean(self._to_long(result, "factor_value"))


class Ret10RankFactor(CrossSectionalFactor):
    """10-day return percentile rank within each trading day."""

    spec = get_factor_spec("ret_10_rank")

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

    spec = get_factor_spec("ret_20_rank")

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

    spec = get_factor_spec("ret_30_rank")

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

    spec = get_factor_spec("ret_60_rank")

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        result = (
            df.sort(["symbol", "time"])
            .with_columns(
                (pl.col("close") / pl.col("close").shift(60).over("symbol") - 1.0).alias("_ret60")
            )
            .pipe(_rank_to_unit, raw_col="_ret60")
        )
        return _clean(self._to_long(result, "factor_value"))


class MomentumReg20RankFactor(CrossSectionalFactor):
    """Cross-sectional percentile rank of 20-day weighted regression momentum."""

    lookback_days = 20

    spec = get_factor_spec("momentum_reg_20_rank")

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        ordered = df.sort(["symbol", "time"])
        raw_frames: list[pl.DataFrame] = []
        for symbol_df in ordered.partition_by("symbol", maintain_order=True):
            close_values = symbol_df.get_column("close").cast(pl.Float64).to_list()
            raw_frames.append(
                symbol_df.with_columns(
                    pl.Series(
                        name="_momentum_reg_20",
                        values=_weighted_regression_momentum_scores(close_values, self.lookback_days),
                        dtype=pl.Float64,
                    )
                )
            )

        result = pl.concat(raw_frames).pipe(_rank_to_unit, raw_col="_momentum_reg_20")
        return _clean(self._to_long(result, "factor_value"))

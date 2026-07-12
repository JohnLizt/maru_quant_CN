"""Momentum factors spanning time-series and cross-sectional forms."""
from __future__ import annotations

import math

import numpy as np
import polars as pl

from app.factors.base import TimeSeriesFactor
from app.factors.specs import get_factor_spec


def _clean(result: pl.DataFrame) -> pl.DataFrame:
    return result.drop_nulls("factor_value").filter(pl.col("factor_value").is_not_nan())


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


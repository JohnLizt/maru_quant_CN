"""Risk-oriented ETF time-series factors shared with backtest overlay."""
from __future__ import annotations

import polars as pl

from app.backtest.risk_overlay import RiskOverlayConfig, add_risk_feature_columns
from app.factors.base import TimeSeriesFactor
from app.factors.specs import get_factor_spec


def _clean(result: pl.DataFrame) -> pl.DataFrame:
    return result.drop_nulls("factor_value").filter(pl.col("factor_value").is_not_nan())


class StdScoreFactor(TimeSeriesFactor):
    spec = get_factor_spec("std_score")

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        result = add_risk_feature_columns(df, RiskOverlayConfig())
        return _clean(self._to_long(result, "std_score"))


class CVFactor(TimeSeriesFactor):
    spec = get_factor_spec("cv")

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        result = add_risk_feature_columns(df, RiskOverlayConfig())
        return _clean(self._to_long(result, "cv"))

"""Risk-oriented ETF time-series factors shared with backtest overlay."""
from __future__ import annotations

import polars as pl

from app.backtest.risk_overlay import RiskOverlayConfig, add_risk_feature_columns
from app.factors.base import TimeSeriesFactor
from app.factors.specs import FactorSpec


def _clean(result: pl.DataFrame) -> pl.DataFrame:
    return result.drop_nulls("factor_value").filter(pl.col("factor_value").is_not_nan())


class StdScoreFactor(TimeSeriesFactor):
    spec = FactorSpec(
        name="std_score",
        category="time_series",
        warmup_days=21,
        suspended_policy="allow",
        required_fields=("close", "amount"),
        ic_min_cross_section=20,
        description="(rolling_std(ret,20) + rolling_std(ret,5)) / 2",
        supported_asset_types=("etf_CN",),
    )

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        result = add_risk_feature_columns(df, RiskOverlayConfig())
        return _clean(self._to_long(result, "std_score"))


class CVFactor(TimeSeriesFactor):
    spec = FactorSpec(
        name="cv",
        category="time_series",
        warmup_days=20,
        suspended_policy="allow",
        required_fields=("close", "amount"),
        ic_min_cross_section=20,
        description="rolling_std(amount,20) / rolling_mean(amount,20)",
        supported_asset_types=("etf_CN",),
    )

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        result = add_risk_feature_columns(df, RiskOverlayConfig())
        return _clean(self._to_long(result, "cv"))

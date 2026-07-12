"""ETF universe rotation strategy based on composite signal ranking."""
from __future__ import annotations

import json
from datetime import date

import polars as pl

from app.backtest.risk_overlay import RiskOverlayConfig
from app.signals.profiles import get_signal_profile
from app.strategy.base import BaseStrategy


class BaseETFUniverseRotationStrategy(BaseStrategy):
    """Pick the top ranked ETF names from the strategy universe snapshot."""

    strategy_name = "etf_rotation_v1"
    strategy_mode = "cross_sectional"
    supported_signal_modes = ("cross_sectional",)
    supported_asset_types = ("*",)
    default_universe = "etf_mixed"
    default_profile_name = "trend_etf_momentum_reg20"

    def __init__(
        self,
        top_n: int = 4,
        profile_name: str | None = None,
        max_per_tag: int = 1,
    ) -> None:
        if top_n <= 0:
            raise ValueError("top_n 必须大于 0")
        if max_per_tag <= 0:
            raise ValueError("max_per_tag 必须大于 0")
        self.top_n = top_n
        self.profile_name = profile_name or self.default_profile_name
        self.max_per_tag = max_per_tag
        profile = get_signal_profile(self.profile_name)
        if self.supported_asset_types == ("*",):
            self.supported_asset_types = profile.supported_asset_types

    def default_risk_config(self) -> RiskOverlayConfig:
        return RiskOverlayConfig(
            std_threshold=0.03,
            cv_threshold=0.5,
            stop_loss_rate=0.10,
            half_weight=0.5,
        )

    def build_decisions(
        self,
        signal_snapshot: pl.DataFrame,
        as_of_date: date | None = None,
    ) -> pl.DataFrame:
        self.validate_signal_snapshot(signal_snapshot)
        if signal_snapshot.is_empty():
            return self.empty_decisions()

        df = signal_snapshot
        if as_of_date is not None:
            df = df.filter(pl.col("time").dt.date() == pl.lit(as_of_date))
        if df.is_empty():
            return self.empty_decisions()

        if "tag" not in df.columns:
            df = df.with_columns(pl.lit("other").alias("tag"))
        else:
            df = df.with_columns(pl.col("tag").fill_null("other"))

        ranked = (
            df.sort(["time", "composite_score", "symbol"], descending=[False, True, False])
            .with_columns(
                pl.col("composite_score")
                .rank(method="ordinal", descending=True)
                .over("time")
                .cast(pl.UInt32)
                .alias("rank")
            )
            .with_columns(
                pl.col("symbol").cum_count().over(["time", "tag"]).cast(pl.UInt32).alias("_tag_rank")
            )
            .filter(pl.col("_tag_rank") <= self.max_per_tag)
            .with_columns(pl.col("symbol").cum_count().over("time").cast(pl.UInt32).alias("_selected_rank"))
            .filter(pl.col("_selected_rank") <= self.top_n)
            .drop(["_tag_rank", "_selected_rank"])
        )

        if ranked.is_empty():
            return self.empty_decisions()

        def _metadata(row: dict[str, object]) -> str:
            return json.dumps(
                {
                    "rank": int(row["rank"]),
                    "tag": str(row.get("tag", "") or ""),
                    "profile": self.profile_name,
                },
                ensure_ascii=False,
            )

        return (
            ranked.with_columns(
                [
                    pl.lit(self.strategy_name).alias("strategy"),
                    pl.lit(self.strategy_mode).alias("strategy_mode"),
                    pl.lit("target_weight").alias("decision_type"),
                    pl.lit(1).alias("signal"),
                    (pl.lit(1.0) / pl.len().over("time")).cast(pl.Float64).alias("target_weight"),
                    pl.col("composite_score").alias("score"),
                    pl.struct(["rank", "tag"]).map_elements(_metadata, return_dtype=pl.Utf8).alias("metadata"),
                ]
            ).select(
                [
                    "time",
                    "asset_type",
                    "strategy",
                    "strategy_mode",
                    "symbol",
                    "decision_type",
                    "signal",
                    "target_weight",
                    "score",
                    "rank",
                    "tag",
                    "metadata",
                ]
            )
        )


class ETFRotationCNStrategy(BaseETFUniverseRotationStrategy):
    supported_asset_types = ("etf_CN",)
    default_universe = "etf_rotation_CN"

    def default_risk_config(self) -> RiskOverlayConfig:
        return RiskOverlayConfig(
            std_threshold=0.03,
            cv_threshold=0.5,
            stop_loss_rate=0.10,
            half_weight=0.5,
        )


class ETFRotationUSStrategy(BaseETFUniverseRotationStrategy):
    supported_asset_types = ("etf_US",)
    default_universe = "etf_rotation_US"

    def default_risk_config(self) -> RiskOverlayConfig:
        return RiskOverlayConfig(
            std_threshold=0.02,
            cv_threshold=0.70,
            stop_loss_rate=0.10,
            half_weight=0.5,
        )


class ETFUniverseRotationStrategy(BaseETFUniverseRotationStrategy):
    """Backward-compatible mixed-universe ETF rotation strategy."""


def resolve_etf_rotation_strategy(
    universe: str,
    *,
    top_n: int = 4,
    profile_name: str | None = None,
    max_per_tag: int = 1,
) -> BaseETFUniverseRotationStrategy:
    if universe == ETFRotationUSStrategy.default_universe:
        return ETFRotationUSStrategy(top_n=top_n, profile_name=profile_name, max_per_tag=max_per_tag)
    if universe == ETFRotationCNStrategy.default_universe:
        return ETFRotationCNStrategy(top_n=top_n, profile_name=profile_name, max_per_tag=max_per_tag)
    return ETFUniverseRotationStrategy(top_n=top_n, profile_name=profile_name, max_per_tag=max_per_tag)

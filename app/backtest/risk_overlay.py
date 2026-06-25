"""Risk feature helpers for the cash-account ETF backtest."""
from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class RiskOverlayConfig:
    """Risk-control parameters used by ETF rotation backtests."""

    std_threshold: float = 0.03
    cv_threshold: float = 0.5
    stop_loss_rate: float = 0.10
    half_weight: float = 0.5
    std_long_window: int = 20
    std_short_window: int = 5
    cv_window: int = 20

    def __post_init__(self) -> None:
        if self.std_threshold < 0:
            raise ValueError("std_threshold 不能小于 0")
        if self.cv_threshold < 0:
            raise ValueError("cv_threshold 不能小于 0")
        if self.stop_loss_rate < 0:
            raise ValueError("stop_loss_rate 不能小于 0")
        if not 0 <= self.half_weight <= 1:
            raise ValueError("half_weight 必须在 0~1 之间")
        if self.std_long_window <= 1 or self.std_short_window <= 1 or self.cv_window <= 1:
            raise ValueError("risk overlay rolling window 必须大于 1")


def get_risk_reason(
    std_score: float | None,
    cv: float | None,
    config: RiskOverlayConfig,
) -> str | None:
    """Return the risk reason if the symbol should be treated as high-risk."""

    high_std = std_score is not None and std_score > config.std_threshold
    high_cv = cv is not None and cv > config.cv_threshold
    if high_std and high_cv:
        return "risk_half_std_cv"
    if high_std:
        return "risk_half_std"
    if high_cv:
        return "risk_half_cv"
    return None


def add_risk_feature_columns(
    market_data: pl.DataFrame,
    config: RiskOverlayConfig,
) -> pl.DataFrame:
    """Attach rolling volatility and amount CV columns to a single panel slice."""

    close = market_data.get_column("close").cast(pl.Float64)
    amount = market_data.get_column("amount").cast(pl.Float64)
    ret = close.pct_change()
    std_long = ret.rolling_std(window_size=config.std_long_window)
    std_short = ret.rolling_std(window_size=config.std_short_window)
    amount_mean = amount.rolling_mean(window_size=config.cv_window)
    amount_std = amount.rolling_std(window_size=config.cv_window)
    return market_data.with_columns(
        [
            ((std_long + std_short) / 2.0).alias("std_score"),
            (amount_std / amount_mean).alias("cv"),
        ]
    )


def build_risk_features(market_data: pl.DataFrame, config: RiskOverlayConfig) -> pl.DataFrame:
    """Compute rolling volatility and amount CV on market daily bars.

    The input is expected to be sorted by `symbol, time` or sortable to that
    order. `time` is normalized to `Date`.
    """

    required = {"time", "symbol", "close", "amount"}
    missing = required - set(market_data.columns)
    if missing:
        raise ValueError(f"risk overlay market_data 缺少列: {sorted(missing)}")

    partition_keys = ["asset_type", "symbol"] if "asset_type" in market_data.columns else ["symbol"]
    sort_keys = [*partition_keys, "time"]

    frames: list[pl.DataFrame] = []
    for symbol_df in market_data.sort(sort_keys).partition_by(partition_keys, maintain_order=True):
        frames.append(add_risk_feature_columns(symbol_df, config))

    if not frames:
        return market_data.with_columns(
            [
                pl.lit(None, dtype=pl.Float64).alias("std_score"),
                pl.lit(None, dtype=pl.Float64).alias("cv"),
                pl.col("time").cast(pl.Date),
            ]
        )

    return pl.concat(frames).with_columns(pl.col("time").cast(pl.Date))

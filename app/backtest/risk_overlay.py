"""Optional risk overlay for target-weight backtests."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

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


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def _feature_reason(std_score: float | None, cv: float | None, config: RiskOverlayConfig) -> str | None:
    high_std = std_score is not None and std_score > config.std_threshold
    high_cv = cv is not None and cv > config.cv_threshold
    if high_std and high_cv:
        return "risk_half_std_cv"
    if high_std:
        return "risk_half_std"
    if high_cv:
        return "risk_half_cv"
    return None


def _market_with_risk_features(market_data: pl.DataFrame, config: RiskOverlayConfig) -> pl.DataFrame:
    required = {"time", "symbol", "close"}
    missing = required - set(market_data.columns)
    if missing:
        raise ValueError(f"risk overlay market_data 缺少列: {sorted(missing)}")

    if {"std_score", "cv"}.issubset(market_data.columns):
        return market_data.with_columns(pl.col("time").cast(pl.Date).alias("_date"))

    if "amount" not in market_data.columns:
        raise ValueError("risk overlay market_data 缺少列: ['amount']")

    frames: list[pl.DataFrame] = []
    for symbol_df in market_data.sort(["symbol", "time"]).partition_by("symbol", maintain_order=True):
        close = symbol_df.get_column("close").cast(pl.Float64)
        amount = symbol_df.get_column("amount").cast(pl.Float64)
        ret = close.pct_change()
        std_long = ret.rolling_std(window_size=config.std_long_window)
        std_short = ret.rolling_std(window_size=config.std_short_window)
        amount_mean = amount.rolling_mean(window_size=config.cv_window)
        amount_std = amount.rolling_std(window_size=config.cv_window)
        symbol_features = symbol_df.with_columns(
            [
                ((std_long + std_short) / 2.0).alias("std_score"),
                (amount_std / amount_mean).alias("cv"),
            ]
        )
        frames.append(symbol_features)

    if not frames:
        return market_data.with_columns(
            [
                pl.lit(None, dtype=pl.Float64).alias("std_score"),
                pl.lit(None, dtype=pl.Float64).alias("cv"),
                pl.col("time").cast(pl.Date).alias("_date"),
            ]
        )

    return pl.concat(frames).with_columns(pl.col("time").cast(pl.Date).alias("_date"))


def apply_risk_overlay(
    holdings_df: pl.DataFrame,
    market_data: pl.DataFrame,
    config: RiskOverlayConfig,
) -> tuple[pl.DataFrame, dict[str, int]]:
    """Apply half-weight and stop-loss rules to expanded daily holdings.

    The input target weights are treated as the strategy's base weights. The
    overlay only reduces weights; unused capital is intentionally left as cash.
    """

    if holdings_df.is_empty():
        return holdings_df, {"risk_half_events": 0, "stop_loss_events": 0}
    if "target_weight" not in holdings_df.columns or "symbol" not in holdings_df.columns:
        raise ValueError("holdings_df 必须包含 symbol 和 target_weight")

    features = _market_with_risk_features(market_data, config)
    feature_map: dict[tuple[str, date], dict[str, float | None]] = {}
    for row in features.select(["_date", "symbol", "close", "std_score", "cv"]).iter_rows(named=True):
        current_date = _as_date(row["_date"])
        if current_date is None:
            continue
        feature_map[(row["symbol"], current_date)] = {
            "close": row["close"],
            "std_score": row["std_score"],
            "cv": row["cv"],
        }

    sorted_holdings = holdings_df.with_columns(pl.col("time").cast(pl.Date)).sort(["time", "rank", "symbol"])
    has_start_date = "holding_start_date" in sorted_holdings.columns
    state: dict[tuple[str, date], dict[str, Any]] = {}
    adjusted_rows: list[dict[str, Any]] = []
    risk_half_events = 0
    stop_loss_events = 0

    for row in sorted_holdings.iter_rows(named=True):
        current_date = _as_date(row["time"])
        if current_date is None:
            continue
        start_date = _as_date(row["holding_start_date"]) if has_start_date else current_date
        if start_date is None:
            start_date = current_date
        key = (row["symbol"], start_date)
        base_weight = float(row["target_weight"] or 0.0)
        feature = feature_map.get((row["symbol"], current_date), {})
        close = feature.get("close")
        std_score = feature.get("std_score")
        cv = feature.get("cv")

        symbol_state = state.setdefault(
            key,
            {
                "entry_close": close,
                "risk_half_triggered": False,
                "risk_half_reason": None,
                "stopped": False,
            },
        )
        if symbol_state["entry_close"] is None and close is not None:
            symbol_state["entry_close"] = close

        target_weight = base_weight
        risk_reason = ""
        risk_adjusted = False

        entry_close = symbol_state["entry_close"]
        if (
            not symbol_state["stopped"]
            and close is not None
            and entry_close not in (None, 0)
            and (float(close) / float(entry_close) - 1.0) <= -config.stop_loss_rate
        ):
            symbol_state["stopped"] = True
            stop_loss_events += 1

        if symbol_state["stopped"]:
            target_weight = 0.0
            risk_reason = "stop_loss"
            risk_adjusted = base_weight != target_weight
        else:
            half_reason = _feature_reason(
                float(std_score) if std_score is not None else None,
                float(cv) if cv is not None else None,
                config,
            )
            if half_reason and not symbol_state["risk_half_triggered"]:
                symbol_state["risk_half_triggered"] = True
                symbol_state["risk_half_reason"] = half_reason
                risk_half_events += 1
            if symbol_state["risk_half_triggered"]:
                target_weight = base_weight * config.half_weight
                risk_reason = str(symbol_state["risk_half_reason"] or "risk_half")
                risk_adjusted = base_weight != target_weight

        adjusted = dict(row)
        adjusted["base_target_weight"] = base_weight
        adjusted["target_weight"] = target_weight
        adjusted["risk_adjusted"] = risk_adjusted
        adjusted["risk_reason"] = risk_reason
        adjusted_rows.append(adjusted)

    return pl.DataFrame(adjusted_rows), {
        "risk_half_events": risk_half_events,
        "stop_loss_events": stop_loss_events,
    }

"""QQQ-enhanced strategies with explicit growth core and defensive overlay."""
from __future__ import annotations

import json
from datetime import date
from typing import Final

import polars as pl

from app.strategy.base import BaseStrategy, STRATEGY_DECISION_SCHEMA


REGIME_RISK_ON: Final = "risk_on"
REGIME_NEUTRAL: Final = "neutral"
REGIME_RISK_OFF: Final = "risk_off"


class QQQOnlyStrategy(BaseStrategy):
    """A pure QQQ baseline strategy for testing the generic risk overlay."""

    strategy_name = "qqq_only_v1"
    strategy_mode = "cross_sectional"
    supported_signal_modes = ("cross_sectional",)
    supported_asset_types = ("etf_US",)
    default_profile_name = "qqq_only"
    default_tag_by_symbol: Final = {"QQQ": "nasdaq_100"}

    def __init__(self, *, symbol: str = "QQQ", profile_name: str | None = None) -> None:
        self.symbol = symbol
        self.profile_name = profile_name or self.default_profile_name

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
        df = df.filter(pl.col("symbol") == self.symbol)
        if df.is_empty():
            return self.empty_decisions()

        if "tag" not in df.columns:
            df = df.with_columns(pl.lit(self.default_tag_by_symbol.get(self.symbol, "other")).alias("tag"))
        else:
            df = df.with_columns(
                pl.col("tag").fill_null(self.default_tag_by_symbol.get(self.symbol, "other"))
            )

        rows: list[dict[str, object]] = []
        for row in df.sort(["time", "symbol"]).iter_rows(named=True):
            tag = str(row.get("tag") or self.default_tag_by_symbol.get(self.symbol, "other"))
            rows.append(
                {
                    "time": row["time"],
                    "asset_type": row["asset_type"],
                    "strategy": self.strategy_name,
                    "strategy_mode": self.strategy_mode,
                    "symbol": self.symbol,
                    "decision_type": "target_weight",
                    "signal": 1,
                    "target_weight": 1.0,
                    "score": float(row.get("composite_score") or 1.0),
                    "rank": 1,
                    "tag": tag,
                    "metadata": json.dumps(
                        {
                            "rank": 1,
                            "tag": tag,
                            "profile": self.profile_name,
                            "template": "pure_qqq",
                        },
                        ensure_ascii=False,
                    ),
                }
            )

        return pl.DataFrame(rows, schema=STRATEGY_DECISION_SCHEMA).sort(["time", "rank", "symbol"])


class QQQOnlyTrailingStopStrategy(BaseStrategy):
    """Pure QQQ strategy that moves to cash after a simple trailing stop."""

    strategy_name = "qqq_only_trailing_stop_v1"
    strategy_mode = "cross_sectional"
    supported_signal_modes = ("cross_sectional",)
    supported_asset_types = ("etf_US",)
    default_profile_name = "qqq_only_trailing_stop"
    default_tag_by_symbol: Final = {"QQQ": "nasdaq_100", "CASH": "cash"}

    def __init__(
        self,
        *,
        symbol: str = "QQQ",
        cash_symbol: str = "CASH",
        trailing_stop_rate: float = 0.10,
        trailing_peak_window: int = 252,
        profile_name: str | None = None,
    ) -> None:
        if not 0 < trailing_stop_rate < 1:
            raise ValueError("trailing_stop_rate 必须在 0 和 1 之间")
        if trailing_peak_window <= 1:
            raise ValueError("trailing_peak_window 必须大于 1")
        self.symbol = symbol
        self.cash_symbol = cash_symbol
        self.trailing_stop_rate = trailing_stop_rate
        self.trailing_peak_window = trailing_peak_window
        self.profile_name = profile_name or self.default_profile_name

    def build_decisions(
        self,
        signal_snapshot: pl.DataFrame,
        as_of_date: date | None = None,
    ) -> pl.DataFrame:
        self.validate_signal_snapshot(signal_snapshot)
        if signal_snapshot.is_empty():
            return self.empty_decisions()
        if "close" not in signal_snapshot.columns:
            raise ValueError("QQQOnlyTrailingStopStrategy 需要 signal_snapshot 包含 close 列")

        df = signal_snapshot.filter(pl.col("symbol") == self.symbol)
        if as_of_date is not None:
            df = df.filter(pl.col("time").dt.date() == pl.lit(as_of_date))
        if df.is_empty():
            return self.empty_decisions()

        if "tag" not in df.columns:
            df = df.with_columns(pl.lit(self.default_tag_by_symbol[self.symbol]).alias("tag"))
        else:
            df = df.with_columns(pl.col("tag").fill_null(self.default_tag_by_symbol[self.symbol]))

        qqq = (
            df.sort("time")
            .with_columns(
                pl.col("close")
                .cast(pl.Float64)
                .rolling_max(window_size=self.trailing_peak_window, min_samples=1)
                .alias("trailing_peak")
            )
            .with_columns(
                (pl.col("trailing_peak") * (1.0 - self.trailing_stop_rate)).alias("stop_line")
            )
            .with_columns((pl.col("close") >= pl.col("stop_line")).alias("risk_on"))
        )

        rows: list[dict[str, object]] = []
        for row in qqq.iter_rows(named=True):
            risk_on = bool(row["risk_on"])
            target_symbol = self.symbol if risk_on else self.cash_symbol
            tag = self.default_tag_by_symbol.get(target_symbol, "other")
            metadata = {
                "rank": 1,
                "tag": tag,
                "profile": self.profile_name,
                "template": "pure_qqq_trailing_stop",
                "trailing_stop_rate": self.trailing_stop_rate,
                "trailing_peak_window": self.trailing_peak_window,
                "close": float(row["close"]),
                "trailing_peak": float(row["trailing_peak"]),
                "stop_line": float(row["stop_line"]),
                "risk_on": risk_on,
            }
            rows.append(
                {
                    "time": row["time"],
                    "asset_type": row["asset_type"],
                    "strategy": self.strategy_name,
                    "strategy_mode": self.strategy_mode,
                    "symbol": target_symbol,
                    "decision_type": "target_weight",
                    "signal": 1 if risk_on else 0,
                    "target_weight": 1.0,
                    "score": float(row.get("composite_score") or 1.0),
                    "rank": 1,
                    "tag": tag,
                    "metadata": json.dumps(metadata, ensure_ascii=False),
                }
            )

        return pl.DataFrame(rows, schema=STRATEGY_DECISION_SCHEMA).sort(["time", "rank", "symbol"])


class QQQEnhancedFixedCoreStrategy(BaseStrategy):
    """A simple QQQ core strategy based on a precomputed market regime.

    This is the first implementation of section 6.1 in
    ``specs/qqq_enhanced_strategy_plan.md``. The strategy intentionally does
    not calculate MA or drawdown regimes itself; experiment scripts can add a
    ``regime`` column to the signal snapshot and this class only converts that
    regime into target weights.
    """

    strategy_name = "qqq_enhanced_fixed_core_v1"
    strategy_mode = "cross_sectional"
    supported_signal_modes = ("cross_sectional",)
    supported_asset_types = ("etf_US",)
    default_profile_name = "trend_etf_momentum_reg20"

    default_growth_symbols: Final = ("XLK", "SMH")
    default_defensive_symbols: Final = ("GLD", "IEF", "UUP")
    default_tag_by_symbol: Final = {
        "QQQ": "nasdaq_100",
        "XLK": "tech",
        "SMH": "semiconductor",
        "GLD": "gold",
        "IEF": "treasury_mid",
        "UUP": "usd",
    }

    def __init__(
        self,
        *,
        core_symbol: str = "QQQ",
        growth_symbols: tuple[str, ...] | None = None,
        defensive_symbols: tuple[str, ...] | None = None,
        profile_name: str | None = None,
    ) -> None:
        self.core_symbol = core_symbol
        self.growth_symbols = growth_symbols or self.default_growth_symbols
        self.defensive_symbols = defensive_symbols or self.default_defensive_symbols
        self.profile_name = profile_name or self.default_profile_name

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
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("tag"))
        if "regime" not in df.columns:
            df = df.with_columns(pl.lit(REGIME_NEUTRAL).alias("regime"))

        decisions: list[dict[str, object]] = []
        for _, group in df.sort(["time", "symbol"]).group_by("time", maintain_order=True):
            decisions.extend(self._build_period_decisions(group))

        if not decisions:
            return self.empty_decisions()

        return (
            pl.DataFrame(decisions, schema=STRATEGY_DECISION_SCHEMA)
            .sort(["time", "rank", "symbol"])
        )

    def _build_period_decisions(self, period_df: pl.DataFrame) -> list[dict[str, object]]:
        rows = {str(row["symbol"]): row for row in period_df.iter_rows(named=True)}
        regime = self._resolve_regime(period_df)
        target_weights = self._target_weights_for_regime(regime, rows)
        if not target_weights:
            return []

        total_weight = sum(target_weights.values())
        if total_weight <= 0:
            return []

        normalized_targets = {
            symbol: weight / total_weight
            for symbol, weight in target_weights.items()
            if weight > 0 and symbol in rows
        }
        ranked_symbols = sorted(
            normalized_targets,
            key=lambda symbol: (
                -normalized_targets[symbol],
                -float(rows[symbol].get("composite_score") or 0.0),
                symbol,
            ),
        )

        decisions: list[dict[str, object]] = []
        for rank, symbol in enumerate(ranked_symbols, start=1):
            row = rows[symbol]
            raw_tag = row.get("tag")
            tag = str(raw_tag) if raw_tag is not None else self.default_tag_by_symbol.get(symbol, "other")
            source_weight = target_weights[symbol]
            metadata = {
                "rank": rank,
                "tag": tag,
                "profile": self.profile_name,
                "regime": regime,
                "template": "fixed_qqq_core_6_1",
                "source_weight": source_weight,
            }
            decisions.append(
                {
                    "time": row["time"],
                    "asset_type": row["asset_type"],
                    "strategy": self.strategy_name,
                    "strategy_mode": self.strategy_mode,
                    "symbol": symbol,
                    "decision_type": "target_weight",
                    "signal": 1,
                    "target_weight": normalized_targets[symbol],
                    "score": float(row.get("composite_score") or 0.0),
                    "rank": rank,
                    "tag": tag,
                    "metadata": json.dumps(metadata, ensure_ascii=False),
                }
            )
        return decisions

    def _resolve_regime(self, period_df: pl.DataFrame) -> str:
        regime_values = (
            period_df.get_column("regime")
            .drop_nulls()
            .cast(pl.Utf8)
            .str.to_lowercase()
            .to_list()
        )
        if not regime_values:
            return REGIME_NEUTRAL

        regime = str(regime_values[0]).replace("-", "_")
        if regime in {REGIME_RISK_ON, "on", "riskon"}:
            return REGIME_RISK_ON
        if regime in {REGIME_RISK_OFF, "off", "riskoff"}:
            return REGIME_RISK_OFF
        return REGIME_NEUTRAL

    def _target_weights_for_regime(
        self,
        regime: str,
        rows_by_symbol: dict[str, dict[str, object]],
    ) -> dict[str, float]:
        if regime == REGIME_RISK_ON:
            growth_leader = self._growth_leader(rows_by_symbol)
            targets = {self.core_symbol: 0.70}
            if growth_leader is not None:
                targets[growth_leader] = targets.get(growth_leader, 0.0) + 0.30
            return self._available_targets(targets, rows_by_symbol)

        if regime == REGIME_RISK_OFF:
            return self._available_targets(
                {
                    self.core_symbol: 0.25,
                    "GLD": 0.35,
                    "IEF": 0.25,
                    "UUP": 0.15,
                },
                rows_by_symbol,
            )

        return self._available_targets(
            {
                self.core_symbol: 0.50,
                "GLD": 0.25,
                "IEF": 0.25,
            },
            rows_by_symbol,
        )

    def _growth_leader(self, rows_by_symbol: dict[str, dict[str, object]]) -> str | None:
        candidates = [symbol for symbol in self.growth_symbols if symbol in rows_by_symbol]
        if not candidates:
            return None
        return max(
            candidates,
            key=lambda symbol: (
                float(rows_by_symbol[symbol].get("composite_score") or 0.0),
                symbol,
            ),
        )

    @staticmethod
    def _available_targets(
        targets: dict[str, float],
        rows_by_symbol: dict[str, dict[str, object]],
    ) -> dict[str, float]:
        return {
            symbol: weight
            for symbol, weight in targets.items()
            if symbol in rows_by_symbol and weight > 0
        }

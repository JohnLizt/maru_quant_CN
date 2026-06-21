"""Strategy base contracts shared by app-facing snapshots and backtests."""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Literal

import polars as pl


SignalMode = Literal["cross_sectional", "time_series"]
DecisionType = Literal["target_weight", "entry_exit_signal"]

STRATEGY_DECISION_SCHEMA: dict[str, pl.DataType] = {
    "time": pl.Datetime("us", "UTC"),
    "asset_type": pl.Utf8,
    "strategy": pl.Utf8,
    "strategy_mode": pl.Utf8,
    "symbol": pl.Utf8,
    "decision_type": pl.Utf8,
    "signal": pl.Int64,
    "target_weight": pl.Float64,
    "score": pl.Float64,
    "rank": pl.UInt32,
    "tag": pl.Utf8,
    "metadata": pl.Utf8,
}


class BaseStrategy(ABC):
    """Base strategy that consumes signal snapshots and emits decision tables."""

    strategy_name: str
    strategy_mode: SignalMode
    supported_signal_modes: tuple[SignalMode, ...]
    supported_asset_types: tuple[str, ...]

    def empty_decisions(self) -> pl.DataFrame:
        return pl.DataFrame(schema=STRATEGY_DECISION_SCHEMA)

    def validate_signal_snapshot(self, signal_snapshot: pl.DataFrame) -> None:
        if signal_snapshot.is_empty():
            return

        required_columns = {"time", "asset_type", "signal_mode", "symbol", "composite_score"}
        missing_columns = required_columns - set(signal_snapshot.columns)
        if missing_columns:
            raise ValueError(f"signal snapshot 缺少列: {sorted(missing_columns)}")

        snapshot_modes = {
            str(value)
            for value in signal_snapshot.get_column("signal_mode").drop_nulls().unique().to_list()
        }
        unsupported_modes = snapshot_modes - set(self.supported_signal_modes)
        if unsupported_modes:
            raise ValueError(
                f"strategy={self.strategy_name} 不支持 signal_mode={sorted(unsupported_modes)}"
            )

        asset_types = {
            str(value)
            for value in signal_snapshot.get_column("asset_type").drop_nulls().unique().to_list()
        }
        if "*" in self.supported_asset_types:
            return
        unsupported_assets = asset_types - set(self.supported_asset_types)
        if unsupported_assets:
            raise ValueError(
                f"strategy={self.strategy_name} 不支持 asset_type={sorted(unsupported_assets)}"
            )

    @abstractmethod
    def build_decisions(
        self,
        signal_snapshot: pl.DataFrame,
        as_of_date: date | None = None,
    ) -> pl.DataFrame:
        """Build strategy decisions from a signal snapshot."""

    def generate_signals(
        self,
        signal_snapshot: pl.DataFrame,
        universe: list[str] | None = None,
    ) -> pl.DataFrame:
        """Backward-compatible alias used by older callers/tests."""

        decisions = self.build_decisions(signal_snapshot)
        if universe:
            decisions = decisions.filter(pl.col("symbol").is_in(universe))
        return decisions

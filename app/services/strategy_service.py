"""Strategy service layer bridging signal snapshots to app/backtest consumers."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

import polars as pl

from app.services.signal_score import build_signal_snapshot
from app.strategy.base import BaseStrategy


@dataclass(frozen=True)
class StrategySnapshotBundle:
    signal_snapshot: pl.DataFrame
    decisions: pl.DataFrame


def build_strategy_snapshot(
    strategy: BaseStrategy,
    *,
    symbols: list[str] | None = None,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    asset_type: str | None = None,
    universe: str | None = None,
    profile_name: str,
    as_of_date: date | None = None,
    extra_factor_names: list[str] | None = None,
) -> StrategySnapshotBundle:
    _, signal_snapshot = build_signal_snapshot(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        asset_type=asset_type,
        universe=universe,
        profile_name=profile_name,
        extra_factor_names=extra_factor_names,
    )
    decisions = strategy.build_decisions(signal_snapshot, as_of_date=as_of_date)
    return StrategySnapshotBundle(signal_snapshot=signal_snapshot, decisions=decisions)


def run_strategy_snapshot(
    strategy: BaseStrategy,
    *,
    symbols: list[str] | None = None,
    target_date: str | date | datetime | None = None,
    asset_type: str | None = None,
    universe: str | None = None,
    profile_name: str,
    extra_factor_names: list[str] | None = None,
) -> pl.DataFrame:
    if target_date is None:
        as_of_date = None
        start_date = None
        end_date = None
    elif isinstance(target_date, datetime):
        as_of_date = target_date.date()
        start_date = as_of_date
        end_date = as_of_date
    elif isinstance(target_date, date):
        as_of_date = target_date
        start_date = as_of_date
        end_date = as_of_date
    else:
        as_of_date = datetime.strptime(target_date, "%Y-%m-%d").date()
        start_date = as_of_date
        end_date = as_of_date

    bundle = build_strategy_snapshot(
        strategy,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        asset_type=asset_type,
        universe=universe,
        profile_name=profile_name,
        as_of_date=as_of_date,
        extra_factor_names=extra_factor_names,
    )
    return bundle.decisions


def run_strategy_history(
    strategy: BaseStrategy,
    *,
    symbols: list[str] | None = None,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    asset_type: str | None = None,
    universe: str | None = None,
    profile_name: str,
    extra_factor_names: list[str] | None = None,
) -> pl.DataFrame:
    bundle = build_strategy_snapshot(
        strategy,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        asset_type=asset_type,
        universe=universe,
        profile_name=profile_name,
        extra_factor_names=extra_factor_names,
    )
    return bundle.decisions

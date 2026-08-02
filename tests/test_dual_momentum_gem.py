from __future__ import annotations

import json
from datetime import date, datetime, timezone

import polars as pl
import pytest

from app.strategy.dual_momentum_gem import (
    GEM_SIGNAL_SCHEMA,
    DualMomentumGEMStrategy,
    build_gem_signal_snapshot,
)


def _signal_row(
    signal_date: date,
    *,
    us_return: float,
    ex_us_return: float,
    t_bill_return: float,
) -> dict[str, object]:
    return {
        "time": datetime.combine(signal_date, datetime.min.time(), tzinfo=timezone.utc),
        "asset_type": "etf_US",
        "signal_mode": "time_series",
        "symbol": "SPY",
        "composite_score": us_return - t_bill_return,
        "us_return_12m": us_return,
        "ex_us_return_12m": ex_us_return,
        "t_bill_return_12m": t_bill_return,
    }


def _market_rows() -> list[dict[str, object]]:
    prices = {
        "SPY": [(200.0, 0.50), (110.0, 1.0)],
        "ACWX": [(100.0, 1.0), (120.0, 1.0)],
        "AGG": [(100.0, 1.0), (102.0, 1.0)],
        "BIL": [(100.0, 1.0), (104.0, 1.0)],
    }
    dates = [date(2024, 1, 31), date(2025, 1, 31)]
    rows: list[dict[str, object]] = []
    for symbol, values in prices.items():
        for current_date, (close, adj_factor) in zip(dates, values, strict=True):
            rows.append(
                {
                    "time": current_date,
                    "asset_type": "etf_US",
                    "symbol": symbol,
                    "close": close,
                    "adj_factor": adj_factor,
                }
            )
    return rows


def test_build_gem_signal_snapshot_uses_month_end_total_returns() -> None:
    snapshot = build_gem_signal_snapshot(pl.DataFrame(_market_rows()))

    assert snapshot.schema == GEM_SIGNAL_SCHEMA
    assert snapshot.height == 1
    row = snapshot.row(0, named=True)
    assert row["time"].date() == date(2025, 1, 31)
    assert row["us_return_12m"] == pytest.approx(0.10)
    assert row["ex_us_return_12m"] == pytest.approx(0.20)
    assert row["t_bill_return_12m"] == pytest.approx(0.04)


def test_build_gem_signal_snapshot_requires_all_four_assets() -> None:
    market_data = pl.DataFrame(_market_rows()).filter(pl.col("symbol") != "AGG")

    with pytest.raises(ValueError, match="AGG"):
        build_gem_signal_snapshot(market_data)


def test_build_gem_signal_snapshot_returns_empty_during_warmup() -> None:
    market_data = pl.DataFrame(_market_rows()).filter(pl.col("time") == date(2024, 1, 31))

    assert build_gem_signal_snapshot(market_data).is_empty()


@pytest.mark.parametrize(
    ("us_return", "ex_us_return", "t_bill_return", "expected_symbol", "expected_reason"),
    [
        (0.12, 0.08, 0.03, "SPY", "us_relative_momentum_winner"),
        (0.10, 0.18, 0.03, "ACWX", "ex_us_relative_momentum_winner"),
        (-0.05, 0.20, 0.02, "AGG", "us_absolute_momentum_failed"),
        (0.03, 0.20, 0.03, "AGG", "us_absolute_momentum_failed"),
    ],
)
def test_gem_strategy_follows_absolute_then_relative_momentum(
    us_return: float,
    ex_us_return: float,
    t_bill_return: float,
    expected_symbol: str,
    expected_reason: str,
) -> None:
    snapshot = pl.DataFrame(
        [
            _signal_row(
                date(2025, 1, 31),
                us_return=us_return,
                ex_us_return=ex_us_return,
                t_bill_return=t_bill_return,
            )
        ],
        schema=GEM_SIGNAL_SCHEMA,
    )

    decision = DualMomentumGEMStrategy().build_decisions(snapshot).row(0, named=True)
    metadata = json.loads(decision["metadata"])

    assert decision["symbol"] == expected_symbol
    assert decision["target_weight"] == pytest.approx(1.0)
    assert metadata["selected_symbol"] == expected_symbol
    assert metadata["selection_reason"] == expected_reason
    assert metadata["absolute_momentum_pass"] is (us_return > t_bill_return)


def test_gem_strategy_filters_as_of_date_and_records_required_metadata() -> None:
    snapshot = pl.DataFrame(
        [
            _signal_row(date(2025, 1, 31), us_return=0.10, ex_us_return=0.08, t_bill_return=0.02),
            _signal_row(date(2025, 2, 28), us_return=-0.02, ex_us_return=0.20, t_bill_return=0.03),
        ],
        schema=GEM_SIGNAL_SCHEMA,
    )

    decisions = DualMomentumGEMStrategy().build_decisions(snapshot, as_of_date=date(2025, 2, 28))
    metadata = json.loads(decisions.get_column("metadata").item())

    assert decisions.get_column("symbol").to_list() == ["AGG"]
    assert {
        "us_return_12m",
        "ex_us_return_12m",
        "t_bill_return_12m",
        "absolute_momentum_pass",
        "selected_symbol",
        "selection_reason",
    }.issubset(metadata)


def test_gem_strategy_rejects_incomplete_signal_snapshot() -> None:
    incomplete = pl.DataFrame({"time": [datetime(2025, 1, 31, tzinfo=timezone.utc)]})

    with pytest.raises(ValueError, match="GEM signal_snapshot 缺少列"):
        DualMomentumGEMStrategy().build_decisions(incomplete)


def test_gem_strategy_requires_distinct_role_symbols() -> None:
    with pytest.raises(ValueError, match="四个角色"):
        DualMomentumGEMStrategy(us_symbol="SPY", ex_us_symbol="SPY")

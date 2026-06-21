from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from app.backtest.risk_overlay import RiskOverlayConfig, apply_risk_overlay


def _holdings(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows).with_columns(
        [
            pl.col("time").cast(pl.Date),
            pl.col("holding_start_date").cast(pl.Date),
        ]
    )


def _market(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows).with_columns(pl.col("time").cast(pl.Date))


def test_new_buy_high_std_is_halved_once() -> None:
    holdings = _holdings(
        [
            {"time": date(2026, 1, 1), "symbol": "AAA", "target_weight": 0.2, "rank": 1, "holding_start_date": date(2026, 1, 1)},
            {"time": date(2026, 1, 2), "symbol": "AAA", "target_weight": 0.2, "rank": 1, "holding_start_date": date(2026, 1, 1)},
        ]
    )
    market = _market(
        [
            {"time": date(2026, 1, 1), "symbol": "AAA", "close": 100.0, "std_score": 0.04, "cv": 0.1},
            {"time": date(2026, 1, 2), "symbol": "AAA", "close": 101.0, "std_score": 0.04, "cv": 0.1},
        ]
    )

    result, metrics = apply_risk_overlay(holdings, market, RiskOverlayConfig())

    assert result.get_column("target_weight").to_list() == pytest.approx([0.1, 0.1])
    assert result.get_column("base_target_weight").to_list() == pytest.approx([0.2, 0.2])
    assert result.get_column("risk_reason").to_list() == ["risk_half_std", "risk_half_std"]
    assert metrics == {"risk_half_events": 1, "stop_loss_events": 0}


def test_new_buy_high_cv_is_halved() -> None:
    holdings = _holdings(
        [
            {"time": date(2026, 1, 1), "symbol": "AAA", "target_weight": 0.2, "rank": 1, "holding_start_date": date(2026, 1, 1)}
        ]
    )
    market = _market(
        [
            {"time": date(2026, 1, 1), "symbol": "AAA", "close": 100.0, "std_score": 0.01, "cv": 0.8}
        ]
    )

    result, metrics = apply_risk_overlay(holdings, market, RiskOverlayConfig())

    assert result.get_column("target_weight").item() == pytest.approx(0.1)
    assert result.get_column("risk_reason").item() == "risk_half_cv"
    assert metrics["risk_half_events"] == 1


def test_stop_loss_zeroes_weight_until_next_holding_cycle() -> None:
    holdings = _holdings(
        [
            {"time": date(2026, 1, 1), "symbol": "AAA", "target_weight": 0.2, "rank": 1, "holding_start_date": date(2026, 1, 1)},
            {"time": date(2026, 1, 2), "symbol": "AAA", "target_weight": 0.2, "rank": 1, "holding_start_date": date(2026, 1, 1)},
            {"time": date(2026, 1, 3), "symbol": "AAA", "target_weight": 0.2, "rank": 1, "holding_start_date": date(2026, 1, 1)},
            {"time": date(2026, 1, 4), "symbol": "AAA", "target_weight": 0.2, "rank": 1, "holding_start_date": date(2026, 1, 4)},
        ]
    )
    market = _market(
        [
            {"time": date(2026, 1, 1), "symbol": "AAA", "close": 100.0, "std_score": 0.0, "cv": 0.0},
            {"time": date(2026, 1, 2), "symbol": "AAA", "close": 89.0, "std_score": 0.0, "cv": 0.0},
            {"time": date(2026, 1, 3), "symbol": "AAA", "close": 95.0, "std_score": 0.0, "cv": 0.0},
            {"time": date(2026, 1, 4), "symbol": "AAA", "close": 95.0, "std_score": 0.0, "cv": 0.0},
        ]
    )

    result, metrics = apply_risk_overlay(holdings, market, RiskOverlayConfig(stop_loss_rate=0.1))

    assert result.get_column("target_weight").to_list() == pytest.approx([0.2, 0.0, 0.0, 0.2])
    assert result.get_column("risk_reason").to_list() == ["", "stop_loss", "stop_loss", ""]
    assert metrics == {"risk_half_events": 0, "stop_loss_events": 1}

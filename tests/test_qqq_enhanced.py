from __future__ import annotations

import json
from datetime import datetime, timezone

import polars as pl
import pytest

from app.strategy.qqq_enhanced import QQQEnhancedFixedCoreStrategy, QQQOnlyStrategy, QQQOnlyTrailingStopStrategy


def _snapshot(rows: list[dict[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            "time": pl.Datetime("us", "UTC"),
            "asset_type": pl.Utf8,
            "signal_mode": pl.Utf8,
            "symbol": pl.Utf8,
            "tag": pl.Utf8,
            "composite_score": pl.Float64,
            "regime": pl.Utf8,
        },
    )


def test_qqq_only_strategy_emits_single_full_weight_qqq_decision() -> None:
    ts = datetime(2026, 5, 27, tzinfo=timezone.utc)
    strategy = QQQOnlyStrategy()
    df = _snapshot(
        [
            {"time": ts, "asset_type": "etf_US", "signal_mode": "cross_sectional", "symbol": "QQQ", "tag": "nasdaq_100", "composite_score": 1.0, "regime": None},
            {"time": ts, "asset_type": "etf_US", "signal_mode": "cross_sectional", "symbol": "SMH", "tag": "semiconductor", "composite_score": 0.9, "regime": None},
        ]
    )

    result = strategy.build_decisions(df)

    assert result.height == 1
    assert result.get_column("symbol").to_list() == ["QQQ"]
    assert result.get_column("target_weight").to_list() == [1.0]
    metadata = json.loads(result.get_column("metadata").item())
    assert metadata["template"] == "pure_qqq"


def test_qqq_only_trailing_stop_moves_between_qqq_and_cash() -> None:
    ts1 = datetime(2026, 5, 25, tzinfo=timezone.utc)
    ts2 = datetime(2026, 5, 26, tzinfo=timezone.utc)
    ts3 = datetime(2026, 5, 27, tzinfo=timezone.utc)
    strategy = QQQOnlyTrailingStopStrategy(trailing_stop_rate=0.10, trailing_peak_window=3)
    df = pl.DataFrame(
        [
            {"time": ts1, "asset_type": "etf_US", "signal_mode": "cross_sectional", "symbol": "QQQ", "tag": "nasdaq_100", "composite_score": 1.0, "close": 100.0},
            {"time": ts2, "asset_type": "etf_US", "signal_mode": "cross_sectional", "symbol": "QQQ", "tag": "nasdaq_100", "composite_score": 1.0, "close": 89.0},
            {"time": ts3, "asset_type": "etf_US", "signal_mode": "cross_sectional", "symbol": "QQQ", "tag": "nasdaq_100", "composite_score": 1.0, "close": 91.0},
        ],
        schema={
            "time": pl.Datetime("us", "UTC"),
            "asset_type": pl.Utf8,
            "signal_mode": pl.Utf8,
            "symbol": pl.Utf8,
            "tag": pl.Utf8,
            "composite_score": pl.Float64,
            "close": pl.Float64,
        },
    )

    result = strategy.build_decisions(df)

    assert result.get_column("symbol").to_list() == ["QQQ", "CASH", "QQQ"]
    metadata = [json.loads(value) for value in result.get_column("metadata").to_list()]
    assert metadata[1]["risk_on"] is False
    assert metadata[1]["stop_line"] == pytest.approx(90.0)


def test_qqq_enhanced_risk_on_uses_qqq_and_best_growth_leader() -> None:
    ts = datetime(2026, 5, 27, tzinfo=timezone.utc)
    strategy = QQQEnhancedFixedCoreStrategy()
    df = _snapshot(
        [
            {"time": ts, "asset_type": "etf_US", "signal_mode": "cross_sectional", "symbol": "QQQ", "tag": "nasdaq_100", "composite_score": 0.7, "regime": "risk_on"},
            {"time": ts, "asset_type": "etf_US", "signal_mode": "cross_sectional", "symbol": "XLK", "tag": "tech", "composite_score": 0.8, "regime": "risk_on"},
            {"time": ts, "asset_type": "etf_US", "signal_mode": "cross_sectional", "symbol": "SMH", "tag": "semiconductor", "composite_score": 0.9, "regime": "risk_on"},
            {"time": ts, "asset_type": "etf_US", "signal_mode": "cross_sectional", "symbol": "GLD", "tag": "gold", "composite_score": 0.2, "regime": "risk_on"},
        ]
    )

    result = strategy.build_decisions(df)

    assert result.get_column("symbol").to_list() == ["QQQ", "SMH"]
    assert result.get_column("target_weight").to_list() == pytest.approx([0.7, 0.3])
    metadata = json.loads(result.filter(pl.col("symbol") == "SMH").get_column("metadata").item())
    assert metadata["regime"] == "risk_on"
    assert metadata["template"] == "fixed_qqq_core_6_1"


def test_qqq_enhanced_neutral_uses_qqq_gld_ief_template() -> None:
    ts = datetime(2026, 5, 27, tzinfo=timezone.utc)
    strategy = QQQEnhancedFixedCoreStrategy()
    df = _snapshot(
        [
            {"time": ts, "asset_type": "etf_US", "signal_mode": "cross_sectional", "symbol": "QQQ", "tag": "nasdaq_100", "composite_score": 0.7, "regime": "neutral"},
            {"time": ts, "asset_type": "etf_US", "signal_mode": "cross_sectional", "symbol": "GLD", "tag": "gold", "composite_score": 0.6, "regime": "neutral"},
            {"time": ts, "asset_type": "etf_US", "signal_mode": "cross_sectional", "symbol": "IEF", "tag": "treasury_mid", "composite_score": 0.5, "regime": "neutral"},
            {"time": ts, "asset_type": "etf_US", "signal_mode": "cross_sectional", "symbol": "SMH", "tag": "semiconductor", "composite_score": 0.9, "regime": "neutral"},
        ]
    )

    result = strategy.build_decisions(df)

    weights = dict(zip(result.get_column("symbol").to_list(), result.get_column("target_weight").to_list()))
    assert weights == pytest.approx({"QQQ": 0.5, "GLD": 0.25, "IEF": 0.25})


def test_qqq_enhanced_risk_off_uses_defensive_template_and_renormalizes_missing_uup() -> None:
    ts = datetime(2026, 5, 27, tzinfo=timezone.utc)
    strategy = QQQEnhancedFixedCoreStrategy()
    df = _snapshot(
        [
            {"time": ts, "asset_type": "etf_US", "signal_mode": "cross_sectional", "symbol": "QQQ", "tag": "nasdaq_100", "composite_score": 0.7, "regime": "risk_off"},
            {"time": ts, "asset_type": "etf_US", "signal_mode": "cross_sectional", "symbol": "GLD", "tag": "gold", "composite_score": 0.6, "regime": "risk_off"},
            {"time": ts, "asset_type": "etf_US", "signal_mode": "cross_sectional", "symbol": "IEF", "tag": "treasury_mid", "composite_score": 0.5, "regime": "risk_off"},
        ]
    )

    result = strategy.build_decisions(df)

    weights = dict(zip(result.get_column("symbol").to_list(), result.get_column("target_weight").to_list()))
    assert weights == pytest.approx(
        {
            "QQQ": 0.25 / 0.85,
            "GLD": 0.35 / 0.85,
            "IEF": 0.25 / 0.85,
        }
    )
    assert sum(weights.values()) == pytest.approx(1.0)

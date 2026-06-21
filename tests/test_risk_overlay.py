from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from app.backtest.risk_overlay import RiskOverlayConfig, build_risk_features, get_risk_reason


def test_get_risk_reason_identifies_high_std_and_cv() -> None:
    config = RiskOverlayConfig()

    assert get_risk_reason(0.04, 0.8, config) == "risk_half_std_cv"
    assert get_risk_reason(0.04, 0.1, config) == "risk_half_std"
    assert get_risk_reason(0.01, 0.8, config) == "risk_half_cv"
    assert get_risk_reason(0.01, 0.1, config) is None


def test_build_risk_features_computes_std_and_cv() -> None:
    rows = []
    for idx in range(25):
        rows.append(
            {
                "time": date(2026, 1, 1) + timedelta(days=idx),
                "symbol": "AAA",
                "close": 100.0 + ((-1) ** idx) * idx,
                "amount": 1000.0 + idx * 20.0,
            }
        )
    market = pl.DataFrame(rows)

    result = build_risk_features(market, RiskOverlayConfig())

    assert "std_score" in result.columns
    assert "cv" in result.columns
    last = result.sort("time").tail(1).row(0, named=True)
    assert last["std_score"] is not None
    assert last["cv"] is not None


def test_build_risk_features_requires_amount() -> None:
    market = pl.DataFrame([{"time": date(2026, 1, 1), "symbol": "AAA", "close": 100.0}])

    with pytest.raises(ValueError, match="缺少列"):
        build_risk_features(market, RiskOverlayConfig())

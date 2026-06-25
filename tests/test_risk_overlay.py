from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from app.backtest.risk_overlay import (
    RiskOverlayConfig,
    add_risk_feature_columns,
    build_risk_features,
    get_risk_reason,
)
from app.factors.risk import CVFactor, StdScoreFactor


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


def test_add_risk_feature_columns_matches_build_risk_features() -> None:
    rows = []
    for idx in range(30):
        rows.append(
            {
                "time": date(2026, 1, 1) + timedelta(days=idx),
                "symbol": "AAA",
                "close": 100.0 + ((-1) ** idx) * idx,
                "amount": 1000.0 + idx * 30.0,
            }
        )
    market = pl.DataFrame(rows)

    expected = build_risk_features(market, RiskOverlayConfig()).sort("time")
    result = add_risk_feature_columns(market, RiskOverlayConfig()).sort("time")

    assert result.get_column("std_score").to_list() == pytest.approx(
        expected.get_column("std_score").to_list(),
        nan_ok=True,
    )
    assert result.get_column("cv").to_list() == pytest.approx(
        expected.get_column("cv").to_list(),
        nan_ok=True,
    )


def test_risk_factors_match_overlay_output() -> None:
    rows = []
    for idx in range(30):
        rows.append(
            {
                "time": date(2026, 1, 1) + timedelta(days=idx),
                "asset_type": "etf_CN",
                "symbol": "AAA",
                "close": 100.0 + ((-1) ** idx) * idx,
                "amount": 1000.0 + idx * 25.0,
            }
        )
    market = pl.DataFrame(rows)

    overlay = add_risk_feature_columns(market, RiskOverlayConfig()).sort("time")
    std_result = StdScoreFactor().compute(market).rename({"factor_value": "std_score"}).sort("time")
    cv_result = CVFactor().compute(market).rename({"factor_value": "cv"}).sort("time")

    assert std_result.get_column("std_score").to_list() == pytest.approx(
        overlay.drop_nulls("std_score").get_column("std_score").to_list()
    )
    assert cv_result.get_column("cv").to_list() == pytest.approx(
        overlay.drop_nulls("cv").get_column("cv").to_list()
    )


def test_build_risk_features_requires_amount() -> None:
    market = pl.DataFrame([{"time": date(2026, 1, 1), "symbol": "AAA", "close": 100.0}])

    with pytest.raises(ValueError, match="缺少列"):
        build_risk_features(market, RiskOverlayConfig())

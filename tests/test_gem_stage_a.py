from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest

from app.backtest.baselines import (
    GEM_STATIC_GAA_WEIGHTS,
    build_cash_return_series,
    build_gem_baseline_decisions,
)
from app.backtest.risk_overlay import RiskOverlayConfig
from app.backtest.runner import (
    _apply_total_return_adjustment,
    _filter_rebalance_dates,
    run_backtest,
)
from app.data_loader.market_data import upsert_daily


def _decision(symbol: str, timestamp: datetime) -> dict[str, object]:
    return {
        "time": timestamp,
        "asset_type": "etf_US",
        "strategy": "test",
        "strategy_mode": "cross_sectional",
        "symbol": symbol,
        "decision_type": "target_weight",
        "signal": 1,
        "target_weight": 1.0,
        "score": 1.0,
        "rank": 1,
        "tag": "benchmark",
        "metadata": "{}",
    }


def _market_bar(current_date: date, *, adj_factor: float | None = None) -> dict[str, object]:
    row: dict[str, object] = {
        "time": current_date,
        "asset_type": "etf_US",
        "symbol": "SPY",
        "open": 100.0,
        "high": 100.0,
        "low": 100.0,
        "close": 100.0,
        "amount": 1000.0,
        "daily_return": 0.0,
        "ohlc4": 100.0,
    }
    if adj_factor is not None:
        row["adj_factor"] = adj_factor
    return row


def test_total_return_adjustment_applies_factor_to_ohlc_prices() -> None:
    market_data = pl.DataFrame(
        [
            _market_bar(date(2026, 1, 2), adj_factor=0.5),
            _market_bar(date(2026, 1, 5), adj_factor=1.0),
        ]
    )

    adjusted = _apply_total_return_adjustment(market_data)

    assert "adj_factor" not in adjusted.columns
    assert adjusted.get_column("close").to_list() == pytest.approx([50.0, 100.0])
    assert adjusted.get_column("ohlc4").to_list() == pytest.approx([50.0, 100.0])


def test_monthly_rebalance_keeps_last_available_decision_in_each_month() -> None:
    timestamps = [
        datetime(2026, 1, 29, tzinfo=timezone.utc),
        datetime(2026, 1, 30, tzinfo=timezone.utc),
        datetime(2026, 2, 26, tzinfo=timezone.utc),
        datetime(2026, 2, 27, tzinfo=timezone.utc),
    ]
    decisions = pl.DataFrame([_decision(f"S{idx}", timestamp) for idx, timestamp in enumerate(timestamps)])

    filtered = _filter_rebalance_dates(decisions, "monthly", None)

    assert filtered.get_column("symbol").to_list() == ["S1", "S3"]


def test_gem_stage_a_baselines_use_standard_decision_schema() -> None:
    vti = build_gem_baseline_decisions("vti", "2026-01-02")
    spy = build_gem_baseline_decisions("spy", "2026-01-02")
    static_gaa = build_gem_baseline_decisions("static_gaa", "2026-01-02")

    assert vti.get_column("symbol").to_list() == ["VTI"]
    assert spy.get_column("symbol").to_list() == ["SPY"]
    assert static_gaa.get_column("symbol").to_list() == list(GEM_STATIC_GAA_WEIGHTS)
    assert static_gaa.get_column("target_weight").sum() == pytest.approx(1.0)
    assert static_gaa.get_column("tag").to_list() == ["us_equity", "ex_us_equity", "bond"]


def test_build_cash_return_series_uses_adjusted_total_return_price() -> None:
    market_data = pl.DataFrame(
        {
            "time": [date(2026, 1, 2), date(2026, 1, 5)],
            "symbol": ["BIL", "BIL"],
            "close": [100.0, 100.0],
            "adj_factor": [0.90, 0.91],
        }
    )

    cash_returns = build_cash_return_series(market_data)

    assert cash_returns.get_column("time").to_list() == [date(2026, 1, 5)]
    assert cash_returns.get_column("cash_return").item() == pytest.approx(91.0 / 90.0 - 1.0)


def test_run_backtest_applies_dynamic_cash_returns(monkeypatch: pytest.MonkeyPatch) -> None:
    signal_time = datetime(2026, 1, 2, tzinfo=timezone.utc)
    decisions = pl.DataFrame([_decision("SPY", signal_time)])
    market_data = pl.DataFrame(
        [
            _market_bar(date(2026, 1, 2)),
            _market_bar(date(2026, 1, 5)),
        ]
    ).with_columns(
        pl.lit(0.04).alias("std_score"),
        pl.lit(0.0).alias("cv"),
    )
    monkeypatch.setattr("app.backtest.runner.build_risk_features", lambda frame, _config: frame)
    cash_returns = pl.DataFrame(
        {
            "time": [date(2026, 1, 2), date(2026, 1, 5)],
            "cash_return": [0.10, 0.20],
        }
    )

    result = run_backtest(
        decisions,
        asset_type="etf_US",
        start="2026-01-02",
        end="2026-01-05",
        execution_lag=0,
        initial_capital=1000.0,
        commission_min=0.0,
        cash_interest_rate=0.0,
        cash_return_series=cash_returns,
        market_data_override=market_data,
        risk_config=RiskOverlayConfig(),
    )

    assert result.returns_df.get_column("cash").to_list() == pytest.approx([550.0, 660.0])
    assert result.metrics["end_nav"] == pytest.approx(1160.0)


def test_upsert_daily_persists_adjustment_factor(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class _Connection:
        def execute(self, statement: object, rows: list[dict[str, object]]) -> None:
            captured["statement"] = str(statement)
            captured["rows"] = rows

    class _Transaction:
        def __enter__(self) -> _Connection:
            return _Connection()

        def __exit__(self, *_args: object) -> None:
            return None

    class _Engine:
        def begin(self) -> _Transaction:
            return _Transaction()

    monkeypatch.setattr("app.data_loader.market_data.get_engine", lambda: _Engine())
    daily = pl.DataFrame(
        {
            "time": [date(2026, 1, 2)],
            "asset_type": ["etf_US"],
            "symbol": ["SPY"],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.0],
            "volume": [1000],
            "amount": [100000.0],
            "adj_factor": [0.95],
            "pct_change": [1.0],
            "is_suspended": [False],
            "data_source": ["yahoo"],
        }
    )

    assert upsert_daily(daily) == 1
    assert captured["rows"][0]["adj_factor"] == pytest.approx(0.95)  # type: ignore[index]
    assert "adj_factor" in captured["statement"]

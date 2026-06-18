from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest

from app.analytics.factor_ic import compute_daily_ic, summarize_daily_ic, summarize_ic_window


def test_compute_daily_ic_lag1_returns_expected_correlations() -> None:
    ts0 = datetime(2026, 5, 27, tzinfo=timezone.utc)
    ts1 = datetime(2026, 5, 28, tzinfo=timezone.utc)
    df_factors = pl.DataFrame(
        [
            {"time": ts0, "symbol": "AAA", "factor_name": "ret_30_rank", "factor_value": 1.0},
            {"time": ts0, "symbol": "BBB", "factor_name": "ret_30_rank", "factor_value": 2.0},
        ]
    )
    df_ret = pl.DataFrame(
        [
            {"time": ts0, "symbol": "AAA", "pct_change": 0.0},
            {"time": ts1, "symbol": "AAA", "pct_change": 10.0},
            {"time": ts0, "symbol": "BBB", "pct_change": 0.0},
            {"time": ts1, "symbol": "BBB", "pct_change": 20.0},
        ]
    )

    result = compute_daily_ic(df_factors, df_ret, 1, {"ret_30_rank": 2})

    assert result.height == 1
    row = result.to_dicts()[0]
    assert row["factor_name"] == "ret_30_rank"
    assert row["lag"] == 1
    assert row["n_stocks"] == 2
    assert row["ic"] == pytest.approx(1.0)
    assert row["rank_ic"] == pytest.approx(1.0)


def test_summarize_daily_ic_returns_expected_ic_ir() -> None:
    ts0 = datetime(2026, 5, 27, tzinfo=timezone.utc)
    ts1 = datetime(2026, 5, 28, tzinfo=timezone.utc)
    ts2 = datetime(2026, 5, 29, tzinfo=timezone.utc)
    daily_ic = pl.DataFrame(
        [
            {"time": ts0, "factor_name": "ret_30_rank", "lag": 5, "ic": 0.10, "rank_ic": 0.20, "n_stocks": 100},
            {"time": ts1, "factor_name": "ret_30_rank", "lag": 5, "ic": 0.20, "rank_ic": 0.30, "n_stocks": 100},
            {"time": ts2, "factor_name": "ret_30_rank", "lag": 5, "ic": 0.30, "rank_ic": 0.40, "n_stocks": 100},
        ]
    )

    summary = summarize_daily_ic(daily_ic, 5)

    assert summary.height == 1
    row = summary.to_dicts()[0]
    assert row["lag"] == 5
    assert row["factor_name"] == "ret_30_rank"
    assert row["mean_ic"] == pytest.approx(0.20)
    assert row["mean_rank_ic"] == pytest.approx(0.30)
    assert row["n_days"] == 3
    assert row["win_rate"] == pytest.approx(1.0)


def test_summarize_ic_window_uses_recent_window_only() -> None:
    ts0 = datetime(2026, 5, 27, tzinfo=timezone.utc)
    ts1 = datetime(2026, 5, 28, tzinfo=timezone.utc)
    ts2 = datetime(2026, 5, 29, tzinfo=timezone.utc)
    daily_ic = pl.DataFrame(
        [
            {"time": ts0, "factor_name": "ret_30_rank", "lag": 10, "ic": 0.10, "rank_ic": 0.10, "n_stocks": 100},
            {"time": ts1, "factor_name": "ret_30_rank", "lag": 10, "ic": 0.20, "rank_ic": 0.20, "n_stocks": 100},
            {"time": ts2, "factor_name": "ret_30_rank", "lag": 10, "ic": 0.40, "rank_ic": 0.30, "n_stocks": 100},
        ]
    )

    summary = summarize_ic_window(
        daily_ic,
        asset_type="etf_CN",
        as_of_dates=[date(2026, 5, 29)],
        window_days=2,
    )

    assert summary.height == 1
    row = summary.to_dicts()[0]
    assert row["asset_type"] == "etf_CN"
    assert row["lag"] == 10
    assert row["window_days"] == 2
    assert row["start_date"] == date(2026, 5, 28)
    assert row["end_date"] == date(2026, 5, 29)
    assert row["mean_ic"] == pytest.approx(0.30)
    assert row["mean_rank_ic"] == pytest.approx(0.25)
    assert row["n_days"] == 2

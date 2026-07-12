from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest

from app.analytics.factor_ic import (
    compute_daily_ic,
    compute_daily_quantile_return,
    compute_daily_topk_return,
    summarize_daily_ic,
    summarize_ic_window,
    summarize_quantile_window,
    summarize_topk_window,
)


def test_compute_daily_ic_lag1_returns_expected_correlations() -> None:
    ts0 = datetime(2026, 5, 27, tzinfo=timezone.utc)
    ts1 = datetime(2026, 5, 28, tzinfo=timezone.utc)
    df_factors = pl.DataFrame(
        [
            {"time": ts0, "symbol": "AAA", "factor_name": "example_factor", "factor_value": 1.0},
            {"time": ts0, "symbol": "BBB", "factor_name": "example_factor", "factor_value": 2.0},
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

    result = compute_daily_ic(df_factors, df_ret, 1, {"example_factor": 2})

    assert result.height == 1
    row = result.to_dicts()[0]
    assert row["factor_name"] == "example_factor"
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
            {"time": ts0, "factor_name": "example_factor", "lag": 5, "ic": 0.10, "rank_ic": 0.20, "n_stocks": 100},
            {"time": ts1, "factor_name": "example_factor", "lag": 5, "ic": 0.20, "rank_ic": 0.30, "n_stocks": 100},
            {"time": ts2, "factor_name": "example_factor", "lag": 5, "ic": 0.30, "rank_ic": 0.40, "n_stocks": 100},
        ]
    )

    summary = summarize_daily_ic(daily_ic, 5)

    assert summary.height == 1
    row = summary.to_dicts()[0]
    assert row["lag"] == 5
    assert row["factor_name"] == "example_factor"
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
            {"time": ts0, "factor_name": "example_factor", "lag": 10, "ic": 0.10, "rank_ic": 0.10, "n_stocks": 100},
            {"time": ts1, "factor_name": "example_factor", "lag": 10, "ic": 0.20, "rank_ic": 0.20, "n_stocks": 100},
            {"time": ts2, "factor_name": "example_factor", "lag": 10, "ic": 0.40, "rank_ic": 0.30, "n_stocks": 100},
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


def test_compute_daily_quantile_return_splits_into_deciles() -> None:
    ts0 = datetime(2026, 5, 27, tzinfo=timezone.utc)
    ts1 = datetime(2026, 5, 28, tzinfo=timezone.utc)
    df_factors = pl.DataFrame(
        [
            {"time": ts0, "symbol": f"S{idx:02d}", "factor_name": "example_factor", "factor_value": float(idx)}
            for idx in range(1, 11)
        ]
    )
    df_ret = pl.DataFrame(
        [
            {"time": ts0, "symbol": f"S{idx:02d}", "pct_change": 0.0}
            for idx in range(1, 11)
        ]
        + [
            {"time": ts1, "symbol": f"S{idx:02d}", "pct_change": float(idx)}
            for idx in range(1, 11)
        ]
    )

    result = compute_daily_quantile_return(df_factors, df_ret, 1, 10, {"example_factor": 10})

    assert result.height == 10
    lowest = result.filter(pl.col("quantile_id") == 1).to_dicts()[0]
    highest = result.filter(pl.col("quantile_id") == 10).to_dicts()[0]
    assert lowest["avg_fwd_ret"] == pytest.approx(0.01)
    assert highest["avg_fwd_ret"] == pytest.approx(0.10)
    assert lowest["n_stocks"] == 1
    assert highest["n_stocks"] == 1


def test_compute_daily_topk_return_returns_absolute_and_excess() -> None:
    ts0 = datetime(2026, 5, 27, tzinfo=timezone.utc)
    ts1 = datetime(2026, 5, 28, tzinfo=timezone.utc)
    df_factors = pl.DataFrame(
        [
            {"time": ts0, "symbol": f"S{idx:02d}", "factor_name": "example_factor", "factor_value": float(idx)}
            for idx in range(1, 11)
        ]
    )
    df_ret = pl.DataFrame(
        [
            {"time": ts0, "symbol": f"S{idx:02d}", "pct_change": 0.0}
            for idx in range(1, 11)
        ]
        + [
            {"time": ts1, "symbol": f"S{idx:02d}", "pct_change": float(idx)}
            for idx in range(1, 11)
        ]
    )

    result = compute_daily_topk_return(df_factors, df_ret, 1, [5, 10], {"example_factor": 10})

    top5 = result.filter(pl.col("top_k") == 5).to_dicts()[0]
    top10 = result.filter(pl.col("top_k") == 10).to_dicts()[0]
    assert top5["topk_ret"] == pytest.approx((0.10 + 0.09 + 0.08 + 0.07 + 0.06) / 5)
    assert top10["universe_ret"] == pytest.approx(0.055)
    assert top5["excess_ret"] == pytest.approx(top5["topk_ret"] - top5["universe_ret"])
    assert top10["topk_ret"] == pytest.approx(top10["universe_ret"])


def test_summarize_quantile_window_uses_recent_window_only() -> None:
    ts0 = datetime(2026, 5, 27, tzinfo=timezone.utc)
    ts1 = datetime(2026, 5, 28, tzinfo=timezone.utc)
    ts2 = datetime(2026, 5, 29, tzinfo=timezone.utc)
    daily_quantile = pl.DataFrame(
        [
            {"time": ts0, "factor_name": "example_factor", "lag": 5, "quantile_n": 10, "quantile_id": 10, "avg_fwd_ret": 0.10, "n_stocks": 20},
            {"time": ts1, "factor_name": "example_factor", "lag": 5, "quantile_n": 10, "quantile_id": 10, "avg_fwd_ret": 0.20, "n_stocks": 20},
            {"time": ts2, "factor_name": "example_factor", "lag": 5, "quantile_n": 10, "quantile_id": 10, "avg_fwd_ret": 0.40, "n_stocks": 20},
        ]
    )

    summary = summarize_quantile_window(
        daily_quantile,
        asset_type="etf_CN",
        as_of_dates=[date(2026, 5, 29)],
        window_days=2,
    )

    row = summary.to_dicts()[0]
    assert row["mean_ret"] == pytest.approx(0.30)
    assert row["quantile_id"] == 10
    assert row["n_days"] == 2
    assert row["start_date"] == date(2026, 5, 28)


def test_summarize_topk_window_tracks_topk_and_excess() -> None:
    ts0 = datetime(2026, 5, 27, tzinfo=timezone.utc)
    ts1 = datetime(2026, 5, 28, tzinfo=timezone.utc)
    ts2 = datetime(2026, 5, 29, tzinfo=timezone.utc)
    daily_topk = pl.DataFrame(
        [
            {"time": ts0, "factor_name": "example_factor", "lag": 5, "top_k": 5, "topk_ret": 0.10, "universe_ret": 0.05, "excess_ret": 0.05, "n_stocks": 5},
            {"time": ts1, "factor_name": "example_factor", "lag": 5, "top_k": 5, "topk_ret": 0.20, "universe_ret": 0.10, "excess_ret": 0.10, "n_stocks": 5},
            {"time": ts2, "factor_name": "example_factor", "lag": 5, "top_k": 5, "topk_ret": 0.40, "universe_ret": 0.20, "excess_ret": 0.20, "n_stocks": 5},
        ]
    )

    summary = summarize_topk_window(
        daily_topk,
        asset_type="etf_CN",
        as_of_dates=[date(2026, 5, 29)],
        window_days=2,
    )

    row = summary.to_dicts()[0]
    assert row["mean_topk_ret"] == pytest.approx(0.30)
    assert row["mean_excess_ret"] == pytest.approx(0.15)
    assert row["top_k"] == 5
    assert row["n_days"] == 2

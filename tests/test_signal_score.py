from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from app.services.signal_score import _finalize_signal_frame
from app.signals.profiles import get_signal_profile


def test_finalize_signal_frame_keeps_cross_sectional_rank() -> None:
    profile = get_signal_profile("trend_etf_momentum_reg20")
    ts = datetime(2026, 6, 30, tzinfo=timezone.utc)
    df = pl.DataFrame(
        [
            {"time": ts, "symbol": "BBB", "composite_score": 0.6},
            {"time": ts, "symbol": "AAA", "composite_score": 0.8},
        ]
    )

    result = _finalize_signal_frame(df, profile=profile)

    assert result.get_column("symbol").to_list() == ["AAA", "BBB"]
    assert result.get_column("rank").to_list() == [1, 2]


def test_finalize_signal_frame_skips_rank_for_time_series() -> None:
    profile = get_signal_profile("trend_v1")
    ts = datetime(2026, 6, 30, tzinfo=timezone.utc)
    df = pl.DataFrame(
        [
            {"time": ts, "symbol": "BBB", "composite_score": 0.1},
            {"time": ts, "symbol": "AAA", "composite_score": 0.8},
        ]
    )

    result = _finalize_signal_frame(df, profile=profile)

    assert result.get_column("symbol").to_list() == ["AAA", "BBB"]
    assert result.get_column("rank").to_list() == [None, None]

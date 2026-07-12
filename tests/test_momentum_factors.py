from __future__ import annotations

from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

from app.factors.momentum import MomentumReg20Factor
from app.factors.registry import resolve_factors


def _build_time(index: int) -> datetime:
    return datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=index)


def test_momentum_reg_20_produces_positive_score_for_clean_uptrend() -> None:
    factor = MomentumReg20Factor()
    df = pl.DataFrame(
        [
            {
                "time": _build_time(idx),
                "symbol": "AAA",
                "close": 100.0 + idx,
                "is_suspended": False,
            }
            for idx in range(25)
        ]
    )

    result = factor.compute(df)

    assert result.height == 5
    latest = result.sort("time").to_dicts()[-1]
    assert latest["factor_name"] == "momentum_reg_20"
    assert latest["factor_value"] > 0.0


def test_resolve_factors_supports_momentum_reg_20_for_etf_us() -> None:
    factors = resolve_factors(["momentum_reg_20"], asset_type="etf_US")

    assert [factor.name for factor in factors] == ["momentum_reg_20"]


def test_resolve_factors_rejects_removed_rank_factor() -> None:
    with pytest.raises(ValueError, match="未知因子"):
        resolve_factors(["momentum_reg_20_rank"], asset_type="etf_US")

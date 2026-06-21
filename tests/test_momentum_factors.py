from __future__ import annotations

from datetime import datetime, timedelta, timezone

import polars as pl

from app.factors.momentum import MomentumReg20Factor, MomentumReg20RankFactor


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


def test_momentum_reg_20_rank_maps_cross_section_to_unit_interval() -> None:
    factor = MomentumReg20RankFactor()
    rows: list[dict[str, object]] = []
    for idx, slope in enumerate([1.0, 2.0, 3.0], start=1):
        for day in range(25):
            rows.append(
                {
                    "time": _build_time(day),
                    "symbol": f"S{idx}",
                    "asset_type": "etf_CN",
                    "close": 100.0 + day * slope,
                    "is_suspended": False,
                }
            )
    df = pl.DataFrame(rows)

    result = factor.compute(df)
    latest_time = result.get_column("time").max()
    latest = result.filter(pl.col("time") == latest_time).sort("symbol")

    assert latest.height == 3
    values = latest.get_column("factor_value").to_list()
    assert values == [0.0, 0.5, 1.0]

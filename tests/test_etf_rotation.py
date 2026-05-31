from __future__ import annotations

import json
from datetime import datetime, timezone

import polars as pl
import pytest

from app.factors.registry import resolve_factors
from app.strategy.etf_rotation import ETFUniverseRotationStrategy


def test_resolve_factors_filters_by_asset_type() -> None:
    stock_factors = [factor.name for factor in resolve_factors(asset_type="stock_CN")]
    etf_factors = [factor.name for factor in resolve_factors(asset_type="etf_CN")]

    assert "limit_up" in stock_factors
    assert "limit_up" not in etf_factors
    assert etf_factors == ["price_to_ma20", "ma_cross", "rsi14", "macd_norm"]


def test_resolve_factors_rejects_unsupported_factor_for_etf() -> None:
    with pytest.raises(ValueError, match="不支持因子"):
        resolve_factors(["limit_up"], asset_type="etf_CN")


def test_etf_rotation_strategy_selects_top_n_and_emits_metadata() -> None:
    strategy = ETFUniverseRotationStrategy(top_n=2, profile_name="trend_v1")
    ts = datetime(2026, 5, 30, tzinfo=timezone.utc)
    df = pl.DataFrame(
        [
            {"time": ts, "asset_type": "etf_CN", "symbol": "518880.SH", "tag": "gold", "composite_score": 0.92},
            {"time": ts, "asset_type": "etf_CN", "symbol": "512760.SH", "tag": "chip", "composite_score": 0.78},
            {"time": ts, "asset_type": "etf_CN", "symbol": "512000.SH", "tag": "broker", "composite_score": 0.51},
        ],
        schema={
            "time": pl.Datetime("us", "UTC"),
            "asset_type": pl.Utf8,
            "symbol": pl.Utf8,
            "tag": pl.Utf8,
            "composite_score": pl.Float64,
        },
    )

    result = strategy.generate_signals(df)

    assert result.height == 2
    assert result.get_column("symbol").to_list() == ["518880.SH", "512760.SH"]
    metadata = json.loads(result.get_column("metadata").to_list()[0])
    assert metadata == {"rank": 1, "tag": "gold", "profile": "trend_v1"}


def test_etf_rotation_strategy_limits_same_tag_exposure() -> None:
    strategy = ETFUniverseRotationStrategy(top_n=3, profile_name="trend_v1", max_per_tag=1)
    ts = datetime(2026, 5, 30, tzinfo=timezone.utc)
    df = pl.DataFrame(
        [
            {"time": ts, "asset_type": "etf_CN", "symbol": "588200.SH", "tag": "chip", "composite_score": 0.95},
            {"time": ts, "asset_type": "etf_CN", "symbol": "512760.SH", "tag": "chip", "composite_score": 0.93},
            {"time": ts, "asset_type": "etf_CN", "symbol": "159819.SZ", "tag": "ai", "composite_score": 0.90},
            {"time": ts, "asset_type": "etf_CN", "symbol": "512000.SH", "tag": "broker", "composite_score": 0.82},
        ],
        schema={
            "time": pl.Datetime("us", "UTC"),
            "asset_type": pl.Utf8,
            "symbol": pl.Utf8,
            "tag": pl.Utf8,
            "composite_score": pl.Float64,
        },
    )

    result = strategy.generate_signals(df)

    assert result.get_column("symbol").to_list() == ["588200.SH", "159819.SZ", "512000.SH"]
    metadata = json.loads(result.get_column("metadata").to_list()[0])
    assert metadata["rank"] == 1

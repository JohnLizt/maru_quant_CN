from __future__ import annotations

import json
from datetime import date, datetime, timezone

import polars as pl
import pytest

from app.backtest.runner import BacktestResult, StrategyBacktestBundle, run_backtest, run_strategy_backtest
from app.factors.registry import resolve_factors
from app.signals.composite import apply_composite_score
from app.signals.profiles import get_signal_profile
from app.services.strategy_service import StrategySnapshotBundle, build_strategy_snapshot, run_strategy_snapshot
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


def test_trend_etf_v1_profile_weights_and_factor_set() -> None:
    profile = get_signal_profile("trend_etf_v1")

    assert profile.factor_names == ["rsi14", "price_to_ma20", "macd_norm", "ma_cross"]
    assert profile.signal_mode == "cross_sectional"
    assert profile.supported_asset_types == ("etf_CN",)
    weights = {rule.factor_name: rule.weight for rule in profile.factor_rules}
    assert pytest.approx(sum(weights.values()), rel=1e-6) == 1.0
    assert weights["rsi14"] > weights["price_to_ma20"] > weights["macd_norm"] > weights["ma_cross"]


def test_trend_etf_v1_composite_includes_macd_contributor() -> None:
    profile = get_signal_profile("trend_etf_v1")
    df = pl.DataFrame(
        [
            {
                "rsi14": 65.0,
                "rsi14_score": 0.8,
                "price_to_ma20": 0.05,
                "price_to_ma20_score": 0.6,
                "macd_norm": 0.02,
                "macd_norm_score": 0.7,
                "ma_cross": 0.01,
                "ma_cross_score": 0.1,
            }
        ]
    )

    result = apply_composite_score(df, profile)

    assert "macd_momentum_strong" in result.get_column("contributors").to_list()[0]
    assert result.get_column("label").to_list()[0] == "strong"


def test_etf_rotation_strategy_selects_top_n_and_emits_metadata() -> None:
    strategy = ETFUniverseRotationStrategy(top_n=2, profile_name="trend_etf_v1")
    ts = datetime(2026, 5, 30, tzinfo=timezone.utc)
    df = pl.DataFrame(
        [
            {"time": ts, "asset_type": "etf_CN", "signal_mode": "cross_sectional", "symbol": "518880.SH", "tag": "gold", "composite_score": 0.92},
            {"time": ts, "asset_type": "etf_CN", "signal_mode": "cross_sectional", "symbol": "512760.SH", "tag": "chip", "composite_score": 0.78},
            {"time": ts, "asset_type": "etf_CN", "signal_mode": "cross_sectional", "symbol": "512000.SH", "tag": "broker", "composite_score": 0.51},
        ],
        schema={
            "time": pl.Datetime("us", "UTC"),
            "asset_type": pl.Utf8,
            "signal_mode": pl.Utf8,
            "symbol": pl.Utf8,
            "tag": pl.Utf8,
            "composite_score": pl.Float64,
        },
    )

    result = strategy.build_decisions(df)

    assert result.height == 2
    assert result.get_column("symbol").to_list() == ["518880.SH", "512760.SH"]
    metadata = json.loads(result.get_column("metadata").to_list()[0])
    assert metadata == {"rank": 1, "tag": "gold", "profile": "trend_etf_v1"}
    assert result.get_column("target_weight").to_list() == [0.5, 0.5]


def test_etf_rotation_strategy_limits_same_tag_exposure() -> None:
    strategy = ETFUniverseRotationStrategy(top_n=3, profile_name="trend_etf_v1", max_per_tag=1)
    ts = datetime(2026, 5, 30, tzinfo=timezone.utc)
    df = pl.DataFrame(
        [
            {"time": ts, "asset_type": "etf_CN", "signal_mode": "cross_sectional", "symbol": "588200.SH", "tag": "chip", "composite_score": 0.95},
            {"time": ts, "asset_type": "etf_CN", "signal_mode": "cross_sectional", "symbol": "512760.SH", "tag": "chip", "composite_score": 0.93},
            {"time": ts, "asset_type": "etf_CN", "signal_mode": "cross_sectional", "symbol": "159819.SZ", "tag": "ai", "composite_score": 0.90},
            {"time": ts, "asset_type": "etf_CN", "signal_mode": "cross_sectional", "symbol": "512000.SH", "tag": "broker", "composite_score": 0.82},
        ],
        schema={
            "time": pl.Datetime("us", "UTC"),
            "asset_type": pl.Utf8,
            "signal_mode": pl.Utf8,
            "symbol": pl.Utf8,
            "tag": pl.Utf8,
            "composite_score": pl.Float64,
        },
    )

    result = strategy.build_decisions(df)

    assert result.get_column("symbol").to_list() == ["588200.SH", "159819.SZ", "512000.SH"]
    metadata = json.loads(result.get_column("metadata").to_list()[0])
    assert metadata["rank"] == 1


def test_strategy_service_builds_snapshot_and_decisions(monkeypatch: pytest.MonkeyPatch) -> None:
    ts = datetime(2026, 5, 30, tzinfo=timezone.utc)
    rankings = pl.DataFrame(
        [
            {"time": ts, "asset_type": "etf_CN", "signal_mode": "cross_sectional", "symbol": "518880.SH", "symbol_name": "黄金ETF华安", "tag": "gold", "rsi14": 65.0, "price_to_ma20": 0.03, "macd_norm": 0.01, "ma_cross": 0.01, "rsi14_score": 0.7, "price_to_ma20_score": 0.4, "macd_norm_score": 0.5, "ma_cross_score": 0.1, "composite_score": 0.8, "label": "strong", "contributors": ["rsi_in_healthy_trend_zone"], "rank": 1},
            {"time": ts, "asset_type": "etf_CN", "signal_mode": "cross_sectional", "symbol": "512000.SH", "symbol_name": "券商ETF华宝", "tag": "broker", "rsi14": 60.0, "price_to_ma20": 0.02, "macd_norm": 0.00, "ma_cross": 0.00, "rsi14_score": 0.6, "price_to_ma20_score": 0.3, "macd_norm_score": 0.1, "ma_cross_score": 0.0, "composite_score": 0.5, "label": "positive", "contributors": ["mixed_signal"], "rank": 2},
        ]
    )

    monkeypatch.setattr(
        "app.services.strategy_service.build_signal_snapshot",
        lambda *args, **kwargs: (get_signal_profile("trend_etf_v1"), rankings),
    )

    strategy = ETFUniverseRotationStrategy(top_n=2, profile_name="trend_etf_v1", max_per_tag=1)
    bundle = build_strategy_snapshot(
        strategy,
        start_date="2026-05-30",
        end_date="2026-05-30",
        asset_type="etf_CN",
        profile_name="trend_etf_v1",
    )

    assert bundle.signal_snapshot.height == 2
    assert bundle.decisions.height == 2
    assert run_strategy_snapshot(
        strategy,
        target_date="2026-05-30",
        asset_type="etf_CN",
        profile_name="trend_etf_v1",
    ).height == 2


def test_run_backtest_consumes_decisions_and_returns_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    ts = datetime(2026, 5, 30, tzinfo=timezone.utc)
    decisions = pl.DataFrame(
        [
            {
                "time": ts,
                "asset_type": "etf_CN",
                "strategy": "etf_rotation_v1",
                "strategy_mode": "cross_sectional",
                "symbol": "518880.SH",
                "decision_type": "target_weight",
                "signal": 1,
                "target_weight": 0.5,
                "score": 0.8,
                "rank": 1,
                "tag": "gold",
                "metadata": json.dumps({"rank": 1, "tag": "gold", "profile": "trend_etf_v1"}),
            },
            {
                "time": ts,
                "asset_type": "etf_CN",
                "strategy": "etf_rotation_v1",
                "strategy_mode": "cross_sectional",
                "symbol": "512000.SH",
                "decision_type": "target_weight",
                "signal": 1,
                "target_weight": 0.5,
                "score": 0.5,
                "rank": 2,
                "tag": "broker",
                "metadata": json.dumps({"rank": 2, "tag": "broker", "profile": "trend_etf_v1"}),
            },
        ]
    )
    market_returns = pl.DataFrame(
        [
            {"time": ts, "symbol": "518880.SH", "daily_return": 0.01},
            {"time": ts, "symbol": "512000.SH", "daily_return": 0.02},
        ]
    )
    monkeypatch.setattr("app.backtest.runner._load_market_returns", lambda *args, **kwargs: market_returns)
    result = run_backtest(
        decisions,
        asset_type="etf_CN",
        start="2026-05-30",
        end="2026-05-30",
        execution_lag=0,
    )

    assert isinstance(result, BacktestResult)
    assert result.holdings_df.height == 2
    assert result.returns_df.height == 1
    assert result.metrics


def test_run_backtest_weekly_uses_python_weekday_and_costs(monkeypatch: pytest.MonkeyPatch) -> None:
    wed = datetime(2026, 5, 27, tzinfo=timezone.utc)
    thu = datetime(2026, 5, 28, tzinfo=timezone.utc)
    next_wed = datetime(2026, 6, 3, tzinfo=timezone.utc)
    decisions = pl.DataFrame(
        [
            {
                "time": wed,
                "asset_type": "etf_CN",
                "strategy": "etf_rotation_v1",
                "strategy_mode": "cross_sectional",
                "symbol": "AAA",
                "decision_type": "target_weight",
                "signal": 1,
                "target_weight": 0.5,
                "score": 0.9,
                "rank": 1,
                "tag": "alpha",
                "metadata": "{}",
            },
            {
                "time": wed,
                "asset_type": "etf_CN",
                "strategy": "etf_rotation_v1",
                "strategy_mode": "cross_sectional",
                "symbol": "BBB",
                "decision_type": "target_weight",
                "signal": 1,
                "target_weight": 0.5,
                "score": 0.8,
                "rank": 2,
                "tag": "beta",
                "metadata": "{}",
            },
            {
                "time": thu,
                "asset_type": "etf_CN",
                "strategy": "etf_rotation_v1",
                "strategy_mode": "cross_sectional",
                "symbol": "CCC",
                "decision_type": "target_weight",
                "signal": 1,
                "target_weight": 1.0,
                "score": 0.7,
                "rank": 1,
                "tag": "gamma",
                "metadata": "{}",
            },
            {
                "time": next_wed,
                "asset_type": "etf_CN",
                "strategy": "etf_rotation_v1",
                "strategy_mode": "cross_sectional",
                "symbol": "AAA",
                "decision_type": "target_weight",
                "signal": 1,
                "target_weight": 0.5,
                "score": 0.9,
                "rank": 1,
                "tag": "alpha",
                "metadata": "{}",
            },
            {
                "time": next_wed,
                "asset_type": "etf_CN",
                "strategy": "etf_rotation_v1",
                "strategy_mode": "cross_sectional",
                "symbol": "DDD",
                "decision_type": "target_weight",
                "signal": 1,
                "target_weight": 0.5,
                "score": 0.85,
                "rank": 2,
                "tag": "delta",
                "metadata": "{}",
            },
        ]
    )
    market_returns = pl.DataFrame(
        [
            {"time": wed, "symbol": "AAA", "daily_return": 0.01},
            {"time": wed, "symbol": "BBB", "daily_return": 0.02},
            {"time": wed, "symbol": "DDD", "daily_return": 0.00},
            {"time": thu, "symbol": "AAA", "daily_return": 0.00},
            {"time": thu, "symbol": "BBB", "daily_return": 0.01},
            {"time": thu, "symbol": "DDD", "daily_return": 0.00},
            {"time": next_wed, "symbol": "AAA", "daily_return": -0.01},
            {"time": next_wed, "symbol": "BBB", "daily_return": 0.00},
            {"time": next_wed, "symbol": "DDD", "daily_return": 0.03},
        ]
    )
    monkeypatch.setattr("app.backtest.runner._load_market_returns", lambda *args, **kwargs: market_returns)

    result = run_backtest(
        decisions,
        asset_type="etf_CN",
        start="2026-05-27",
        end="2026-06-03",
        rebalance_frequency="weekly",
        rebalance_weekday=2,
        execution_lag=0,
        commission_bps=10.0,
        slippage_bps=0.0,
    )

    assert result.holdings_df.filter(pl.col("time") == date(2026, 5, 28)).get_column("symbol").to_list() == ["AAA", "BBB"]
    assert "CCC" not in result.holdings_df.get_column("symbol").to_list()
    first_cost = result.returns_df.filter(pl.col("time") == date(2026, 5, 27)).get_column("cost").item()
    second_turnover = result.returns_df.filter(pl.col("time") == date(2026, 6, 3)).get_column("turnover").item()
    assert first_cost == pytest.approx(0.001)
    assert second_turnover == pytest.approx(1.0)


def test_run_strategy_backtest_builds_snapshot_decisions_and_result(monkeypatch: pytest.MonkeyPatch) -> None:
    ts = datetime(2026, 5, 27, tzinfo=timezone.utc)
    snapshot = pl.DataFrame(
        [
            {"time": ts, "asset_type": "etf_CN", "signal_mode": "cross_sectional", "symbol": "AAA", "symbol_name": "AAA", "tag": "alpha", "rsi14": 60.0, "price_to_ma20": 0.02, "macd_norm": 0.01, "ma_cross": 0.01, "rsi14_score": 0.7, "price_to_ma20_score": 0.5, "macd_norm_score": 0.3, "ma_cross_score": 0.2, "composite_score": 0.8, "label": "strong", "contributors": ["mixed_signal"], "rank": 1},
            {"time": ts, "asset_type": "etf_CN", "signal_mode": "cross_sectional", "symbol": "BBB", "symbol_name": "BBB", "tag": "beta", "rsi14": 58.0, "price_to_ma20": 0.01, "macd_norm": 0.00, "ma_cross": 0.00, "rsi14_score": 0.6, "price_to_ma20_score": 0.4, "macd_norm_score": 0.1, "ma_cross_score": 0.1, "composite_score": 0.6, "label": "positive", "contributors": ["mixed_signal"], "rank": 2},
        ]
    )
    market_returns = pl.DataFrame(
        [
            {"time": ts, "symbol": "AAA", "daily_return": 0.01},
            {"time": ts, "symbol": "BBB", "daily_return": 0.02},
        ]
    )
    monkeypatch.setattr(
        "app.backtest.runner.build_strategy_snapshot",
        lambda *args, **kwargs: StrategySnapshotBundle(
            signal_snapshot=snapshot,
            decisions=args[0].build_decisions(snapshot),
        ),
    )
    monkeypatch.setattr("app.backtest.runner._load_market_returns", lambda *args, **kwargs: market_returns)

    strategy = ETFUniverseRotationStrategy(top_n=2, profile_name="trend_etf_v1", max_per_tag=1)
    result = run_strategy_backtest(
        strategy,
        asset_type="etf_CN",
        profile_name="trend_etf_v1",
        start="2026-05-27",
        end="2026-05-27",
        rebalance_frequency="weekly",
        rebalance_weekday=2,
        execution_lag=0,
    )

    assert isinstance(result, StrategyBacktestBundle)
    assert result.signal_snapshot.height == 2
    assert result.decisions_df.height == 2
    assert result.backtest_result.returns_df.height == 1

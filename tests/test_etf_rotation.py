from __future__ import annotations

import json
from datetime import date, datetime, timezone

import polars as pl
import pytest

from app.backtest.risk_overlay import RiskOverlayConfig
from app.backtest.runner import BacktestResult, StrategyBacktestBundle, run_backtest, run_strategy_backtest
from app.factors.registry import resolve_factors
from app.signals.composite import apply_composite_score
from app.signals.profiles import get_signal_profile
from app.services.strategy_service import StrategySnapshotBundle, build_strategy_snapshot, run_strategy_snapshot
from app.strategy.etf_rotation import ETFUniverseRotationStrategy


def _market_bar(
    ts: datetime,
    symbol: str,
    *,
    asset_type: str = "etf_CN",
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
    amount: float = 1000.0,
) -> dict[str, object]:
    ohlc4 = (open_price + high_price + low_price + close_price) / 4.0
    return {
        "time": ts.date(),
        "asset_type": asset_type,
        "symbol": symbol,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "amount": amount,
        "daily_return": 0.0,
        "ohlc4": ohlc4,
    }


def test_resolve_factors_filters_by_asset_type() -> None:
    stock_factors = [factor.name for factor in resolve_factors(asset_type="stock_CN")]
    etf_factors = [factor.name for factor in resolve_factors(asset_type="etf_CN")]

    assert "limit_up" in stock_factors
    assert "limit_up" not in etf_factors
    assert {"price_to_ma20", "ma_cross", "rsi14", "macd_norm"}.issubset(etf_factors)
    assert {"std_score", "cv"}.issubset(etf_factors)


def test_resolve_factors_rejects_unsupported_factor_for_etf() -> None:
    with pytest.raises(ValueError, match="不支持因子"):
        resolve_factors(["limit_up"], asset_type="etf_CN")


def test_trend_v1_profile_weights_and_factor_set() -> None:
    profile = get_signal_profile("trend_v1")

    assert profile.factor_names == ["ma_cross", "price_to_ma20", "rsi14"]
    assert profile.signal_mode == "cross_sectional"
    assert profile.supported_asset_types == ("stock_CN",)
    assert [rule.weight for rule in profile.factor_rules] == [0.4, 0.3, 0.3]


def test_trend_v1_composite_uses_stock_factor_scores() -> None:
    profile = get_signal_profile("trend_v1")
    df = pl.DataFrame(
        [
            {
                "ma_cross": 0.10,
                "price_to_ma20": 0.05,
                "rsi14": 65.0,
                "ma_cross_score": 0.7,
                "price_to_ma20_score": 0.5,
                "rsi14_score": 0.8,
            }
        ]
    )

    result = apply_composite_score(df, profile)

    assert result.get_column("composite_score").to_list()[0] == pytest.approx(0.67, abs=1e-6)
    assert result.get_column("contributors").to_list()[0] == [
        "trend_structure_strong",
        "price_above_ma20",
        "rsi_in_healthy_trend_zone",
    ]
    assert result.get_column("label").to_list()[0] == "strong"


def test_trend_etf_momentum_reg20_profile_weights_and_factor_set() -> None:
    profile = get_signal_profile("trend_etf_momentum_reg20")

    assert profile.factor_names == ["momentum_reg_20_rank"]
    assert profile.signal_mode == "cross_sectional"
    assert profile.supported_asset_types == ("*",)
    rule = profile.factor_rules[0]
    assert rule.method == "linear_clip"
    assert rule.weight == 1.0
    assert rule.clip_lower == 0.0
    assert rule.clip_upper == 1.0


def test_trend_etf_momentum_reg20_composite_uses_factor_score() -> None:
    profile = get_signal_profile("trend_etf_momentum_reg20")
    df = pl.DataFrame(
        [
            {
                "momentum_reg_20_rank": 0.95,
                "momentum_reg_20_rank_score": 0.9,
            }
        ]
    )

    result = apply_composite_score(df, profile)

    assert result.get_column("composite_score").to_list()[0] == pytest.approx(0.9)
    assert result.get_column("contributors").to_list()[0] == ["mixed_signal"]
    assert result.get_column("label").to_list()[0] == "strong"


def test_trend_etf_momentum_reg20_profile_supports_mixed_universe() -> None:
    profile = get_signal_profile("trend_etf_momentum_reg20")
    assert profile.supported_asset_types == ("*",)


def test_etf_rotation_strategy_selects_top_n_and_emits_metadata() -> None:
    strategy = ETFUniverseRotationStrategy(top_n=2, profile_name="trend_etf_momentum_reg20")
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
    assert metadata == {"rank": 1, "tag": "gold", "profile": "trend_etf_momentum_reg20"}
    assert result.get_column("target_weight").to_list() == [0.5, 0.5]


def test_etf_rotation_strategy_limits_same_tag_exposure() -> None:
    strategy = ETFUniverseRotationStrategy(top_n=3, profile_name="trend_etf_momentum_reg20", max_per_tag=1)
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
            {"time": ts, "asset_type": "etf_CN", "signal_mode": "cross_sectional", "symbol": "518880.SH", "symbol_name": "黄金ETF华安", "tag": "gold", "momentum_reg_20_rank": 0.80, "momentum_reg_20_rank_score": 0.8, "composite_score": 0.8, "label": "strong", "contributors": ["mixed_signal"], "rank": 1},
            {"time": ts, "asset_type": "etf_CN", "signal_mode": "cross_sectional", "symbol": "512000.SH", "symbol_name": "券商ETF华宝", "tag": "broker", "momentum_reg_20_rank": 0.50, "momentum_reg_20_rank_score": 0.5, "composite_score": 0.5, "label": "positive", "contributors": ["mixed_signal"], "rank": 2},
        ]
    )

    monkeypatch.setattr(
        "app.services.strategy_service.build_signal_snapshot",
        lambda *args, **kwargs: (get_signal_profile("trend_etf_momentum_reg20"), rankings),
    )

    strategy = ETFUniverseRotationStrategy(top_n=2, profile_name="trend_etf_momentum_reg20", max_per_tag=1)
    bundle = build_strategy_snapshot(
        strategy,
        start_date="2026-05-30",
        end_date="2026-05-30",
        asset_type="etf_CN",
        profile_name="trend_etf_momentum_reg20",
    )

    assert bundle.signal_snapshot.height == 2
    assert bundle.decisions.height == 2
    assert run_strategy_snapshot(
        strategy,
        target_date="2026-05-30",
        asset_type="etf_CN",
        profile_name="trend_etf_momentum_reg20",
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
                "metadata": json.dumps({"rank": 1, "tag": "gold", "profile": "trend_etf_momentum_reg20"}),
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
                "metadata": json.dumps({"rank": 2, "tag": "broker", "profile": "trend_etf_momentum_reg20"}),
            },
        ]
    )
    market_data = pl.DataFrame(
        [
            _market_bar(ts, "518880.SH", open_price=2.0, high_price=2.1, low_price=1.9, close_price=2.05),
            _market_bar(ts, "512000.SH", open_price=4.0, high_price=4.1, low_price=3.9, close_price=4.10),
        ]
    )
    monkeypatch.setattr("app.backtest.runner._load_market_data", lambda *args, **kwargs: market_data)
    result = run_backtest(
        decisions,
        asset_type="etf_CN",
        start="2026-05-30",
        end="2026-05-30",
        execution_lag=0,
    )

    assert isinstance(result, BacktestResult)
    assert result.holdings_df.height == 2
    assert result.trades_df.height == 2
    assert result.returns_df.height == 1
    assert result.metrics["initial_capital"] == pytest.approx(40000.0)
    assert result.metrics["end_nav"] > 0


def test_run_backtest_applies_risk_overlay_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    ts = datetime(2026, 5, 27, tzinfo=timezone.utc)
    decisions = pl.DataFrame(
        [
            {
                "time": ts,
                "asset_type": "etf_CN",
                "strategy": "etf_rotation_v1",
                "strategy_mode": "cross_sectional",
                "symbol": "AAA",
                "decision_type": "target_weight",
                "signal": 1,
                "target_weight": 1.0,
                "score": 0.9,
                "rank": 1,
                "tag": "alpha",
                "metadata": "{}",
            }
        ]
    )
    market_data = pl.DataFrame(
        [
            _market_bar(ts, "AAA", open_price=100.0, high_price=101.0, low_price=99.0, close_price=100.0),
        ]
    ).with_columns(
        [
            pl.lit(0.04).alias("std_score"),
            pl.lit(0.1).alias("cv"),
        ]
    )
    monkeypatch.setattr("app.backtest.runner._load_market_data", lambda *args, **kwargs: market_data)
    monkeypatch.setattr("app.backtest.runner.build_risk_features", lambda df, *_args, **_kwargs: df)

    result = run_backtest(
        decisions,
        asset_type="etf_CN",
        start="2026-05-27",
        end="2026-05-27",
        execution_lag=0,
        risk_config=RiskOverlayConfig(),
    )

    assert result.trades_df.height == 1
    trade = result.trades_df.row(0, named=True)
    assert trade["action"] == "调仓买入"
    assert trade["risk_reason"] == "risk_half_std"
    assert result.holdings_df.get_column("risk_half_triggered").item() is True
    assert result.metrics["risk_half_events"] == pytest.approx(1.0)
    assert result.metrics["stop_loss_events"] == pytest.approx(0.0)


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
    market_data = pl.DataFrame(
        [
            _market_bar(wed, "AAA", open_price=10.0, high_price=10.2, low_price=9.8, close_price=10.1),
            _market_bar(wed, "BBB", open_price=10.0, high_price=10.2, low_price=9.8, close_price=10.2),
            _market_bar(wed, "DDD", open_price=10.0, high_price=10.0, low_price=10.0, close_price=10.0),
            _market_bar(thu, "AAA", open_price=10.0, high_price=10.0, low_price=10.0, close_price=10.0),
            _market_bar(thu, "BBB", open_price=10.0, high_price=10.2, low_price=9.8, close_price=10.1),
            _market_bar(thu, "DDD", open_price=10.0, high_price=10.0, low_price=10.0, close_price=10.0),
            _market_bar(next_wed, "AAA", open_price=9.8, high_price=9.9, low_price=9.7, close_price=9.8),
            _market_bar(next_wed, "BBB", open_price=10.0, high_price=10.0, low_price=10.0, close_price=10.0),
            _market_bar(next_wed, "DDD", open_price=10.2, high_price=10.4, low_price=10.0, close_price=10.3),
        ]
    )
    monkeypatch.setattr("app.backtest.runner._load_market_data", lambda *args, **kwargs: market_data)

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
    first_day = result.returns_df.filter(pl.col("time") == date(2026, 5, 27)).row(0, named=True)
    second_rebalance = result.returns_df.filter(pl.col("time") == date(2026, 6, 3)).row(0, named=True)
    assert first_day["cost"] > 0
    assert second_rebalance["turnover"] > 0
    assert result.trades_df.filter(pl.col("action") == "调仓卖出").height == 1
    assert result.trades_df.filter(pl.col("action") == "调仓买入").height == 3


def test_run_backtest_biweekly_keeps_every_other_weekday(monkeypatch: pytest.MonkeyPatch) -> None:
    wed1 = datetime(2026, 5, 27, tzinfo=timezone.utc)
    wed2 = datetime(2026, 6, 3, tzinfo=timezone.utc)
    wed3 = datetime(2026, 6, 10, tzinfo=timezone.utc)
    decisions = pl.DataFrame(
        [
            {
                "time": wed1,
                "asset_type": "etf_CN",
                "strategy": "etf_rotation_v1",
                "strategy_mode": "cross_sectional",
                "symbol": "AAA",
                "decision_type": "target_weight",
                "signal": 1,
                "target_weight": 1.0,
                "score": 0.9,
                "rank": 1,
                "tag": "alpha",
                "metadata": "{}",
            },
            {
                "time": wed2,
                "asset_type": "etf_CN",
                "strategy": "etf_rotation_v1",
                "strategy_mode": "cross_sectional",
                "symbol": "BBB",
                "decision_type": "target_weight",
                "signal": 1,
                "target_weight": 1.0,
                "score": 0.8,
                "rank": 1,
                "tag": "beta",
                "metadata": "{}",
            },
            {
                "time": wed3,
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
        ]
    )
    market_data = pl.DataFrame(
        [
            _market_bar(wed1, "AAA", open_price=10.0, high_price=10.1, low_price=9.9, close_price=10.1),
            _market_bar(wed1, "BBB", open_price=10.0, high_price=10.0, low_price=10.0, close_price=10.0),
            _market_bar(wed1, "CCC", open_price=10.0, high_price=10.0, low_price=10.0, close_price=10.0),
            _market_bar(wed2, "AAA", open_price=10.0, high_price=10.0, low_price=10.0, close_price=10.0),
            _market_bar(wed2, "BBB", open_price=10.2, high_price=10.3, low_price=10.1, close_price=10.2),
            _market_bar(wed2, "CCC", open_price=10.0, high_price=10.0, low_price=10.0, close_price=10.0),
            _market_bar(wed3, "AAA", open_price=10.0, high_price=10.0, low_price=10.0, close_price=10.0),
            _market_bar(wed3, "BBB", open_price=10.0, high_price=10.0, low_price=10.0, close_price=10.0),
            _market_bar(wed3, "CCC", open_price=10.3, high_price=10.5, low_price=10.1, close_price=10.3),
        ]
    )
    monkeypatch.setattr("app.backtest.runner._load_market_data", lambda *args, **kwargs: market_data)

    result = run_backtest(
        decisions,
        asset_type="etf_CN",
        start="2026-05-27",
        end="2026-06-10",
        rebalance_frequency="biweekly",
        rebalance_weekday=2,
        execution_lag=0,
    )

    assert result.holdings_df.filter(pl.col("time") == date(2026, 5, 27)).get_column("symbol").to_list() == ["AAA"]
    assert result.holdings_df.filter(pl.col("time") == date(2026, 6, 3)).get_column("symbol").to_list() == ["AAA"]
    assert result.holdings_df.filter(pl.col("time") == date(2026, 6, 10)).get_column("symbol").to_list() == ["CCC"]


def test_run_backtest_stop_loss_can_reenter_on_same_rebalance_day(monkeypatch: pytest.MonkeyPatch) -> None:
    d1 = datetime(2026, 5, 27, tzinfo=timezone.utc)
    d2 = datetime(2026, 5, 28, tzinfo=timezone.utc)
    decisions = pl.DataFrame(
        [
            {
                "time": d1,
                "asset_type": "etf_CN",
                "strategy": "etf_rotation_v1",
                "strategy_mode": "cross_sectional",
                "symbol": "AAA",
                "decision_type": "target_weight",
                "signal": 1,
                "target_weight": 1.0,
                "score": 0.9,
                "rank": 1,
                "tag": "alpha",
                "metadata": "{}",
            },
            {
                "time": d2,
                "asset_type": "etf_CN",
                "strategy": "etf_rotation_v1",
                "strategy_mode": "cross_sectional",
                "symbol": "AAA",
                "decision_type": "target_weight",
                "signal": 1,
                "target_weight": 1.0,
                "score": 0.95,
                "rank": 1,
                "tag": "alpha",
                "metadata": "{}",
            },
        ]
    )
    market_data = pl.DataFrame(
        [
            _market_bar(d1, "AAA", open_price=100.0, high_price=100.0, low_price=100.0, close_price=100.0),
            _market_bar(d2, "AAA", open_price=88.0, high_price=88.0, low_price=88.0, close_price=88.0),
        ]
    ).with_columns(
        [
            pl.lit(0.0).alias("std_score"),
            pl.lit(0.0).alias("cv"),
        ]
    )
    monkeypatch.setattr("app.backtest.runner._load_market_data", lambda *args, **kwargs: market_data)
    monkeypatch.setattr("app.backtest.runner.build_risk_features", lambda df, *_args, **_kwargs: df)

    result = run_backtest(
        decisions,
        asset_type="etf_CN",
        start="2026-05-27",
        end="2026-05-28",
        rebalance_frequency="daily",
        execution_lag=0,
        risk_config=RiskOverlayConfig(stop_loss_rate=0.1),
    )

    same_day_actions = result.trades_df.filter(pl.col("time") == date(2026, 5, 28)).get_column("action").to_list()
    assert same_day_actions == ["止损", "调仓买入"]
    latest_holding = result.holdings_df.filter(pl.col("time") == date(2026, 5, 28)).row(0, named=True)
    assert latest_holding["symbol"] == "AAA"
    assert latest_holding["buy_date"] == date(2026, 5, 28)


def test_run_strategy_backtest_builds_snapshot_decisions_and_result(monkeypatch: pytest.MonkeyPatch) -> None:
    ts = datetime(2026, 5, 27, tzinfo=timezone.utc)
    snapshot = pl.DataFrame(
        [
            {"time": ts, "asset_type": "etf_CN", "signal_mode": "cross_sectional", "symbol": "AAA", "symbol_name": "AAA", "tag": "alpha", "momentum_reg_20_rank": 0.8, "momentum_reg_20_rank_score": 0.8, "composite_score": 0.8, "label": "strong", "contributors": ["mixed_signal"], "rank": 1},
            {"time": ts, "asset_type": "etf_CN", "signal_mode": "cross_sectional", "symbol": "BBB", "symbol_name": "BBB", "tag": "beta", "momentum_reg_20_rank": 0.6, "momentum_reg_20_rank_score": 0.6, "composite_score": 0.6, "label": "positive", "contributors": ["mixed_signal"], "rank": 2},
        ]
    )
    market_data = pl.DataFrame(
        [
            _market_bar(ts, "AAA", open_price=10.0, high_price=10.1, low_price=9.9, close_price=10.1),
            _market_bar(ts, "BBB", open_price=10.0, high_price=10.2, low_price=9.8, close_price=10.2),
        ]
    )
    monkeypatch.setattr(
        "app.backtest.runner.build_strategy_snapshot",
        lambda *args, **kwargs: StrategySnapshotBundle(
            signal_snapshot=snapshot,
            decisions=args[0].build_decisions(snapshot),
        ),
    )
    monkeypatch.setattr("app.backtest.runner._load_market_data", lambda *args, **kwargs: market_data)

    strategy = ETFUniverseRotationStrategy(top_n=2, profile_name="trend_etf_momentum_reg20", max_per_tag=1)
    result = run_strategy_backtest(
        strategy,
        asset_type="etf_CN",
        profile_name="trend_etf_momentum_reg20",
        start="2026-05-27",
        end="2026-05-27",
        rebalance_frequency="biweekly",
        rebalance_weekday=2,
        execution_lag=0,
    )

    assert isinstance(result, StrategyBacktestBundle)
    assert result.signal_snapshot.height == 2
    assert result.decisions_df.height == 2
    assert result.backtest_result.returns_df.height == 1


def test_query_etf_rotation_cli_accepts_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.cli import query_etf_rotation

    ts = datetime(2026, 5, 30, tzinfo=timezone.utc)
    snapshot = pl.DataFrame(
        [
            {
                "time": ts,
                "asset_type": "etf_CN",
                "signal_mode": "cross_sectional",
                "symbol": "AAA",
                "symbol_name": "AAA",
                "tag": "alpha",
                "momentum_reg_20_rank": 0.9,
                "momentum_reg_20_rank_score": 0.8,
                "composite_score": 0.8,
                "label": "strong",
                "contributors": ["mixed_signal"],
                "rank": 1,
            }
        ]
    )
    decisions = pl.DataFrame(
        [
            {
                "time": ts,
                "asset_type": "etf_CN",
                "strategy": "etf_rotation_v1",
                "strategy_mode": "cross_sectional",
                "symbol": "AAA",
                "decision_type": "target_weight",
                "signal": 1,
                "target_weight": 1.0,
                "score": 0.8,
                "rank": 1,
                "tag": "alpha",
                "metadata": json.dumps({"rank": 1, "tag": "alpha", "profile": "trend_etf_momentum_reg20"}),
            }
        ]
    )

    captured: dict[str, object] = {}

    def _fake_build_strategy_snapshot(*args, **kwargs):
        captured["profile_name"] = kwargs["profile_name"]
        captured["asset_type"] = kwargs.get("asset_type")
        captured["universe"] = kwargs["universe"]
        return StrategySnapshotBundle(signal_snapshot=snapshot, decisions=decisions)

    monkeypatch.setattr(query_etf_rotation, "build_strategy_snapshot", _fake_build_strategy_snapshot)

    exit_code = query_etf_rotation.main("2026-05-30", 5, "trend_etf_momentum_reg20", "etf_mixed")

    assert exit_code == 0
    assert captured["profile_name"] == "trend_etf_momentum_reg20"
    assert captured["asset_type"] is None
    assert captured["universe"] == "etf_mixed"


def test_backtest_etf_rotation_cli_accepts_profile(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    from app.cli import backtest_etf_rotation

    ts = datetime(2026, 5, 30, tzinfo=timezone.utc)
    signal_snapshot = pl.DataFrame(
        [
            {
                "time": ts,
                "asset_type": "etf_CN",
                "signal_mode": "cross_sectional",
                "symbol": "AAA",
                "symbol_name": "AAA",
                "tag": "alpha",
                "momentum_reg_20_rank": 0.9,
                "momentum_reg_20_rank_score": 0.8,
                "composite_score": 0.8,
                "label": "strong",
                "contributors": ["mixed_signal"],
                "rank": 1,
            }
        ]
    )
    decisions = pl.DataFrame(
        [
            {
                "time": ts,
                "asset_type": "etf_CN",
                "strategy": "etf_rotation_v1",
                "strategy_mode": "cross_sectional",
                "symbol": "AAA",
                "decision_type": "target_weight",
                "signal": 1,
                "target_weight": 1.0,
                "score": 0.8,
                "rank": 1,
                "tag": "alpha",
                "metadata": json.dumps({"rank": 1, "tag": "alpha", "profile": "trend_etf_momentum_reg20"}),
            }
        ]
    )
    returns_df = pl.DataFrame([{"time": date(2026, 5, 30), "nav": 40000.0, "cash": 0.0, "cash_ratio": 0.0, "gross_return": 0.01, "cost": 0.0, "turnover": 0.0, "net_return": 0.01}])
    holdings_df = pl.DataFrame([{"time": date(2026, 5, 30), "asset_type": "etf_CN", "symbol": "AAA", "shares": 1.0, "close": 10.0, "market_value": 10.0, "weight": 1.0, "buy_price": 10.0, "buy_date": date(2026, 5, 30), "risk_half_triggered": False, "strategy": "etf_rotation_v1", "score": 0.8, "rank": 1, "tag": "alpha", "metadata": "{}"}])
    trades_df = pl.DataFrame([{"time": date(2026, 5, 30), "asset_type": "etf_CN", "symbol": "AAA", "action": "调仓买入", "side": "buy", "price": 10.0, "shares": 1.0, "notional": 10.0, "fee": 0.0, "cash_before": 40000.0, "cash_after": 39990.0, "nav_after_trade": 40000.0, "signal_date": date(2026, 5, 30), "risk_reason": ""}])

    captured: dict[str, object] = {}

    def _fake_run_strategy_backtest(strategy, **kwargs):
        captured["strategy_profile_name"] = strategy.profile_name
        captured["profile_name"] = kwargs["profile_name"]
        captured["asset_type"] = kwargs["asset_type"]
        captured["universe"] = kwargs["universe"]
        captured["risk_config"] = kwargs["risk_config"]
        return StrategyBacktestBundle(
            signal_snapshot=signal_snapshot,
            decisions_df=decisions,
            backtest_result=BacktestResult(
                holdings_df=holdings_df,
                trades_df=trades_df,
                returns_df=returns_df,
                equity_curve_df=pl.DataFrame([{"time": date(2026, 5, 30), "gross_return": 0.01, "turnover": 0.0, "cost": 0.0, "net_return": 0.01, "equity_curve": 1.01}]),
                metrics={"total_return": 0.01},
                log_path=None,
                artifacts_dir=None,
                equity_chart_path=None,
                artifact_paths=None,
            ),
        )

    monkeypatch.setattr(backtest_etf_rotation, "run_strategy_backtest", _fake_run_strategy_backtest)

    exit_code = backtest_etf_rotation.main(
        "2026-05-30",
        "2026-05-30",
        "trend_etf_momentum_reg20",
        "etf_mixed",
        5,
        1,
        2,
        1,
        5.0,
        5.0,
        "json",
        "INFO",
        str(tmp_path),
        False,
        False,
    )

    assert exit_code == 0
    assert captured["strategy_profile_name"] == "trend_etf_momentum_reg20"
    assert captured["profile_name"] == "trend_etf_momentum_reg20"
    assert captured["asset_type"] is None
    assert captured["universe"] == "etf_mixed"
    assert captured["risk_config"] is None


def test_backtest_etf_rotation_cli_passes_risk_config(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    from app.cli import backtest_etf_rotation

    ts = datetime(2026, 5, 30, tzinfo=timezone.utc)
    signal_snapshot = pl.DataFrame([{"time": ts, "asset_type": "etf_CN", "symbol": "AAA"}])
    decisions = pl.DataFrame([{"time": ts, "asset_type": "etf_CN", "symbol": "AAA"}])
    returns_df = pl.DataFrame([{"time": date(2026, 5, 30), "nav": 40000.0, "cash": 0.0, "cash_ratio": 0.0, "gross_return": 0.01, "cost": 0.0, "turnover": 0.0, "net_return": 0.01}])
    holdings_df = pl.DataFrame([{"time": date(2026, 5, 30), "asset_type": "etf_CN", "symbol": "AAA", "shares": 1.0, "close": 10.0, "market_value": 10.0, "weight": 1.0, "buy_price": 10.0, "buy_date": date(2026, 5, 30), "risk_half_triggered": False, "strategy": "etf_rotation_v1", "score": 0.8, "rank": 1, "tag": "alpha", "metadata": "{}"}])
    trades_df = pl.DataFrame([{"time": date(2026, 5, 30), "asset_type": "etf_CN", "symbol": "AAA", "action": "调仓买入", "side": "buy", "price": 10.0, "shares": 1.0, "notional": 10.0, "fee": 0.0, "cash_before": 40000.0, "cash_after": 39990.0, "nav_after_trade": 40000.0, "signal_date": date(2026, 5, 30), "risk_reason": ""}])

    captured: dict[str, object] = {}

    def _fake_run_strategy_backtest(strategy, **kwargs):
        captured["risk_config"] = kwargs["risk_config"]
        return StrategyBacktestBundle(
            signal_snapshot=signal_snapshot,
            decisions_df=decisions,
            backtest_result=BacktestResult(
                holdings_df=holdings_df,
                trades_df=trades_df,
                returns_df=returns_df,
                equity_curve_df=pl.DataFrame([{"time": date(2026, 5, 30), "gross_return": 0.01, "turnover": 0.0, "cost": 0.0, "net_return": 0.01, "equity_curve": 1.01}]),
                metrics={"total_return": 0.01},
            ),
        )

    monkeypatch.setattr(backtest_etf_rotation, "run_strategy_backtest", _fake_run_strategy_backtest)

    exit_code = backtest_etf_rotation.main(
        "2026-05-30",
        "2026-05-30",
        "trend_etf_momentum_reg20",
        "etf_mixed",
        5,
        1,
        2,
        1,
        5.0,
        5.0,
        "json",
        "INFO",
        str(tmp_path),
        False,
        False,
        True,
        0.04,
        0.6,
        0.12,
        0.4,
        50000.0,
        1.0,
        0.02,
    )

    assert exit_code == 0
    risk_config = captured["risk_config"]
    assert isinstance(risk_config, RiskOverlayConfig)
    assert risk_config.std_threshold == pytest.approx(0.04)
    assert risk_config.cv_threshold == pytest.approx(0.6)
    assert risk_config.stop_loss_rate == pytest.approx(0.12)
    assert risk_config.half_weight == pytest.approx(0.4)


def test_query_etf_rotation_requests_extra_display_factors(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from app.cli import query_etf_rotation

    ts = datetime(2026, 6, 23, tzinfo=timezone.utc)
    captured: dict[str, object] = {}

    def _fake_build_strategy_snapshot(*args, **kwargs):
        captured.update(kwargs)
        return StrategySnapshotBundle(
            signal_snapshot=pl.DataFrame(
                [
                    {
                        "time": ts,
                        "asset_type": "etf_CN",
                        "signal_mode": "cross_sectional",
                        "symbol": "159915.SZ",
                        "symbol_name": "创业板ETF易方达",
                        "tag": "growth_index",
                        "momentum_reg_20_rank": 1.0,
                        "std_score": 0.041,
                        "cv": 0.62,
                        "momentum_reg_20_rank_score": 1.0,
                        "composite_score": 1.0,
                        "label": "strong",
                        "contributors": ["mixed_signal"],
                        "rank": 1,
                    }
                ]
            ),
            decisions=pl.DataFrame(
                [
                    {
                        "time": ts,
                        "asset_type": "etf_CN",
                        "strategy": "etf_rotation_v1",
                        "strategy_mode": "cross_sectional",
                        "symbol": "159915.SZ",
                        "decision_type": "target_weight",
                        "signal": 1,
                        "target_weight": 0.25,
                        "score": 1.0,
                        "rank": 1,
                        "tag": "growth_index",
                        "metadata": json.dumps({"rank": 1, "tag": "growth_index", "profile": "trend_etf_momentum_reg20"}),
                    }
                ]
            ),
        )

    monkeypatch.setattr(query_etf_rotation, "build_strategy_snapshot", _fake_build_strategy_snapshot)

    assert query_etf_rotation.main("2026-06-23", 4, "trend_etf_momentum_reg20", "etf_mixed") == 0

    payload = json.loads(capsys.readouterr().out)
    assert captured["extra_factor_names"] == ["std_score", "cv"]
    assert payload["results"][0]["raw_factors"]["std_score"] == pytest.approx(0.041)
    assert payload["results"][0]["raw_factors"]["cv"] == pytest.approx(0.62)
    assert "std_score_score" not in payload["results"][0]["normalized_factors"]

from __future__ import annotations

import io
import json
from datetime import date

import polars as pl
import pytest
from loguru import logger

from app.backtest.reporting import build_rebalance_period_analysis, export_backtest_artifacts
from app.backtest.runner import BacktestResult


def _sample_backtest_result() -> BacktestResult:
    effective_decisions_df = pl.DataFrame(
        [
            {
                "time": date(2026, 1, 1),
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
                "signal_date": date(2026, 1, 1),
                "effective_date": date(2026, 1, 1),
            },
            {
                "time": date(2026, 1, 1),
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
                "signal_date": date(2026, 1, 1),
                "effective_date": date(2026, 1, 1),
            },
            {
                "time": date(2026, 1, 2),
                "asset_type": "etf_CN",
                "strategy": "etf_rotation_v1",
                "strategy_mode": "cross_sectional",
                "symbol": "BBB",
                "decision_type": "target_weight",
                "signal": 1,
                "target_weight": 0.5,
                "score": 0.85,
                "rank": 1,
                "tag": "beta",
                "metadata": "{}",
                "signal_date": date(2026, 1, 2),
                "effective_date": date(2026, 1, 3),
            },
            {
                "time": date(2026, 1, 2),
                "asset_type": "etf_CN",
                "strategy": "etf_rotation_v1",
                "strategy_mode": "cross_sectional",
                "symbol": "CCC",
                "decision_type": "target_weight",
                "signal": 1,
                "target_weight": 0.5,
                "score": 0.75,
                "rank": 2,
                "tag": "gamma",
                "metadata": "{}",
                "signal_date": date(2026, 1, 2),
                "effective_date": date(2026, 1, 3),
            },
        ]
    )
    returns_df = pl.DataFrame(
        [
            {"time": date(2026, 1, 1), "nav": 100.0, "cash": 0.0, "cash_ratio": 0.0, "gross_return": 0.0, "cost": 0.0, "turnover": 1.0, "net_return": 0.0},
            {"time": date(2026, 1, 2), "nav": 110.0, "cash": 0.0, "cash_ratio": 0.0, "gross_return": 0.10, "cost": 0.0, "turnover": 0.0, "net_return": 0.10},
            {"time": date(2026, 1, 3), "nav": 105.0, "cash": 0.0, "cash_ratio": 0.0, "gross_return": -0.0454545, "cost": 0.01, "turnover": 1.1, "net_return": -0.0554545},
            {"time": date(2026, 1, 4), "nav": 120.0, "cash": 0.0, "cash_ratio": 0.0, "gross_return": 0.1428571, "cost": 0.0, "turnover": 0.0, "net_return": 0.1428571},
        ]
    )
    holdings_df = pl.DataFrame(
        [
            {"time": date(2026, 1, 1), "asset_type": "etf_CN", "symbol": "AAA", "shares": 5.0, "close": 10.0, "market_value": 50.0, "weight": 0.5, "buy_price": 10.0, "buy_date": date(2026, 1, 1), "risk_half_triggered": False, "strategy": "etf_rotation_v1", "score": 0.9, "rank": 1, "tag": "alpha", "metadata": "{}"},
            {"time": date(2026, 1, 1), "asset_type": "etf_CN", "symbol": "BBB", "shares": 2.5, "close": 20.0, "market_value": 50.0, "weight": 0.5, "buy_price": 20.0, "buy_date": date(2026, 1, 1), "risk_half_triggered": False, "strategy": "etf_rotation_v1", "score": 0.8, "rank": 2, "tag": "beta", "metadata": "{}"},
            {"time": date(2026, 1, 2), "asset_type": "etf_CN", "symbol": "AAA", "shares": 5.0, "close": 12.0, "market_value": 60.0, "weight": 60.0 / 110.0, "buy_price": 10.0, "buy_date": date(2026, 1, 1), "risk_half_triggered": False, "strategy": "etf_rotation_v1", "score": 0.9, "rank": 1, "tag": "alpha", "metadata": "{}"},
            {"time": date(2026, 1, 2), "asset_type": "etf_CN", "symbol": "BBB", "shares": 2.5, "close": 20.0, "market_value": 50.0, "weight": 50.0 / 110.0, "buy_price": 20.0, "buy_date": date(2026, 1, 1), "risk_half_triggered": False, "strategy": "etf_rotation_v1", "score": 0.8, "rank": 2, "tag": "beta", "metadata": "{}"},
            {"time": date(2026, 1, 3), "asset_type": "etf_CN", "symbol": "BBB", "shares": 2.75, "close": 20.0, "market_value": 55.0, "weight": 55.0 / 105.0, "buy_price": 20.0, "buy_date": date(2026, 1, 1), "risk_half_triggered": False, "strategy": "etf_rotation_v1", "score": 0.85, "rank": 1, "tag": "beta", "metadata": "{}"},
            {"time": date(2026, 1, 3), "asset_type": "etf_CN", "symbol": "CCC", "shares": 5.0, "close": 10.0, "market_value": 50.0, "weight": 50.0 / 105.0, "buy_price": 10.0, "buy_date": date(2026, 1, 3), "risk_half_triggered": False, "strategy": "etf_rotation_v1", "score": 0.75, "rank": 2, "tag": "gamma", "metadata": "{}"},
            {"time": date(2026, 1, 4), "asset_type": "etf_CN", "symbol": "BBB", "shares": 2.75, "close": 21.8181818, "market_value": 60.0, "weight": 0.5, "buy_price": 20.0, "buy_date": date(2026, 1, 1), "risk_half_triggered": False, "strategy": "etf_rotation_v1", "score": 0.85, "rank": 1, "tag": "beta", "metadata": "{}"},
            {"time": date(2026, 1, 4), "asset_type": "etf_CN", "symbol": "CCC", "shares": 5.0, "close": 12.0, "market_value": 60.0, "weight": 0.5, "buy_price": 10.0, "buy_date": date(2026, 1, 3), "risk_half_triggered": False, "strategy": "etf_rotation_v1", "score": 0.75, "rank": 2, "tag": "gamma", "metadata": "{}"},
        ]
    )
    trades_df = pl.DataFrame(
        [
            {"time": date(2026, 1, 1), "asset_type": "etf_CN", "symbol": "AAA", "action": "调仓买入", "side": "buy", "price": 10.0, "shares": 5.0, "notional": 50.0, "fee": 0.0, "cash_before": 100.0, "cash_after": 50.0, "nav_after_trade": 100.0, "signal_date": date(2026, 1, 1), "risk_reason": ""},
            {"time": date(2026, 1, 1), "asset_type": "etf_CN", "symbol": "BBB", "action": "调仓买入", "side": "buy", "price": 20.0, "shares": 2.5, "notional": 50.0, "fee": 0.0, "cash_before": 50.0, "cash_after": 0.0, "nav_after_trade": 100.0, "signal_date": date(2026, 1, 1), "risk_reason": ""},
            {"time": date(2026, 1, 3), "asset_type": "etf_CN", "symbol": "AAA", "action": "调仓卖出", "side": "sell", "price": 12.0, "shares": 5.0, "notional": 60.0, "fee": 0.0, "cash_before": 0.0, "cash_after": 60.0, "nav_after_trade": 110.0, "signal_date": date(2026, 1, 2), "risk_reason": ""},
            {"time": date(2026, 1, 3), "asset_type": "etf_CN", "symbol": "CCC", "action": "调仓买入", "side": "buy", "price": 10.0, "shares": 5.0, "notional": 50.0, "fee": 0.0, "cash_before": 60.0, "cash_after": 10.0, "nav_after_trade": 105.0, "signal_date": date(2026, 1, 2), "risk_reason": ""},
        ]
    )
    equity_curve_df = returns_df.select(["time", "nav", "gross_return", "turnover", "cost", "net_return"]).with_columns(
        (pl.col("nav") / 100.0).alias("equity_curve")
    ).select(["time", "gross_return", "turnover", "cost", "net_return", "equity_curve"])
    return BacktestResult(
        holdings_df=holdings_df,
        trades_df=trades_df,
        returns_df=returns_df,
        equity_curve_df=equity_curve_df,
        metrics={"initial_capital": 100.0, "end_nav": 120.0, "total_return": 0.2, "annualized_return": 0.2, "max_drawdown": -0.05, "sharpe": 1.2},
        effective_decisions_df=effective_decisions_df,
    )


def test_build_rebalance_period_analysis_groups_by_effective_period() -> None:
    result = _sample_backtest_result()

    periods_df, period_holdings_df, summary = build_rebalance_period_analysis(result)

    assert periods_df.height == 2
    first_period = periods_df.row(0, named=True)
    assert first_period["effective_date"] == date(2026, 1, 1)
    assert first_period["period_end_date"] == date(2026, 1, 2)
    assert first_period["top_contributor_symbol"] == "AAA"
    assert period_holdings_df.filter(pl.col("period_index") == 1).height == 2
    aaa_period = period_holdings_df.filter((pl.col("period_index") == 1) & (pl.col("symbol") == "AAA")).row(0, named=True)
    assert aaa_period["exit_reason"] == "rebalance_out"
    assert aaa_period["period_pnl"] == 10.0
    assert summary["recent_period"]["period_index"] == 2
    assert summary["period_structure"]["period_win_rate"] == pytest.approx(1.0)
    assert summary["period_structure"]["best10_period_positive_return_share"] == pytest.approx(1.0)
    assert summary["asset_concentration"]["top_symbol_name"] == "AAA"
    assert summary["asset_concentration"]["top1_profit_share"] == pytest.approx(0.5)
    assert summary["asset_concentration"]["top3_profit_share"] == pytest.approx(1.25)


def test_export_backtest_artifacts_writes_analysis_csv_and_logs_summary(tmp_path) -> None:
    result = _sample_backtest_result()
    sink = io.StringIO()
    sink_id = logger.add(sink, format="{message}")
    try:
        exported = export_backtest_artifacts(
            result,
            artifacts_dir=tmp_path,
            chart_title="Test",
            chart_subtitle=None,
            save_chart=False,
            write_artifacts=True,
            summary_context={
                "start_date": "2026-01-01",
                "end_date": "2026-01-04",
                "universe": "etf_CN",
                "profile_name": "trend_etf_momentum_reg20",
                "top_n": 2,
                "max_per_tag": 1,
                "total_fee_bps": 10.0,
                "risk_control": False,
            },
        )
    finally:
        logger.remove(sink_id)

    assert exported.rebalance_periods_df is not None
    assert exported.rebalance_period_holdings_df is not None
    assert (tmp_path / "rebalance_periods.csv").exists()
    assert (tmp_path / "rebalance_period_holdings.csv").exists()
    assert (tmp_path / "summary.json").exists()
    assert exported.artifact_paths is not None
    assert "rebalance_periods_csv" in exported.artifact_paths
    assert "rebalance_period_holdings_csv" in exported.artifact_paths
    assert "summary_json" in exported.artifact_paths
    summary_payload = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    assert summary_payload["metrics"]["end_nav"] == 120.0
    assert summary_payload["period_structure"]["period_win_rate"] == pytest.approx(1.0)
    assert summary_payload["asset_concentration"]["top_symbol_name"] == "AAA"
    output = sink.getvalue()
    assert "=== 回测摘要 ===" in output
    assert "=== 核心利润来源（按调仓周期） ===" in output
    assert "=== 周期平滑度与集中度 ===" in output
    assert "=== 资产贡献集中度 ===" in output
    assert "=== 最近调仓周期详情 ===" in output
    assert "=== 全年主要贡献标的 ===" in output


def test_export_backtest_artifacts_writes_and_plots_benchmark_curve(tmp_path) -> None:
    result = _sample_backtest_result()
    benchmark_curve = result.equity_curve_df.with_columns(
        (pl.col("equity_curve") * 1.1).alias("equity_curve")
    )

    exported = export_backtest_artifacts(
        result,
        artifacts_dir=tmp_path,
        chart_title="Strategy vs Benchmark",
        save_chart=True,
        benchmark_equity_curve_df=benchmark_curve,
        strategy_label="Rotation",
        benchmark_label="VTI",
    )

    assert (tmp_path / "benchmark_equity_curve.csv").exists()
    assert (tmp_path / "equity.png").exists()
    assert exported.artifact_paths is not None
    assert exported.artifact_paths["benchmark_equity_curve_csv"].endswith(
        "benchmark_equity_curve.csv"
    )


def test_build_rebalance_period_analysis_empty_defaults() -> None:
    empty_result = BacktestResult(
        holdings_df=pl.DataFrame(),
        trades_df=pl.DataFrame(),
        returns_df=pl.DataFrame(),
        equity_curve_df=pl.DataFrame(),
        metrics={"initial_capital": 100.0, "end_nav": 100.0},
        effective_decisions_df=pl.DataFrame(),
    )

    periods_df, period_holdings_df, summary = build_rebalance_period_analysis(empty_result)

    assert periods_df.is_empty()
    assert period_holdings_df.is_empty()
    assert summary["period_structure"]["worst_6w_return"] == 0.0
    assert summary["period_structure"]["worst_12w_return"] == 0.0
    assert summary["asset_concentration"]["top1_profit_share"] == 0.0
    assert summary["asset_concentration"]["symbol_contributor_count"] == 0

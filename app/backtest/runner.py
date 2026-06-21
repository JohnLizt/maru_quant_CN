"""Strategy-decision-driven backtest runner."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import polars as pl
from loguru import logger
from sqlalchemy import text

from app.backtest.metrics import compute_metrics
from app.backtest.risk_overlay import RiskOverlayConfig, apply_risk_overlay
from app.services.strategy_service import StrategySnapshotBundle, build_strategy_snapshot
from app.strategy.base import BaseStrategy
from app.utils.db import get_engine


@dataclass
class BacktestResult:
    holdings_df: pl.DataFrame
    trades_df: pl.DataFrame
    returns_df: pl.DataFrame
    equity_curve_df: pl.DataFrame
    metrics: dict[str, float]
    log_path: str | None = None
    artifacts_dir: str | None = None
    equity_chart_path: str | None = None
    artifact_paths: dict[str, str] | None = None


@dataclass(frozen=True)
class StrategyBacktestBundle:
    signal_snapshot: pl.DataFrame
    decisions_df: pl.DataFrame
    backtest_result: BacktestResult


def _normalize_date(value: str | date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def _load_market_returns(
    asset_type: str,
    start_date: date,
    end_date: date,
    *,
    include_risk_fields: bool = False,
) -> pl.DataFrame:
    selected_columns = "time, symbol, pct_change, close, amount" if include_risk_fields else "time, symbol, pct_change"
    sql = text(
        f"""
        SELECT {selected_columns}
        FROM market.daily
        WHERE asset_type = :asset_type
          AND time >= :start_date
          AND time < (CAST(:end_date AS date) + INTERVAL '30 day')
        ORDER BY time, symbol
        """
    )
    with get_engine().connect() as conn:
        rows = conn.execute(
            sql,
            {
                "asset_type": asset_type,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
        ).fetchall()

    if not rows:
        schema = {"time": pl.Datetime("us", "UTC"), "symbol": pl.Utf8, "daily_return": pl.Float64}
        if include_risk_fields:
            schema.update({"close": pl.Float64, "amount": pl.Float64})
        return pl.DataFrame(schema=schema)

    if include_risk_fields:
        return pl.DataFrame(
            rows,
            schema=["time", "symbol", "pct_change", "close", "amount"],
            orient="row",
        ).with_columns(
            [
                (pl.col("pct_change").cast(pl.Float64) / 100.0).alias("daily_return"),
                pl.col("close").cast(pl.Float64),
                pl.col("amount").cast(pl.Float64),
            ]
        ).select(["time", "symbol", "daily_return", "close", "amount"]).sort(["time", "symbol"])

    return (
        pl.DataFrame(rows, schema=["time", "symbol", "pct_change"], orient="row")
        .with_columns((pl.col("pct_change").cast(pl.Float64) / 100.0).alias("daily_return"))
        .select(["time", "symbol", "daily_return"])
        .sort(["time", "symbol"])
    )


def _empty_holdings_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "time": pl.Date,
            "symbol": pl.Utf8,
            "target_weight": pl.Float64,
            "score": pl.Float64,
            "strategy": pl.Utf8,
            "rank": pl.UInt32,
            "tag": pl.Utf8,
            "metadata": pl.Utf8,
            "holding_start_date": pl.Date,
        }
    )


def _empty_trades_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "time": pl.Date,
            "symbol": pl.Utf8,
            "target_weight": pl.Float64,
            "prev_weight": pl.Float64,
            "delta_weight": pl.Float64,
        }
    )


def _empty_returns_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "time": pl.Date,
            "gross_return": pl.Float64,
            "turnover": pl.Float64,
            "cost": pl.Float64,
            "net_return": pl.Float64,
        }
    )


def _resolve_effective_dates(
    decision_dates: list[date],
    trading_dates: list[date],
    execution_lag: int,
) -> dict[date, date]:
    if execution_lag < 0:
        raise ValueError("execution_lag 不能小于 0")

    trading_index = {current: idx for idx, current in enumerate(trading_dates)}
    resolved: dict[date, date] = {}
    for decision_date in decision_dates:
        idx = trading_index.get(decision_date)
        if idx is None:
            continue
        effective_idx = idx + execution_lag
        if effective_idx >= len(trading_dates):
            continue
        resolved[decision_date] = trading_dates[effective_idx]
    return resolved


def _filter_rebalance_dates(
    decisions: pl.DataFrame,
    rebalance_frequency: str,
    rebalance_weekday: int | None,
) -> pl.DataFrame:
    if rebalance_frequency == "daily":
        return decisions
    if rebalance_frequency not in {"weekly", "biweekly"}:
        raise NotImplementedError(f"暂不支持的调仓频率: {rebalance_frequency}")
    if rebalance_weekday is None:
        raise ValueError(f"{rebalance_frequency} 调仓必须指定 rebalance_weekday")
    if not 0 <= rebalance_weekday <= 6:
        raise ValueError("rebalance_weekday 必须在 0~6 之间，采用 Python weekday 语义（周一=0）")

    weekday_filtered = decisions.filter((pl.col("time").dt.weekday() - 1) == pl.lit(rebalance_weekday))
    if rebalance_frequency == "weekly":
        return weekday_filtered
    if weekday_filtered.is_empty():
        return weekday_filtered

    selected_dates = sorted({
        current for current in weekday_filtered.get_column("time").dt.date().to_list() if current is not None
    })
    biweekly_dates = selected_dates[::2]
    return weekday_filtered.filter(pl.col("time").dt.date().is_in(biweekly_dates))


def _build_effective_holdings(
    decisions: pl.DataFrame,
    trading_dates: list[date],
    execution_lag: int,
    start_date: date,
    end_date: date,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    filtered = decisions.filter(
        (pl.col("time").dt.date() >= pl.lit(start_date))
        & (pl.col("time").dt.date() <= pl.lit(end_date))
    ).sort(["time", "rank", "symbol"])
    if filtered.is_empty():
        empty = _empty_holdings_frame()
        return empty, empty

    decision_dates = sorted({value for value in filtered.get_column("time").dt.date().to_list() if value is not None})
    effective_date_map = _resolve_effective_dates(decision_dates, trading_dates, execution_lag)
    if not effective_date_map:
        empty = _empty_holdings_frame()
        return empty, empty

    effective_frames: list[pl.DataFrame] = []
    for decision_date, effective_date in effective_date_map.items():
        current = filtered.filter(pl.col("time").dt.date() == pl.lit(decision_date)).with_columns(
            pl.lit(effective_date).cast(pl.Date).alias("effective_date")
        )
        effective_frames.append(current)

    effective_decisions = pl.concat(effective_frames).sort(["effective_date", "rank", "symbol"])
    effective_dates = effective_decisions.get_column("effective_date").unique().sort().to_list()
    logger.info("有效调仓日 {} 个: {}", len(effective_dates), [str(value) for value in effective_dates])

    holdings_frames: list[pl.DataFrame] = []
    for idx, effective_date in enumerate(effective_dates):
        next_effective = effective_dates[idx + 1] if idx + 1 < len(effective_dates) else None
        active_dates = [
            current
            for current in trading_dates
            if current >= effective_date and (next_effective is None or current < next_effective) and current <= end_date
        ]
        if not active_dates:
            continue
        current_holdings = (
            effective_decisions.filter(pl.col("effective_date") == pl.lit(effective_date))
            .drop(["time"])
            .rename({"target_weight": "decision_target_weight", "effective_date": "holding_start_date"})
        )
        logger.debug(
            "调仓日 {} 持仓列表: {}",
            effective_date,
            [
                {
                    "symbol": row["symbol"],
                    "target_weight": row["decision_target_weight"],
                    "rank": row["rank"],
                    "tag": row["tag"],
                    "score": row["score"],
                }
                for row in current_holdings.iter_rows(named=True)
            ],
        )
        expanded = current_holdings.join(
            pl.DataFrame({"time": active_dates}, schema={"time": pl.Date}),
            how="cross",
        ).rename({"decision_target_weight": "target_weight"})
        holdings_frames.append(expanded)

    holdings_df = pl.concat(holdings_frames) if holdings_frames else pl.DataFrame()
    return effective_decisions, holdings_df


def _build_trades(holdings_df: pl.DataFrame) -> pl.DataFrame:
    if holdings_df.is_empty():
        return pl.DataFrame(
            schema={"time": pl.Date, "symbol": pl.Utf8, "target_weight": pl.Float64, "prev_weight": pl.Float64, "delta_weight": pl.Float64}
        )

    dates = holdings_df.select("time").unique().sort("time")
    symbols = holdings_df.select("symbol").unique().sort("symbol")
    target_weights = holdings_df.select(["time", "symbol", "target_weight"]).unique()
    daily_weights = (
        dates.join(symbols, how="cross")
        .join(target_weights, on=["time", "symbol"], how="left")
        .with_columns(pl.col("target_weight").fill_null(0.0))
        .sort(["symbol", "time"])
    )
    return (
        daily_weights.with_columns(pl.col("target_weight").shift(1).over("symbol").fill_null(0.0).alias("prev_weight"))
        .with_columns((pl.col("target_weight") - pl.col("prev_weight")).alias("delta_weight"))
        .filter(pl.col("delta_weight") != 0)
        .sort(["time", "symbol"])
    )


def run_backtest(
    decisions: pl.DataFrame,
    *,
    asset_type: str,
    start: str | date | datetime,
    end: str | date | datetime,
    rebalance_frequency: str = "daily",
    rebalance_weekday: int | None = None,
    execution_lag: int = 1,
    commission_bps: float = 0.0,
    slippage_bps: float = 0.0,
    risk_config: RiskOverlayConfig | None = None,
) -> BacktestResult:
    """Run a generic backtest from StrategyDecisionTable rows."""

    start_date = _normalize_date(start)
    end_date = _normalize_date(end)
    if start_date > end_date:
        raise ValueError("start 不能晚于 end")
    if execution_lag < 0:
        raise ValueError("execution_lag 不能小于 0")
    logger.info(
        "开始回测 | asset_type={} | start={} | end={} | rebalance_frequency={} | rebalance_weekday={} | execution_lag={} | commission_bps={} | slippage_bps={} | risk_control={}",
        asset_type,
        start_date,
        end_date,
        rebalance_frequency,
        rebalance_weekday,
        execution_lag,
        commission_bps,
        slippage_bps,
        risk_config is not None,
    )

    required_columns = {
        "time",
        "asset_type",
        "strategy",
        "strategy_mode",
        "symbol",
        "decision_type",
        "signal",
        "target_weight",
        "score",
        "rank",
        "tag",
        "metadata",
    }
    missing_columns = required_columns - set(decisions.columns)
    if missing_columns:
        raise ValueError(f"decisions 缺少列: {sorted(missing_columns)}")

    filtered = decisions.filter(pl.col("asset_type") == asset_type).filter(pl.col("decision_type") == "target_weight")
    filtered = _filter_rebalance_dates(filtered, rebalance_frequency, rebalance_weekday)
    logger.info("输入 decision 行数: {} | 过滤后调仓 decision 行数: {}", decisions.height, filtered.height)
    if filtered.is_empty():
        logger.warning("无可用调仓 decision，返回空回测结果")
        empty_holdings = _empty_holdings_frame()
        empty_trades = _empty_trades_frame()
        empty_returns = _empty_returns_frame()
        return BacktestResult(
            holdings_df=empty_holdings,
            trades_df=empty_trades,
            returns_df=empty_returns,
            equity_curve_df=empty_returns,
            metrics={},
        )

    market_returns = _load_market_returns(
        asset_type,
        start_date,
        end_date,
        include_risk_fields=risk_config is not None,
    )
    logger.info("市场收益记录数: {}", market_returns.height)
    trading_dates = sorted({current for current in market_returns.get_column("time").dt.date().to_list() if current is not None})
    effective_decisions, holdings_df = _build_effective_holdings(
        filtered,
        trading_dates,
        execution_lag,
        start_date,
        end_date,
    )
    logger.info(
        "回测中间结果 | effective_decisions={} | holdings={}",
        effective_decisions.height,
        holdings_df.height,
    )

    if holdings_df.is_empty():
        logger.warning("无有效持仓展开结果，返回空回测结果")
        empty_trades = _empty_trades_frame()
        empty_returns = _empty_returns_frame()
        return BacktestResult(
            holdings_df=holdings_df,
            trades_df=empty_trades,
            returns_df=empty_returns,
            equity_curve_df=empty_returns,
            metrics={},
        )

    risk_metrics: dict[str, int] = {}
    if risk_config is not None:
        holdings_df, risk_metrics = apply_risk_overlay(holdings_df, market_returns, risk_config)
        logger.info(
            "风控 overlay 完成 | risk_half_events={} | stop_loss_events={}",
            risk_metrics.get("risk_half_events", 0),
            risk_metrics.get("stop_loss_events", 0),
        )

    holdings_for_join = holdings_df.with_columns(pl.col("time").cast(pl.Datetime("us", "UTC")))
    joined = (
        holdings_for_join.join(market_returns, on=["time", "symbol"], how="left")
        .with_columns(pl.col("daily_return").fill_null(0.0))
        .with_columns((pl.col("target_weight") * pl.col("daily_return")).alias("weighted_return"))
    )
    trades_df = _build_trades(holdings_df)
    logger.info("交易记录数: {}", trades_df.height)
    cost_rate = (commission_bps + slippage_bps) / 10000.0
    trade_costs = (
        trades_df.group_by("time")
        .agg(pl.col("delta_weight").abs().sum().alias("turnover"))
        .with_columns((pl.col("turnover") * cost_rate).alias("cost"))
    )

    returns_df = (
        joined.group_by("time")
        .agg(pl.col("weighted_return").sum().alias("gross_return"))
        .with_columns(pl.col("time").dt.date().alias("time"))
        .join(trade_costs, on="time", how="left")
        .with_columns([
            pl.col("turnover").fill_null(0.0),
            pl.col("cost").fill_null(0.0),
        ])
        .with_columns((pl.col("gross_return") - pl.col("cost")).alias("net_return"))
        .sort("time")
    )
    logger.info("收益记录数: {}", returns_df.height)

    equity_curve_df = (
        returns_df.with_columns((pl.col("net_return") + 1.0).cum_prod().alias("equity_curve"))
        .select(["time", "gross_return", "turnover", "cost", "net_return", "equity_curve"])
    )
    metrics = compute_metrics(
        equity_curve_df.get_column("net_return"),
        freq="daily",
    ) if equity_curve_df.height else {}
    if risk_config is not None:
        metrics.update(risk_metrics)
    logger.success("回测完成 | metrics={}", metrics)

    return BacktestResult(
        holdings_df=holdings_df.sort(["time", "rank", "symbol"]),
        trades_df=trades_df.sort(["time", "symbol"]),
        returns_df=returns_df,
        equity_curve_df=equity_curve_df,
        metrics=metrics,
    )


def run_strategy_backtest(
    strategy: BaseStrategy,
    *,
    asset_type: str,
    profile_name: str,
    start: str | date | datetime,
    end: str | date | datetime,
    symbols: list[str] | None = None,
    rebalance_frequency: str = "weekly",
    rebalance_weekday: int | None = 2,
    execution_lag: int = 1,
    commission_bps: float = 5.0,
    slippage_bps: float = 5.0,
    log_path: str | None = None,
    artifacts_dir: str | None = None,
    equity_chart_path: str | None = None,
    snapshot_kwargs: dict[str, Any] | None = None,
    risk_config: RiskOverlayConfig | None = None,
) -> StrategyBacktestBundle:
    """Build signal snapshot + strategy decisions, then run backtest."""

    bundle: StrategySnapshotBundle = build_strategy_snapshot(
        strategy,
        symbols=symbols,
        start_date=start,
        end_date=end,
        asset_type=asset_type,
        profile_name=profile_name,
        **(snapshot_kwargs or {}),
    )
    result = run_backtest(
        bundle.decisions,
        asset_type=asset_type,
        start=start,
        end=end,
        rebalance_frequency=rebalance_frequency,
        rebalance_weekday=rebalance_weekday,
        execution_lag=execution_lag,
        commission_bps=commission_bps,
        slippage_bps=slippage_bps,
        risk_config=risk_config,
    )
    result.log_path = log_path
    result.artifacts_dir = artifacts_dir
    result.equity_chart_path = equity_chart_path
    return StrategyBacktestBundle(
        signal_snapshot=bundle.signal_snapshot,
        decisions_df=bundle.decisions,
        backtest_result=result,
    )

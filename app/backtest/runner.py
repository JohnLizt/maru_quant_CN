"""Strategy-decision-driven cash-account backtest runner."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import polars as pl
from loguru import logger
from sqlalchemy import text

from app.backtest.metrics import compute_metrics
from app.backtest.risk_overlay import RiskOverlayConfig, build_risk_features, get_risk_reason
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


@dataclass
class PositionState:
    asset_type: str
    shares: float
    buy_price: float
    buy_date: date
    risk_half_triggered: bool
    strategy: str
    score: float
    rank: int
    tag: str
    metadata: str


def _normalize_date(value: str | date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def _load_market_data(asset_types: list[str], start_date: date, end_date: date) -> pl.DataFrame:
    sql = text(
        """
        SELECT time, asset_type, symbol, open, high, low, close, amount, pct_change
        FROM market.daily
        WHERE asset_type = ANY(:asset_types)
          AND time >= :start_date
          AND time <= :end_date
        ORDER BY time, asset_type, symbol
        """
    )
    with get_engine().connect() as conn:
        rows = conn.execute(
            sql,
            {
                "asset_types": asset_types,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
        ).fetchall()

    schema = {
        "time": pl.Date,
        "asset_type": pl.Utf8,
        "symbol": pl.Utf8,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "amount": pl.Float64,
        "daily_return": pl.Float64,
        "ohlc4": pl.Float64,
    }
    if not rows:
        return pl.DataFrame(schema=schema)

    return (
        pl.DataFrame(
            rows,
            schema=["time", "asset_type", "symbol", "open", "high", "low", "close", "amount", "pct_change"],
            orient="row",
        )
        .with_columns(
            [
                pl.col("time").cast(pl.Date),
                pl.col("asset_type").cast(pl.Utf8),
                pl.col("open").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("amount").cast(pl.Float64),
                (pl.col("pct_change").cast(pl.Float64) / 100.0).alias("daily_return"),
                ((pl.col("open") + pl.col("high") + pl.col("low") + pl.col("close")) / 4.0).alias("ohlc4"),
            ]
        )
        .select(["time", "asset_type", "symbol", "open", "high", "low", "close", "amount", "daily_return", "ohlc4"])
        .sort(["time", "asset_type", "symbol"])
    )


def _empty_holdings_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "time": pl.Date,
            "asset_type": pl.Utf8,
            "symbol": pl.Utf8,
            "shares": pl.Float64,
            "close": pl.Float64,
            "market_value": pl.Float64,
            "weight": pl.Float64,
            "buy_price": pl.Float64,
            "buy_date": pl.Date,
            "risk_half_triggered": pl.Boolean,
            "strategy": pl.Utf8,
            "score": pl.Float64,
            "rank": pl.UInt32,
            "tag": pl.Utf8,
            "metadata": pl.Utf8,
        }
    )


def _empty_trades_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "time": pl.Date,
            "asset_type": pl.Utf8,
            "symbol": pl.Utf8,
            "action": pl.Utf8,
            "side": pl.Utf8,
            "price": pl.Float64,
            "shares": pl.Float64,
            "notional": pl.Float64,
            "fee": pl.Float64,
            "cash_before": pl.Float64,
            "cash_after": pl.Float64,
            "nav_after_trade": pl.Float64,
            "signal_date": pl.Date,
            "risk_reason": pl.Utf8,
        }
    )


def _empty_returns_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "time": pl.Date,
            "nav": pl.Float64,
            "cash": pl.Float64,
            "cash_ratio": pl.Float64,
            "gross_return": pl.Float64,
            "cost": pl.Float64,
            "net_return": pl.Float64,
            "turnover": pl.Float64,
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


def _build_effective_decisions(
    decisions: pl.DataFrame,
    trading_dates: list[date],
    execution_lag: int,
    start_date: date,
    end_date: date,
) -> pl.DataFrame:
    filtered = decisions.filter(
        (pl.col("time").dt.date() >= pl.lit(start_date))
        & (pl.col("time").dt.date() <= pl.lit(end_date))
    ).sort(["time", "rank", "symbol"])
    if filtered.is_empty():
        return pl.DataFrame()

    decision_dates = sorted({value for value in filtered.get_column("time").dt.date().to_list() if value is not None})
    effective_date_map = _resolve_effective_dates(decision_dates, trading_dates, execution_lag)
    if not effective_date_map:
        return pl.DataFrame()

    effective_frames: list[pl.DataFrame] = []
    for decision_date, effective_date in effective_date_map.items():
        effective_frames.append(
            filtered.filter(pl.col("time").dt.date() == pl.lit(decision_date)).with_columns(
                pl.lit(decision_date).cast(pl.Date).alias("signal_date"),
                pl.lit(effective_date).cast(pl.Date).alias("effective_date"),
            )
        )
    return pl.concat(effective_frames).sort(["effective_date", "rank", "symbol"])


def _prepare_market_calendar(market_data: pl.DataFrame, instruments: pl.DataFrame) -> pl.DataFrame:
    if market_data.is_empty() or instruments.is_empty():
        empty_schema: dict[str, pl.DataType] = {
            "time": pl.Date,
            "asset_type": pl.Utf8,
            "symbol": pl.Utf8,
            "close": pl.Float64,
            "ohlc4": pl.Float64,
            "amount": pl.Float64,
        }
        if "std_score" in market_data.columns:
            empty_schema["std_score"] = pl.Float64
        if "cv" in market_data.columns:
            empty_schema["cv"] = pl.Float64
        return pl.DataFrame(schema=empty_schema)

    trading_dates = market_data.select("time").unique().sort("time")
    base_columns = ["time", "symbol", "close", "ohlc4", "amount"]
    fill_columns = ["close", "ohlc4", "amount"]
    if "std_score" in market_data.columns:
        base_columns.append("std_score")
        fill_columns.append("std_score")
    if "cv" in market_data.columns:
        base_columns.append("cv")
        fill_columns.append("cv")

    base_columns.insert(1, "asset_type")
    calendar = (
        trading_dates.join(instruments, how="cross")
        .join(market_data.select(base_columns), on=["time", "asset_type", "symbol"], how="left")
        .sort(["asset_type", "symbol", "time"])
        .with_columns(
            [pl.col(column).fill_null(strategy="forward").over(["asset_type", "symbol"]) for column in fill_columns]
        )
    )
    return calendar


def _position_nav(
    positions: dict[tuple[str, str], PositionState],
    price_map: dict[tuple[date, str, str], dict[str, float | None]],
    current_date: date,
) -> float:
    nav = 0.0
    for (asset_type, symbol), position in positions.items():
        close = price_map.get((current_date, asset_type, symbol), {}).get("close")
        if close is not None:
            nav += position.shares * float(close)
    return nav


def _commission(notional: float, cost_rate: float, commission_min: float) -> float:
    if notional <= 0:
        return 0.0
    return max(notional * cost_rate, commission_min)


def _cash_after_buy(cash_budget: float, price: float, cost_rate: float, commission_min: float) -> tuple[float, float, float]:
    if cash_budget <= 0 or price <= 0:
        return 0.0, 0.0, cash_budget

    def total_cost(shares: float) -> float:
        notional = shares * price
        return notional + _commission(notional, cost_rate, commission_min)

    lo, hi = 0.0, cash_budget / price
    for _ in range(64):
        mid = (lo + hi) / 2.0
        if total_cost(mid) <= cash_budget:
            lo = mid
        else:
            hi = mid
    shares = lo
    notional = shares * price
    fee = _commission(notional, cost_rate, commission_min)
    return shares, fee, cash_budget - notional - fee


def _proceeds_after_sell(shares: float, price: float, cost_rate: float, commission_min: float) -> tuple[float, float]:
    if shares <= 0 or price <= 0:
        return 0.0, 0.0
    notional = shares * price
    fee = _commission(notional, cost_rate, commission_min)
    return notional - fee, fee


def run_backtest(
    decisions: pl.DataFrame,
    *,
    asset_type: str | None = None,
    start: str | date | datetime,
    end: str | date | datetime,
    rebalance_frequency: str = "daily",
    rebalance_weekday: int | None = None,
    execution_lag: int = 1,
    commission_bps: float = 0.0,
    slippage_bps: float = 0.0,
    risk_config: RiskOverlayConfig | None = None,
    initial_capital: float = 40000.0,
    commission_min: float = 0.01,
    cash_interest_rate: float = 0.01,
) -> BacktestResult:
    """Run a cash-account backtest from StrategyDecisionTable rows."""

    start_date = _normalize_date(start)
    end_date = _normalize_date(end)
    if start_date > end_date:
        raise ValueError("start 不能晚于 end")
    if execution_lag < 0:
        raise ValueError("execution_lag 不能小于 0")
    if initial_capital <= 0:
        raise ValueError("initial_capital 必须大于 0")
    logger.info(
        "开始回测 | asset_type={} | start={} | end={} | rebalance_frequency={} | rebalance_weekday={} | execution_lag={} | commission_bps={} | slippage_bps={} | risk_control={} | initial_capital={} | commission_min={} | cash_interest_rate={}",
        asset_type or "ALL",
        start_date,
        end_date,
        rebalance_frequency,
        rebalance_weekday,
        execution_lag,
        commission_bps,
        slippage_bps,
        risk_config is not None,
        initial_capital,
        commission_min,
        cash_interest_rate,
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

    filtered = decisions.filter(pl.col("decision_type") == "target_weight")
    if asset_type is not None:
        filtered = filtered.filter(pl.col("asset_type") == asset_type)
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
            equity_curve_df=empty_returns.with_columns(pl.lit(1.0).alias("equity_curve")),
            metrics={},
        )

    decision_asset_types = sorted({
        str(current)
        for current in filtered.get_column("asset_type").drop_nulls().unique().to_list()
    })
    market_data = _load_market_data(decision_asset_types, start_date, end_date)
    logger.info("市场行情记录数: {}", market_data.height)
    trading_dates = sorted({current for current in market_data.get_column("time").to_list() if current is not None})
    effective_decisions = _build_effective_decisions(filtered, trading_dates, execution_lag, start_date, end_date)
    logger.info("有效调仓 decision 行数: {}", effective_decisions.height)
    if effective_decisions.is_empty():
        logger.warning("无有效调仓 decision，返回空回测结果")
        empty_holdings = _empty_holdings_frame()
        empty_trades = _empty_trades_frame()
        empty_returns = _empty_returns_frame()
        return BacktestResult(
            holdings_df=empty_holdings,
            trades_df=empty_trades,
            returns_df=empty_returns,
            equity_curve_df=empty_returns.with_columns(pl.lit(1.0).alias("equity_curve")),
            metrics={},
        )

    tracked_instruments = (
        pl.concat(
            [
                market_data.select(["asset_type", "symbol"]),
                effective_decisions.select(["asset_type", "symbol"]),
            ],
            how="vertical",
        )
        .unique()
        .sort(["asset_type", "symbol"])
    )
    price_calendar = _prepare_market_calendar(market_data, tracked_instruments)
    if risk_config is not None:
        price_calendar = build_risk_features(price_calendar, risk_config)
    else:
        price_calendar = price_calendar.with_columns(
            [
                pl.lit(None, dtype=pl.Float64).alias("std_score"),
                pl.lit(None, dtype=pl.Float64).alias("cv"),
            ]
        )

    price_map: dict[tuple[date, str, str], dict[str, float | None]] = {}
    for row in price_calendar.select(["time", "asset_type", "symbol", "close", "ohlc4", "std_score", "cv"]).iter_rows(named=True):
        price_map[(row["time"], row["asset_type"], row["symbol"])] = {
            "close": row["close"],
            "ohlc4": row["ohlc4"],
            "std_score": row["std_score"],
            "cv": row["cv"],
        }

    rebalance_map: dict[date, list[dict[str, Any]]] = {}
    for row in effective_decisions.iter_rows(named=True):
        rebalance_map.setdefault(row["effective_date"], []).append(row)
    active_dates = sorted([current for current in rebalance_map])
    logger.info("有效调仓日 {} 个: {}", len(active_dates), [str(value) for value in active_dates])

    first_active_date = min(active_dates)
    backtest_dates = [current for current in trading_dates if first_active_date <= current <= end_date]
    cost_rate = (commission_bps + slippage_bps) / 10000.0
    daily_interest_rate = cash_interest_rate / 365.0

    cash = initial_capital
    positions: dict[tuple[str, str], PositionState] = {}
    trade_rows: list[dict[str, Any]] = []
    holdings_rows: list[dict[str, Any]] = []
    return_rows: list[dict[str, Any]] = []
    risk_half_events = 0
    stop_loss_events = 0
    rebalance_trade_events = 0
    prev_nav = initial_capital

    def append_trade(
        *,
        current_date: date,
        asset_type: str,
        symbol: str,
        action: str,
        side: str,
        price: float,
        shares: float,
        notional: float,
        fee: float,
        cash_before: float,
        cash_after: float,
        signal_date: date | None,
        risk_reason: str = "",
    ) -> None:
        nav_after_trade = cash_after + _position_nav(positions, price_map, current_date)
        trade_rows.append(
            {
                "time": current_date,
                "asset_type": asset_type,
                "symbol": symbol,
                "action": action,
                "side": side,
                "price": price,
                "shares": shares,
                "notional": notional,
                "fee": fee,
                "cash_before": cash_before,
                "cash_after": cash_after,
                "nav_after_trade": nav_after_trade,
                "signal_date": signal_date,
                "risk_reason": risk_reason,
            }
        )

    for current_date in backtest_dates:
        daily_fee = 0.0
        daily_notional = 0.0

        # Risk half-sell only fires once per continuous holding cycle.
        if risk_config is not None:
            for asset_type_key, symbol in sorted(list(positions.keys())):
                position = positions.get((asset_type_key, symbol))
                if position is None or position.risk_half_triggered:
                    continue
                feature = price_map.get((current_date, asset_type_key, symbol), {})
                price = feature.get("ohlc4")
                risk_reason = get_risk_reason(feature.get("std_score"), feature.get("cv"), risk_config)
                if price is None or price <= 0 or risk_reason is None:
                    continue

                sell_shares = position.shares / 2.0
                cash_before = cash
                proceeds, fee = _proceeds_after_sell(sell_shares, float(price), cost_rate, commission_min)
                cash += proceeds
                daily_fee += fee
                daily_notional += sell_shares * float(price)
                position.shares -= sell_shares
                position.risk_half_triggered = True
                risk_half_events += 1
                append_trade(
                    current_date=current_date,
                    asset_type=asset_type_key,
                    symbol=symbol,
                    action="风险半仓",
                    side="sell",
                    price=float(price),
                    shares=sell_shares,
                    notional=sell_shares * float(price),
                    fee=fee,
                    cash_before=cash_before,
                    cash_after=cash,
                    signal_date=current_date,
                    risk_reason=risk_reason,
                )

        # Stop-loss is checked on close, then sold at the same day's OHLC4.
        if risk_config is not None:
            for asset_type_key, symbol in sorted(list(positions.keys())):
                position = positions.get((asset_type_key, symbol))
                if position is None:
                    continue
                feature = price_map.get((current_date, asset_type_key, symbol), {})
                close_price = feature.get("close")
                trade_price = feature.get("ohlc4")
                if close_price is None or trade_price is None or trade_price <= 0 or position.buy_price <= 0:
                    continue
                if float(close_price) / position.buy_price - 1.0 > -risk_config.stop_loss_rate:
                    continue

                shares = position.shares
                cash_before = cash
                proceeds, fee = _proceeds_after_sell(shares, float(trade_price), cost_rate, commission_min)
                cash += proceeds
                daily_fee += fee
                daily_notional += shares * float(trade_price)
                append_trade(
                    current_date=current_date,
                    asset_type=asset_type_key,
                    symbol=symbol,
                    action="止损",
                    side="sell",
                    price=float(trade_price),
                    shares=shares,
                    notional=shares * float(trade_price),
                    fee=fee,
                    cash_before=cash_before,
                    cash_after=cash,
                    signal_date=current_date,
                    risk_reason="stop_loss",
                )
                del positions[(asset_type_key, symbol)]
                stop_loss_events += 1

        if current_date in rebalance_map:
            rebalance_rows = sorted(rebalance_map[current_date], key=lambda row: (row["rank"], row["symbol"]))
            target_symbols = {(str(row["asset_type"]), str(row["symbol"])) for row in rebalance_rows}

            for asset_type_key, symbol in sorted(list(positions.keys())):
                position = positions[(asset_type_key, symbol)]
                if (asset_type_key, symbol) in target_symbols:
                    continue
                trade_price = price_map.get((current_date, asset_type_key, symbol), {}).get("ohlc4")
                if trade_price is None or trade_price <= 0:
                    continue

                shares = position.shares
                cash_before = cash
                proceeds, fee = _proceeds_after_sell(shares, float(trade_price), cost_rate, commission_min)
                cash += proceeds
                daily_fee += fee
                daily_notional += shares * float(trade_price)
                append_trade(
                    current_date=current_date,
                    asset_type=asset_type_key,
                    symbol=symbol,
                    action="调仓卖出",
                    side="sell",
                    price=float(trade_price),
                    shares=shares,
                    notional=shares * float(trade_price),
                    fee=fee,
                    cash_before=cash_before,
                    cash_after=cash,
                    signal_date=rebalance_rows[0]["signal_date"],
                )
                del positions[(asset_type_key, symbol)]
                rebalance_trade_events += 1

            buy_rows = [
                row
                for row in rebalance_rows
                if (str(row["asset_type"]), str(row["symbol"])) not in positions
            ]
            buy_budget_base = cash
            total_weight = sum(float(row["target_weight"]) for row in buy_rows if float(row["target_weight"]) > 0)
            for row in buy_rows:
                if total_weight <= 0 or buy_budget_base <= 0:
                    break
                symbol = str(row["symbol"])
                instrument_asset_type = str(row["asset_type"])
                trade_price = price_map.get((current_date, instrument_asset_type, symbol), {}).get("ohlc4")
                if trade_price is None or trade_price <= 0:
                    continue

                allocation = buy_budget_base * float(row["target_weight"]) / total_weight
                risk_reason = ""
                risk_half_triggered = False
                if risk_config is not None:
                    feature = price_map.get((current_date, instrument_asset_type, symbol), {})
                    risk_reason = get_risk_reason(feature.get("std_score"), feature.get("cv"), risk_config) or ""
                    if risk_reason:
                        allocation *= risk_config.half_weight
                        risk_half_triggered = True
                        risk_half_events += 1

                cash_before = cash
                shares, fee, leftover = _cash_after_buy(allocation, float(trade_price), cost_rate, commission_min)
                if shares <= 0:
                    continue

                notional = shares * float(trade_price)
                spent_cash = allocation - leftover
                cash -= spent_cash
                daily_fee += fee
                daily_notional += notional
                positions[(instrument_asset_type, symbol)] = PositionState(
                    asset_type=instrument_asset_type,
                    shares=shares,
                    buy_price=float(trade_price),
                    buy_date=current_date,
                    risk_half_triggered=risk_half_triggered,
                    strategy=str(row["strategy"]),
                    score=float(row["score"]),
                    rank=int(row["rank"]),
                    tag=str(row["tag"]),
                    metadata=str(row["metadata"]),
                )
                append_trade(
                    current_date=current_date,
                    asset_type=instrument_asset_type,
                    symbol=symbol,
                    action="调仓买入",
                    side="buy",
                    price=float(trade_price),
                    shares=shares,
                    notional=notional,
                    fee=fee,
                    cash_before=cash_before,
                    cash_after=cash,
                    signal_date=row["signal_date"],
                    risk_reason=risk_reason,
                )
                rebalance_trade_events += 1

        cash *= 1.0 + daily_interest_rate
        market_value = _position_nav(positions, price_map, current_date)
        nav = cash + market_value
        cash_ratio = cash / nav if nav > 0 else 0.0
        cost_ratio = daily_fee / prev_nav if prev_nav > 0 else 0.0
        net_return = nav / prev_nav - 1.0 if prev_nav > 0 else 0.0
        gross_return = net_return + cost_ratio
        turnover = daily_notional / prev_nav if prev_nav > 0 else 0.0
        return_rows.append(
            {
                "time": current_date,
                "nav": nav,
                "cash": cash,
                "cash_ratio": cash_ratio,
                "gross_return": gross_return,
                "cost": cost_ratio,
                "net_return": net_return,
                "turnover": turnover,
            }
        )

        for (asset_type_key, symbol), position in sorted(positions.items()):
            close_price = price_map.get((current_date, asset_type_key, symbol), {}).get("close")
            if close_price is None:
                continue
            current_value = position.shares * float(close_price)
            holdings_rows.append(
                {
                    "time": current_date,
                    "asset_type": asset_type_key,
                    "symbol": symbol,
                    "shares": position.shares,
                    "close": float(close_price),
                    "market_value": current_value,
                    "weight": current_value / nav if nav > 0 else 0.0,
                    "buy_price": position.buy_price,
                    "buy_date": position.buy_date,
                    "risk_half_triggered": position.risk_half_triggered,
                    "strategy": position.strategy,
                    "score": position.score,
                    "rank": position.rank,
                    "tag": position.tag,
                    "metadata": position.metadata,
                }
            )
        prev_nav = nav

    holdings_df = pl.DataFrame(holdings_rows) if holdings_rows else _empty_holdings_frame()
    trades_df = pl.DataFrame(trade_rows) if trade_rows else _empty_trades_frame()
    returns_df = pl.DataFrame(return_rows) if return_rows else _empty_returns_frame()
    if not returns_df.is_empty():
        returns_df = returns_df.sort("time")
    equity_curve_df = (
        returns_df.with_columns((pl.col("nav") / initial_capital).alias("equity_curve"))
        if not returns_df.is_empty()
        else returns_df.with_columns(pl.lit(1.0).alias("equity_curve"))
    )

    metrics = compute_metrics(returns_df.get_column("net_return"), freq="daily") if not returns_df.is_empty() else {}
    if not returns_df.is_empty():
        end_nav = float(returns_df.get_column("nav").tail(1).item())
        metrics.update(
            {
                "initial_capital": float(round(initial_capital, 4)),
                "end_nav": float(round(end_nav, 4)),
                "total_return": float(round(end_nav / initial_capital - 1.0, 4)),
                "risk_half_events": float(risk_half_events),
                "stop_loss_events": float(stop_loss_events),
                "rebalance_trade_events": float(rebalance_trade_events),
            }
        )
    logger.success("回测完成 | metrics={}", metrics)

    return BacktestResult(
        holdings_df=holdings_df.sort(["time", "asset_type", "symbol"]) if not holdings_df.is_empty() else holdings_df,
        trades_df=trades_df.sort(["time", "asset_type", "symbol"]) if not trades_df.is_empty() else trades_df,
        returns_df=returns_df,
        equity_curve_df=equity_curve_df.select(["time", "gross_return", "turnover", "cost", "net_return", "equity_curve"]),
        metrics=metrics,
    )


def run_strategy_backtest(
    strategy: BaseStrategy,
    *,
    asset_type: str | None = None,
    universe: str | None = None,
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
    initial_capital: float = 40000.0,
    commission_min: float = 0.01,
    cash_interest_rate: float = 0.01,
) -> StrategyBacktestBundle:
    """Build signal snapshot + strategy decisions, then run a cash-account backtest."""

    bundle: StrategySnapshotBundle = build_strategy_snapshot(
        strategy,
        symbols=symbols,
        start_date=start,
        end_date=end,
        asset_type=asset_type,
        universe=universe,
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
        initial_capital=initial_capital,
        commission_min=commission_min,
        cash_interest_rate=cash_interest_rate,
    )
    result.log_path = log_path
    result.artifacts_dir = artifacts_dir
    result.equity_chart_path = equity_chart_path
    return StrategyBacktestBundle(
        signal_snapshot=bundle.signal_snapshot,
        decisions_df=bundle.decisions,
        backtest_result=result,
    )

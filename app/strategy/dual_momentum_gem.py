"""Gary Antonacci Global Equities Momentum decision module."""
from __future__ import annotations

import json
from datetime import date

import polars as pl

from app.strategy.base import BaseStrategy, STRATEGY_DECISION_SCHEMA


GEM_SIGNAL_SCHEMA: dict[str, pl.DataType] = {
    "time": pl.Datetime("us", "UTC"),
    "asset_type": pl.Utf8,
    "signal_mode": pl.Utf8,
    "symbol": pl.Utf8,
    "composite_score": pl.Float64,
    "us_return_12m": pl.Float64,
    "ex_us_return_12m": pl.Float64,
    "t_bill_return_12m": pl.Float64,
}


def _monthly_symbol_returns(
    market_data: pl.DataFrame,
    symbol: str,
    price_column: str,
    lookback_months: int,
    output_prefix: str,
) -> pl.DataFrame:
    monthly = (
        market_data.filter(pl.col("symbol") == symbol)
        .sort("time")
        .with_columns(
            (
                pl.col("time").dt.year() * 12
                + pl.col("time").dt.month()
            ).alias("_month_index")
        )
        .group_by("_month_index")
        .agg(
            pl.col("time").last().alias(f"_{output_prefix}_time"),
            pl.col(price_column).last().alias(f"_{output_prefix}_price"),
        )
        .sort("_month_index")
    )
    if monthly.is_empty():
        return monthly

    lag_prices = monthly.select(
        (pl.col("_month_index") + lookback_months).alias("_month_index"),
        pl.col(f"_{output_prefix}_price").alias(f"_{output_prefix}_lag_price"),
    )
    return (
        monthly.join(lag_prices, on="_month_index", how="left")
        .with_columns(
            (
                pl.col(f"_{output_prefix}_price")
                / pl.col(f"_{output_prefix}_lag_price")
                - 1.0
            ).alias(f"{output_prefix}_return_12m")
        )
        .select(
            "_month_index",
            f"_{output_prefix}_time",
            f"{output_prefix}_return_12m",
        )
    )


def _monthly_symbol_presence(
    market_data: pl.DataFrame,
    symbol: str,
) -> pl.DataFrame:
    return (
        market_data.filter(pl.col("symbol") == symbol)
        .sort("time")
        .with_columns(
            (
                pl.col("time").dt.year() * 12
                + pl.col("time").dt.month()
            ).alias("_month_index")
        )
        .group_by("_month_index")
        .agg(pl.col("time").last().alias("_defensive_time"))
    )


def build_gem_signal_snapshot(
    market_data: pl.DataFrame,
    *,
    us_symbol: str = "SPY",
    ex_us_symbol: str = "ACWX",
    defensive_symbol: str = "AGG",
    t_bill_symbol: str = "BIL",
    lookback_months: int = 12,
    asset_type: str = "etf_US",
) -> pl.DataFrame:
    """Build month-end GEM momentum inputs from total-return market prices."""
    if lookback_months <= 0:
        raise ValueError("lookback_months 必须大于 0")
    required_columns = {"time", "asset_type", "symbol", "close"}
    missing_columns = required_columns - set(market_data.columns)
    if missing_columns:
        raise ValueError(f"market_data 缺少列: {sorted(missing_columns)}")

    symbols = {
        "us": us_symbol.strip().upper(),
        "ex_us": ex_us_symbol.strip().upper(),
        "defensive": defensive_symbol.strip().upper(),
        "t_bill": t_bill_symbol.strip().upper(),
    }
    if any(not symbol for symbol in symbols.values()):
        raise ValueError("GEM symbols 不能为空")
    if len(set(symbols.values())) != len(symbols):
        raise ValueError("GEM 的四个角色必须使用不同 symbol")

    selected = (
        market_data.filter(
            (pl.col("asset_type") == asset_type)
            & pl.col("symbol").is_in(list(symbols.values()))
        )
        .with_columns(
            pl.col("time").cast(pl.Date),
            pl.col("close").cast(pl.Float64),
        )
    )
    available_symbols = set(selected.get_column("symbol").unique().to_list()) if not selected.is_empty() else set()
    missing_symbols = set(symbols.values()) - available_symbols
    if missing_symbols:
        raise ValueError(f"market_data 缺少 GEM symbols: {sorted(missing_symbols)}")

    price_expression = pl.col("close")
    if "adj_factor" in selected.columns:
        valid_factor = (
            pl.when(
                pl.col("adj_factor").cast(pl.Float64).is_finite()
                & (pl.col("adj_factor").cast(pl.Float64) > 0)
            )
            .then(pl.col("adj_factor").cast(pl.Float64))
            .otherwise(1.0)
        )
        price_expression = price_expression * valid_factor
    selected = selected.with_columns(price_expression.alias("_total_return_price"))

    us = _monthly_symbol_returns(
        selected,
        symbols["us"],
        "_total_return_price",
        lookback_months,
        "us",
    )
    ex_us = _monthly_symbol_returns(
        selected,
        symbols["ex_us"],
        "_total_return_price",
        lookback_months,
        "ex_us",
    )
    t_bill = _monthly_symbol_returns(
        selected,
        symbols["t_bill"],
        "_total_return_price",
        lookback_months,
        "t_bill",
    )
    defensive = _monthly_symbol_presence(selected, symbols["defensive"])

    snapshot = (
        us.join(ex_us, on="_month_index", how="inner")
        .join(t_bill, on="_month_index", how="inner")
        .join(defensive, on="_month_index", how="inner")
        .drop_nulls(["us_return_12m", "ex_us_return_12m", "t_bill_return_12m"])
        .with_columns(
            pl.col("_us_time")
            .cast(pl.Datetime("us"))
            .dt.replace_time_zone("UTC")
            .alias("time"),
            pl.lit(asset_type).alias("asset_type"),
            pl.lit("time_series").alias("signal_mode"),
            pl.lit(symbols["us"]).alias("symbol"),
            (pl.col("us_return_12m") - pl.col("t_bill_return_12m")).alias("composite_score"),
        )
        .select(list(GEM_SIGNAL_SCHEMA))
        .sort("time")
    )
    return snapshot


class DualMomentumGEMStrategy(BaseStrategy):
    """Select US equity, ex-US equity, or aggregate bonds once per month."""

    strategy_name = "dual_momentum_gem_v1"
    strategy_mode = "time_series"
    supported_signal_modes = ("time_series",)
    supported_asset_types = ("etf_US",)

    def __init__(
        self,
        *,
        us_symbol: str = "SPY",
        ex_us_symbol: str = "ACWX",
        defensive_symbol: str = "AGG",
        t_bill_symbol: str = "BIL",
        lookback_months: int = 12,
    ) -> None:
        if lookback_months <= 0:
            raise ValueError("lookback_months 必须大于 0")
        self.us_symbol = us_symbol.strip().upper()
        self.ex_us_symbol = ex_us_symbol.strip().upper()
        self.defensive_symbol = defensive_symbol.strip().upper()
        self.t_bill_symbol = t_bill_symbol.strip().upper()
        self.lookback_months = lookback_months
        symbols = {
            self.us_symbol,
            self.ex_us_symbol,
            self.defensive_symbol,
            self.t_bill_symbol,
        }
        if "" in symbols:
            raise ValueError("GEM symbols 不能为空")
        if len(symbols) != 4:
            raise ValueError("GEM 的四个角色必须使用不同 symbol")

    def build_signal_snapshot(self, market_data: pl.DataFrame) -> pl.DataFrame:
        return build_gem_signal_snapshot(
            market_data,
            us_symbol=self.us_symbol,
            ex_us_symbol=self.ex_us_symbol,
            defensive_symbol=self.defensive_symbol,
            t_bill_symbol=self.t_bill_symbol,
            lookback_months=self.lookback_months,
        )

    def build_decisions(
        self,
        signal_snapshot: pl.DataFrame,
        as_of_date: date | None = None,
    ) -> pl.DataFrame:
        required_columns = set(GEM_SIGNAL_SCHEMA)
        missing_columns = required_columns - set(signal_snapshot.columns)
        if missing_columns:
            raise ValueError(f"GEM signal_snapshot 缺少列: {sorted(missing_columns)}")
        if signal_snapshot.is_empty():
            return self.empty_decisions()

        snapshot = signal_snapshot
        if as_of_date is not None:
            snapshot = snapshot.filter(pl.col("time").dt.date() == pl.lit(as_of_date))
        if snapshot.is_empty():
            return self.empty_decisions()

        rows: list[dict[str, object]] = []
        for row in snapshot.sort("time").iter_rows(named=True):
            us_return = float(row["us_return_12m"])
            ex_us_return = float(row["ex_us_return_12m"])
            t_bill_return = float(row["t_bill_return_12m"])
            absolute_pass = us_return > t_bill_return
            if not absolute_pass:
                selected_symbol = self.defensive_symbol
                selection_reason = "us_absolute_momentum_failed"
                tag = "bond"
                score = us_return - t_bill_return
            elif us_return >= ex_us_return:
                selected_symbol = self.us_symbol
                selection_reason = "us_relative_momentum_winner"
                tag = "us_equity"
                score = us_return
            else:
                selected_symbol = self.ex_us_symbol
                selection_reason = "ex_us_relative_momentum_winner"
                tag = "ex_us_equity"
                score = ex_us_return

            metadata = {
                "us_return_12m": us_return,
                "ex_us_return_12m": ex_us_return,
                "t_bill_return_12m": t_bill_return,
                "absolute_momentum_pass": absolute_pass,
                "selected_symbol": selected_symbol,
                "selection_reason": selection_reason,
                "lookback_months": self.lookback_months,
                "us_symbol": self.us_symbol,
                "ex_us_symbol": self.ex_us_symbol,
                "defensive_symbol": self.defensive_symbol,
                "t_bill_symbol": self.t_bill_symbol,
            }
            rows.append(
                {
                    "time": row["time"],
                    "asset_type": row["asset_type"],
                    "strategy": self.strategy_name,
                    "strategy_mode": self.strategy_mode,
                    "symbol": selected_symbol,
                    "decision_type": "target_weight",
                    "signal": 1,
                    "target_weight": 1.0,
                    "score": score,
                    "rank": 1,
                    "tag": tag,
                    "metadata": json.dumps(metadata, ensure_ascii=True, sort_keys=True),
                }
            )
        return pl.DataFrame(rows, schema=STRATEGY_DECISION_SCHEMA).sort("time")

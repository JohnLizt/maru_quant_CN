"""Base interfaces for market data providers."""
from __future__ import annotations

from typing import Protocol

import polars as pl


class MarketDataLoader(Protocol):
    source_name: str

    def supports(self, asset_type: str) -> bool:
        ...

    def get_trading_dates(self, asset_type: str, start: str, end: str) -> list[str]:
        ...

    def fetch_daily_by_date(
        self,
        asset_type: str,
        trade_date: str,
        symbols: list[str] | None = None,
    ) -> pl.DataFrame:
        ...

    def fetch_daily_by_symbol(
        self,
        asset_type: str,
        symbol: str,
        start: str,
        end: str,
    ) -> pl.DataFrame:
        ...

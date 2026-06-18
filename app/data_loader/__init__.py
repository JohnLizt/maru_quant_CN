"""Data loader interfaces and provider registry."""

from app.data_loader.market_data import fetch_daily_by_symbol, get_market_data_loader, upsert_daily
from app.data_loader.symbol_backfill import backfill_symbol_factors, sync_universe_symbol

__all__ = [
    "fetch_daily_by_symbol",
    "get_market_data_loader",
    "upsert_daily",
    "backfill_symbol_factors",
    "sync_universe_symbol",
]

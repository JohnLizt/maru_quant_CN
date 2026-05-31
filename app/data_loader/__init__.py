"""Data loader interfaces and provider registry."""

from app.data_loader.market_data import fetch_daily_by_symbol, get_market_data_loader, upsert_daily

__all__ = ["fetch_daily_by_symbol", "get_market_data_loader", "upsert_daily"]

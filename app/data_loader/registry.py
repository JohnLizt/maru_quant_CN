"""Registry for market data loaders."""
from __future__ import annotations

from app.data_loader.providers.tushare import TushareLoader


def get_market_data_loader(loader_key: str):
    normalized = loader_key.strip().lower()
    if normalized == "tushare":
        return TushareLoader()
    raise ValueError(f"未知 loader_key: {loader_key}")

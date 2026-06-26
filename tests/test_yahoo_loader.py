from __future__ import annotations

import pandas as pd
import pytest

from app.data_loader.registry import get_market_data_loader
from app.data_loader.providers.yahoo import YahooLoader


def _sample_history() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Open": [100.0, 102.0],
            "High": [101.0, 103.0],
            "Low": [99.0, 101.0],
            "Close": [100.0, 102.0],
            "Adj Close": [100.0, 101.0],
            "Volume": [1000, 1200],
        },
        index=pd.to_datetime(["2026-06-23", "2026-06-24"]),
    )


def test_registry_resolves_yahoo_loader() -> None:
    loader = get_market_data_loader("yahoo")
    assert isinstance(loader, YahooLoader)


def test_yahoo_loader_supports_stock_us_and_etf_us() -> None:
    loader = YahooLoader()
    assert loader.supports("stock_US")
    assert loader.supports("etf_US")
    assert loader.get_capabilities("etf_US").supports_by_date
    assert loader.get_capabilities("etf_US").supports_by_symbol


def test_yahoo_loader_fetch_daily_by_date_requires_symbols() -> None:
    loader = YahooLoader()
    with pytest.raises(ValueError, match="必须提供 symbols"):
        loader.fetch_daily_by_date("etf_US", "20260624")


def test_yahoo_loader_fetch_daily_by_date_normalizes_single_day_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    loader = YahooLoader()

    def _fake_download(symbol: str, *, start: str, end: str) -> pd.DataFrame:
        assert start == "2026-06-24"
        assert end == "2026-06-25"
        return _sample_history().iloc[1:]

    monkeypatch.setattr(loader, "_download_history", _fake_download)

    df = loader.fetch_daily_by_date("etf_US", "20260624", ["spy", "qqq"])

    assert df.height == 2
    assert df.get_column("symbol").to_list() == ["QQQ", "SPY"]
    assert df.get_column("asset_type").to_list() == ["etf_US", "etf_US"]
    assert df.get_column("time").dt.strftime("%Y%m%d").to_list() == ["20260624", "20260624"]


def test_yahoo_loader_fetch_daily_by_symbol_normalizes_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    loader = YahooLoader()
    monkeypatch.setattr(loader, "_download_history", lambda symbol, start, end: _sample_history())

    df = loader.fetch_daily_by_symbol("etf_US", "spy", "20260623", "20260624")

    assert df.columns == [
        "time",
        "asset_type",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "pct_change",
        "is_suspended",
        "data_source",
    ]
    assert df.height == 2
    assert df.get_column("asset_type").to_list() == ["etf_US", "etf_US"]
    assert df.get_column("symbol").to_list() == ["SPY", "SPY"]
    assert df.get_column("data_source").to_list() == ["yahoo", "yahoo"]
    assert df.get_column("volume").to_list() == [1000, 1200]
    assert df.get_column("amount").to_list() == [100000.0, 122400.0]
    assert df.get_column("is_suspended").to_list() == [False, False]
    assert df.get_column("pct_change").to_list()[0] is None
    assert df.get_column("pct_change").to_list()[1] == pytest.approx(1.0)


def test_yahoo_loader_get_trading_dates_uses_calendar_symbol(monkeypatch: pytest.MonkeyPatch) -> None:
    loader = YahooLoader()

    def _fake_download(symbol: str, *, start: str, end: str) -> pd.DataFrame:
        assert symbol == "SPY"
        return _sample_history()

    monkeypatch.setattr(loader, "_download_history", _fake_download)

    dates = loader.get_trading_dates("etf_US", "20260623", "20260624")

    assert dates == ["20260623", "20260624"]

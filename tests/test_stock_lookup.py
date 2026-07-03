from __future__ import annotations

import pytest

from app.services.stock_lookup import lookup_stock_candidates, resolve_stock_symbol


def test_lookup_stock_candidates_prefers_etl_universe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "app.services.stock_lookup.load_etl_universe",
        lambda asset_type: [
            {"asset_type": "stock_CN", "symbol": "600519.SH", "name": "贵州茅台", "is_active": "true"},
        ],
    )
    monkeypatch.setattr(
        "app.services.stock_lookup._fetch_tushare_stock_basics",
        lambda: [
            {"symbol": "600519.SH", "name": "贵州茅台", "cnspell": "GZMT", "market": "主板"},
            {"symbol": "000001.SZ", "name": "平安银行", "cnspell": "PAYH", "market": "主板"},
        ],
    )

    candidates = lookup_stock_candidates("贵州茅台")

    assert candidates[0]["symbol"] == "600519.SH"
    assert candidates[0]["in_etl_universe"] is True


def test_resolve_stock_symbol_returns_unique_exact_match(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "app.services.stock_lookup.load_etl_universe",
        lambda asset_type: [],
    )
    monkeypatch.setattr(
        "app.services.stock_lookup._fetch_tushare_stock_basics",
        lambda: [
            {"symbol": "300750.SZ", "name": "宁德时代", "cnspell": "NDSD", "market": "创业板"},
        ],
    )

    result = resolve_stock_symbol("宁德时代")

    assert result["symbol"] == "300750.SZ"


def test_resolve_stock_symbol_raises_on_ambiguous_query(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "app.services.stock_lookup.load_etl_universe",
        lambda asset_type: [],
    )
    monkeypatch.setattr(
        "app.services.stock_lookup._fetch_tushare_stock_basics",
        lambda: [
            {"symbol": "000001.SZ", "name": "平安银行", "cnspell": "PAYH", "market": "主板"},
            {"symbol": "601318.SH", "name": "中国平安", "cnspell": "ZGPA", "market": "主板"},
        ],
    )

    with pytest.raises(ValueError, match="存在歧义"):
        resolve_stock_symbol("平安")

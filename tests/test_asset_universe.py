from __future__ import annotations

from pathlib import Path

import pytest

from app.services.asset_universe import (
    AssetTypeConfig,
    get_asset_type_config,
    get_pipeline_symbol_name_map,
    get_universe_config,
    list_asset_types,
    list_universes,
    load_pipeline_universe,
    load_universe,
    resolve_pipeline_symbols,
    resolve_universe_symbols,
    write_pipeline_universe_rows,
)
from scripts.etl_daily import _choose_etf_fetch_mode, _resolve_asset_types as resolve_etl_asset_types
from scripts.etl_daily import _resolve_etf_fetch_mode
from scripts.factor_daily import _resolve_asset_types as resolve_factor_asset_types


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _config_paths(tmp_path: Path) -> tuple[Path, Path, Path]:
    asset_types = tmp_path / "asset_types.csv"
    universes = tmp_path / "universes.csv"
    universes_dir = tmp_path / "universes"
    return asset_types, universes, universes_dir


def test_list_asset_types_and_universes_enabled_only(tmp_path: Path) -> None:
    asset_types, universes, universes_dir = _config_paths(tmp_path)
    _write(
        asset_types,
        "\n".join(
            [
                "asset_type,display_name,data_source,calendar_key,loader_key,pipeline_universe,enabled",
                "stock_CN,A股股票,tushare,CN,tushare,stock_CN,true",
                "etf_CN,A股ETF,tushare,CN,tushare,etf_mixed,true",
                "stock_US,美股,yahoo,US,yahoo,stock_US,false",
            ]
        ),
    )
    _write(
        universes,
        "\n".join(
            [
                "universe,display_name,enabled",
                "stock_CN,A股股票池,true",
                "etf_mixed,混合ETF池,true",
                "stock_US,美股股票池,false",
            ]
        ),
    )
    _write(universes_dir / "stock_CN.csv", "asset_type,symbol,name,is_active\nstock_CN,603019.SH,中科曙光,true\n")
    _write(universes_dir / "etf_mixed.csv", "asset_type,symbol,name,is_active\netf_CN,510300.SH,沪深300ETF,true\n")
    _write(universes_dir / "stock_US.csv", "asset_type,symbol,name,is_active\nstock_US,AAPL,Apple,true\n")

    asset_type_configs = list_asset_types(path=asset_types)
    universe_configs = list_universes(path=universes)

    assert [item.asset_type for item in asset_type_configs] == ["etf_CN", "stock_CN"]
    assert [item.universe for item in universe_configs] == ["etf_mixed", "stock_CN"]
    assert get_asset_type_config("stock_US", path=asset_types).loader_key == "yahoo"
    assert get_universe_config("etf_mixed", path=universes).display_name == "混合ETF池"
    assert resolve_pipeline_symbols(
        "etf_CN",
        path=asset_types,
        universes_path=universes,
        universes_dir=universes_dir,
    ) == ["510300.SH"]


def test_load_universe_validates_missing_duplicate_and_empty(tmp_path: Path) -> None:
    asset_types, universes, universes_dir = _config_paths(tmp_path)
    _write(
        asset_types,
        "\n".join(
            [
                "asset_type,display_name,data_source,calendar_key,loader_key,pipeline_universe,enabled",
                "stock_CN,A股股票,tushare,CN,tushare,stock_CN,true",
                "etf_CN,A股ETF,tushare,CN,tushare,etf_mixed,true",
            ]
        ),
    )
    _write(
        universes,
        "\n".join(
            [
                "universe,display_name,enabled",
                "stock_CN,A股股票池,true",
                "etf_mixed,混合ETF池,true",
            ]
        ),
    )
    _write(
        universes_dir / "etf_mixed.csv",
        "\n".join(
            [
                "asset_type,symbol,name,is_active",
                "etf_CN,510300.SH,沪深300ETF,true",
                "etf_CN,510300.SH,沪深300ETF,true",
            ]
        ),
    )
    _write(universes_dir / "stock_CN.csv", "asset_type,symbol,name,is_active\nstock_CN,603019.SH,中科曙光,false\n")

    with pytest.raises(ValueError, match="重复成员"):
        load_universe("etf_mixed", universes_path=universes, universes_dir=universes_dir)

    with pytest.raises(ValueError, match="未知 universe"):
        load_universe("missing", universes_path=universes, universes_dir=universes_dir)

    with pytest.raises(ValueError, match="无 active 标的"):
        load_universe("stock_CN", universes_path=universes, universes_dir=universes_dir)


def test_load_universe_supports_mixed_asset_types(tmp_path: Path) -> None:
    asset_types, universes, universes_dir = _config_paths(tmp_path)
    _write(
        asset_types,
        "\n".join(
            [
                "asset_type,display_name,data_source,calendar_key,loader_key,pipeline_universe,enabled",
                "stock_CN,A股股票,tushare,CN,tushare,stock_CN,true",
                "etf_CN,A股ETF,tushare,CN,tushare,etf_mixed,true",
            ]
        ),
    )
    _write(universes, "universe,display_name,enabled\netf_mixed,混合ETF池,true\n")
    _write(
        universes_dir / "etf_mixed.csv",
        "\n".join(
            [
                "asset_type,symbol,name,is_active,tag",
                "etf_CN,510300.SH,沪深300ETF,true,broad_market",
                "stock_CN,600519.SH,贵州茅台,true,defensive",
            ]
        ),
    )

    rows = load_universe("etf_mixed", universes_path=universes, universes_dir=universes_dir)

    assert rows == [
        {
            "asset_type": "etf_CN",
            "symbol": "510300.SH",
            "name": "沪深300ETF",
            "is_active": "true",
            "tag": "broad_market",
        },
        {
            "asset_type": "stock_CN",
            "symbol": "600519.SH",
            "name": "贵州茅台",
            "is_active": "true",
            "tag": "defensive",
        },
    ]
    assert resolve_universe_symbols(
        "etf_mixed",
        universes_path=universes,
        universes_dir=universes_dir,
        asset_type="etf_CN",
    ) == ["510300.SH"]


def test_pipeline_universe_filters_generic_universe_by_asset_type(tmp_path: Path) -> None:
    asset_types, universes, universes_dir = _config_paths(tmp_path)
    _write(
        asset_types,
        "\n".join(
            [
                "asset_type,display_name,data_source,calendar_key,loader_key,pipeline_universe,enabled",
                "stock_CN,A股股票,tushare,CN,tushare,stock_CN,true",
                "etf_CN,A股ETF,tushare,CN,tushare,etf_mixed,true",
            ]
        ),
    )
    _write(
        universes,
        "\n".join(
            [
                "universe,display_name,enabled",
                "stock_CN,A股股票池,true",
                "etf_mixed,混合ETF池,true",
            ]
        ),
    )
    _write(
        universes_dir / "etf_mixed.csv",
        "\n".join(
            [
                "asset_type,symbol,name,is_active,tag",
                "etf_CN,518880.SH,黄金ETF华安,true,gold",
                "stock_CN,600519.SH,贵州茅台,true,defensive",
            ]
        ),
    )

    assert load_pipeline_universe(
        "etf_CN",
        path=asset_types,
        universes_path=universes,
        universes_dir=universes_dir,
    ) == [
        {
            "asset_type": "etf_CN",
            "symbol": "518880.SH",
            "name": "黄金ETF华安",
            "is_active": "true",
            "tag": "gold",
        }
    ]
    assert get_pipeline_symbol_name_map(
        "etf_CN",
        path=asset_types,
        universes_path=universes,
        universes_dir=universes_dir,
    ) == {"518880.SH": "黄金ETF华安"}


def test_write_pipeline_universe_rows_preserves_asset_type_column(tmp_path: Path) -> None:
    asset_types, universes, universes_dir = _config_paths(tmp_path)
    _write(
        asset_types,
        "asset_type,display_name,data_source,calendar_key,loader_key,pipeline_universe,enabled\netf_CN,A股ETF,tushare,CN,tushare,etf_mixed,true\n",
    )
    _write(universes, "universe,display_name,enabled\netf_mixed,混合ETF池,true\n")
    _write(
        universes_dir / "etf_mixed.csv",
        "asset_type,symbol,name,is_active,tag\netf_CN,518880.SH,黄金ETF华安,true,gold\n",
    )

    updated_rows = write_pipeline_universe_rows(
        "etf_CN",
        [
            {"symbol": "518880.SH", "name": "黄金ETF华安", "is_active": "true", "tag": "gold"},
            {"symbol": "512000.SH", "name": "券商ETF华宝", "is_active": "true", "tag": "broker"},
        ],
        path=asset_types,
        universes_path=universes,
        universes_dir=universes_dir,
    )

    broker_row = next(row for row in updated_rows if row["symbol"] == "512000.SH")
    assert broker_row["asset_type"] == "etf_CN"
    assert broker_row["tag"] == "broker"
    assert (universes_dir / "etf_mixed.csv").read_text(encoding="utf-8").splitlines()[0] == "asset_type,symbol,name,is_active,tag"


def test_default_asset_type_resolution_uses_enabled_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    configs = [
        AssetTypeConfig("stock_CN", "A股股票", "tushare", "CN", "tushare", "stock_CN", True),
        AssetTypeConfig("etf_CN", "A股ETF", "tushare", "CN", "tushare", "etf_mixed", True),
    ]
    monkeypatch.setattr("scripts.etl_daily.list_asset_types", lambda enabled_only=True: configs)
    monkeypatch.setattr("scripts.factor_daily.list_asset_types", lambda enabled_only=True: configs)

    assert resolve_etl_asset_types(None) == ["stock_CN", "etf_CN"]
    assert resolve_factor_asset_types(None) == ["stock_CN", "etf_CN"]
    assert resolve_etl_asset_types(["stock_CN"]) == ["stock_CN"]
    assert resolve_factor_asset_types(["stock_CN"]) == ["stock_CN"]


def test_etf_fetch_mode_resolution_and_auto_selection() -> None:
    assert _resolve_etf_fetch_mode("auto") == "auto"
    assert _resolve_etf_fetch_mode("by_date") == "by_date"
    assert _resolve_etf_fetch_mode("by_symbol") == "by_symbol"

    with pytest.raises(ValueError, match="非法取值"):
        _resolve_etf_fetch_mode("bad_mode")

    assert _choose_etf_fetch_mode("auto", force_update=True, existing_dates=set(), missing_dates=["20260529"]) == "by_symbol"
    assert _choose_etf_fetch_mode("auto", force_update=False, existing_dates=set(), missing_dates=["20260529"]) == "by_symbol"
    assert _choose_etf_fetch_mode("auto", force_update=False, existing_dates={"20260528"}, missing_dates=["20260529"]) == "by_date"
    assert _choose_etf_fetch_mode(
        "auto",
        force_update=False,
        existing_dates={"20260501"},
        missing_dates=["20260526", "20260527", "20260528", "20260529"],
    ) == "by_symbol"

from __future__ import annotations

from pathlib import Path

import pytest

from app.services.asset_universe import (
    AssetTypeConfig,
    get_asset_type_config,
    get_pipeline_symbol_name_map,
    list_asset_types,
    load_pipeline_universe,
    resolve_pipeline_symbols,
)
from scripts.etl_daily import _resolve_asset_types as resolve_etl_asset_types
from scripts.factor_daily import _resolve_asset_types as resolve_factor_asset_types


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _config_paths(tmp_path: Path) -> tuple[Path, Path]:
    asset_types = tmp_path / "asset_types.csv"
    universes_dir = tmp_path / "universes"
    return asset_types, universes_dir


def test_list_asset_types_enabled_only(tmp_path: Path) -> None:
    asset_types, universes_dir = _config_paths(tmp_path)
    _write(
        asset_types,
        "\n".join(
            [
                "asset_type,display_name,data_source,calendar_key,loader_key,pipeline_universe,enabled",
                "stock_CN,A股股票,tushare,CN,tushare,stock_CN,true",
                "etf_CN,A股ETF,tushare,CN,tushare,etf_CN,true",
                "stock_US,美股,yahoo,US,yahoo,stock_US,false",
            ]
        ),
    )
    _write(universes_dir / "stock_CN.csv", "symbol,name,is_active\n603019.SH,中科曙光,true\n")
    _write(universes_dir / "etf_CN.csv", "symbol,name,is_active\n510300.SH,沪深300ETF,true\n")
    _write(universes_dir / "stock_US.csv", "symbol,name,is_active\nAAPL,Apple,true\n")

    configs = list_asset_types(path=asset_types)

    assert [item.asset_type for item in configs] == ["etf_CN", "stock_CN"]
    assert get_asset_type_config("stock_US", path=asset_types).loader_key == "yahoo"
    assert resolve_pipeline_symbols("stock_CN", path=asset_types, universes_dir=universes_dir) == ["603019.SH"]


def test_load_pipeline_universe_validates_missing_duplicate_and_empty(tmp_path: Path) -> None:
    asset_types, universes_dir = _config_paths(tmp_path)
    _write(
        asset_types,
        "\n".join(
            [
                "asset_type,display_name,data_source,calendar_key,loader_key,pipeline_universe,enabled",
                "stock_CN,A股股票,tushare,CN,tushare,stock_CN,true",
                "etf_CN,A股ETF,tushare,CN,tushare,etf_CN,true",
                "stock_US,美股,yahoo,US,yahoo,stock_US,true",
            ]
        ),
    )
    _write(
        universes_dir / "stock_CN.csv",
        "\n".join(
            [
                "symbol,name,is_active",
                "603019.SH,中科曙光,true",
                "603019.SH,中科曙光,true",
            ]
        ),
    )
    _write(universes_dir / "stock_US.csv", "symbol,name,is_active\nAAPL,Apple,false\n")

    with pytest.raises(ValueError, match="重复 symbol"):
        load_pipeline_universe("stock_CN", path=asset_types, universes_dir=universes_dir)

    with pytest.raises(FileNotFoundError):
        load_pipeline_universe("etf_CN", path=asset_types, universes_dir=universes_dir)

    with pytest.raises(ValueError, match="无 active 标的"):
        load_pipeline_universe("stock_US", path=asset_types, universes_dir=universes_dir)


def test_pipeline_symbol_name_map_comes_from_asset_universe(tmp_path: Path) -> None:
    asset_types, universes_dir = _config_paths(tmp_path)
    _write(
        asset_types,
        "\n".join(
            [
                "asset_type,display_name,data_source,calendar_key,loader_key,pipeline_universe,enabled",
                "stock_CN,A股股票,tushare,CN,tushare,stock_CN,true",
            ]
        ),
    )
    _write(
        universes_dir / "stock_CN.csv",
        "\n".join(
            [
                "symbol,name,is_active",
                "603019.SH,中科曙光,true",
                "300059.SZ,东方财富,true",
            ]
        ),
    )

    assert get_pipeline_symbol_name_map("stock_CN", path=asset_types, universes_dir=universes_dir) == {
        "603019.SH": "中科曙光",
        "300059.SZ": "东方财富",
    }


def test_default_asset_type_resolution_uses_enabled_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    configs = [
        AssetTypeConfig("stock_CN", "A股股票", "tushare", "CN", "tushare", "stock_CN", True),
        AssetTypeConfig("etf_CN", "A股ETF", "tushare", "CN", "tushare", "etf_CN", True),
    ]
    monkeypatch.setattr("scripts.etl_daily.list_asset_types", lambda enabled_only=True: configs)
    monkeypatch.setattr("scripts.factor_daily.list_asset_types", lambda enabled_only=True: configs)

    assert resolve_etl_asset_types(None) == ["stock_CN", "etf_CN"]
    assert resolve_factor_asset_types(None) == ["stock_CN", "etf_CN"]
    assert resolve_etl_asset_types(["stock_CN"]) == ["stock_CN"]
    assert resolve_factor_asset_types(["stock_CN"]) == ["stock_CN"]

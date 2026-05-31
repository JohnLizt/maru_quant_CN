"""Asset-type registry and pipeline universe configuration helpers."""
from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = Path("/app/config")
if not CONFIG_DIR.exists():
    CONFIG_DIR = _REPO_ROOT / "config"
ASSET_TYPES_CSV = CONFIG_DIR / "asset_types.csv"
UNIVERSES_DIR = CONFIG_DIR / "universes"


@dataclass(frozen=True)
class AssetTypeConfig:
    asset_type: str
    display_name: str
    data_source: str
    calendar_key: str
    loader_key: str
    pipeline_universe: str
    enabled: bool


def normalize_asset_type(asset_type: str) -> str:
    normalized = asset_type.strip()
    if not normalized:
        raise ValueError("asset_type 不能为空")
    return normalized


def normalize_symbol(symbol: str) -> str:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        raise ValueError("symbol 不能为空")
    return normalized


def _normalize_name(name: str | None) -> str:
    return str(name or "").strip()


def _parse_bool(raw: str | bool | None, *, field_name: str) -> bool:
    if isinstance(raw, bool):
        return raw
    normalized = str(raw or "").strip().lower()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    raise ValueError(f"{field_name} 非法布尔值: {raw!r}")


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"配置文件不存在: {path}")

    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _asset_type_from_row(row: dict[str, str]) -> AssetTypeConfig:
    asset_type = normalize_asset_type(str(row.get("asset_type", "")))
    display_name = _normalize_name(row.get("display_name"))
    data_source = str(row.get("data_source", "")).strip()
    calendar_key = str(row.get("calendar_key", "")).strip()
    loader_key = str(row.get("loader_key", "")).strip()
    pipeline_universe = str(row.get("pipeline_universe", "")).strip()
    enabled = _parse_bool(row.get("enabled"), field_name="enabled")

    if not display_name:
        raise ValueError(f"asset_types.csv 配置缺少 display_name: {asset_type}")
    if not data_source:
        raise ValueError(f"asset_types.csv 配置缺少 data_source: {asset_type}")
    if not calendar_key:
        raise ValueError(f"asset_types.csv 配置缺少 calendar_key: {asset_type}")
    if not loader_key:
        raise ValueError(f"asset_types.csv 配置缺少 loader_key: {asset_type}")
    if not pipeline_universe:
        raise ValueError(f"asset_types.csv 配置缺少 pipeline_universe: {asset_type}")

    return AssetTypeConfig(
        asset_type=asset_type,
        display_name=display_name,
        data_source=data_source,
        calendar_key=calendar_key,
        loader_key=loader_key,
        pipeline_universe=pipeline_universe,
        enabled=enabled,
    )


def list_asset_types(*, enabled_only: bool = True, path: Path = ASSET_TYPES_CSV) -> list[AssetTypeConfig]:
    rows = _read_csv_rows(path)
    configs: list[AssetTypeConfig] = []
    seen: set[str] = set()

    for row in rows:
        config = _asset_type_from_row(row)
        if config.asset_type in seen:
            raise ValueError(f"asset_types.csv 存在重复 asset_type: {config.asset_type}")
        seen.add(config.asset_type)
        if enabled_only and not config.enabled:
            continue
        configs.append(config)

    if enabled_only and not configs:
        raise ValueError("asset_types.csv 中没有 enabled=true 的 asset_type")

    return sorted(configs, key=lambda item: item.asset_type)


def get_asset_type_config(asset_type: str, *, path: Path = ASSET_TYPES_CSV) -> AssetTypeConfig:
    normalized = normalize_asset_type(asset_type)
    for config in list_asset_types(enabled_only=False, path=path):
        if config.asset_type == normalized:
            return config
    raise ValueError(f"未知 asset_type: {normalized}")


def _resolve_universe_path(config: AssetTypeConfig, universes_dir: Path = UNIVERSES_DIR) -> Path:
    return universes_dir / f"{config.pipeline_universe}.csv"


def _read_universe_rows(config: AssetTypeConfig, *, universes_dir: Path = UNIVERSES_DIR) -> list[dict[str, str]]:
    path = _resolve_universe_path(config, universes_dir)
    rows = _read_csv_rows(path)

    normalized_rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        symbol_raw = str(row.get("symbol", "")).strip()
        if not symbol_raw:
            raise ValueError(f"universe 文件存在空 symbol: {path}")

        if "asset_type" in row and str(row.get("asset_type", "")).strip():
            raise ValueError(f"universe 文件不应包含 asset_type 列: {path}")

        symbol = normalize_symbol(symbol_raw)
        if symbol in seen:
            raise ValueError(f"universe 文件存在重复 symbol: {symbol} | {path}")
        seen.add(symbol)

        is_active = _parse_bool(row.get("is_active", "true"), field_name="is_active")
        normalized_rows.append({
            "symbol": symbol,
            "name": _normalize_name(row.get("name")),
            "is_active": "true" if is_active else "false",
        })

    if not normalized_rows:
        raise ValueError(f"universe 文件为空: {path}")

    return normalized_rows


def load_pipeline_universe(asset_type: str, *, universes_dir: Path = UNIVERSES_DIR, path: Path = ASSET_TYPES_CSV) -> list[dict[str, str]]:
    config = get_asset_type_config(asset_type, path=path)
    rows = _read_universe_rows(config, universes_dir=universes_dir)
    active_rows = [row for row in rows if row["is_active"] == "true"]
    if not active_rows:
        raise ValueError(f"asset_type={config.asset_type} 的 pipeline universe 无 active 标的")
    return active_rows


def resolve_pipeline_symbols(asset_type: str, *, universes_dir: Path = UNIVERSES_DIR, path: Path = ASSET_TYPES_CSV) -> list[str]:
    return [row["symbol"] for row in load_pipeline_universe(asset_type, universes_dir=universes_dir, path=path)]


def get_pipeline_symbol_name_map(asset_type: str, *, universes_dir: Path = UNIVERSES_DIR, path: Path = ASSET_TYPES_CSV) -> dict[str, str]:
    return {row["symbol"]: row["name"] for row in load_pipeline_universe(asset_type, universes_dir=universes_dir, path=path)}


def write_pipeline_universe_rows(
    asset_type: str,
    rows: list[dict[str, str | bool]],
    *,
    universes_dir: Path = UNIVERSES_DIR,
    path: Path = ASSET_TYPES_CSV,
) -> list[dict[str, str]]:
    config = get_asset_type_config(asset_type, path=path)
    target = _resolve_universe_path(config, universes_dir)
    target.parent.mkdir(parents=True, exist_ok=True)

    normalized_rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        symbol = normalize_symbol(str(row.get("symbol", "")))
        if symbol in seen:
            raise ValueError(f"写入 universe 时发现重复 symbol: {symbol}")
        seen.add(symbol)
        normalized_rows.append({
            "symbol": symbol,
            "name": _normalize_name(str(row.get("name", ""))),
            "is_active": "true" if _parse_bool(row.get("is_active", "true"), field_name="is_active") else "false",
        })

    if not normalized_rows:
        raise ValueError(f"asset_type={asset_type} 的 pipeline universe 不能为空")

    normalized_rows.sort(key=lambda row: row["symbol"])
    with target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["symbol", "name", "is_active"])
        writer.writeheader()
        writer.writerows(normalized_rows)

    return load_pipeline_universe(config.asset_type, universes_dir=universes_dir, path=path)

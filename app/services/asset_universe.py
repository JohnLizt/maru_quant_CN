"""Asset-type and universe registry helpers."""
from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = Path("/app/config")
if not CONFIG_DIR.exists():
    CONFIG_DIR = _REPO_ROOT / "config"
ASSET_TYPES_CSV = CONFIG_DIR / "asset_types.csv"
UNIVERSES_CSV = CONFIG_DIR / "universes.csv"
STRATEGY_UNIVERSES_DIR = CONFIG_DIR / "strategy_universes"
ETL_UNIVERSES_DIR = CONFIG_DIR / "etl_universes"
UNIVERSES_DIR = STRATEGY_UNIVERSES_DIR


@dataclass(frozen=True)
class AssetTypeConfig:
    asset_type: str
    display_name: str
    data_source: str
    calendar_key: str
    loader_key: str
    etl_universe: str
    etl_fetch_mode: str
    strict_date_coverage: bool
    fill_missing_as_suspended: bool
    enabled: bool


@dataclass(frozen=True)
class UniverseConfig:
    universe: str
    display_name: str
    enabled: bool


def normalize_asset_type(asset_type: str) -> str:
    normalized = asset_type.strip()
    if not normalized:
        raise ValueError("asset_type 不能为空")
    return normalized


def normalize_universe(universe: str) -> str:
    normalized = universe.strip()
    if not normalized:
        raise ValueError("universe 不能为空")
    return normalized


def normalize_symbol(symbol: str) -> str:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        raise ValueError("symbol 不能为空")
    return normalized


def _infer_cn_exchange(symbol: str) -> str:
    if len(symbol) != 6 or not symbol.isdigit():
        return symbol
    if symbol[0] in {"6", "5", "9"}:
        return f"{symbol}.SH"
    if symbol[0] in {"4", "8"}:
        return f"{symbol}.BJ"
    return f"{symbol}.SZ"


def normalize_symbol_for_asset_type(asset_type: str, symbol: str) -> str:
    normalized_asset_type = normalize_asset_type(asset_type)
    normalized_symbol = normalize_symbol(symbol)
    if "." in normalized_symbol:
        return normalized_symbol
    if normalized_asset_type in {"stock_CN", "etf_CN"}:
        return _infer_cn_exchange(normalized_symbol)
    return normalized_symbol


def _normalize_name(name: str | None) -> str:
    return str(name or "").strip()


def _normalize_tag(tag: str | None) -> str:
    return str(tag or "").strip()


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
    etl_universe = normalize_universe(str(row.get("etl_universe", "")))
    etl_fetch_mode = str(row.get("etl_fetch_mode", "")).strip().lower()
    strict_date_coverage = _parse_bool(row.get("strict_date_coverage"), field_name="strict_date_coverage")
    fill_missing_as_suspended = _parse_bool(row.get("fill_missing_as_suspended"), field_name="fill_missing_as_suspended")
    enabled = _parse_bool(row.get("enabled"), field_name="enabled")
    if not display_name:
        raise ValueError(f"asset_types.csv 配置缺少 display_name: {asset_type}")
    if not data_source:
        raise ValueError(f"asset_types.csv 配置缺少 data_source: {asset_type}")
    if not calendar_key:
        raise ValueError(f"asset_types.csv 配置缺少 calendar_key: {asset_type}")
    if not loader_key:
        raise ValueError(f"asset_types.csv 配置缺少 loader_key: {asset_type}")
    return AssetTypeConfig(
        asset_type=asset_type,
        display_name=display_name,
        data_source=data_source,
        calendar_key=calendar_key,
        loader_key=loader_key,
        etl_universe=etl_universe,
        etl_fetch_mode=etl_fetch_mode,
        strict_date_coverage=strict_date_coverage,
        fill_missing_as_suspended=fill_missing_as_suspended,
        enabled=enabled,
    )


def _universe_from_row(row: dict[str, str]) -> UniverseConfig:
    universe = normalize_universe(str(row.get("universe", "")))
    display_name = _normalize_name(row.get("display_name"))
    enabled = _parse_bool(row.get("enabled"), field_name="enabled")
    if not display_name:
        raise ValueError(f"universes.csv 配置缺少 display_name: {universe}")
    return UniverseConfig(universe=universe, display_name=display_name, enabled=enabled)


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


def list_universes(*, enabled_only: bool = True, path: Path = UNIVERSES_CSV) -> list[UniverseConfig]:
    rows = _read_csv_rows(path)
    configs: list[UniverseConfig] = []
    seen: set[str] = set()
    for row in rows:
        config = _universe_from_row(row)
        if config.universe in seen:
            raise ValueError(f"universes.csv 存在重复 universe: {config.universe}")
        seen.add(config.universe)
        if enabled_only and not config.enabled:
            continue
        configs.append(config)
    if enabled_only and not configs:
        raise ValueError("universes.csv 中没有 enabled=true 的 universe")
    return sorted(configs, key=lambda item: item.universe)


def get_universe_config(universe: str, *, path: Path = UNIVERSES_CSV) -> UniverseConfig:
    normalized = normalize_universe(universe)
    for config in list_universes(enabled_only=False, path=path):
        if config.universe == normalized:
            return config
    raise ValueError(f"未知 universe: {normalized}")


def _resolve_universe_path(universe: str, universes_dir: Path = UNIVERSES_DIR) -> Path:
    return universes_dir / f"{normalize_universe(universe)}.csv"


def _resolve_etl_universe_path(universe: str, universes_dir: Path = ETL_UNIVERSES_DIR) -> Path:
    return universes_dir / f"{normalize_universe(universe)}.csv"


def _infer_row_asset_type(default_asset_type: str | None, raw_asset_type: str | None) -> str:
    explicit = str(raw_asset_type or "").strip()
    if explicit:
        return normalize_asset_type(explicit)
    if default_asset_type is None:
        raise ValueError("universe 成员缺少 asset_type 列")
    return normalize_asset_type(default_asset_type)


def _read_universe_rows(
    universe: str,
    *,
    universes_path: Path = UNIVERSES_CSV,
    universes_dir: Path = UNIVERSES_DIR,
    default_asset_type: str | None = None,
) -> list[dict[str, str]]:
    get_universe_config(universe, path=universes_path)
    path = _resolve_universe_path(universe, universes_dir)
    rows = _read_csv_rows(path)

    normalized_rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        symbol_raw = str(row.get("symbol", "")).strip()
        if not symbol_raw:
            raise ValueError(f"universe 文件存在空 symbol: {path}")
        asset_type = _infer_row_asset_type(default_asset_type, row.get("asset_type"))
        symbol = normalize_symbol_for_asset_type(asset_type, symbol_raw)
        key = (asset_type, symbol)
        if key in seen:
            raise ValueError(f"universe 文件存在重复成员: {asset_type}:{symbol} | {path}")
        seen.add(key)

        is_active = _parse_bool(row.get("is_active", "true"), field_name="is_active")
        normalized_row = {
            "asset_type": asset_type,
            "symbol": symbol,
            "name": _normalize_name(row.get("name")),
            "is_active": "true" if is_active else "false",
        }
        for key_name, value in row.items():
            if key_name in {"asset_type", "symbol", "name", "is_active"}:
                continue
            normalized_row[str(key_name).strip()] = _normalize_tag(value)
        normalized_rows.append(normalized_row)

    if not normalized_rows:
        raise ValueError(f"universe 文件为空: {path}")
    return normalized_rows


def _read_etl_universe_rows(
    universe: str,
    *,
    universes_dir: Path = ETL_UNIVERSES_DIR,
    default_asset_type: str | None = None,
) -> list[dict[str, str]]:
    path = _resolve_etl_universe_path(universe, universes_dir)
    rows = _read_csv_rows(path)

    normalized_rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        symbol_raw = str(row.get("symbol", "")).strip()
        if not symbol_raw:
            raise ValueError(f"etl universe 文件存在空 symbol: {path}")
        asset_type = _infer_row_asset_type(default_asset_type, row.get("asset_type"))
        symbol = normalize_symbol_for_asset_type(asset_type, symbol_raw)
        key = (asset_type, symbol)
        if key in seen:
            raise ValueError(f"etl universe 文件存在重复成员: {asset_type}:{symbol} | {path}")
        seen.add(key)

        is_active = _parse_bool(row.get("is_active", "true"), field_name="is_active")
        normalized_row = {
            "asset_type": asset_type,
            "symbol": symbol,
            "name": _normalize_name(row.get("name")),
            "is_active": "true" if is_active else "false",
        }
        for key_name, value in row.items():
            if key_name in {"asset_type", "symbol", "name", "is_active"}:
                continue
            normalized_row[str(key_name).strip()] = _normalize_tag(value)
        normalized_rows.append(normalized_row)

    if not normalized_rows:
        raise ValueError(f"etl universe 文件为空: {path}")
    return normalized_rows


def load_universe(
    universe: str,
    *,
    universes_path: Path = UNIVERSES_CSV,
    universes_dir: Path = UNIVERSES_DIR,
    default_asset_type: str | None = None,
) -> list[dict[str, str]]:
    rows = _read_universe_rows(
        universe,
        universes_path=universes_path,
        universes_dir=universes_dir,
        default_asset_type=default_asset_type,
    )
    active_rows = [row for row in rows if row["is_active"] == "true"]
    if not active_rows:
        raise ValueError(f"universe={universe} 无 active 标的")
    return active_rows


def resolve_universe_rows(
    universe: str,
    *,
    universes_path: Path = UNIVERSES_CSV,
    universes_dir: Path = UNIVERSES_DIR,
    default_asset_type: str | None = None,
) -> list[dict[str, str]]:
    return load_universe(
        universe,
        universes_path=universes_path,
        universes_dir=universes_dir,
        default_asset_type=default_asset_type,
    )


def resolve_universe_symbols(
    universe: str,
    *,
    asset_type: str | None = None,
    universes_path: Path = UNIVERSES_CSV,
    universes_dir: Path = UNIVERSES_DIR,
    default_asset_type: str | None = None,
) -> list[str]:
    rows = load_universe(
        universe,
        universes_path=universes_path,
        universes_dir=universes_dir,
        default_asset_type=default_asset_type,
    )
    if asset_type is not None:
        normalized_asset_type = normalize_asset_type(asset_type)
        rows = [row for row in rows if row["asset_type"] == normalized_asset_type]
    return [row["symbol"] for row in rows]


def get_universe_symbol_name_map(
    universe: str,
    *,
    universes_path: Path = UNIVERSES_CSV,
    universes_dir: Path = UNIVERSES_DIR,
    default_asset_type: str | None = None,
) -> dict[tuple[str, str], str]:
    return {
        (row["asset_type"], row["symbol"]): row["name"]
        for row in load_universe(
            universe,
            universes_path=universes_path,
            universes_dir=universes_dir,
            default_asset_type=default_asset_type,
        )
    }


def get_universe_symbol_tag_map(
    universe: str,
    *,
    universes_path: Path = UNIVERSES_CSV,
    universes_dir: Path = UNIVERSES_DIR,
    default_asset_type: str | None = None,
) -> dict[tuple[str, str], str]:
    return {
        (row["asset_type"], row["symbol"]): str(row.get("tag", "")).strip()
        for row in load_universe(
            universe,
            universes_path=universes_path,
            universes_dir=universes_dir,
            default_asset_type=default_asset_type,
        )
    }


def load_etl_universe(
    asset_type: str,
    *,
    universes_dir: Path = ETL_UNIVERSES_DIR,
    path: Path = ASSET_TYPES_CSV,
) -> list[dict[str, str]]:
    config = get_asset_type_config(asset_type, path=path)
    rows = _read_etl_universe_rows(
        config.etl_universe,
        universes_dir=universes_dir,
        default_asset_type=config.asset_type,
    )
    normalized_asset_type = normalize_asset_type(asset_type)
    filtered = [row for row in rows if row["asset_type"] == normalized_asset_type]
    if not filtered:
        raise ValueError(f"asset_type={config.asset_type} 的 etl universe 无 active 标的")
    return filtered


def resolve_etl_symbols(
    asset_type: str,
    *,
    universes_dir: Path = ETL_UNIVERSES_DIR,
    path: Path = ASSET_TYPES_CSV,
) -> list[str]:
    return [
        row["symbol"]
        for row in load_etl_universe(
            asset_type,
            universes_dir=universes_dir,
            path=path,
        )
    ]


def get_etl_symbol_name_map(
    asset_type: str,
    *,
    universes_dir: Path = ETL_UNIVERSES_DIR,
    path: Path = ASSET_TYPES_CSV,
) -> dict[str, str]:
    return {
        row["symbol"]: row["name"]
        for row in load_etl_universe(
            asset_type,
            universes_dir=universes_dir,
            path=path,
        )
    }


def get_etl_symbol_tag_map(
    asset_type: str,
    *,
    universes_dir: Path = ETL_UNIVERSES_DIR,
    path: Path = ASSET_TYPES_CSV,
) -> dict[str, str]:
    return {
        row["symbol"]: str(row.get("tag", "")).strip()
        for row in load_etl_universe(
            asset_type,
            universes_dir=universes_dir,
            path=path,
        )
    }


def write_universe_rows(
    universe: str,
    rows: list[dict[str, str | bool]],
    *,
    universes_path: Path = UNIVERSES_CSV,
    universes_dir: Path = UNIVERSES_DIR,
    default_asset_type: str | None = None,
) -> list[dict[str, str]]:
    config = get_universe_config(universe, path=universes_path)
    target = _resolve_universe_path(config.universe, universes_dir)
    target.parent.mkdir(parents=True, exist_ok=True)

    normalized_rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    extra_fieldnames: set[str] = set()
    for row in rows:
        asset_type = _infer_row_asset_type(default_asset_type, row.get("asset_type"))
        symbol = normalize_symbol_for_asset_type(asset_type, str(row.get("symbol", "")))
        key = (asset_type, symbol)
        if key in seen:
            raise ValueError(f"写入 universe 时发现重复成员: {asset_type}:{symbol}")
        seen.add(key)
        normalized_row = {
            "asset_type": asset_type,
            "symbol": symbol,
            "name": _normalize_name(str(row.get("name", ""))),
            "is_active": "true" if _parse_bool(row.get("is_active", "true"), field_name="is_active") else "false",
        }
        for key_name, value in row.items():
            normalized_key = str(key_name).strip()
            if normalized_key in {"asset_type", "symbol", "name", "is_active"} or not normalized_key:
                continue
            normalized_row[normalized_key] = _normalize_tag(str(value))
            extra_fieldnames.add(normalized_key)
        normalized_rows.append(normalized_row)

    if not normalized_rows:
        raise ValueError(f"universe={universe} 不能为空")

    normalized_rows.sort(key=lambda row: (row["asset_type"], row["symbol"]))
    with target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["asset_type", "symbol", "name", "is_active", *sorted(extra_fieldnames)],
        )
        writer.writeheader()
        writer.writerows(normalized_rows)

    return load_universe(
        config.universe,
        universes_path=universes_path,
        universes_dir=universes_dir,
        default_asset_type=default_asset_type,
    )


def write_etl_universe_rows(
    asset_type: str,
    rows: list[dict[str, str | bool]],
    *,
    universes_dir: Path = ETL_UNIVERSES_DIR,
    path: Path = ASSET_TYPES_CSV,
) -> list[dict[str, str]]:
    config = get_asset_type_config(asset_type, path=path)
    normalized_asset_type = normalize_asset_type(asset_type)
    normalized_rows = []
    for row in rows:
        current = dict(row)
        current["asset_type"] = normalize_asset_type(str(current.get("asset_type", normalized_asset_type)) or normalized_asset_type)
        normalized_rows.append(current)

    target = _resolve_etl_universe_path(config.etl_universe, universes_dir)
    target.parent.mkdir(parents=True, exist_ok=True)

    normalized_output_rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    extra_fieldnames: set[str] = set()
    for row in normalized_rows:
        current_asset_type = _infer_row_asset_type(config.asset_type, row.get("asset_type"))
        symbol = normalize_symbol_for_asset_type(current_asset_type, str(row.get("symbol", "")))
        key = (current_asset_type, symbol)
        if key in seen:
            raise ValueError(f"写入 etl universe 时发现重复成员: {current_asset_type}:{symbol}")
        seen.add(key)
        normalized_row = {
            "asset_type": current_asset_type,
            "symbol": symbol,
            "name": _normalize_name(str(row.get("name", ""))),
            "is_active": "true" if _parse_bool(row.get("is_active", "true"), field_name="is_active") else "false",
        }
        for key_name, value in row.items():
            normalized_key = str(key_name).strip()
            if normalized_key in {"asset_type", "symbol", "name", "is_active"} or not normalized_key:
                continue
            normalized_row[normalized_key] = _normalize_tag(str(value))
            extra_fieldnames.add(normalized_key)
        normalized_output_rows.append(normalized_row)

    if not normalized_output_rows:
        raise ValueError(f"asset_type={asset_type} 的 etl universe 不能为空")

    normalized_output_rows.sort(key=lambda current: (current["asset_type"], current["symbol"]))
    with target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["asset_type", "symbol", "name", "is_active", *sorted(extra_fieldnames)],
        )
        writer.writeheader()
        writer.writerows(normalized_output_rows)

    return load_etl_universe(
        asset_type,
        universes_dir=universes_dir,
        path=path,
    )


load_pipeline_universe = load_etl_universe
resolve_pipeline_symbols = resolve_etl_symbols
get_pipeline_symbol_name_map = get_etl_symbol_name_map
get_pipeline_symbol_tag_map = get_etl_symbol_tag_map
write_pipeline_universe_rows = write_etl_universe_rows

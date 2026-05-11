"""Stock pool CSV management helpers."""
from __future__ import annotations

import csv
from itertools import zip_longest
from pathlib import Path

STOCK_POOL_CSV = Path("/app/config/stock_pool.csv")


def normalize_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not normalized:
        raise ValueError("symbol 不能为空")
    return normalized


def _normalize_name(name: str | None) -> str:
    return str(name or "").strip()


def _read_rows(path: Path = STOCK_POOL_CSV) -> list[dict[str, str]]:
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows: list[dict[str, str]] = []
        for row in reader:
            symbol = str(row.get("symbol", "")).strip().upper()
            if not symbol:
                continue
            rows.append({
                "symbol": symbol,
                "name": _normalize_name(row.get("name")),
            })
        return rows


def _write_rows(rows: list[dict[str, str]], path: Path = STOCK_POOL_CSV) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized_rows = sorted(
        [
            {
                "symbol": normalize_symbol(row["symbol"]),
                "name": _normalize_name(row.get("name")),
            }
            for row in rows
        ],
        key=lambda row: row["symbol"],
    )

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["symbol", "name"])
        writer.writeheader()
        writer.writerows(normalized_rows)


def list_stock_pool(path: Path = STOCK_POOL_CSV) -> list[dict[str, str]]:
    return sorted(_read_rows(path), key=lambda row: row["symbol"])


def get_stock_pool_map(path: Path = STOCK_POOL_CSV) -> dict[str, str]:
    return {row["symbol"]: row["name"] for row in _read_rows(path)}


def _resolve_name(symbol: str, *, existing_map: dict[str, str], override_name: str | None = None) -> str:
    explicit_name = _normalize_name(override_name)
    if explicit_name:
        return explicit_name

    existing_name = _normalize_name(existing_map.get(symbol))
    if existing_name:
        return existing_name

    return ""


def add_symbols(
    symbols: list[str],
    *,
    names: list[str | None] | None = None,
    path: Path = STOCK_POOL_CSV,
) -> list[dict[str, str]]:
    existing_map = get_stock_pool_map(path)
    for raw_symbol, raw_name in zip_longest(symbols, names or [], fillvalue=None):
        if raw_symbol is None:
            continue
        symbol = normalize_symbol(raw_symbol)
        existing_map[symbol] = _resolve_name(symbol, existing_map=existing_map, override_name=raw_name)

    rows = [{"symbol": symbol, "name": name} for symbol, name in existing_map.items()]
    _write_rows(rows, path)
    return list_stock_pool(path)


def update_symbol(symbol: str, *, name: str | None = None, path: Path = STOCK_POOL_CSV) -> list[dict[str, str]]:
    normalized_symbol = normalize_symbol(symbol)
    existing_map = get_stock_pool_map(path)
    existing_map[normalized_symbol] = _resolve_name(
        normalized_symbol,
        existing_map=existing_map,
        override_name=name,
    )
    rows = [{"symbol": current_symbol, "name": current_name} for current_symbol, current_name in existing_map.items()]
    _write_rows(rows, path)
    return list_stock_pool(path)


def remove_symbols(symbols: list[str], *, path: Path = STOCK_POOL_CSV) -> list[dict[str, str]]:
    remove_set = {normalize_symbol(symbol) for symbol in symbols}
    rows = [row for row in _read_rows(path) if row["symbol"] not in remove_set]
    _write_rows(rows, path)
    return list_stock_pool(path)


def replace_symbols(symbols: list[str], *, path: Path = STOCK_POOL_CSV) -> list[dict[str, str]]:
    existing_map = get_stock_pool_map(path)
    deduped_symbols: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        symbol = normalize_symbol(raw_symbol)
        if symbol in seen:
            continue
        seen.add(symbol)
        deduped_symbols.append(symbol)

    rows = [
        {
            "symbol": symbol,
            "name": _resolve_name(symbol, existing_map=existing_map),
        }
        for symbol in deduped_symbols
    ]
    _write_rows(rows, path)
    return list_stock_pool(path)

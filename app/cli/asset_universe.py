"""CLI API for managing pipeline universes by asset_type."""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
for candidate in ["/app", str(REPO_ROOT)]:
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from app.services.asset_universe import (
    get_pipeline_symbol_name_map,
    load_pipeline_universe,
    normalize_asset_type,
    normalize_symbol_for_asset_type,
    write_pipeline_universe_rows,
)
from app.data_loader.symbol_backfill import sync_universe_symbol


def _expand_symbols(raw_symbols: list[str]) -> list[str]:
    expanded: list[str] = []
    seen: set[str] = set()

    for raw_symbol in raw_symbols:
        for token in re.split(r"[\s,]+", raw_symbol.strip()):
            symbol = token.strip().upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            expanded.append(symbol)

    return expanded


def _print_rows(rows: list[dict[str, str]]) -> None:
    print(json.dumps({"count": len(rows), "rows": rows}, ensure_ascii=False, indent=2))


def _list_rows(asset_type: str) -> list[dict[str, str]]:
    return sorted(load_pipeline_universe(asset_type), key=lambda row: row["symbol"])


def _get_name_map(asset_type: str) -> dict[str, str]:
    return get_pipeline_symbol_name_map(asset_type)


def _resolve_name(symbol: str, *, existing_map: dict[str, str], override_name: str | None = None) -> str:
    explicit_name = str(override_name or "").strip()
    if explicit_name:
        return explicit_name

    existing_name = str(existing_map.get(symbol, "")).strip()
    if existing_name:
        return existing_name

    return ""


def add_symbols(asset_type: str, symbols: list[str], names: list[str | None] | None = None) -> list[dict[str, str]]:
    existing_map = _get_name_map(asset_type)
    existing_symbols = set(existing_map)
    for index, raw_symbol in enumerate(symbols):
        symbol = normalize_symbol_for_asset_type(asset_type, raw_symbol)
        raw_name = names[index] if names and index < len(names) else None
        existing_map[symbol] = _resolve_name(symbol, existing_map=existing_map, override_name=raw_name)

    rows = [{"symbol": symbol, "name": name, "is_active": "true"} for symbol, name in existing_map.items()]
    written_rows = write_pipeline_universe_rows(asset_type, rows)
    new_symbols = [row["symbol"] for row in written_rows if row["symbol"] not in existing_symbols]
    sync_results = [sync_universe_symbol(symbol, asset_type=asset_type) for symbol in new_symbols]
    return written_rows, sync_results


def remove_symbols(asset_type: str, symbols: list[str]) -> list[dict[str, str]]:
    remove_set = {normalize_symbol_for_asset_type(asset_type, symbol) for symbol in symbols}
    rows = [row for row in _list_rows(asset_type) if row["symbol"] not in remove_set]
    return write_pipeline_universe_rows(asset_type, rows)


def replace_symbols(asset_type: str, symbols: list[str]) -> list[dict[str, str]]:
    existing_map = _get_name_map(asset_type)
    deduped_symbols: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        symbol = normalize_symbol_for_asset_type(asset_type, raw_symbol)
        if symbol in seen:
            continue
        seen.add(symbol)
        deduped_symbols.append(symbol)

    rows = [
        {"symbol": symbol, "name": _resolve_name(symbol, existing_map=existing_map), "is_active": "true"}
        for symbol in deduped_symbols
    ]
    return write_pipeline_universe_rows(asset_type, rows)


def update_symbol(asset_type: str, symbol: str, name: str | None = None) -> list[dict[str, str]]:
    normalized_symbol = normalize_symbol_for_asset_type(asset_type, symbol)
    existing_map = _get_name_map(asset_type)
    existing_map[normalized_symbol] = _resolve_name(
        normalized_symbol,
        existing_map=existing_map,
        override_name=name,
    )
    rows = [{"symbol": current_symbol, "name": current_name, "is_active": "true"} for current_symbol, current_name in existing_map.items()]
    written_rows = write_pipeline_universe_rows(asset_type, rows)
    sync_results = [sync_universe_symbol(normalized_symbol, asset_type=asset_type)]
    return written_rows, sync_results


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage pipeline universes by asset_type")
    parser.add_argument("--asset-type", default="stock_CN", help="资产域，默认 stock_CN")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list", help="List universe rows")

    add_parser = subparsers.add_parser("add", help="Add symbols to the universe")
    add_parser.add_argument("--symbol", action="append", dest="symbols", required=True, help="股票代码，可重复传入")
    add_parser.add_argument("--name", action="append", dest="names", default=[], help="股票中文名，按 --symbol 顺序对应，可重复传入")

    remove_parser = subparsers.add_parser("remove", help="Remove symbols from the universe")
    remove_parser.add_argument("--symbol", action="append", dest="symbols", required=True, help="股票代码，可重复传入")

    replace_parser = subparsers.add_parser("replace", help="Replace the universe with the provided symbols")
    replace_parser.add_argument("--symbol", action="append", dest="symbols", required=True, help="股票代码，可重复传入")

    update_parser = subparsers.add_parser("update", help="Update one symbol row")
    update_parser.add_argument("--symbol", required=True, help="股票代码")
    update_parser.add_argument("--name", default=None, help="可选：显式指定中文名")

    args = parser.parse_args()
    asset_type = normalize_asset_type(args.asset_type)

    if args.command == "list":
        _print_rows(_list_rows(asset_type))
        return

    if args.command == "add":
        rows, sync_results = add_symbols(asset_type, _expand_symbols(args.symbols or []), names=args.names or [])
        print(json.dumps({"count": len(rows), "rows": rows, "sync_results": sync_results}, ensure_ascii=False, indent=2))
        return

    if args.command == "remove":
        _print_rows(remove_symbols(asset_type, _expand_symbols(args.symbols or [])))
        return

    if args.command == "replace":
        _print_rows(replace_symbols(asset_type, _expand_symbols(args.symbols or [])))
        return

    if args.command == "update":
        rows, sync_results = update_symbol(asset_type, args.symbol, name=args.name)
        print(json.dumps({"count": len(rows), "rows": rows, "sync_results": sync_results}, ensure_ascii=False, indent=2))
        return

    raise ValueError(f"未知命令: {args.command}")


if __name__ == "__main__":
    try:
        main()
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

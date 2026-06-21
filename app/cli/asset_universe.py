"""CLI API for managing universes explicitly by universe name."""
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

from app.data_loader.symbol_backfill import sync_universe_symbol
from app.services.asset_universe import (
    load_universe,
    normalize_asset_type,
    normalize_symbol_for_asset_type,
    normalize_universe,
    write_universe_rows,
)


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


def _list_rows(universe: str) -> list[dict[str, str]]:
    return sorted(load_universe(universe), key=lambda row: (row["asset_type"], row["symbol"]))


def _get_existing_rows(universe: str) -> list[dict[str, str]]:
    return _list_rows(universe)


def _resolve_name(symbol: str, *, existing_rows: list[dict[str, str]], asset_type: str, override_name: str | None = None) -> str:
    explicit_name = str(override_name or "").strip()
    if explicit_name:
        return explicit_name
    for row in existing_rows:
        if row["asset_type"] == asset_type and row["symbol"] == symbol:
            existing_name = str(row.get("name", "")).strip()
            if existing_name:
                return existing_name
    return ""


def _align_asset_types(symbols: list[str], asset_types: list[str]) -> list[str]:
    if not asset_types:
        raise ValueError("必须通过 --asset-type 指定成员 asset_type")
    normalized = [normalize_asset_type(asset_type) for asset_type in asset_types]
    if len(normalized) == 1 and len(symbols) > 1:
        return normalized * len(symbols)
    if len(normalized) != len(symbols):
        raise ValueError("--asset-type 数量必须等于 --symbol 数量，或者只传 1 个供全部 symbol 复用")
    return normalized


def add_symbols(
    universe: str,
    symbols: list[str],
    asset_types: list[str],
    names: list[str | None] | None = None,
) -> tuple[list[dict[str, str]], list[dict[str, object]]]:
    existing_rows = _get_existing_rows(universe)
    existing_members = {(row["asset_type"], row["symbol"]) for row in existing_rows}
    rows_by_key = {(row["asset_type"], row["symbol"]): dict(row) for row in existing_rows}
    resolved_asset_types = _align_asset_types(symbols, asset_types)
    for index, raw_symbol in enumerate(symbols):
        member_asset_type = resolved_asset_types[index]
        symbol = normalize_symbol_for_asset_type(member_asset_type, raw_symbol)
        raw_name = names[index] if names and index < len(names) else None
        key = (member_asset_type, symbol)
        rows_by_key[key] = {
            "asset_type": member_asset_type,
            "symbol": symbol,
            "name": _resolve_name(symbol, existing_rows=existing_rows, asset_type=member_asset_type, override_name=raw_name),
            "is_active": "true",
            "tag": str(rows_by_key.get(key, {}).get("tag", "")).strip(),
        }

    written_rows = write_universe_rows(universe, list(rows_by_key.values()))
    new_members = [row for row in written_rows if (row["asset_type"], row["symbol"]) not in existing_members]
    sync_results = [sync_universe_symbol(row["symbol"], asset_type=row["asset_type"]) for row in new_members]
    return written_rows, sync_results


def remove_symbols(universe: str, symbols: list[str], asset_types: list[str]) -> list[dict[str, str]]:
    resolved_asset_types = _align_asset_types(symbols, asset_types)
    remove_set = {
        (resolved_asset_types[index], normalize_symbol_for_asset_type(resolved_asset_types[index], symbol))
        for index, symbol in enumerate(symbols)
    }
    rows = [row for row in _list_rows(universe) if (row["asset_type"], row["symbol"]) not in remove_set]
    return write_universe_rows(universe, rows)


def replace_symbols(universe: str, symbols: list[str], asset_types: list[str], names: list[str | None] | None = None) -> list[dict[str, str]]:
    existing_rows = _get_existing_rows(universe)
    resolved_asset_types = _align_asset_types(symbols, asset_types)
    deduped_rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for index, raw_symbol in enumerate(symbols):
        member_asset_type = resolved_asset_types[index]
        symbol = normalize_symbol_for_asset_type(member_asset_type, raw_symbol)
        key = (member_asset_type, symbol)
        if key in seen:
            continue
        seen.add(key)
        raw_name = names[index] if names and index < len(names) else None
        existing_row = next((row for row in existing_rows if row["asset_type"] == member_asset_type and row["symbol"] == symbol), {})
        deduped_rows.append(
            {
                "asset_type": member_asset_type,
                "symbol": symbol,
                "name": _resolve_name(symbol, existing_rows=existing_rows, asset_type=member_asset_type, override_name=raw_name),
                "is_active": "true",
                "tag": str(existing_row.get("tag", "")).strip(),
            }
        )
    return write_universe_rows(universe, deduped_rows)


def update_symbol(universe: str, asset_type: str, symbol: str, name: str | None = None) -> tuple[list[dict[str, str]], list[dict[str, object]]]:
    normalized_asset_type = normalize_asset_type(asset_type)
    normalized_symbol = normalize_symbol_for_asset_type(normalized_asset_type, symbol)
    rows = _get_existing_rows(universe)
    existing_row = next(
        (row for row in rows if row["asset_type"] == normalized_asset_type and row["symbol"] == normalized_symbol),
        None,
    )
    updated_rows = [row for row in rows if not (row["asset_type"] == normalized_asset_type and row["symbol"] == normalized_symbol)]
    updated_rows.append(
        {
            "asset_type": normalized_asset_type,
            "symbol": normalized_symbol,
            "name": _resolve_name(normalized_symbol, existing_rows=rows, asset_type=normalized_asset_type, override_name=name),
            "is_active": "true",
            "tag": str(existing_row.get("tag", "") if existing_row else "").strip(),
        }
    )
    written_rows = write_universe_rows(universe, updated_rows)
    sync_results = [sync_universe_symbol(normalized_symbol, asset_type=normalized_asset_type)]
    return written_rows, sync_results


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage universes by universe name")
    parser.add_argument("--universe", default="stock_CN", help="universe 名称，默认 stock_CN")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list", help="List universe rows")

    add_parser = subparsers.add_parser("add", help="Add symbols to the universe")
    add_parser.add_argument("--symbol", action="append", dest="symbols", required=True, help="股票代码，可重复传入")
    add_parser.add_argument("--asset-type", action="append", dest="asset_types", required=True, help="成员 asset_type，可重复传入")
    add_parser.add_argument("--name", action="append", dest="names", default=[], help="股票中文名，按 --symbol 顺序对应，可重复传入")

    remove_parser = subparsers.add_parser("remove", help="Remove symbols from the universe")
    remove_parser.add_argument("--symbol", action="append", dest="symbols", required=True, help="股票代码，可重复传入")
    remove_parser.add_argument("--asset-type", action="append", dest="asset_types", required=True, help="成员 asset_type，可重复传入")

    replace_parser = subparsers.add_parser("replace", help="Replace the universe with the provided symbols")
    replace_parser.add_argument("--symbol", action="append", dest="symbols", required=True, help="股票代码，可重复传入")
    replace_parser.add_argument("--asset-type", action="append", dest="asset_types", required=True, help="成员 asset_type，可重复传入")
    replace_parser.add_argument("--name", action="append", dest="names", default=[], help="股票中文名，按 --symbol 顺序对应，可重复传入")

    update_parser = subparsers.add_parser("update", help="Update one symbol row")
    update_parser.add_argument("--asset-type", required=True, help="成员 asset_type")
    update_parser.add_argument("--symbol", required=True, help="股票代码")
    update_parser.add_argument("--name", default=None, help="可选：显式指定中文名")

    args = parser.parse_args()
    universe = normalize_universe(args.universe)

    if args.command == "list":
        _print_rows(_list_rows(universe))
        return

    if args.command == "add":
        rows, sync_results = add_symbols(universe, _expand_symbols(args.symbols or []), args.asset_types or [], names=args.names or [])
        print(json.dumps({"count": len(rows), "rows": rows, "sync_results": sync_results}, ensure_ascii=False, indent=2))
        return

    if args.command == "remove":
        _print_rows(remove_symbols(universe, _expand_symbols(args.symbols or []), args.asset_types or []))
        return

    if args.command == "replace":
        _print_rows(replace_symbols(universe, _expand_symbols(args.symbols or []), args.asset_types or [], names=args.names or []))
        return

    if args.command == "update":
        rows, sync_results = update_symbol(universe, args.asset_type, args.symbol, name=args.name)
        print(json.dumps({"count": len(rows), "rows": rows, "sync_results": sync_results}, ensure_ascii=False, indent=2))
        return

    raise ValueError(f"未知命令: {args.command}")


if __name__ == "__main__":
    try:
        main()
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

"""CLI API for config/stock_pool.csv."""
from __future__ import annotations

import argparse
import json
import re
import sys

sys.path.insert(0, "/app")

from app.services.stock_pool import add_symbols, list_stock_pool, remove_symbols, replace_symbols, update_symbol


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage config/stock_pool.csv")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list", help="List stock pool rows")

    add_parser = subparsers.add_parser("add", help="Add symbols to stock pool")
    add_parser.add_argument("--symbol", action="append", dest="symbols", required=True, help="股票代码，可重复传入")
    add_parser.add_argument("--name", action="append", dest="names", default=[], help="股票中文名，按 --symbol 顺序对应，可重复传入")

    remove_parser = subparsers.add_parser("remove", help="Remove symbols from stock pool")
    remove_parser.add_argument("--symbol", action="append", dest="symbols", required=True, help="股票代码，可重复传入")

    replace_parser = subparsers.add_parser("replace", help="Replace stock pool with the provided symbols")
    replace_parser.add_argument("--symbol", action="append", dest="symbols", required=True, help="股票代码，可重复传入")

    update_parser = subparsers.add_parser("update", help="Update one symbol row")
    update_parser.add_argument("--symbol", required=True, help="股票代码")
    update_parser.add_argument("--name", default=None, help="可选：显式指定中文名")

    args = parser.parse_args()

    if args.command == "list":
        _print_rows(list_stock_pool())
        return

    if args.command == "add":
        _print_rows(add_symbols(_expand_symbols(args.symbols or []), names=args.names or []))
        return

    if args.command == "remove":
        _print_rows(remove_symbols(_expand_symbols(args.symbols or [])))
        return

    if args.command == "replace":
        _print_rows(replace_symbols(_expand_symbols(args.symbols or [])))
        return

    if args.command == "update":
        _print_rows(update_symbol(args.symbol, name=args.name))
        return

    raise ValueError(f"未知命令: {args.command}")


if __name__ == "__main__":
    try:
        main()
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

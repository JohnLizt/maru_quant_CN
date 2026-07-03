"""CLI helpers for ETL universe membership."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
for candidate in ["/app", str(REPO_ROOT)]:
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from app.services.asset_universe import (  # noqa: E402
    ensure_etl_universe_symbol,
    etl_universe_contains_symbol,
    get_etl_symbol_name_map,
    normalize_asset_type,
    normalize_symbol_for_asset_type,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Check or update ETL universe membership")
    parser.add_argument("--asset-type", required=True, help="asset_type 名称")
    subparsers = parser.add_subparsers(dest="command", required=True)

    contains_parser = subparsers.add_parser("contains", help="Check whether a symbol is in the ETL universe")
    contains_parser.add_argument("--symbol", required=True, help="股票代码")

    ensure_parser = subparsers.add_parser("ensure", help="Ensure a symbol exists in the ETL universe")
    ensure_parser.add_argument("--symbol", required=True, help="股票代码")
    ensure_parser.add_argument("--name", default=None, help="可选中文名")

    args = parser.parse_args()
    asset_type = normalize_asset_type(args.asset_type)
    symbol = normalize_symbol_for_asset_type(asset_type, args.symbol)

    if args.command == "contains":
        contains = etl_universe_contains_symbol(asset_type, symbol)
        symbol_name = get_etl_symbol_name_map(asset_type).get(symbol, "") if contains else ""
        print(
            json.dumps(
                {
                    "asset_type": asset_type,
                    "symbol": symbol,
                    "contains": contains,
                    "name": symbol_name,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    if args.command == "ensure":
        rows, added = ensure_etl_universe_symbol(asset_type, symbol, name=args.name)
        row = next((item for item in rows if item["symbol"] == symbol), None)
        print(
            json.dumps(
                {
                    "asset_type": asset_type,
                    "symbol": symbol,
                    "added": added,
                    "row": row,
                    "count": len(rows),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    raise ValueError(f"未知命令: {args.command}")


if __name__ == "__main__":
    try:
        main()
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

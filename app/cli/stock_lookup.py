"""CLI for CN stock name/symbol lookup."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
for candidate in ["/app", str(REPO_ROOT)]:
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from app.services.stock_lookup import lookup_stock_candidates, resolve_stock_symbol  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Resolve CN stock name to symbol candidates")
    parser.add_argument("--query", required=True, help="股票名称、简称或代码片段")
    parser.add_argument("--asset-type", default="stock_CN", help="asset_type，默认 stock_CN")
    parser.add_argument("--limit", type=int, default=5, help="候选数量，默认 5")
    parser.add_argument("--resolve", action="store_true", help="要求唯一解析；歧义时报错")
    args = parser.parse_args()

    if args.resolve:
        result = resolve_stock_symbol(args.query, asset_type=args.asset_type)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    candidates = lookup_stock_candidates(args.query, asset_type=args.asset_type, limit=args.limit)
    print(
        json.dumps(
            {
                "query": args.query,
                "asset_type": args.asset_type,
                "count": len(candidates),
                "candidates": candidates,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    try:
        main()
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

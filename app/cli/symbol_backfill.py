"""CLI for single-symbol market and factor backfill."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
for candidate in ["/app", str(REPO_ROOT)]:
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from app.data_loader.symbol_backfill import backfill_symbol_factors  # noqa: E402
from app.services.asset_universe import normalize_asset_type, normalize_symbol_for_asset_type  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill one symbol's market data and time-series factors")
    parser.add_argument("--asset-type", required=True, help="asset_type 名称")
    parser.add_argument("--symbol", required=True, help="股票代码")
    parser.add_argument("--end-date", default=None, help="截止日期 YYYY-MM-DD，默认今天")
    args = parser.parse_args()

    asset_type = normalize_asset_type(args.asset_type)
    symbol = normalize_symbol_for_asset_type(asset_type, args.symbol)
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date else None
    synced = backfill_symbol_factors(symbol, asset_type=asset_type, end_date=end_date)

    print(
        json.dumps(
            {
                "asset_type": asset_type,
                "symbol": symbol,
                "synced": synced,
                "end_date": end_date.isoformat() if end_date else None,
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

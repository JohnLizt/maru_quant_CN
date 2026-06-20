"""Summarize daily ETL/factor sync status by asset_type."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
for candidate in ["/app", str(REPO_ROOT)]:
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from sqlalchemy import text

from app.services.asset_universe import list_asset_types
from app.utils.db import get_engine


def _rows() -> list[dict[str, object]]:
    asset_types = [config.asset_type for config in list_asset_types(enabled_only=True)]
    sql = text(
        """
        WITH latest AS (
            SELECT DISTINCT ON (data_type, asset_type)
                data_type,
                asset_type,
                data_source,
                last_date,
                status,
                error_msg,
                updated_at
            FROM meta.sync_status
            WHERE asset_type = ANY(:asset_types)
              AND data_type = ANY(:data_types)
              AND (symbol IS NULL OR symbol = '')
            ORDER BY data_type, asset_type, updated_at DESC
        )
        SELECT data_type, asset_type, data_source, last_date, status, error_msg, updated_at
        FROM latest
        ORDER BY asset_type, data_type
        """
    )
    with get_engine().connect() as conn:
        rows = conn.execute(
            sql,
            {
                "asset_types": asset_types,
                "data_types": [
                    "daily_market",
                    "daily_factors",
                    "factor_daily_ic",
                    "factor_ic_summary",
                    "factor_daily_quantile_return",
                    "factor_quantile_summary",
                    "factor_daily_topk_return",
                    "factor_topk_summary",
                ],
            },
        ).fetchall()

    return [
        {
            "data_type": row[0],
            "asset_type": row[1],
            "data_source": row[2],
            "last_date": row[3],
            "status": row[4],
            "error_msg": row[5],
            "updated_at": row[6],
        }
        for row in rows
    ]


def _summary_payload() -> dict[str, object]:
    rows = _rows()
    by_asset: dict[str, dict[str, object]] = {}
    for row in rows:
        asset_type = str(row["asset_type"])
        current = by_asset.setdefault(asset_type, {"asset_type": asset_type, "steps": {}})
        current["steps"][str(row["data_type"])] = {
            "data_source": row["data_source"],
            "last_date": str(row["last_date"]) if row["last_date"] is not None else None,
            "status": row["status"],
            "error_msg": row["error_msg"],
            "updated_at": str(row["updated_at"]) if row["updated_at"] is not None else None,
        }
    return {"assets": list(by_asset.values())}


def main(output_format: str) -> None:
    payload = _summary_payload()
    if output_format == "json":
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    for asset in payload["assets"]:
        print(f"[{asset['asset_type']}]")
        steps = asset["steps"]
        for data_type in [
            "daily_market",
            "daily_factors",
            "factor_daily_ic",
            "factor_ic_summary",
            "factor_daily_quantile_return",
            "factor_quantile_summary",
            "factor_daily_topk_return",
            "factor_topk_summary",
        ]:
            step = steps.get(data_type)
            if step is None:
                print(f"  {data_type}: missing")
                continue
            print(
                f"  {data_type}: status={step['status']} last_date={step['last_date']} "
                f"source={step['data_source']}"
            )
            if step["error_msg"]:
                print(f"    error={step['error_msg']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Summarize daily sync status by asset_type")
    parser.add_argument("--format", choices=["table", "json"], default="table")
    args = parser.parse_args()
    main(args.format)

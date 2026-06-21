"""Query ETF rotation candidates from strategy snapshot service."""
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

from app.services.strategy_service import build_strategy_snapshot
from app.strategy.etf_rotation import ETFUniverseRotationStrategy


def _to_result_rows(signal_snapshot, top_n: int) -> list[dict]:
    results = []
    for row in signal_snapshot.filter(signal_snapshot["rank"] <= top_n).iter_rows(named=True):
        factor_names = [
            name for name in signal_snapshot.columns
            if not name.endswith("_score")
            and name not in {"time", "asset_type", "signal_mode", "symbol", "symbol_name", "tag", "composite_score", "label", "contributors", "rank"}
        ]
        results.append({
            "time": row["time"].isoformat() if row["time"] is not None else None,
            "symbol": row["symbol"],
            "symbol_name": row.get("symbol_name", ""),
            "tag": row.get("tag", ""),
            "signal_mode": row.get("signal_mode"),
            "raw_factors": {factor_name: row.get(factor_name) for factor_name in factor_names},
            "normalized_factors": {factor_name: row.get(f"{factor_name}_score") for factor_name in factor_names},
            "composite_score": row["composite_score"],
            "label": row["label"],
            "contributors": row["contributors"],
            "rank": row["rank"],
        })
    return results


def _to_strategy_rows(decisions) -> list[dict]:
    results = []
    for row in decisions.iter_rows(named=True):
        results.append({
            "time": row["time"].isoformat() if row["time"] is not None else None,
            "symbol": row["symbol"],
            "strategy": row["strategy"],
            "strategy_mode": row["strategy_mode"],
            "decision_type": row["decision_type"],
            "signal": row["signal"],
            "target_weight": row["target_weight"],
            "score": row["score"],
            "rank": row["rank"],
            "tag": row["tag"],
            "metadata": json.loads(row["metadata"]) if row.get("metadata") else {},
        })
    return results


def main(date: str | None, top_n: int, profile_name: str, universe: str) -> int:
    strategy = ETFUniverseRotationStrategy(top_n=top_n, profile_name=profile_name, max_per_tag=1)
    as_of_date = datetime.strptime(date, "%Y-%m-%d").date() if date else None
    bundle = build_strategy_snapshot(
        strategy,
        start_date=as_of_date,
        end_date=as_of_date,
        universe=universe,
        profile_name=profile_name,
        as_of_date=as_of_date,
    )
    payload = {
        "query": {
            "universe": universe,
            "profile": profile_name,
            "strategy": strategy.strategy_name,
            "strategy_mode": strategy.strategy_mode,
            "date": date,
            "top_n": top_n,
        },
        "row_count": 0,
        "selected_count": bundle.decisions.height,
        "results": [],
        "selected_results": _to_strategy_rows(bundle.decisions),
    }

    if not bundle.signal_snapshot.is_empty():
        payload["results"] = _to_result_rows(bundle.signal_snapshot, top_n)
        payload["row_count"] = len(payload["results"])

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query ETF rotation candidates")
    parser.add_argument("--date", default=None, help="单日查询日期 YYYY-MM-DD")
    parser.add_argument("--top", type=int, default=4, help="返回前 N 名展示结果，默认 4")
    parser.add_argument(
        "--profile",
        default="trend_etf_momentum_reg20",
        help="ETF signal profile，默认 trend_etf_momentum_reg20",
    )
    parser.add_argument("--universe", default="etf_mixed", help="策略池，默认 etf_mixed")
    args = parser.parse_args()
    raise SystemExit(main(args.date, args.top, args.profile, args.universe))

"""Stock symbol lookup helpers for natural-language favorite management."""
from __future__ import annotations

import os
import re
from functools import lru_cache
from typing import Any

import tushare as ts

from app.services.asset_universe import load_etl_universe, normalize_asset_type, normalize_symbol_for_asset_type


def _normalize_query(value: str) -> str:
    normalized = re.sub(r"\s+", "", str(value or "")).strip().upper()
    if not normalized:
        raise ValueError("query 不能为空")
    return normalized


def _score_candidate(query: str, *, symbol: str, name: str, cnspell: str = "") -> int:
    symbol_u = symbol.upper()
    name_u = re.sub(r"\s+", "", name).upper()
    cnspell_u = cnspell.upper()

    if query == symbol_u:
        return 120
    if query == name_u:
        return 110
    if query == cnspell_u and cnspell_u:
        return 105
    if symbol_u.startswith(query):
        return 100
    if name_u.startswith(query):
        return 95
    if query in name_u:
        return 90 - max(0, len(name_u) - len(query))
    if cnspell_u.startswith(query) and cnspell_u:
        return 85
    if query in cnspell_u and cnspell_u:
        return 80 - max(0, len(cnspell_u) - len(query))
    return -1


@lru_cache(maxsize=1)
def _fetch_tushare_stock_basics() -> list[dict[str, str]]:
    token = os.environ.get("TUSHARE_TOKEN", "")
    ts.set_token(token)
    pro = ts.pro_api()
    df = pro.stock_basic(
        exchange="",
        list_status="L",
        fields="ts_code,symbol,name,cnspell,market",
    )
    if df is None or df.empty:
        return []
    records: list[dict[str, str]] = []
    for record in df.to_dict(orient="records"):
        records.append(
            {
                "symbol": str(record.get("ts_code", "")).strip().upper(),
                "name": str(record.get("name", "")).strip(),
                "cnspell": str(record.get("cnspell", "")).strip().upper(),
                "market": str(record.get("market", "")).strip(),
            }
        )
    return records


def lookup_stock_candidates(
    query: str,
    *,
    asset_type: str = "stock_CN",
    limit: int = 10,
) -> list[dict[str, Any]]:
    normalized_asset_type = normalize_asset_type(asset_type)
    if normalized_asset_type != "stock_CN":
        raise ValueError(f"暂不支持 asset_type={asset_type} 的股票名称检索")

    normalized_query = _normalize_query(query)
    local_rows = load_etl_universe("stock_CN")
    local_by_symbol = {row["symbol"]: row for row in local_rows}
    candidates: dict[str, dict[str, Any]] = {}

    def merge_candidate(symbol: str, name: str, *, source: str, cnspell: str = "", market: str = "") -> None:
        normalized_symbol = normalize_symbol_for_asset_type("stock_CN", symbol)
        score = _score_candidate(normalized_query, symbol=normalized_symbol, name=name, cnspell=cnspell)
        if score < 0:
            return
        current = candidates.get(normalized_symbol)
        in_etl_universe = normalized_symbol in local_by_symbol
        row_name = local_by_symbol.get(normalized_symbol, {}).get("name", "") if in_etl_universe else ""
        payload = {
            "asset_type": "stock_CN",
            "symbol": normalized_symbol,
            "name": row_name or name,
            "market": market,
            "cnspell": cnspell,
            "in_etl_universe": in_etl_universe,
            "match_score": score,
            "source": source,
        }
        if current is None or payload["match_score"] > current["match_score"] or (
            payload["match_score"] == current["match_score"] and payload["in_etl_universe"] and not current["in_etl_universe"]
        ):
            candidates[normalized_symbol] = payload

    for row in local_rows:
        merge_candidate(
            row["symbol"],
            str(row.get("name", "")).strip(),
            source="etl_universe",
        )

    for row in _fetch_tushare_stock_basics():
        merge_candidate(
            row["symbol"],
            row["name"],
            source="tushare_stock_basic",
            cnspell=row.get("cnspell", ""),
            market=row.get("market", ""),
        )

    ranked = sorted(
        candidates.values(),
        key=lambda row: (-int(row["match_score"]), not bool(row["in_etl_universe"]), row["symbol"]),
    )
    return ranked[:limit]


def resolve_stock_symbol(query: str, *, asset_type: str = "stock_CN") -> dict[str, Any]:
    candidates = lookup_stock_candidates(query, asset_type=asset_type, limit=5)
    if not candidates:
        raise ValueError(f"未找到匹配股票: {query}")

    top = candidates[0]
    if len(candidates) == 1:
        return top

    second = candidates[1]
    if top["match_score"] >= 110 and top["match_score"] > second["match_score"]:
        return top
    if top["match_score"] >= 95 and top["name"] and top["name"] != second["name"] and top["match_score"] >= second["match_score"] + 10:
        return top

    preview = " / ".join(f"{row['name']}({row['symbol']})" for row in candidates[:5])
    raise ValueError(f"股票名称存在歧义: {query}，候选: {preview}")

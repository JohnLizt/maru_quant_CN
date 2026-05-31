"""Query composite signal scores for the full factor universe."""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
for candidate in ["/app", str(REPO_ROOT)]:
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

import polars as pl
from loguru import logger

from app.services.signal_score import query_signal_scores


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


def _build_json_payload(
    profile_name: str,
    signal_mode: str,
    factor_names: list[str],
    normalization_scope: str,
    asset_type: str,
    top_n: int | None,
    df: pl.DataFrame,
    start_date: str | None,
    end_date: str | None,
) -> dict:
    results = []
    for row in df.iter_rows(named=True):
        results.append({
            "time": row["time"].isoformat() if row["time"] is not None else None,
            "symbol": row["symbol"],
            "symbol_name": row["symbol_name"],
            "tag": row.get("tag", ""),
            "signal_mode": row.get("signal_mode"),
            "raw_factors": {factor_name: row[factor_name] for factor_name in factor_names},
            "normalized_factors": {factor_name: row[f"{factor_name}_score"] for factor_name in factor_names},
            "composite_score": row["composite_score"],
            "label": row["label"],
            "contributors": row["contributors"],
            "rank": row.get("rank"),
        })

    query: dict[str, str | None] = {
        "profile": profile_name,
        "signal_mode": signal_mode,
        "asset_type": asset_type,
        "normalization_scope": normalization_scope,
        "start_date": start_date,
        "end_date": end_date,
        "top_n": top_n,
    }
    if start_date == end_date:
        query["date"] = start_date

    return {
        "query": query,
        "row_count": len(results),
        "results": results,
    }


def _print_result(
    profile,
    df: pl.DataFrame,
    output_format: str,
    *,
    profile_name: str,
    normalization_scope: str,
    asset_type: str,
    top_n: int | None,
    start_date: str | None,
    end_date: str | None,
) -> None:
    if output_format == "json":
        payload = _build_json_payload(
            profile_name,
            profile.signal_mode,
            profile.factor_names,
            normalization_scope,
            asset_type,
            top_n,
            df,
            start_date,
            end_date,
        )
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    flat = df.with_columns(pl.col("contributors").list.join("|").alias("contributors"))

    if output_format == "csv":
        print(flat.write_csv())
        return

    print(flat)


def main(
    symbols: list[str],
    start_date: str | None,
    end_date: str | None,
    output: str | None,
    output_format: str,
    profile_name: str,
    asset_type: str,
    top_n: int | None,
) -> None:
    expanded_symbols = _expand_symbols(symbols)
    profile, df = query_signal_scores(
        expanded_symbols,
        start_date,
        end_date,
        asset_type=asset_type,
        profile_name=profile_name,
        top_n=top_n,
    )

    logger.info(
        "查询信号评分 | asset_type={} | profile={} | scope={} | symbols={} | start={} | end={} | top_n={}",
        asset_type,
        profile.name,
        profile.normalization_scope,
        expanded_symbols or "ALL",
        start_date or "today",
        end_date or "today",
        top_n or "ALL",
    )

    if df.is_empty():
        logger.warning("未查询到信号评分数据")
    else:
        _print_result(
            profile,
            df,
            output_format,
            profile_name=profile.name,
            normalization_scope=profile.normalization_scope,
            asset_type=asset_type,
            top_n=top_n,
            start_date=start_date,
            end_date=end_date,
        )
        logger.success("查询完成 | 返回 {} 条记录", df.height)

    if output:
        os.makedirs(os.path.dirname(output) or ".", exist_ok=True)
        df.with_columns(pl.col("contributors").list.join("|").alias("contributors")).write_csv(output)
        logger.info("结果已保存至 {}", output)


if __name__ == "__main__":
    default_today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser(description="Query composite signal scores for one or more stocks")
    parser.add_argument("--symbol", action="append", dest="symbols", help="股票代码，可重复传入多次；不传则返回全池结果")
    parser.add_argument("--date", default=None, help=f"单日查询日期 YYYY-MM-DD（会同时作为 start/end，默认 {default_today}）")
    parser.add_argument("--start-date", default=None, help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="结束日期 YYYY-MM-DD")
    parser.add_argument("--profile", default="trend_v1", help="信号评分 profile，默认 trend_v1")
    parser.add_argument("--asset-type", default="stock_CN", help="资产域，默认 stock_CN")
    parser.add_argument("--output", default=None, help="可选：将结果导出为 CSV，如 logs/query_signal_scores.csv")
    parser.add_argument("--top", type=int, default=None, help="可选：按单日综合分截取前 N 名")
    parser.add_argument("--format", choices=["table", "json", "csv"], default="table", help="输出格式：table（默认）、json、csv")
    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date
    if args.date:
        start_date = args.date
        end_date = args.date

    try:
        main(args.symbols or [], start_date, end_date, args.output, args.format, args.profile, args.asset_type, args.top)
    except ValueError as exc:
        logger.error(str(exc))
        sys.exit(1)

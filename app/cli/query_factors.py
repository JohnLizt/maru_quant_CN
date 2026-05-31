"""
查询个股因子数据。

用法：
  python app/cli/query_factors.py --symbol 603019.SH --factor price_to_ma20
  python app/cli/query_factors.py --symbol 603019.SH --factor price_to_ma20 --factor limit_up
  python app/cli/query_factors.py --symbol 603019.SH --symbol 300059.SZ --factor price_to_ma20 --date 2026-04-14
  python app/cli/query_factors.py --symbol 603019.SH --factor price_to_ma20 --start-date 2026-04-10 --end-date 2026-04-14 --output logs/query.csv
"""
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

from loguru import logger

from app.factors.registry import FACTOR_REGISTRY
from app.services.factor_query import query_stock_factor, query_stock_factors, query_stocks_factors


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


def _resolve_factors(factors: list[str]) -> list[str]:
    if factors:
        return factors
    return list(FACTOR_REGISTRY)


def _print_result(df, output_format: str) -> None:
    if output_format == "json":
        print(json.dumps(df.to_dicts(), ensure_ascii=False, default=str))
        return

    if output_format == "csv":
        print(df.write_csv())
        return

    print(df)


def main(
    symbols: list[str],
    factors: list[str],
    start_date: str | None,
    end_date: str | None,
    output: str | None,
    output_format: str,
) -> None:
    symbols = _expand_symbols(symbols)
    if not symbols:
        raise ValueError("至少提供一个 --symbol")
    factors = _resolve_factors(factors)

    if start_date and end_date is None:
        end_date = start_date
    if end_date and start_date is None:
        start_date = end_date

    logger.info(
        f"查询因子 | symbols={symbols} | factors={factors} | "
        f"start={start_date or 'today'} | end={end_date or 'today'}"
    )

    if len(symbols) == 1 and len(factors) == 1:
        df = query_stock_factor(symbols[0], factors[0], start_date, end_date)
    elif len(symbols) == 1:
        df = query_stock_factors(symbols[0], factors, start_date, end_date)
    else:
        df = query_stocks_factors(symbols, factors, start_date, end_date)

    if df.is_empty():
        logger.warning("未查询到因子数据")
    else:
        _print_result(df, output_format)
        logger.success(f"查询完成 | 返回 {df.height} 条记录")

    if output:
        os.makedirs(os.path.dirname(output) or ".", exist_ok=True)
        df.write_csv(output)
        logger.info(f"结果已保存至 {output}")


if __name__ == "__main__":
    default_today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser(description="Query factor values for one or more stocks")
    parser.add_argument("--symbol", action="append", dest="symbols", help="股票代码，可重复传入多次，如 --symbol 603019.SH --symbol 300059.SZ")
    parser.add_argument("--factor", action="append", dest="factors", help="因子名称，可重复传入多次；不传则默认查询全部已注册因子")
    parser.add_argument("--date", default=None, help=f"单日查询日期 YYYY-MM-DD（会同时作为 start/end，默认 {default_today}）")
    parser.add_argument("--start-date", default=None, help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="结束日期 YYYY-MM-DD")
    parser.add_argument("--output", default=None, help="可选：将结果导出为 CSV，如 logs/query_factors.csv")
    parser.add_argument("--format", choices=["table", "json", "csv"], default="table", help="输出格式：table（默认）、json、csv")
    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date
    if args.date:
        start_date = args.date
        end_date = args.date

    try:
        main(args.symbols or [], args.factors or [], start_date, end_date, args.output, args.format)
    except ValueError as exc:
        logger.error(str(exc))
        sys.exit(1)

"""Query persisted factor IC summary rows."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from sqlalchemy import text


REPO_ROOT = Path(__file__).resolve().parents[2]
for candidate in ["/app", str(REPO_ROOT)]:
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from app.utils.db import get_engine


def query_factor_ic_summary(
    *,
    asset_type: str,
    as_of_date: str | None,
    factor_names: list[str] | None,
    lags: list[int] | None,
    window_days: int,
) -> pl.DataFrame:
    factor_filter = ""
    lag_filter = ""
    params: dict[str, object] = {"asset_type": asset_type, "window_days": window_days}

    if as_of_date:
        params["as_of_date"] = as_of_date
        date_filter = "AND as_of_date = :as_of_date"
    else:
        date_filter = """
          AND as_of_date = (
              SELECT MAX(as_of_date)
              FROM analytics.factor_ic_summary latest
              WHERE latest.asset_type = :asset_type
                AND latest.window_days = :window_days
          )
        """

    if factor_names:
        placeholders = ", ".join(f":f_{index}" for index in range(len(factor_names)))
        factor_filter = f"AND factor_name IN ({placeholders})"
        params |= {f"f_{index}": factor_name for index, factor_name in enumerate(factor_names)}

    if lags:
        placeholders = ", ".join(f":lag_{index}" for index in range(len(lags)))
        lag_filter = f"AND lag IN ({placeholders})"
        params |= {f"lag_{index}": lag for index, lag in enumerate(lags)}

    sql = text(
        f"""
        SELECT
            as_of_date, asset_type, factor_name, lag, window_days,
            mean_ic, ic_std, ic_ir, t_stat, win_rate,
            mean_rank_ic, rank_ic_std, rank_ic_ir, n_days,
            start_date, end_date, calc_version
        FROM analytics.factor_ic_summary
        WHERE asset_type = :asset_type
          AND window_days = :window_days
          {date_filter}
          {factor_filter}
          {lag_filter}
        ORDER BY as_of_date DESC, lag, factor_name
        """
    )
    with get_engine().connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return pl.DataFrame(
        rows,
        schema=[
            "as_of_date",
            "asset_type",
            "factor_name",
            "lag",
            "window_days",
            "mean_ic",
            "ic_std",
            "ic_ir",
            "t_stat",
            "win_rate",
            "mean_rank_ic",
            "rank_ic_std",
            "rank_ic_ir",
            "n_days",
            "start_date",
            "end_date",
            "calc_version",
        ],
        orient="row",
    )


def main(
    asset_type: str,
    as_of_date: str | None,
    factors: list[str] | None,
    lags: list[int] | None,
    window_days: int,
    output_format: str,
) -> int:
    df = query_factor_ic_summary(
        asset_type=asset_type,
        as_of_date=as_of_date,
        factor_names=factors,
        lags=lags,
        window_days=window_days,
    )
    if output_format == "json":
        print(json.dumps(df.to_dicts(), ensure_ascii=False, default=str, indent=2))
    elif output_format == "csv":
        print(df.write_csv())
    else:
        print(df)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query stored factor IC summary rows")
    parser.add_argument("--asset-type", default="stock_CN", help="资产类型，默认 stock_CN")
    parser.add_argument(
        "--date",
        default=None,
        help="查询日期 YYYY-MM-DD；不传则返回最新可用 as_of_date",
    )
    parser.add_argument("--factor", action="append", dest="factors", default=[], help="因子名称，可重复传入")
    parser.add_argument("--lag", action="append", dest="lags", type=int, default=[], help="forward lag，可重复传入")
    parser.add_argument("--window-days", type=int, default=126, help="滚动窗口交易日数，默认 126")
    parser.add_argument("--format", choices=["table", "json", "csv"], default="table", help="输出格式")
    args = parser.parse_args()
    raise SystemExit(
        main(
            args.asset_type,
            args.date,
            args.factors or None,
            args.lags or None,
            args.window_days,
            args.format,
        )
    )

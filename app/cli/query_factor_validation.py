"""Query persisted factor validation summary rows."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import polars as pl
from sqlalchemy import text


REPO_ROOT = Path(__file__).resolve().parents[2]
for candidate in ["/app", str(REPO_ROOT)]:
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from app.utils.db import get_engine


def query_factor_quantile_summary(
    *,
    asset_type: str,
    as_of_date: str | None,
    factor_names: list[str] | None,
    lags: list[int] | None,
    quantile_n: int | None,
    quantile_ids: list[int] | None,
    window_days: int,
) -> pl.DataFrame:
    params: dict[str, object] = {"asset_type": asset_type, "window_days": window_days}
    factor_filter = ""
    lag_filter = ""
    quantile_n_filter = ""
    quantile_id_filter = ""

    if as_of_date:
        params["as_of_date"] = as_of_date
        date_filter = "AND as_of_date = :as_of_date"
    else:
        date_filter = """
          AND as_of_date = (
              SELECT MAX(as_of_date)
              FROM analytics.factor_quantile_summary latest
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

    if quantile_n is not None:
        quantile_n_filter = "AND quantile_n = :quantile_n"
        params["quantile_n"] = quantile_n

    if quantile_ids:
        placeholders = ", ".join(f":qid_{index}" for index in range(len(quantile_ids)))
        quantile_id_filter = f"AND quantile_id IN ({placeholders})"
        params |= {f"qid_{index}": quantile_id for index, quantile_id in enumerate(quantile_ids)}

    sql = text(
        f"""
        SELECT
            as_of_date, asset_type, factor_name, lag, quantile_n, quantile_id, window_days,
            mean_ret, ret_std, ret_ir, win_rate, n_days,
            start_date, end_date, calc_version
        FROM analytics.factor_quantile_summary
        WHERE asset_type = :asset_type
          AND window_days = :window_days
          {date_filter}
          {factor_filter}
          {lag_filter}
          {quantile_n_filter}
          {quantile_id_filter}
        ORDER BY as_of_date DESC, lag, factor_name, quantile_id
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
            "quantile_n",
            "quantile_id",
            "window_days",
            "mean_ret",
            "ret_std",
            "ret_ir",
            "win_rate",
            "n_days",
            "start_date",
            "end_date",
            "calc_version",
        ],
        orient="row",
    )


def query_factor_topk_summary(
    *,
    asset_type: str,
    as_of_date: str | None,
    factor_names: list[str] | None,
    lags: list[int] | None,
    top_ks: list[int] | None,
    window_days: int,
) -> pl.DataFrame:
    params: dict[str, object] = {"asset_type": asset_type, "window_days": window_days}
    factor_filter = ""
    lag_filter = ""
    topk_filter = ""

    if as_of_date:
        params["as_of_date"] = as_of_date
        date_filter = "AND as_of_date = :as_of_date"
    else:
        date_filter = """
          AND as_of_date = (
              SELECT MAX(as_of_date)
              FROM analytics.factor_topk_summary latest
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

    if top_ks:
        placeholders = ", ".join(f":topk_{index}" for index in range(len(top_ks)))
        topk_filter = f"AND top_k IN ({placeholders})"
        params |= {f"topk_{index}": top_k for index, top_k in enumerate(top_ks)}

    sql = text(
        f"""
        SELECT
            as_of_date, asset_type, factor_name, lag, top_k, window_days,
            mean_topk_ret, topk_ret_std, topk_ret_ir, topk_win_rate,
            mean_excess_ret, excess_ret_std, excess_ret_ir, excess_win_rate,
            n_days, start_date, end_date, calc_version
        FROM analytics.factor_topk_summary
        WHERE asset_type = :asset_type
          AND window_days = :window_days
          {date_filter}
          {factor_filter}
          {lag_filter}
          {topk_filter}
        ORDER BY as_of_date DESC, lag, factor_name, top_k
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
            "top_k",
            "window_days",
            "mean_topk_ret",
            "topk_ret_std",
            "topk_ret_ir",
            "topk_win_rate",
            "mean_excess_ret",
            "excess_ret_std",
            "excess_ret_ir",
            "excess_win_rate",
            "n_days",
            "start_date",
            "end_date",
            "calc_version",
        ],
        orient="row",
    )


def main(
    mode: str,
    asset_type: str,
    as_of_date: str | None,
    factors: list[str] | None,
    lags: list[int] | None,
    quantile_n: int | None,
    quantile_ids: list[int] | None,
    top_ks: list[int] | None,
    window_days: int,
    output_format: str,
) -> int:
    if mode == "quantile-summary":
        df = query_factor_quantile_summary(
            asset_type=asset_type,
            as_of_date=as_of_date,
            factor_names=factors,
            lags=lags,
            quantile_n=quantile_n,
            quantile_ids=quantile_ids,
            window_days=window_days,
        )
    else:
        df = query_factor_topk_summary(
            asset_type=asset_type,
            as_of_date=as_of_date,
            factor_names=factors,
            lags=lags,
            top_ks=top_ks,
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
    parser = argparse.ArgumentParser(description="Query stored factor validation summary rows")
    parser.add_argument("--mode", choices=["quantile-summary", "topk-summary"], required=True)
    parser.add_argument("--asset-type", default="stock_CN", help="资产类型，默认 stock_CN")
    parser.add_argument("--date", default=None, help="查询日期 YYYY-MM-DD；不传则返回最新可用 as_of_date")
    parser.add_argument("--factor", action="append", dest="factors", default=[], help="因子名称，可重复传入")
    parser.add_argument("--lag", action="append", dest="lags", type=int, default=[], help="forward lag，可重复传入")
    parser.add_argument("--quantile-n", type=int, default=None, help="分组数过滤，仅 quantile-summary 生效")
    parser.add_argument("--quantile-id", action="append", dest="quantile_ids", type=int, default=[], help="分组编号，可重复传入")
    parser.add_argument("--top-k", action="append", dest="top_ks", type=int, default=[], help="Top-K 编号，可重复传入")
    parser.add_argument("--window-days", type=int, default=126, help="滚动窗口交易日数，默认 126")
    parser.add_argument("--format", choices=["table", "json", "csv"], default="table", help="输出格式")
    args = parser.parse_args()
    raise SystemExit(
        main(
            args.mode,
            args.asset_type,
            args.date,
            args.factors or None,
            args.lags or None,
            args.quantile_n,
            args.quantile_ids or None,
            args.top_ks or None,
            args.window_days,
            args.format,
        )
    )

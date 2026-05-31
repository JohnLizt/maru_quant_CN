"""Generate the etf_CN universe from Tushare-listed exchange funds."""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import tushare as ts

from app.data_loader.providers.tushare import TushareLoader
from app.services.asset_universe import write_pipeline_universe_rows


REPO_ROOT = Path(__file__).resolve().parents[1]
for candidate in ["/app", str(REPO_ROOT)]:
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

DEFAULT_LOOKBACK_DAYS = 60
DEFAULT_MIN_AVG_AMOUNT = 300_000.0
DEFAULT_MIN_VALID_DAYS = 40
OVERRIDES_PATH = REPO_ROOT / "config" / "universe_rules" / "etf_cn_tag_overrides.csv"

EXCLUDE_NAME_PATTERNS = ("LOF", "REIT")
ETF_INCLUDE_PATTERN = re.compile(r"ETF", re.IGNORECASE)

TAG_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("satellite", ("卫星",)),
    ("aerospace", ("航空航天", "航天", "航空")),
    ("gold", ("黄金", "金ETF")),
    ("money_market", ("货币ETF", "银华日利", "华宝添益")),
    ("bond", ("债ETF", "国债", "政金债", "信用债", "公司债", "可转债", "地方债", "城投债", "短融ETF")),
    ("free_cash_flow", ("自由现金流",)),
    ("dividend", ("红利", "股息", "低波")),
    ("food_beverage", ("食品饮料", "酒ETF", "白酒")),
    ("home_appliances", ("家电",)),
    ("consumer_electronics", ("消费电子",)),
    ("consumer", ("消费",)),
    ("fintech", ("金融科技",)),
    ("software", ("软件",)),
    ("gaming", ("游戏",)),
    ("semiconductor_equipment", ("半导体设备",)),
    ("chip", ("芯片", "半导体", "集成电路")),
    ("robotics", ("机器人",)),
    ("ai", ("人工智能", "AI")),
    ("new_energy", ("创业板新能源", "新能源")),
    ("ev", ("新能源车", "汽车")),
    ("solar", ("光伏",)),
    ("power_grid", ("电网", "电力设备", "智能电网")),
    ("power", ("电力", "绿电")),
    ("energy_storage_battery", ("储能", "电池")),
    ("cpo", ("CPO", "通信", "光模块")),
    ("cloud_computing", ("云计算", "算力", "大数据")),
    ("coal", ("煤炭",)),
    ("oil_gas", ("石油", "油气")),
    ("commodity", ("商品", "有色", "稀有金属", "稀土", "豆粕", "能源化工", "金属")),
    ("medical_device", ("医疗器械",)),
    ("innovative_drug", ("创新药", "生物医药", "CXO")),
    ("pharma", ("医药",)),
    ("medical", ("医疗",)),
    ("agriculture", ("农业", "养殖", "畜牧")),
    ("chemical", ("化工",)),
    ("bank", ("银行",)),
    ("broker", ("券商", "证券",)),
    ("military", ("军工", "国防",)),
    ("cross_border_hk", ("恒生", "恒指", "港股", "港股通", "H股")),
    ("cross_border_us", ("纳斯达克", "纳指", "标普", "道琼斯", "美国", "美股", "中概")),
    ("cross_border_jp", ("日经", "日本", "东证")),
    ("cross_border_eu", ("德国", "法国", "欧洲", "欧盟")),
    ("cross_border_global", ("沙特", "东南亚", "亚太", "全球", "海外", "印度", "越南")),
    ("broad_market", ("沪深300", "中证500", "中证800", "中证1000", "上证50", "A50", "央企", "红利价值", "深证100", "中证2000", "中证A500")),
    ("growth_index", ("创业板", "科创", "成长", "双创")),
]


def _load_tag_overrides(path: Path = OVERRIDES_PATH) -> dict[str, str]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    overrides: dict[str, str] = {}
    for row in rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        tag = str(row.get("tag", "")).strip()
        if symbol and tag:
            overrides[symbol] = tag
    return overrides


def _infer_tag(symbol: str, name: str, overrides: dict[str, str]) -> str:
    override = overrides.get(symbol.strip().upper())
    if override:
        return override

    normalized_name = str(name or "").strip()
    for tag, patterns in TAG_RULES:
        if any(pattern in normalized_name for pattern in patterns):
            return tag
    return "other"


def _is_candidate_name(name: str) -> bool:
    normalized = str(name or "").strip()
    return bool(normalized) and bool(ETF_INCLUDE_PATTERN.search(normalized)) and not any(
        pattern.upper() in normalized.upper() for pattern in EXCLUDE_NAME_PATTERNS
    )


def _load_candidate_funds() -> pl.DataFrame:
    ts.set_token(os.environ.get("TUSHARE_TOKEN", ""))
    pro = ts.pro_api()
    df_pd = pro.fund_basic(
        market="E",
        status="L",
        fields="ts_code,name,fund_type",
    )
    if df_pd is None or df_pd.empty:
        return pl.DataFrame(schema={"symbol": pl.Utf8, "name": pl.Utf8, "fund_type": pl.Utf8})

    df = (
        pl.from_pandas(df_pd)
        .rename({"ts_code": "symbol"})
        .with_columns(
            [
                pl.col("symbol").cast(pl.Utf8),
                pl.col("name").cast(pl.Utf8),
                pl.col("fund_type").cast(pl.Utf8),
            ]
        )
        .filter(pl.col("name").map_elements(_is_candidate_name, return_dtype=pl.Boolean))
        .unique(subset=["symbol"], keep="first")
        .sort("symbol")
    )
    return df


def _load_liquidity_snapshot(symbols: list[str], lookback_days: int) -> pl.DataFrame:
    if not symbols:
        return pl.DataFrame(schema={"symbol": pl.Utf8, "avg_amount_60d": pl.Float64, "valid_days_60d": pl.UInt32})

    loader = TushareLoader()
    end = datetime.now(timezone.utc).strftime("%Y%m%d")
    start = (datetime.now(timezone.utc) - timedelta(days=lookback_days * 2 + 10)).strftime("%Y%m%d")
    trading_dates = loader.get_trading_dates("stock_CN", start, end)[-lookback_days:]
    frames: list[pl.DataFrame] = []
    for trade_date in trading_dates:
        frame = loader.fetch_daily_by_date("etf_CN", trade_date, symbols=symbols)
        if not frame.is_empty():
            frames.append(frame.select(["symbol", "amount"]))

    if not frames:
        return pl.DataFrame(schema={"symbol": pl.Utf8, "avg_amount_60d": pl.Float64, "valid_days_60d": pl.UInt32})

    return (
        pl.concat(frames)
        .group_by("symbol")
        .agg(
            [
                pl.col("amount").mean().alias("avg_amount_60d"),
                pl.len().alias("valid_days_60d"),
            ]
        )
        .with_columns(
            [
                pl.col("avg_amount_60d").cast(pl.Float64),
                pl.col("valid_days_60d").cast(pl.UInt32),
            ]
        )
    )


def build_etf_cn_universe_rows(
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    min_avg_amount: float = DEFAULT_MIN_AVG_AMOUNT,
    min_valid_days: int = DEFAULT_MIN_VALID_DAYS,
    overrides_path: Path = OVERRIDES_PATH,
) -> tuple[list[dict[str, str]], dict[str, object]]:
    candidates = _load_candidate_funds()
    candidate_symbols = candidates.get_column("symbol").to_list() if not candidates.is_empty() else []
    liquidity = _load_liquidity_snapshot(candidate_symbols, lookback_days)
    overrides = _load_tag_overrides(overrides_path)

    merged = candidates.join(liquidity, on="symbol", how="left").with_columns(
        [
            pl.col("avg_amount_60d").fill_null(0.0),
            pl.col("valid_days_60d").fill_null(0),
        ]
    )

    selected = merged.filter(
        (pl.col("avg_amount_60d") >= min_avg_amount) & (pl.col("valid_days_60d") >= min_valid_days)
    )

    rows: list[dict[str, str]] = []
    tag_counts: Counter[str] = Counter()
    for row in selected.sort(["symbol"]).iter_rows(named=True):
        tag = _infer_tag(row["symbol"], row["name"], overrides)
        tag_counts[tag] += 1
        rows.append(
            {
                "symbol": str(row["symbol"]),
                "name": str(row["name"]),
                "is_active": "true",
                "tag": tag,
            }
        )

    filtered_out = merged.filter(
        (pl.col("avg_amount_60d") < min_avg_amount) | (pl.col("valid_days_60d") < min_valid_days)
    )
    summary: dict[str, object] = {
        "candidate_count": candidates.height,
        "selected_count": len(rows),
        "filtered_out_count": filtered_out.height,
        "tag_counts": dict(sorted(tag_counts.items())),
        "thresholds": {
            "lookback_days": lookback_days,
            "min_avg_amount": min_avg_amount,
            "min_valid_days": min_valid_days,
        },
    }
    return rows, summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate etf_CN universe from Tushare")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS, help="流动性回看交易日数，默认 60")
    parser.add_argument("--min-avg-amount", type=float, default=DEFAULT_MIN_AVG_AMOUNT, help="近窗口日均成交额阈值，默认 300000")
    parser.add_argument("--min-valid-days", type=int, default=DEFAULT_MIN_VALID_DAYS, help="近窗口最少有效交易日，默认 40")
    parser.add_argument("--dry-run", action="store_true", help="只输出摘要，不写入 etf_CN universe")
    args = parser.parse_args()

    rows, summary = build_etf_cn_universe_rows(
        lookback_days=args.lookback_days,
        min_avg_amount=args.min_avg_amount,
        min_valid_days=args.min_valid_days,
    )

    if not args.dry_run:
        write_pipeline_universe_rows("etf_CN", rows)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

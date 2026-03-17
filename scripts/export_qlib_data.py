"""
将 TimescaleDB market.daily 数据导出为 Qlib 所需的二进制格式

目录结构：
  qlib_data/
    calendars/day.txt          — 交易日历（每行一个日期）
    instruments/all.txt        — 股票列表（代码 起始日 结束日）
    features/<instrument>/     — 每字段一个 .day.bin 文件（float32 numpy array）

使用方法：
  docker compose exec app python scripts/export_qlib_data.py
  docker compose exec app python scripts/export_qlib_data.py --symbols 000001.SZ 600000.SH
"""
from __future__ import annotations

import argparse
import struct
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger
from sqlalchemy import text

# 路径放在 /app/qlib_data 下的子目录，避免覆盖已有数据
QLIB_DATA_DIR = Path("/app/qlib_data")

# Qlib CN 字段映射：Qlib 字段名 → market.daily 列名
# Qlib 内部价格字段是 "调整后价格 / adj_factor"（即复权因子还原），
# 但对于回测只需要未复权价格 + factor 字段，Qlib 会自己处理
FIELD_MAP = {
    "open":   "open",
    "high":   "high",
    "low":    "low",
    "close":  "close",
    "volume": "volume",
    "factor": "adj_factor",   # Qlib 用 factor 做复权
    "change": "pct_change",
}


def to_qlib_code(symbol: str) -> str:
    """'000001.SZ' → 'SZ000001'"""
    code, exchange = symbol.split(".")
    return f"{exchange}{code}"


def write_bin(path: Path, values: np.ndarray) -> None:
    """写 Qlib float32 二进制文件（首4字节是 offset int32，后续是 float32 数组）"""
    path.parent.mkdir(parents=True, exist_ok=True)
    arr = values.astype(np.float32)
    with open(path, "wb") as f:
        # Qlib bin 格式：首4字节是 calendar offset（这里写0，初始化后 Qlib 会自行对齐）
        f.write(struct.pack("<I", 0))
        f.write(arr.tobytes())


def export(symbols: list[str] | None = None) -> None:
    from app.utils.db import get_engine

    engine = get_engine()

    # ── 1. 拉取数据 ──────────────────────────────────────────
    symbol_filter = ""
    if symbols:
        quoted = ", ".join(f"'{s}'" for s in symbols)
        symbol_filter = f"AND symbol IN ({quoted})"

    query = text(f"""
        SELECT
            time AT TIME ZONE 'UTC' AS time,
            symbol,
            open, high, low, close,
            volume, adj_factor, pct_change
        FROM market.daily
        WHERE open IS NOT NULL
          {symbol_filter}
        ORDER BY symbol, time
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, parse_dates=["time"])

    if df.empty:
        logger.error("market.daily 无数据，请先运行数据拉取流水线")
        return

    df["time"] = df["time"].dt.tz_localize(None).dt.normalize()
    df["adj_factor"] = df["adj_factor"].astype(float).fillna(1.0)
    df["pct_change"] = df["pct_change"].astype(float).fillna(0.0)

    all_symbols = df["symbol"].unique().tolist()
    logger.info(f"导出 {len(all_symbols)} 只股票，共 {len(df)} 条记录")

    # ── 2. 写交易日历 ─────────────────────────────────────────
    cal_path = QLIB_DATA_DIR / "calendars" / "day.txt"
    cal_path.parent.mkdir(parents=True, exist_ok=True)

    all_dates = sorted(df["time"].unique())
    with open(cal_path, "w") as f:
        for d in all_dates:
            f.write(pd.Timestamp(d).strftime("%Y-%m-%d") + "\n")
    logger.info(f"日历: {all_dates[0].date()} ~ {all_dates[-1].date()}，{len(all_dates)} 个交易日 → {cal_path}")

    # ── 3. 写 instruments/all.txt ────────────────────────────
    inst_path = QLIB_DATA_DIR / "instruments" / "all.txt"
    inst_path.parent.mkdir(parents=True, exist_ok=True)

    with open(inst_path, "w") as f:
        for sym in all_symbols:
            sub = df[df["symbol"] == sym]["time"]
            start = sub.min().strftime("%Y-%m-%d")
            end   = sub.max().strftime("%Y-%m-%d")
            qlib_code = to_qlib_code(sym)
            f.write(f"{qlib_code}\t{start}\t{end}\n")
    logger.info(f"instruments/all.txt → {len(all_symbols)} 条")

    # ── 4. 写 features/<instrument>/<field>.day.bin ──────────
    date_to_idx = {d: i for i, d in enumerate(all_dates)}

    for sym in all_symbols:
        sub = df[df["symbol"] == sym].set_index("time").sort_index()
        qlib_code = to_qlib_code(sym)
        feat_dir = QLIB_DATA_DIR / "features" / qlib_code.lower()

        # 对齐到完整日历（缺失日期填 NaN）
        full_index = pd.DatetimeIndex(all_dates)
        sub = sub.reindex(full_index)

        for qlib_field, db_col in FIELD_MAP.items():
            values = sub[db_col].values.astype(np.float64)
            bin_path = feat_dir / f"{qlib_field}.day.bin"
            write_bin(bin_path, values)

        logger.info(f"  {qlib_code}: {len(sub)} 行 × {len(FIELD_MAP)} 字段 → {feat_dir}")

    logger.success(f"Qlib 数据导出完成 → {QLIB_DATA_DIR}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export TimescaleDB data to Qlib format")
    parser.add_argument("--symbols", nargs="*", help="股票代码列表，如 000001.SZ 600000.SH；留空导出全部")
    args = parser.parse_args()
    export(symbols=args.symbols)

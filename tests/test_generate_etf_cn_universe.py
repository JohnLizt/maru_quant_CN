from __future__ import annotations

from pathlib import Path

import polars as pl

from scripts.generate_etf_cn_universe import (
    _infer_tag,
    _is_candidate_name,
    build_etf_cn_universe_rows,
)


def test_candidate_name_filter_excludes_lof_and_reit() -> None:
    assert _is_candidate_name("沪深300ETF")
    assert not _is_candidate_name("标普500LOF")
    assert not _is_candidate_name("华夏华电清洁能源REIT")


def test_infer_tag_covers_cross_border_and_bond() -> None:
    overrides = {"515880.SH": "cpo"}

    assert _infer_tag("159513.SZ", "纳斯达克100ETF大成", overrides) == "cross_border_us"
    assert _infer_tag("513000.SH", "日经225ETF易方达", overrides) == "cross_border_jp"
    assert _infer_tag("511010.SH", "国债ETF", overrides) == "bond"
    assert _infer_tag("515880.SH", "通信ETF国泰", overrides) == "cpo"


def test_build_universe_rows_applies_liquidity_filter_and_tagging(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "scripts.generate_etf_cn_universe._load_candidate_funds",
        lambda: pl.DataFrame(
            [
                {"symbol": "510300.SH", "name": "沪深300ETF", "fund_type": "股票型"},
                {"symbol": "159513.SZ", "name": "纳斯达克100ETF大成", "fund_type": "股票型"},
                {"symbol": "511010.SH", "name": "国债ETF", "fund_type": "债券型"},
                {"symbol": "159001.SZ", "name": "冷门ETF", "fund_type": "股票型"},
            ]
        ),
    )
    monkeypatch.setattr(
        "scripts.generate_etf_cn_universe._load_liquidity_snapshot",
        lambda symbols, lookback_days: pl.DataFrame(
            [
                {"symbol": "510300.SH", "avg_amount_60d": 30_000_000.0, "valid_days_60d": 60},
                {"symbol": "159513.SZ", "avg_amount_60d": 20_000_000.0, "valid_days_60d": 55},
                {"symbol": "511010.SH", "avg_amount_60d": 11_000_000.0, "valid_days_60d": 45},
                {"symbol": "159001.SZ", "avg_amount_60d": 1_000_000.0, "valid_days_60d": 60},
            ]
        ),
    )
    overrides = tmp_path / "overrides.csv"
    overrides.write_text("symbol,tag\n", encoding="utf-8")

    rows, summary = build_etf_cn_universe_rows(
        lookback_days=60,
        min_avg_amount=10_000_000.0,
        min_valid_days=40,
        overrides_path=overrides,
    )

    assert [row["symbol"] for row in rows] == ["159513.SZ", "510300.SH", "511010.SH"]
    assert [row["tag"] for row in rows] == ["cross_border_us", "broad_market", "bond"]
    assert summary["candidate_count"] == 4
    assert summary["selected_count"] == 3
    assert summary["filtered_out_count"] == 1

"""Yahoo Finance market data loader."""
from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import polars as pl

from app.data_loader.base import LoaderCapabilities


CALENDAR_SYMBOL = "SPY"
SUPPORTED_ASSET_TYPES = {"stock_US", "etf_US"}
MAX_ATTEMPTS = 2
RETRY_WAIT_SECONDS = 2


def _empty_daily_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "time": pl.Datetime("us", "UTC"),
            "asset_type": pl.Utf8,
            "symbol": pl.Utf8,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Int64,
            "amount": pl.Float64,
            "pct_change": pl.Float64,
            "is_suspended": pl.Boolean,
            "data_source": pl.Utf8,
        }
    )


def _extract_symbol_frame(df_pd: pd.DataFrame, symbol: str) -> pd.DataFrame:
    frame = df_pd.copy()
    if isinstance(frame.columns, pd.MultiIndex):
        tickers = frame.columns.get_level_values(-1)
        if symbol in tickers:
            frame = frame.xs(symbol, axis=1, level=-1)
        elif len(set(tickers)) == 1:
            frame = frame.droplevel(-1, axis=1)
    return frame


def _build_daily_frame(df_pd: pd.DataFrame, asset_type: str, symbol: str) -> pl.DataFrame:
    if df_pd.empty:
        return _empty_daily_frame()

    frame = _extract_symbol_frame(df_pd, symbol).reset_index()
    if "Date" in frame.columns:
        frame = frame.rename(columns={"Date": "time"})
    elif "Datetime" in frame.columns:
        frame = frame.rename(columns={"Datetime": "time"})
    elif frame.columns[0] != "time":
        frame = frame.rename(columns={frame.columns[0]: "time"})

    normalized_columns = {column: str(column).strip().lower().replace(" ", "_") for column in frame.columns}
    frame = frame.rename(columns=normalized_columns)
    if "adj_close" in frame.columns:
        pct_source = pd.to_numeric(frame["adj_close"], errors="coerce")
    else:
        pct_source = pd.to_numeric(frame.get("close"), errors="coerce")

    frame["time"] = pd.to_datetime(frame["time"], utc=True)
    frame["open"] = pd.to_numeric(frame.get("open"), errors="coerce")
    frame["high"] = pd.to_numeric(frame.get("high"), errors="coerce")
    frame["low"] = pd.to_numeric(frame.get("low"), errors="coerce")
    frame["close"] = pd.to_numeric(frame.get("close"), errors="coerce")
    frame["volume"] = pd.to_numeric(frame.get("volume"), errors="coerce").fillna(0)
    frame["amount"] = frame["close"] * frame["volume"]
    frame["pct_change"] = pct_source.pct_change() * 100.0
    frame["symbol"] = symbol.upper()
    frame["asset_type"] = asset_type
    frame["is_suspended"] = False
    frame["data_source"] = "yahoo"

    frame = frame.dropna(subset=["close"]).copy()
    if frame.empty:
        return _empty_daily_frame()

    return (
        pl.from_pandas(frame)
        .with_columns(
            [
                pl.col("time").cast(pl.Datetime("us", "UTC")),
                pl.col("open").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("volume").round(0).cast(pl.Int64),
                pl.col("amount").cast(pl.Float64),
                pl.col("pct_change").cast(pl.Float64),
                pl.col("symbol").cast(pl.Utf8),
                pl.col("asset_type").cast(pl.Utf8),
                pl.col("data_source").cast(pl.Utf8),
                pl.col("is_suspended").cast(pl.Boolean),
            ]
        )
        .select(
            [
                "time",
                "asset_type",
                "symbol",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "pct_change",
                "is_suspended",
                "data_source",
            ]
        )
    )


class YahooLoader:
    source_name = "yahoo"

    def supports(self, asset_type: str) -> bool:
        return asset_type in SUPPORTED_ASSET_TYPES

    def get_capabilities(self, asset_type: str) -> LoaderCapabilities:
        self._ensure_supported(asset_type)
        return LoaderCapabilities(
            supports_by_date=True,
            supports_by_symbol=True,
            supports_suspended_status=False,
        )

    def get_trading_dates(self, asset_type: str, start: str, end: str) -> list[str]:
        self._ensure_supported(asset_type)
        end_exclusive = (datetime.strptime(end, "%Y%m%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        df_pd = self._download_history(
            CALENDAR_SYMBOL,
            start=self._format_date(start),
            end=end_exclusive,
        )
        daily = _build_daily_frame(df_pd, asset_type, CALENDAR_SYMBOL)
        if daily.is_empty():
            return []
        return sorted(daily.get_column("time").dt.strftime("%Y%m%d").unique().to_list())

    def fetch_daily_by_date(
        self,
        asset_type: str,
        trade_date: str,
        symbols: list[str] | None = None,
    ) -> pl.DataFrame:
        self._ensure_supported(asset_type)
        normalized_symbols = [str(symbol).strip().upper() for symbol in (symbols or []) if str(symbol).strip()]
        if not normalized_symbols:
            raise ValueError("Yahoo loader 按日抓取必须提供 symbols")

        daily_frames: list[pl.DataFrame] = []
        for symbol in normalized_symbols:
            frame = self.fetch_daily_by_symbol(asset_type, symbol, trade_date, trade_date)
            if frame.is_empty():
                continue
            filtered = frame.filter(pl.col("time").dt.strftime("%Y%m%d") == trade_date)
            if not filtered.is_empty():
                daily_frames.append(filtered)

        if not daily_frames:
            return _empty_daily_frame()
        return pl.concat(daily_frames, how="vertical").sort(["time", "symbol"])

    def fetch_daily_by_symbol(
        self,
        asset_type: str,
        symbol: str,
        start: str,
        end: str,
    ) -> pl.DataFrame:
        self._ensure_supported(asset_type)
        end_exclusive = (datetime.strptime(end, "%Y%m%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        df_pd = self._download_history(
            symbol.upper(),
            start=self._format_date(start),
            end=end_exclusive,
        )
        return _build_daily_frame(df_pd, asset_type, symbol.upper())

    def _download_history(self, symbol: str, *, start: str, end: str) -> pd.DataFrame:
        yf = self._get_yfinance()
        last_exc: Exception | None = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                return yf.download(
                    symbol,
                    start=start,
                    end=end,
                    auto_adjust=False,
                    progress=False,
                    actions=False,
                    threads=False,
                )
            except Exception as exc:
                last_exc = exc
                if attempt >= MAX_ATTEMPTS:
                    raise
                time.sleep(RETRY_WAIT_SECONDS)
        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"Yahoo 历史行情下载失败: {symbol}")

    @staticmethod
    def _format_date(yyyymmdd: str) -> str:
        return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"

    @staticmethod
    def _get_yfinance() -> Any:
        try:
            import yfinance as yf
        except ImportError as exc:
            raise ImportError("请先安装 yfinance 以启用 Yahoo Finance loader") from exc
        return yf

    def _ensure_supported(self, asset_type: str) -> None:
        if not self.supports(asset_type):
            raise ValueError(f"Yahoo 暂不支持 asset_type={asset_type}")

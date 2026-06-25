"""Tushare market data loader."""
from __future__ import annotations

import os

import polars as pl
import tushare as ts

from app.data_loader.base import LoaderCapabilities


CAL_SYMBOL = "000001.SZ"
STOCK_FIELDS = "ts_code,trade_date,open,high,low,close,pct_chg,vol,amount"
ETF_FIELDS = "ts_code,trade_date,open,high,low,close,pct_chg,vol,amount"
SUPPORTED_ASSET_TYPES = {"stock_CN", "etf_CN"}
SUSPEND_TYPES = {"S"}


def _build_daily_frame(df_pd, asset_type: str) -> pl.DataFrame:
    return (
        pl.from_pandas(df_pd)
        .rename(
            {
                "ts_code": "symbol",
                "trade_date": "time",
                "pct_chg": "pct_change",
                "vol": "volume",
            }
        )
        .with_columns(
            pl.col("time").str.strptime(pl.Date, "%Y%m%d").cast(pl.Datetime("us", "UTC"))
        )
        .with_columns(
            [
                pl.col("open").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("pct_change").cast(pl.Float64),
                pl.col("volume").cast(pl.Int64),
                pl.col("amount").cast(pl.Float64),
                pl.lit(asset_type).alias("asset_type"),
                pl.lit("tushare").alias("data_source"),
                pl.lit(False).alias("is_suspended"),
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


class TushareLoader:
    source_name = "tushare"

    def __init__(self) -> None:
        token = os.environ.get("TUSHARE_TOKEN", "")
        ts.set_token(token)
        self._pro = ts.pro_api()

    def supports(self, asset_type: str) -> bool:
        return asset_type in SUPPORTED_ASSET_TYPES

    def get_capabilities(self, asset_type: str) -> LoaderCapabilities:
        self._ensure_supported(asset_type)
        if asset_type == "stock_CN":
            return LoaderCapabilities(
                supports_by_date=True,
                supports_by_symbol=True,
                supports_suspended_status=True,
            )
        return LoaderCapabilities(
            supports_by_date=True,
            supports_by_symbol=True,
            supports_suspended_status=False,
        )

    def get_trading_dates(self, asset_type: str, start: str, end: str) -> list[str]:
        self._ensure_supported(asset_type)
        df = self._pro.daily(ts_code=CAL_SYMBOL, start_date=start, end_date=end, fields="trade_date")
        return sorted(df["trade_date"].tolist())

    def fetch_daily_by_date(
        self,
        asset_type: str,
        trade_date: str,
        symbols: list[str] | None = None,
    ) -> pl.DataFrame:
        self._ensure_supported(asset_type)
        df_pd = self._fetch_frame(asset_type, trade_date=trade_date)
        if df_pd is None or df_pd.empty:
            return self._empty_daily_frame()

        if symbols:
            df_pd = df_pd[df_pd["ts_code"].isin(symbols)]
        if df_pd.empty:
            return self._empty_daily_frame()
        return _build_daily_frame(df_pd, asset_type)

    def fetch_daily_by_symbol(
        self,
        asset_type: str,
        symbol: str,
        start: str,
        end: str,
    ) -> pl.DataFrame:
        self._ensure_supported(asset_type)
        df_pd = self._fetch_frame(asset_type, ts_code=symbol, start_date=start, end_date=end)
        if df_pd is None or df_pd.empty:
            return self._empty_daily_frame()
        return _build_daily_frame(df_pd, asset_type)

    def get_suspended_symbols(self, trade_date: str, symbols: list[str]) -> set[str]:
        df = self._pro.suspend_d(ts_code="", trade_date=trade_date)
        if df is None or df.empty:
            return set()
        df = df[df["ts_code"].isin(symbols) & df["suspend_type"].isin(SUSPEND_TYPES)]
        return set(df["ts_code"].tolist())

    def _ensure_supported(self, asset_type: str) -> None:
        if not self.supports(asset_type):
            raise ValueError(f"Tushare 暂不支持 asset_type={asset_type}")

    def _fetch_frame(self, asset_type: str, **kwargs):
        if asset_type == "stock_CN":
            df = self._pro.daily(fields=STOCK_FIELDS, **kwargs)
        elif asset_type == "etf_CN":
            df = self._pro.fund_daily(fields=ETF_FIELDS, **kwargs)
        else:
            raise ValueError(f"Tushare 暂不支持 asset_type={asset_type}")
        return df

    @staticmethod
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

"""ETF universe rotation strategy based on composite signal ranking."""
from __future__ import annotations

import json

import polars as pl

from app.strategy.base import BaseStrategy


class ETFUniverseRotationStrategy(BaseStrategy):
    """Pick the top ranked ETF names from the full etf_CN universe."""

    name = "etf_rotation_v1"
    asset_type = "etf_CN"

    def __init__(self, top_n: int = 5, profile_name: str = "trend_v1") -> None:
        if top_n <= 0:
            raise ValueError("top_n 必须大于 0")
        self.top_n = top_n
        self.profile_name = profile_name

    def generate_signals(
        self,
        factors: pl.DataFrame,
        universe: list[str] | None = None,
    ) -> pl.DataFrame:
        if factors.is_empty():
            return pl.DataFrame(
                schema={
                    "time": pl.Datetime("us", "UTC"),
                    "symbol": pl.Utf8,
                    "strategy": pl.Utf8,
                    "signal": pl.Int64,
                    "score": pl.Float64,
                    "metadata": pl.Utf8,
                }
            )

        required = {"time", "symbol", "composite_score"}
        missing = required - set(factors.columns)
        if missing:
            raise ValueError(f"ETF 轮动策略缺少列: {sorted(missing)}")

        df = factors
        if "asset_type" in df.columns:
            df = df.filter(pl.col("asset_type") == self.asset_type)
        if universe:
            df = df.filter(pl.col("symbol").is_in(universe))
        if df.is_empty():
            return pl.DataFrame(
                schema={
                    "time": pl.Datetime("us", "UTC"),
                    "symbol": pl.Utf8,
                    "strategy": pl.Utf8,
                    "signal": pl.Int64,
                    "score": pl.Float64,
                    "metadata": pl.Utf8,
                }
            )

        ranked = (
            df.sort(["time", "composite_score", "symbol"], descending=[False, True, False])
            .with_columns(pl.col("composite_score").rank(method="ordinal", descending=True).over("time").alias("rank"))
            .filter(pl.col("rank") <= self.top_n)
        )

        def _metadata(row: dict[str, object]) -> str:
            return json.dumps(
                {
                    "rank": int(row["rank"]),
                    "tag": str(row.get("tag", "") or ""),
                    "profile": self.profile_name,
                },
                ensure_ascii=False,
            )

        return (
            ranked.with_columns([
                pl.lit(self.name).alias("strategy"),
                pl.lit(1).alias("signal"),
                pl.col("composite_score").alias("score"),
                pl.struct(["rank", "tag"]).map_elements(_metadata, return_dtype=pl.Utf8).alias("metadata"),
            ])
            .select(["time", "symbol", "strategy", "signal", "score", "metadata"])
        )

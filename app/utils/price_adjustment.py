"""Helpers for applying persisted total-return adjustment factors."""
from __future__ import annotations

from collections.abc import Iterable

import polars as pl


DEFAULT_PRICE_COLUMNS = ("open", "high", "low", "close", "ohlc4")


def apply_price_adjustment(
    frame: pl.DataFrame,
    *,
    price_columns: Iterable[str] = DEFAULT_PRICE_COLUMNS,
    drop_factor: bool = True,
) -> pl.DataFrame:
    """Return a frame whose price columns are multiplied by a valid adj_factor."""
    if frame.is_empty() or "adj_factor" not in frame.columns:
        return frame

    adjusted = frame.with_columns(
        pl.when(
            pl.col("adj_factor").cast(pl.Float64).is_finite()
            & (pl.col("adj_factor").cast(pl.Float64) > 0)
        )
        .then(pl.col("adj_factor").cast(pl.Float64))
        .otherwise(1.0)
        .alias("_valid_adj_factor")
    )
    available_columns = [column for column in price_columns if column in adjusted.columns]
    if available_columns:
        adjusted = adjusted.with_columns(
            [
                (pl.col(column).cast(pl.Float64) * pl.col("_valid_adj_factor")).alias(column)
                for column in available_columns
            ]
        )

    drop_columns = ["_valid_adj_factor"]
    if drop_factor:
        drop_columns.append("adj_factor")
    return adjusted.drop(drop_columns)

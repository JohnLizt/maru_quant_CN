"""Composite score helpers."""
from __future__ import annotations

from typing import Any

import polars as pl

from app.signals.profiles import SignalProfile


def _build_contributors(row: dict[str, Any]) -> list[str]:
    contributors: list[str] = []

    ma_cross_score = float(row.get("ma_cross_score") or 0.0)
    price_score = float(row.get("price_to_ma20_score") or 0.0)
    rsi_score = float(row.get("rsi14_score") or 0.0)
    raw_rsi = float(row.get("rsi14") or 0.0)

    if ma_cross_score >= 0.5:
        contributors.append("trend_structure_strong")
    elif ma_cross_score <= -0.5:
        contributors.append("trend_structure_weak")

    if price_score >= 0.5:
        contributors.append("price_above_ma20")
    elif price_score <= -0.5:
        contributors.append("price_below_ma20")

    if rsi_score >= 0.5:
        contributors.append("rsi_in_healthy_trend_zone")
    elif raw_rsi > 85.0:
        contributors.append("rsi_overheated")
    elif raw_rsi < 35.0:
        contributors.append("rsi_weak")

    if not contributors:
        contributors.append("mixed_signal")

    return contributors


def _label_expr(score_column: str = "composite_score") -> pl.Expr:
    return (
        pl.when(pl.col(score_column) >= 0.6)
        .then(pl.lit("strong"))
        .when(pl.col(score_column) >= 0.2)
        .then(pl.lit("positive"))
        .when(pl.col(score_column) > -0.2)
        .then(pl.lit("neutral"))
        .when(pl.col(score_column) > -0.6)
        .then(pl.lit("weak"))
        .otherwise(pl.lit("very_weak"))
        .alias("label")
    )


def apply_composite_score(df: pl.DataFrame, profile: SignalProfile) -> pl.DataFrame:
    """Compute composite score, label, and contributor tags."""

    weight_sum = sum(rule.weight for rule in profile.factor_rules)
    if weight_sum <= 0:
        raise ValueError(f"profile={profile.name} 的权重和必须大于 0")

    weighted_sum = sum(pl.col(f"{rule.factor_name}_score") * rule.weight for rule in profile.factor_rules)
    required_score_columns = [f"{rule.factor_name}_score" for rule in profile.factor_rules]

    return (
        df.with_columns((weighted_sum / weight_sum).alias("composite_score"))
        .with_columns(_label_expr())
        .with_columns(
            pl.struct([*profile.factor_names, *required_score_columns])
            .map_elements(_build_contributors, return_dtype=pl.List(pl.Utf8))
            .alias("contributors")
        )
    )

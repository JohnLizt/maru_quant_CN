"""Normalization helpers for composite signal scoring."""
from __future__ import annotations

import polars as pl

from app.signals.profiles import FactorScoreRule, PiecewiseSegment, SignalProfile


def _linear_interpolation_expr(value_expr: pl.Expr, segment: PiecewiseSegment) -> pl.Expr:
    slope = (segment.end_score - segment.start_score) / (segment.end - segment.start)
    return pl.lit(segment.start_score) + ((value_expr - segment.start) * slope)


def _linear_clip_expr(column: str, rule: FactorScoreRule) -> pl.Expr:
    if rule.clip_lower is None or rule.clip_upper is None:
        raise ValueError(f"{rule.factor_name} 缺少 linear_clip 参数")
    if rule.clip_lower >= rule.clip_upper:
        raise ValueError(f"{rule.factor_name} 的 clip_lower 必须小于 clip_upper")

    clipped = pl.col(column).clip(rule.clip_lower, rule.clip_upper)
    scaled = ((clipped - rule.clip_lower) / (rule.clip_upper - rule.clip_lower)) * 2.0 - 1.0
    return scaled if rule.higher_better else -scaled


def _rank_to_unit_expr(column: str, rule: FactorScoreRule) -> pl.Expr:
    rank_expr = pl.col(column).rank(method="average").over("time")
    count_expr = pl.col(column).count().over("time")
    score = pl.when(count_expr <= 1).then(0.0).otherwise(((rank_expr - 1.0) / (count_expr - 1.0)) * 2.0 - 1.0)
    return score if rule.higher_better else -score


def _piecewise_expr(column: str, rule: FactorScoreRule) -> pl.Expr:
    if not rule.segments:
        raise ValueError(f"{rule.factor_name} 缺少 piecewise segments")
    if rule.left_score is None or rule.right_score is None:
        raise ValueError(f"{rule.factor_name} 缺少 piecewise 边界分数")

    value_expr = pl.col(column)
    first_segment = rule.segments[0]
    last_segment = rule.segments[-1]

    expr = pl.when(value_expr.is_null()).then(None)
    expr = expr.when(value_expr < first_segment.start).then(rule.left_score)

    for segment in rule.segments:
        expr = expr.when((value_expr >= segment.start) & (value_expr <= segment.end)).then(
            _linear_interpolation_expr(value_expr, segment)
        )

    expr = expr.when(value_expr > last_segment.end).then(rule.right_score).otherwise(None)
    return expr if rule.higher_better else -expr


def build_score_expr(column: str, rule: FactorScoreRule) -> pl.Expr:
    """Create a Polars expression for factor score normalization."""

    if rule.method == "linear_clip":
        return _linear_clip_expr(column, rule).alias(f"{column}_score")
    if rule.method == "rank_to_unit":
        return _rank_to_unit_expr(column, rule).alias(f"{column}_score")
    if rule.method == "piecewise":
        return _piecewise_expr(column, rule).alias(f"{column}_score")
    raise ValueError(f"不支持的标准化方法: {rule.method}")


def apply_signal_profile(df: pl.DataFrame, profile: SignalProfile) -> pl.DataFrame:
    """Apply all normalization rules in the given signal profile."""

    expressions = [build_score_expr(rule.factor_name, rule) for rule in profile.factor_rules]
    return df.with_columns(expressions)

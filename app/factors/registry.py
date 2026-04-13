"""
因子注册表与解析工具。
"""
from __future__ import annotations

from app.factors.base import BaseFactor
from app.factors.cross_sectional.cross_sectional import LimitUpFactor
from app.factors.technical import MACDNormFactor, MACrossGactor, PriceToMA20Factor, RSIFactor


DEFAULT_FACTORS: list[BaseFactor] = [
    PriceToMA20Factor(),
    MACrossGactor(),
    RSIFactor(),
    MACDNormFactor(),
    LimitUpFactor(),
]

FACTOR_REGISTRY: dict[str, BaseFactor] = {factor.name: factor for factor in DEFAULT_FACTORS}


def resolve_factors(factor_names: list[str] | None = None) -> list[BaseFactor]:
    """解析用户指定的因子列表；未指定时返回默认全部因子。"""
    if not factor_names:
        return DEFAULT_FACTORS

    unknown = [name for name in factor_names if name not in FACTOR_REGISTRY]
    if unknown:
        raise ValueError(f"未知因子: {unknown}，可用: {list(FACTOR_REGISTRY)}")
    return [FACTOR_REGISTRY[name] for name in factor_names]


def max_warmup_days(factors: list[BaseFactor]) -> int:
    """返回本次运行所需的最大 warm-up 窗口。"""
    return max((factor.warmup_days for factor in factors), default=0)


def required_market_fields(factors: list[BaseFactor]) -> set[str]:
    """返回本次运行所需的 market.daily 字段集合。"""
    fields = {"time", "symbol", "is_suspended"}
    for factor in factors:
        fields.update(factor.required_fields)
    return fields

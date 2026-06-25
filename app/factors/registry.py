"""
因子注册表与解析工具。
"""
from __future__ import annotations

from app.factors.base import BaseFactor, CrossSectionalFactor, TimeSeriesFactor
from app.factors.momentum import (
    MomentumReg20Factor,
    MomentumReg20RankFactor,
    Ret10Factor,
    Ret10RankFactor,
    Ret20Factor,
    Ret20RankFactor,
    Ret30Factor,
    Ret30RankFactor,
    Ret60Factor,
    Ret60RankFactor,
)
from app.factors.risk import CVFactor, StdScoreFactor
from app.factors.technical import (
    MACDNormFactor,
    MACrossGactor,
    LimitUpFactor,
    PriceToMA20Factor,
    RSIFactor,
)


DEFAULT_FACTORS: list[BaseFactor] = [
    PriceToMA20Factor(),
    MACrossGactor(),
    RSIFactor(),
    MACDNormFactor(),
    MomentumReg20Factor(),
    MomentumReg20RankFactor(),
    Ret10Factor(),
    Ret10RankFactor(),
    Ret20Factor(),
    Ret20RankFactor(),
    Ret30Factor(),
    Ret30RankFactor(),
    Ret60Factor(),
    Ret60RankFactor(),
    StdScoreFactor(),
    CVFactor(),
    LimitUpFactor(),
]

FACTOR_REGISTRY: dict[str, BaseFactor] = {factor.name: factor for factor in DEFAULT_FACTORS}


def factors_for_asset_type(asset_type: str) -> list[BaseFactor]:
    return [factor for factor in DEFAULT_FACTORS if factor.supports_asset_type(asset_type)]


def time_series_factors(factors: list[BaseFactor]) -> list[TimeSeriesFactor]:
    return [factor for factor in factors if isinstance(factor, TimeSeriesFactor)]


def cross_sectional_factors(factors: list[BaseFactor]) -> list[CrossSectionalFactor]:
    return [factor for factor in factors if isinstance(factor, CrossSectionalFactor)]


def resolve_factors(factor_names: list[str] | None = None, *, asset_type: str | None = None) -> list[BaseFactor]:
    """解析用户指定的因子列表；未指定时返回默认全部因子。"""
    available_factors = factors_for_asset_type(asset_type) if asset_type else DEFAULT_FACTORS
    available_names = {factor.name for factor in available_factors}

    if not factor_names:
        return available_factors

    unknown = [name for name in factor_names if name not in FACTOR_REGISTRY]
    if unknown:
        raise ValueError(f"未知因子: {unknown}，可用: {list(FACTOR_REGISTRY)}")

    if asset_type:
        unsupported = [name for name in factor_names if name not in available_names]
        if unsupported:
            raise ValueError(
                f"asset_type={asset_type} 不支持因子: {unsupported}，可用: {sorted(available_names)}"
            )

    return [FACTOR_REGISTRY[name] for name in factor_names if name in available_names or not asset_type]


def max_warmup_days(factors: list[BaseFactor]) -> int:
    """返回本次运行所需的最大 warm-up 窗口。"""
    return max((factor.warmup_days for factor in factors), default=0)


def required_market_fields(factors: list[BaseFactor]) -> set[str]:
    """返回本次运行所需的 market.daily 字段集合。"""
    fields = {"time", "symbol", "is_suspended"}
    for factor in factors:
        fields.update(factor.required_fields)
    return fields


def ic_min_cross_sections(factors: list[BaseFactor] | None = None) -> dict[str, int | None]:
    """返回因子名到 IC 最小截面样本阈值的映射。"""
    selected = factors if factors is not None else DEFAULT_FACTORS
    return {factor.name: factor.ic_min_cross_section for factor in selected}

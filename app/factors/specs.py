"""
因子元信息定义。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


FactorCategory = Literal["time_series", "cross_sectional"]
SuspendedPolicy = Literal["allow", "mask"]
AssetTypeName = Literal["stock_CN", "etf_CN", "stock_US"]


@dataclass(frozen=True)
class FactorSpec:
    """描述单个因子的元信息。"""

    name: str
    category: FactorCategory
    warmup_days: int
    suspended_policy: SuspendedPolicy = "allow"
    required_fields: tuple[str, ...] = ("open", "high", "low", "close", "volume")
    ic_min_cross_section: int | None = 20
    description: str = ""
    supported_asset_types: tuple[AssetTypeName, ...] = ("stock_CN", "etf_CN", "stock_US")

"""
因子元信息定义。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


FactorCategory = Literal["time_series", "cross_sectional"]
SuspendedPolicy = Literal["allow", "mask"]


@dataclass(frozen=True)
class FactorSpec:
    """描述单个因子的元信息。"""

    name: str
    category: FactorCategory
    warmup_days: int
    suspended_policy: SuspendedPolicy = "allow"
    required_fields: tuple[str, ...] = ("open", "high", "low", "close", "volume")
    description: str = ""

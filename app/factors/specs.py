"""Factor metadata loaded from the factor catalog table."""
from __future__ import annotations

import csv
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Literal


FactorCategory = Literal["time_series", "cross_sectional"]
SuspendedPolicy = Literal["allow", "mask"]
AssetTypeName = Literal["stock_CN", "etf_CN", "stock_US", "etf_US"]

_REPO_ROOT = Path(__file__).resolve().parents[2]
FACTOR_SPECS_CSV = Path(__file__).resolve().parent / "factor_specs.csv"
if not FACTOR_SPECS_CSV.exists():
    FACTOR_SPECS_CSV = _REPO_ROOT / "app" / "factors" / "factor_specs.csv"


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
    production_enabled: bool = True


def _parse_csv_list(raw: str) -> tuple[str, ...]:
    normalized = str(raw or "").strip()
    if not normalized:
        return tuple()
    return tuple(part.strip() for part in normalized.split("|") if part.strip())


def _parse_bool(raw: str | bool | None) -> bool:
    if isinstance(raw, bool):
        return raw
    normalized = str(raw or "").strip().lower()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    raise ValueError(f"非法布尔值: {raw!r}")


@lru_cache(maxsize=1)
def load_factor_specs(path: str | None = None) -> dict[str, FactorSpec]:
    csv_path = Path(path) if path else FACTOR_SPECS_CSV
    if not csv_path.exists():
        raise FileNotFoundError(f"因子配置表不存在: {csv_path}")

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    specs: dict[str, FactorSpec] = {}
    for row in rows:
        name = str(row.get("name", "")).strip()
        if not name:
            raise ValueError(f"因子配置缺少 name: {csv_path}")
        if name in specs:
            raise ValueError(f"因子配置存在重复 name: {name}")

        ic_min_cross_section_raw = str(row.get("ic_min_cross_section", "")).strip()
        ic_min_cross_section = int(ic_min_cross_section_raw) if ic_min_cross_section_raw else None

        specs[name] = FactorSpec(
            name=name,
            category=str(row.get("category", "")).strip(),  # type: ignore[arg-type]
            warmup_days=int(str(row.get("warmup_days", "")).strip()),
            suspended_policy=str(row.get("suspended_policy", "allow")).strip() or "allow",  # type: ignore[arg-type]
            required_fields=_parse_csv_list(str(row.get("required_fields", ""))),
            ic_min_cross_section=ic_min_cross_section,
            description=str(row.get("description", "")).strip(),
            supported_asset_types=_parse_csv_list(str(row.get("supported_asset_types", ""))),  # type: ignore[arg-type]
            production_enabled=_parse_bool(row.get("production_enabled")),
        )
    return specs


def get_factor_spec(name: str) -> FactorSpec:
    normalized = str(name or "").strip()
    if not normalized:
        raise ValueError("factor name 不能为空")
    specs = load_factor_specs()
    if normalized not in specs:
        raise ValueError(f"未知因子配置: {normalized}")
    return specs[normalized]

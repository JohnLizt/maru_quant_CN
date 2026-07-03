"""Signal scoring profiles."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


NormalizationMethod = Literal["linear_clip", "rank_to_unit", "piecewise"]
SignalMode = Literal["cross_sectional", "time_series"]


@dataclass(frozen=True)
class PiecewiseSegment:
    """Linear segment for piecewise normalization."""

    start: float
    end: float
    start_score: float
    end_score: float


@dataclass(frozen=True)
class FactorScoreRule:
    """Normalization + weight config for a single factor."""

    factor_name: str
    method: NormalizationMethod
    weight: float
    higher_better: bool = True
    clip_lower: float | None = None
    clip_upper: float | None = None
    left_score: float | None = None
    right_score: float | None = None
    segments: tuple[PiecewiseSegment, ...] = ()


@dataclass(frozen=True)
class SignalProfile:
    """Composite scoring profile."""

    name: str
    signal_mode: SignalMode
    normalization_scope: str
    factor_rules: tuple[FactorScoreRule, ...]
    supported_asset_types: tuple[str, ...] = ("stock_CN",)
    strong_threshold: float = 0.6
    positive_threshold: float = 0.2
    neutral_lower_threshold: float = -0.2
    weak_threshold: float = -0.6
    score_smoothing_window: int | None = None

    @property
    def factor_names(self) -> list[str]:
        return [rule.factor_name for rule in self.factor_rules]


TREND_V1_PROFILE = SignalProfile(
    name="trend_v1",
    signal_mode="time_series",
    normalization_scope="by_symbol",
    supported_asset_types=("stock_CN",),
    factor_rules=(
        FactorScoreRule(
            factor_name="momentum_reg_20",
            method="piecewise",
            weight=1.0,
            higher_better=True,
            left_score=-1.0,
            right_score=1.0,
            segments=(
                PiecewiseSegment(start=-1.0, end=-0.25, start_score=-1.0, end_score=-0.35),
                PiecewiseSegment(start=-0.25, end=0.0, start_score=-0.35, end_score=0.0),
                PiecewiseSegment(start=0.0, end=0.7, start_score=0.0, end_score=0.35),
                PiecewiseSegment(start=0.7, end=6.7, start_score=0.35, end_score=0.75),
                PiecewiseSegment(start=6.7, end=30.0, start_score=0.75, end_score=1.0),
            ),
        ),
    ),
)


TREND_ETF_MOMENTUM_REG20_PROFILE = SignalProfile(
    name="trend_etf_momentum_reg20",
    signal_mode="cross_sectional",
    normalization_scope="full_universe",
    supported_asset_types=("*",),
    factor_rules=(
        FactorScoreRule(
            factor_name="momentum_reg_20_rank",
            method="linear_clip",
            weight=1.0,
            clip_lower=0.0,
            clip_upper=1.0,
        ),
    ),
    strong_threshold=0.5,
    positive_threshold=0.15,
    neutral_lower_threshold=-0.15,
    weak_threshold=-0.5,
)


SIGNAL_PROFILES: dict[str, SignalProfile] = {
    TREND_V1_PROFILE.name: TREND_V1_PROFILE,
    TREND_ETF_MOMENTUM_REG20_PROFILE.name: TREND_ETF_MOMENTUM_REG20_PROFILE,
}


def get_signal_profile(name: str) -> SignalProfile:
    """Resolve signal profile by name."""

    normalized = name.strip()
    if not normalized:
        raise ValueError("profile 不能为空")
    if normalized not in SIGNAL_PROFILES:
        raise ValueError(f"未知 profile: {normalized}，可用: {list(SIGNAL_PROFILES)}")
    return SIGNAL_PROFILES[normalized]

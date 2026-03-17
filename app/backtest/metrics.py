"""
回测绩效指标计算
"""
from __future__ import annotations

import numpy as np
import polars as pl


def compute_metrics(returns: pl.Series, freq: str = "daily") -> dict:
    """
    计算常用回测指标

    Args:
        returns: 每期收益率序列（小数，如 0.01 表示 1%）
        freq:    数据频率 "daily" | "weekly" | "monthly"

    Returns:
        dict with keys: annualized_return, annualized_vol, sharpe, max_drawdown, calmar
    """
    periods = {"daily": 252, "weekly": 52, "monthly": 12}[freq]
    r = returns.cast(pl.Float64).to_numpy()

    ann_return = (1 + r).prod() ** (periods / len(r)) - 1
    ann_vol = r.std() * np.sqrt(periods)
    sharpe = ann_return / ann_vol if ann_vol > 0 else 0.0

    cum = (1 + r).cumprod()
    running_max = np.maximum.accumulate(cum)
    drawdown = (cum - running_max) / running_max
    max_dd = drawdown.min()

    calmar = ann_return / abs(max_dd) if max_dd != 0 else 0.0

    return {
        "annualized_return": round(ann_return, 4),
        "annualized_vol": round(ann_vol, 4),
        "sharpe": round(sharpe, 4),
        "max_drawdown": round(max_dd, 4),
        "calmar": round(calmar, 4),
    }

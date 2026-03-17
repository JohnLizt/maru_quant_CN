"""
Qlib 回测入口
封装 Qlib backtest，从本地 qlib_data 加载数据运行策略
"""
from __future__ import annotations

import os
from typing import Any

import pandas as pd
from loguru import logger

from app.utils.qlib_helper import init_qlib


def run_backtest(
    strategy_config: dict[str, Any],
    start_date: str = "2020-01-01",
    end_date: str = "2023-12-31",
    benchmark: str = "SH000300",
) -> dict[str, Any]:
    """
    运行 Qlib 回测

    Args:
        strategy_config: Qlib 策略配置字典（对应 yaml config）
        start_date:      回测开始日期 "YYYY-MM-DD"
        end_date:        回测结束日期 "YYYY-MM-DD"
        benchmark:       基准指数代码

    Returns:
        dict with keys: report_df, positions_df, metrics
    """
    init_qlib()

    try:
        import qlib
        from qlib.workflow import R
        from qlib.workflow.record_temp import PortAnaRecord, SignalRecord
    except ImportError:
        raise ImportError("请先安装 Qlib: pip install pyqlib")

    logger.info(f"开始回测: {start_date} ~ {end_date}，基准={benchmark}")

    # TODO: 根据 strategy_config 构建 Qlib RecordTemp pipeline
    # 当前为占位实现，待补全具体策略模型接入
    raise NotImplementedError(
        "请在 strategy_config 中指定 Qlib 模型和策略，参考 config/strategies/momentum.yaml"
    )

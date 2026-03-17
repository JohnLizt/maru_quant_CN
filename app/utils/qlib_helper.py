"""
Qlib 初始化工具
"""
from __future__ import annotations

import os

from loguru import logger

_qlib_initialized = False


def init_qlib(region: str = "cn") -> None:
    """
    初始化 Qlib（幂等，重复调用无副作用）

    Args:
        region: "cn" (中国 A 股) 或 "us" (美股)
    """
    global _qlib_initialized
    if _qlib_initialized:
        return

    try:
        import qlib
        from qlib.config import REG_CN, REG_US
    except ImportError:
        raise ImportError("请先安装 Qlib: pip install pyqlib")

    provider_uri = os.environ.get("QLIB_DATA_DIR", "/app/qlib_data")
    reg = REG_CN if region == "cn" else REG_US

    qlib.init(provider_uri=provider_uri, region=reg)
    _qlib_initialized = True
    logger.info(f"Qlib 初始化完成: uri={provider_uri}, region={region}")

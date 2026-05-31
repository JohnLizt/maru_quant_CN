"""Shared loguru setup helpers for task-oriented CLIs."""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from loguru import logger


def ensure_log_directories(root: str | Path = "logs") -> dict[str, Path]:
    base_dir = Path(root)
    daily_dir = base_dir / "daily"
    backtest_dir = base_dir / "backtest"
    daily_dir.mkdir(parents=True, exist_ok=True)
    backtest_dir.mkdir(parents=True, exist_ok=True)
    return {
        "root": base_dir,
        "daily": daily_dir,
        "backtest": backtest_dir,
    }


def build_timestamped_prefix(name: str, *, now: datetime | None = None) -> str:
    current = now or datetime.now()
    safe_name = name.replace(" ", "_").replace("/", "_")
    return f"{safe_name}_{current.strftime('%Y%m%d_%H%M%S')}"


def configure_task_logger(
    *,
    log_path: str | Path,
    file_level: str = "DEBUG",
    console_level: str = "INFO",
    enable_console: bool = True,
) -> Path:
    resolved_path = Path(log_path)
    resolved_path.parent.mkdir(parents=True, exist_ok=True)

    logger.remove()
    if enable_console:
        logger.add(
            sys.stderr,
            level=console_level.upper(),
            colorize=True,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}:{function}:{line}</cyan> - <level>{message}</level>",
        )
    logger.add(
        resolved_path,
        level=file_level.upper(),
        encoding="utf-8",
        enqueue=False,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    )
    return resolved_path

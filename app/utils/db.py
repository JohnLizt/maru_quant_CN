"""
数据库连接工具
支持 SQLAlchemy (同步) 连接 TimescaleDB
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from loguru import logger


def get_database_url() -> str:
    return os.environ.get(
        "DATABASE_URL",
        "postgresql://quant:quantpass@localhost:5432/akshare_db",
    )


def make_engine(pool_size: int = 5, max_overflow: int = 10):
    url = get_database_url()
    engine = create_engine(
        url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_pre_ping=True,   # 自动检测断连
        echo=False,
    )
    logger.info(f"数据库引擎已创建: {url.split('@')[-1]}")
    return engine


# 全局引擎（单例）
_engine = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = make_engine()
    return _engine


def get_session_factory() -> sessionmaker:
    return sessionmaker(bind=get_engine(), autoflush=False, autocommit=False)


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """上下文管理器：自动提交/回滚"""
    SessionLocal = get_session_factory()
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def health_check() -> bool:
    """检查数据库连接是否正常"""
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        logger.error(f"数据库连接失败: {e}")
        return False

#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# 运行因子批量计算流水线
# 使用方法：docker compose exec app bash scripts/run_factor_pipeline.sh [start] [end]
# 示例：docker compose exec app bash scripts/run_factor_pipeline.sh 2023-01-01 2024-12-31
# ─────────────────────────────────────────────────────────────
set -e

START="${1:-2023-01-01}"
END="${2:-$(date +%Y-%m-%d)}"

echo "计算因子: ${START} ~ ${END}"

python - <<EOF
from app.factors.pipeline import run_factor_pipeline
from app.utils.db import get_engine
from sqlalchemy import text

# 从 meta.stocks 读取活跃股票列表
engine = get_engine()
with engine.connect() as conn:
    rows = conn.execute(text(
        "SELECT symbol FROM meta.stocks WHERE is_active = TRUE ORDER BY symbol"
    )).fetchall()

symbols = [r[0] for r in rows]
print(f"股票池: {len(symbols)} 只")

n = run_factor_pipeline(symbols, start="${START}", end="${END}")
print(f"✅ 因子计算完成，共写入 {n} 条记录")
EOF

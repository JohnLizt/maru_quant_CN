#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# 每日增量 ETL：market.daily 孔洞检测 + 数据补全
#
# 用法：
#   docker compose exec app bash scripts/etl_daily.sh              # 默认 7 日回溯
#   docker compose exec app bash scripts/etl_daily.sh --lookback-days 30  # 每周对账
#
# Cron（容器外，每周一至五 17:30 CST = 09:30 UTC）：
#   30 9 * * 1-5  docker compose exec -T app bash scripts/etl_daily.sh
#   0  20 * * 0   docker compose exec -T app bash scripts/etl_daily.sh --lookback-days 30
# ─────────────────────────────────────────────────────────────
set -euo pipefail

LOG_DIR="${LOG_DIR:-/app/logs}"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/etl_daily_$(date +%Y%m%d_%H%M%S).log"

echo "$(date '+%Y-%m-%d %H:%M:%S') [ETL] 开始每日增量更新..." | tee -a "$LOG_FILE"

python /app/scripts/etl_daily.py "$@" 2>&1 | tee -a "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}

if [ "$EXIT_CODE" -eq 0 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') [ETL] ✅ 完成" | tee -a "$LOG_FILE"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') [ETL] ❌ 失败 (exit=$EXIT_CODE)，详见 $LOG_FILE" | tee -a "$LOG_FILE"
    exit "$EXIT_CODE"
fi

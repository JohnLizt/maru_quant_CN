#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# 每日增量因子计算：gap 检测 + warm-up 窗口
#
# 用法：
#   docker compose exec app bash scripts/factor_daily.sh                              # 默认 7 日，全部因子
#   docker compose exec app bash scripts/factor_daily.sh --lookback-days 30           # 每周对账
#   docker compose exec app bash scripts/factor_daily.sh --force-update               # 强制重算
#   docker compose exec app bash scripts/factor_daily.sh --factors ma20,rsi14         # 指定因子
#   docker compose exec app bash scripts/factor_daily.sh --factors limit_up --force-update
#
# Cron（容器外，每周一至五 18:00 CST = 10:00 UTC，ETL 后 30 分钟）：
#   0  10 * * 1-5  docker compose exec -T app bash scripts/factor_daily.sh
#   30 20 * * 0    docker compose exec -T app bash scripts/factor_daily.sh --lookback-days 30
# ─────────────────────────────────────────────────────────────
set -euo pipefail

LOG_DIR="${LOG_DIR:-/app/logs}"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/factor_daily_$(date +%Y%m%d_%H%M%S).log"

echo "$(date '+%Y-%m-%d %H:%M:%S') [FACTOR] 开始每日因子计算..." | tee -a "$LOG_FILE"

python /app/scripts/factor_daily.py "$@" 2>&1 | tee -a "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}

if [ "$EXIT_CODE" -eq 0 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') [FACTOR] ✅ 完成" | tee -a "$LOG_FILE"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') [FACTOR] ❌ 失败 (exit=$EXIT_CODE)，详见 $LOG_FILE" | tee -a "$LOG_FILE"
    exit "$EXIT_CODE"
fi

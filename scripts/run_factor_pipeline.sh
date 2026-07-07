#!/bin/bash
set -euo pipefail
ROOT_DIR="/Users/eason/dev/code/maru_quant_CN"
LOG_DIR="$ROOT_DIR/logs/daily"
LOG_FILE="$LOG_DIR/factor_pipeline_$(date +%Y%m%d_%H%M%S).log"
mkdir -p "$LOG_DIR"
{
  echo ">>> Log file: $LOG_FILE"
  echo ">>> Running factor_daily.py --lookback-days 7"
} >> "$LOG_FILE"
docker-compose -f "$ROOT_DIR/docker-compose.yml" exec -T app python scripts/factor_daily.py --lookback-days 7 >> "$LOG_FILE" 2>&1
{
  echo ">>> Running factor_ic_daily.py --lookback-days 365"
} >> "$LOG_FILE"
docker-compose -f "$ROOT_DIR/docker-compose.yml" exec -T app python scripts/factor_ic_daily.py --lookback-days 365 >> "$LOG_FILE" 2>&1
{
  echo ">>> Factor pipeline completed"
  echo ">>> Log file: $LOG_FILE"
} >> "$LOG_FILE"
echo "Log file: $LOG_FILE"

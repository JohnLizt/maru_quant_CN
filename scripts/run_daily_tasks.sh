#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/daily_tasks_${TIMESTAMP}.log"

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1

echo ">>> Daily task log: $LOG_FILE"

echo ">>> Running daily market ETL"
docker-compose -f "$ROOT_DIR/docker-compose.yml" exec app python scripts/etl_daily.py

echo ">>> Running daily factor pipeline"
docker-compose -f "$ROOT_DIR/docker-compose.yml" exec app python scripts/factor_daily.py

echo ">>> Daily tasks completed"

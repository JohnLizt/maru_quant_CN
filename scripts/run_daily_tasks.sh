#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs/daily"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/daily_tasks_${TIMESTAMP}.log"
DAILY_ETL_LOOKBACK_DAYS="${DAILY_ETL_LOOKBACK_DAYS:-7}"
DAILY_FACTOR_LOOKBACK_DAYS="${DAILY_FACTOR_LOOKBACK_DAYS:-7}"
DAILY_ETF_FETCH_MODE="${DAILY_ETF_FETCH_MODE:-by_date}"

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1

echo ">>> Daily task log: $LOG_FILE"
echo ">>> Daily task params: etl_lookback=${DAILY_ETL_LOOKBACK_DAYS} factor_lookback=${DAILY_FACTOR_LOOKBACK_DAYS} etf_fetch_mode=${DAILY_ETF_FETCH_MODE}"
echo ">>> Enabled asset types"
docker-compose -f "$ROOT_DIR/docker-compose.yml" exec -T app python - <<'PY'
from app.services.asset_universe import list_asset_types

for config in list_asset_types(enabled_only=True):
    print(f"- {config.asset_type} | source={config.data_source} | universe={config.pipeline_universe}")
PY

echo ">>> Running daily market ETL"
docker-compose -f "$ROOT_DIR/docker-compose.yml" exec -T app python scripts/etl_daily.py --lookback-days "$DAILY_ETL_LOOKBACK_DAYS" --etf-fetch-mode "$DAILY_ETF_FETCH_MODE"

echo ">>> Running daily factor pipeline"
docker-compose -f "$ROOT_DIR/docker-compose.yml" exec -T app python scripts/factor_daily.py --lookback-days "$DAILY_FACTOR_LOOKBACK_DAYS"

echo ">>> Sync status summary"
docker-compose -f "$ROOT_DIR/docker-compose.yml" exec -T app python app/cli/daily_task_status.py --format table

echo ">>> Daily tasks completed"

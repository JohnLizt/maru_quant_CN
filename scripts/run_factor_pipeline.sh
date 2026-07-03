#!/bin/bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
docker-compose -f "$ROOT_DIR/docker-compose.yml" exec -T app python scripts/factor_daily.py --lookback-days 7
docker-compose -f "$ROOT_DIR/docker-compose.yml" exec -T app python scripts/factor_ic_daily.py --lookback-days 365

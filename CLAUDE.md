# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Chinese A-stock (A股) quantitative analysis system. Data flows from **Tushare Pro** → **Polars** processing → **TimescaleDB** storage, with **Qlib** for strategy/backtesting and **JupyterLab** for research. All services run in Docker.

## First-Time Setup

```bash
# 1. Copy and configure environment variables
cp .env.example .env
# Edit .env: set TUSHARE_TOKEN and update credentials

# 2. Build images
docker compose build

# 3. Start services (TimescaleDB auto-runs init.sql on first boot)
docker compose up -d

# 4. Verify DB schema was initialized
docker compose exec timescaledb psql -U quant -d quant_db -c "\dn"
# Expected schemas: market, factors, signals, meta

# 5. Initialize Qlib data (one-time)
docker compose exec app bash scripts/init_qlib_data.sh
```

> **Note:** `docker/timescaledb/init.sql` runs automatically only on the **first** `docker compose up` (when the volume is empty). To re-run it, wipe the volume first: `docker compose down -v && docker compose up -d`

## Common Commands

```bash
# Start all services
docker compose up -d

# Stop services (preserving data volumes)
docker compose down

# Stop and wipe all data
docker compose down -v

# Rebuild images after dependency changes
docker compose build

# Execute Python code in app container
docker compose exec app python -m app.data_pipeline.fetch_daily

# Open a shell in the app container
docker compose exec app bash

# Database access
docker compose exec timescaledb psql -U quant -d quant_db

# Initialize Qlib data (one-time setup)
docker compose exec app bash scripts/init_qlib_data.sh

# View logs
docker compose logs -f [service]   # services: app, timescaledb, redis, jupyter, grafana
```

## Service Ports

| Service     | Default Port | Env Var            |
|-------------|-------------|---------------------|
| TimescaleDB | 5432        | `TIMESCALEDB_PORT`  |
| Redis       | 6379        | `REDIS_PORT`        |
| JupyterLab  | 8888        | `JUPYTER_PORT`      |
| Grafana     | 3000        | `GRAFANA_PORT`      |

## Architecture

### Data Layer (`docker/timescaledb/init.sql`)
Four schemas in TimescaleDB:
- **`market`** — Hypertables for `daily` (1-month chunks), `minute` (1-week chunks), `index_daily` data; continuous aggregate views `weekly` and `monthly` auto-roll up from `daily`
- **`factors`** — `daily_factors` hypertable for computed factor/indicator data
- **`signals`** — `trading_signals` with JSONB metadata
- **`meta`** — `stocks` master data, `sync_status` for tracking data fetch state

Compression policies auto-compress data older than 30–90 days. All hypertables are indexed on `(symbol, time)`.

### Application Layer (`app/`)
- **`app/utils/db.py`** — SQLAlchemy engine singleton with connection pooling; use `get_session()` context manager for all DB operations
- **`app/data_pipeline/fetch_daily.py`** — Reference implementation: Tushare Pro fetch → Polars → UPSERT into TimescaleDB
- **`app/strategy/`** — Empty placeholder for Qlib-based strategies

### Key Libraries
- **Tushare** (`tushare>=1.4.0`) — Chinese market data source; requires `TUSHARE_TOKEN` env var; stock codes in `000001.SZ` format
- **Polars** (`polars>=0.20.0`) — Primary DataFrame library; prefer over pandas except where Qlib requires pandas
- **Qlib** (`pyqlib>=0.9.0`) — Strategy/backtest framework; requires pandas DataFrames
- **ta** (`ta>=0.11.0`) — Technical indicators (pandas 2.x compatible)
- **Loguru** — Logging throughout the app
- **Pydantic v2 + pydantic-settings** — Config management

### Configuration
Copy `.env.example` to `.env`, set `TUSHARE_TOKEN`, and update other credentials before first run.

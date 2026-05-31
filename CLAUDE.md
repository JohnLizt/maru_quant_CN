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
docker compose exec app python scripts/init_qlib_data.py
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

# Daily market ETL
docker compose exec app python scripts/etl_daily.py

# Daily factor pipeline
docker compose exec app python scripts/factor_daily.py

# Open a shell in the app container
docker compose exec app bash

# Database access
docker compose exec timescaledb psql -U quant -d quant_db

# Initialize Qlib data (one-time setup)
docker compose exec app python scripts/init_qlib_data.py

# Query factor API
docker compose exec app python app/cli/query_factors.py --symbol 603019.SH --date 2026-04-30

# Query ETF rotation snapshot
docker compose exec app python app/cli/query_etf_rotation.py --date 2026-05-29 --top 10

# Run ETF weekly backtest
docker compose exec app python app/cli/backtest_etf_rotation.py --start-date 2025-06-03 --end-date 2026-05-29

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
- **`market`** — `daily` is the main hypertable used by ETL/factor/backtest flows
- **`factors`** — `daily_factors` hypertable for computed factor data
- **`signals`** — `trading_signals` with JSONB metadata for persisted strategy outputs
- **`meta`** — asset metadata and `sync_status`

The current project is multi-asset by `asset_type` (for example `stock_CN`, `etf_CN`), and ETL / factor completeness is tracked per `asset_type`.

### Application Layer (`app/`)
- **`app/utils/db.py`** — SQLAlchemy engine singleton with connection pooling; use `get_session()` context manager for all DB operations
- **`app/utils/signals.py`** — `upsert_signals()`: write strategy signals to `signals.trading_signals`
- **`app/data_loader/`** — Provider-based market data loading layer; Tushare is the current primary provider
- **`app/services/signal_score.py`** — Builds `SignalSnapshot` tables (`build_signal_snapshot`) and query-facing score output
- **`app/services/strategy_service.py`** — Bridges signal snapshots to strategy decisions for app and backtest consumers
- **`app/strategy/etf_rotation.py`** — ETF cross-sectional rotation strategy; consumes signal snapshots and emits `StrategyDecisionTable`
- **`app/strategy/momentum.py`** — Legacy momentum strategy prototype; not yet migrated to the new signal snapshot interface
- **`app/backtest/runner.py`** — Decision-driven backtest runner; supports strategy-fed ETF rotation research, including weekly rebalance and costs
- **`app/backtest/metrics.py`** — Annualized return, vol, Sharpe, max drawdown, Calmar

### Signal / Strategy / Backtest split
- **Signal layer** — produces normalized cross-sectional or time-series observations (`SignalSnapshot`)
- **Strategy layer** — turns signal snapshots into executable decisions (`StrategyDecisionTable`)
- **Backtest layer** — consumes strategy decisions only; it should not encode ranking or selection rules itself

For ETF rotation specifically:
- `app/cli/query_signal_scores.py` is the generic signal snapshot query entrypoint
- `app/cli/query_etf_rotation.py` is the strategy-facing ETF daily snapshot entrypoint
- `app/cli/backtest_etf_rotation.py` is the weekly backtest entrypoint

### Key Libraries
- **Tushare** (`tushare>=1.4.0`) — Chinese market data source; requires `TUSHARE_TOKEN` env var; stock codes in `000001.SZ` format
- **Polars** (`polars>=0.20.0`) — Primary DataFrame library; prefer over pandas except where Qlib requires pandas
- **Qlib** (`pyqlib>=0.9.0`) — Available for research integration, but the current ETF rotation backtest path is the in-repo `app/backtest/runner.py`
- **ta** (`ta>=0.11.0`) — Technical indicators (pandas 2.x compatible)
- **Loguru** — Logging throughout the app
- **Pydantic v2 + pydantic-settings** — Config management

### Configuration
Copy `.env.example` to `.env`, set `TUSHARE_TOKEN`, and update other credentials before first run.

# CN Equity Quant Pipeline

> A Dockerized A-share data pipeline built around **Tushare + TimescaleDB + Polars + Qlib**.

This project focuses on the boring-but-critical part of quant infra: getting daily Chinese equity data into a usable pipeline, keeping it complete, computing factors consistently, and exposing results through a simple query interface.

## Why this project matters

Most quant demos stop at notebooks. This repo is more practical:

- **Incremental ETL for A-shares** instead of one-shot scripts
- **Gap detection and backfill** for missing market / factor dates
- **Suspension-aware handling** so downstream factor data stays usable
- **TimescaleDB-backed storage** for structured historical data
- **Factor pipeline + query entrypoint** for downstream research or services
- **Dockerized local stack** for reproducible setup

If you want a base layer for an A-share factor platform, this is the point of the build.

## Current scope

This repository is currently strongest at:

- daily market ETL
- daily factor generation
- factor querying
- notebook-based exploration

It is **not yet a polished end-to-end trading platform**. In particular, the strategy/backtest layer still needs naming and integration cleanup before it can be treated as production-ready.

## Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│                        Docker Network                       │
│                                                             │
│   Tushare API  -->  ETL / Factor App  -->  TimescaleDB      │
│                            │                 │               │
│                            │                 ├─ market.*     │
│                            │                 ├─ factors.*    │
│                            │                 └─ meta.*       │
│                            │                                 │
│                            ├─ Qlib data volume              │
│                            ├─ logs/                         │
│                            └─ query CLI                     │
│                                                             │
│   JupyterLab  <------------------------------------------>  │
│   Grafana     <------------------------------------------>  │
└─────────────────────────────────────────────────────────────┘
```

## Core workflow

```text
Tushare daily data
    -> ETL completeness check
    -> market.daily
    -> factor computation with warmup window
    -> factors.daily_factors
    -> query API / notebooks / downstream signals
```

## Key features

### 1. Incremental market ETL

The main ETL entrypoint is:

```bash
docker compose exec app python scripts/etl_daily.py
```

What it does:

- derives trading dates from `000001.SZ`
- checks which dates are incomplete in `market.daily`
- fetches only missing data by default
- updates `meta.sync_status`

### 2. Suspension-aware data repair

If a stock in the pool is suspended on a trading date, the ETL fills a synthetic row using the previous close:

- `open/high/low/close = prev_close`
- `volume = 0`
- `amount = 0`
- `pct_change = 0`
- `is_suspended = true`

This keeps downstream panel-style factor computation continuous while preserving suspension information.

### 3. Daily factor pipeline

The factor entrypoint is:

```bash
docker compose exec app python scripts/factor_daily.py
```

The pipeline:

- reads from `market.daily`
- detects missing factor dates
- applies a warmup window per symbol
- computes registered factors
- writes long-format rows into `factors.daily_factors`
- updates `meta.sync_status`

### 4. Query-facing factor API entrypoint

External callers should use:

```bash
docker compose exec app python scripts/api/query_factors.py --symbol 603019.SH --date 2026-04-30
```

This is a CLI-style query interface over the factor query service.

## Tech stack

- **Tushare**: China market data source
- **TimescaleDB / PostgreSQL**: historical storage
- **Polars**: fast dataframe processing
- **Qlib**: research/backtest ecosystem integration
- **JupyterLab**: interactive exploration
- **Grafana**: lightweight monitoring and visualization
- **Docker Compose**: reproducible local environment

## Quick start

### 1. Prepare environment

```bash
cp .env.example .env
```

Then edit `.env` and set at least:

- `TUSHARE_TOKEN`
- database / Redis / Grafana credentials as needed

### 2. Build and start

```bash
docker compose build
docker compose up -d
```

### 3. Verify setup

Check database schemas:

```bash
docker compose exec timescaledb psql -U quant -d quant_db -c "\dn"
```

Expected schemas include:

- `market`
- `factors`
- `signals`
- `meta`

Check Qlib import:

```bash
docker compose exec app python -c "import qlib; print(qlib.__version__)"
```

### 4. Initialize Qlib data once

```bash
docker compose exec app python scripts/init_qlib_data.py
```

### 5. Run daily ETL

```bash
docker compose exec app python scripts/etl_daily.py
```

### 6. Run daily factor pipeline

```bash
docker compose exec app python scripts/factor_daily.py
```

## Common commands

### Reconcile a longer window

```bash
docker compose exec app python scripts/etl_daily.py --lookback-days 30
docker compose exec app python scripts/factor_daily.py --lookback-days 30
```

### Force refresh

```bash
docker compose exec app python scripts/etl_daily.py --force-update
docker compose exec app python scripts/factor_daily.py --force-update
```

### Run selected factors only

```bash
docker compose exec app python scripts/factor_daily.py --factors rsi14,limit_up
```

### Query factor values

```bash
docker compose exec app python scripts/api/query_factors.py --symbol 603019.SH --date 2026-04-30
docker compose exec app python scripts/api/query_factors.py --symbol "603019.SH 300059.SZ" --date 2026-04-30 --format json
```

## Notebook entrypoints

After JupyterLab is up, start with:

- `notebooks/quick_start/01_quick_start.ipynb`
- `notebooks/quick_start/02_factor_research.ipynb`
- `notebooks/quick_start/03_simple_backtest.ipynb`
- `notebooks/quick_start/04_qlib_backtest.ipynb`

## Repo layout

```text
.
├── app/
│   ├── data_pipeline/        # market data ingestion helpers
│   ├── factors/              # factor definitions and pipeline logic
│   ├── services/             # factor query / backfill services
│   ├── strategy/             # strategy prototypes
│   ├── backtest/             # backtest scaffolding
│   └── utils/                # DB, Qlib, signal helpers
├── config/
│   └── strategies/
├── docker/
│   ├── grafana/
│   └── timescaledb/
├── notebooks/
├── scripts/
│   ├── api/
│   ├── etl_daily.py
│   ├── factor_daily.py
│   └── init_qlib_data.py
├── logs/
├── docker-compose.yml
├── Dockerfile
└── .env.example
```

## Important notes

### Docker / Apple Silicon

On Apple Silicon with Colima, this stack was verified more reliably with an **`x86_64` VM** because `pyqlib>=0.9.0` did not install successfully in an `aarch64` container in this setup.

### Database init behavior

`docker/timescaledb/init.sql` only runs on the **first boot with an empty `timescaledb_data` volume**.

If schema changes are not appearing:

```bash
docker compose down -v
docker compose up -d
```

### Stock pool config

The market ETL expects a local `config/stock_pool.csv` file. This file is intentionally ignored by Git in this repo, so create your own with at least:

```text
symbol,name
603019.SH,Stock A
300059.SZ,Stock B
```

## Testing

The checked-in test is:

```bash
pytest tests/test_tushare_connection.py
```

It is an integration test that requires:

- a valid `TUSHARE_TOKEN`
- network access

## What makes this repo useful on GitHub

This project can be valuable to:

- builders who want a clean starting point for A-share quant infra
- researchers who need reproducible daily factor data pipelines
- engineers who want a Dockerized reference for Tushare + TimescaleDB + Qlib

It is especially useful because it solves the annoying infra layer that usually gets hand-waved away in quant demos.

## License / publishing note

Before making the repo public, make sure you do **not** publish:

- your real `.env`
- local logs
- downloaded datasets
- database volumes / exports
- any proprietary stock pool files you do not want to share

Yeah, basic opsec. Don't leak loot.

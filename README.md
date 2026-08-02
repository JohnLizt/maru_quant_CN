# Multi-Asset Quant Pipeline

> A Dockerized multi-asset quant pipeline built around **Tushare + TimescaleDB + Polars + Qlib**.

This project focuses on the boring-but-critical part of quant infra: getting daily market data into a usable pipeline, keeping it complete, computing factors consistently, exposing signal and strategy results, and supporting lightweight strategy backtests.

## Why this project matters

Most quant demos stop at notebooks. This repo is more practical:

- **Incremental ETL for multiple asset domains** instead of one-shot scripts
- **Gap detection and backfill** for missing market / factor dates
- **Suspension-aware handling** so downstream factor data stays usable
- **TimescaleDB-backed storage** for structured historical data
- **Factor + signal + strategy layers** with query entrypoints
- **Decision-driven ETF rotation backtest path** for research
- **Dockerized local stack** for reproducible setup

If you want a base layer for a China-first, multi-asset quant platform, this is the point of the build.

## Current scope

This repository is currently strongest at:

- daily market ETL
- daily factor generation
- factor and signal querying
- ETF cross-sectional rotation research
- strategy-fed backtesting
- notebook-based exploration

It is **not yet a polished end-to-end trading platform**. The current backtest path is research-oriented, and time-series strategy migration is still incomplete.

## Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│                        Docker Network                       │
│                                                             │
│   Tushare API  -->  ETL / Factor App  -->  TimescaleDB      │
│                            │                 │               │
│                            │                 ├─ market.*     │
│                            │                 ├─ factors.*    │
│                            │                 ├─ signals.*    │
│                            │                 └─ meta.*       │
│                            │                                 │
│                            ├─ Qlib data volume              │
│                            ├─ logs/                         │
│                            └─ signal / strategy / backtest CLI │
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
    -> signal snapshot
    -> strategy decisions
    -> query CLI / notebooks / downstream backtests
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

### 4. Signal / strategy split

The app now has three explicit layers:

- `signal`: builds reusable `SignalSnapshot` tables
- `strategy`: converts snapshots into `StrategyDecisionTable`
- `backtest`: consumes strategy decisions only

External callers can use:

```bash
docker compose exec app python app/cli/query_factors.py --symbol 603019.SH --date 2026-04-30
docker compose exec app python app/cli/query_signal_scores.py --date 2026-05-15 --format json
docker compose exec app python app/cli/query_etf_rotation.py --date 2026-05-29 --top 10
```

This keeps daily reporting and backtests aligned around the same strategy interface.

## Tech stack

- **Tushare**: China market data source
- **TimescaleDB / PostgreSQL**: historical storage
- **Polars**: fast dataframe processing
- **Qlib**: optional research/backtest ecosystem integration
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

### 2.1 Restart services after a server reboot

If the machine was rebooted, bring the full local stack back with:

```bash
docker-compose up -d
```

Then verify all services are running:

```bash
docker-compose ps
```

Expected core services:

- `app`
- `timescaledb`
- `redis`
- `jupyter`
- `grafana`

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
docker compose exec app python app/cli/query_factors.py --symbol 603019.SH --date 2026-04-30
docker compose exec app python app/cli/query_factors.py --symbol "603019.SH 300059.SZ" --date 2026-04-30 --format json
```

### Query composite signal scores

```bash
docker compose exec app python app/cli/query_signal_scores.py --date 2026-05-15 --symbol 000988.SZ --symbol 600126.SH --format json
docker compose exec app python app/cli/query_signal_scores.py --date 2026-05-15 --format csv
```

### Query ETF rotation strategy snapshot

```bash
docker compose exec app python app/cli/query_etf_rotation.py --date 2026-05-29 --top 10
```

### Run ETF rotation backtest

回测入口默认采用单边 `4bps` 交易成本，其中手续费 `1.5bps`、滑点 `2.5bps`；
可通过 `--commission-bps` 和 `--slippage-bps` 覆盖。

```bash
docker compose exec app python app/cli/backtest_etf_rotation.py --start-date 2025-06-03 --end-date 2026-05-29
docker compose exec app python app/cli/backtest_etf_rotation.py --start-date 2025-06-03 --end-date 2026-05-29 --format json
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
│   ├── data_loader/          # provider-based market data loaders
│   ├── factors/              # factor definitions and pipeline logic
│   ├── services/             # signal / strategy / query services
│   ├── signals/              # profiles, normalization, composite scoring
│   ├── strategy/             # strategy decisions
│   ├── backtest/             # decision-driven backtest runner
│   ├── cli/                  # query and backtest entrypoints
│   └── utils/                # DB, Qlib, signal persistence helpers
├── config/
│   ├── asset_types.csv
│   ├── universes/
│   └── strategies/
├── docker/
│   ├── grafana/
│   └── timescaledb/
├── notebooks/
├── scripts/
│   ├── etl_daily.py
│   ├── factor_daily.py
│   └── init_qlib_data.py
├── logs/
│   ├── daily/
│   └── backtest/
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

### Asset-type config

ETL 和 factor 现在默认读取两层配置：

- `config/asset_types.csv`: `asset_type` 注册表
- `config/universes/{asset_type}.csv`: 对应资产域的 pipeline universe

默认行为：

- 不传 `--asset-type` 时，遍历 `asset_types.csv` 中 `enabled=true` 的全部资产域
- 每个资产域只读取注册表中指定的 `pipeline_universe`

当前重点验证过的资产域包括：

- `stock_CN`
- `etf_CN`

其中 `etf_CN` 已接入：

- ETF signal snapshot
- ETF rotation strategy snapshot
- ETF weekly backtest CLI

旧的 `stock_pool` / `data_pipeline` 兼容入口已移除，调用方应直接使用：

- `app/services/asset_universe.py`
- `app/data_loader/*`

### Logs

日志目录现在按用途分层：

- `logs/daily/`: daily ETL / factor / automation 相关日志
- `logs/backtest/`: backtest 日志、CSV 产物、账户价值曲线图

`app/cli/backtest_etf_rotation.py` 默认会为每次运行创建一个带时间戳的子目录，例如：

- `logs/backtest/etf_rotation_YYYYMMDD_HHMMSS/`

目录内默认导出：

- `backtest.log`
- `returns.csv`
- `holdings.csv`
- `trades.csv`
- `equity_curve.csv`
- `equity.png`

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

- builders who want a clean starting point for China-first multi-asset quant infra
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

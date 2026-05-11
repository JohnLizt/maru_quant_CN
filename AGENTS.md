# AGENTS.md

## Setup and runtime gotchas
- This repo is designed to run inside Docker Compose; `app` and `jupyter` mount local source directories, so code edits apply without rebuilding unless dependencies or the Dockerfile change.
- On Apple Silicon / Colima, build the Docker VM as `x86_64`. `pyqlib>=0.9.0` did not install in an `aarch64` container here, but succeeded once Colima was recreated as `x86_64`.
- `docker/timescaledb/init.sql` only runs on first boot with an empty `timescaledb_data` volume. If schema changes are not showing up, `docker compose down -v` is required before `up -d`.
- `.env` must exist before meaningful runs; `TUSHARE_TOKEN` is required for ETL and the only test.

## Verified command flow
- First-time bring-up: `cp .env.example .env` -> `docker compose build` -> `docker compose up -d`.
- Verify DB init: `docker compose exec timescaledb psql -U quant -d quant_db -c "\dn"` and expect at least `market`, `factors`, `signals`, `meta`.
- Verify Qlib is usable: `docker compose exec app python -c "import qlib; print(qlib.__version__)"`.
- One-time Qlib dataset download: `docker compose exec app python scripts/init_qlib_data.py`.
- Daily market ETL: `docker compose exec app python scripts/etl_daily.py [--lookback-days 30] [--force-update]`.
- Daily factor pipeline: `docker compose exec app python scripts/factor_daily.py [--lookback-days 30] [--force-update] [--factors ...]`.
- Query API entrypoint: `docker compose exec app python api/query_factors.py --symbol 603019.SH --date 2026-04-30`.

## Source-of-truth entrypoints
- Market ETL entrypoint is `scripts/etl_daily.py`; it derives trading dates from Tushare daily data for `000001.SZ`, filters fetched rows to `config/stock_pool.csv`, treats a date as complete only when every pool symbol is present in `market.daily`, then updates `meta.sync_status`.
- Market ETL auto-fills suspended pool symbols using prior close (`open/high/low/close=prev_close`, `volume/amount=0`, `pct_change=0`, `is_suspended=true`). Missing rows are only considered errors if Tushare also does not mark the symbol as suspended for that date.
- Factor entrypoint is `scripts/factor_daily.py`; it reads from `market.daily`, computes factors per symbol with a 90-day warmup window, writes long-format rows into `factors.daily_factors`, then updates `meta.sync_status`.
- Query entrypoint for external callers is `api/query_factors.py`; it is a CLI-shaped API over the factor query service and should stay separate from internal ETL / factor pipeline scripts.
- `factor_daily.py` now loads `is_suspended` from `market.daily`. `BaseFactor` centralizes suspended-row handling via `suspended_policy`: technical factors currently `allow`, while `LimitUpFactor` uses `mask` so停牌日不会产出该事件因子。
- Signal persistence lives in `app/utils/signals.py` via `upsert_signals()`.
- DB access should go through `app/utils/db.py`; avoid relying on its localhost fallback because it points to `akshare_db`, not this repo's `quant_db`.

## Important repo drift to account for
- `README.md` / `CLAUDE.md` mention `app/factors/pipeline.py` and `scripts/run_factor_pipeline.sh`; those files do not exist in this checkout. The real factor pipeline is `scripts/factor_daily.py`.
- `app/backtest/runner.py` is not a working backtest pipeline yet; `run_backtest()` currently raises `NotImplementedError` after Qlib init.
- The current factor/strategy wiring is inconsistent: `scripts/factor_daily.py` emits factor names like `price_to_ma20`, `ma_cross`, `rsi14`, `macd_norm`, `limit_up`, while `app/strategy/momentum.py` expects pivoted columns `ma20`, `ma60`, `rsi14`, and `config/strategies/momentum.yaml` lists `macd`. Do not assume end-to-end strategy/backtest flow works without reconciling these names first.

## Testing and verification
- There is no visible lint/typecheck/CI config in this repo.
- The only checked-in test is `tests/test_tushare_connection.py`; it is an integration test that hits the real Tushare API and requires `TUSHARE_TOKEN` plus network access.
- Prefer focused verification in containers after changes: schema check via `psql`, Qlib import check, and running the relevant ETL/factor script against a short lookback window.

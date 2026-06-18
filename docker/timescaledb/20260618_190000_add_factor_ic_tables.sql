CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.factor_daily_ic (
    time            TIMESTAMPTZ      NOT NULL,
    asset_type      VARCHAR(32)      NOT NULL,
    factor_name     VARCHAR(50)      NOT NULL,
    lag             SMALLINT         NOT NULL,
    ic              DOUBLE PRECISION,
    rank_ic         DOUBLE PRECISION,
    n_stocks        INTEGER,
    calc_version    VARCHAR(32)      NOT NULL DEFAULT 'v1',
    created_at      TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (time, asset_type, factor_name, lag)
);

SELECT create_hypertable(
    'analytics.factor_daily_ic',
    'time',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

ALTER TABLE analytics.factor_daily_ic SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'asset_type, factor_name, lag',
    timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('analytics.factor_daily_ic', INTERVAL '60 days', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS analytics.factor_ic_summary (
    as_of_date       DATE              NOT NULL,
    asset_type       VARCHAR(32)       NOT NULL,
    factor_name      VARCHAR(50)       NOT NULL,
    lag              SMALLINT          NOT NULL,
    window_days      SMALLINT          NOT NULL,
    mean_ic          DOUBLE PRECISION,
    ic_std           DOUBLE PRECISION,
    ic_ir            DOUBLE PRECISION,
    t_stat           DOUBLE PRECISION,
    win_rate         DOUBLE PRECISION,
    mean_rank_ic     DOUBLE PRECISION,
    rank_ic_std      DOUBLE PRECISION,
    rank_ic_ir       DOUBLE PRECISION,
    n_days           INTEGER,
    start_date       DATE,
    end_date         DATE,
    calc_version     VARCHAR(32)       NOT NULL DEFAULT 'v1',
    created_at       TIMESTAMPTZ       NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (as_of_date, asset_type, factor_name, lag, window_days)
);

SELECT create_hypertable(
    'analytics.factor_ic_summary',
    'as_of_date',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

ALTER TABLE analytics.factor_ic_summary SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'asset_type, factor_name, lag, window_days',
    timescaledb.compress_orderby = 'as_of_date DESC'
);
SELECT add_compression_policy('analytics.factor_ic_summary', INTERVAL '60 days', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_factor_daily_ic_lookup
    ON analytics.factor_daily_ic (asset_type, factor_name, lag, time DESC);
CREATE INDEX IF NOT EXISTS idx_factor_ic_summary_lookup
    ON analytics.factor_ic_summary (asset_type, factor_name, lag, window_days, as_of_date DESC);

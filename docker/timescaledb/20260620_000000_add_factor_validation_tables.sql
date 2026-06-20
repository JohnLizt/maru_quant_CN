CREATE TABLE IF NOT EXISTS analytics.factor_daily_quantile_return (
    time            TIMESTAMPTZ      NOT NULL,
    asset_type      VARCHAR(32)      NOT NULL,
    factor_name     VARCHAR(50)      NOT NULL,
    lag             SMALLINT         NOT NULL,
    quantile_n      SMALLINT         NOT NULL,
    quantile_id     SMALLINT         NOT NULL,
    avg_fwd_ret     DOUBLE PRECISION,
    n_stocks        INTEGER,
    calc_version    VARCHAR(32)      NOT NULL DEFAULT 'v1',
    created_at      TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (time, asset_type, factor_name, lag, quantile_n, quantile_id)
);

SELECT create_hypertable(
    'analytics.factor_daily_quantile_return',
    'time',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

ALTER TABLE analytics.factor_daily_quantile_return SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'asset_type, factor_name, lag, quantile_n, quantile_id',
    timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('analytics.factor_daily_quantile_return', INTERVAL '60 days', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS analytics.factor_quantile_summary (
    as_of_date       DATE              NOT NULL,
    asset_type       VARCHAR(32)       NOT NULL,
    factor_name      VARCHAR(50)       NOT NULL,
    lag              SMALLINT          NOT NULL,
    quantile_n       SMALLINT          NOT NULL,
    quantile_id      SMALLINT          NOT NULL,
    window_days      SMALLINT          NOT NULL,
    mean_ret         DOUBLE PRECISION,
    ret_std          DOUBLE PRECISION,
    ret_ir           DOUBLE PRECISION,
    win_rate         DOUBLE PRECISION,
    n_days           INTEGER,
    start_date       DATE,
    end_date         DATE,
    calc_version     VARCHAR(32)       NOT NULL DEFAULT 'v1',
    created_at       TIMESTAMPTZ       NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (as_of_date, asset_type, factor_name, lag, quantile_n, quantile_id, window_days)
);

SELECT create_hypertable(
    'analytics.factor_quantile_summary',
    'as_of_date',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

ALTER TABLE analytics.factor_quantile_summary SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'asset_type, factor_name, lag, quantile_n, quantile_id, window_days',
    timescaledb.compress_orderby = 'as_of_date DESC'
);
SELECT add_compression_policy('analytics.factor_quantile_summary', INTERVAL '60 days', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS analytics.factor_daily_topk_return (
    time            TIMESTAMPTZ      NOT NULL,
    asset_type      VARCHAR(32)      NOT NULL,
    factor_name     VARCHAR(50)      NOT NULL,
    lag             SMALLINT         NOT NULL,
    top_k           SMALLINT         NOT NULL,
    topk_ret        DOUBLE PRECISION,
    universe_ret    DOUBLE PRECISION,
    excess_ret      DOUBLE PRECISION,
    n_stocks        INTEGER,
    calc_version    VARCHAR(32)      NOT NULL DEFAULT 'v1',
    created_at      TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (time, asset_type, factor_name, lag, top_k)
);

SELECT create_hypertable(
    'analytics.factor_daily_topk_return',
    'time',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

ALTER TABLE analytics.factor_daily_topk_return SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'asset_type, factor_name, lag, top_k',
    timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('analytics.factor_daily_topk_return', INTERVAL '60 days', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS analytics.factor_topk_summary (
    as_of_date         DATE              NOT NULL,
    asset_type         VARCHAR(32)       NOT NULL,
    factor_name        VARCHAR(50)       NOT NULL,
    lag                SMALLINT          NOT NULL,
    top_k              SMALLINT          NOT NULL,
    window_days        SMALLINT          NOT NULL,
    mean_topk_ret      DOUBLE PRECISION,
    topk_ret_std       DOUBLE PRECISION,
    topk_ret_ir        DOUBLE PRECISION,
    topk_win_rate      DOUBLE PRECISION,
    mean_excess_ret    DOUBLE PRECISION,
    excess_ret_std     DOUBLE PRECISION,
    excess_ret_ir      DOUBLE PRECISION,
    excess_win_rate    DOUBLE PRECISION,
    n_days             INTEGER,
    start_date         DATE,
    end_date           DATE,
    calc_version       VARCHAR(32)       NOT NULL DEFAULT 'v1',
    created_at         TIMESTAMPTZ       NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (as_of_date, asset_type, factor_name, lag, top_k, window_days)
);

SELECT create_hypertable(
    'analytics.factor_topk_summary',
    'as_of_date',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

ALTER TABLE analytics.factor_topk_summary SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'asset_type, factor_name, lag, top_k, window_days',
    timescaledb.compress_orderby = 'as_of_date DESC'
);
SELECT add_compression_policy('analytics.factor_topk_summary', INTERVAL '60 days', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_factor_daily_quantile_return_lookup
    ON analytics.factor_daily_quantile_return (asset_type, factor_name, lag, quantile_n, quantile_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_factor_quantile_summary_lookup
    ON analytics.factor_quantile_summary (asset_type, factor_name, lag, quantile_n, quantile_id, window_days, as_of_date DESC);
CREATE INDEX IF NOT EXISTS idx_factor_daily_topk_return_lookup
    ON analytics.factor_daily_topk_return (asset_type, factor_name, lag, top_k, time DESC);
CREATE INDEX IF NOT EXISTS idx_factor_topk_summary_lookup
    ON analytics.factor_topk_summary (asset_type, factor_name, lag, top_k, window_days, as_of_date DESC);

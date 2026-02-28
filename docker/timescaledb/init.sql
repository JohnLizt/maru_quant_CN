-- ─────────────────────────────────────────────────────────────
-- A股量化系统 - TimescaleDB 初始化脚本
-- 自动在首次启动时执行
-- ─────────────────────────────────────────────────────────────

-- 启用 TimescaleDB 扩展
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ════════════════════════════════════════════════════════════
-- Schema 分区
-- ════════════════════════════════════════════════════════════
CREATE SCHEMA IF NOT EXISTS market;    -- 行情数据
CREATE SCHEMA IF NOT EXISTS factors;   -- 因子数据
CREATE SCHEMA IF NOT EXISTS signals;   -- 信号数据
CREATE SCHEMA IF NOT EXISTS meta;      -- 元数据（股票信息等）

-- ════════════════════════════════════════════════════════════
-- 1. 股票基础信息表 (普通表)
-- ════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS meta.stocks (
    symbol          VARCHAR(10)  PRIMARY KEY,   -- 股票代码, e.g. 000001
    name            VARCHAR(50)  NOT NULL,       -- 股票名称
    exchange        VARCHAR(10)  NOT NULL,       -- SH | SZ | BJ
    sector          VARCHAR(50),                 -- 行业
    list_date       DATE,                        -- 上市日期
    delist_date     DATE,                        -- 退市日期 (NULL=在市)
    is_active       BOOLEAN      DEFAULT TRUE,
    updated_at      TIMESTAMPTZ  DEFAULT NOW()
);

COMMENT ON TABLE meta.stocks IS '股票基础信息';

-- ════════════════════════════════════════════════════════════
-- 2. 日线行情表 (TimescaleDB 超表)
-- ════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS market.daily (
    time            TIMESTAMPTZ  NOT NULL,   -- 交易日 (UTC)
    symbol          VARCHAR(10)  NOT NULL,   -- 股票代码
    open            NUMERIC(12,4),
    high            NUMERIC(12,4),
    low             NUMERIC(12,4),
    close           NUMERIC(12,4),
    volume          BIGINT,                  -- 成交量 (手)
    amount          NUMERIC(20,4),           -- 成交额 (元)
    adj_factor      NUMERIC(10,6),           -- 复权因子
    -- 涨跌
    pct_change      NUMERIC(8,4),            -- 涨跌幅 %
    -- 限价标记
    is_st           BOOLEAN      DEFAULT FALSE,
    is_suspended    BOOLEAN      DEFAULT FALSE,
    PRIMARY KEY (time, symbol)
);

-- 转换为 TimescaleDB 超表（按月分区）
SELECT create_hypertable(
    'market.daily',
    'time',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

-- 压缩策略：90天前的数据自动压缩
ALTER TABLE market.daily SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('market.daily', INTERVAL '90 days', if_not_exists => TRUE);

-- 数据保留策略（按需开启，例如保留10年）
-- SELECT add_retention_policy('market.daily', INTERVAL '10 years', if_not_exists => TRUE);

-- ════════════════════════════════════════════════════════════
-- 3. 分钟线行情表
-- ════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS market.minute (
    time            TIMESTAMPTZ  NOT NULL,
    symbol          VARCHAR(10)  NOT NULL,
    freq            VARCHAR(5)   NOT NULL DEFAULT '1m',  -- 1m | 5m | 15m | 30m | 60m
    open            NUMERIC(12,4),
    high            NUMERIC(12,4),
    low             NUMERIC(12,4),
    close           NUMERIC(12,4),
    volume          BIGINT,
    amount          NUMERIC(20,4),
    PRIMARY KEY (time, symbol, freq)
);

SELECT create_hypertable(
    'market.minute',
    'time',
    chunk_time_interval => INTERVAL '1 week',
    if_not_exists => TRUE
);

ALTER TABLE market.minute SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, freq',
    timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('market.minute', INTERVAL '30 days', if_not_exists => TRUE);

-- ════════════════════════════════════════════════════════════
-- 4. 指数行情表
-- ════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS market.index_daily (
    time            TIMESTAMPTZ  NOT NULL,
    symbol          VARCHAR(10)  NOT NULL,   -- e.g. 000001 (上证指数)
    name            VARCHAR(50),
    open            NUMERIC(12,4),
    high            NUMERIC(12,4),
    low             NUMERIC(12,4),
    close           NUMERIC(12,4),
    volume          BIGINT,
    amount          NUMERIC(20,4),
    pct_change      NUMERIC(8,4),
    PRIMARY KEY (time, symbol)
);

SELECT create_hypertable(
    'market.index_daily',
    'time',
    chunk_time_interval => INTERVAL '3 months',
    if_not_exists => TRUE
);

-- ════════════════════════════════════════════════════════════
-- 5. 因子数据表
-- ════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS factors.daily_factors (
    time            TIMESTAMPTZ  NOT NULL,
    symbol          VARCHAR(10)  NOT NULL,
    factor_name     VARCHAR(50)  NOT NULL,
    factor_value    DOUBLE PRECISION,
    PRIMARY KEY (time, symbol, factor_name)
);

SELECT create_hypertable(
    'factors.daily_factors',
    'time',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

ALTER TABLE factors.daily_factors SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, factor_name',
    timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('factors.daily_factors', INTERVAL '60 days', if_not_exists => TRUE);

-- ════════════════════════════════════════════════════════════
-- 6. 交易信号表
-- ════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS signals.trading_signals (
    time            TIMESTAMPTZ  NOT NULL,
    symbol          VARCHAR(10)  NOT NULL,
    strategy        VARCHAR(50)  NOT NULL,
    signal          SMALLINT     NOT NULL,  -- 1:买 | -1:卖 | 0:持有
    score           DOUBLE PRECISION,       -- 信号强度
    metadata        JSONB,                  -- 策略附加信息
    PRIMARY KEY (time, symbol, strategy)
);

SELECT create_hypertable(
    'signals.trading_signals',
    'time',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

-- ════════════════════════════════════════════════════════════
-- 7. 数据同步状态表
-- ════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS meta.sync_status (
    id              SERIAL       PRIMARY KEY,
    data_type       VARCHAR(50)  NOT NULL,   -- daily | minute | index 等
    symbol          VARCHAR(10),             -- NULL 表示全市场任务
    last_sync_time  TIMESTAMPTZ,
    last_date       DATE,
    status          VARCHAR(20)  DEFAULT 'pending',  -- pending|running|success|failed
    error_msg       TEXT,
    updated_at      TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (data_type, symbol)
);

-- ════════════════════════════════════════════════════════════
-- 索引
-- ════════════════════════════════════════════════════════════
CREATE INDEX IF NOT EXISTS idx_daily_symbol     ON market.daily (symbol, time DESC);
CREATE INDEX IF NOT EXISTS idx_minute_symbol    ON market.minute (symbol, freq, time DESC);
CREATE INDEX IF NOT EXISTS idx_factors_name     ON factors.daily_factors (factor_name, time DESC);
CREATE INDEX IF NOT EXISTS idx_signals_strategy ON signals.trading_signals (strategy, time DESC);
CREATE INDEX IF NOT EXISTS idx_stocks_exchange  ON meta.stocks (exchange);
CREATE INDEX IF NOT EXISTS idx_stocks_active    ON meta.stocks (is_active) WHERE is_active = TRUE;

-- ════════════════════════════════════════════════════════════
-- 连续聚合视图（Continuous Aggregates）：周线 / 月线
-- ════════════════════════════════════════════════════════════
CREATE MATERIALIZED VIEW IF NOT EXISTS market.weekly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 week', time)  AS time,
    symbol,
    first(open,  time)           AS open,
    max(high)                    AS high,
    min(low)                     AS low,
    last(close,  time)           AS close,
    sum(volume)                  AS volume,
    sum(amount)                  AS amount
FROM market.daily
GROUP BY 1, 2
WITH NO DATA;

-- 刷新策略：每天刷新最近3周数据（窗口需覆盖至少2个桶：2×1week + end_offset）
SELECT add_continuous_aggregate_policy(
    'market.weekly',
    start_offset => INTERVAL '3 weeks',
    end_offset   => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE MATERIALIZED VIEW IF NOT EXISTS market.monthly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 month', time) AS time,
    symbol,
    first(open,  time)           AS open,
    max(high)                    AS high,
    min(low)                     AS low,
    last(close,  time)           AS close,
    sum(volume)                  AS volume,
    sum(amount)                  AS amount
FROM market.daily
GROUP BY 1, 2
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'market.monthly',
    start_offset => INTERVAL '3 months',
    end_offset   => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- ════════════════════════════════════════════════════════════
-- 完成提示
-- ════════════════════════════════════════════════════════════
DO $$
BEGIN
    RAISE NOTICE '✅ TimescaleDB 初始化完成！';
    RAISE NOTICE '   Schemas: market, factors, signals, meta';
    RAISE NOTICE '   超表: market.daily | market.minute | market.index_daily';
    RAISE NOTICE '         factors.daily_factors | signals.trading_signals';
    RAISE NOTICE '   视图: market.weekly | market.monthly';
END $$;

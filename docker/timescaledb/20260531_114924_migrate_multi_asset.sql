-- Incremental migration for the multi-asset config / asset_type refactor.
-- Apply manually on an existing database after taking a backup.

ALTER TABLE market.daily
    ADD COLUMN IF NOT EXISTS asset_type VARCHAR(32),
    ADD COLUMN IF NOT EXISTS data_source VARCHAR(32);

UPDATE market.daily
SET asset_type = COALESCE(asset_type, 'stock_CN'),
    data_source = COALESCE(data_source, 'tushare')
WHERE asset_type IS NULL OR data_source IS NULL;

ALTER TABLE market.daily
    ALTER COLUMN asset_type SET NOT NULL,
    ALTER COLUMN data_source SET NOT NULL;

ALTER TABLE factors.daily_factors
    ADD COLUMN IF NOT EXISTS asset_type VARCHAR(32);

UPDATE factors.daily_factors
SET asset_type = COALESCE(asset_type, 'stock_CN')
WHERE asset_type IS NULL;

ALTER TABLE factors.daily_factors
    ALTER COLUMN asset_type SET NOT NULL;

ALTER TABLE signals.trading_signals
    ADD COLUMN IF NOT EXISTS asset_type VARCHAR(32);

UPDATE signals.trading_signals
SET asset_type = COALESCE(asset_type, 'stock_CN')
WHERE asset_type IS NULL;

ALTER TABLE signals.trading_signals
    ALTER COLUMN asset_type SET NOT NULL;

ALTER TABLE meta.sync_status
    ADD COLUMN IF NOT EXISTS asset_type VARCHAR(32),
    ADD COLUMN IF NOT EXISTS data_source VARCHAR(32);

UPDATE meta.sync_status
SET asset_type = COALESCE(asset_type, 'stock_CN')
WHERE asset_type IS NULL AND data_type IN ('daily_market', 'daily_factors');

UPDATE meta.sync_status
SET data_source = COALESCE(data_source, 'tushare')
WHERE data_source IS NULL AND data_type = 'daily_market';

DELETE FROM meta.sync_status AS current_row
USING (
    SELECT id
    FROM (
        SELECT
            id,
            ROW_NUMBER() OVER (
                PARTITION BY
                    data_type,
                    COALESCE(asset_type, 'stock_CN'),
                    COALESCE(symbol, ''),
                    COALESCE(data_source, CASE WHEN data_type = 'daily_market' THEN 'tushare' ELSE '' END)
                ORDER BY updated_at DESC NULLS LAST, id DESC
            ) AS row_num
        FROM meta.sync_status
    ) ranked
    WHERE row_num > 1
) duplicates
WHERE current_row.id = duplicates.id;

UPDATE meta.sync_status
SET asset_type = COALESCE(asset_type, ''),
    data_source = COALESCE(data_source, ''),
    symbol = COALESCE(symbol, '')
WHERE asset_type IS NULL OR data_source IS NULL OR symbol IS NULL;

ALTER TABLE meta.sync_status
    ALTER COLUMN asset_type SET NOT NULL,
    ALTER COLUMN data_source SET NOT NULL,
    ALTER COLUMN symbol SET NOT NULL;

ALTER TABLE market.daily DROP CONSTRAINT IF EXISTS daily_pkey;
ALTER TABLE market.daily
    ADD CONSTRAINT daily_pkey PRIMARY KEY (time, asset_type, symbol);

ALTER TABLE factors.daily_factors DROP CONSTRAINT IF EXISTS daily_factors_pkey;
ALTER TABLE factors.daily_factors
    ADD CONSTRAINT daily_factors_pkey PRIMARY KEY (time, asset_type, symbol, factor_name);

ALTER TABLE signals.trading_signals DROP CONSTRAINT IF EXISTS trading_signals_pkey;
ALTER TABLE signals.trading_signals
    ADD CONSTRAINT trading_signals_pkey PRIMARY KEY (time, asset_type, symbol, strategy);

ALTER TABLE meta.sync_status DROP CONSTRAINT IF EXISTS sync_status_data_type_symbol_key;
ALTER TABLE meta.sync_status
    ADD CONSTRAINT sync_status_data_type_asset_type_symbol_data_source_key
    UNIQUE (data_type, asset_type, symbol, data_source);

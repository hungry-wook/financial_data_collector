-- platform_postgres_migrations.sql

ALTER TABLE instruments DROP COLUMN IF EXISTS instrument_name_abbr;
ALTER TABLE instruments DROP COLUMN IF EXISTS instrument_name_eng;
ALTER TABLE instruments DROP COLUMN IF EXISTS security_group;
ALTER TABLE instruments DROP COLUMN IF EXISTS sector_name;
ALTER TABLE instruments DROP COLUMN IF EXISTS stock_type;
ALTER TABLE instruments DROP COLUMN IF EXISTS par_value;

ALTER TABLE daily_market_data ADD COLUMN IF NOT EXISTS base_price NUMERIC(20,6) NULL;
ALTER TABLE daily_market_data DROP COLUMN IF EXISTS price_change;
ALTER TABLE daily_market_data DROP COLUMN IF EXISTS change_rate;
ALTER TABLE daily_market_data DROP COLUMN IF EXISTS is_under_supervision;

ALTER TABLE benchmark_index_data DROP COLUMN IF EXISTS raw_open;
ALTER TABLE benchmark_index_data DROP COLUMN IF EXISTS raw_high;
ALTER TABLE benchmark_index_data DROP COLUMN IF EXISTS raw_low;
ALTER TABLE benchmark_index_data DROP COLUMN IF EXISTS raw_close;
ALTER TABLE benchmark_index_data DROP COLUMN IF EXISTS price_change;
ALTER TABLE benchmark_index_data DROP COLUMN IF EXISTS change_rate;

DROP TABLE IF EXISTS export_jobs CASCADE;
DROP TABLE IF EXISTS event_validation_results CASCADE;
DROP TABLE IF EXISTS corporate_events CASCADE;

ALTER TABLE price_adjustment_factors DROP COLUMN IF EXISTS factor_source;
ALTER TABLE price_adjustment_factors DROP COLUMN IF EXISTS confidence;

DROP VIEW IF EXISTS core_market_dataset_v1 CASCADE;
DROP VIEW IF EXISTS core_market_dataset_v2 CASCADE;
DROP VIEW IF EXISTS benchmark_dataset_v1 CASCADE;
DROP VIEW IF EXISTS trading_calendar_v1 CASCADE;
DROP VIEW IF EXISTS instrument_daily_v1 CASCADE;
DROP VIEW IF EXISTS benchmark_daily_v1 CASCADE;

CREATE VIEW instrument_daily_v1 AS
SELECT d.instrument_id,
       i.external_code,
       i.market_code,
       i.instrument_name,
       i.listing_date,
       i.delisting_date,
       d.trade_date,
       d.open,
       d.high,
       d.low,
       d.close,
       d.volume,
       d.turnover_value,
       d.market_value,
       d.listed_shares,
       d.base_price,
       COALESCE(p.factor, 1.0) AS daily_factor,
       COALESCE(p.cumulative_factor, 1.0) AS cumulative_factor,
       d.open * COALESCE(p.cumulative_factor, 1.0) AS adj_open,
       d.high * COALESCE(p.cumulative_factor, 1.0) AS adj_high,
       d.low * COALESCE(p.cumulative_factor, 1.0) AS adj_low,
       d.close * COALESCE(p.cumulative_factor, 1.0) AS adj_close,
       d.volume / COALESCE(NULLIF(p.cumulative_factor, 0), 1.0) AS adj_volume,
       d.is_trade_halted,
       d.record_status,
       d.source_name,
       d.collected_at
FROM daily_market_data d
JOIN instruments i ON i.instrument_id = d.instrument_id
LEFT JOIN price_adjustment_factors p
  ON p.instrument_id = d.instrument_id
 AND p.trade_date = d.trade_date
 AND p.as_of_date = DATE '9999-12-31';

CREATE VIEW benchmark_daily_v1 AS
SELECT index_code,
       index_name,
       trade_date,
       open,
       high,
       low,
       close,
       volume,
       turnover_value,
       market_cap,
       record_status,
       source_name,
       collected_at
FROM benchmark_index_data;

CREATE VIEW trading_calendar_v1 AS
SELECT market_code,
       trade_date,
       is_open,
       holiday_name,
       source_name,
       collected_at
FROM trading_calendar;
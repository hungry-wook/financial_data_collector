-- platform_schema.sql
-- Minimal schema for swing-momentum backtesting

CREATE TABLE instruments (
    instrument_id UUID PRIMARY KEY,
    external_code VARCHAR(20) NOT NULL,
    market_code VARCHAR(20) NOT NULL,
    instrument_name VARCHAR(200) NOT NULL,
    listing_date DATE NOT NULL,
    delisting_date DATE NULL,
    listed_shares BIGINT NULL,
    source_name VARCHAR(30) NOT NULL,
    collected_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NULL,
    UNIQUE (market_code, external_code),
    CHECK (delisting_date IS NULL OR delisting_date >= listing_date)
);

CREATE TABLE instrument_delisting_snapshot (
    delisting_snapshot_id BIGSERIAL PRIMARY KEY,
    market_code VARCHAR(20) NOT NULL,
    external_code VARCHAR(20) NOT NULL,
    delisting_date DATE NOT NULL,
    delisting_reason TEXT NULL,
    note TEXT NULL,
    source_name VARCHAR(30) NOT NULL,
    collected_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NULL,
    run_id UUID NULL,
    UNIQUE (market_code, external_code)
);

CREATE TABLE collection_runs (
    run_id UUID PRIMARY KEY,
    pipeline_name VARCHAR(50) NOT NULL,
    source_name VARCHAR(30) NOT NULL,
    window_start DATE NOT NULL,
    window_end DATE NOT NULL,
    status VARCHAR(20) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP NULL,
    success_count BIGINT NOT NULL DEFAULT 0,
    failure_count BIGINT NOT NULL DEFAULT 0,
    warning_count BIGINT NOT NULL DEFAULT 0,
    metadata JSONB NULL,
    CHECK (status IN ('RUNNING', 'SUCCESS', 'PARTIAL', 'FAILED')),
    CHECK (window_end >= window_start)
);

CREATE TABLE trading_calendar (
    market_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    is_open BOOLEAN NOT NULL,
    holiday_name VARCHAR(120) NULL,
    source_name VARCHAR(30) NOT NULL,
    collected_at TIMESTAMP NOT NULL,
    run_id UUID NULL,
    PRIMARY KEY (market_code, trade_date)
);

CREATE TABLE daily_market_data (
    instrument_id UUID NOT NULL,
    trade_date DATE NOT NULL,
    open NUMERIC(20,6) NOT NULL,
    high NUMERIC(20,6) NOT NULL,
    low NUMERIC(20,6) NOT NULL,
    close NUMERIC(20,6) NOT NULL,
    volume BIGINT NOT NULL,
    turnover_value NUMERIC(28,6) NULL,
    market_value NUMERIC(28,6) NULL,
    listed_shares BIGINT NULL,
    base_price NUMERIC(20,6) NULL,
    is_trade_halted BOOLEAN NOT NULL DEFAULT FALSE,
    record_status VARCHAR(20) NOT NULL DEFAULT 'VALID',
    source_name VARCHAR(30) NOT NULL,
    collected_at TIMESTAMP NOT NULL,
    run_id UUID NULL,
    PRIMARY KEY (instrument_id, trade_date),
    CHECK (high >= GREATEST(open, close, low)),
    CHECK (low <= LEAST(open, close, high)),
    CHECK (volume >= 0),
    CHECK (turnover_value IS NULL OR turnover_value >= 0),
    CHECK (market_value IS NULL OR market_value >= 0),
    CHECK (record_status IN ('VALID', 'INVALID', 'MISSING'))
);

CREATE TABLE benchmark_index_data (
    index_code VARCHAR(30) NOT NULL,
    index_name VARCHAR(200) NOT NULL,
    trade_date DATE NOT NULL,
    open NUMERIC(20,6) NULL,
    high NUMERIC(20,6) NULL,
    low NUMERIC(20,6) NULL,
    close NUMERIC(20,6) NOT NULL,
    volume BIGINT NULL,
    turnover_value NUMERIC(28,6) NULL,
    market_cap NUMERIC(28,6) NULL,
    record_status VARCHAR(20) NOT NULL DEFAULT 'VALID',
    source_name VARCHAR(30) NOT NULL,
    collected_at TIMESTAMP NOT NULL,
    run_id UUID NULL,
    PRIMARY KEY (index_code, index_name, trade_date),
    CHECK (
      open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL
      OR high >= GREATEST(open, close, low)
    ),
    CHECK (
      open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL
      OR low <= LEAST(open, close, high)
    ),
    CHECK (volume IS NULL OR volume >= 0),
    CHECK (record_status IN ('VALID', 'PARTIAL', 'INVALID'))
);

CREATE TABLE data_quality_issues (
    issue_id BIGSERIAL PRIMARY KEY,
    dataset_name VARCHAR(50) NOT NULL,
    trade_date DATE NULL,
    instrument_id UUID NULL,
    index_code VARCHAR(30) NULL,
    issue_code VARCHAR(50) NOT NULL,
    severity VARCHAR(10) NOT NULL,
    issue_detail TEXT NULL,
    source_name VARCHAR(30) NULL,
    detected_at TIMESTAMP NOT NULL,
    run_id UUID NULL,
    resolved_at TIMESTAMP NULL,
    CHECK (severity IN ('INFO', 'WARN', 'ERROR'))
);

CREATE TABLE price_adjustment_factors (
    instrument_id UUID NOT NULL,
    trade_date DATE NOT NULL,
    as_of_date DATE NOT NULL DEFAULT DATE '9999-12-31',
    factor NUMERIC(18,10) NOT NULL,
    cumulative_factor NUMERIC(18,10) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    run_id UUID NULL,
    PRIMARY KEY (instrument_id, trade_date, as_of_date),
    CHECK (factor > 0),
    CHECK (cumulative_factor > 0)
);

ALTER TABLE daily_market_data
ADD CONSTRAINT fk_daily_market_data_instrument
FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id);

ALTER TABLE instrument_delisting_snapshot
ADD CONSTRAINT fk_instrument_delisting_snapshot_run
FOREIGN KEY (run_id) REFERENCES collection_runs(run_id);

ALTER TABLE daily_market_data
ADD CONSTRAINT fk_daily_market_data_run
FOREIGN KEY (run_id) REFERENCES collection_runs(run_id);

ALTER TABLE benchmark_index_data
ADD CONSTRAINT fk_benchmark_index_data_run
FOREIGN KEY (run_id) REFERENCES collection_runs(run_id);

ALTER TABLE trading_calendar
ADD CONSTRAINT fk_trading_calendar_run
FOREIGN KEY (run_id) REFERENCES collection_runs(run_id);

ALTER TABLE data_quality_issues
ADD CONSTRAINT fk_data_quality_issues_instrument
FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id);

ALTER TABLE data_quality_issues
ADD CONSTRAINT fk_data_quality_issues_run
FOREIGN KEY (run_id) REFERENCES collection_runs(run_id);

ALTER TABLE price_adjustment_factors
ADD CONSTRAINT fk_price_adjustment_factors_instrument
FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id);

ALTER TABLE price_adjustment_factors
ADD CONSTRAINT fk_price_adjustment_factors_run
FOREIGN KEY (run_id) REFERENCES collection_runs(run_id);

CREATE INDEX idx_instruments_market_code ON instruments(market_code, external_code);
CREATE INDEX idx_instruments_external_code ON instruments(external_code);
CREATE INDEX idx_delisting_snapshot_market_date ON instrument_delisting_snapshot(market_code, delisting_date);
CREATE INDEX idx_delisting_snapshot_external_code ON instrument_delisting_snapshot(external_code);
CREATE INDEX idx_daily_trade_date ON daily_market_data(trade_date);
CREATE INDEX idx_daily_status_date ON daily_market_data(record_status, trade_date);
CREATE INDEX idx_index_trade_date ON benchmark_index_data(index_code, trade_date);
CREATE INDEX idx_index_series_trade_date ON benchmark_index_data(index_code, index_name, trade_date);
CREATE INDEX idx_calendar_market_open_date ON trading_calendar(market_code, is_open, trade_date);
CREATE INDEX idx_issues_date ON data_quality_issues(trade_date, severity);
CREATE INDEX idx_issues_instrument_date ON data_quality_issues(instrument_id, trade_date);
CREATE INDEX idx_runs_pipeline_time ON collection_runs(pipeline_name, started_at DESC);
CREATE INDEX idx_price_adjustment_factors_trade_date ON price_adjustment_factors(trade_date, as_of_date);

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
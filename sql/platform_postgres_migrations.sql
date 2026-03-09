-- platform_postgres_migrations.sql
-- Reconcile existing PostgreSQL databases to the current runtime schema.

CREATE TABLE IF NOT EXISTS instrument_delisting_snapshot (
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

ALTER TABLE instrument_delisting_snapshot
DROP CONSTRAINT IF EXISTS fk_instrument_delisting_snapshot_run;

ALTER TABLE instrument_delisting_snapshot
ADD CONSTRAINT fk_instrument_delisting_snapshot_run
FOREIGN KEY (run_id) REFERENCES collection_runs(run_id);

CREATE INDEX IF NOT EXISTS idx_delisting_snapshot_market_date
ON instrument_delisting_snapshot(market_code, delisting_date);

CREATE INDEX IF NOT EXISTS idx_delisting_snapshot_external_code
ON instrument_delisting_snapshot(external_code);

CREATE INDEX IF NOT EXISTS idx_instruments_external_code
ON instruments(external_code);

CREATE INDEX IF NOT EXISTS idx_instruments_name
ON instruments(instrument_name);

ALTER TABLE collection_runs
DROP CONSTRAINT IF EXISTS collection_runs_status_check;

ALTER TABLE collection_runs
ADD CONSTRAINT collection_runs_status_check
CHECK (status IN ('RUNNING', 'SUCCESS', 'PARTIAL', 'FAILED'));

CREATE TABLE IF NOT EXISTS export_jobs (
    job_id UUID PRIMARY KEY,
    status VARCHAR(20) NOT NULL,
    progress INTEGER NOT NULL DEFAULT 0,
    submitted_at TIMESTAMP NOT NULL,
    started_at TIMESTAMP NULL,
    finished_at TIMESTAMP NULL,
    output_path TEXT NULL,
    files JSONB NULL,
    row_counts JSONB NULL,
    error_code VARCHAR(50) NULL,
    error_message TEXT NULL,
    request_payload JSONB NOT NULL,
    CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED')),
    CHECK (progress >= 0 AND progress <= 100)
);

CREATE INDEX IF NOT EXISTS idx_export_jobs_status_submitted_at
ON export_jobs(status, submitted_at DESC);

ALTER TABLE corporate_events
ADD COLUMN IF NOT EXISTS raw_factor NUMERIC(18,10) NULL;

ALTER TABLE corporate_events
ADD COLUMN IF NOT EXISTS confidence VARCHAR(20) NOT NULL DEFAULT 'MEDIUM';

ALTER TABLE corporate_events
ADD COLUMN IF NOT EXISTS status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE';

ALTER TABLE corporate_events
DROP CONSTRAINT IF EXISTS corporate_events_status_check;

ALTER TABLE corporate_events
ADD CONSTRAINT corporate_events_status_check
CHECK (status IN ('ACTIVE', 'NEEDS_REVIEW', 'REJECTED'));

ALTER TABLE corporate_events
DROP CONSTRAINT IF EXISTS corporate_events_confidence_check;

ALTER TABLE corporate_events
ADD CONSTRAINT corporate_events_confidence_check
CHECK (confidence IN ('HIGH', 'MEDIUM', 'LOW'));

CREATE INDEX IF NOT EXISTS idx_corporate_events_source_event_id
ON corporate_events(source_event_id);

CREATE TABLE IF NOT EXISTS event_validation_results (
    validation_id BIGSERIAL PRIMARY KEY,
    source_event_id VARCHAR(120) NOT NULL,
    check_name VARCHAR(50) NOT NULL,
    result VARCHAR(20) NOT NULL,
    detail TEXT NULL,
    validated_at TIMESTAMP NOT NULL,
    CHECK (result IN ('MATCH', 'MISMATCH', 'PARSE_FAIL', 'SKIP'))
);

CREATE TABLE IF NOT EXISTS price_adjustment_factors (
    instrument_id UUID NOT NULL,
    trade_date DATE NOT NULL,
    as_of_date DATE NOT NULL DEFAULT DATE '9999-12-31',
    factor NUMERIC(18,10) NOT NULL,
    cumulative_factor NUMERIC(18,10) NOT NULL,
    factor_source VARCHAR(30) NOT NULL,
    confidence VARCHAR(20) NOT NULL DEFAULT 'MEDIUM',
    created_at TIMESTAMP NOT NULL,
    run_id UUID NULL,
    PRIMARY KEY (instrument_id, trade_date, as_of_date),
    CHECK (factor > 0),
    CHECK (cumulative_factor > 0),
    CHECK (confidence IN ('HIGH', 'MEDIUM', 'LOW'))
);

ALTER TABLE price_adjustment_factors
DROP CONSTRAINT IF EXISTS fk_price_adjustment_factors_instrument;

ALTER TABLE price_adjustment_factors
ADD CONSTRAINT fk_price_adjustment_factors_instrument
FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id);

ALTER TABLE price_adjustment_factors
DROP CONSTRAINT IF EXISTS fk_price_adjustment_factors_run;

ALTER TABLE price_adjustment_factors
ADD CONSTRAINT fk_price_adjustment_factors_run
FOREIGN KEY (run_id) REFERENCES collection_runs(run_id);

CREATE INDEX IF NOT EXISTS idx_price_adjustment_factors_trade_date
ON price_adjustment_factors(trade_date, as_of_date);

DROP TABLE IF EXISTS snapshot_runs;
DROP TABLE IF EXISTS dataset_snapshots;
DROP TABLE IF EXISTS source_policies;
DROP TABLE IF EXISTS run_partitions;
DROP TABLE IF EXISTS quality_metrics;

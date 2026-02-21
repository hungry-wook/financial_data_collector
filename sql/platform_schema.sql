-- platform_schema.sql
-- General-purpose financial time-series ingestion schema
-- DB: PostgreSQL 14+

-- =====================================================
-- Phase 1: Core Ingestion
-- =====================================================

CREATE TABLE instruments (
    instrument_id UUID PRIMARY KEY,
    external_code VARCHAR(20) NOT NULL,
    market_code VARCHAR(20) NOT NULL,
    instrument_name VARCHAR(200) NOT NULL,
    instrument_name_abbr VARCHAR(200) NULL,
    instrument_name_eng VARCHAR(200) NULL,
    listing_date DATE NOT NULL,
    delisting_date DATE NULL,
    listed_shares BIGINT NULL,
    security_group VARCHAR(100) NULL,
    sector_name VARCHAR(100) NULL,
    stock_type VARCHAR(100) NULL,
    par_value NUMERIC(20,6) NULL,
    source_name VARCHAR(30) NOT NULL,
    collected_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NULL,
    UNIQUE (market_code, external_code),
    CHECK (delisting_date IS NULL OR delisting_date >= listing_date)
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
    price_change NUMERIC(20,6) NULL,
    change_rate NUMERIC(20,6) NULL,
    listed_shares BIGINT NULL,
    is_trade_halted BOOLEAN NOT NULL DEFAULT FALSE,
    is_under_supervision BOOLEAN NOT NULL DEFAULT FALSE,
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
    raw_open VARCHAR(40) NULL,
    raw_high VARCHAR(40) NULL,
    raw_low VARCHAR(40) NULL,
    raw_close VARCHAR(40) NULL,
    volume BIGINT NULL,
    turnover_value NUMERIC(28,6) NULL,
    market_cap NUMERIC(28,6) NULL,
    price_change NUMERIC(20,6) NULL,
    change_rate NUMERIC(20,6) NULL,
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

ALTER TABLE daily_market_data
ADD CONSTRAINT fk_daily_market_data_instrument
FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id);

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

CREATE INDEX idx_instruments_market_code ON instruments(market_code, external_code);
CREATE INDEX idx_daily_trade_date ON daily_market_data(trade_date);
CREATE INDEX idx_daily_status_date ON daily_market_data(record_status, trade_date);
CREATE INDEX idx_index_trade_date ON benchmark_index_data(index_code, trade_date);
CREATE INDEX idx_index_series_trade_date ON benchmark_index_data(index_code, index_name, trade_date);
CREATE INDEX idx_calendar_market_open_date ON trading_calendar(market_code, is_open, trade_date);
CREATE INDEX idx_issues_date ON data_quality_issues(trade_date, severity);
CREATE INDEX idx_issues_instrument_date ON data_quality_issues(instrument_id, trade_date);
CREATE INDEX idx_runs_pipeline_time ON collection_runs(pipeline_name, started_at DESC);

-- Consumer views
CREATE VIEW core_market_dataset_v1 AS
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
       d.is_trade_halted,
       d.is_under_supervision,
       d.record_status,
       d.source_name,
       d.collected_at
FROM daily_market_data d
JOIN instruments i ON i.instrument_id = d.instrument_id;

CREATE VIEW benchmark_dataset_v1 AS
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
       price_change,
       change_rate,
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

-- =====================================================
-- Phase 2: Reliability (additive)
-- =====================================================

CREATE TABLE run_partitions (
    partition_id UUID PRIMARY KEY,
    run_id UUID NOT NULL,
    partition_key VARCHAR(120) NOT NULL,
    status VARCHAR(20) NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error_code VARCHAR(50) NULL,
    last_error_message VARCHAR(500) NULL,
    next_retry_at TIMESTAMP NULL,
    started_at TIMESTAMP NULL,
    finished_at TIMESTAMP NULL,
    UNIQUE (run_id, partition_key),
    CHECK (status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED'))
);

ALTER TABLE run_partitions
ADD CONSTRAINT fk_run_partitions_run
FOREIGN KEY (run_id) REFERENCES collection_runs(run_id);

CREATE TABLE quality_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    dataset_name VARCHAR(50) NOT NULL,
    metric_name VARCHAR(50) NOT NULL,
    metric_value NUMERIC(12,6) NOT NULL,
    threshold_value NUMERIC(12,6) NULL,
    measured_at TIMESTAMP NOT NULL
);

ALTER TABLE quality_metrics
ADD CONSTRAINT fk_quality_metrics_run
FOREIGN KEY (run_id) REFERENCES collection_runs(run_id);

CREATE INDEX idx_partitions_status_retry ON run_partitions(status, next_retry_at);
CREATE INDEX idx_quality_metrics_run_name ON quality_metrics(run_id, dataset_name, metric_name);

-- =====================================================
-- Phase 3: Domain Expansion (additive)
-- =====================================================

CREATE TABLE corporate_events (
    event_id VARCHAR(120) NOT NULL,
    event_version INTEGER NOT NULL,
    instrument_id UUID NOT NULL,
    event_type VARCHAR(40) NOT NULL,
    announce_date DATE NULL,
    effective_date DATE NULL,
    source_event_id VARCHAR(120) NULL,
    source_name VARCHAR(30) NOT NULL,
    collected_at TIMESTAMP NOT NULL,
    run_id UUID NULL,
    payload JSONB NULL,
    PRIMARY KEY (event_id, event_version),
    CHECK (event_version >= 1)
);

ALTER TABLE corporate_events
ADD CONSTRAINT fk_corporate_events_instrument
FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id);

ALTER TABLE corporate_events
ADD CONSTRAINT fk_corporate_events_run
FOREIGN KEY (run_id) REFERENCES collection_runs(run_id);

CREATE INDEX idx_corporate_events_instrument_date ON corporate_events(instrument_id, effective_date);
CREATE INDEX idx_corporate_events_type_date ON corporate_events(event_type, effective_date);

-- =====================================================
-- Phase 4: Platform Hardening (additive)
-- =====================================================

CREATE TABLE source_policies (
    policy_id UUID PRIMARY KEY,
    domain_name VARCHAR(30) NOT NULL,
    source_name VARCHAR(30) NOT NULL,
    priority_rank INTEGER NOT NULL,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    CHECK (priority_rank >= 1)
);

CREATE TABLE dataset_snapshots (
    snapshot_id UUID PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    cutoff_time TIMESTAMP NOT NULL,
    schema_version VARCHAR(32) NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    CHECK (status IN ('ACTIVE', 'STALE', 'ARCHIVED'))
);

CREATE TABLE snapshot_runs (
    snapshot_id UUID NOT NULL,
    run_id UUID NOT NULL,
    PRIMARY KEY (snapshot_id, run_id)
);

ALTER TABLE snapshot_runs
ADD CONSTRAINT fk_snapshot_runs_snapshot
FOREIGN KEY (snapshot_id) REFERENCES dataset_snapshots(snapshot_id);

ALTER TABLE snapshot_runs
ADD CONSTRAINT fk_snapshot_runs_run
FOREIGN KEY (run_id) REFERENCES collection_runs(run_id);

CREATE INDEX idx_source_policies_domain_active ON source_policies(domain_name, is_active, valid_from DESC);
CREATE INDEX idx_dataset_snapshots_date_status ON dataset_snapshots(snapshot_date, status);

-- =====================================================
-- Schema comments: tables and columns
-- =====================================================

COMMENT ON TABLE instruments IS 'Master table for tradable instruments.';
COMMENT ON COLUMN instruments.instrument_id IS 'Internal immutable instrument identifier.';
COMMENT ON COLUMN instruments.external_code IS 'Exchange-level instrument code.';
COMMENT ON COLUMN instruments.market_code IS 'Market classification code.';
COMMENT ON COLUMN instruments.instrument_name IS 'Display name of the instrument.';
COMMENT ON COLUMN instruments.listing_date IS 'Listing start date.';
COMMENT ON COLUMN instruments.delisting_date IS 'Delisting date, if applicable.';
COMMENT ON COLUMN instruments.source_name IS 'Upstream source name for master data.';
COMMENT ON COLUMN instruments.collected_at IS 'UTC timestamp when the row was collected.';
COMMENT ON COLUMN instruments.updated_at IS 'UTC timestamp when the row was last updated.';

COMMENT ON TABLE collection_runs IS 'Execution metadata for ingestion runs.';
COMMENT ON COLUMN collection_runs.run_id IS 'Unique ingestion run identifier.';
COMMENT ON COLUMN collection_runs.pipeline_name IS 'Pipeline job name.';
COMMENT ON COLUMN collection_runs.source_name IS 'Source system name.';
COMMENT ON COLUMN collection_runs.window_start IS 'Start date of the ingestion window.';
COMMENT ON COLUMN collection_runs.window_end IS 'End date of the ingestion window.';
COMMENT ON COLUMN collection_runs.status IS 'Run status: SUCCESS, PARTIAL, FAILED.';
COMMENT ON COLUMN collection_runs.started_at IS 'UTC timestamp when the run started.';
COMMENT ON COLUMN collection_runs.finished_at IS 'UTC timestamp when the run finished.';
COMMENT ON COLUMN collection_runs.success_count IS 'Count of successfully processed records.';
COMMENT ON COLUMN collection_runs.failure_count IS 'Count of failed records.';
COMMENT ON COLUMN collection_runs.warning_count IS 'Count of warning events.';
COMMENT ON COLUMN collection_runs.metadata IS 'Free-form run metadata payload.';

COMMENT ON TABLE trading_calendar IS 'Market trading calendar by date.';
COMMENT ON COLUMN trading_calendar.market_code IS 'Market code for the calendar row.';
COMMENT ON COLUMN trading_calendar.trade_date IS 'Trading date.';
COMMENT ON COLUMN trading_calendar.is_open IS 'Whether the market is open on trade_date.';
COMMENT ON COLUMN trading_calendar.holiday_name IS 'Holiday reason when market is closed.';
COMMENT ON COLUMN trading_calendar.source_name IS 'Upstream source name for calendar data.';
COMMENT ON COLUMN trading_calendar.collected_at IS 'UTC timestamp when the row was collected.';
COMMENT ON COLUMN trading_calendar.run_id IS 'Ingestion run identifier.';

COMMENT ON TABLE daily_market_data IS 'Daily OHLCV and market state for instruments.';
COMMENT ON COLUMN daily_market_data.instrument_id IS 'Reference to instruments.instrument_id.';
COMMENT ON COLUMN daily_market_data.trade_date IS 'Trading date.';
COMMENT ON COLUMN daily_market_data.open IS 'Open price.';
COMMENT ON COLUMN daily_market_data.high IS 'High price.';
COMMENT ON COLUMN daily_market_data.low IS 'Low price.';
COMMENT ON COLUMN daily_market_data.close IS 'Close price.';
COMMENT ON COLUMN daily_market_data.volume IS 'Traded volume.';
COMMENT ON COLUMN daily_market_data.turnover_value IS 'Traded value in local currency.';
COMMENT ON COLUMN daily_market_data.market_value IS 'End-of-day market capitalization.';
COMMENT ON COLUMN daily_market_data.is_trade_halted IS 'Whether trading was halted on trade_date.';
COMMENT ON COLUMN daily_market_data.is_under_supervision IS 'Whether instrument was under supervision/watch status.';
COMMENT ON COLUMN daily_market_data.record_status IS 'Validation state: VALID, INVALID, MISSING.';
COMMENT ON COLUMN daily_market_data.source_name IS 'Upstream source name for daily data.';
COMMENT ON COLUMN daily_market_data.collected_at IS 'UTC timestamp when the row was collected.';
COMMENT ON COLUMN daily_market_data.run_id IS 'Ingestion run identifier.';

COMMENT ON TABLE benchmark_index_data IS 'Daily OHLC data for benchmark indices.';
COMMENT ON COLUMN benchmark_index_data.index_code IS 'Benchmark index code.';
COMMENT ON COLUMN benchmark_index_data.trade_date IS 'Trading date.';
COMMENT ON COLUMN benchmark_index_data.open IS 'Open index level.';
COMMENT ON COLUMN benchmark_index_data.high IS 'High index level.';
COMMENT ON COLUMN benchmark_index_data.low IS 'Low index level.';
COMMENT ON COLUMN benchmark_index_data.close IS 'Close index level.';
COMMENT ON COLUMN benchmark_index_data.source_name IS 'Upstream source name for index data.';
COMMENT ON COLUMN benchmark_index_data.collected_at IS 'UTC timestamp when the row was collected.';
COMMENT ON COLUMN benchmark_index_data.run_id IS 'Ingestion run identifier.';

COMMENT ON TABLE data_quality_issues IS 'Issue log for missing data, validation failures, and warnings.';
COMMENT ON COLUMN data_quality_issues.issue_id IS 'Surrogate issue identifier.';
COMMENT ON COLUMN data_quality_issues.dataset_name IS 'Dataset where the issue was detected.';
COMMENT ON COLUMN data_quality_issues.trade_date IS 'Affected trading date, if available.';
COMMENT ON COLUMN data_quality_issues.instrument_id IS 'Affected instrument identifier, if applicable.';
COMMENT ON COLUMN data_quality_issues.index_code IS 'Affected benchmark index code, if applicable.';
COMMENT ON COLUMN data_quality_issues.issue_code IS 'Normalized issue code.';
COMMENT ON COLUMN data_quality_issues.severity IS 'Issue severity: INFO, WARN, ERROR.';
COMMENT ON COLUMN data_quality_issues.issue_detail IS 'Human-readable issue details.';
COMMENT ON COLUMN data_quality_issues.source_name IS 'Related source system.';
COMMENT ON COLUMN data_quality_issues.detected_at IS 'UTC timestamp when issue was detected.';
COMMENT ON COLUMN data_quality_issues.run_id IS 'Ingestion run identifier.';
COMMENT ON COLUMN data_quality_issues.resolved_at IS 'UTC timestamp when issue was resolved.';

COMMENT ON TABLE run_partitions IS 'Partition-level execution units for reliable retries.';
COMMENT ON COLUMN run_partitions.partition_id IS 'Partition identifier.';
COMMENT ON COLUMN run_partitions.run_id IS 'Parent ingestion run identifier.';
COMMENT ON COLUMN run_partitions.partition_key IS 'Logical key representing partition scope.';
COMMENT ON COLUMN run_partitions.status IS 'Partition status: PENDING, RUNNING, SUCCESS, FAILED.';
COMMENT ON COLUMN run_partitions.retry_count IS 'Number of retries for this partition.';
COMMENT ON COLUMN run_partitions.last_error_code IS 'Last error code observed.';
COMMENT ON COLUMN run_partitions.last_error_message IS 'Last error message observed.';
COMMENT ON COLUMN run_partitions.next_retry_at IS 'UTC timestamp for next retry scheduling.';
COMMENT ON COLUMN run_partitions.started_at IS 'UTC timestamp when partition execution started.';
COMMENT ON COLUMN run_partitions.finished_at IS 'UTC timestamp when partition execution finished.';

COMMENT ON TABLE quality_metrics IS 'Run-level quality measurements by dataset and metric.';
COMMENT ON COLUMN quality_metrics.metric_id IS 'Surrogate metric identifier.';
COMMENT ON COLUMN quality_metrics.run_id IS 'Ingestion run identifier.';
COMMENT ON COLUMN quality_metrics.dataset_name IS 'Dataset name for the metric.';
COMMENT ON COLUMN quality_metrics.metric_name IS 'Metric name, for example completeness.';
COMMENT ON COLUMN quality_metrics.metric_value IS 'Measured metric value.';
COMMENT ON COLUMN quality_metrics.threshold_value IS 'Configured threshold value, if any.';
COMMENT ON COLUMN quality_metrics.measured_at IS 'UTC timestamp when metric was recorded.';

COMMENT ON TABLE corporate_events IS 'Normalized corporate event records for domain expansion.';
COMMENT ON COLUMN corporate_events.event_id IS 'Stable event identifier.';
COMMENT ON COLUMN corporate_events.event_version IS 'Monotonic event version number.';
COMMENT ON COLUMN corporate_events.instrument_id IS 'Affected instrument identifier.';
COMMENT ON COLUMN corporate_events.event_type IS 'Normalized event type.';
COMMENT ON COLUMN corporate_events.announce_date IS 'Announcement date.';
COMMENT ON COLUMN corporate_events.effective_date IS 'Effective date for event application.';
COMMENT ON COLUMN corporate_events.source_event_id IS 'Original event identifier in source system.';
COMMENT ON COLUMN corporate_events.source_name IS 'Source system name.';
COMMENT ON COLUMN corporate_events.collected_at IS 'UTC timestamp when event was collected.';
COMMENT ON COLUMN corporate_events.run_id IS 'Ingestion run identifier.';
COMMENT ON COLUMN corporate_events.payload IS 'Raw normalized payload for extensibility.';

COMMENT ON TABLE source_policies IS 'Source priority and validity policy by data domain.';
COMMENT ON COLUMN source_policies.policy_id IS 'Policy identifier.';
COMMENT ON COLUMN source_policies.domain_name IS 'Data domain name.';
COMMENT ON COLUMN source_policies.source_name IS 'Source system name.';
COMMENT ON COLUMN source_policies.priority_rank IS 'Priority rank where lower means higher priority.';
COMMENT ON COLUMN source_policies.valid_from IS 'UTC timestamp policy becomes effective.';
COMMENT ON COLUMN source_policies.valid_to IS 'UTC timestamp policy expires.';
COMMENT ON COLUMN source_policies.is_active IS 'Whether this policy is currently active.';

COMMENT ON TABLE dataset_snapshots IS 'Immutable snapshot metadata for delivery versioning.';
COMMENT ON COLUMN dataset_snapshots.snapshot_id IS 'Snapshot identifier.';
COMMENT ON COLUMN dataset_snapshots.snapshot_date IS 'Business date represented by snapshot.';
COMMENT ON COLUMN dataset_snapshots.cutoff_time IS 'Data cutoff timestamp for snapshot.';
COMMENT ON COLUMN dataset_snapshots.schema_version IS 'Schema/view version used by snapshot.';
COMMENT ON COLUMN dataset_snapshots.status IS 'Snapshot status: ACTIVE, STALE, ARCHIVED.';
COMMENT ON COLUMN dataset_snapshots.created_at IS 'UTC timestamp when snapshot was created.';

COMMENT ON TABLE snapshot_runs IS 'Mapping table from snapshots to included ingestion runs.';
COMMENT ON COLUMN snapshot_runs.snapshot_id IS 'Snapshot identifier.';
COMMENT ON COLUMN snapshot_runs.run_id IS 'Included ingestion run identifier.';

COMMENT ON VIEW core_market_dataset_v1 IS 'Phase 1 consumer view for instrument daily data with master attributes.';
COMMENT ON VIEW benchmark_dataset_v1 IS 'Phase 1 consumer view for benchmark daily index data.';
COMMENT ON VIEW trading_calendar_v1 IS 'Phase 1 consumer view for trading calendar data.';

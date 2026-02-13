# 전체 스키마 명세 (Phase 1/2/3, 전체 컬럼 DDL)

## 0. DB 방언 및 공통 규약
- DBMS: PostgreSQL 14+
- 문자셋: UTF-8
- 시간:
  - DATE는 KST 의미
  - TIMESTAMP는 UTC 저장
- 원장 테이블(prices_raw/corp_actions 계열)은 append-only 사용

권장 확장:
```sql
CREATE EXTENSION IF NOT EXISTS btree_gist;
```

---

## 1. ENUM/체크 값 사전 (문서 규약)
아래 값은 CHECK 제약 또는 애플리케이션 validation으로 강제한다.

- batch status: `SUCCESS`, `PARTIAL`, `FAILED`
- partition status: `PENDING`, `RUNNING`, `SUCCESS`, `FAILED`
- trading state: `LISTED`, `SUSPENDED`, `RESUMED`, `DELISTED`
- event_validation_status: `VALID`, `INCOMPLETE`, `ECONOMICALLY_COMPLEX`
- adjustment_status: `APPLIED`, `SKIPPED_REQUIRES_POSITION_ENGINE`, `SKIPPED_INSUFFICIENT_DATA`, `SKIPPED_POLICY`
- effective_date_source: `EXPLICIT_SOURCE`, `DERIVED_NEXT_TRADING_DAY`, `UNKNOWN`
- effective_date_confidence: `HIGH`, `MED`, `LOW`
- snapshot status: `ACTIVE`, `STALE`, `REBUILDING`, `ARCHIVED`
- missing_reason: `MARKET_HOLIDAY`, `SYMBOL_SUSPENDED`, `COLLECTION_FAILURE`, `SOURCE_EMPTY`, `UNKNOWN`

---

## 2. Phase 1 (MVP) 테이블 DDL

## 2.1 symbols
```sql
CREATE TABLE symbols (
    symbol_id UUID PRIMARY KEY,
    market VARCHAR(20) NOT NULL,
    code VARCHAR(12) NOT NULL,
    name VARCHAR(200) NOT NULL,
    instrument_type VARCHAR(30) NOT NULL DEFAULT 'EQUITY',
    listing_date DATE NOT NULL,
    delisting_date DATE NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    source VARCHAR(30) NOT NULL DEFAULT 'KRX',
    collected_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NULL,
    UNIQUE (market, code, listing_date),
    CHECK (delisting_date IS NULL OR delisting_date >= listing_date)
);

CREATE INDEX idx_symbols_code_market ON symbols(code, market);
CREATE INDEX idx_symbols_active ON symbols(is_active, market);
```

## 2.2 symbol_code_history
```sql
CREATE TABLE symbol_code_history (
    symbol_id UUID NOT NULL,
    code VARCHAR(12) NOT NULL,
    market VARCHAR(20) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NULL,
    reason VARCHAR(50) NOT NULL DEFAULT 'KRX_SYNC',
    source VARCHAR(30) NOT NULL DEFAULT 'KRX',
    collected_at TIMESTAMP NOT NULL,
    PRIMARY KEY (symbol_id, code, start_date),
    CHECK (end_date IS NULL OR end_date > start_date)
);

CREATE INDEX idx_sch_code_market_range ON symbol_code_history(code, market, start_date, end_date);
CREATE INDEX idx_sch_symbol_range ON symbol_code_history(symbol_id, start_date, end_date);
```

## 2.3 symbol_trading_state
```sql
CREATE TABLE symbol_trading_state (
    symbol_id UUID NOT NULL,
    trading_state VARCHAR(20) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NULL,
    reason VARCHAR(200) NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'KRX',
    collected_at TIMESTAMP NOT NULL,
    PRIMARY KEY (symbol_id, trading_state, start_date),
    CHECK (trading_state IN ('LISTED', 'SUSPENDED', 'RESUMED', 'DELISTED')),
    CHECK (end_date IS NULL OR end_date > start_date)
);

CREATE INDEX idx_sts_symbol_range ON symbol_trading_state(symbol_id, start_date, end_date);
CREATE INDEX idx_sts_state_date ON symbol_trading_state(trading_state, start_date);
```

## 2.4 symbol_market_state
```sql
CREATE TABLE symbol_market_state (
    symbol_id UUID NOT NULL,
    market VARCHAR(20) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NULL,
    reason VARCHAR(100) NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'KRX',
    collected_at TIMESTAMP NOT NULL,
    PRIMARY KEY (symbol_id, market, start_date),
    CHECK (end_date IS NULL OR end_date > start_date)
);

CREATE INDEX idx_sms_symbol_range ON symbol_market_state(symbol_id, start_date, end_date);
```

## 2.5 trading_calendar
```sql
CREATE TABLE trading_calendar (
    market VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    is_open BOOLEAN NOT NULL,
    holiday_name VARCHAR(100) NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'KRX',
    collected_at TIMESTAMP NOT NULL,
    PRIMARY KEY (market, date)
);

CREATE INDEX idx_calendar_open ON trading_calendar(market, date, is_open);
```

## 2.6 ingestion_batches
```sql
CREATE TABLE ingestion_batches (
    ingestion_batch_id UUID PRIMARY KEY,
    pipeline_name VARCHAR(50) NOT NULL,
    source VARCHAR(30) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP NULL,
    status VARCHAR(20) NOT NULL,
    success_count BIGINT NOT NULL DEFAULT 0,
    failure_count BIGINT NOT NULL DEFAULT 0,
    retry_count BIGINT NOT NULL DEFAULT 0,
    warning_count BIGINT NOT NULL DEFAULT 0,
    error_summary TEXT NULL,
    metadata JSONB NULL,
    CHECK (status IN ('SUCCESS', 'PARTIAL', 'FAILED')),
    CHECK (end_date >= start_date)
);

CREATE INDEX idx_batches_pipeline_time ON ingestion_batches(pipeline_name, started_at DESC);
CREATE INDEX idx_batches_status_time ON ingestion_batches(status, started_at DESC);
```

## 2.7 prices_raw
```sql
CREATE TABLE prices_raw (
    symbol_id UUID NOT NULL,
    date DATE NOT NULL,
    market VARCHAR(20) NOT NULL,
    open NUMERIC(20,6) NOT NULL,
    high NUMERIC(20,6) NOT NULL,
    low NUMERIC(20,6) NOT NULL,
    close NUMERIC(20,6) NOT NULL,
    volume BIGINT NOT NULL,
    value NUMERIC(28,6) NULL,
    source VARCHAR(30) NOT NULL,
    source_row_id VARCHAR(120) NULL,
    logical_hash CHAR(64) NOT NULL,
    price_revision_seq INTEGER NOT NULL,
    price_validation_status VARCHAR(20) NOT NULL,
    validation_error_code VARCHAR(50) NULL,
    collected_at TIMESTAMP NOT NULL,
    ingestion_batch_id UUID NOT NULL,
    PRIMARY KEY (symbol_id, date, source, price_revision_seq),
    CHECK (price_revision_seq >= 1),
    CHECK (price_validation_status IN ('VALID', 'INVALID')),
    CHECK (volume >= 0)
);

CREATE INDEX idx_prices_symbol_date ON prices_raw(symbol_id, date);
CREATE INDEX idx_prices_cutoff ON prices_raw(collected_at);
CREATE INDEX idx_prices_latest ON prices_raw(symbol_id, date, source, price_revision_seq DESC);
CREATE INDEX idx_prices_batch ON prices_raw(ingestion_batch_id);
```

## 2.8 pending_symbol_prices
```sql
CREATE TABLE pending_symbol_prices (
    code VARCHAR(12) NOT NULL,
    date DATE NOT NULL,
    market VARCHAR(20) NOT NULL,
    open NUMERIC(20,6) NOT NULL,
    high NUMERIC(20,6) NOT NULL,
    low NUMERIC(20,6) NOT NULL,
    close NUMERIC(20,6) NOT NULL,
    volume BIGINT NOT NULL,
    value NUMERIC(28,6) NULL,
    source VARCHAR(30) NOT NULL,
    source_row_id VARCHAR(120) NULL,
    collected_at TIMESTAMP NOT NULL,
    ingestion_batch_id UUID NOT NULL,
    resolved_symbol_id UUID NULL,
    resolved_reason VARCHAR(100) NULL,
    resolved_at TIMESTAMP NULL,
    PRIMARY KEY (code, date, source, collected_at)
);

CREATE INDEX idx_pending_unresolved ON pending_symbol_prices(resolved_symbol_id, date);
```

## 2.9 corp_actions
```sql
CREATE TABLE corp_actions (
    event_id VARCHAR(120) NOT NULL,
    event_version INTEGER NOT NULL,
    symbol_id UUID NOT NULL,
    event_type VARCHAR(40) NOT NULL,
    announce_date DATE NULL,
    ex_date DATE NULL,
    effective_date DATE NULL,
    effective_date_source VARCHAR(40) NOT NULL,
    effective_date_confidence VARCHAR(10) NOT NULL DEFAULT 'MED',
    effective_date_reason VARCHAR(50) NOT NULL DEFAULT 'SOURCE_EXPLICIT',
    event_priority INTEGER NOT NULL DEFAULT 100,
    source_event_id VARCHAR(120) NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'DART',
    event_validation_status VARCHAR(30) NOT NULL,
    usable_for_price BOOLEAN NOT NULL,
    usable_for_factor BOOLEAN NOT NULL,
    usable_for_position BOOLEAN NOT NULL,
    canonical_payload_hash CHAR(64) NULL,
    event_change_reason VARCHAR(200) NULL,
    collected_at TIMESTAMP NOT NULL,
    ingestion_batch_id UUID NOT NULL,
    PRIMARY KEY (event_id, event_version),
    CHECK (event_version >= 1),
    CHECK (effective_date_source IN ('EXPLICIT_SOURCE', 'DERIVED_NEXT_TRADING_DAY', 'UNKNOWN')),
    CHECK (effective_date_confidence IN ('HIGH', 'MED', 'LOW')),
    CHECK (event_validation_status IN ('VALID', 'INCOMPLETE', 'ECONOMICALLY_COMPLEX'))
);

CREATE INDEX idx_ca_symbol_eff ON corp_actions(symbol_id, effective_date, event_type);
CREATE INDEX idx_ca_source_event ON corp_actions(source_event_id, event_version);
CREATE INDEX idx_ca_batch ON corp_actions(ingestion_batch_id);
```

## 2.10 corp_action_ratio
```sql
CREATE TABLE corp_action_ratio (
    event_id VARCHAR(120) NOT NULL,
    event_version INTEGER NOT NULL,
    ratio_num NUMERIC(30,10) NOT NULL,
    ratio_den NUMERIC(30,10) NOT NULL,
    ratio_type VARCHAR(30) NOT NULL,
    unit_note VARCHAR(100) NULL,
    PRIMARY KEY (event_id, event_version),
    CHECK (ratio_den <> 0)
);
```

## 2.11 corp_action_dividend
```sql
CREATE TABLE corp_action_dividend (
    event_id VARCHAR(120) NOT NULL,
    event_version INTEGER NOT NULL,
    cash_dividend_per_share NUMERIC(20,6) NULL,
    stock_dividend_ratio_num NUMERIC(30,10) NULL,
    stock_dividend_ratio_den NUMERIC(30,10) NULL,
    record_date DATE NULL,
    pay_date DATE NULL,
    currency VARCHAR(10) NULL,
    tax_note VARCHAR(100) NULL,
    PRIMARY KEY (event_id, event_version)
);
```

## 2.12 adjustment_factors
```sql
CREATE TABLE adjustment_factors (
    snapshot_id UUID NOT NULL,
    symbol_id UUID NOT NULL,
    event_id VARCHAR(120) NOT NULL,
    event_version INTEGER NOT NULL,
    effective_date DATE NOT NULL,
    adjustment_factor NUMERIC(38,18) NOT NULL,
    adjustment_status VARCHAR(40) NOT NULL,
    adjustment_skip_reason VARCHAR(120) NULL,
    rule_id VARCHAR(40) NOT NULL,
    computed_at TIMESTAMP NOT NULL,
    PRIMARY KEY (snapshot_id, symbol_id, event_id, event_version),
    CHECK (adjustment_status IN ('APPLIED', 'SKIPPED_REQUIRES_POSITION_ENGINE', 'SKIPPED_INSUFFICIENT_DATA', 'SKIPPED_POLICY'))
);

CREATE INDEX idx_af_symbol_date ON adjustment_factors(snapshot_id, symbol_id, effective_date);
```

## 2.13 daily_cumulative_adjustment
```sql
CREATE TABLE daily_cumulative_adjustment (
    snapshot_id UUID NOT NULL,
    symbol_id UUID NOT NULL,
    date DATE NOT NULL,
    cumulative_factor NUMERIC(38,18) NOT NULL,
    computed_at TIMESTAMP NOT NULL,
    PRIMARY KEY (snapshot_id, symbol_id, date)
);

CREATE INDEX idx_dca_symbol_date ON daily_cumulative_adjustment(snapshot_id, symbol_id, date);
```

## 2.14 adjustment_evaluation_log
```sql
CREATE TABLE adjustment_evaluation_log (
    snapshot_id UUID NOT NULL,
    symbol_id UUID NOT NULL,
    event_id VARCHAR(120) NOT NULL,
    event_version INTEGER NOT NULL,
    event_type VARCHAR(40) NOT NULL,
    effective_date DATE NULL,
    adjustment_status VARCHAR(40) NOT NULL,
    adjustment_skip_reason VARCHAR(120) NULL,
    rule_id VARCHAR(40) NOT NULL,
    adjustment_engine_version VARCHAR(64) NOT NULL,
    evaluated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (snapshot_id, symbol_id, event_id, event_version)
);

CREATE INDEX idx_ael_snapshot_time ON adjustment_evaluation_log(snapshot_id, evaluated_at DESC);
```

## 2.15 collection_gap_report
```sql
CREATE TABLE collection_gap_report (
    ingestion_batch_id UUID NOT NULL,
    symbol_id UUID NOT NULL,
    market VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    missing_reason VARCHAR(30) NOT NULL,
    reason_detail VARCHAR(300) NULL,
    detected_at TIMESTAMP NOT NULL,
    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    resolved_at TIMESTAMP NULL,
    PRIMARY KEY (ingestion_batch_id, symbol_id, date),
    CHECK (missing_reason IN ('MARKET_HOLIDAY', 'SYMBOL_SUSPENDED', 'COLLECTION_FAILURE', 'SOURCE_EMPTY', 'UNKNOWN'))
);

CREATE INDEX idx_gap_reason_date ON collection_gap_report(missing_reason, date);
```

## 2.16 factor_snapshot
```sql
CREATE TABLE factor_snapshot (
    snapshot_id UUID NOT NULL,
    date DATE NOT NULL,
    symbol_id UUID NOT NULL,
    code VARCHAR(12) NULL,
    factor_name VARCHAR(100) NOT NULL,
    factor_value NUMERIC(30,12) NOT NULL,
    computed_at TIMESTAMP NOT NULL,
    computed_with_version VARCHAR(64) NOT NULL,
    PRIMARY KEY (snapshot_id, date, symbol_id, factor_name)
);

CREATE INDEX idx_factor_snapshot_lookup ON factor_snapshot(snapshot_id, symbol_id, date);
```

## 2.17 factor_metadata
```sql
CREATE TABLE factor_metadata (
    factor_name VARCHAR(100) PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    frequency VARCHAR(20) NOT NULL,
    lag_days INTEGER NOT NULL DEFAULT 0,
    description TEXT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMP NOT NULL
);
```

## 2.18 view_snapshot_metadata
```sql
CREATE TABLE view_snapshot_metadata (
    snapshot_id UUID PRIMARY KEY,
    snapshot_as_of_date DATE NOT NULL,
    snapshot_data_cutoff_time TIMESTAMP NOT NULL,
    price_view_version VARCHAR(64) NOT NULL,
    factor_view_version VARCHAR(64) NOT NULL,
    adjustment_engine_version VARCHAR(64) NOT NULL,
    included_ingestion_batches_hash CHAR(64) NOT NULL,
    included_source_priority_policy_version VARCHAR(64) NOT NULL,
    require_batch_status VARCHAR(20) NOT NULL DEFAULT 'SUCCESS_ONLY',
    allow_partial_price BOOLEAN NOT NULL DEFAULT FALSE,
    allow_partial_event BOOLEAN NOT NULL DEFAULT FALSE,
    effective_date_preset VARCHAR(40) NOT NULL,
    derived_effective_date_opt_in BOOLEAN NOT NULL,
    status VARCHAR(20) NOT NULL,
    superseded_by UUID NULL,
    staleness_reason VARCHAR(300) NULL,
    created_at TIMESTAMP NOT NULL,
    CHECK (status IN ('ACTIVE', 'STALE', 'REBUILDING', 'ARCHIVED')),
    CHECK (require_batch_status IN ('SUCCESS_ONLY', 'ALLOW_PARTIAL'))
);

CREATE INDEX idx_snapshot_status_created ON view_snapshot_metadata(status, created_at DESC);
```

## 2.19 snapshot_input_batches
```sql
CREATE TABLE snapshot_input_batches (
    snapshot_id UUID NOT NULL,
    ingestion_batch_id UUID NOT NULL,
    pipeline_name VARCHAR(50) NOT NULL,
    source VARCHAR(30) NOT NULL,
    included_at TIMESTAMP NOT NULL,
    PRIMARY KEY (snapshot_id, ingestion_batch_id)
);

CREATE INDEX idx_sib_batch ON snapshot_input_batches(ingestion_batch_id);
```

## 2.20 quality_gate_evaluations
```sql
CREATE TABLE quality_gate_evaluations (
    snapshot_id UUID NOT NULL,
    gate_name VARCHAR(50) NOT NULL,
    metric_name VARCHAR(50) NOT NULL,
    metric_value NUMERIC(10,6) NOT NULL,
    threshold_value NUMERIC(10,6) NOT NULL,
    passed BOOLEAN NOT NULL,
    decision_reason VARCHAR(300) NULL,
    evaluated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (snapshot_id, gate_name, metric_name)
);
```

---

## 3. Phase 2 테이블 DDL

## 3.1 ingestion_partitions
```sql
CREATE TABLE ingestion_partitions (
    partition_id UUID PRIMARY KEY,
    ingestion_batch_id UUID NOT NULL,
    pipeline_name VARCHAR(50) NOT NULL,
    source VARCHAR(30) NOT NULL,
    partition_key VARCHAR(120) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    status VARCHAR(20) NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error_code VARCHAR(50) NULL,
    last_error_message VARCHAR(500) NULL,
    next_retry_at TIMESTAMP NULL,
    started_at TIMESTAMP NULL,
    finished_at TIMESTAMP NULL,
    worker_id VARCHAR(80) NULL,
    metadata JSONB NULL,
    UNIQUE (pipeline_name, source, partition_key, start_date, end_date),
    CHECK (status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED')),
    CHECK (end_date >= start_date)
);

CREATE INDEX idx_partitions_sched ON ingestion_partitions(status, next_retry_at);
CREATE INDEX idx_partitions_batch ON ingestion_partitions(ingestion_batch_id);
```

## 3.2 pipeline_run_lock
```sql
CREATE TABLE pipeline_run_lock (
    lock_name VARCHAR(80) PRIMARY KEY,
    owner_id VARCHAR(80) NOT NULL,
    acquired_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    metadata JSONB NULL
);
```

## 3.3 stale_detection_events
```sql
CREATE TABLE stale_detection_events (
    detection_id UUID PRIMARY KEY,
    old_snapshot_id UUID NOT NULL,
    reason_rule_id VARCHAR(40) NOT NULL,
    reason_key VARCHAR(200) NOT NULL,
    detected_at TIMESTAMP NOT NULL,
    trigger_batch_id UUID NULL,
    trigger_partition_id UUID NULL,
    details JSONB NULL
);
```

---

## 4. Phase 3 테이블 DDL

## 4.1 source_priority_policy
```sql
CREATE TABLE source_priority_policy (
    domain VARCHAR(30) NOT NULL,
    source_name VARCHAR(50) NOT NULL,
    priority_rank INTEGER NOT NULL,
    conflict_policy VARCHAR(30) NOT NULL,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP NULL,
    is_active BOOLEAN NOT NULL,
    note VARCHAR(200) NULL,
    PRIMARY KEY (domain, source_name, valid_from),
    CHECK (conflict_policy IN ('PICK_HIGHER_PRIORITY', 'REQUIRE_CONSENSUS', 'ALERT_ONLY'))
);

CREATE INDEX idx_spp_domain_active ON source_priority_policy(domain, is_active, valid_from DESC);
```

## 4.2 event_type_policy
```sql
CREATE TABLE event_type_policy (
    event_type VARCHAR(40) PRIMARY KEY,
    supported_by_price_engine BOOLEAN NOT NULL,
    supported_by_position_engine BOOLEAN NOT NULL,
    required_fields JSON NOT NULL,
    default_usable_for_price BOOLEAN NOT NULL,
    default_usable_for_factor BOOLEAN NOT NULL,
    default_usable_for_position BOOLEAN NOT NULL,
    default_priority INTEGER NOT NULL,
    validation_rule_set VARCHAR(80) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMP NOT NULL
);
```

## 4.3 legal_entities
```sql
CREATE TABLE legal_entities (
    corp_id UUID PRIMARY KEY,
    dart_corp_code VARCHAR(8) NOT NULL UNIQUE,
    corp_name VARCHAR(200) NOT NULL,
    biz_reg_no VARCHAR(20) NULL,
    country_code VARCHAR(2) NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'DART',
    collected_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NULL
);
```

## 4.4 symbol_entity_mapping
```sql
CREATE TABLE symbol_entity_mapping (
    symbol_id UUID NOT NULL,
    corp_id UUID NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'DART',
    mapping_confidence NUMERIC(5,4) NOT NULL DEFAULT 1.0,
    collected_at TIMESTAMP NOT NULL,
    PRIMARY KEY (symbol_id, corp_id, start_date),
    CHECK (end_date IS NULL OR end_date > start_date)
);

CREATE INDEX idx_sem_symbol_range ON symbol_entity_mapping(symbol_id, start_date, end_date);
CREATE INDEX idx_sem_corp_range ON symbol_entity_mapping(corp_id, start_date, end_date);
```

## 4.5 entity_resolution_history
```sql
CREATE TABLE entity_resolution_history (
    resolution_id UUID PRIMARY KEY,
    source_key VARCHAR(200) NOT NULL,
    source_payload_hash CHAR(64) NULL,
    resolved_symbol_id UUID NULL,
    resolved_corp_id UUID NULL,
    resolution_rule_id VARCHAR(40) NOT NULL,
    resolution_confidence NUMERIC(5,4) NOT NULL,
    manual_override BOOLEAN NOT NULL DEFAULT FALSE,
    resolved_at TIMESTAMP NOT NULL,
    ingestion_batch_id UUID NOT NULL,
    notes VARCHAR(300) NULL
);

CREATE INDEX idx_erh_source_key ON entity_resolution_history(source_key, resolved_at DESC);
```

## 4.6 corp_action_rights
```sql
CREATE TABLE corp_action_rights (
    event_id VARCHAR(120) NOT NULL,
    event_version INTEGER NOT NULL,
    rights_ratio_num NUMERIC(30,10) NOT NULL,
    rights_ratio_den NUMERIC(30,10) NOT NULL,
    subscription_price NUMERIC(20,6) NULL,
    rights_listing_date DATE NULL,
    rights_expiry_date DATE NULL,
    PRIMARY KEY (event_id, event_version),
    CHECK (rights_ratio_den <> 0)
);
```

## 4.7 corp_action_merger
```sql
CREATE TABLE corp_action_merger (
    event_id VARCHAR(120) NOT NULL,
    event_version INTEGER NOT NULL,
    target_symbol_id UUID NULL,
    exchange_ratio_num NUMERIC(30,10) NULL,
    exchange_ratio_den NUMERIC(30,10) NULL,
    merger_type VARCHAR(30) NULL,
    settlement_date DATE NULL,
    PRIMARY KEY (event_id, event_version)
);
```

## 4.8 corp_action_convertible
```sql
CREATE TABLE corp_action_convertible (
    event_id VARCHAR(120) NOT NULL,
    event_version INTEGER NOT NULL,
    conversion_price NUMERIC(20,6) NULL,
    conversion_ratio_num NUMERIC(30,10) NULL,
    conversion_ratio_den NUMERIC(30,10) NULL,
    maturity_date DATE NULL,
    conversion_start_date DATE NULL,
    conversion_end_date DATE NULL,
    PRIMARY KEY (event_id, event_version)
);
```

## 4.9 corp_action_capital_reduction
```sql
CREATE TABLE corp_action_capital_reduction (
    event_id VARCHAR(120) NOT NULL,
    event_version INTEGER NOT NULL,
    reduction_type VARCHAR(30) NOT NULL,
    ratio_num NUMERIC(30,10) NULL,
    ratio_den NUMERIC(30,10) NULL,
    refund_per_share NUMERIC(20,6) NULL,
    settlement_date DATE NULL,
    PRIMARY KEY (event_id, event_version),
    CHECK (reduction_type IN ('FREE', 'PAID'))
);
```

## 4.10 corp_action_entities
```sql
CREATE TABLE corp_action_entities (
    event_id VARCHAR(120) NOT NULL,
    event_version INTEGER NOT NULL,
    source_symbol_id UUID NULL,
    target_symbol_id UUID NULL,
    corp_id UUID NULL,
    role VARCHAR(40) NOT NULL,
    PRIMARY KEY (event_id, event_version, role)
);
```

## 4.11 corp_action_overrides
```sql
CREATE TABLE corp_action_overrides (
    override_id UUID PRIMARY KEY,
    event_id VARCHAR(120) NOT NULL,
    event_version INTEGER NOT NULL,
    field_name VARCHAR(80) NOT NULL,
    old_value VARCHAR(500) NULL,
    new_value VARCHAR(500) NOT NULL,
    reason VARCHAR(300) NOT NULL,
    applied_at TIMESTAMP NOT NULL,
    author VARCHAR(80) NOT NULL,
    ticket_id VARCHAR(80) NULL,
    approved_by VARCHAR(80) NULL,
    approval_at TIMESTAMP NULL
);

CREATE INDEX idx_overrides_event ON corp_action_overrides(event_id, event_version, applied_at DESC);
```

---

## 5. 표준 View / Materialized View DDL

## 5.1 prices_raw_latest
```sql
CREATE VIEW prices_raw_latest AS
WITH latest AS (
    SELECT p.*,
           ROW_NUMBER() OVER (
               PARTITION BY p.symbol_id, p.date, p.source
               ORDER BY p.price_revision_seq DESC
           ) AS rn
    FROM prices_raw p
)
SELECT *
FROM latest
WHERE rn = 1;
```

## 5.2 corp_actions_latest
```sql
CREATE VIEW corp_actions_latest AS
WITH latest AS (
    SELECT c.*,
           ROW_NUMBER() OVER (
               PARTITION BY c.event_id
               ORDER BY c.event_version DESC
           ) AS rn
    FROM corp_actions c
)
SELECT *
FROM latest
WHERE rn = 1;
```

## 5.3 raw_price_view
```sql
CREATE VIEW raw_price_view AS
SELECT p.*
FROM prices_raw_latest p;
```

## 5.4 dividend_cashflow_view
```sql
CREATE VIEW dividend_cashflow_view AS
SELECT ca.symbol_id,
       ca.event_id,
       ca.event_version,
       ca.ex_date,
       cad.record_date,
       cad.pay_date,
       cad.cash_dividend_per_share,
       cad.currency
FROM corp_actions_latest ca
JOIN corp_action_dividend cad
  ON ca.event_id = cad.event_id
 AND ca.event_version = cad.event_version
WHERE ca.event_type = 'CASH_DIVIDEND'
  AND ca.event_validation_status = 'VALID';
```

## 5.5 position_transform_view
```sql
CREATE VIEW position_transform_view AS
SELECT ca.symbol_id,
       ca.event_id,
       ca.event_version,
       ca.event_type,
       ca.effective_date,
       cae.target_symbol_id,
       cam.exchange_ratio_num,
       cam.exchange_ratio_den,
       car.rights_ratio_num,
       car.rights_ratio_den,
       ccr.refund_per_share
FROM corp_actions_latest ca
LEFT JOIN corp_action_entities cae
  ON ca.event_id = cae.event_id
 AND ca.event_version = cae.event_version
LEFT JOIN corp_action_merger cam
  ON ca.event_id = cam.event_id
 AND ca.event_version = cam.event_version
LEFT JOIN corp_action_rights car
  ON ca.event_id = car.event_id
 AND ca.event_version = car.event_version
LEFT JOIN corp_action_capital_reduction ccr
  ON ca.event_id = ccr.event_id
 AND ca.event_version = ccr.event_version
WHERE ca.event_type IN ('RIGHTS', 'MERGER', 'SPINOFF', 'PAID_CAPITAL_REDUCTION');
```

## 5.6 universe_asof_view (운영은 파라미터 쿼리 권장)
```sql
CREATE VIEW universe_asof_view AS
SELECT s.symbol_id,
       s.market,
       s.code,
       s.name,
       CURRENT_DATE AS as_of_date
FROM symbols s
LEFT JOIN symbol_trading_state sts
  ON sts.symbol_id = s.symbol_id
 AND sts.start_date <= CURRENT_DATE
 AND (sts.end_date IS NULL OR sts.end_date > CURRENT_DATE)
WHERE s.listing_date <= CURRENT_DATE
  AND (s.delisting_date IS NULL OR s.delisting_date > CURRENT_DATE)
  AND COALESCE(sts.trading_state, 'LISTED') <> 'SUSPENDED';
```

## 5.7 adjusted_price_view (snapshot별)
```sql
CREATE VIEW adjusted_price_view AS
SELECT p.symbol_id,
       p.date,
       p.market,
       p.open,
       p.high,
       p.low,
       p.close AS raw_close,
       dca.cumulative_factor,
       p.close * dca.cumulative_factor AS adjusted_close
FROM prices_raw_latest p
JOIN daily_cumulative_adjustment dca
  ON dca.symbol_id = p.symbol_id
 AND dca.date = p.date;
```

## 5.8 strategy_price_view
```sql
CREATE VIEW strategy_price_view AS
SELECT ap.symbol_id,
       ap.date,
       ap.adjusted_close,
       dc.cash_dividend_per_share,
       dc.currency
FROM adjusted_price_view ap
LEFT JOIN dividend_cashflow_view dc
  ON dc.symbol_id = ap.symbol_id
 AND dc.ex_date = ap.date;
```

---

## 6. 제약 강화를 위한 추가 권장 SQL

## 6.1 구간 중첩 금지 (PostgreSQL exclusion)
```sql
-- symbol_code_history
ALTER TABLE symbol_code_history
ADD CONSTRAINT ex_symbol_code_no_overlap
EXCLUDE USING gist (
    symbol_id WITH =,
    daterange(start_date, COALESCE(end_date, '9999-12-31'::date), '[)') WITH &&
);

-- symbol_trading_state
ALTER TABLE symbol_trading_state
ADD CONSTRAINT ex_symbol_state_no_overlap
EXCLUDE USING gist (
    symbol_id WITH =,
    daterange(start_date, COALESCE(end_date, '9999-12-31'::date), '[)') WITH &&
);
```

## 6.2 revision 연속성 검증용 뷰
```sql
CREATE VIEW prices_revision_gap_check AS
WITH seq AS (
    SELECT symbol_id,
           date,
           source,
           price_revision_seq,
           LAG(price_revision_seq) OVER (
               PARTITION BY symbol_id, date, source
               ORDER BY price_revision_seq
           ) AS prev_seq
    FROM prices_raw
)
SELECT *
FROM seq
WHERE (prev_seq IS NULL AND price_revision_seq <> 1)
   OR (prev_seq IS NOT NULL AND price_revision_seq <> prev_seq + 1);
```

---

## 7. 삭제/수정 정책
- 원장 테이블: UPDATE/DELETE 금지
- 파생/운영 테이블: 정책에 따라 UPDATE 허용 가능
- snapshot 관련 테이블: 기존 snapshot row 논리 상태 전환만 허용

---

## 8. 문서/코드 동기화 규칙
- 신규 테이블/컬럼 추가 시 반드시 `SCHEMA.md` 먼저 수정
- 마이그레이션 파일명은 Phase를 명시 (`phase1_`, `phase2_`, `phase3_`)
- view 변경 시 버전 문자열(`price_view_version` 등) 증가 필수

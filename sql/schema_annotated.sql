-- SCHEMA_ANNOTATED.sql
-- 모든 테이블 컬럼에 의미 주석을 포함한 DDL

CREATE EXTENSION IF NOT EXISTS btree_gist;

-- =====================================================
-- Phase 1
-- =====================================================

CREATE TABLE symbols (
    symbol_id UUID PRIMARY KEY,                      -- 내부 증권 식별자
    market VARCHAR(20) NOT NULL,                     -- 시장(KOSPI/KOSDAQ/KONEX)
    code VARCHAR(12) NOT NULL,                       -- 거래소 종목코드
    name VARCHAR(200) NOT NULL,                      -- 종목명
    instrument_type VARCHAR(30) NOT NULL DEFAULT 'EQUITY', -- 자산 유형
    listing_date DATE NOT NULL,                      -- 상장일
    delisting_date DATE NULL,                        -- 상장폐지일
    is_active BOOLEAN NOT NULL DEFAULT TRUE,         -- 현재 활성 여부
    source VARCHAR(30) NOT NULL DEFAULT 'KRX',       -- 데이터 출처
    collected_at TIMESTAMP NOT NULL,                 -- 수집 시각(UTC)
    updated_at TIMESTAMP NULL,                       -- 최신 갱신 시각(UTC)
    UNIQUE (market, code, listing_date),
    CHECK (delisting_date IS NULL OR delisting_date >= listing_date)
);

CREATE TABLE symbol_code_history (
    symbol_id UUID NOT NULL,                         -- symbols.symbol_id
    code VARCHAR(12) NOT NULL,                       -- 과거/현재 종목코드
    market VARCHAR(20) NOT NULL,                     -- 코드가 속한 시장
    start_date DATE NOT NULL,                        -- 코드 유효 시작일(inclusive)
    end_date DATE NULL,                              -- 코드 유효 종료일(exclusive)
    reason VARCHAR(50) NOT NULL DEFAULT 'KRX_SYNC', -- 변경 사유
    source VARCHAR(30) NOT NULL DEFAULT 'KRX',       -- 데이터 출처
    collected_at TIMESTAMP NOT NULL,                 -- 수집 시각
    PRIMARY KEY (symbol_id, code, start_date),
    CHECK (end_date IS NULL OR end_date > start_date)
);

CREATE TABLE symbol_trading_state (
    symbol_id UUID NOT NULL,                         -- symbols.symbol_id
    trading_state VARCHAR(20) NOT NULL,              -- LISTED/SUSPENDED/RESUMED/DELISTED
    start_date DATE NOT NULL,                        -- 상태 시작일
    end_date DATE NULL,                              -- 상태 종료일
    reason VARCHAR(200) NULL,                        -- 상태 변경 사유
    source VARCHAR(30) NOT NULL DEFAULT 'KRX',       -- 데이터 출처
    collected_at TIMESTAMP NOT NULL,                 -- 수집 시각
    PRIMARY KEY (symbol_id, trading_state, start_date),
    CHECK (trading_state IN ('LISTED', 'SUSPENDED', 'RESUMED', 'DELISTED')),
    CHECK (end_date IS NULL OR end_date > start_date)
);

CREATE TABLE symbol_market_state (
    symbol_id UUID NOT NULL,                         -- symbols.symbol_id
    market VARCHAR(20) NOT NULL,                     -- 소속 시장
    start_date DATE NOT NULL,                        -- 시장 소속 시작일
    end_date DATE NULL,                              -- 시장 소속 종료일
    reason VARCHAR(100) NULL,                        -- 이동 사유
    source VARCHAR(30) NOT NULL DEFAULT 'KRX',       -- 데이터 출처
    collected_at TIMESTAMP NOT NULL,                 -- 수집 시각
    PRIMARY KEY (symbol_id, market, start_date),
    CHECK (end_date IS NULL OR end_date > start_date)
);

CREATE TABLE trading_calendar (
    market VARCHAR(20) NOT NULL,                     -- 시장
    date DATE NOT NULL,                              -- 날짜
    is_open BOOLEAN NOT NULL,                        -- 개장 여부
    holiday_name VARCHAR(100) NULL,                  -- 휴장 사유명
    source VARCHAR(30) NOT NULL DEFAULT 'KRX',       -- 출처
    collected_at TIMESTAMP NOT NULL,                 -- 수집 시각
    PRIMARY KEY (market, date)
);

CREATE TABLE ingestion_batches (
    ingestion_batch_id UUID PRIMARY KEY,             -- 배치 식별자
    pipeline_name VARCHAR(50) NOT NULL,              -- PRICE_DAILY/EVENT_DAILY/SNAPSHOT_BUILD
    source VARCHAR(30) NOT NULL,                     -- KRX/DART/SYSTEM
    start_date DATE NOT NULL,                        -- 대상 시작일
    end_date DATE NOT NULL,                          -- 대상 종료일
    started_at TIMESTAMP NOT NULL,                   -- 실행 시작 시각
    finished_at TIMESTAMP NULL,                      -- 실행 종료 시각
    status VARCHAR(20) NOT NULL,                     -- SUCCESS/PARTIAL/FAILED
    success_count BIGINT NOT NULL DEFAULT 0,         -- 성공 건수
    failure_count BIGINT NOT NULL DEFAULT 0,         -- 실패 건수
    retry_count BIGINT NOT NULL DEFAULT 0,           -- 재시도 횟수
    warning_count BIGINT NOT NULL DEFAULT 0,         -- 경고 건수
    error_summary TEXT NULL,                         -- 오류 요약
    metadata JSONB NULL,                             -- 부가 메타
    CHECK (status IN ('SUCCESS', 'PARTIAL', 'FAILED')),
    CHECK (end_date >= start_date)
);

CREATE TABLE prices_raw (
    symbol_id UUID NOT NULL,                         -- symbols.symbol_id
    date DATE NOT NULL,                              -- 거래일
    market VARCHAR(20) NOT NULL,                     -- 시장
    open NUMERIC(20,6) NOT NULL,                     -- 시가
    high NUMERIC(20,6) NOT NULL,                     -- 고가
    low NUMERIC(20,6) NOT NULL,                      -- 저가
    close NUMERIC(20,6) NOT NULL,                    -- 종가
    volume BIGINT NOT NULL,                          -- 거래량
    value NUMERIC(28,6) NULL,                        -- 거래대금
    source VARCHAR(30) NOT NULL,                     -- 출처(KRX)
    source_row_id VARCHAR(120) NULL,                 -- 원천 행 식별자(있다면)
    logical_hash CHAR(64) NOT NULL,                  -- 내용 해시(정정 감지)
    price_revision_seq INTEGER NOT NULL,             -- revision 시퀀스
    price_validation_status VARCHAR(20) NOT NULL,    -- VALID/INVALID
    validation_error_code VARCHAR(50) NULL,          -- 검증 실패 코드
    collected_at TIMESTAMP NOT NULL,                 -- 수집 시각
    ingestion_batch_id UUID NOT NULL,                -- 배치 ID
    PRIMARY KEY (symbol_id, date, source, price_revision_seq),
    CHECK (price_revision_seq >= 1),
    CHECK (price_validation_status IN ('VALID', 'INVALID')),
    CHECK (volume >= 0)
);

CREATE TABLE pending_symbol_prices (
    code VARCHAR(12) NOT NULL,                       -- 아직 symbol_id 미해결 코드
    date DATE NOT NULL,                              -- 거래일
    market VARCHAR(20) NOT NULL,                     -- 시장
    open NUMERIC(20,6) NOT NULL,                     -- 시가
    high NUMERIC(20,6) NOT NULL,                     -- 고가
    low NUMERIC(20,6) NOT NULL,                      -- 저가
    close NUMERIC(20,6) NOT NULL,                    -- 종가
    volume BIGINT NOT NULL,                          -- 거래량
    value NUMERIC(28,6) NULL,                        -- 거래대금
    source VARCHAR(30) NOT NULL,                     -- 출처
    source_row_id VARCHAR(120) NULL,                 -- 원천 행 식별자
    collected_at TIMESTAMP NOT NULL,                 -- 수집 시각
    ingestion_batch_id UUID NOT NULL,                -- 배치 ID
    resolved_symbol_id UUID NULL,                    -- 해소된 symbol_id
    resolved_reason VARCHAR(100) NULL,               -- 해소 방식 설명
    resolved_at TIMESTAMP NULL,                      -- 해소 시각
    PRIMARY KEY (code, date, source, collected_at)
);

CREATE TABLE corp_actions (
    event_id VARCHAR(120) NOT NULL,                  -- 내부 이벤트 ID
    event_version INTEGER NOT NULL,                  -- 이벤트 버전
    symbol_id UUID NOT NULL,                         -- 대상 symbol_id
    event_type VARCHAR(40) NOT NULL,                 -- 이벤트 타입
    announce_date DATE NULL,                         -- 공시일
    ex_date DATE NULL,                               -- 권리락/배당락일
    effective_date DATE NULL,                        -- 시스템 효력일
    effective_date_source VARCHAR(40) NOT NULL,      -- 효력일 출처
    effective_date_confidence VARCHAR(10) NOT NULL DEFAULT 'MED', -- 효력일 신뢰도
    effective_date_reason VARCHAR(50) NOT NULL DEFAULT 'SOURCE_EXPLICIT', -- 효력일 산정 근거
    event_priority INTEGER NOT NULL DEFAULT 100,     -- 동일일 정렬 우선순위
    source_event_id VARCHAR(120) NULL,               -- 원천 이벤트 ID(rcept_no)
    source VARCHAR(30) NOT NULL DEFAULT 'DART',      -- 출처
    event_validation_status VARCHAR(30) NOT NULL,    -- VALID/INCOMPLETE/ECONOMICALLY_COMPLEX
    usable_for_price BOOLEAN NOT NULL,               -- 가격엔진 사용 가능 여부
    usable_for_factor BOOLEAN NOT NULL,              -- 팩터엔진 사용 가능 여부
    usable_for_position BOOLEAN NOT NULL,            -- 포지션엔진 사용 가능 여부
    canonical_payload_hash CHAR(64) NULL,            -- 이벤트 canonical hash
    event_change_reason VARCHAR(200) NULL,           -- version 증가 사유
    collected_at TIMESTAMP NOT NULL,                 -- 수집 시각
    ingestion_batch_id UUID NOT NULL,                -- 배치 ID
    PRIMARY KEY (event_id, event_version),
    CHECK (event_version >= 1),
    CHECK (effective_date_source IN ('EXPLICIT_SOURCE', 'DERIVED_NEXT_TRADING_DAY', 'UNKNOWN')),
    CHECK (effective_date_confidence IN ('HIGH', 'MED', 'LOW')),
    CHECK (event_validation_status IN ('VALID', 'INCOMPLETE', 'ECONOMICALLY_COMPLEX'))
);

CREATE TABLE corp_action_ratio (
    event_id VARCHAR(120) NOT NULL,                  -- 이벤트 ID
    event_version INTEGER NOT NULL,                  -- 이벤트 버전
    ratio_num NUMERIC(30,10) NOT NULL,               -- 비율 분자
    ratio_den NUMERIC(30,10) NOT NULL,               -- 비율 분모
    ratio_type VARCHAR(30) NOT NULL,                 -- SPLIT/BONUS 등
    unit_note VARCHAR(100) NULL,                     -- 단위/해석 보조
    PRIMARY KEY (event_id, event_version),
    CHECK (ratio_den <> 0)
);

CREATE TABLE corp_action_dividend (
    event_id VARCHAR(120) NOT NULL,                  -- 이벤트 ID
    event_version INTEGER NOT NULL,                  -- 이벤트 버전
    cash_dividend_per_share NUMERIC(20,6) NULL,      -- 주당 현금배당
    stock_dividend_ratio_num NUMERIC(30,10) NULL,    -- 주식배당 분자
    stock_dividend_ratio_den NUMERIC(30,10) NULL,    -- 주식배당 분모
    record_date DATE NULL,                           -- 기준일
    pay_date DATE NULL,                              -- 지급일
    currency VARCHAR(10) NULL,                       -- 통화
    tax_note VARCHAR(100) NULL,                      -- 세금/비고
    PRIMARY KEY (event_id, event_version)
);

CREATE TABLE adjustment_factors (
    snapshot_id UUID NOT NULL,                       -- 스냅샷 ID
    symbol_id UUID NOT NULL,                         -- symbol_id
    event_id VARCHAR(120) NOT NULL,                  -- 이벤트 ID
    event_version INTEGER NOT NULL,                  -- 이벤트 버전
    effective_date DATE NOT NULL,                    -- 효력일
    adjustment_factor NUMERIC(38,18) NOT NULL,       -- 이벤트 factor
    adjustment_status VARCHAR(40) NOT NULL,          -- APPLIED/SKIPPED_*
    adjustment_skip_reason VARCHAR(120) NULL,        -- skip 사유
    rule_id VARCHAR(40) NOT NULL,                    -- 적용 규칙 ID
    computed_at TIMESTAMP NOT NULL,                  -- 계산 시각
    PRIMARY KEY (snapshot_id, symbol_id, event_id, event_version),
    CHECK (adjustment_status IN ('APPLIED', 'SKIPPED_REQUIRES_POSITION_ENGINE', 'SKIPPED_INSUFFICIENT_DATA', 'SKIPPED_POLICY'))
);

CREATE TABLE daily_cumulative_adjustment (
    snapshot_id UUID NOT NULL,                       -- 스냅샷 ID
    symbol_id UUID NOT NULL,                         -- symbol_id
    date DATE NOT NULL,                              -- 가격일
    cumulative_factor NUMERIC(38,18) NOT NULL,       -- 누적 factor
    computed_at TIMESTAMP NOT NULL,                  -- 계산 시각
    PRIMARY KEY (snapshot_id, symbol_id, date)
);

CREATE TABLE adjustment_evaluation_log (
    snapshot_id UUID NOT NULL,                       -- 스냅샷 ID
    symbol_id UUID NOT NULL,                         -- symbol_id
    event_id VARCHAR(120) NOT NULL,                  -- 이벤트 ID
    event_version INTEGER NOT NULL,                  -- 이벤트 버전
    event_type VARCHAR(40) NOT NULL,                 -- 이벤트 타입
    effective_date DATE NULL,                        -- 효력일
    adjustment_status VARCHAR(40) NOT NULL,          -- 적용/스킵 상태
    adjustment_skip_reason VARCHAR(120) NULL,        -- 스킵 사유
    rule_id VARCHAR(40) NOT NULL,                    -- 판정 규칙 ID
    adjustment_engine_version VARCHAR(64) NOT NULL,  -- 엔진 버전
    evaluated_at TIMESTAMP NOT NULL,                 -- 평가 시각
    PRIMARY KEY (snapshot_id, symbol_id, event_id, event_version)
);

CREATE TABLE collection_gap_report (
    ingestion_batch_id UUID NOT NULL,                -- 배치 ID
    symbol_id UUID NOT NULL,                         -- symbol_id
    market VARCHAR(20) NOT NULL,                     -- 시장
    date DATE NOT NULL,                              -- 결측 날짜
    missing_reason VARCHAR(30) NOT NULL,             -- 결측 분류
    reason_detail VARCHAR(300) NULL,                 -- 상세 원인
    detected_at TIMESTAMP NOT NULL,                  -- 탐지 시각
    resolved BOOLEAN NOT NULL DEFAULT FALSE,         -- 해소 여부
    resolved_at TIMESTAMP NULL,                      -- 해소 시각
    PRIMARY KEY (ingestion_batch_id, symbol_id, date),
    CHECK (missing_reason IN ('MARKET_HOLIDAY', 'SYMBOL_SUSPENDED', 'COLLECTION_FAILURE', 'SOURCE_EMPTY', 'UNKNOWN'))
);

CREATE TABLE factor_snapshot (
    snapshot_id UUID NOT NULL,                       -- 스냅샷 ID
    date DATE NOT NULL,                              -- 기준일
    symbol_id UUID NOT NULL,                         -- symbol_id
    code VARCHAR(12) NULL,                           -- 디버그용 code
    factor_name VARCHAR(100) NOT NULL,               -- 팩터명
    factor_value NUMERIC(30,12) NOT NULL,            -- 팩터값
    computed_at TIMESTAMP NOT NULL,                  -- 계산 시각
    computed_with_version VARCHAR(64) NOT NULL,      -- 계산 로직 버전
    PRIMARY KEY (snapshot_id, date, symbol_id, factor_name)
);

CREATE TABLE factor_metadata (
    factor_name VARCHAR(100) PRIMARY KEY,            -- 팩터명
    source VARCHAR(50) NOT NULL,                     -- 소스
    frequency VARCHAR(20) NOT NULL,                  -- 주기
    lag_days INTEGER NOT NULL DEFAULT 0,             -- lag 일수
    description TEXT NULL,                           -- 설명
    is_active BOOLEAN NOT NULL DEFAULT TRUE,         -- 활성 여부
    updated_at TIMESTAMP NOT NULL                    -- 갱신 시각
);

CREATE TABLE view_snapshot_metadata (
    snapshot_id UUID PRIMARY KEY,                    -- 스냅샷 ID
    snapshot_as_of_date DATE NOT NULL,               -- 분석 기준일
    snapshot_data_cutoff_time TIMESTAMP NOT NULL,    -- 데이터 cutoff
    price_view_version VARCHAR(64) NOT NULL,         -- price view 버전
    factor_view_version VARCHAR(64) NOT NULL,        -- factor view 버전
    adjustment_engine_version VARCHAR(64) NOT NULL,  -- adjustment 엔진 버전
    included_ingestion_batches_hash CHAR(64) NOT NULL, -- 입력 배치 집합 hash
    included_source_priority_policy_version VARCHAR(64) NOT NULL, -- 소스정책 버전
    require_batch_status VARCHAR(20) NOT NULL DEFAULT 'SUCCESS_ONLY', -- 배치 요구 상태
    allow_partial_price BOOLEAN NOT NULL DEFAULT FALSE, -- 가격 partial 허용
    allow_partial_event BOOLEAN NOT NULL DEFAULT FALSE, -- 이벤트 partial 허용
    effective_date_preset VARCHAR(40) NOT NULL,      -- 효력일 프리셋
    derived_effective_date_opt_in BOOLEAN NOT NULL,  -- 파생 효력일 허용 여부
    status VARCHAR(20) NOT NULL,                     -- ACTIVE/STALE/REBUILDING/ARCHIVED
    superseded_by UUID NULL,                         -- 대체 snapshot_id
    staleness_reason VARCHAR(300) NULL,              -- stale 사유
    created_at TIMESTAMP NOT NULL,                   -- 생성 시각
    CHECK (status IN ('ACTIVE', 'STALE', 'REBUILDING', 'ARCHIVED')),
    CHECK (require_batch_status IN ('SUCCESS_ONLY', 'ALLOW_PARTIAL'))
);

CREATE TABLE snapshot_input_batches (
    snapshot_id UUID NOT NULL,                       -- 스냅샷 ID
    ingestion_batch_id UUID NOT NULL,                -- 포함된 배치 ID
    pipeline_name VARCHAR(50) NOT NULL,              -- 파이프라인명
    source VARCHAR(30) NOT NULL,                     -- 소스
    included_at TIMESTAMP NOT NULL,                  -- 포함 시각
    PRIMARY KEY (snapshot_id, ingestion_batch_id)
);

CREATE TABLE quality_gate_evaluations (
    snapshot_id UUID NOT NULL,                       -- 스냅샷 ID
    gate_name VARCHAR(50) NOT NULL,                  -- 게이트명
    metric_name VARCHAR(50) NOT NULL,                -- 메트릭명
    metric_value NUMERIC(10,6) NOT NULL,             -- 메트릭값
    threshold_value NUMERIC(10,6) NOT NULL,          -- 임계값
    passed BOOLEAN NOT NULL,                         -- 통과 여부
    decision_reason VARCHAR(300) NULL,               -- 판정 사유
    evaluated_at TIMESTAMP NOT NULL,                 -- 평가 시각
    PRIMARY KEY (snapshot_id, gate_name, metric_name)
);

-- =====================================================
-- Phase 2
-- =====================================================

CREATE TABLE ingestion_partitions (
    partition_id UUID PRIMARY KEY,                   -- 파티션 ID
    ingestion_batch_id UUID NOT NULL,                -- 상위 배치 ID
    pipeline_name VARCHAR(50) NOT NULL,              -- 파이프라인명
    source VARCHAR(30) NOT NULL,                     -- 소스
    partition_key VARCHAR(120) NOT NULL,             -- 실행 단위 키
    start_date DATE NOT NULL,                        -- 파티션 시작일
    end_date DATE NOT NULL,                          -- 파티션 종료일
    status VARCHAR(20) NOT NULL,                     -- PENDING/RUNNING/SUCCESS/FAILED
    retry_count INTEGER NOT NULL DEFAULT 0,          -- 재시도 횟수
    last_error_code VARCHAR(50) NULL,                -- 마지막 오류 코드
    last_error_message VARCHAR(500) NULL,            -- 마지막 오류 메시지
    next_retry_at TIMESTAMP NULL,                    -- 다음 재시도 시각
    started_at TIMESTAMP NULL,                       -- 실행 시작 시각
    finished_at TIMESTAMP NULL,                      -- 실행 종료 시각
    worker_id VARCHAR(80) NULL,                      -- 처리 워커 ID
    metadata JSONB NULL,                             -- 부가 메타
    UNIQUE (pipeline_name, source, partition_key, start_date, end_date),
    CHECK (status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED')),
    CHECK (end_date >= start_date)
);

CREATE TABLE pipeline_run_lock (
    lock_name VARCHAR(80) PRIMARY KEY,               -- 락 이름
    owner_id VARCHAR(80) NOT NULL,                   -- 락 소유자
    acquired_at TIMESTAMP NOT NULL,                  -- 획득 시각
    expires_at TIMESTAMP NOT NULL,                   -- 만료 시각
    metadata JSONB NULL                              -- 메타데이터
);

CREATE TABLE stale_detection_events (
    detection_id UUID PRIMARY KEY,                   -- stale 탐지 ID
    old_snapshot_id UUID NOT NULL,                   -- 기존 snapshot
    reason_rule_id VARCHAR(40) NOT NULL,             -- 탐지 규칙 ID
    reason_key VARCHAR(200) NOT NULL,                -- 탐지 키
    detected_at TIMESTAMP NOT NULL,                  -- 탐지 시각
    trigger_batch_id UUID NULL,                      -- 유발 배치
    trigger_partition_id UUID NULL,                  -- 유발 파티션
    details JSONB NULL                               -- 상세 정보
);

-- =====================================================
-- Phase 3
-- =====================================================

CREATE TABLE source_priority_policy (
    domain VARCHAR(30) NOT NULL,                     -- PRICE/EVENT/CALENDAR
    source_name VARCHAR(50) NOT NULL,                -- KRX/NAVER/...
    priority_rank INTEGER NOT NULL,                  -- 낮을수록 우선
    conflict_policy VARCHAR(30) NOT NULL,            -- PICK_HIGHER_PRIORITY/REQUIRE_CONSENSUS/ALERT_ONLY
    valid_from TIMESTAMP NOT NULL,                   -- 정책 시작
    valid_to TIMESTAMP NULL,                         -- 정책 종료
    is_active BOOLEAN NOT NULL,                      -- 활성 여부
    note VARCHAR(200) NULL,                          -- 비고
    PRIMARY KEY (domain, source_name, valid_from),
    CHECK (conflict_policy IN ('PICK_HIGHER_PRIORITY', 'REQUIRE_CONSENSUS', 'ALERT_ONLY'))
);

CREATE TABLE event_type_policy (
    event_type VARCHAR(40) PRIMARY KEY,              -- 이벤트 타입
    supported_by_price_engine BOOLEAN NOT NULL,      -- 가격엔진 지원 여부
    supported_by_position_engine BOOLEAN NOT NULL,   -- 포지션엔진 지원 여부
    required_fields JSON NOT NULL,                   -- 필수 필드 정의
    default_usable_for_price BOOLEAN NOT NULL,       -- 기본 가격 사용 여부
    default_usable_for_factor BOOLEAN NOT NULL,      -- 기본 팩터 사용 여부
    default_usable_for_position BOOLEAN NOT NULL,    -- 기본 포지션 사용 여부
    default_priority INTEGER NOT NULL,               -- 기본 우선순위
    validation_rule_set VARCHAR(80) NOT NULL,        -- 검증 룰셋명
    is_active BOOLEAN NOT NULL DEFAULT TRUE,         -- 활성 여부
    updated_at TIMESTAMP NOT NULL                    -- 갱신 시각
);

CREATE TABLE legal_entities (
    corp_id UUID PRIMARY KEY,                        -- 법인 ID
    dart_corp_code VARCHAR(8) NOT NULL UNIQUE,       -- DART 법인코드
    corp_name VARCHAR(200) NOT NULL,                 -- 법인명
    biz_reg_no VARCHAR(20) NULL,                     -- 사업자번호
    country_code VARCHAR(2) NULL,                    -- 국가코드
    source VARCHAR(30) NOT NULL DEFAULT 'DART',      -- 출처
    collected_at TIMESTAMP NOT NULL,                 -- 수집 시각
    updated_at TIMESTAMP NULL                        -- 갱신 시각
);

CREATE TABLE symbol_entity_mapping (
    symbol_id UUID NOT NULL,                         -- symbol_id
    corp_id UUID NOT NULL,                           -- corp_id
    start_date DATE NOT NULL,                        -- 매핑 시작일
    end_date DATE NULL,                              -- 매핑 종료일
    source VARCHAR(30) NOT NULL DEFAULT 'DART',      -- 출처
    mapping_confidence NUMERIC(5,4) NOT NULL DEFAULT 1.0, -- 매핑 신뢰도
    collected_at TIMESTAMP NOT NULL,                 -- 수집 시각
    PRIMARY KEY (symbol_id, corp_id, start_date),
    CHECK (end_date IS NULL OR end_date > start_date)
);

CREATE TABLE entity_resolution_history (
    resolution_id UUID PRIMARY KEY,                  -- 해석 이력 ID
    source_key VARCHAR(200) NOT NULL,                -- 입력 키
    source_payload_hash CHAR(64) NULL,               -- 입력 payload hash
    resolved_symbol_id UUID NULL,                    -- 해석된 symbol_id
    resolved_corp_id UUID NULL,                      -- 해석된 corp_id
    resolution_rule_id VARCHAR(40) NOT NULL,         -- 적용 룰 ID
    resolution_confidence NUMERIC(5,4) NOT NULL,     -- 신뢰도
    manual_override BOOLEAN NOT NULL DEFAULT FALSE,  -- 수동 개입 여부
    resolved_at TIMESTAMP NOT NULL,                  -- 해석 시각
    ingestion_batch_id UUID NOT NULL,                -- 배치 ID
    notes VARCHAR(300) NULL                          -- 메모
);

CREATE TABLE corp_action_rights (
    event_id VARCHAR(120) NOT NULL,                  -- 이벤트 ID
    event_version INTEGER NOT NULL,                  -- 이벤트 버전
    rights_ratio_num NUMERIC(30,10) NOT NULL,        -- 권리비율 분자
    rights_ratio_den NUMERIC(30,10) NOT NULL,        -- 권리비율 분모
    subscription_price NUMERIC(20,6) NULL,           -- 청약가
    rights_listing_date DATE NULL,                   -- 권리증권 상장일
    rights_expiry_date DATE NULL,                    -- 권리 만료일
    PRIMARY KEY (event_id, event_version),
    CHECK (rights_ratio_den <> 0)
);

CREATE TABLE corp_action_merger (
    event_id VARCHAR(120) NOT NULL,                  -- 이벤트 ID
    event_version INTEGER NOT NULL,                  -- 이벤트 버전
    target_symbol_id UUID NULL,                      -- 대상 symbol
    exchange_ratio_num NUMERIC(30,10) NULL,          -- 교환비율 분자
    exchange_ratio_den NUMERIC(30,10) NULL,          -- 교환비율 분모
    merger_type VARCHAR(30) NULL,                    -- 합병 타입
    settlement_date DATE NULL,                       -- 정산일
    PRIMARY KEY (event_id, event_version)
);

CREATE TABLE corp_action_convertible (
    event_id VARCHAR(120) NOT NULL,                  -- 이벤트 ID
    event_version INTEGER NOT NULL,                  -- 이벤트 버전
    conversion_price NUMERIC(20,6) NULL,             -- 전환가액
    conversion_ratio_num NUMERIC(30,10) NULL,        -- 전환비율 분자
    conversion_ratio_den NUMERIC(30,10) NULL,        -- 전환비율 분모
    maturity_date DATE NULL,                         -- 만기일
    conversion_start_date DATE NULL,                 -- 전환 시작일
    conversion_end_date DATE NULL,                   -- 전환 종료일
    PRIMARY KEY (event_id, event_version)
);

CREATE TABLE corp_action_capital_reduction (
    event_id VARCHAR(120) NOT NULL,                  -- 이벤트 ID
    event_version INTEGER NOT NULL,                  -- 이벤트 버전
    reduction_type VARCHAR(30) NOT NULL,             -- FREE/PAID
    ratio_num NUMERIC(30,10) NULL,                   -- 감자비율 분자
    ratio_den NUMERIC(30,10) NULL,                   -- 감자비율 분모
    refund_per_share NUMERIC(20,6) NULL,             -- 주당 환급금
    settlement_date DATE NULL,                       -- 정산일
    PRIMARY KEY (event_id, event_version),
    CHECK (reduction_type IN ('FREE', 'PAID'))
);

CREATE TABLE corp_action_entities (
    event_id VARCHAR(120) NOT NULL,                  -- 이벤트 ID
    event_version INTEGER NOT NULL,                  -- 이벤트 버전
    source_symbol_id UUID NULL,                      -- 원 종목
    target_symbol_id UUID NULL,                      -- 대상 종목
    corp_id UUID NULL,                               -- 관련 법인
    role VARCHAR(40) NOT NULL,                       -- 역할(ISSUER/TARGET/...)
    PRIMARY KEY (event_id, event_version, role)
);

CREATE TABLE corp_action_overrides (
    override_id UUID PRIMARY KEY,                    -- override ID
    event_id VARCHAR(120) NOT NULL,                  -- 이벤트 ID
    event_version INTEGER NOT NULL,                  -- 대상 버전
    field_name VARCHAR(80) NOT NULL,                 -- 수정 필드명
    old_value VARCHAR(500) NULL,                     -- 변경 전 값
    new_value VARCHAR(500) NOT NULL,                 -- 변경 후 값
    reason VARCHAR(300) NOT NULL,                    -- 사유
    applied_at TIMESTAMP NOT NULL,                   -- 적용 시각
    author VARCHAR(80) NOT NULL,                     -- 작업자
    ticket_id VARCHAR(80) NULL,                      -- 이슈/티켓 ID
    approved_by VARCHAR(80) NULL,                    -- 승인자
    approval_at TIMESTAMP NULL                       -- 승인 시각
);

-- =====================================================
-- 인덱스 (핵심)
-- =====================================================
CREATE INDEX idx_symbols_code_market ON symbols(code, market);
CREATE INDEX idx_symbols_active ON symbols(is_active, market);
CREATE INDEX idx_sch_code_market_range ON symbol_code_history(code, market, start_date, end_date);
CREATE INDEX idx_sch_symbol_range ON symbol_code_history(symbol_id, start_date, end_date);
CREATE INDEX idx_sts_symbol_range ON symbol_trading_state(symbol_id, start_date, end_date);
CREATE INDEX idx_sts_state_date ON symbol_trading_state(trading_state, start_date);
CREATE INDEX idx_sms_symbol_range ON symbol_market_state(symbol_id, start_date, end_date);
CREATE INDEX idx_calendar_open ON trading_calendar(market, date, is_open);
CREATE INDEX idx_batches_pipeline_time ON ingestion_batches(pipeline_name, started_at DESC);
CREATE INDEX idx_batches_status_time ON ingestion_batches(status, started_at DESC);
CREATE INDEX idx_prices_symbol_date ON prices_raw(symbol_id, date);
CREATE INDEX idx_prices_cutoff ON prices_raw(collected_at);
CREATE INDEX idx_prices_latest ON prices_raw(symbol_id, date, source, price_revision_seq DESC);
CREATE INDEX idx_prices_batch ON prices_raw(ingestion_batch_id);
CREATE INDEX idx_pending_unresolved ON pending_symbol_prices(resolved_symbol_id, date);
CREATE INDEX idx_ca_symbol_eff ON corp_actions(symbol_id, effective_date, event_type);
CREATE INDEX idx_ca_source_event ON corp_actions(source_event_id, event_version);
CREATE INDEX idx_ca_batch ON corp_actions(ingestion_batch_id);
CREATE INDEX idx_af_symbol_date ON adjustment_factors(snapshot_id, symbol_id, effective_date);
CREATE INDEX idx_dca_symbol_date ON daily_cumulative_adjustment(snapshot_id, symbol_id, date);
CREATE INDEX idx_ael_snapshot_time ON adjustment_evaluation_log(snapshot_id, evaluated_at DESC);
CREATE INDEX idx_gap_reason_date ON collection_gap_report(missing_reason, date);
CREATE INDEX idx_factor_snapshot_lookup ON factor_snapshot(snapshot_id, symbol_id, date);
CREATE INDEX idx_snapshot_status_created ON view_snapshot_metadata(status, created_at DESC);
CREATE INDEX idx_sib_batch ON snapshot_input_batches(ingestion_batch_id);
CREATE INDEX idx_partitions_sched ON ingestion_partitions(status, next_retry_at);
CREATE INDEX idx_partitions_batch ON ingestion_partitions(ingestion_batch_id);
CREATE INDEX idx_spp_domain_active ON source_priority_policy(domain, is_active, valid_from DESC);
CREATE INDEX idx_sem_symbol_range ON symbol_entity_mapping(symbol_id, start_date, end_date);
CREATE INDEX idx_sem_corp_range ON symbol_entity_mapping(corp_id, start_date, end_date);
CREATE INDEX idx_erh_source_key ON entity_resolution_history(source_key, resolved_at DESC);
CREATE INDEX idx_overrides_event ON corp_action_overrides(event_id, event_version, applied_at DESC);

-- =====================================================
-- 표준 View
-- =====================================================

CREATE VIEW prices_raw_latest AS
WITH latest AS (
    SELECT p.*,
           ROW_NUMBER() OVER (
               PARTITION BY p.symbol_id, p.date, p.source
               ORDER BY p.price_revision_seq DESC
           ) AS rn
    FROM prices_raw p
)
SELECT * FROM latest WHERE rn = 1;

CREATE VIEW corp_actions_latest AS
WITH latest AS (
    SELECT c.*,
           ROW_NUMBER() OVER (
               PARTITION BY c.event_id
               ORDER BY c.event_version DESC
           ) AS rn
    FROM corp_actions c
)
SELECT * FROM latest WHERE rn = 1;

CREATE VIEW raw_price_view AS
SELECT * FROM prices_raw_latest;

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

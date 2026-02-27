# 스키마 명세 (Generalized, Phase-based)

## 1. 목적
이 문서는 데이터 수집 레포의 스키마 계약을 정의한다.
- Phase 1: 핵심 데이터셋 제공
- Phase 2~4: 운영 안정성 및 도메인 확장

## 2. 설계 원칙
1. Phase 1 테이블은 장기 호환 계약으로 간주한다.
2. Phase 2+는 additive migration(테이블/컬럼 추가) 우선으로 확장한다.
3. 소비 인터페이스는 버전 뷰(`*_v1`)로 고정한다.
4. 결측/오류는 원본 값 보정 대신 이슈 테이블로 관리한다.

## 3. Phase 1 Core Schema

## 3.1 `instruments`
역할:
- 마스터 데이터(식별자/시장/상장기간)

핵심 키:
- PK: `instrument_id`
- Unique: `(market_code, external_code)`

핵심 컬럼:
- `external_code, market_code, instrument_name`
- `listing_date, delisting_date`
- `source_name, collected_at, updated_at`

## 3.1.1 `instrument_delisting_snapshot`
역할:
- KIND 상장폐지 상세정보의 최신 스냅샷(시장+종목 단위 1건)

핵심 키:
- PK: `delisting_snapshot_id`
- Unique: `(market_code, external_code)`

핵심 컬럼:
- `market_code, external_code, delisting_date`
- `delisting_reason, note`
- `source_name, collected_at, updated_at, run_id`

## 3.2 `collection_runs`
역할:
- 수집 실행 단위 메타데이터

핵심 키:
- PK: `run_id`

핵심 컬럼:
- `pipeline_name, source_name`
- `window_start, window_end`
- `status, started_at, finished_at`
- `success_count, failure_count, warning_count, metadata`

## 3.3 `daily_market_data`
역할:
- 종목 단위 일봉 시계열

핵심 키:
- PK: `(instrument_id, trade_date)`
- FK: `instrument_id -> instruments`
- FK: `run_id -> collection_runs`

핵심 컬럼:
- 가격/거래: `open, high, low, close, volume, turnover_value, market_value`
- 상태: `is_trade_halted, is_under_supervision, record_status`
- 라인리지: `source_name, collected_at, run_id`

기본 제약:
- OHLC 논리 제약
- 음수 거래량/거래대금/시가총액 금지
- `record_status IN ('VALID','INVALID','MISSING')`

## 3.4 `trading_calendar`
역할:
- 시장 거래일 기준(개장/휴장) 제공

핵심 키:
- PK: `(market_code, trade_date)`
- FK: `run_id -> collection_runs`

핵심 컬럼:
- `is_open, holiday_name`
- `source_name, collected_at, run_id`

## 3.5 `benchmark_index_data`
역할:
- 벤치마크 지수 일봉 시계열

핵심 키:
- PK: `(index_code, index_name, trade_date)`
- FK: `run_id -> collection_runs`

핵심 컬럼:
- `open, high, low, close`
- `source_name, collected_at, run_id`

## 3.6 `data_quality_issues`
역할:
- 결측/오류/경고 이슈 추적

핵심 키:
- PK: `issue_id`
- FK: `instrument_id -> instruments`
- FK: `run_id -> collection_runs`

핵심 컬럼:
- `dataset_name, trade_date, instrument_id, index_code`
- `issue_code, severity, issue_detail`
- `detected_at, resolved_at`

## 3.7 Consumer Views (Phase 1 계약)
1. `core_market_dataset_v1`
- 종목 일봉 + 마스터 결합 뷰

2. `benchmark_dataset_v1`
- 지수 일봉 제공 뷰

3. `trading_calendar_v1`
- 거래일 캘린더 제공 뷰

원칙:
- 파생 계산 컬럼은 포함하지 않는다.
- 소비 시스템이 도메인 계산을 수행한다.

## 4. Phase 2 확장 (Reliability)
추가 테이블:
1. `run_partitions`
2. `quality_metrics`

목적:
- 재시도 자동화
- 품질 지표 저장

확장 방식:
- 기존 Phase 1 테이블/뷰 변경 없이 추가

## 5. Phase 3 확장 (Domain Expansion)
추가 테이블:
1. `corporate_events`

목적:
- 공시/이벤트 도메인 데이터 추가

확장 방식:
- `instruments` FK 참조로 도메인 결합
- Phase 1 소비 계약 불변

## 6. Phase 4 확장 (Platform Hardening)
추가 테이블:
1. `source_policies`
2. `dataset_snapshots`
3. `snapshot_runs`

목적:
- 멀티소스 정책
- 버전/스냅샷 관리

확장 방식:
- collection runs와 스냅샷 연결만 추가
- 기존 일봉 계약 불변

## 7. 마이그레이션 가이드
1. Phase 1 PK/컬럼 삭제 또는 rename 금지
2. 새 요구사항은 우선 "새 컬럼 nullable 추가" 또는 "새 테이블 추가"로 반영
3. 뷰 변경은 `v2` 신규 뷰 생성 후 점진 전환
4. 운영 중 drop column/table은 deprecation 기간 후 수행
5. 역호환 테스트:
- 기존 `core_market_dataset_v1` 쿼리가 릴리즈 후 동일 동작해야 함

## 8. 참고
- 실행 DDL: `sql/platform_schema.sql`

## 9. Phase 1 실행 DDL (명시)
아래 DDL은 Phase 1 최소 구현 기준이다.

```sql
CREATE TABLE instruments (
    instrument_id UUID PRIMARY KEY,
    external_code VARCHAR(20) NOT NULL,
    market_code VARCHAR(20) NOT NULL,
    instrument_name VARCHAR(200) NOT NULL,
    listing_date DATE NOT NULL,
    delisting_date DATE NULL,
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
SELECT index_code, index_name, trade_date, open, high, low, close, record_status, source_name, collected_at
FROM benchmark_index_data;

CREATE VIEW trading_calendar_v1 AS
SELECT market_code, trade_date, is_open, holiday_name, source_name, collected_at
FROM trading_calendar;
```

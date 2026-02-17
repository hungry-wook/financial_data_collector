# 구현 지시서 (Ingestion Core)

## 1. 범위
본 문서는 데이터 수집 파이프라인 구현 지침이다.
- 포함: 수집, 정규화, 검증, 적재, 추출
- 제외: 분석 로직, 실행 로직, 성과 리포트

## 2. Phase 1: Core Dataset 구축

## 2.1 Task P1-01: Core 스키마 생성
테이블:
1. `instruments`
- `instrument_id` (PK)
- `market_code`
- `listing_date`
- `delisting_date` (nullable)

2. `collection_runs`
- `run_id` (PK)
- `pipeline_name, source_name`
- `window_start, window_end`
- `status, started_at, finished_at`

3. `daily_market_data`
- PK: `(instrument_id, trade_date)`
- `open, high, low, close, volume, turnover_value, market_value`
- `is_trade_halted, is_under_supervision`
- `source_name, collected_at, run_id`

4. `benchmark_index_data`
- PK: `(index_code, trade_date)`
- `open, high, low, close`
- `source_name, collected_at, run_id`

5. `trading_calendar`
- PK: `(market_code, trade_date)`
- `is_open, holiday_name`
- `source_name, collected_at, run_id`

6. `data_quality_issues`
- PK: `issue_id`
- `issue_detail, detected_at, run_id`

완료 기준:
- 테이블/PK/필수 제약 생성 완료

## 2.2 Task P1-02: Instrument Master Collector
입력:
- 시장별 종목 마스터 소스

출력:
- `instruments` upsert

필수 처리:
- 식별자 정규화
- 상장/상폐 일자 정규화
- market 코드 표준화
- 과거 비활성(상폐) 종목 포함 백필 정책 적용

완료 기준:
- 대상 기간 거래 종목의 마스터 누락 없음

## 2.3 Task P1-03: Daily Market Collector
입력:
- 거래일 범위
- 종목 목록

출력:
- `daily_market_data`

필수 처리:
- OHLCV/거래대금/시가총액 매핑
- 거래정지/관리 상태 매핑
- 수집 실패/결측은 `data_quality_issues`에 기록
- 결측 보간(forward/back fill) 금지

완료 기준:
- `(instrument_id, trade_date)` 중복 없음
- 결측/실패 데이터는 이슈 테이블에서 추적 가능

## 2.4 Task P1-04: Benchmark Index Collector
입력:
- 거래일 범위

출력:
- `benchmark_index_data`

필수 처리:
- 기준 지수 코드 표준화
- 결측 시 `data_quality_issues` 기록

완료 기준:
- 요청 기간 데이터 적재 완료

## 2.5 Task P1-05: Trading Calendar Collector
입력:
- 시장 코드
- 기간 범위

출력:
- `trading_calendar`

필수 처리:
- 개장/휴장 상태 표준화
- 휴장 사유(`holiday_name`) 정규화
- 결측 시 `data_quality_issues` 기록

완료 기준:
- 대상 기간 거래일 캘린더 적재 완료

## 2.6 Task P1-06: Validation Job
검증 규칙:
1. `high >= max(open, close, low)`
2. `low <= min(open, close, high)`
3. `volume >= 0`
4. `turnover_value >= 0` (null 정책 고정)
5. `market_value >= 0` (null 정책 고정)
6. 캘린더 개장일인데 종목/지수 데이터가 전면 누락이면 issue 기록

실패 처리:
- row 삭제 대신 issue 기록 + 검증 상태 태깅

완료 기준:
- 일자별 검증 결과 리포트 생성

## 2.7 Task P1-07: Consumer View
뷰:
- `core_market_dataset_v1`
- `benchmark_dataset_v1`
- `trading_calendar_v1`

포함 컬럼:
1. `core_market_dataset_v1`
- Instrument: `instrument_id, market_code, listing_date, delisting_date`
- Daily: `trade_date, open, high, low, close, volume, turnover_value, market_value, is_trade_halted, is_under_supervision`
2. `benchmark_dataset_v1`
- Index: `index_code, trade_date, open, high, low, close`
3. `trading_calendar_v1`
- Calendar: `market_code, trade_date, is_open, holiday_name`

원칙:
- 계산 파생 컬럼은 뷰에 포함하지 않는다.
- 소비 시스템에서 도메인 계산을 수행한다.

완료 기준:
- 외부 시스템이 추가 정규화 없이 바로 사용할 수 있음

## 3. Phase 2: Reliability
1. 배치 상태/재시도 정책
2. 완전성/적시성 품질 지표
3. 수집 라인리지 표준화
4. 이슈 코드 사전 고정

## 4. Phase 3: Domain Expansion
1. 공시/이벤트 데이터 도메인 추가
2. 도메인별 스키마 확장
3. 파생 데이터셋 제공 정책 수립

## 5. Phase 4: Platform Hardening
1. 멀티 소스 병합 정책
2. 데이터 버전/스냅샷 관리
3. 파티션 복구 자동화

## 6. Phase 1 체크리스트
1. Core 컬럼 계약 충족 여부
2. 상태 컬럼의 일자 결합 여부
3. 마스터 기간 정보 제공 여부
4. 지수 데이터 정렬 일관성 여부
5. 이슈 추적 가능 여부

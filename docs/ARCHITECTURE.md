# 금융 시계열 데이터 수집 아키텍처

## 1. 목적
이 레포지토리는 외부 소비 시스템이 사용할 금융 시계열 데이터를 수집, 정규화, 검증, 제공한다.
- 이 레포는 데이터 생산 책임만 가진다.
- 분석/시뮬레이션/리포팅은 외부 시스템 책임이다.

## 2. 설계 원칙
1. Source-first: 계산 데이터보다 원천 데이터 품질을 우선한다.
2. Minimal Core First: Phase 1에서 핵심 데이터셋을 먼저 안정화한다.
3. Backward-compatible Expansion: 후속 Phase는 Phase 1 계약을 깨지 않고 확장한다.
4. Reproducible Delivery: 동일 입력 조건에서 동일 추출 결과를 보장한다.

## 3. Core Dataset (Phase 1)
Phase 1은 아래 4개 데이터 도메인을 제공한다.

### 3.1 Instrument Master
- 키: `instrument_id`
- 주요 컬럼: `market_code, listing_date, delisting_date`

### 3.2 Daily Market Data (Instrument)
- 키: `instrument_id, trade_date`
- 주요 컬럼: `open, high, low, close, volume, turnover_value, market_value`
- 상태 컬럼: `is_trade_halted, is_under_supervision`

### 3.3 Daily Market Data (Benchmark Index)
- 키: `index_code, trade_date`
- 주요 컬럼: `open, high, low, close`

### 3.4 Trading Calendar
- 키: `market_code, trade_date`
- 주요 컬럼: `is_open, holiday_name`

### 3.5 데이터 계약
- 거래일 기준 날짜(`trade_date`) 사용
- 결측 값 보간 금지
- 결측/오류는 별도 이슈 코드로 관리

## 4. Phase 로드맵

## Phase 1: Core Ingestion
목표:
- 핵심 데이터셋(마스터/종목 일봉/지수 일봉) 제공

범위:
- 원천 수집
- 표준 스키마 정규화
- 기본 무결성 검증
- 품질 이슈 기록

산출물:
- Core 테이블 6종
- 소비용 표준 뷰 3종

## Phase 2: Reliability
목표:
- 수집 안정성과 운영 가시성 강화

범위:
- 배치 재시도
- 완전성/적시성 지표
- 배치 단위 라인리지

## Phase 3: Domain Expansion
목표:
- 추가 데이터 도메인 확장

범위:
- 기업행위/공시 이벤트
- 도메인별 정규화 정책
- 파생 데이터셋(선택)

## Phase 4: Platform Hardening
목표:
- 대규모 운영 환경 대응

범위:
- 멀티 소스 정책
- 스냅샷/버전 관리
- 파티션 단위 복구 자동화

## 5. Phase 1 수용 기준
1. Core 컬럼 계약을 충족한다.
2. 상태 데이터가 일자 단위 시계열에 결합된다.
3. 상장/상폐 기간 정보가 마스터에서 조회된다.
4. 지수 데이터가 동일 거래일 축으로 제공된다.
5. 결측/오류 데이터가 이슈 테이블에서 추적된다.

## 6. 비범위
- 도메인별 파생 규칙 계산
- 실행 엔진 로직
- 분석 지표 계산 및 리포팅

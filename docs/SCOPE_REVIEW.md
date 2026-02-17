# 스코프 점검 (Current Baseline)

## 1. 점검 목적
1. Phase 1만 구현해도 외부 소비 시스템이 도메인 계산을 시작할 수 있는지 확인
2. Phase 2~4 확장 시 스키마 마이그레이션이 원활한지 확인
3. 네이밍/문서 구조가 일반화 원칙에 맞는지 확인

## 2. 점검 결과 요약
- 네이밍: 일반화됨 (`platform_schema`, `core_market_dataset_v1`, `daily_market_data`)
- Phase 1 최소셋: 충족
- 확장성: additive migration 중심으로 설계됨

## 3. Phase 1 최소셋 충족 여부
필수 데이터 도메인:
1. Instrument Master
2. Instrument Daily Time Series
3. Benchmark Daily Time Series
4. Trading Calendar
5. Data Quality Issues

판정:
- 핵심 컬럼과 상태 컬럼이 모두 포함되어 외부 계산 로직에서 추가 수집 없이 시작 가능
- 결측/오류는 별도 이슈 테이블로 추적 가능

요구 데이터 매핑 점검:
1. 일봉 OHLCV: `daily_market_data.open/high/low/close/volume`
2. 거래대금: `daily_market_data.turnover_value`
3. 시가총액: `daily_market_data.market_value`
4. 거래정지/관리 상태: `daily_market_data.is_trade_halted/is_under_supervision`
5. 상장기간 정보: `instruments.listing_date/delisting_date`
6. 벤치마크 지수 일봉: `benchmark_index_data.open/high/low/close`
7. 거래일 캘린더: `trading_calendar.market_code/trade_date/is_open`

누락 여부:
- Phase 1 기준 필수 입력 컬럼 누락 없음

## 4. 마이그레이션 적합성 점검
적합 항목:
1. Phase 1 PK 구조가 안정적임
2. Phase 2~4 확장이 신규 테이블 추가 중심
3. 소비 인터페이스가 버전 뷰(`*_v1`)로 분리됨
4. run 메타(`collection_runs`)가 초기부터 존재해 운영 확장 경로가 단순함

주의 항목:
1. `core_market_dataset_v1` 계약 변경 시 신규 뷰 버전(`v2`) 추가 원칙을 유지해야 함
2. Phase 3 도메인 확장 시 기존 Phase 1 컬럼 의미를 재해석하지 말아야 함

## 5. 운영 권고
1. 릴리즈마다 역호환 쿼리 테스트를 수행
2. 비호환 변경은 deprecation 기간 후 반영
3. 소스별 결측 코드 사전을 운영 문서로 고정

## 6. 전략 계열 적합성 점검 (Phase 1 단독)
대상:
- 모멘텀 계열
- 상대강도(RS) 계열
- 지수 레짐 필터를 사용하는 크로스섹셔널 전략

필수 데이터 요구사항 매핑:
1. 종목 OHLCV: 충족 (`daily_market_data`)
2. 거래대금/시가총액: 충족 (`turnover_value`, `market_value`)
3. 종목 상태 필터(거래정지/관리): 충족 (`is_trade_halted`, `is_under_supervision`)
4. 상장일수/상장기간 필터: 충족 (`listing_date`, `delisting_date`)
5. 벤치마크 지수 레짐 계산: 충족 (`benchmark_index_data`)
6. 거래일 기준 N일 계산: 충족 (`trading_calendar`)
7. 결측/오류 추적: 충족 (`data_quality_issues`)

판정:
- Phase 1만으로 모멘텀/상대강도 전략의 백테스트 입력 데이터 생산 가능

제약/주의:
1. 기업행위(분할/병합/배당) 보정이 필요하면 Phase 3 확장 필요
2. 소스 정정 이력 추적이 필요하면 Phase 2+의 run/partition 메타를 활성 활용해야 함
3. 생존편향 방지를 위해 마스터 수집 시 \"현재 활성 종목만\" 수집하지 않도록 정책 강제 필요

Phase 1 필수 운영 조건:
1. 과거 상폐 종목 포함 백필(backfill)
2. 거래일 캘린더 기반 윈도우 계산(단순 달력일 금지)
3. 결측 row 보간 금지 및 issue 기록 의무화

# 데이터 소스 수집 계획 (Generalized)

## 1. 목적
본 문서는 데이터 수집 소스와 내부 스키마의 연결 기준을 정의한다.
- Phase 1: Core Ingestion 소스만 적용
- Phase 3+: 이벤트 도메인 소스 확장

## 2. 소스 분류

## 2.1 Market Source (Instrument Daily)
수집 대상:
- 종목 마스터
- 종목 일봉(OHLCV, 거래대금, 시가총액)
- 거래 상태(거래정지/관리 등)

내부 매핑:
- `instruments`
- `daily_market_data`

## 2.2 Benchmark Source (Index Daily)
수집 대상:
- 벤치마크 지수 일봉(OHLC)

내부 매핑:
- `benchmark_index_data`

## 2.3 Trading Calendar Source
수집 대상:
- 시장 거래일/휴장일 정보

내부 매핑:
- `trading_calendar`

## 2.4 Corporate Event Source (Phase 3)
수집 대상:
- 공시/이벤트 정보

내부 매핑:
- `corporate_events`

## 3. Phase별 수집 범위

## Phase 1
- Market Source
- Benchmark Source
- Trading Calendar Source
- 수집 실행 메타(`collection_runs`)
- 품질 이슈 기록(`data_quality_issues`)

## Phase 2
- Phase 1 + 배치 신뢰성 메타
- `run_partitions`, `quality_metrics`

## Phase 3
- Phase 2 + Corporate Event Source

## Phase 4
- Phase 3 + 멀티소스 정책/스냅샷

## 4. 기본 검증 규칙
1. OHLC 논리 일관성
2. 거래량/거래대금/시가총액 음수 금지
3. PK 중복 금지
4. 결측은 보간하지 않고 이슈로 기록

## 5. 결측/오류 처리
- 수집 실패/소스 공백/검증 실패는 `data_quality_issues`에 기록
- 원본 row를 임의 보정하지 않는다
- 재수집은 run 단위로 수행한다

## 6. 운영 체크포인트
1. 소스별 수집 창(window) 관리
2. run 상태 추적(SUCCESS/PARTIAL/FAILED)
3. 품질 지표(completeness/timeliness) 저장
4. 소비 뷰 버전 호환성 확인

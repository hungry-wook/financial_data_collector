# Phase 1 + 조회 인터페이스 설계

## 1. 목표
- Phase 1 범위에서 데이터 생산(수집/정규화/검증/적재)과 데이터 조회를 모두 가능하게 한다.
- 소비 시스템이 `*_v1` 뷰를 바로 사용할 수 있는 안정된 조회 계약을 제공한다.

## 2. Phase 1 구현 범위
문서 기준 필수 도메인:
1. `instruments`
2. `daily_market_data`
3. `benchmark_index_data`
4. `trading_calendar`
5. `data_quality_issues`
6. `collection_runs`

필수 산출 뷰:
1. `core_market_dataset_v1`
2. `benchmark_dataset_v1`
3. `trading_calendar_v1`

## 3. 제안 아키텍처 (Phase 1)
1. Ingestion Layer
- 소스별 Collector 3종
- `collection_runs` 생성/종료 기록
- 적재 실패/결측을 `data_quality_issues`에 기록

2. Validation Layer
- OHLC, 음수값, 전면 누락 규칙 검증
- row 삭제 없이 `record_status`/issue 기록

3. Serving Layer
- SQL View(`*_v1`) 기반 Read API
- 조회 API는 뷰만 사용하고 원본 테이블 직접 노출 금지

4. UI Layer
- 운영자용 조회 화면: 종목 시계열, 지수 시계열, 거래일 캘린더, 품질 이슈, 수집 run 상태

## 4. 조회 인터페이스 설계
## 4.1 API 계약 (초안)
1. `GET /api/v1/core-market`
- params: `market_code`, `external_code`, `instrument_id`, `date_from`, `date_to`, `limit`
- source: `core_market_dataset_v1`

2. `GET /api/v1/benchmark`
- params: `index_code`, `date_from`, `date_to`, `limit`
- source: `benchmark_dataset_v1`

3. `GET /api/v1/calendar`
- params: `market_code`, `date_from`, `date_to`
- source: `trading_calendar_v1`

4. `GET /api/v1/issues`
- params: `dataset_name`, `severity`, `issue_code`, `date_from`, `date_to`, `run_id`
- source: `data_quality_issues`

5. `GET /api/v1/runs`
- params: `pipeline_name`, `source_name`, `status`, `date_from`, `date_to`
- source: `collection_runs`

## 4.2 화면 구성 (운영자 기준)
1. Overview
- 최근 run 성공/실패 건수
- 최근 WARN/ERROR issue 집계

2. 종목 데이터 조회
- 종목/시장/기간 필터
- OHLCV + 상태 컬럼 표시

3. 벤치마크 조회
- 지수코드/기간 필터
- OHLC 시계열 표시

4. 거래일 캘린더 조회
- 시장/기간 필터
- 휴장일과 사유 표시

5. 품질 이슈 조회
- severity/코드/기간 필터
- run 연계 추적

## 5. 운영 규칙
1. 결측 보간 금지
2. 비정상 값은 삭제 대신 이슈 기록
3. API는 `v1` 뷰 계약을 기준으로 역호환 유지
4. 비호환 변경 시 `v2` 엔드포인트/뷰 추가

## 6. 구현 언어 권장
권장: **Python**

근거:
1. 데이터 수집/정규화/검증 파이프라인에 필요한 생태계가 가장 성숙함
- `pandas`, `pydantic`, `sqlalchemy`, `psycopg`
2. API/운영 UI를 같은 언어로 빠르게 구성 가능
- `FastAPI`(조회 API), `Streamlit` 또는 `FastAPI + Jinja`(운영 UI)
3. 향후 Phase 2~4(품질지표, 이벤트 확장, 스냅샷)로 확장 시 작업 비용이 낮음

권장 조합:
1. DB: PostgreSQL
2. Backend/API: Python + FastAPI
3. Batch/Collector: Python
4. 운영 UI(초기): Streamlit

## 7. 최소 구현 순서
1. `sql/platform_schema.sql` 적용
2. Collector 3종 + `collection_runs` 기록
3. Validation Job + `data_quality_issues` 적재
4. `GET` 조회 API 5개 구현
5. 운영 UI 5개 화면 구현
6. 샘플 기간 백필 후 조회 검증

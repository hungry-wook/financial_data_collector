# financial_data_collector

금융 시계열 데이터 수집/정규화/검증 문서 저장소입니다.

## 개발 환경(uv)
1. uv 설치 확인:
- `uv --version`
2. 가상환경 + 의존성 설치:
- `uv sync --extra dev --extra parquet`
3. 전체 테스트 실행:
- `uv run pytest -q`

## 문서 구조
- `docs/README.md`: 문서 전체 내비게이션
- `docs/ARCHITECTURE.md`: 설계 원칙과 Phase 로드맵
- `docs/API_COLLECTION_PLAN.md`: 데이터 소스-스키마 매핑 계획
- `docs/KRX_API_COLLECTION_SPEC.md`: KRX 수집 API 명세
- `docs/IMPLEMENTATION_GUIDE.md`: Phase별 구현 지시서
- `docs/PHASE1_INTERFACE_DESIGN.md`: Phase 1 + 조회 인터페이스 설계안
- `docs/BULK_EXPORT_API_SPEC.md`: 기간 벌크 추출 API 규격
- `docs/SCHEMA.md`: 스키마 계약 문서
- `docs/SCOPE_REVIEW.md`: 범위 및 적합성 점검 기록
- `sql/platform_schema.sql`: 실행 DDL(Phase 1~4)

## 권장 읽기 순서
1. `docs/ARCHITECTURE.md`
2. `docs/SCHEMA.md`
3. `docs/API_COLLECTION_PLAN.md`
4. `docs/IMPLEMENTATION_GUIDE.md`

## 구현 시작점
- DB 생성: `sql/platform_schema.sql`
- 구현 착수: `docs/IMPLEMENTATION_GUIDE.md`의 Phase 1

## KRX 실연동 테스트 준비
1. `.env.example`을 참고해 `.env` 작성
2. 필수 값 입력:
- `KRX_AUTH_KEY`
- `KRX_BASE_URL`
- `KRX_API_PATH_INSTRUMENTS`
- `KRX_API_PATH_DAILY_MARKET`
- `KRX_API_PATH_INDEX_DAILY`
3. 사전점검 테스트:
- `uv run pytest -q tests/test_preflight.py`
4. 실연동 스모크 테스트:
- `uv run pytest -q -m integration`

## PostgreSQL 스키마 계약 테스트
1. PostgreSQL 연결 문자열 준비(예: `postgresql://user:pass@localhost:5432/postgres`)
2. 환경변수 설정:
- `TEST_POSTGRES_DSN=<연결문자열>`
3. 실행:
- `uv run pytest -q -m postgres tests/test_schema_contract_postgres.py`

임시 컨테이너로 한 번에 실행(Windows PowerShell):
- `.\scripts\run_postgres_tests.ps1`

pytest만으로 임시 컨테이너 자동 실행:
1. `.env` 설정
- `TEST_POSTGRES_USE_TEMP_CONTAINER=1`
- `TEST_POSTGRES_DOCKER_PORT=5431`
2. 실행
- `uv run pytest -q -m postgres -rs`

## 1Y KOSDAQ 내보내기 성능 테스트(옵션)
1. 실행 플래그 설정:
- `RUN_PERF_TESTS=1`
2. 허용 시간(초) 설정(옵션):
- `PERF_MAX_EXPORT_SECONDS=10`
3. 실행:
- `uv run pytest -q -m performance tests/test_export_performance.py`

## 백테스트 샘플 데이터셋 생성
코드 진입점:
- `src/financial_data_collector/sample_backtest_run.py`

## uv lock 파일 생성
현재 세션(샌드박스)에서는 Python 실행 파일 접근 제한으로 `uv lock` 생성이 실패할 수 있습니다.
로컬 일반 터미널에서는 아래 명령으로 lock 파일을 생성할 수 있습니다.
- `uv lock`

## KRX 데이터 수집 스크립트
- 실행(기본: KOSDAQ + KOSPI 동시 수집): `uv run collect-krx-data --date-from 2026-01-02 --date-to 2026-01-10`
- 모듈 실행(기본: KOSDAQ + KOSPI 동시 수집): `uv run python -m financial_data_collector.collect_krx_data --date-from 2026-01-02 --date-to 2026-01-10`
- 옵션:
- `--db-path data/financial_data.db`
- 단일 시장만 수집: `--market-code KOSDAQ` (필요 시 `--index-code KOSDAQ`)
- 다중 시장 지정: `--market-codes KOSDAQ,KOSPI`

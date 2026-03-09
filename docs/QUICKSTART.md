# 빠른 시작 가이드

## 1. 사전 준비
- Python 3.9 이상
- Docker 및 Docker Compose
- `DATABASE_URL`, `POSTGRES_*`, `KRX_AUTH_KEY`, `OPEN_DART_API_KEY`가 포함된 `.env`

## 2. 의존성 설치
- `uv sync --extra dev --extra parquet`

## 3. 서비스 시작
- `just up`
- `just health`
- API: `http://localhost:8000`
- 대시보드: `http://localhost:8000/dashboard`

## 4. KRX 시세 수집
- 직접 실행:
  - `uv run collect-krx-data --date-from 2026-02-09 --date-to 2026-03-06`
- `just` 래퍼 사용:
  - `just collect-local 2026-02-09 2026-03-06`

## 5. DART 이벤트 수집
- 직접 실행:
  - `uv run collect-dart-corporate-events --database-url $env:DATABASE_URL --bgn-de 2026-03-01 --end-de 2026-03-09`
- 수집 후 계수까지 재구축:
  - `just collect-dart-adjusted-local 2026-03-01 2026-03-09 7`

## 6. 테스트 실행
- 전체 테스트: `uv run pytest -q`
- 빠른 회귀 확인: `uv run pytest -q tests/test_collect_krx_data.py tests/test_repository_resilience.py`

## 7. 유용한 명령
- `just logs api`
- `just logs collector`
- `just doctor`
- `docker compose ps`

## 8. 운영 메모
- Postgres는 로컬 전용으로 노출됩니다.
- Export는 이미 물질화된 수정 계수를 읽으며, 요청 시점에 재구축하지 않습니다.
- DART는 일일 호출 한도가 있으므로 재수집 범위를 계획해서 실행해야 합니다.

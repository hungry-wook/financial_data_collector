# KRX 연동 테스트 가이드

## 목적
실제 KRX OpenAPI 파이프라인이 로컬 Postgres와 정상적으로 동작하는지 확인합니다.

## 사전 조건
- 유효한 `KRX_AUTH_KEY`
- 유효한 `.env`의 `DATABASE_URL`
- 실행 중인 로컬 Postgres

## 점검 순서
1. 헬스 체크
- `just health`

2. 짧은 수집 구간 실행
- `uv run collect-krx-data --date-from 2026-03-03 --date-to 2026-03-06`

3. 회귀 테스트 실행
- `uv run pytest -q tests/test_collect_krx_data.py tests/test_repository_resilience.py`

4. 결과 확인
- 대시보드: `http://localhost:8000/dashboard`
- 또는 `collection_runs` 직접 조회

## 실패 시 확인 항목
- KRX 키의 유효성 및 권한
- DB 연결 설정
- 수집기 로그에서 `OperationalError`가 반복되는지 여부
- 휴장일에 따른 빈 응답인지 실제 수집 실패인지 구분

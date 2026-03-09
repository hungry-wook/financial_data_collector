# 배포 및 운영 가이드

## 목적
로컬 머신이나 단일 호스트에서 Postgres, API, 수집기를 운영하기 위한 최소 절차를 정리합니다.

## 서비스 구성
- `postgres`: 기본 데이터 저장소
- `api`: FastAPI 애플리케이션, 대시보드, export API
- `collector`: 주기 실행 KRX 일간 수집기
- `collector-once`: 단발성 수집 작업

## 기본 보안 설정
- Postgres는 `127.0.0.1:${POSTGRES_PORT}:5432`에만 바인딩합니다.
- `POSTGRES_PASSWORD`는 충분히 강한 값으로 설정합니다.
- KRX, DART 키는 `.env`에만 보관합니다.

## 표준 기동 절차
1. `.env` 준비
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`
- `DATABASE_URL`
- `KRX_AUTH_KEY`
- `OPEN_DART_API_KEY`

2. 시작
- `docker compose up -d --build`

3. 확인
- `docker compose ps`
- `just health`
- `docker compose logs --no-color --tail 100 api`

## 배치 작업
### KRX 수집
- 기간 지정 1회 수집:
  - `docker compose --profile collector run --rm -e DATE_FROM=2026-02-09 -e DATE_TO=2026-03-06 collector-once`

### DART 수집
- 호스트 셸에서 직접 실행하는 방식을 권장:
  - `uv run collect-dart-corporate-events --database-url $env:DATABASE_URL --bgn-de 2026-03-01 --end-de 2026-03-09 --rebuild-adjustments --overlap-days 7`

## 장애 점검
### API 헬스 실패
- `just doctor`
- `docker compose logs --no-color --tail 120 api`
- `docker compose exec -T api printenv DATABASE_URL`

### Postgres 문제
- `docker compose ps`
- `docker compose logs --no-color --tail 120 postgres`
- `.env`의 `DATABASE_URL`과 `POSTGRES_*` 값이 일치하는지 확인

### KRX 수집 문제
- 최근 `collection_runs`를 점검
- `just logs collector`
- 짧은 기간으로 1회 수집을 다시 실행

## 저장소 메모
- 기본 Postgres 데이터는 Docker 볼륨 `pg_data`에 저장됩니다.
- `./data`는 산출물과 부가 파일용이며, 기본 Postgres 저장소는 아닙니다.

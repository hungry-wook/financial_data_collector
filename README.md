# financial_data_collector

KRX 일봉, 기준가, 벤치마크, 거래일 캘린더, 상장폐지 스냅샷을 수집하고 기준가 기반 수정주가를 제공하는 최소 백테스트 데이터 프로젝트입니다.

## 범위
- KRX 종목 일봉 수집
- KRX 기준가 수집
- 벤치마크 지수 일봉 수집
- 거래일 캘린더 구축
- KIND 상장폐지 스냅샷 수집
- 기준가 기반 수정주가 계수 산출
- 읽기 전용 API 및 대시보드 제공

## 빠른 시작
1. 의존성 설치
- `uv sync --extra dev`

2. 로컬 서비스 시작
- `just up`
- `just health`

3. KRX 시세/기준가 수집
- `uv run collect-krx-data --date-from 2026-02-09 --date-to 2026-03-06`

4. 상폐 스냅샷 수집
- `just collect-delisted-local 1900-01-01`

5. 조정계수 재구축
- `just rebuild-adjustments-local 2026-02-09 2026-03-06`

6. 테스트 실행
- `uv run pytest -q`

## 자주 쓰는 명령
- `just collect-local <from> <to>`: 로컬 KRX 수집
- `just collect-delisted-local [from] [to]`: 로컬 KIND 상폐 수집
- `just rebuild-adjustments-local <from> <to>`: 기준가 기반 조정계수 재구축
- `just serve-local`: API 로컬 실행
- `just test`: 테스트 실행

## 참고
- Postgres는 `127.0.0.1:${POSTGRES_PORT}:5432`에 바인딩됩니다.
- `.env`의 `DATABASE_URL`은 호스트 셸 명령 기준입니다.
- DDL: `sql/platform_schema.sql`
- PostgreSQL 마이그레이션: `sql/platform_postgres_migrations.sql`
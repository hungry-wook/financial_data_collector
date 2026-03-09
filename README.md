# financial_data_collector

KRX 시장 데이터, DART 기업 이벤트, 수정주가 계수, 백테스트용 추출 데이터를 수집하는 프로젝트입니다.

## 범위
- KRX 일별 시세, 벤치마크, 거래일 캘린더 수집
- KIND 상장폐지 스냅샷 수집
- OpenDART 기업 이벤트 수집 및 정규화
- 수정주가 계수 산출
- 원주가/수정주가 추출 API 및 대시보드 제공

## 빠른 시작
1. 의존성 설치
- `uv sync --extra dev --extra parquet`

2. 로컬 서비스 시작
- `just up`
- `just health`

3. KRX 시세 수집
- `uv run collect-krx-data --date-from 2026-02-09 --date-to 2026-03-06`

4. DART 이벤트 수집 및 계수 재구축
- `uv run collect-dart-corporate-events --database-url $env:DATABASE_URL --bgn-de 2026-03-01 --end-de 2026-03-09 --rebuild-adjustments --overlap-days 7`

5. 테스트 실행
- `uv run pytest -q`

## 자주 쓰는 명령
- `just up`: postgres, api, collector 시작
- `just down`: 서비스 중지
- `just health`: API 헬스 체크
- `just collect-local <from> <to>`: 로컬 KRX 수집
- `just collect-dart-adjusted-local <from> <to> [overlap_days]`: DART 수집 후 계수 재구축
- `just serve-local`: API 로컬 실행
- `just test`: 테스트 실행

## 주요 문서
- `docs/README.md`: 문서 인덱스
- `docs/QUICKSTART.md`: 로컬 설치 및 실행 흐름
- `docs/ARCHITECTURE.md`: 시스템 구조
- `docs/SCHEMA.md`: 테이블/뷰 계약
- `docs/ADJUSTED_PRICE_IMPLEMENTATION_PLAN.md`: 남은 수정주가 작업 계획
- `docs/ADJUSTED_PRICE_BACKLOG.md`: 남은 백로그

## 참고
- Postgres는 `127.0.0.1:${POSTGRES_PORT}:5432`에만 바인딩됩니다.
- `.env`의 `DATABASE_URL`은 호스트 셸 명령 기준입니다.
- Compose 내부 명령에는 `postgres:5432`가 주입됩니다.

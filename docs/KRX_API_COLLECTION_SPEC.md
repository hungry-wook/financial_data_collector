# KRX API 수집 명세

## 목적
Phase 1 시장 데이터를 KRX OpenAPI에서 어떻게 수집하고 정규화하는지 정의합니다.

## 대상 데이터셋
- 종목 마스터: `instruments`
- 일별 시세: `daily_market_data`
- 벤치마크: `benchmark_index_data`
- 거래일 캘린더: `trading_calendar`
- 품질 이슈: `data_quality_issues`

## 시장 범위
- 기본값: `KOSDAQ`, `KOSPI`
- 벤치마크 매핑도 동일한 시장 코드를 따릅니다.

## 정규화 규칙
### 종목 코드
- `A005930` -> `005930`
- 숫자 코드는 6자리 0패딩 문자열로 맞춥니다.
- `instrument_id`는 `market_code + external_code`를 기반으로 한 UUID5입니다.

### 일별 시세
- 필수 필드: `open`, `high`, `low`, `close`, `volume`
- 거래정지 추정 규칙:
  - `open/high/low = 0` 이고 `close != 0`
  - 또는 `volume = 0` 이고 OHLC가 모두 동일
- 거래정지로 판단된 행은 저장 전 보정될 수 있습니다.

### 벤치마크
- `index_code`, `index_name`, `trade_date`, `open/high/low/close`를 저장합니다.
- OHLC가 비어 있는 행은 `PARTIAL`로 낮춰 기록합니다.

## 수집 흐름
1. 종료일 기준 종목 마스터를 조회합니다.
2. 기간 내 각 날짜별 시세를 수집합니다.
3. 기간 내 각 날짜별 벤치마크를 수집합니다.
4. 벤치마크 거래일을 기준으로 캘린더를 만듭니다.
5. 검증을 수행합니다.
6. 실행 결과를 `collection_runs`에 저장합니다.

## 실행 명령
- `uv run collect-krx-data --date-from 2026-02-09 --date-to 2026-03-06`
- `just collect-local 2026-02-09 2026-03-06`

## 운영 메모
최근 수정으로 DB 연결 재시도와 종목 존재 여부 배치 조회가 추가되어, 로컬 환경의 일시적인 연결 고갈 문제를 줄였습니다.

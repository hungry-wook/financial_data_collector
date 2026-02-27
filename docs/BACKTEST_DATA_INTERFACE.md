# 백테스트용 데이터 조회 인터페이스 (Phase 1 기반)

## 1. 목표
- 다른 레포의 백테스트 엔진이 최소한의 조인/정리로 바로 시뮬레이션을 수행할 수 있게 한다.
- Phase 1 계약(`*_v1`)을 유지하면서 전략 입력 데이터를 안정적으로 제공한다.

## 2. 전략 입력 매핑
전략 요구 입력 -> Phase 1 컬럼:
1. 종목 OHLCV: `core_market_dataset_v1.open/high/low/close/volume`
2. 거래대금: `core_market_dataset_v1.turnover_value`
3. 시가총액: `core_market_dataset_v1.market_value`
4. 상태: `core_market_dataset_v1.is_trade_halted/is_under_supervision`
5. 상장일: `core_market_dataset_v1.listing_date`
6. 벤치마크(KOSDAQ): `benchmark_dataset_v1.open/high/low/close`
7. 거래일 기준: `trading_calendar_v1.is_open`

## 3. 조회 방식 권장
1. 1순위: DB read-only 연결 + SQL 추출 (내부 서비스 간)
2. 2순위: HTTP API로 Parquet/CSV 일괄 추출 (네트워크 분리 환경)

원칙:
- 백테스트는 대량 시계열 조회가 핵심이므로, 단건 REST 반복 호출 방식은 비권장.
- 날짜 구간 단위 bulk 추출을 기본으로 한다.

## 4. 최소 조회 계약 (SQL)
## 4.1 종목 패널
```sql
SELECT
    instrument_id,
    external_code,
    market_code,
    instrument_name,
    listing_date,
    delisting_date,
    trade_date,
    open, high, low, close,
    volume,
    turnover_value,
    market_value,
    is_trade_halted,
    is_under_supervision,
    record_status
FROM core_market_dataset_v1
WHERE market_code IN :market_codes
  AND trade_date BETWEEN :date_from AND :date_to
ORDER BY trade_date, market_code, instrument_id;
```

## 4.2 벤치마크 패널
```sql
SELECT
    index_code,
    trade_date,
    open, high, low, close
FROM benchmark_dataset_v1
WHERE index_code = :index_code
  AND trade_date BETWEEN :date_from AND :date_to
ORDER BY trade_date;
```

## 4.3 거래일 캘린더
```sql
SELECT
    market_code,
    trade_date,
    is_open,
    holiday_name
FROM trading_calendar_v1
WHERE market_code IN :market_codes
  AND trade_date BETWEEN :date_from AND :date_to
ORDER BY market_code, trade_date;
```

## 4.4 품질 이슈 (옵션)
```sql
SELECT
    dataset_name,
    trade_date,
    instrument_id,
    index_code,
    issue_code,
    severity,
    issue_detail,
    run_id,
    detected_at
FROM data_quality_issues
WHERE trade_date BETWEEN :date_from AND :date_to
  AND severity IN ('WARN','ERROR')
ORDER BY detected_at;
```

## 5. 백테스트 친화 운영 규칙
1. 필수 정렬: 모든 결과를 `trade_date` 오름차순으로 제공
2. 시간 기준: EOD 확정 데이터만 제공 (`trade_date` 기준)
3. 결측 처리: 보간 금지, `record_status`/`data_quality_issues`로 전달
4. 포인트인타임: 상장일/상폐일 기준 필터 가능하도록 마스터 컬럼 포함
5. 역호환: 컬럼 추가는 허용, 삭제/rename 금지 (`v2` 분리 원칙)

## 6. API로 감쌀 때의 최소 엔드포인트
1. `POST /api/v1/backtest/export`
- body: `market_codes`, `index_codes`, `date_from`, `date_to`, `format(parquet|csv)`
- 동작: 비동기 추출 작업 생성

2. `GET /api/v1/backtest/export/{job_id}`
- 동작: 작업 상태 조회 + 파일 다운로드 URL 반환

권장 출력:
- `instrument_daily.parquet`
- `benchmark_daily.parquet`
- `trading_calendar.parquet`
- `data_quality_issues.parquet` (옵션)

## 7. 구현 언어 권장
권장: **Python**

이유:
1. 수집 파이프라인과 조회 서비스를 같은 언어로 유지 가능
2. `FastAPI + SQLAlchemy + Polars/PyArrow` 조합으로 bulk 추출 구현이 빠름
3. 다른 레포(백테스트)도 Python일 가능성이 높아 인터페이스/검증 코드 재사용이 쉬움

성능 참고:
- API 앞단이 아니라 DB/파일 포맷(Parquet)과 쿼리 범위가 병목을 좌우한다.
- 초기에 Python으로 시작하고, 병목이 확인되면 추출 워커만 별도 최적화한다.

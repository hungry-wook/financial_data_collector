# 백테스트 데이터 인터페이스

## 목적
하위 백테스트 소비자가 사용하는 데이터 계약을 정의합니다.

## 원칙
- 시그널 생성에는 `adjusted` 시계열을 사용합니다.
- 체결 및 리스크 점검에는 `raw` 시계열을 사용합니다.
- 시점 일관성이 중요하면 `as_of_timestamp`를 사용합니다.

## 핵심 데이터셋
### 가격 데이터
- `core_market_dataset_v1`: 원주가 기준 소비 뷰
- `core_market_dataset_v2`: 원주가와 수정주가 컬럼을 함께 제공하는 뷰

### 벤치마크
- `benchmark_dataset_v1`

### 거래일 캘린더
- `trading_calendar_v1`

## 가격 필드
### 원주가
- `open`, `high`, `low`, `close`, `volume`

### 수정주가
- `adj_open`, `adj_high`, `adj_low`, `adj_close`, `adj_volume`

## 참고
- `turnover_value`, `market_value`는 정책상 원본 값을 유지합니다.
- 수정 계수는 `price_adjustment_factors`에서 읽습니다.
- 근거 신뢰도가 낮은 이벤트는 자동 반영하지 않습니다.

## Export/API 계약
- `series_type`: `raw`, `adjusted`, `both`
- 선택 파라미터 `as_of_timestamp`
- export는 미리 구축된 계수만 읽고 즉시 재계산하지 않습니다.
- `adjusted`/`both` export? ??? materialized factor? ?? ??? ?????.

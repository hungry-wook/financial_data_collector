# KRX API 수집 명세 (Phase 1 데이터 생성용)

## 1. 목적
- Phase 1 테이블(`instruments`, `daily_market_data`, `benchmark_index_data`, `trading_calendar`, `data_quality_issues`)을 생성하기 위해 호출할 KRX OPEN API를 정의한다.
- 백테스트 입력 데이터(종목 일봉/지수 일봉/상장정보)를 안정적으로 적재한다.

## 2. 공식 출처
1. 서비스 이용방법: `https://openapi.krx.co.kr/contents/OPP/INFO/OPPINFO003.jsp`
2. 서비스 목록: `https://openapi.krx.co.kr/contents/OPP/INFO/service/OPPINFO004.cmd`
3. 지수 카테고리: `https://openapi.krx.co.kr/contents/OPP/USES/service/OPPUSES001_S1.cmd`
4. 주식 카테고리: `https://openapi.krx.co.kr/contents/OPP/USES/service/OPPUSES002_S1.cmd`
5. 이용약관(호출 제한): `https://openapi.krx.co.kr/contents/OPP/INFO/OPPINFO002.jsp`

## 3. 인증/호출 공통 규칙
1. API 인증키 신청 + 서비스별 이용신청 승인 후 호출 가능
2. 요청 헤더에 인증키 전달
- header key: `AUTH_KEY`
3. 응답 포맷: `json` 또는 `xml`
4. 호출 제한: 1개 키당 1일 10,000회 이하(약관 기준)

호출 기본값(권장):
1. Base URL: `https://data-dbg.krx.co.kr`
2. 코스닥 종목기본정보: `/svc/apis/sto/ksq_isu_base_info`
3. 코스닥 일별매매정보: `/svc/apis/sto/ksq_bydd_trd`
4. 코스닥 지수 일별시세: `/svc/apis/idx/kosdaq_dd_trd`

## 4. Phase 1 필수 호출 API
## 4.1 코스닥 종목기본정보 (Instrument Master)
- 서비스 페이지: `https://openapi.krx.co.kr/contents/OPP/USES/service/OPPUSES002_S2.cmd?BO_ID=CifLHplnUFMgpHIMMPXs`
- 사용 목적:
1. `instruments.external_code`
2. `instruments.instrument_name`
3. `instruments.market_code`
4. `instruments.listing_date`
5. `instruments.delisting_date` (소스 제공 시)
- 수집 주기: 일 1회(장 종료 후)
- 적재 정책: upsert (key: `market_code + external_code`)

## 4.2 코스닥 일별매매정보 (Instrument Daily)
- 서비스 페이지: `https://openapi.krx.co.kr/contents/OPP/USES/service/OPPUSES002_S2.cmd?BO_ID=hZjGpkllgCBCWqeTsYFj`
- 사용 목적:
1. `daily_market_data.open/high/low/close`
2. `daily_market_data.volume`
3. `daily_market_data.turnover_value`
4. `daily_market_data.market_value`
5. 상태 관련 필드(거래정지/관리) 소스 제공 시 매핑
- 수집 주기: 거래일 기준 일 1회
- 적재 정책: upsert (key: `instrument_id + trade_date`)

## 4.3 KOSDAQ 시리즈 일별시세정보 (Benchmark)
- 서비스 페이지: `https://openapi.krx.co.kr/contents/OPP/USES/service/OPPUSES001_S2.cmd?BO_ID=nimebcamqFNIPNcRrHoO`
- 사용 목적:
1. `benchmark_index_data.index_code`
2. `benchmark_index_data.trade_date`
3. `benchmark_index_data.open/high/low/close`
- 수집 주기: 거래일 기준 일 1회
- 적재 정책: upsert (key: `index_code + trade_date`)

## 5. 거래일 캘린더 생성 정책
KRX OPEN API 서비스 목록에서 전용 캘린더 API를 별도 사용하지 않는 경우, 아래 우선순위로 생성:
1. 지수 일별시세의 `trade_date`를 open day(`is_open=true`)로 생성
2. 기간 내 누락일은 휴장일로 채움(`is_open=false`)
3. `holiday_name`은 별도 공휴일 소스 연동 전까지 `NULL`

참고:
- 캘린더 정확도를 높이려면 추후 별도 휴장일 데이터 소스(Phase 2+) 연결 권장

## 6. API ID / 요청 파라미터 확정 방법
서비스 상세 페이지의 "개발 명세서 다운로드"에서 아래를 확정한다.
1. API ID
2. 요청 파라미터명(예: 기준일자, 종목코드, 인덱스코드 등)
3. 샘플 URL/HTTP 메서드
4. 응답 필드명/타입

주의:
- KRX 페이지는 동적 렌더링이라 브라우저 비로그인 크롤링 시 API ID/샘플 URL이 노출되지 않을 수 있다.
- 구현 시점에는 승인된 계정으로 다운로드한 개발 명세서를 기준으로 코드/테스트를 고정한다.

## 7. Phase 1 컬럼 매핑 체크리스트
1. `instruments`
- `external_code`, `market_code`, `instrument_name`, `listing_date`, `delisting_date`

2. `daily_market_data`
- `open`, `high`, `low`, `close`, `volume`, `turnover_value`, `market_value`
- `is_trade_halted`, `is_under_supervision`

3. `benchmark_index_data`
- `index_code`, `trade_date`, `open`, `high`, `low`, `close`

4. `trading_calendar`
- `market_code`, `trade_date`, `is_open`, `holiday_name`

5. `data_quality_issues`
- 누락/검증실패/소스오류를 row 단위 또는 일자 단위로 기록

## 8. 수집 실행 표준 (권장)
1. `collection_runs`에 run 시작/종료 기록
2. API 실패 시 지수백오프 재시도(예: 1s, 2s, 4s, 8s, max 5회)
3. 최종 실패는 `data_quality_issues` 기록 + run 상태 `PARTIAL` 또는 `FAILED`
4. 결측 보간 금지, 원본값 유지

## 9. 변경 관리
1. KRX 서비스 최근 수정일 점검 후(서비스 상세 페이지) 파서 회귀 테스트 수행
2. 응답 스키마 변경 시 `docs/API_COLLECTION_PLAN.md`, `docs/SCHEMA.md` 동시 갱신
3. 소비 뷰 계약(`*_v1`) 깨지면 신규 버전 뷰(`v2`) 추가

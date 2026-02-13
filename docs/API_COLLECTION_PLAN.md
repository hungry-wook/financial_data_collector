# KRX + OpenDART 데이터 수집 계획 (페이지 검토 반영)

## 1. 검토한 공식 페이지
- OpenDART 개발가이드(DS001): https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS001
- KRX OPEN API 서비스 목록: https://openapi.krx.co.kr/contents/OPP/INFO/service/OPPINFO004.cmd
- KRX 휴장일 페이지: https://open.krx.co.kr/contents/MKD/01/0110/01100305/MKD01100305.jsp

## 2. 페이지에서 확인된 사실

## 2.1 OpenDART (DS001)
공시정보 API 4종이 공식 목록으로 제시됨.
1. 공시검색
2. 기업개황
3. 공시서류원본파일
4. 고유번호

상세 페이지 기준 확인 결과:
- 공시검색: GET `https://opendart.fss.or.kr/api/list.json` (XML 대체 URL 존재)
- 기업개황: GET `https://opendart.fss.or.kr/api/company.json`
- 공시서류원본: GET `https://opendart.fss.or.kr/api/document.xml` (Zip binary)
- 고유번호: GET `https://opendart.fss.or.kr/api/corpCode.xml` (Zip binary)

## 2.2 KRX OPEN API
서비스 목록 페이지에서 주식 영역 핵심 API 확인:
- 유가증권 일별매매정보
- 코스닥 일별매매정보
- 코넥스 일별매매정보
- 유가증권/코스닥/코넥스 종목기본정보

개별 API 페이지 확인:
- 요청 헤더 `AUTH_KEY` 사용
- "개발 명세서 다운로드" 제공
- 최근 수정일 표시됨 (변경 감시 필요)

주의:
- 동적 페이지 특성상 요청/응답 상세 필드가 본문 텍스트로 완전 노출되지 않음
- 구현 전 반드시 "개발 명세서 다운로드" 파일을 받아 키 이름을 확정해야 함

## 2.3 KRX 휴장일 페이지
- 연도 선택 범위(최근 다년) + 거래소 구분(한국거래소/CME/EUREX) 제공
- 조회/다운로드 동작 존재

운영 해석:
- 캘린더는 API가 아닌 페이지/다운로드 기반 수집 대상으로 간주
- `trading_calendar`는 정기 동기화 + 증분 검증 방식으로 운영

## 3. Phase별 수집 설계

## 3.1 Phase 1 (MVP)
목표: 모멘텀 전략 실행에 필요한 최소/정확 수집

수집 소스:
- KRX: 주식 일별매매정보 + 종목기본정보
- KRX: 휴장일 페이지(캘린더)
- OpenDART: list.json + document.xml(필요 시) + corpCode.xml

동작 순서:
1. 캘린더 동기화
2. 심볼 동기화
3. 가격 수집
4. 이벤트 후보 수집(list)
5. 이벤트 상세 보강(document)
6. 원장 적재 + 보정 계산

정책:
- KRX 단일 소스 고정 (source conflict 정책 미적용)
- 미지원 이벤트는 `SKIPPED_*`로 로깅

## 3.2 Phase 2
목표: 실패 복구 자동화

추가:
- ingestion_partitions로 파티션 단위 재시도
- 캘린더/가격/이벤트 각 파티션 상태 추적
- snapshot stale/supersede 자동 생성

## 3.3 Phase 3
목표: 범용 데이터 플랫폼

추가:
- 다중 가격 소스 도입 (source_priority_policy)
- 이벤트 타입 정책 테이블(event_type_policy)
- 엔터티 해석 계층 강화

## 4. API -> DB 연결 매핑

## 4.1 KRX 일별매매정보 -> prices_raw
입력:
- market, date

출력 매핑:
- 종목코드 -> symbol resolver 입력(code/date/market)
- 거래일자 -> prices_raw.date
- 시고저종 -> prices_raw.open/high/low/close
- 거래량 -> prices_raw.volume
- 거래대금 -> prices_raw.value

검증:
- high >= max(open, close, low)
- low <= min(open, close, high)
- volume >= 0

정정 처리:
- logical_hash 불일치 시 price_revision_seq 증가 append

## 4.2 KRX 종목기본정보 -> symbols/symbol_code_history/symbol_trading_state
매핑:
- 종목코드/시장/종목명 -> symbols
- 코드 변경 시 -> symbol_code_history 구간 이력
- 거래정지/상폐 상태 -> symbol_trading_state

## 4.3 KRX 휴장일 페이지 -> trading_calendar
매핑:
- date, market, holiday_name -> trading_calendar
- is_open 계산:
  - 휴장일 목록 포함 날짜: FALSE
  - 그 외 평일: TRUE (주말은 FALSE)

검증:
- 거래일인데 가격 전무하면 COLLECTION_FAILURE 또는 SOURCE_EMPTY 후보
- 휴장일인데 가격 존재하면 경고

## 4.4 DART list.json -> corp_actions(초기 이벤트)
핵심 입력 파라미터:
- crtfc_key
- corp_code (선택)
- bgn_de/end_de
- pblntf_ty/pblntf_detail_ty
- page_no/page_count

핵심 응답 필드 매핑:
- rcept_no -> source_event_id
- rcept_dt -> announce_date
- stock_code -> symbol resolver 입력
- report_nm -> event_type 1차 분류

## 4.5 DART document.xml -> corp_action_ratio / corp_action_dividend
용도:
- list 응답으로 부족한 ratio/dividend/pay_date/effective 단서 보강

정책:
- 문서 파싱 실패 시 이벤트 폐기 금지
- event_validation_status=INCOMPLETE로 저장

## 4.6 DART corpCode.xml -> 코드 마스터
매핑:
- corp_code, corp_name, stock_code, modify_date

용도:
- stock_code 없는 공시 케이스 보조 해석
- 향후 legal_entities 확장 기반

## 5. 스케줄/증분 전략

## 5.1 Price (KRX)
- 매 거래일 1회 기본 수집
- 최근 N일(기본 5일) 재수집으로 소급 정정 탐지

## 5.2 Event (DART)
- 최근 90일 rolling 재스캔
- 분기 1회 full 스캔

## 5.3 Calendar
- 월 1회 전체 동기화
- 매일 수집 시 직전/당일 불일치 체크

## 6. 실패 처리
- 429/5xx: 백오프 재시도
- DART status 코드 기반 분기
  - 013(데이터 없음): no-data 처리
  - 020/800/900: 재시도
  - 인증키/권한 오류: 즉시 실패

## 7. 구현 전 확정해야 할 항목
1. KRX 각 API의 개발 명세서 파일 확보
2. KRX 응답 키 -> 내부 컬럼 매핑표 확정
3. DART report_nm -> event_type 사전 확정
4. document.xml 파싱 규칙/정규식 테스트 케이스 작성

## 8. 산출물 체크리스트
- [ ] `collectors/krx_price_collector.py`
- [ ] `collectors/krx_symbol_collector.py`
- [ ] `collectors/dart_event_collector.py`
- [ ] `normalizers/event_normalizer.py`
- [ ] `validators/event_validator.py`
- [ ] `repositories/*`
- [ ] `jobs/run_daily_price_job.py`
- [ ] `jobs/run_daily_event_job.py`
- [ ] `jobs/run_snapshot_job.py`

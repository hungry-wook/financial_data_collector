# 구현 지시서 (Phase별 상세 작업)

## 1. 공통 전제
- DB: PostgreSQL 14+
- 시간 규칙: DATE=KST 의미, TIMESTAMP=UTC 저장
- 코드 규칙: raw/event append-only

## 2. 프로젝트 구조 권장
```text
src/
  collectors/
    krx_price_collector.py
    krx_symbol_collector.py
    dart_event_collector.py
  normalizers/
    price_normalizer.py
    event_normalizer.py
  validators/
    price_validator.py
    event_validator.py
  engines/
    adjustment_engine.py
    snapshot_engine.py
    quality_gate_engine.py
  repositories/
    prices_repository.py
    events_repository.py
    snapshot_repository.py
  resolvers/
    symbol_resolver.py
  jobs/
    run_daily_price_job.py
    run_daily_event_job.py
    run_snapshot_job.py
sql/
  schema_phase1.sql
  schema_phase2.sql
  schema_phase3.sql
tests/
```

## 3. Phase 1 상세 작업

## 3.1 Task P1-01: 스키마 생성
- `SCHEMA.md`의 Phase 1 DDL 적용
- 마이그레이션 파일 분리:
  - `001_phase1_core.sql`
  - `002_phase1_views.sql`

검증:
- 모든 테이블 생성 여부
- 인덱스 생성 여부
- view 조회 성공 여부

## 3.2 Task P1-02: KRX 심볼/캘린더 수집기
입력:
- market 목록 (KOSPI/KOSDAQ)
- 기준일

출력:
- symbols, symbol_code_history, symbol_trading_state, trading_calendar

처리 절차:
1. API 호출
2. 응답 파싱
3. code->symbol upsert
4. 상태 이력 구간 업데이트

실패 처리:
- 배치 실패 시 ingestion_batches status=FAILED
- 부분 실패 시 PARTIAL + error_summary 기록

## 3.3 Task P1-03: KRX 가격 수집기
입력:
- market/date 파티션

출력:
- prices_raw append
- collection_gap_report

핵심 로직:
1. 응답 row별 symbol resolve
2. logical_hash 계산
3. 기존 최신 revision과 비교
4. 변경 시 price_revision_seq+1 append
5. 미변경 시 no-op

결측 처리:
- MARKET_HOLIDAY
- SYMBOL_SUSPENDED
- COLLECTION_FAILURE
- SOURCE_EMPTY
- UNKNOWN

## 3.4 Task P1-04: DART 이벤트 수집기
입력:
- 기간(bgn_de/end_de)

출력:
- corp_actions, corp_action_ratio, corp_action_dividend

로직:
1. `list.json`으로 후보 수집
2. report_nm 기반 1차 event_type 분류
3. 필요 시 `document.xml` 파싱으로 상세 필드 추출
4. event_id 생성
5. event_version 판정

version 판정:
- source_event_id 동일 + 경제 필드 변경 -> version++
- 메타만 변경 -> no-op

## 3.5 Task P1-05: Normalizer/Validator
가격 검증:
- high/low 논리
- volume 음수 금지
- scale=0 강제(MVP)

이벤트 검증:
- event_type 필수 필드 검증
- effective_date_source 설정
- usable_for_* 플래그 결정

## 3.6 Task P1-06: 보정 엔진
입력:
- prices_raw_latest
- corp_actions_latest

출력:
- adjustment_factors
- adjustment_evaluation_log
- daily_cumulative_adjustment

처리:
1. symbol별 유효 이벤트 로드
2. same-date 정렬 규칙 적용
3. factor 계산
4. RULE-ADJ-006 근사 검증
5. APPLIED/SKIPPED 로그 기록

## 3.7 Task P1-07: Snapshot 빌드
입력:
- 대상 batch 목록
- cutoff time

출력:
- view_snapshot_metadata ACTIVE row

필수 저장:
- included_ingestion_batches_hash
- include 정책(allow_partial_*)
- engine/view version

## 3.8 Task P1-08: 기본 품질 게이트
지표:
- completeness, timeliness, accuracy

차단:
- completeness < 0.95 이면 snapshot 생성 차단(기본)

## 3.9 Task P1-09: 테스트 자동화
필수 회귀:
1. 삼성전자 50:1 split
2. split+bonus same-date
3. cash+stock dividend same-date
4. ratio inverse 오류 검출

---

## 4. Phase 2 상세 작업

## 4.1 Task P2-01: ingestion_partitions 도입
- 기존 batch 중심 로직을 partition 단위 실행으로 리팩터링
- partition 상태머신:
  - PENDING -> RUNNING -> SUCCESS/FAILED

## 4.2 Task P2-02: 파티션 재시도 스케줄러
- FAILED + next_retry_at<=now 대상 재실행
- retry_count 상한 초과 시 manual queue

## 4.3 Task P2-03: snapshot stale/supersede 자동화
stale 감지 입력:
- cutoff 이전 revision/version 변경
- version/정책 변경

동작:
- 기존 snapshot STALE/ARCHIVED
- 새 snapshot 생성 + superseded_by 연결

## 4.4 Task P2-04: 품질 게이트 고도화
- ERROR 급증 임계치 기반 strategy view materialization 차단
- 게이트 판정 로그 저장

## 4.5 Task P2-05: 장애 플레이북 자동화
- API 장애 유형별 자동 fallback
- DB 장애 시 안전 재시작(runbook 코드화)

---

## 5. Phase 3 상세 작업

## 5.1 Task P3-01: 다중 소스 도입
- source_priority_policy 테이블 운영
- raw_price_view에서 source 충돌 정책 적용

## 5.2 Task P3-02: 이벤트 정책 테이블화
- event_type_policy 기반 required_fields 검증
- 신규 이벤트 추가 시 코드 변경 최소화

## 5.3 Task P3-03: 엔터티 해석 계층
- legal_entities, symbol_entity_mapping
- entity_resolution_history 기록 의무화
- resolver 서비스 분리

## 5.4 Task P3-04: 수동 개입 원장화
- corp_action_overrides 운영
- override 반영 시 새 event_version 강제

## 5.5 Task P3-05: 포지션 변환 엔진
- RIGHTS/MERGER/SPINOFF/PAID_CAPITAL_REDUCTION 처리
- strategy_price_view와 명시 연결

---

## 6. API -> DB 매핑 상세

## 6.1 KRX 가격
요청:
- AUTH_KEY 헤더
- market/date 파라미터

응답 -> DB:
- 종목코드: symbol resolver 입력
- 일자: prices_raw.date
- 가격/거래량/거래대금: prices_raw 각 필드

주의:
- KRX 상세 key 명은 개발 명세서 다운로드로 확정
- 파싱 키를 상수 파일로 고정

## 6.2 DART list.json
요청:
- crtfc_key, bgn_de, end_de, page_no...

응답 -> DB:
- rcept_no -> corp_actions.source_event_id
- rcept_dt -> announce_date
- stock_code -> symbol resolve 입력
- report_nm -> event_type 1차 분류

## 6.3 DART document.xml
입력:
- rcept_no

용도:
- ratio/dividend/effective_date 추가 추출

출력:
- corp_action_ratio/corp_action_dividend 보강

## 6.4 DART corpCode.xml
출력:
- corp_code/stock_code 마스터

용도:
- symbol resolve 보조

---

## 7. 체크리스트

## 7.1 Phase 1 시작 전
1. KRX 개발 명세서 파일 확보
2. DART API 키 설정
3. DB 마이그레이션 준비

## 7.2 Phase 1 완료 전
1. 회귀 테스트 4종 통과
2. snapshot 재현성 확인
3. 결측 사유 분류 확인

## 7.3 Phase 2 완료 전
1. 파티션 재시도 자동화
2. stale/supersede 자동화
3. 게이트 차단 동작 확인

## 7.4 Phase 3 완료 전
1. 다중 소스 충돌 정책 검증
2. 신규 이벤트 정책 등록 플로우 검증
3. resolver 일관성 검증

---

## 8. 주니어 개발자를 위한 구현 팁
1. 먼저 DDL과 repository 테스트부터 만든다.
2. collector를 완성하기 전에 normalizer/validator 단위 테스트를 작성한다.
3. 보정 엔진은 이벤트 1개/심볼 1개 데이터셋으로 시작한다.
4. snapshot hash가 바뀌는 조건을 테스트로 고정한다.
5. ad-hoc SQL 대신 표준 view를 repository에 캡슐화한다.

---

## 9. 산출물 정의
- 코드: collector/normalizer/validator/engine/repository
- SQL: phase별 마이그레이션
- 테스트: unit/integration/regression
- 운영: 배치 리포트/게이트 리포트

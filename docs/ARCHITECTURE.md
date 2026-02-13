# KRX + OpenDART 데이터 파이프라인 상세 설계서

## 1. 설계 목표
이 설계의 목적은 다음 3가지를 동시에 만족하는 것이다.
1. 빠른 MVP 구축 (모멘텀 전략 검증 가능)
2. 재현 가능한 실험/백테스트 (snapshot 불변)
3. 운영 확장성 (장애 복구, 이벤트 확장, 다중 소스)

## 2. 비기능 요구사항
- 재현성: 동일 snapshot_id는 동일 결과
- 감사성: 결과 row -> 원천 수집 row 추적 가능
- 안전성: 잘못된 ad-hoc 조회로 최신 선택 로직이 깨지지 않도록 표준 view 강제
- 확장성: Phase 3에서 이벤트/소스 확장 가능

## 3. 아키텍처
## 3.1 데이터 흐름
1. Calendar Sync
2. Symbol Sync
3. Price Collect (KRX)
4. Event Collect (OpenDART)
5. Normalize
6. Validate
7. Append Ledger
8. Snapshot Build
9. Adjustment Build
10. Factor/View Build
11. Strategy Consumption

## 3.2 계층 정의
- Raw Ledger 계층: 원천 사실 저장 (append-only)
- Derived 계층: 보정 factor, 품질 평가, snapshot 메타
- Consumption 계층: 최신 view, adjusted view, universe view

## 4. Phase 전략
## 4.1 Phase 1 (MVP)
목표:
- KRX 단일 소스 + DART 핵심 이벤트로 모멘텀 백테스트 수행 가능

범위:
- 이벤트 타입: SPLIT/REVERSE_SPLIT/BONUS/STOCK_DIVIDEND/CASH_DIVIDEND
- 미지원 이벤트는 skip + 로그
- snapshot 기반 재현성 확보

핵심 규칙:
- RULE-LATEST-001: raw 직접 조회 금지, latest view 사용
- RULE-ADJ-001: effective_date 기준 적용
- RULE-BT-001: 거래정지 구간 untradable
- RULE-BT-004: universe_asof 고정

## 4.2 Phase 2 (운영 안정화)
목표:
- 장애 자동복구, snapshot stale/supersede 자동화

추가 기능:
- ingestion_partitions 도입 (복구 단위)
- 품질 게이트에 따른 snapshot 생성 차단
- stale 원인 자동 판정

핵심 규칙:
- RULE-PART-001: 배치는 묶음, 파티션이 실행 단위
- RULE-SNP-002: cutoff 이전 영향 정정 유입 시 stale
- RULE-QG-001: 품질 미달 snapshot 생성 금지

## 4.3 Phase 3 (플랫폼 확장)
목표:
- 다중 소스, 복잡 이벤트, 엔터티 해석 고도화

추가 기능:
- source_priority_policy
- event_type_policy
- legal_entities/symbol_entity_mapping/entity_resolution_history
- corp_action_overrides 고도화

핵심 규칙:
- RULE-SRC-001: 소스 우선순위 정책 고정
- RULE-EVT-POLICY-001: 신규 이벤트 정책 등록 선행
- RULE-RESOLVE-001: 단일 symbol resolver 사용

## 5. 도메인 규칙
## 5.1 가격/이벤트 불변성
- Raw/Event 원장은 UPDATE/DELETE 금지
- 정정은 revision/version append

## 5.2 revision/version
- price: (symbol_id, date, source) 논리키 내 seq 증가
- event: source_event_id 체인에서 경제 필드 변경 시 version 증가

## 5.3 effective_date 정책
- source 명시 시 EXPLICIT_SOURCE
- ex_date 기반 파생 시 DERIVED_NEXT_TRADING_DAY
- 불명확하면 UNKNOWN
- derived는 opt-in 없으면 기본 미적용

## 5.4 same-date 이벤트 정렬
동일 (symbol_id, effective_date):
1. SPLIT/REVERSE_SPLIT
2. BONUS/STOCK_DIVIDEND
3. CAPITAL_REDUCTION(FREE)
4. CASH_DIVIDEND(가격엔진 제외)
5. RIGHTS/MERGER/SPINOFF/PAID_CAPITAL_REDUCTION(포지션 엔진)

## 5.5 백테스트 소비 규칙
- 거래정지: 기본 untradable, forward-fill 금지
- 상폐/재상장: 기본 disconnected
- code 변경: 동일 symbol 내 연속
- universe: as-of 상장 + 비정지 + 비상폐

## 6. Snapshot 설계
## 6.1 Snapshot 식별 요소
- snapshot_as_of_date
- snapshot_data_cutoff_time
- engine/view version
- included_ingestion_batches_hash
- included_source_priority_policy_version
- allow_partial_price/event

## 6.2 stale/supersede
stale 조건:
- cutoff 이전 영향 데이터 변경
- engine/view 버전 변경
- 정책 버전 변경

supersede:
- 기존 snapshot 삭제 금지
- 새 snapshot_id 생성
- 기존 snapshot은 ARCHIVED + superseded_by 연결

## 7. 품질/게이트 설계
품질 지표:
- completeness
- timeliness
- accuracy

게이트:
- completeness < 0.95 -> snapshot 생성 차단 (기본)
- ERROR rule 급증 -> strategy view materialization 차단

## 8. API 설계
## 8.1 KRX
- 공식 서비스 인덱스: OPPINFO004
- AUTH_KEY 헤더 사용
- API별 상세 필드는 개발 명세서 다운로드로 최종 고정

## 8.2 OpenDART DS001
- list.json: 이벤트 후보 수집
- company.json: 법인 보조정보
- document.xml: 상세 파싱(필수 시)
- corpCode.xml: 코드 마스터

## 9. 장애/복구 설계
- Phase 1: batch 단위 재시도
- Phase 2: partition 단위 자동 재시도 + next_retry_at 스케줄
- 장애 시 누락 파티션/결측 사유를 반드시 DB에 기록

## 10. 성능 설계
- adjusted 조회는 daily_cumulative_adjustment 전처리 사용
- 이벤트 추가 시 영향 구간(해당 symbol, effective_date 이전) 부분 재계산

## 11. 보안/운영
- API 키는 환경변수/시크릿 매니저로 관리
- API 응답 전문은 필요 최소 범위 저장
- 감사 로그는 최소 90일 hot 보관

## 12. 개발자가 자주 실수하는 지점
1. raw 최신값을 ad-hoc SQL로 뽑는 것
2. effective_date 당일에 factor를 적용하는 것
3. 거래정지/휴장/수집실패 결측을 구분하지 않는 것
4. snapshot 입력 배치 집합을 고정하지 않는 것
5. ratio 방향(분자/분모)을 뒤집는 것

## 13. 수용 기준
- Phase 1 완료: 모멘텀 백테스트 재현 가능 + 핵심 회귀 테스트 통과
- Phase 2 완료: 자동 복구/게이트/스냅샷 supersede 자동화
- Phase 3 완료: 다중 소스/확장 이벤트/엔터티 해석 안정화

# 최초 피드백 대비 현재 문서 상태 점검

## 1. 기준
검토 기준은 사용자가 처음 제시한 3가지다.
- A. 가독성 개선 필요사항
- B. 고도화 가능 영역(P0/P1/P2)
- C. 기존 섹션 보강 포인트

또한 최근 피드백(과설계/누락)도 함께 반영 여부를 확인했다.

---

## 2. 누락 여부 점검

## 2.1 A(가독성) 항목
- 목차: 반영됨
- 용어집: 반영됨
- 다이어그램(ASCII -> Mermaid): 반영됨
- worked example: 반영됨
- 섹션 불균형 보강: 반영됨
- 규칙 ID 부여: 반영됨
- callout 사용: 반영됨

판정: **주요 누락 없음**

## 2.2 B(P0/P1/P2) 항목
- P0 same-date ordering: 반영됨
- P0 timezone 정책: 반영됨
- P0 symbol lifecycle: 반영됨
- P1 data quality framework: 반영됨
- P1 circuit breaker: 반영됨
- P1 recovery playbook: 반영됨
- P1 testing strategy: 반영됨
- P2 performance/retention/migration/lineage/index: 반영됨

판정: **기능적 누락 없음**

## 2.3 C(기존 섹션 보강) 항목
- Design principles 반패턴/근거: 반영됨
- Data source 매핑: 반영됨
- Collector 시퀀스/오류: 반영됨
- Validation 표 형태: 반영됨
- Adjustment worked example: 반영됨
- Logging/output guarantee 검증: 반영됨

판정: **핵심 누락 없음**

---

## 3. 현재 과한 정보(Phase 1 기준)
다음은 구현 순서상 Phase 1에서 과할 수 있다.
1. legal_entities/symbol_entity_mapping/entity_resolution_history 전면 도입
2. source_priority_policy/conflict_policy (KRX 단일 소스 MVP에서 미사용)
3. event_type_policy JSON required_fields (이벤트 소수일 때 과함)
4. ingestion_partitions + stale 이벤트 테이블 전체

권고:
- 삭제가 아니라 **Phase 3 이관** 유지
- 문서/DDL에서 "Phase 1 필수"와 "Phase 2/3 선택" 태그를 명확히 유지

---

## 4. 구현에 필요한데 빠지기 쉬운 정보 (추가 체크)
1. KRX API 상세 필드명 확정 절차
- 현재 페이지만으로 완전 추출이 어렵기 때문에 "개발 명세서 다운로드 파일" 기반 확정 절차를 반드시 실행해야 함

2. DART 문서 파싱 규칙
- report_nm 기반 분류만으로는 ratio/배당 상세가 부족할 수 있어 document.xml 파싱 규칙/테스트가 필수

3. 전략 소비 디폴트
- 거래정지(untradable), 상폐/재상장(disconnected), universe_asof 기준은 이미 정의되어 있으나, 코드 기본값과 문서 값이 일치하는지 테스트가 필요

4. revision/version 무결성 검증
- DB 제약만으로 완전 강제가 어려운 영역은 배치 검증 쿼리 + 실패 정책을 구현해야 함

---

## 5. 문서별 역할 분리 적정성
- `DESIGN.md`: 왜 이렇게 설계했는지(원칙/정책)
- `IMPLEMENTATION_GUIDE.md`: 무엇을 어떤 순서로 구현할지
- `SCHEMA.md`: 전체 스키마 상세 DDL
- `SCHEMA_ANNOTATED.sql`: 컬럼별 의미 주석 포함 실행 DDL
- `API_COLLECTION_PLAN.md`: 공식 페이지 기반 API 수집 계획

판정: **분리 적절**

---

## 6. 최종 권고
1. 즉시 개발은 Phase 1만 적용
2. Phase 2/3 테이블은 마이그레이션 파일 분리
3. KRX 명세서 확정 전에는 파싱 키 하드코딩 금지
4. 회귀 테스트 4종 + snapshot 재현성 테스트를 CI 필수로 지정

# financial_data_collector

금융 시계열 데이터 수집/정규화/검증 문서 저장소입니다.

## 문서 구조
- `docs/README.md`: 문서 전체 내비게이션
- `docs/ARCHITECTURE.md`: 설계 원칙과 Phase 로드맵
- `docs/API_COLLECTION_PLAN.md`: 데이터 소스-스키마 매핑 계획
- `docs/KRX_API_COLLECTION_SPEC.md`: KRX 수집 API 명세
- `docs/IMPLEMENTATION_GUIDE.md`: Phase별 구현 지시서
- `docs/TDD_TODO_CHECKLIST.md`: TDD 기반 구현 TODO
- `docs/PHASE1_INTERFACE_DESIGN.md`: Phase 1 + 조회 인터페이스 설계안
- `docs/BULK_EXPORT_API_SPEC.md`: 기간 벌크 추출 API 규격
- `docs/SCHEMA.md`: 스키마 계약 문서
- `docs/SCOPE_REVIEW.md`: 범위 및 적합성 점검 기록
- `sql/platform_schema.sql`: 실행 DDL(Phase 1~4)

## 권장 읽기 순서
1. `docs/ARCHITECTURE.md`
2. `docs/SCHEMA.md`
3. `docs/API_COLLECTION_PLAN.md`
4. `docs/IMPLEMENTATION_GUIDE.md`

## 구현 시작점
- DB 생성: `sql/platform_schema.sql`
- 구현 착수: `docs/IMPLEMENTATION_GUIDE.md`의 Phase 1

## KRX 실연동 테스트 준비
1. `.env.example`을 참고해 `.env` 작성
2. 필수 값 입력:
- `KRX_AUTH_KEY`
- `KRX_BASE_URL`
- `KRX_API_PATH_INSTRUMENTS`
- `KRX_API_PATH_DAILY_MARKET`
- `KRX_API_PATH_INDEX_DAILY`
3. 사전점검 테스트:
- `pytest -q tests/test_preflight.py`
4. 실연동 스모크 테스트:
- `pytest -q -m integration`

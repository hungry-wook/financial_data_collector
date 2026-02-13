# financial_data_collector

KRX + OpenDART 기반 한국 주식 데이터 파이프라인 문서입니다.

## 문서 구조
- `docs/README.md`: 문서 전체 내비게이션
- `docs/ARCHITECTURE.md`: 설계 원칙/규칙/Phase 전략
- `docs/API_COLLECTION_PLAN.md`: 공식 페이지 기반 API 수집/매핑 계획
- `docs/IMPLEMENTATION_GUIDE.md`: 단계별 구현 지시서
- `docs/SCHEMA.md`: 스키마 상세 명세(설명형)
- `docs/SCOPE_REVIEW.md`: 누락/과설계/보강 검토
- `sql/schema_annotated.sql`: 컬럼 주석 포함 실행 DDL

## 권장 읽기 순서
1. `docs/ARCHITECTURE.md`
2. `docs/API_COLLECTION_PLAN.md`
3. `docs/SCHEMA.md`
4. `docs/IMPLEMENTATION_GUIDE.md`
5. `docs/SCOPE_REVIEW.md`

## 구현 시작점
- DB 생성: `sql/schema_annotated.sql`
- Phase 1 구현은 `docs/IMPLEMENTATION_GUIDE.md`의 Phase 1 작업부터 진행

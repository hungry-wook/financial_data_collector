# financial_data_collector

금융 시계열 데이터 수집/정규화/검증 문서 저장소입니다.

## 문서 구조
- `docs/README.md`: 문서 전체 내비게이션
- `docs/ARCHITECTURE.md`: 설계 원칙과 Phase 로드맵
- `docs/API_COLLECTION_PLAN.md`: 데이터 소스-스키마 매핑 계획
- `docs/IMPLEMENTATION_GUIDE.md`: Phase별 구현 지시서
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

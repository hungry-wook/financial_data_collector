# Documentation Index

## 문서 목록
- 설계: `ARCHITECTURE.md`
- 소스 수집 계획: `API_COLLECTION_PLAN.md`
- KRX 수집 API 명세: `KRX_API_COLLECTION_SPEC.md`
- KRX 실연동 테스트 가이드: `KRX_INTEGRATION_TEST_GUIDE.md`
- 구현 지시: `IMPLEMENTATION_GUIDE.md`
- Phase 1 + 조회 인터페이스 설계: `PHASE1_INTERFACE_DESIGN.md`
- 백테스트 데이터 조회 계약: `BACKTEST_DATA_INTERFACE.md`
- 벌크 추출 API 규격: `BULK_EXPORT_API_SPEC.md`
- 스키마 계약: `SCHEMA.md`
- 범위 점검: `SCOPE_REVIEW.md`
- 실행 DDL: `../sql/platform_schema.sql`

## 문서 역할
- `ARCHITECTURE.md`
: 설계 원칙, 책임 경계, Phase 로드맵
- `API_COLLECTION_PLAN.md`
: 소스별 수집 범위와 내부 스키마 매핑
- `IMPLEMENTATION_GUIDE.md`
: 개발 순서와 Task별 완료 기준
- `SCHEMA.md`
: 테이블/뷰/제약의 계약 정의
- `SCOPE_REVIEW.md`
: 현재 스코프 적합성 점검 결과
- `../sql/platform_schema.sql`
: 적용 가능한 기준 DDL

## 유지보수 규칙
1. DDL 변경 시 `../sql/platform_schema.sql` 먼저 수정
2. 계약 변경 시 `SCHEMA.md`와 `IMPLEMENTATION_GUIDE.md` 동시 업데이트
3. 소스 변경 시 `API_COLLECTION_PLAN.md` 먼저 갱신

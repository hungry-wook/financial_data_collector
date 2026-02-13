# Documentation Index

## 목적별 문서
- 설계: `ARCHITECTURE.md`
- API 수집 계획: `API_COLLECTION_PLAN.md`
- 구현 지시: `IMPLEMENTATION_GUIDE.md`
- 스키마 설명: `SCHEMA.md`
- 범위/품질 점검: `SCOPE_REVIEW.md`
- 실행 DDL: `../sql/schema_annotated.sql`

## 문서 역할
- `ARCHITECTURE.md`
: 왜 이렇게 설계했는지(원칙, 트레이드오프, 규칙)
- `API_COLLECTION_PLAN.md`
: KRX/DART 페이지 기반으로 무엇을 어떻게 수집할지
- `IMPLEMENTATION_GUIDE.md`
: 개발자가 바로 작업 가능한 Task 순서와 완료 기준
- `SCHEMA.md`
: 테이블/뷰/제약의 설명형 사양
- `SCOPE_REVIEW.md`
: 과설계/누락/필수 보강 항목 점검
- `../sql/schema_annotated.sql`
: 실제 적용 가능한 SQL(모든 컬럼 주석 포함)

## 유지보수 규칙
1. DDL 변경 시 `../sql/schema_annotated.sql` 먼저 수정
2. 규칙 변경 시 `ARCHITECTURE.md`와 `IMPLEMENTATION_GUIDE.md` 동시 업데이트
3. API 변경 시 `API_COLLECTION_PLAN.md` 매핑표 먼저 갱신

# 문서 인덱스

## 핵심 문서
- `ARCHITECTURE.md`: 시스템 구조와 책임 경계
- `SCHEMA.md`: 테이블, 뷰, 제약 조건 계약
- `QUICKSTART.md`: 로컬 설치 및 실행 절차
- `DEPLOYMENT_GUIDE.md`: 운영 배포 및 점검 가이드
- `BACKTEST_DATA_INTERFACE.md`: 백테스트 입력 데이터 계약
- `PHASE1_INTERFACE_DESIGN.md`: Phase 1 읽기/쓰기 인터페이스 설계
- `KRX_API_COLLECTION_SPEC.md`: KRX 수집 규칙과 엔드포인트
- `KRX_INTEGRATION_TEST_GUIDE.md`: 실제 KRX 연동 점검 절차
- `ADJUSTED_PRICE_IMPLEMENTATION_PLAN.md`: 남은 수정주가 구현 계획
- `ADJUSTED_PRICE_BACKLOG.md`: 남은 수정주가 백로그

## 권장 읽기 순서
1. `QUICKSTART.md`
2. `ARCHITECTURE.md`
3. `SCHEMA.md`
4. `BACKTEST_DATA_INTERFACE.md`
5. 필요한 운영 문서를 추가로 확인

## 유지보수 규칙
- 계약이 바뀌면 `SCHEMA.md`, 관련 인터페이스 문서, 테스트를 함께 갱신합니다.
- 실행 명령이 바뀌면 `README.md`, `QUICKSTART.md`, `DEPLOYMENT_GUIDE.md`를 함께 갱신합니다.
- 수정주가 관련 잔여 작업은 구현 계획 문서와 백로그 문서에서만 관리합니다.

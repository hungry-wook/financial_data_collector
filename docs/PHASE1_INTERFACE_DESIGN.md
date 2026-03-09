# Phase 1 인터페이스 설계

## 목표
Phase 1 수집 및 제공 계층에 대해 안정적인 쓰기/읽기 인터페이스를 노출합니다.

## 쓰기 경로
- KRX 수집기 -> `instruments`, `daily_market_data`, `benchmark_index_data`, `trading_calendar`
- DART 수집기 -> `corporate_events`, `event_validation_results`
- 수정 계수 재구축 -> `price_adjustment_factors`

## 읽기 경로
- 대시보드 API
- 백테스트 export API
- 리포지토리 조회 메서드

## 조회 규칙
- 기본 시계열은 `raw`입니다.
- `adjusted`, `both`는 명시적으로 요청해야 합니다.
- `as_of_timestamp`는 해당 시점까지 알려진 공시만 반영하도록 제한합니다.

## 주요 API 엔드포인트
- `GET /health`
- `GET /dashboard`
- `GET /api/v1/dashboard/...`
- `POST /api/v1/backtest/exports`
- `GET /api/v1/backtest/exports/{job_id}`
- `GET /api/v1/backtest/exports/{job_id}/manifest`

## 비목표
- 전략 실행
- 포트폴리오 시뮬레이션
- 성과 리포팅

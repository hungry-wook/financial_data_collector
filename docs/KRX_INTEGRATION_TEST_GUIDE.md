# KRX Integration Test Guide

## 1. Pre-check
1. KRX Open API 이용신청 승인 완료
2. 인증키 발급 완료
3. 대상 API(종목기본/일별매매/지수일별) 사용권한 승인 완료

## 2. .env 준비
필수:
1. `KRX_AUTH_KEY`
2. `KRX_BASE_URL` (기본: `https://data-dbg.krx.co.kr`)
3. `KRX_API_PATH_INSTRUMENTS` (기본: `/svc/apis/sto/ksq_isu_base_info`)
4. `KRX_API_PATH_DAILY_MARKET` (기본: `/svc/apis/sto/ksq_bydd_trd`)
5. `KRX_API_PATH_INDEX_DAILY` (기본: `/svc/apis/idx/kosdaq_dd_trd`)

옵션:
1. `KRX_TIMEOUT_SEC`
2. `KRX_MAX_RETRIES`
3. `KRX_DAILY_LIMIT`

## 3. 테스트 실행 순서
1. 사전점검
- `pytest -q tests/test_preflight.py`
2. 단위 테스트
- `pytest -q tests/test_krx_client.py`
3. 실연동 테스트
- `pytest -q -m integration`

## 4. 실연동 테스트 판정
성공 기준:
1. 응답이 dict 구조로 파싱됨
2. HTTP 오류 없이 호출 완료
3. 응답 key 1개 이상 확인

실패 시 점검:
1. 인증키 만료/오입력
2. API path 오입력
3. 서비스 권한 미승인
4. 호출 제한 초과
5. KRX 서버 장애/일시 오류

## 5. 운영 권장
1. integration 테스트는 CI 기본 파이프라인에서 제외하고 수동/야간 배치로 실행
2. 실패 시 원인별 재시도 정책(네트워크 vs 인증)을 분리
3. 응답 스키마가 바뀌면 파서 테스트를 즉시 업데이트

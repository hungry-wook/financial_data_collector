# KOREA EQUITY SYSTEM : END-TO-END DATA COLLECTION & BACKTEST PIPELINE SPECIFICATION

## 목표

* KRX + OpenDART를 사용하여 다음을 완전 재현 가능하게 구축한다.
  * 원천 가격 데이터(raw price)
  * corporate action 이벤트
  * point-in-time 팩터
  * 수정주가(adjusted price)
  * 백테스트용 가격/팩터 view

## 핵심 보장

* 재실행해도 결과가 동일해야 함 (Idempotent)
* DB를 날리고 전체 재수집해도 동일한 결과 생성
* 모든 데이터는 시점 기준(point-in-time) 관리
* 전략/백테스트 레이어는 수집 로직을 전혀 몰라도 됨
* survivorship bias, look-ahead bias 제거

## DATA MUTABILITY CONTRACT

* Raw Price / Corporate Action Ledger는 append-only이다.
* UPDATE / DELETE는 허용되지 않는다.
* 정정 데이터는 새로운 row로 추가한다.
* "최신 값 선택"은 반드시 View 레이어의 책임이다.

## DESIGN PRINCIPLES

### Raw data is immutable

* 모든 파생 데이터는 raw + event로부터 재계산 가능
* 이벤트는 "발생 사실"과 "효력 시점"을 분리
* 가격 / 이벤트 / 팩터 / 캘린더는 독립 모듈
* View는 전략 편의를 위한 논리 계층일 뿐, source of truth 아님

### Price, Position, Cashflow 분리

* Price, Position, Cashflow는 서로 다른 개념이며 절대 혼합하지 않는다.
* 가격 조정은 순수 multiplicative 이벤트만 처리한다.
* 주식 수 변화, 종목 변환은 가격 조정과 분리된 엔진에서 처리한다.
* 배당 및 현금 유입은 가격과 분리된 cashflow로만 기록한다.

### Entity와 Security 분리

* 기업 법인(Legal Entity)과 상장된 거래 대상(Listed Security)은 분리된 identity로 관리한다.
* 가격 데이터는 Listed Security 기준으로만 귀속된다.
* 합병/분할/재상장 시 새로운 Listed Security를 생성한다.

### Append-only ledger

* Raw data는 append-only ledger로 취급하며, 수정(update)은 허용하지 않는다.

## GLOBAL PIPELINE OVERVIEW

```
[Collector]
    ↓
[Normalizer]
    ↓
[Validator]
    ↓
[Raw Storage]
    ↓
[Event Ledger]
    ↓
[Adjustment Engine]
    ↓
[Factor Engine]
    ↓
[Price / Factor Views]
    ↓
[Backtest / Strategy]
```

모든 단계는 재실행 가능. 중간 실패 후 이어서 실행 가능

## DATA SOURCES

### KRX

#### 지수

* KOSPI 시리즈 일별시세정보
  * https://openapi.krx.co.kr/contents/OPP/USES/service/OPPUSES001_S2.cmd?BO_ID=EREKZauXnMmxyIlqzeDN
* KOSDAQ 시리즈 일별시세정보
  * https://openapi.krx.co.kr/contents/OPP/USES/service/OPPUSES001_S2.cmd?BO_ID=nimebcamqFNIPNcRrHoO

#### 주식

* 유가증권 일별매매정보
  * 유가증권시장에 상장되어 있는 주권의 매매정보 제공 ('10년01월04일 데이터부터 제공)
  * https://openapi.krx.co.kr/contents/OPP/USES/service/OPPUSES002_S2.cmd?BO_ID=JvJFzlAENzZlPBDNGAWC
* 코스닥 일별매매정보
  * 코스닥시장에 상장되어 있는 주권의 매매정보 제공 ('10년01월04일 데이터부터 제공)
  * https://openapi.krx.co.kr/contents/OPP/USES/service/OPPUSES002_S2.cmd?BO_ID=hZjGpkllgCBCWqeTsYFj
* 유가증권 종목기본정보
  * 유가증권 종목기본정보 ('10년01월04일 데이터부터 제공)
  * https://openapi.krx.co.kr/contents/OPP/USES/service/OPPUSES002_S2.cmd?BO_ID=PiwgMdTwmsenXhmqqxuj
* 코스닥 종목기본정보
  * 코스닥 종목기본정보 ('10년01월04일 데이터부터 제공)
  * https://openapi.krx.co.kr/contents/OPP/USES/service/OPPUSES002_S2.cmd?BO_ID=CifLHplnUFMgpHIMMPXs

#### 휴장일

* https://open.krx.co.kr/contents/MKD/01/0110/01100305/MKD01100305.jsp
  * 미래 5년치까지 있음

### OpenDART

* 공시검색 : 공시 유형별, 회사별, 날짜별 등 여러가지 조건으로 공시보고서 검색기능을 제공합니다
  * https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019001
* 기업개황 : DART에 등록되어있는 기업의 개황정보를 제공합니다.
  * https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019002
* 공시서류원본파일 : 공시보고서 원본파일을 제공합니다.
  * https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019003
* 고유번호 : DART에 등록되어있는 공시대상회사의 고유번호,회사명,종목코드, 최근변경일자를 파일로 제공합니다.
  * https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019018

## DATABASE SCHEMA (CORE)

### prices_raw

원천 가격 데이터 (절대 수정 금지)

외부 데이터 소스(KRX 등)로부터 수집한 일별 원시 시세 데이터
어떤 corporate action도 반영하지 않은 "역사적 사실"만 저장
모든 수정주가 / 팩터 / 백테스트 가격은 이 테이블로부터 재계산 가능해야 함
재현성 보장을 위해 UPDATE 금지
동일 date + symbol_id의 정정 데이터는 overwrite하지 않고
새로운 수집 기록으로 추가 저장하는 것을 원칙으로 한다.
최신 값 선택은 view 레이어의 책임이다.

```sql
date            DATE
-- 거래일
-- trading_calendar 기준 실제 거래가 있었던 날짜
-- KST 기준
-- PK 구성 요소

symbol_id       UUID
-- 시스템 내부 종목 identity
-- symbols.symbol_id FK
-- 코드 변경, 시장 이동과 무관한 절대 식별자
-- PK 구성 요소

code            VARCHAR
-- 수집 시점의 거래소 종목 코드 (KRX code)
-- 원본 데이터 추적 및 감사(audit) 목적
-- identity 용도로 사용 금지

open            BIGINT
-- 시가
-- 원본 단위 그대로 저장 (KRW 정수)
-- 소수점 사용하지 않음 (부동소수점 오차 방지)

high            BIGINT
-- 고가
-- 반드시 high ≥ max(open, close, low)

low             BIGINT
-- 저가
-- 반드시 low ≤ min(open, close, high)

close           BIGINT
-- 종가
-- 모든 adjustment 계산의 기준 가격

volume          BIGINT
-- 거래량 (주식 수 기준)
-- 0 가능 (거래정지, 단일가 거래 등)
-- 음수 불가

value           BIGINT
-- 거래대금 (통상 KRW)
-- volume × 평균가격과 근사해야 함
-- 데이터 무결성 검증 및 이상치 탐지용

source          VARCHAR
-- 데이터 수집 출처
-- 예: 'KRX_OPENDATA'
-- 다중 소스 수집 시 provenance 추적 가능

collected_at    TIMESTAMP
-- 실제 수집 시각
-- idempotent 재수집 검증 및 audit 목적
-- 정정 데이터 유입 감지용

logical_hash    VARCHAR
-- date, symbol_id, open, high, low, close, volume, value, source
-- 를 기반으로 생성한 deterministic hash
-- 동일 logical_hash는 동일한 경제적 사실을 의미한다.

ingestion_batch_id UUID
-- 수집 실행 단위 식별자
-- 동일 배치 내 수집된 row들을 논리적으로 묶기 위함
-- 재수집 / 장애 복구 / audit 시점 재현에 사용

price_revision_seq INTEGER
-- 동일 (date, symbol_id, source) 내 논리적 정정 버전
-- DB 트랜잭션 내에서 (symbol_id, date, source) 기준
-- MAX(price_revision_seq) + 1로 결정된다.
-- Collector는 seq를 계산하지 않는다.

price_revision_reason VARCHAR
-- revision 발생 사유 (audit / debugging 목적)
-- SOURCE_CORRECTION : 거래소/원천 정정
-- LATE_ARRIVAL      : 지연 도착 데이터
-- PARSER_CHANGE     : 파서 / 수집 로직 변경
-- MANUAL_FIX        : 수동 보정
-- UNKNOWN           : 사유 미상

PK: (date, symbol_id, source, price_revision_seq)

-- IDEMPOTENCY INDEX:
-- UNIQUE INDEX ON (symbol_id, date, source, logical_hash)

-- RECOMMENDED INDEX:
-- UNIQUE INDEX ON (logical_hash)
-- logical_hash 충돌 방지 및 idempotent 수집 최적화용
-- NOTE:
-- logical_hash 단독 UNIQUE는
-- 서로 다른 symbol/date 간 우연적 충돌 가능성이 있으므로 금지한다.
-- logical_hash는 반드시 (symbol_id, date, source) 범위 내에서만
-- idempotency 판단에 사용한다.

-- OPERATIONAL NOTE (OPTIONAL):
-- 대규모 운영 환경에서는 index 수 증가로 인한 write amplification을
-- 방지하기 위해 logical_hash 단독 UNIQUE INDEX는 선택적으로 생략 가능하다.
-- 이 경우에도 (symbol_id, date, source, logical_hash) 범위 내
-- idempotency 보장은 반드시 유지되어야 한다.
```

#### REVISION RULE

동일 (symbol_id, date, source) 내에서
logical_hash가 다른 경우
DB에서 atomic하게 price_revision_seq를 증가시켜 저장한다.
병렬 수집 환경에서도 seq 충돌이 발생하지 않아야 한다.

#### IMPLEMENTATION CONSTRAINT

Collector는 price_revision_seq를 계산하지 않는다.
seq 증가는 반드시 DB 트랜잭션 레벨에서 수행되어야 하며,
다음 중 하나의 방식이 강제된다:

1. (symbol_id, date, source) 기준 row-level lock
2. 별도의 revision sequence allocator 테이블

#### CONCURRENCY SAFETY (MANDATORY)

병렬 수집 환경에서 price_revision_seq 충돌을 방지하기 위해
다음 중 하나를 반드시 구현해야 한다.

**(A) revision allocator table**

```sql
price_revision_allocator
  symbol_id
  date
  source
  current_seq

INSERT ... ON CONFLICT ... DO UPDATE
+ SELECT FOR UPDATE 로 seq 증가 보장
```

**(B) advisory lock**

```
lock_key = hash(symbol_id, date, source)
```

어느 방식이든 DB 레벨에서 atomicity가 보장되어야 한다.

### corp_actions

모든 corporate action 이벤트 원장 (ledger)

기업 행위(corporate action)에 대한 "사실 기록 원장"
가격 조정, 주식수 변화, 포지션 변환의 유일한 source of truth
announce / ex / effective 시점을 명확히 분리하여
look-ahead bias 및 survivorship bias를 원천 차단

```sql
event_id        VARCHAR
-- 이벤트 고유 식별자 (경제적 동일 이벤트)
-- 정정 여부와 무관하게 동일 event_id 유지
-- EVENT_ID GENERATION RULE (STRICT):
-- 1) source_event_id (예: DART rcept_no)가 존재하는 경우
--    → 반드시 이를 기반으로 event_id를 생성한다.
-- 2) source_event_id가 없는 경우에만
--    (source + code + announce_date + event_type + sequence)를
--    기반으로 hash를 생성한다.
-- event_id는 생성 이후 절대 재계산하지 않는다.
-- STABILITY NOTE (RECOMMENDED):
-- source_event_id가 없는 이벤트의 경우
-- sequence 기반 event_id 생성은 collector 구현 변경 시
-- 동일 이벤트에 대해 서로 다른 event_id가 생성될 수 있다.
--
-- 이를 방지하기 위해 다음 중 하나 이상을 포함한
-- event payload fingerprint 기반 hash 생성을 권장한다.
--   * announce_date
--   * ex_date
--   * effective_date (if exists)
--   * 주요 경제 파라미터 (ratio, cash_amount 등)
--   * 공시 원문 content hash (가능한 경우)

--
-- STRONG RECOMMENDATION:
-- DART 기반 이벤트의 경우 공시 원문(content) hash를
-- fingerprint 구성 요소에 포함하는 것을 사실상 REQUIRED로 간주한다.
-- 이를 포함하지 않을 경우, collector 로직 변경 시
-- 동일 경제 이벤트에 대해 event_id 불안정성이 발생할 수 있다.
-- PK

code            VARCHAR
-- 이벤트 수집 당시의 거래소 종목 코드
-- 감사 및 원본 추적 목적
-- identity 용도로 사용 금지

event_type      VARCHAR
-- Corporate action 유형
-- 예: SPLIT, REVERSE_SPLIT, BONUS, RIGHTS,
--     CASH_DIVIDEND, MERGER, SPINOFF,
--     CB_ISSUE, BW_ISSUE 등
-- Adjustment Engine / Factor Engine 분기 기준
-- 엔진 로직은 반드시 event_type 기준으로만 동작해야 함
-- EVENT TYPE CLASSIFICATION RULE:
-- 아래 이벤트만 Price Adjustment Engine에서 처리 가능
--   * SPLIT
--   * REVERSE_SPLIT
--   * BONUS
--
-- 그 외 이벤트 (RIGHTS, MERGER, SPINOFF 등)는
-- 가격 조정이 아닌 Position / Cashflow 이벤트로 처리해야 한다.

event_version   INTEGER
-- 동일 event_id에 대한 정정/업데이트 순번
-- 1부터 증가

announce_date   DATE
-- 공시 발표일 (information release date)
-- 시장 참여자가 해당 이벤트 사실을 "인지 가능"해진 최초 시점
-- Point-in-time 팩터 계산 시
-- 반드시 announce_date 이후부터만 이벤트 정보 사용 가능
-- 가격 보정에는 직접 사용하지 않음
-- look-ahead bias 방지를 위한 핵심 기준일

ex_date         DATE
-- 권리락 / 배당락 등 경제적 권리가 분리되는 기준일
-- 해당 날짜의 거래부터 권리가 제거된 상태로 거래됨
-- 대부분의 가격 갭이 발생하는 날짜
-- announce_date와 다를 수 있으며 반드시 분리 관리해야 함

effective_date  DATE
-- 가격에 실제로 adjustment factor가 적용되기 시작하는 날짜
-- Adjustment Engine이 보정계수를 누적 적용할 때 사용하는 기준일

-- EFFECTIVE DATE RULE (PRIORITY ORDER):
-- 1) source가 effective_date를 명시한 경우
--    → 해당 값을 그대로 저장
-- 2) source가 ex_date만 제공한 경우
--    → effective_date = next trading day (calendar 기준)
-- 3) 둘 다 없는 경우
--    → adjustment_status = SKIPPED_INSUFFICIENT_DATA

-- EFFECTIVE DATE PRESET (ENGINE-LEVEL)
-- effective_date 파생 사용 정책은 preset으로 명시된다.
--
-- PRESET DEFINITIONS:
-- STRICT_EXPLICIT_ONLY (DEFAULT)
--   * effective_date_source = EXPLICIT_SOURCE 인 이벤트만 사용
--
-- KRX_STANDARD_SPLIT_ONLY
--   * event_type ∈ {SPLIT, REVERSE_SPLIT, BONUS} 인 경우
--   * effective_date_source = DERIVED_NEXT_TRADING_DAY 허용
--
-- TRUST_SOURCE_EX_DATE
--   * source가 제공한 ex_date 기반 파생 effective_date 허용
--
-- 선택된 preset은 snapshot metadata에 반드시 기록되어야 한다.

-- SNAPSHOT ENFORCEMENT RULE (MANDATORY):
-- snapshot 생성 시 다음 필드는 반드시 snapshot metadata에 기록되어야 한다.
--   * effective_date_preset
--   * derived_effective_date_opt_in (BOOLEAN)
--
-- 이를 기록하지 않은 snapshot은 INVALID로 간주한다.

--
-- AUDIT NOTE:
-- snapshot metadata에 기록된 effective_date 관련 설정은
-- Adjustment Engine이 실제 적용한 규칙과 반드시 일치해야 하며,
-- 불일치 시 해당 snapshot은 재현 불가능 상태로 간주한다.

-- DERIVED DATE SAFETY RULE:
-- effective_date_source = DERIVED_NEXT_TRADING_DAY 인 경우
-- Adjustment Engine은 기본적으로 이를 사용하지 않는다.
-- 전략 또는 엔진 옵션에서 명시적으로 opt-in 해야만 적용 가능하다.
-- Adjustment Engine은 기본적으로 이를 사용하지 않는다.
-- 단, event_type ∈ {SPLIT, REVERSE_SPLIT, BONUS} 인 경우에 한해
-- engine-level option으로 일괄 opt-in을 허용할 수 있다.
-- (default = OFF)

-- 역할 구분:
--   announce_date  : 정보 인지 가능 시점 (factor / signal용)
--   ex_date        : 경제적 권리 분리일 (시장 이벤트 발생일)
--   effective_date : 가격 보정 시작 기준일 (엔진 기준)

-- 주의:
--   합병, 분할, 종목 변경 등 일부 이벤트는
--   ex_date와 effective_date가 다를 수 있음
--   따라서 effective_date는 계산하지 말고
--   수집된 값을 그대로 저장
--   effective_date는 명시적 규칙에 의해서만 계산될 수 있다.
--   암묵적 계산은 금지한다.

effective_date_source VARCHAR
-- EXPLICIT_SOURCE
-- DERIVED_NEXT_TRADING_DAY
-- UNKNOWN
-- effective_date가 추론된 값인지 여부를 명시적으로 기록
-- downstream 전략에서 opt-out 가능하도록 하기 위함

source          VARCHAR
-- 이벤트 수집 출처
-- 예: 'DART', 'KRX'

source_event_id VARCHAR
-- 원본 시스템 이벤트 ID
-- 예: DART rcept_no
-- 원본 공시 추적용

collected_at    TIMESTAMP
-- 이벤트 수집 시각
-- 재수집 / 정정 / audit 추적용

event_validation_status VARCHAR
-- VALID
-- INCOMPLETE
-- ECONOMICALLY_COMPLEX

event_usability_flags JSONB
-- downstream 사용 가능성 명시
-- {
--   "price": true,
--   "factor": true,
--   "position": false
-- }

PK: (event_id)
```

#### VERSIONING RULE

동일 source_event_id (예: DART rcept_no)에 대한
정정 공시는 새로운 row로 추가되며
event_version이 증가한다.

#### EVENT VERSION SELECTION RULE

For snapshot (snapshot_as_of_date, snapshot_time):

1. announce_date <= snapshot_as_of_date
2. collected_at <= snapshot_time
3. event_version = max(event_version)

#### DOWNSTREAM CONSUMPTION RULE

모든 View / Engine은 기본적으로
event_validation_status = 'VALID' 인 이벤트만 사용한다.
INCOMPLETE / ECONOMICALLY_COMPLEX 이벤트는
전략 또는 엔진이 명시적으로 opt-in 한 경우에만 사용 가능하다.

**NOTE:**
event_version은 시간 개념이 아니며
반드시 collected_at과 함께 사용해야 한다.

### corp_action_entities

```sql
event_id        VARCHAR
symbol_id       UUID NULL
role            VARCHAR  -- SOURCE / TARGET / NEW
entity_ref_type     VARCHAR -- SYMBOL_ID | DART_CORP_CODE | TEMP_ID
entity_ref_value    VARCHAR -- symbol_id가 없는 경우 사용
```

**PK RULE:**
symbol_id가 존재하는 경우: (event_id, symbol_id)
symbol_id가 없는 경우: (event_id, entity_ref_type, entity_ref_value)
물리 PK 구현 시 surrogate key 사용을 권장한다.

하나의 corporate action은
여러 symbol identity에 동시에 영향을 줄 수 있다.

**NOTE:**
corporate action은 symbol 생성 이전에 수집될 수 있다. 이 경우 entity_ref_*를 사용하여 연결한다.

### entity_resolution_history

symbol 생성 이전 이벤트 해소 이력

```sql
event_id            VARCHAR
entity_ref_type     VARCHAR
entity_ref_value    VARCHAR
resolved_symbol_id  UUID
resolved_at         TIMESTAMP
resolution_source   VARCHAR
-- MANUAL / RULE / BACKFILL

PK: (event_id, entity_ref_type, entity_ref_value)
```

#### RESOLUTION RULE

TEMP_ID 또는 DART_CORP_CODE 기반 이벤트는
반드시 resolution history를 통해 symbol_id로 귀속된다.

Snapshot 기준 재현 시, 해당 시점의 resolution 상태를 사용한다.
즉, resolved_at > snapshot_data_cutoff_time 인 resolution은
해당 snapshot에서 존재하지 않는 것으로 간주한다.

#### VIEW ENFORCEMENT RULE (MANDATORY)

corp_action_entities 를 symbol_id로 해소하는 모든 View는
반드시 다음 조건을 강제해야 한다:

```
entity_resolution_history.resolved_at
    <= snapshot_data_cutoff_time
```

이를 위반할 경우 look-ahead bias가 발생한다.

#### IMPLEMENTATION NOTE (STRONGLY RECOMMENDED)

entity resolution 조건

```
resolved_at <= snapshot_data_cutoff_time
```

은 모든 View에서 반복 구현하지 말고
공통 CTE / SQL macro / DB View 로 강제할 것을 권장한다.

### corp_actions 의 event type 별 세부 테이블

#### corp_action_ratio

**(A) SPLIT / BONUS / MERGE (비율형)**

```sql
event_id        VARCHAR
-- corp_actions.event_id 참조
-- event_type ∈ {SPLIT, REVERSE_SPLIT, BONUS}
-- 단순 비율 기반 주식수 / 가격 변환 이벤트

ratio_num       DECIMAL(20,10)
-- 분자 (after)
-- 예: 1:5 액면분할 → ratio_num = 5

ratio_den       DECIMAL(20,10)
-- 분모 (before)
-- 예: 1:5 액면분할 → ratio_den = 1
```

**해석 규칙**

* 신주식수 = 기존주식수 × (ratio_num / ratio_den)
* 가격 조정 = 역비율 적용
* adjustment_factor = ratio_den / ratio_num

**주의**

* FLOAT 사용 금지
* 반드시 DECIMAL 사용 (누적 곱 오차 방지)

#### corp_action_dividend

**(B) CASH DIVIDEND (현금배당)**

```sql
event_id        VARCHAR
-- corp_actions.event_id 참조
-- event_type = CASH_DIVIDEND

cash_amount     DECIMAL(20,6)
-- 1주당 현금 배당금
-- ex_date 기준 권리락 반영 금액

currency        VARCHAR
-- 통화 단위 (KRW, USD 등)

record_date     DATE
-- 배당 권리 확정일
-- 가격 보정에는 사용하지 않음

pay_date        DATE
-- 실제 현금 지급일
-- total-return 계산 시 cashflow 타이밍용
```

**가격 처리 방식 (전략 선택)**

1. Total-return (DEFAULT)
   * 가격은 조정하지 않음
   * 배당은 dividend_cashflow 로만 기록
2. Price-adjusted only (EXPLICIT OPT-IN)
   * adjustment_factor = (close_pre - cash_amount) / close_pre

**HARD DEFAULT RULE**

* Dividend는 price에 반영하지 않는다.
* Price-adjusted dividend는 전략이 명시적으로 선언한 경우에만 허용된다.

#### corp_action_rights

**(C) RIGHTS OFFERING (유상증자)**

```sql
event_id                VARCHAR
-- corp_actions.event_id 참조
-- event_type = RIGHTS

rights_ratio            DECIMAL(20,10)
-- 신주 배정 비율
-- 예: 0.2 → 5주 보유 시 1주 청약 가능

subscription_price      DECIMAL(20,6)
-- 신주 청약가

theoretical_ex_price    DECIMAL(20,6)
-- TERP (Theoretical Ex-Rights Price)

adjustment_allowed BOOLEAN DEFAULT FALSE
-- RIGHTS 이벤트는 Adjustment Engine에서 가격 보정이 기본적으로 금지된다.
```

**주의**

* 단순 split 처리 및 price-only adjustment는 절대 금지
* 현금 유입 + 지분 희석이 동시에 발생

**Adjustment 전략 (엔진 옵션화)**

1. TERP 기반 price adjustment
2. shares outstanding 증가 + cashflow 반영
3. 전략별 선택 적용

**OPTIONAL CONVENIENCE VIEWS (NON-SOURCE-OF-TRUTH)**

* rights_cashflow_estimate_view
* rights_dilution_factor_view

**NOTE:**
위 View들은 전략 편의용이며
raw ledger로부터 항상 재계산 가능해야 한다.

#### corp_action_merger

**(D) MERGER / SPINOFF (합병/분할)**

```sql
event_id        VARCHAR
-- corp_actions.event_id 참조
-- event_type ∈ {MERGER, SPINOFF}

target_symbol_id UUID
-- 합병 상대 또는 신설 법인 symbol_id
-- code 대신 identity 기준 사용

exchange_ratio  DECIMAL(20,10)
-- 교환 비율
-- 예: A 1주 → B 0.35주
```

**처리 원칙**

* 가격 adjustment factor로 처리 불가
* position transform 이벤트로 취급

**포지션 변환**

* old_shares × exchange_ratio → new_shares

**권장**

* 백테스트 엔진에서 별도 Position Transformation Engine으로 처리

#### corp_action_convertible

**(E) 전환사채(CB) / 신주인수권부사채(BW)**

```sql
event_id            VARCHAR
-- corp_actions.event_id 참조
-- event_type ∈ {CB_ISSUE, BW_ISSUE}

instrument_type     VARCHAR
-- CB | BW

conversion_price    DECIMAL(20,6)
-- 전환가

strike_price        DECIMAL(20,6)
-- BW 행사가 (CB는 NULL)

maturity_date       DATE
```

**주의**

* 발행 자체는 즉시 희석 아님
* 실제 전환 이벤트 발생 시 별도 corp_action으로 기록

**활용**

1. Fully-diluted shares 팩터 계산
2. 실제 전환 시점에 adjustment 적용

### symbols

```sql
symbol_id       UUID
-- 시스템 내부 상장 거래 대상 identity (Listed Security)
-- 가격, 거래, 조정은 모두 이 identity 기준
-- 법인 단위 identity와 분리 관리됨
symbol_id = hash(
    DART_corp_code +
    first_list_date +
    legal_entity_type
)

current_code    VARCHAR
-- 현재 KRX 코드 (편의용)

name            VARCHAR
-- 현재 종목명 (스냅샷)

list_date       DATE
-- 최초 상장일

delist_date     DATE
-- 상장폐지일 (NULL = 현재 상장)

PK: (symbol_id)
```

**NOTE:**
하나의 법인(Legal Entity)은 시간에 따라
여러 개의 symbols(symbol_id)를 가질 수 있다.
(합병, 분할, 재상장 등)

### symbol_code_history

```sql
symbol_id       UUID
-- symbols.symbol_id FK

code            VARCHAR
-- KRX 종목 코드

start_date      DATE
-- 코드 유효 시작일 (inclusive)

end_date        DATE
-- 종료일 (exclusive, NULL = 현재)

source          VARCHAR
-- KRX / DART

collected_at    TIMESTAMP

PK: (symbol_id, start_date)
```

### symbol_market_state

```sql
symbol_id       UUID
-- 종목 identity 기준

market          VARCHAR
-- KOSPI / KOSDAQ

announce_date   DATE
-- 시장 변경 공시일 (정보 인지 시점)

effective_date  DATE
-- 실제 거래소 반영일

source          VARCHAR
-- KRX

collected_at    TIMESTAMP

PK: (symbol_id, effective_date)
```

### symbol_trading_state

```sql
symbol_id       UUID
-- symbols.symbol_id FK

date            DATE
-- 거래일

is_tradable     BOOLEAN
-- 정상 거래 여부

reason          VARCHAR
-- HALT / SUSPENSION / etc.

source          VARCHAR
-- KRX

collected_at    TIMESTAMP

PK: (symbol_id, date)
```

### trading_calendar

```sql
date        DATE
-- KST 기준 달력 날짜

market      VARCHAR
-- KOSPI / KOSDAQ
-- NOTE:
-- 현재는 KOSPI / KOSDAQ 만을 기본 대상으로 하나,
-- ETF / ETN / KONEX 등으로의 확장을 고려하여
-- enum이 아닌 VARCHAR로 유지한다

is_open     BOOLEAN
-- 실제 거래일 여부

PK: (date, market)
```

## COLLECTOR LAYER

### 공통 인터페이스

```python
class Collector:
    def collect(self, start_date: date, end_date: date) -> list[dict]
```

### KRX PRICE COLLECTOR

**책임**

* 거래일 기준 일별 시세 수집

**동작**

* trading_calendar에서 거래일 조회
* repository.get_symbol_list()
  * 이미 수집된 마지막 날짜 이후부터 실행
* 수집 단위는 (symbol_id, trading_date)
* code는 symbol_code_history를 통해 조회
* 실패 시 retry, 실패 로그 기록

### KRX SYMBOL COLLECTOR

* 신규 상장
* 상폐
* 종목명 변경
* 시장 이동

→ symbols 테이블 업데이트

### DART EVENT COLLECTOR

**수집 대상**

* 액면분할 / 병합
* 무상증자 / 유상증자
* 배당
* 합병 / 분할
* CB / BW

**중요 원칙**

* announce_date ≠ ex_date
* 공시 사실과 가격 효력 분리

## NORMALIZATION

### price_normalizer

* 숫자 타입 변환
* zero padding
* 날짜 변환
* 결측값 정리

### event_normalizer

* 비율 통일
* 이벤트 타입 정규화
* event_id 생성
  * hash(source_event_id (DART rcept_no) OR (source + code + announce_date + event_type + sequence))

## VALIDATION

Validator는 reject가 아니라 classify를 수행한다.
오류 이벤트는 flag 처리하고 ledger에는 저장한다.

### price_validator

* high ≥ max(open, close, low)
* low ≤ min(open, close, high)
* volume ≥ 0
* PK 중복 금지

### event_validator

* ex_date 필수
* announce_date > ex_date → warning
* event_type 기반으로 분기
* event_validation_status:
  * VALID
  * INCOMPLETE
  * ECONOMICALLY_COMPLEX
* event_usability_flags:
  * usable_for_price BOOLEAN
  * usable_for_factor BOOLEAN
  * usable_for_position BOOLEAN

**NOTE:**
validation_status는 이벤트의 경제적 복잡도를 표현하며,
실제 downstream 사용 가능 여부는 usability_flags로 판단한다.

### symbol state

* announce_date ≤ effective_date

### DOWNSTREAM RULE

* 모든 Adjustment / Factor / Strategy View는 기본적으로 validation_status = 'VALID'만 사용한다.
* INCOMPLETE / ECONOMICALLY_COMPLEX 이벤트는 전략이 명시적으로 opt-in하지 않는 한 제외된다.

## STORAGE (IDEMPOTENCY CORE)

* Raw ledger (prices_raw, corp_actions)는 append-only
  * upsert 금지
  * 동일 logical key라도 새로운 row 추가
* Dimension / State table (symbols, code_history 등)은 upsert 허용
* View 레이어에서 최신 상태를 결정한다.
* collected_at 반드시 기록

## CORPORATE ACTION APPLICATION (ADJUSTMENT ENGINE)

### 기본 원칙

**backward adjustment**

* ex_date + 1일부터 적용
* 이벤트는 누적 곱으로 적용
  * adjusted_price(date) = raw_price(date) * Π adjustment_factor(code, t > date)

**DERIVED EFFECTIVE DATE SAFETY CHECK (RECOMMENDED):**

effective_date_source = DERIVED_NEXT_TRADING_DAY 인 이벤트가 존재하나
snapshot metadata에서 derived_effective_date_opt_in = false 인 경우
Adjustment Engine은 반드시 다음을 수행해야 한다.

* adjustment_status = SKIPPED_INSUFFICIENT_DATA
* adjustment_skip_reason = 'DERIVED_EFFECTIVE_DATE_NOT_OPTED_IN'
* warning log 기록

### event_type 별 규칙

```python
if event.type in RATIO_EVENTS:
    apply_ratio(event)
elif event.type == CASH_DIVIDEND:
    apply_dividend(event)
```

**유상증자**

* 단순 분할 아님
* 별도 adjustment factor 저장

**전략별 선택 적용**

**배당**

* 가격 보정용 / 팩터용 분리

### Adjustment Engine 필수 조건

Adjustment Engine은 다음 조건을 반드시 만족해야 한다:

* price × factor 형태의 순수 곱셈만 수행
* 주식 수, 종목 변경, 현금 흐름을 직접 처리하지 않음
* Adjustment Engine은 처리 불가능 이벤트를 reject하지 않는다.
* 대신 adjustment_skip_reason을 기록한다.
  * adjustment_status:
    * APPLIED
    * SKIPPED_REQUIRES_POSITION_ENGINE
    * SKIPPED_INSUFFICIENT_DATA
* 모든 corp_action 이벤트는 adjustment 결과가 존재하지 않더라도 반드시 adjustment evaluation 기록을 남겨야 한다.
  * APPLIED / SKIPPED 포함

### adjustment_evaluation_log (REQUIRED)

```sql
symbol_id
event_id
snapshot_id
adjustment_status
adjustment_skip_reason
adjustment_engine_version
evaluated_at
```

**LOG RETENTION POLICY (RECOMMENDED):**

adjustment_evaluation_log 는 snapshot 단위로 materialize 될 수 있다.

**RETENTION:**

* full event-level log : 90 days
* snapshot-level summary : indefinite

**NOTE:**
retention 정책은 auditability를 해치지 않는 범위에서
운영 성능을 위해 조정 가능하다.

### HARD RULE

* event_type = RIGHTS, MERGER, SPINOFF
* → Adjustment Engine은 무조건 SKIP

## ADJUSTMENT FACTOR TABLE (EVENT-LEVEL)

```sql
symbol_id
event_id
effective_date
adjustment_factor
```

**PRECISION RULE (MANDATORY):**

adjustment_factor는 반드시 DECIMAL 타입으로 저장해야 하며
최소 권장 정밀도는 DECIMAL(38, 18)이다.

누적 곱 계산 과정에서는 rounding을 수행하지 않으며
최종 adjusted_price 산출 단계에서만 rounding policy를 적용한다.
rounding policy는 snapshot metadata에 명시되어야 한다.

**ROUNDING POLICY RULE:**

rounding은 View 레이어의 책임이며
Raw / Event / Adjustment Factor 테이블에는
절대 rounding 된 값이 저장되어서는 안 된다.

```sql
adjustment_status
- APPLIED
- SKIPPED_REQUIRES_POSITION_ENGINE
- SKIPPED_INSUFFICIENT_DATA

adjustment_skip_reason
```

**NOTE:**

누적 adjustment factor는 View 레이어에서 다음 규칙으로 계산한다.
adjustment applies to all price dates strictly BEFORE effective_date

```
cumulative_factor(price_date) =
    Π(adjustment_factor
      WHERE event.effective_date > price_date)
```

effective_date 당일 가격에는 해당 이벤트를 적용하지 않는다.

**PERFORMANCE NOTE:**

누적 adjustment factor 계산 비용 절감을 위해
daily cumulative adjustment materialized view를 둘 수 있다.
단, source of truth는 항상 event-level factor이다.

## POINT-IN-TIME FACTOR SYSTEM

### 기본 원칙

시스템 내 가격(price)은 단 하나만 존재한다.

* 배당 및 현금 효과는 price에 반영하지 않는다.
* 총수익(total return)은 price + cashflow + position 변화로 계산한다.

### factor_snapshot

```sql
date
symbol_id
code
factor_name
factor_value
computed_at
computed_with_version
```

**RULE:**

factor identity는 symbol_id 기준으로 귀속된다.
code는 snapshot 시점의 식별자 보존 및 디버깅 용도로만 사용한다.

**PERFORMANCE NOTE (RECOMMENDED):**

대규모 PIT as-of join 비용을 줄이기 위해
snapshot 생성 시 factor_snapshot을 snapshot_id 기준으로
materialize 하는 것을 권장한다.

backtest / strategy layer는
raw factor source가 아닌 snapshot materialized factor만 소비해야 한다.

### factor_metadata

```sql
factor_name
source
frequency
lag_days
```

### 규칙

* 공시 기반 팩터는 lag 적용
* 미래 정보 접근 불가
* 모든 팩터는 단일 시간 기준으로 환원하지 않는다.
  * 공시 기반 팩터: announce_date 기준
  * 회계 기반 팩터: 회계 기간 종료 + lag
  * 시장 반영 팩터: market effective date 기준

## PRICE & FACTOR VIEWS (BACKTEST INTERFACE)

### Snapshot Metadata (Reproducibility Contract)

#### view_snapshot_metadata

백테스트 실행 시점의 논리적 데이터 상태를 고정(freeze) 하기 위한 메타데이터.

**스키마**

```sql
snapshot_id                  -- immutable snapshot identifier
snapshot_as_of_date          -- point-in-time 기준 날짜
snapshot_data_cutoff_time    -- 해당 snapshot에서 사용 가능한 최대 collected_at 시각
                             -- DART 정정공시 재현성을 보장하기 위한 hard boundary
price_view_version           -- price view 로직 버전
factor_view_version          -- factor view 로직 버전
adjustment_engine_version    -- adjustment engine 로직 버전
created_at                   -- snapshot 생성 시각
```

**VERSIONING RECOMMENDATION:**

\*_version 필드는 단순 문자열이 아닌
git commit hash 또는 deterministic build hash 사용을 권장한다.

#### Snapshot Guarantee

**SNAPSHOT IMMUTABILITY GUARANTEE:**

동일 snapshot_id는 다음이 완전히 동일함을 의미한다.

* source snapshot time
* adjustment engine version
* price view version
* factor view version

데이터 재수집 여부와 무관하게
동일 snapshot_id를 사용한 백테스트 결과는
반드시 완전히 재현 가능해야 한다.

### Price Views (Read-only, Deterministic)

#### raw_price_view

**목적:**

* 원본 가격 데이터 검증
* adjustment 전/후 비교

**특징:**

* prices_raw 최신 유효 revision만 노출
* corporate action 미반영

#### adjusted_price_view

**목적:**

* corporate action이 반영된 price 제공

**적용 규칙:**

* backward adjustment
* event-level adjustment_factor의 누적 곱
* price × factor 형태의 순수 곱셈만 수행

**포함 이벤트:**

* SPLIT
* REVERSE_SPLIT
* BONUS

**제외 이벤트:**

* RIGHTS
* MERGER
* SPINOFF

#### Strategy Safety Default

**STRATEGY SAFETY DEFAULT:**

effective_date_source != EXPLICIT_SOURCE 인 이벤트는
adjusted_price_view에서 기본적으로 제외된다.

전략 또는 엔진 옵션에서 명시적으로 opt-in 해야만
해당 이벤트를 price에 반영할 수 있다.

### Strategy Interface View (Explicit Consumption Only)

#### strategy_price_view

**원칙:**

* price view는 항상 단 하나만 존재한다.
* 전략은 price에 암묵적으로 cashflow나 position 효과를
  포함해서는 안 된다.

**전략 레이어는 반드시 다음을 명시적으로 소비해야 한다.**

* adjusted_price
* dividend_cashflow
* position_transform

**total return은 전략 레이어에서만 계산된다.**

#### STRATEGY SAFETY CHECKS (RECOMMENDED)

전략 레이어는 다음 조건을 만족하지 않을 경우
warning 또는 error를 발생시켜야 한다.

1. dividend_cashflow 가 존재하나 소비되지 않은 경우
2. position_transform 이벤트가 존재하나 소비되지 않은 경우

**목적:**

암묵적 total-return 계산 및 전략 오류 방지

### Adjustment Convenience View (Performance Only)

#### daily_cumulative_adjustment_view

**목적:**

* adjustment factor 누적 곱 계산 비용 절감
* 대규모 백테스트 성능 최적화

**주의:**

* source of truth 아님
* event-level adjustment factor로부터
  항상 재계산 가능해야 함

**스키마**

```sql
symbol_id
date
cumulative_adjustment_factor
snapshot_id
computed_at
```

### Layer Responsibility Summary

**Raw Ledger:**

```
prices_raw
corp_actions
corp_action_* tables
    → immutable, append-only, source of truth
```

**View Layer:**

```
raw_price_view
adjusted_price_view
daily_cumulative_adjustment_view
    → deterministic, rebuildable, read-only
```

**Strategy Layer:**

```
strategy_price_view
dividend_cashflow
position_transform
    → explicit composition only
```

## INCREMENTAL UPDATE LOGIC

```python
last_date = repository.get_last_price_date()
start = last_date + 1
end = today
```

event collector는 정정 공시를 고려하여

* 최근 N년
* 또는 정정 목록 기반 재수집을 수행한다.

## FULL REBUILD GUARANTEE

모든 테이블 truncate 후 동일 source snapshot 시점 기준으로 전체 기간 재수집해도 결과 완전 동일

**NOTE**

외부 API(KRX, DART)의 과거 데이터 추가/정정은 snapshot 시점이 달라질 경우 결과 차이를 유발할 수 있다.

## FAILURE HANDLING

* 날짜 단위 재시도
* 부분 성공 허용
* 다음 실행 시 이어서 진행

## REAL-WORLD DATA FAILURE MODES

* 거래정지 후 재개
* 액면분할 + 무상증자 동시 발생
* 유상증자 + 합병 연계
* DART 정정공시 지연

**이 경우:**

* Raw/Event ledger는 항상 보존
* Adjustment Engine은 부분 적용
* Strategy layer는 명시적 선택

## LOGGING & AUDIT

* 수집 건수
* 실패 건수
* retry 횟수
* API 응답 없음
* 이벤트 적용 로그

## OUTPUT GUARANTEE

이 파이프라인 종료 시 보장:

* raw 가격 완전 보존
* corporate action 완전 원장
* point-in-time 팩터 재현 가능
* 수정주가 100% 재계산 가능
* 전략 로직은 데이터 신뢰 가능

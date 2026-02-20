# TDD TODO Checklist (Phase 1 + Bulk Export)

Last updated: 2026-02-20

## 0. Test Harness
- [x] `pytest` base setup (`tests/`, shared fixtures)
- [x] isolated PostgreSQL test DB
- [x] schema/DDL fixture setup (SQLite for local tests)
- [x] time-freeze fixture (`freezegun`)

## 1. Schema Contract Tests
- [x] `instruments` PK/UK/check constraints
- [x] `daily_market_data` OHLC/non-negative constraints
- [x] `benchmark_index_data` OHLC constraint explicit test
- [x] `collection_runs` status constraint
- [x] `data_quality_issues` severity constraint explicit test
- [x] `*_v1` views existence test

## 2. KRX Client (Unit)
- [x] `AUTH_KEY` header test
- [x] success response parse test
- [x] error/empty response handling test
- [x] retry(backoff) behavior test
- [x] daily call limit guard test

## 3. Collector: Instruments
- [x] invalid required fields handling test
- [x] date normalization failure -> issue record test
- [x] upsert + market code normalization
- [x] mixed new/existing input success test

## 4. Collector: Daily Market
- [x] duplicate key path covered by upsert test flow
- [x] invalid OHLC/volume -> issue record test (collector-level)
- [x] load with `record_status`
- [x] valid rows saved as `VALID`

## 5. Collector: Benchmark
- [x] unmapped index code failure test
- [x] missing day -> issue record test
- [x] benchmark upsert implementation
- [x] period load success test

## 6. Trading Calendar Builder
- [x] duplicate date protection via PK/upsert path
- [x] generate `is_open` from index trade days
- [x] generate closed days for missing days
- [x] continuous date range generation test

## 7. Validation Job
- [x] open-day total missing detection test
- [x] negative/logical error issue generation implementation
- [x] validation report output
- [x] no-issue on fully valid range explicit test

## 8. Collection Run Lifecycle
- [x] exception -> `FAILED` path
- [x] start with `RUNNING`
- [x] finish with `SUCCESS/PARTIAL/FAILED`
- [x] success/failure/warning count consistency test

## 9. Bulk Export API (Contract)
- [x] `date_from > date_to` failure test
- [x] required parameter missing -> 400
- [x] `POST /api/v1/backtest/exports` -> 202
- [x] `GET /api/v1/backtest/exports/{job_id}` status
- [x] `GET /api/v1/backtest/exports/{job_id}/manifest`

## 10. Export Worker
- [x] query/export failure -> job `FAILED`
- [x] `core/benchmark/calendar/issues` query execution
- [x] 3-4 output files generation logic
- [x] temp path -> final path atomic move
- [x] file/row-count/manifest assertions in tests

## 11. Data Contract for Backtest Repo
- [x] parquet schema snapshot test (`instrument_daily.parquet`)
- [x] parquet schema snapshot test (`benchmark_daily.parquet`)
- [x] parquet schema snapshot test (`trading_calendar.parquet`)
- [x] `manifest.json` required keys test path

## 12. End-to-End Test
- [x] Given: sample period + instrument/index sample data
- [x] When: full pipeline run
- [x] Then: backtest input dataset creation path verified
- [x] Then: issue tracking path verified

## 13. Non-Functional
- [x] performance test for 1Y KOSDAQ export
- [x] backward compatibility regression test for `*_v1`
- [x] mid-run failure recovery integrity test

## 14. Definition of Done (Phase 1)
- [x] all Phase 1 contract tests complete (including PostgreSQL)
- [x] real KRX integration for 3 APIs + validation + issue logging
- [x] bulk export API contract tests complete
- [x] backtest-repo sample run with produced parquet complete

## 15. Integration Readiness
- [x] `.env` + `.env.example` added
- [x] preflight env validation added
- [x] real KRX integration tests added (`pytest -m integration`)
- [x] fill `KRX_AUTH_KEY` and run integration tests successfully

## Remaining
- [x] 없음

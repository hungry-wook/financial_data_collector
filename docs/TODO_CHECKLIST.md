# TODO Checklist

This document summarizes the prioritized action items from the full codebase review.

Baseline:
- Test status: `82 passed, 1 skipped` (`uv run pytest -q`)
- Goal: improve backtest/signal reliability and production stability

## P0 (Immediate)

- [x] Fix HTTP status code handling in `POST /api/v1/backtest/exports`
  - Current behavior ignores `status_code` from `api.post_exports()` and returns body only
  - Reference: `src/financial_data_collector/server.py`
- [x] Replace in-memory export job state with DB persistence
  - Current behavior uses in-memory `self.jobs`
  - Reference: `src/financial_data_collector/export_service.py`
- [x] Apply hard filter for signal-generation API
  - Default filters: `record_status='VALID'`, `is_trade_halted=false`, `is_under_supervision=false`
  - Optional per strategy: `volume > 0`
  - Implemented in repository query contract: `Repository.get_signal_market(..., require_positive_volume=...)`
  - Reference: `sql/platform_schema.sql`

## P1 (Current Sprint)

- [x] Change date validation from string comparison to parsed date comparison
  - Reference: `src/financial_data_collector/export_service.py`
- [x] Prevent benchmark series mixing (`series_name` required or fixed default)
  - Reference: `src/financial_data_collector/repository.py`
- [ ] Separate signal vs backtest data contract
  - Signal calculation: adjusted series
  - Execution/risk checks: raw series
- [ ] Implement adjusted price pipeline
  - Build adjustment factors and adjusted OHLC from `corporate_events`
  - Reference: `sql/platform_schema.sql`
- [ ] Add as-of contract to prevent look-ahead bias
  - Add `as_of_timestamp` query boundary and cutoff logic
- [ ] Improve export job operational stability
  - Define state machine, retry policy, and failure-recovery behavior

## P2 (Next Sprint)

- [ ] Add instrument lifecycle history model (SCD2) for survivorship-bias control
- [ ] Add export `idempotency_key`
- [ ] Add HTTP-level E2E tests (`FastAPI TestClient + Postgres`)
- [ ] Update docs for new contracts
  - Target docs: `BULK_EXPORT_API_SPEC.md`, `BACKTEST_DATA_INTERFACE.md`, `SCHEMA.md`
- [ ] Split CI jobs by scope
  - unit / integration / e2e / performance

## Operations and Cleanup

- [ ] Enforce cleanup policy for generated artifacts
  - Ensure `__pycache__` and `*.egg-info` are excluded from repo/deploy outputs
- [ ] Define checklist completion rules
  - Each completed item must include related test updates and doc updates

## Recommended Execution Order

1. Complete all P0 items.
2. In P1, do date validation + benchmark series + as-of contract first.
3. Then implement adjusted price pipeline and signal/backtest contract split.
4. Execute P2 items in sequence.

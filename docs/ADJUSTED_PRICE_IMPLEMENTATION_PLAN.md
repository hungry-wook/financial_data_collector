# Adjusted Price Implementation Plan (Ex-Dividend)

## 1. Goal
- Build an adjusted OHLC pipeline for momentum swing strategy evaluation.
- Scope excludes cash dividend adjustment in the first release.
- Keep `raw` and `adjusted` series separated to avoid hidden behavior changes.

## 2. Data Source Strategy
- Primary event source: OpenDART `DS005` (major event APIs).
- Support source for discovery/verification: OpenDART `DS001`.
- Why both:
  - `DS005` provides normalized event fields (ratio/date).
  - `DS001` provides filing search, final amendment filtering, and document access by `rcept_no`.

## 3. Coverage and Limits
- Covered (target): bonus issue, rights issue, capital reduction, merger, split, stock swap/transfer.
- Not covered in v1: cash dividend total-return adjustment.
- Known API constraints:
  - DS005 coverage starts from 2015.
  - DS005 is queried by `corp_code + period` (no direct `rcept_no` lookup).
  - DS001 list search without `corp_code` is limited to 3 months.

## 4. Data Model Changes
1. `corporate_events` usage hardening
- Required fields: `instrument_id`, `event_type`, `announce_date`, `effective_date`, `source_event_id`, `payload`.
- Add normalized factor fields:
  - `raw_factor NUMERIC(18,10) NOT NULL`
  - `confidence VARCHAR(20) NOT NULL` (`HIGH`, `MEDIUM`, `LOW`)
  - `status VARCHAR(20) NOT NULL` (`ACTIVE`, `NEEDS_REVIEW`, `REJECTED`)

2. New table `event_validation_results`
- `validation_id`, `source_event_id`, `check_name`, `result`, `detail`, `validated_at`.
- Used for DS005-vs-document consistency tracking.

3. New table `price_adjustment_factors`
- `instrument_id`, `trade_date`, `factor`, `factor_source`, `confidence`, `created_at`.
- Daily factor materialization for deterministic export/query.

4. New view `core_market_dataset_v2`
- Includes both:
  - raw: `open/high/low/close`
  - adjusted: `adj_open/adj_high/adj_low/adj_close`
- Keep `core_market_dataset_v1` untouched for backward compatibility.

## 5. Pipeline Design
1. Discovery (`DS001`)
- Pull filings with major report categories.
- Store: `rcept_no`, `rcept_dt`, `corp_code`, `report_nm`, `last_reprt_at`.

2. Event extraction (`DS005`)
- Map filing type to DS005 endpoint.
- Normalize fields into a canonical event format.

3. Verification (`DS001` document)
- Download filing document by `rcept_no`.
- Compare key fields (ratio/effective date/base date).
- Mark mismatches as `NEEDS_REVIEW`.

4. Factor build
- Convert normalized events into per-event factors.
- Resolve same-day multi-event collisions by deterministic priority and multiplication.
- Materialize daily factors and cumulative factors.

5. Serving
- Signal input uses adjusted series.
- Execution/risk checks use raw series.
- Export supports explicit series selection (`raw`, `adjusted`, or both).

## 6. As-Of Contract (Look-Ahead Control)
- Add `as_of_timestamp` at query/export boundaries.
- Apply only events where `announce_date <= as_of_timestamp`.
- Persist run metadata with as-of value for reproducibility.

## 7. Validation and Release Gate
- Unit tests:
  - factor calculation by event type
  - same-day multi-event composition
- Integration tests:
  - DS001 -> DS005 -> validation -> factor materialization
- Backtest regression:
  - compare `raw` vs `adjusted` strategy outputs
- Release gate:
  - no critical mismatch in gold cases
  - event parsing success rate threshold met

## 8. Rollout Plan
1. Phase A: schema + staging ingestion + event normalization
2. Phase B: document verification + review queue
3. Phase C: factor materialization + `core_market_dataset_v2`
4. Phase D: API/export integration with `as_of_timestamp`
5. Phase E: strategy regression and production cutover

## 9. Operational Rules
- Keep immutable event history (`event_version`) for amendments.
- Never overwrite historical factor rows without run trace.
- Rebuild factors for a bounded window when amendments are detected.

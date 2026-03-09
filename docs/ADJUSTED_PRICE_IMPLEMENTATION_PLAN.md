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

## 3A. Event Modeling Corrections
- `effective_date` must not default to `rcept_dt` for all event types.
- `announce_date` and `effective_date` have different semantics:
  - `announce_date`: filing receipt date (`rcept_dt`)
  - `effective_date`: actual market adjustment date inferred from DS005/document fields
- `RIGHTS_ISSUE` must be split into subtypes:
  - `RIGHTS_ISSUE_SHAREHOLDER`
  - `RIGHTS_ISSUE_PUBLIC`
  - `RIGHTS_ISSUE_THIRD_PARTY`
- `RIGHTS_BONUS_ISSUE` must preserve the paid/unpaid split in payload so factor timing can be derived per leg.

## 3B. Effective Date Rules
1. `BONUS_ISSUE`
- Preferred date: rights ex-date / new-share listing date from DS005 or document.
- Fallback: filing review queue (`NEEDS_REVIEW`), not `rcept_dt`.

2. `CAPITAL_REDUCTION`
- Preferred date: new-share listing date / effective date stated in DS005 or document.
- Example DS005 field already observed in production: `crsc_nstklstprd`.
- `rcept_dt` must not be used as effective date.

3. `SPLIT` / `SPLIT_MERGER`
- Preferred date: new-share listing date or split effective date.
- Fallback only if explicit ratio and listing date are both unavailable.

4. `MERGER` / `STOCK_SWAP` / `STOCK_TRANSFER`
- Preferred date: share delivery / listing / merger effective date from DS005 or document.
- If only structure/no-new-share text is available, keep `factor=1.0` only when the document explicitly states that no new shares are issued.

5. `RIGHTS_ISSUE_SHAREHOLDER`
- Preferred date: rights ex-date / listing date of new shares.
- Use document/DS005 dates, never `rcept_dt`.

6. `RIGHTS_ISSUE_PUBLIC`
- Preferred date: listing date of newly issued shares.
- Public offering without shareholder rights should not reuse shareholder-rights timing assumptions.

7. `RIGHTS_ISSUE_THIRD_PARTY`
- Default policy: do not auto-activate on `rcept_dt`.
- Preferred date: listing date of newly issued shares.
- If listing/effective date is missing, keep `NEEDS_REVIEW`.

## 3D. Implementation Targets
1. `collect_dart_corporate_events.py`
- `_map_event_type(report_nm)`:
  - split `RIGHTS_ISSUE` into shareholder/public/third-party subtype candidates
  - keep coarse filing-name mapping only as a first-pass classifier
- `_extract_ds005_row(...)`:
  - preserve raw DS005 payload fields needed for subtype/date derivation
- `_infer_ds005_factor(event_type, ds005_row)`:
  - compute factor only
  - do not embed effective-date assumptions here
- `_filing_effective_date(filing)`:
  - replace with event-specific derivation helper
  - only return a date when DS005/document fields explicitly support it
- event upsert path:
  - persist both `announce_date=rcept_dt` and separately derived `effective_date`
  - persist subtype/date evidence in `payload`

2. `dart_event_parser.py`
- add parsers for:
  - listing date
  - merger/swap effective date
  - split effective date
  - rights-issue subtype hints
- keep factor extraction and date extraction separated
- preserve rule names so validation reports can identify whether a miss was a factor-rule issue or a date-rule issue

3. `adjustment_service.py`
- arithmetic stays unchanged unless same-day multi-event ordering is proven wrong
- upstream event timing/modeling is the current defect, not cumulative-factor math

4. Validation harness
- external comparison must use:
  - local adjusted close on the locally derived `effective_date`
  - vendor adjusted close on the same trade-date window
- every miss must be labeled:
  - wrong factor
  - wrong effective date
  - vendor mismatch
  - missing price overlap

## 3C. Validation Standard
- External adjusted-price validation must be evaluated against the event's true market-effective date window.
- A comparison failure where Yahoo/other vendor shows no adjustment before listing date is evidence of date-model error, not factor error.
- Validation report must label each miss as one of:
  - `factor_error`
  - `effective_date_error`
  - `vendor_mismatch`
  - `insufficient_price_history`

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
  - effective-date derivation by event subtype
- Integration tests:
  - DS001 -> DS005 -> validation -> factor materialization
- Backtest regression:
  - compare `raw` vs `adjusted` strategy outputs
- Release gate:
  - no critical mismatch in gold cases
  - event parsing success rate threshold met
  - external adjusted-price validation passes for:
    - `>= 99%` within `2%`
    - `>= 95%` within `0.1%` after excluding labeled `vendor_mismatch`

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

# Adjusted Price Remaining Plan

## Goal
- Close the remaining gaps in the adjusted-price pipeline for momentum swing backtests.
- Keep the current `ACTIVE` event set conservative and correct.
- Expand high-confidence coverage without reintroducing premature or mispriced adjustments.

## Current Baseline
- Raw KRX price collection is running successfully on local Postgres.
- DART event ingestion, validation storage, factor materialization, adjusted export/query, and repair flow are already implemented.
- `ACTIVE` events are now gated conservatively; unresolved cases are held in `NEEDS_REVIEW`.

## Remaining Work

### 1. Rights-Issue Pricing Model
- Problem: many `RIGHTS_ISSUE` and `RIGHTS_BONUS_ISSUE` cases still need review because share-count-only factors are not sufficient.
- Work:
  - Implement price-aware TERP-style factor calculation when DS005 pricing inputs are available.
  - Distinguish shareholder allotment, public offering, and third-party allotment in pricing rules.
  - Persist the exact pricing inputs used for later validation.
- Done when:
  - document-only constant-factor rights issues are no longer auto-activated
  - DS005-priced rights issues can be promoted from `NEEDS_REVIEW` to `ACTIVE` with reproducible evidence

### 2. Effective-Date Modeling
- Problem: legal/event dates and vendor adjustment dates do not always match, especially for capital reductions and issuance events.
- Work:
  - separate `legal_effective_date` from actual adjustment-apply date if required by validation results
  - codify date rules per event subtype
  - store date evidence from DS005/document parsing in payload for auditability
- Done when:
  - remaining validation misses can be explained as either date-policy choice or vendor mismatch

### 3. DART Recollection and Review Pool Reduction
- Problem: current review pool is limited by OpenDART quota and missing DS005/document evidence.
- Work:
  - rerun DART collection after quota reset
  - reprocess `NEEDS_REVIEW` events with newly available DS005/document payloads
  - shrink the review pool without weakening activation rules
- Done when:
  - the recent-window `NEEDS_REVIEW` count materially drops through new evidence, not fallback assumptions

### 4. External Validation Harness Upgrade
- Problem: validation exists, but the acceptance target for broad samples is not yet closed.
- Work:
  - rerun broad-sample validation after KRX+DART refresh
  - keep failure taxonomy explicit: `factor_error`, `effective_date_error`, `vendor_mismatch`, `insufficient_price_history`
  - store reproducible reports for sampled windows
- Done when:
  - broad-sample validation is stable and failures are explainable by taxonomy, not unknown behavior

### 5. KRX Collection Reliability Hardening
- Problem: the main connection-thrash issue was fixed, but the collector still needs operational hardening.
- Work:
  - monitor for recurrence of transient DB connection exhaustion
  - add collection-run diagnostics if failures recur
  - keep CLI behavior aligned with `.env`/runtime expectations
- Done when:
  - repeated monthly-window runs complete without manual intervention

## Release Criteria
- No unresolved `ACTIVE` events with unexplained external validation misses in the target window.
- Rights-issue pricing is evidence-based, not constant-factor approximation where price inputs are required.
- Remaining `NEEDS_REVIEW` cases are explainable by missing source evidence, not parser gaps already solved.

# Adjusted Price Backlog

This backlog is the execution breakdown for adjusted OHLC implementation (ex-dividend).

## P0 - Foundation (must-have)

1. Schema migration for adjustment pipeline
- Add fields to `corporate_events`: `raw_factor`, `confidence`, `status`.
- Add tables: `event_validation_results`, `price_adjustment_factors`.
- Add indexes on `(instrument_id, effective_date)` and `(instrument_id, trade_date)`.
- DoD: migration applied on local Postgres and schema tests pass.

2. Repository contract extensions
- Add repository methods for:
  - upsert/list corporate events
  - insert validation results
  - upsert/list daily adjustment factors
- DoD: repository unit tests cover insert/update/query paths.

3. Event normalization model
- Define canonical `event_type` enum and required fields per type.
- Map DS005 responses to canonical model.
- DoD: fixture-based tests for all supported event types.

4. As-of query contract
- Add `as_of_timestamp` parameter in data access path.
- Ensure only `announce_date <= as_of` events are applied.
- DoD: regression test proves no look-ahead behavior.

## P1 - Ingestion and Verification

5. DS001 filing discovery collector
- Collect `rcept_no`, `rcept_dt`, `corp_code`, `report_nm`, `last_reprt_at`.
- Keep amendment history while marking final filings.
- DoD: idempotent re-run with no duplicate active rows.

6. DS005 event collectors
- Implement per-endpoint fetchers for major event APIs.
- Rate-limit and retry policy with exponential backoff.
- DoD: 1-month backfill job succeeds for KOSPI/KOSDAQ sample universe.

7. DS001 document verification worker
- Download document ZIP by `rcept_no`.
- Parse core fields and compare against DS005 normalized values.
- DoD: mismatch rows persisted with reason codes.

8. Review queue handling
- Mark `NEEDS_REVIEW` and exclude from factor build by default.
- Add manual override path (`ACTIVE`/`REJECTED`).
- DoD: override flow tested end-to-end.

9. Effective-date derivation engine
- Replace `effective_date = rcept_dt` fallback with event-specific rules.
- Parse DS005/document fields for:
  - listing date
  - merger effective date
  - split effective date
  - capital reduction new-share listing date
- DoD: known mismatch samples reclassified as `effective_date_error` or corrected to external vendor timing.

10. Rights-issue subtype model
- Split `RIGHTS_ISSUE` into shareholder/public/third-party issuance.
- Preserve subtype in canonical event payload and DB event_type.
- Default `RIGHTS_ISSUE_THIRD_PARTY` to review unless listing/effective date is explicit.
- DoD: recent third-party allotment samples no longer auto-adjust on filing date.

## P2 - Factor Build and Serving

11. Factor engine
- Calculate per-event factor and same-day composition.
- Materialize daily factors and cumulative factors.
- DoD: gold-case tests for split/merge/rights/merger scenarios.

12. Dataset v2 view
- Create `core_market_dataset_v2` with raw + adjusted OHLC.
- Keep v1 immutable.
- DoD: view contract documented and tested.

13. Export and API integration
- Add series selector (`raw`, `adjusted`, `both`) and `as_of_timestamp`.
- Keep backward compatibility defaults.
- DoD: API and export tests updated and green.

14. Strategy input split
- Signal path uses adjusted series.
- Execution/risk path uses raw series.
- DoD: sample strategy run verifies split path behavior.

## P3 - Reliability and Rollout

15. External validation harness
- Compare local adjusted series to vendor adjusted close on sampled instruments.
- Label failures as:
  - `factor_error`
  - `effective_date_error`
  - `vendor_mismatch`
  - `insufficient_price_history`
- DoD: validation report produced for 50-sample and broad-sample runs.

16. Backfill jobs
- Historical backfill from 2015-01-01 to present.
- Sliding reprocess window (recent 30 days) for amendments.
- DoD: daily run report includes new/amended/rejected counts.

17. Quality gates
- Metrics: parse success rate, verification mismatch rate, factor coverage.
- Release gate thresholds:
  - parse success >= 99.0%
  - mismatch <= 1.0%
  - factor coverage >= 98.0%
  - external validation >= 99.0% within 2%
  - external validation >= 95.0% within 0.1% after excluding `vendor_mismatch`
- DoD: CI blocks release when thresholds fail.

18. Regression benchmark pack
- Compare `raw` vs `adjusted` backtest outputs over fixed periods.
- Track KPI deltas: CAGR, MDD, win-rate, turnover.
- DoD: automated report generated in CI artifact.

## Suggested Order
1. #1 -> #4
2. #5 -> #8
3. #9 -> #12
4. #13 -> #15

## Risks and Mitigations
- Risk: DS005-only ambiguity on effective dates.
- Mitigation: DS001 document verification + manual review state.

- Risk: amendment churn changes history.
- Mitigation: immutable versioning and bounded factor rebuild.

- Risk: performance regression in export/query.
- Mitigation: pre-materialized factors and dedicated indexes.

- Risk: third-party allotment is modeled as ordinary rights issue.
- Mitigation: subtype split and listing-date-based activation.

- Risk: vendor mismatch is misdiagnosed as factor error.
- Mitigation: validation outcome taxonomy and sample-based review.

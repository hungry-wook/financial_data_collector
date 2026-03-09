# Adjusted Price Backlog

Only unresolved work is listed here.

## P0

1. Rights-issue TERP pricing
- Implement price-aware factor rules for `RIGHTS_ISSUE` / `RIGHTS_BONUS_ISSUE` when DS005 pricing fields exist.
- Persist pricing inputs in payload and validation evidence.
- DoD: reviewed rights-issue cases with pricing inputs can be activated without manual overrides.

2. Event-date policy split
- Review whether adjustment apply date must be separated from legal effective date.
- Add explicit per-subtype date policy and evidence fields.
- DoD: remaining date-related misses are classified, not ambiguous.

3. DART review-pool reduction
- Recollect DART after quota reset.
- Re-run repair/review flow on recent-window `NEEDS_REVIEW` events.
- DoD: review pool shrinks through new source evidence.

## P1

4. Broad-sample external validation rerun
- Re-run 50-sample and broad-sample validation after fresh KRX+DART inputs.
- Keep miss taxonomy stable: `factor_error`, `effective_date_error`, `vendor_mismatch`, `insufficient_price_history`.
- DoD: validation report is reproducible and actionable.

5. KRX collector reliability follow-up
- Monitor the recent DB connection fix under repeated month-window runs.
- Add diagnostics only if failures recur.
- DoD: repeated runs finish successfully without the prior `10048` failure mode.

## P2

6. Strategy regression pack
- Compare `raw` vs `adjusted` strategy outputs over fixed windows after the review pool is reduced.
- Track impact on CAGR, MDD, win rate, turnover.
- DoD: backtest deltas are documented and attributable to adjustment changes.

# TODO Checklist

Only open work is tracked here.

## P0
- [ ] Implement price-aware rights-issue factor rules using DS005 pricing fields
- [ ] Revisit event-date policy for cases where legal effective date and adjustment-apply date diverge
- [ ] Re-run DART collection after quota reset and reprocess `NEEDS_REVIEW`

## P1
- [ ] Re-run broad-sample external validation with refreshed KRX + DART inputs
- [ ] Reduce recent-window `NEEDS_REVIEW` counts using new source evidence, not fallback assumptions
- [ ] Verify KRX collector stability across repeated month-window runs

## P2
- [ ] Run raw-vs-adjusted strategy regression once review-pool reduction is complete
- [ ] Update interface/schema docs only if the remaining work changes contracts

## What
<!-- One to three lines, imperative mood. -->

## Why
<!-- Business or data context: what breaks without this change? -->

## How
<!-- Approach taken. Note any breaking changes to schema, contracts, or schedules. -->

## Impact
- [ ] Affects raw layer / curated layer / silver / gold
- [ ] Breaking schema change (describe migration plan)
- [ ] Requires backfill

## Verification
- [ ] `make check` passes locally
- [ ] Sample output attached (e.g. `SELECT * FROM ghn_analytics.silver_orders LIMIT 10`)
- [ ] Downstream lineage checked

## Rollback
<!-- How do you undo this if it goes wrong in production? -->

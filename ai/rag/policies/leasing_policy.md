# Leasing & Occupancy Policy

**doc_id:** leasing_policy

## Occupancy risk / churn flag
A property is flagged for **churn / underperformance risk** when either:
- occupancy rate falls by more than **5 percentage points** versus the prior 30-day
  window (`occupancy_change_ppts < -0.05`), or
- maintenance cost rises by more than **15%** versus the prior 30-day window.

These properties appear in `gold_realestate.mart_property_underperformance` with
`is_underperforming = true`.

## Lease renewal
Leases within **90 days** of `lease_end` must have a renewal action logged. A property
with multiple expiring leases and falling occupancy is prioritised for pricing review.

## Rent roll
The monthly rent roll is the sum of `monthly_rent` across ACTIVE leases. A declining
rent roll alongside falling occupancy is a leadership-attention signal.

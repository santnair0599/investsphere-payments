# Revenue Management Policy (Hospitality)

**doc_id:** revenue_management_policy

## RevPAR target
Each hotel has a RevPAR target set at budget. A hotel is flagged for **revenue risk**
when any of the following hold:
- RevPAR falls by more than **8%** versus the prior 30-day window,
- occupancy falls by more than **5 percentage points**, or
- average guest rating drops below **3.5** / 5.

Flagged hotels appear in `gold_hospitality.mart_hotel_revenue_risk` with
`is_revenue_risk = true` and drivers in `risk_reasons`.

## Pricing response
A RevPAR decline driven by occupancy (not rate) triggers a demand-generation review;
a decline driven by rate triggers a pricing-floor review. F&B revenue is monitored
alongside room revenue for total-revenue-per-room health.

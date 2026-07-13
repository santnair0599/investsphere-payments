# Marketing Campaign Playbook (Entertainment)

**doc_id:** marketing_campaign_playbook

## Campaign ROI threshold
A campaign is considered effective when **ROI > 0** and **ROAS ≥ 3.0**
(attributed revenue ≥ 3× spend). Campaigns below this are paused for review.

## Footfall vs conversion
High footfall with low ticket conversion means marketing is driving traffic but not
purchase. When a venue shows rising footfall (`footfall_change_pct >= 0`) but a
conversion rate below **10%** (or a conversion drop > 5 ppts), the campaign–product
match is reviewed. These venues surface in
`gold_entertainment.mart_venue_conversion_risk` with `is_conversion_risk = true`.

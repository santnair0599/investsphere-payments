# Investment Risk Policy

**doc_id:** investment_risk_policy

## Risk rating
An asset is rated **HIGH risk** when any of the following hold:
- its risk threshold is breached (`risk_threshold_breached = true`),
- 95% Value-at-Risk (VaR-95) exceeds the asset's approved limit, or
- annualised volatility rises by more than **20%** versus the prior valuation snapshot.

## Concentration limit
No single asset may represent more than **10%** of total portfolio NAV
(`concentration_pct <= 0.10`). Breaches must be reported to the Investment Committee.

## Rising exposure
An asset shows **rising risk exposure** when it is HIGH-rated, has a threshold breach,
or shows a >20% volatility increase. These assets surface in
`gold_investment.mart_investment_risk` with `is_rising_risk = true` and the drivers in
`risk_reasons`.

## Excess return
Performance is judged net of benchmark: `excess_return = ytd_return - benchmark_return`.
Sustained negative excess return with rising volatility is escalated for review.

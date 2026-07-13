# Guest Experience Standards (Hospitality)

**doc_id:** guest_experience_standards

## Service recovery trigger
A **service recovery** is initiated when either:
- a guest review rating is below **3.5** / 5, or
- guest sentiment is **NEGATIVE**.

## Sentiment monitoring
The share of negative reviews (`negative_review_pct`) is tracked per hotel. A rising
negative share alongside falling RevPAR escalates the hotel to revenue-risk review.
Guest sentiment is also rolled up to customer segments in
`gold_customer.fact_customer_sentiment` to detect segment-level experience decline.

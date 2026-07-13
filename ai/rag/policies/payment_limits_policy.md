# Payment Limits & Approvals Policy (POL-PAY-001)

## Transaction limits
- Standard customers: single-payment limit **50,000 AED**; daily aggregate limit **150,000 AED**.
- Premium customers (status = PREMIUM): single-payment limit **250,000 AED**; daily aggregate
  limit **1,000,000 AED**.
- Any payment above the single-payment limit requires **dual approval** and is routed to the
  payment-risk review queue.

## Currency and FX rules
- Supported settlement currencies: AED, USD, EUR, GBP, INR, SAR.
- Payments in other currencies are converted at the daily FX reference rate; an FX variance
  above **2%** from the booked rate triggers a review.

## Refunds
- A refund (payment_type = REFUND) must reference an original payment id and cannot exceed the
  original amount. A refund amount above **25,000 AED** requires manager approval.

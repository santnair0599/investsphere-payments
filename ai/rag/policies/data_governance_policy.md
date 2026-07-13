# Data Governance Policy

**doc_id:** data_governance_policy

## PII in reports and AI answers
Customer/guest/tenant **PII (names, emails, phone numbers, addresses) must never be
exposed** in reports or AI-agent answers. The agent reads through Unity Catalog
**masked views** and reports only at the aggregate / segment / asset level.

## Access
The AI service principal has **read-only** grants on `gold_*` schemas and no access to
raw Bronze/Silver PII columns. Write/business actions require human approval.

## Trust and auditability
Every AI answer is logged to `ai_control.agent_runs` with the tools called, documents
cited, groundedness and safety status. A recommendation may only be presented with its
data **trust level** (from `gold_ops_trust.mart_business_recommendation_trust`).

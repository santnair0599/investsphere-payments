-- Unity Catalog governance for the Azure GenAI plane's service principal.
-- The agent gets READ-ONLY access to the Gold marts + ops-trust, and reads
-- customer/guest data ONLY through PII-masked views. No Bronze/Silver PII access.
-- {{catalog}} and {{ai_sp}} (the agent's service principal / group) are substituted
-- by the governance generator per environment.

-- ---- read-only on the business + trust Gold schemas -----------------------
GRANT USE CATALOG ON CATALOG {{catalog}} TO `{{ai_sp}}`;

GRANT USE SCHEMA, SELECT ON SCHEMA {{catalog}}.gold_realestate    TO `{{ai_sp}}`;
GRANT USE SCHEMA, SELECT ON SCHEMA {{catalog}}.gold_hospitality   TO `{{ai_sp}}`;
GRANT USE SCHEMA, SELECT ON SCHEMA {{catalog}}.gold_entertainment TO `{{ai_sp}}`;
GRANT USE SCHEMA, SELECT ON SCHEMA {{catalog}}.gold_investment    TO `{{ai_sp}}`;
GRANT USE SCHEMA, SELECT ON SCHEMA {{catalog}}.gold_ops_trust     TO `{{ai_sp}}`;

-- ---- customer domain: expose a MASKED view, not the base tables -----------
CREATE SCHEMA IF NOT EXISTS {{catalog}}.gold_customer_masked;

-- Segment-level marts carry no raw PII → grant directly.
GRANT USE SCHEMA ON SCHEMA {{catalog}}.gold_customer TO `{{ai_sp}}`;
GRANT SELECT ON TABLE {{catalog}}.gold_customer.mart_declining_customer_segments TO `{{ai_sp}}`;
GRANT SELECT ON TABLE {{catalog}}.gold_customer.dim_customer_segment            TO `{{ai_sp}}`;
GRANT SELECT ON TABLE {{catalog}}.gold_customer.fact_customer_activity          TO `{{ai_sp}}`;
GRANT SELECT ON TABLE {{catalog}}.gold_customer.fact_customer_sentiment         TO `{{ai_sp}}`;

-- The shared customer dimension DOES carry PII → agent sees only a masked view.
CREATE OR REPLACE VIEW {{catalog}}.gold_customer_masked.dim_customer AS
SELECT
    customer_id,
    -- names/emails/phones masked; only non-identifying attributes exposed
    'REDACTED'                              AS customer_name,
    concat('***@', split(email, '@')[1])    AS email_domain,
    nationality,
    status
FROM {{catalog}}.gold.dim_customer;

GRANT USE SCHEMA, SELECT ON SCHEMA {{catalog}}.gold_customer_masked TO `{{ai_sp}}`;

-- Explicitly DENY the raw PII base tables (defence in depth; agent must use marts/views).
-- (No GRANT on {{catalog}}.gold.dim_customer / silver_* to {{ai_sp}} — absence = no access.)

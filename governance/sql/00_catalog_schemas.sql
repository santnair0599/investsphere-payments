-- Catalog + schemas (idempotent).
CREATE CATALOG IF NOT EXISTS investsphere;
CREATE SCHEMA IF NOT EXISTS investsphere.governance;  -- mask/row-filter UDFs
CREATE SCHEMA IF NOT EXISTS investsphere.gold;
CREATE SCHEMA IF NOT EXISTS investsphere.gold_marts;
CREATE SCHEMA IF NOT EXISTS investsphere.silver_cdc;
CREATE SCHEMA IF NOT EXISTS investsphere.silver_clean;
CREATE SCHEMA IF NOT EXISTS investsphere.silver_control;
CREATE SCHEMA IF NOT EXISTS investsphere.silver_quarantine;
CREATE SCHEMA IF NOT EXISTS investsphere.gold_masked;

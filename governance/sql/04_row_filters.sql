-- Region row filter: global groups see all; region teams (add as UC
-- groups, e.g. region_uae) see only their region.
CREATE OR REPLACE FUNCTION investsphere.governance.region_filter(region STRING)
  RETURN CASE
    WHEN is_account_group_member('pii_approved_users') OR is_account_group_member('data_stewards') OR is_account_group_member('spn_investsphere_etl') THEN true
    WHEN is_account_group_member('region_' || lower(region)) THEN true
    ELSE false
  END;

ALTER TABLE investsphere.silver_cdc.customer_scd2 SET ROW FILTER investsphere.governance.region_filter ON (nationality);
ALTER TABLE investsphere.gold.dim_customer SET ROW FILTER investsphere.governance.region_filter ON (nationality);
ALTER TABLE investsphere.gold.dim_customer_history SET ROW FILTER investsphere.governance.region_filter ON (nationality);
ALTER TABLE investsphere.gold_marts.daily_payment_summary SET ROW FILTER investsphere.governance.region_filter ON (customer_country);

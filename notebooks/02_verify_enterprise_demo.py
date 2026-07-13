# Databricks notebook source
# MAGIC %md
# MAGIC # Verify the enterprise demo (Gold marts + trust + quarantine)
# MAGIC
# MAGIC Run this **after** `dbt build` + `dbt test`. It checks the acceptance criteria:
# MAGIC quarantine has rows, every Gold `mart_*` is populated with `risk_reasons`, and
# MAGIC `gold_ops_trust` produces a `trust_level` + `confidence_score`.
# MAGIC
# MAGIC Set `catalog` to match your run (default `investsphere_dev`).

# COMMAND ----------
dbutils.widgets.text("catalog", "investsphere_dev")
catalog = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {catalog}")
print("catalog:", catalog)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Quarantine — DQ gate routed the `# BAD:` rows

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT source_table, failed_rule_name, COUNT(*) AS quarantined
# MAGIC FROM silver_quarantine.failed_records
# MAGIC GROUP BY source_table, failed_rule_name
# MAGIC ORDER BY quarantined DESC;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Operational trust — SUCCESS + PARTIAL scenarios, trust_level + confidence_score

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT run_date, trust_level, confidence_score, max_quarantine_rate,
# MAGIC        stale_sources, trust_reasons
# MAGIC FROM gold_ops_trust.mart_business_recommendation_trust;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT run_date, job_name, status, is_partial, is_failed, error_message
# MAGIC FROM gold_ops_trust.mart_pipeline_status
# MAGIC ORDER BY run_date DESC;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Business Gold marts — populated, with `risk_reasons`

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT property_id, property_name, occupancy_change_ppts,
# MAGIC        maintenance_cost_change_pct, is_underperforming, risk_reasons
# MAGIC FROM gold_realestate.mart_property_underperformance
# MAGIC ORDER BY is_underperforming DESC;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT hotel_id, hotel_name, revpar_change_pct, occupancy_change_ppts,
# MAGIC        avg_rating, is_revenue_risk, risk_reasons
# MAGIC FROM gold_hospitality.mart_hotel_revenue_risk
# MAGIC ORDER BY is_revenue_risk DESC;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT venue_id, venue_name, footfall_change_pct, conversion_rate,
# MAGIC        conversion_change_ppts, is_conversion_risk, risk_reasons
# MAGIC FROM gold_entertainment.mart_venue_conversion_risk
# MAGIC ORDER BY is_conversion_risk DESC;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT asset_id, asset_name, risk_rating, volatility_change_pct,
# MAGIC        excess_return, is_rising_risk, risk_reasons
# MAGIC FROM gold_investment.mart_investment_risk
# MAGIC ORDER BY is_rising_risk DESC;

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT segment, active_change_pct, revenue_change_pct, avg_sentiment_score,
# MAGIC        is_declining, risk_reasons
# MAGIC FROM gold_customer.mart_declining_customer_segments
# MAGIC ORDER BY is_declining DESC;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. One-glance PASS/FAIL assertion
# MAGIC Fails loudly if quarantine is empty, any mart is empty, or the trust score is missing.

# COMMAND ----------
def _count(sql):
    return spark.sql(sql).collect()[0][0]

results = {}
results["quarantine_rows"] = _count(
    f"SELECT COUNT(*) FROM {catalog}.silver_quarantine.failed_records")
results["trust_rows"] = _count(
    f"SELECT COUNT(*) FROM {catalog}.gold_ops_trust.mart_business_recommendation_trust")
results["trust_has_score"] = _count(
    f"SELECT COUNT(*) FROM {catalog}.gold_ops_trust.mart_business_recommendation_trust "
    f"WHERE trust_level IS NOT NULL AND confidence_score IS NOT NULL")

MARTS = {
    "realestate":   "gold_realestate.mart_property_underperformance",
    "hospitality":  "gold_hospitality.mart_hotel_revenue_risk",
    "entertainment":"gold_entertainment.mart_venue_conversion_risk",
    "investment":   "gold_investment.mart_investment_risk",
    "customer":     "gold_customer.mart_declining_customer_segments",
}
for name, tbl in MARTS.items():
    results[f"{name}_rows"] = _count(f"SELECT COUNT(*) FROM {catalog}.{tbl}")

print("=== demo verification ===")
for k, v in results.items():
    print(f"  {k:<24} {v}")

problems = []
if results["quarantine_rows"] == 0:
    problems.append("quarantine empty (expected the # BAD: rows) — did the conformers run?")
if results["trust_has_score"] == 0:
    problems.append("trust score missing — check silver_control seed + gold_ops_trust build")
empty_marts = [n for n in MARTS if results[f"{n}_rows"] == 0]
if empty_marts:
    problems.append(
        f"empty marts {empty_marts} — likely the current_date() window vs synthetic "
        f"data date mismatch (see RUNBOOK troubleshooting)")

if problems:
    raise Exception("DEMO VERIFICATION FAILED:\n  - " + "\n  - ".join(problems))
print("\n✅ DEMO VERIFICATION PASSED — quarantine, all 5 marts, and trust score populated.")

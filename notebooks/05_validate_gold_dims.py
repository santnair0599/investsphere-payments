# Databricks notebook source
# MAGIC %md
# MAGIC # Validate: Gold customer dimension + daily payment summary
# MAGIC
# MAGIC Confirms dbt Gold builds off the real SCD2 dimension:
# MAGIC - `gold.dim_customer` — **current** customers (`is_current = true AND is_deleted = false`)
# MAGIC - `gold.dim_customer_history` — full SCD2 history (soft-deletes flagged)
# MAGIC - `gold_marts.daily_payment_summary` — fact joined to the customer dimension
# MAGIC
# MAGIC Prereq: the Silver `customer_scd2` + `payment_clean` tables exist and
# MAGIC `dbt build` has run (the `dbt_build` job task).

# COMMAND ----------
dbutils.widgets.text("catalog", "investsphere_dev")
catalog = dbutils.widgets.get("catalog")
dim = f"{catalog}.gold.dim_customer"
hist = f"{catalog}.gold.dim_customer_history"
summ = f"{catalog}.gold_marts.daily_payment_summary"
scd2 = f"{catalog}.silver_cdc.customer_scd2"
print(dim, hist, summ)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Current customer dimension (`dim_customer`)
# MAGIC Only current, non-deleted customers.

# COMMAND ----------
display(spark.sql(f"SELECT customer_id, customer_name, email, nationality, status FROM {dim} ORDER BY customer_id"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Soft-deleted customers are EXCLUDED from `dim_customer`, flagged in history
# MAGIC A customer deleted in the CDC feed must not appear in the current dimension,
# MAGIC but must remain in `dim_customer_history` with `is_deleted = true`.

# COMMAND ----------
# customers that are soft-deleted in the SCD2 source
display(spark.sql(f"""
  SELECT customer_id, is_current, is_deleted, effective_from, effective_to
  FROM {scd2}
  WHERE is_deleted = true
  ORDER BY customer_id
"""))

# COMMAND ----------
# assert: none of the soft-deleted customers leaked into dim_customer  (expect 0 rows)
display(spark.sql(f"""
  SELECT d.customer_id
  FROM {dim} d
  JOIN (SELECT DISTINCT customer_id FROM {scd2} WHERE is_deleted = true) x
    ON d.customer_id = x.customer_id
"""))

# COMMAND ----------
# they ARE preserved in history (soft-deletes flagged)
display(spark.sql(f"""
  SELECT customer_id, is_current, is_deleted, effective_from, effective_to
  FROM {hist}
  ORDER BY customer_id, effective_from
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Daily payment summary still builds (fact + customer dimension)
# MAGIC LEFT JOIN to `dim_customer`, so payments to a since-deleted customer are
# MAGIC still counted, with a null `customer_country`.

# COMMAND ----------
display(spark.sql(f"""
  SELECT transaction_date, currency_code, payment_type, customer_country,
         payment_count, total_amount, avg_amount
  FROM {summ}
  ORDER BY transaction_date, currency_code, payment_type
"""))

# COMMAND ----------
display(spark.sql(f"SELECT count(*) AS summary_rows, sum(payment_count) AS payments FROM {summ}"))

# Databricks notebook source
# MAGIC %md
# MAGIC # Validate: Customer CDC → Bronze → Silver SCD2
# MAGIC
# MAGIC Confirms the two real Spark modules ran end-to-end:
# MAGIC - `src/payments_platform/databricks/bronze_customer_cdc.py` → `bronze.customer_cdc`
# MAGIC - `src/payments_platform/databricks/silver_customer_scd2.py` → `silver_cdc.customer_scd2`
# MAGIC
# MAGIC Prereq: land the CDC seed and run the two tasks, e.g.
# MAGIC `cp seeds/cdc/customer_cdc_events.json` → `/Volumes/<catalog>/landing/raw/customer_cdc/`,
# MAGIC then run the `bronze_customer_cdc` + `silver_customer_scd2` tasks (or the job).

# COMMAND ----------
dbutils.widgets.text("catalog", "investsphere_dev")
catalog = dbutils.widgets.get("catalog")
bronze = f"{catalog}.bronze.customer_cdc"
scd2 = f"{catalog}.silver_cdc.customer_scd2"
print(bronze, "->", scd2)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Bronze CDC — the landed event stream
# MAGIC One row per Debezium envelope, with `operation_type`, `sequence_number`
# MAGIC (source LSN) and `event_timestamp` preserved for Silver to order by.

# COMMAND ----------
display(spark.sql(f"""
  SELECT id, name, email, country, status,
         operation_type, sequence_number, event_timestamp, run_id
  FROM {bronze}
  ORDER BY id, sequence_number
"""))

# COMMAND ----------
display(spark.sql(f"""
  SELECT operation_type, count(*) AS events
  FROM {bronze} GROUP BY operation_type ORDER BY operation_type
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Silver SCD2 — full table (current + history)
# MAGIC `is_current` / `effective_from` / `effective_to` give the version timeline;
# MAGIC `is_deleted = true` is a soft-deleted (tombstone) current version.

# COMMAND ----------
display(spark.sql(f"""
  SELECT customer_id, customer_name, email, nationality, status,
         is_current, is_deleted, effective_from, effective_to,
         sequence_number, substr(record_hash, 1, 12) AS record_hash
  FROM {scd2}
  ORDER BY customer_id, effective_from
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Current dimension (what dbt `dim_customer` reads)
# MAGIC `is_current = true AND is_deleted = false`.

# COMMAND ----------
display(spark.sql(f"""
  SELECT customer_id, customer_name, email, nationality, status
  FROM {scd2}
  WHERE is_current = true AND is_deleted = false
  ORDER BY customer_id
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. History + soft-deletes (SCD2 audit)
# MAGIC Expired versions (`is_current = false`) and tombstones (`is_deleted = true`).
# MAGIC Re-land a new change for an existing customer and re-run Silver to watch a
# MAGIC current version expire and a new one appear (history builds across batches).

# COMMAND ----------
display(spark.sql(f"""
  SELECT customer_id, is_current, is_deleted, effective_from, effective_to, sequence_number
  FROM {scd2}
  WHERE is_current = false OR is_deleted = true
  ORDER BY customer_id, effective_from
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Sanity checks
# MAGIC At most one current row per customer; a tombstone means `is_deleted = true`.

# COMMAND ----------
display(spark.sql(f"""
  SELECT customer_id, count(*) AS current_versions
  FROM {scd2}
  WHERE is_current = true
  GROUP BY customer_id
  HAVING count(*) > 1
"""))    # expect: 0 rows

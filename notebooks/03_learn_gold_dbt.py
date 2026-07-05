# Databricks notebook source
# MAGIC %md
# MAGIC # Learn: Silver → Gold (dbt)
# MAGIC
# MAGIC Gold is built by **dbt**, not a Spark script — so "learning it" means two
# MAGIC things: (1) *see* the transformation the dbt model expresses, and (2) *see*
# MAGIC that dbt's incremental model compiles to a **Delta MERGE**.
# MAGIC
# MAGIC The models live in the repo:
# MAGIC - `dbt/models/staging/stg_payments.sql` — a thin **view** over `silver_clean.payment_clean`
# MAGIC - `dbt/models/gold/fact_payments.sql` — **incremental** fact, `unique_key='payment_id'` → **MERGE**
# MAGIC
# MAGIC > Lines starting with `# MAGIC` are how a Databricks notebook file stores
# MAGIC > markdown; the cells below are **plain Python** (`spark.sql(...)`) so you can
# MAGIC > run them and see output. Prereq: you ran `02_learn_silver_payments`.

# COMMAND ----------
dbutils.widgets.text("catalog", "investsphere_dev")
catalog = dbutils.widgets.get("catalog")
print("catalog:", catalog)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. stg_payments — the staging view (SQL from the dbt model, run by hand)
# MAGIC A thin view over `silver_clean.payment_clean` — keeps Gold decoupled from
# MAGIC Silver's physical table. Run this cell to see its output.

# COMMAND ----------
stg = spark.sql(f"""
    SELECT payment_id, customer_id, account_id, amount, currency_code,
           payment_type, transaction_date, source_system, ingestion_timestamp
    FROM {catalog}.silver_clean.payment_clean
""")
display(stg)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. fact_payments — the Gold fact SELECT (adds gold_processed_timestamp)
# MAGIC This is the exact transformation in `fact_payments.sql`. On later runs dbt
# MAGIC only reprocesses a recent window (`transaction_date >= current_date - 3`) —
# MAGIC the incremental pattern for a large fact.

# COMMAND ----------
fact = spark.sql(f"""
    SELECT p.payment_id, p.customer_id, p.account_id, p.amount, p.currency_code,
           p.payment_type, p.transaction_date, p.source_system,
           current_timestamp() AS gold_processed_timestamp
    FROM {catalog}.silver_clean.payment_clean p
""")
display(fact)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. See the MERGE dbt generates (the key learning)
# MAGIC `fact_payments` is `materialized='incremental'` with `unique_key='payment_id'`,
# MAGIC which on Delta compiles to a `MERGE INTO`. Here's the equivalent MERGE run by
# MAGIC hand into a demo table, so you see the mechanics dbt performs for you.

# COMMAND ----------
demo = f"{catalog}.gold.fact_payments_demo"

# 3a. create an empty shell once (WHERE 1=0 -> no rows, just the schema)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {demo} AS
    SELECT p.payment_id, p.customer_id, p.amount, p.currency_code,
           p.transaction_date, current_timestamp() AS gold_processed_timestamp
    FROM {catalog}.silver_clean.payment_clean p WHERE 1=0
""")

# 3b. MERGE — exactly what dbt does on every incremental run
spark.sql(f"""
    MERGE INTO {demo} t
    USING (
      SELECT p.payment_id, p.customer_id, p.amount, p.currency_code,
             p.transaction_date, current_timestamp() AS gold_processed_timestamp
      FROM {catalog}.silver_clean.payment_clean p
    ) s
    ON t.payment_id = s.payment_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

display(spark.sql(f"SELECT count(*) AS fact_rows FROM {demo}"))

# COMMAND ----------
# MAGIC %md
# MAGIC **Re-run the cell above** — `fact_rows` does **not** double. Matched
# MAGIC `payment_id`s are updated in place, not re-inserted. That's MERGE idempotency
# MAGIC (and why re-running the pipeline is safe), vs a plain append that would duplicate.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. See dbt's ACTUAL compiled MERGE (on your machine, from the repo)
# MAGIC ```bash
# MAGIC cd dbt
# MAGIC dbt compile --vars '{catalog: investsphere_dev}' --select fact_payments
# MAGIC #   then open: dbt/target/run/investsphere_payments/models/gold/fact_payments.sql
# MAGIC #   -> a real: create ... ; merge into ... using (...) on ... when matched ...
# MAGIC ```
# MAGIC The job's `dbt_build` task runs exactly this against the SQL warehouse to
# MAGIC build the real `gold.fact_payments` (this notebook only made a `_demo` copy).

# COMMAND ----------
# 5. clean up the demo table
spark.sql(f"DROP TABLE IF EXISTS {catalog}.gold.fact_payments_demo")
print("dropped demo table")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Recap — where each transformation lives
# MAGIC | Transition | Code | Engine |
# MAGIC |---|---|---|
# MAGIC | file → Bronze | `databricks/bronze_payments_autoloader.py` | Auto Loader |
# MAGIC | Bronze → Silver | `databricks/silver_payments.py` | Spark + Delta MERGE |
# MAGIC | Silver → Gold | `dbt/models/**` (`fact_payments.sql`) | dbt → Delta MERGE |
# MAGIC
# MAGIC The **real** `gold.fact_payments` is built by dbt (the `dbt_build` job task).
# MAGIC This notebook is to *understand* what that task does.

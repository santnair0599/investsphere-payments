# Databricks notebook source
# MAGIC %md
# MAGIC # Learn: Bronze → Silver (payments)
# MAGIC
# MAGIC Run **cell by cell** to *see* every transformation between Bronze and Silver.
# MAGIC This mirrors the production module
# MAGIC `src/payments_platform/databricks/silver_payments.py` — but interactive, so
# MAGIC you inspect each stage:
# MAGIC
# MAGIC **read Bronze → parse/cast → DQ rules → quarantine bad rows → dedup → MERGE clean rows**
# MAGIC
# MAGIC Prereq: you've run `01_learn_bronze_autoloader` (so `bronze.payments_file` exists).

# COMMAND ----------
dbutils.widgets.text("catalog", "investsphere_dev")
catalog = dbutils.widgets.get("catalog")
run_id = "manual-run-01"

bronze_table = f"{catalog}.bronze.payments_file"
clean_table = f"{catalog}.silver_clean.payment_clean"
quarantine_table = f"{catalog}.silver_quarantine.failed_records"
print(bronze_table, "->", clean_table, "/", quarantine_table)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. What Silver starts from — the Bronze table
# MAGIC Raw, string-typed business columns + audit columns + `_rescued_data`.

# COMMAND ----------
from pyspark.sql import functions as F, Window

bronze = spark.table(bronze_table)
display(bronze)
print("bronze rows:", bronze.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Parse / cast / standardize → conformed schema
# MAGIC Trim strings, **cast `amount` to decimal**, **`transaction_date` to a real
# MAGIC date**, upper-case currency/type. Bad values become `NULL` here (a failed
# MAGIC cast → null), which the DQ step then catches.

# COMMAND ----------
parsed = bronze.select(
    F.trim("payment_id").alias("payment_id"),
    F.trim("customer_id").alias("customer_id"),
    F.trim("account_id").alias("account_id"),
    F.col("amount").cast("decimal(18,2)").alias("amount"),
    F.upper(F.trim(F.col("currency"))).alias("currency_code"),
    F.upper(F.trim(F.col("payment_type"))).alias("payment_type"),
    F.to_date("transaction_date").alias("transaction_date"),
    "source_system", "run_id", "ingestion_timestamp", "record_hash",
)
display(parsed)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Data-quality rules — SEE exactly which rows fail and why
# MAGIC These mirror `silver/dq.py::PAYMENT_RULES`. We show each rule as its own
# MAGIC column so you can see the failures, then a combined `dq_failed` flag.

# COMMAND ----------
ALLOWED_CURRENCIES = ["AED", "USD", "EUR", "GBP", "INR", "SAR"]     # == dq.ALLOWED_CURRENCIES
ALLOWED_PAYMENT_TYPES = ["CREDIT", "DEBIT", "TRANSFER", "REFUND"]   # == dq.ALLOWED_PAYMENT_TYPES

checked = (parsed
    .withColumn("r_missing_payment_id", F.col("payment_id").isNull())
    .withColumn("r_missing_customer_id", F.col("customer_id").isNull())
    .withColumn("r_amount_null", F.col("amount").isNull())
    .withColumn("r_amount_negative", F.col("amount") < 0)
    .withColumn("r_bad_currency", ~F.col("currency_code").isin(ALLOWED_CURRENCIES))
    .withColumn("r_bad_type", ~F.col("payment_type").isin(ALLOWED_PAYMENT_TYPES))
    .withColumn("r_bad_date", F.col("transaction_date").isNull()))

dq_failed = (F.col("r_missing_payment_id") | F.col("r_missing_customer_id")
             | F.col("r_amount_null") | F.col("r_amount_negative")
             | F.col("r_bad_currency") | F.col("r_bad_type") | F.col("r_bad_date"))
checked = checked.withColumn("dq_failed", dq_failed)

# eyeball the per-rule flags
display(checked.select("payment_id", "amount", "currency_code", "payment_type",
                       "transaction_date", "dq_failed",
                       "r_amount_negative", "r_bad_currency", "r_bad_type"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Quarantine the failures (Failed Record Register — never dropped)
# MAGIC Bad rows go to `silver_quarantine.failed_records` with full context: the
# MAGIC failing rule, reason, the raw payload, lineage, and `status='OPEN'`.

# COMMAND ----------
quarantine = (checked.where(F.col("dq_failed"))
    .withColumn("source_table", F.lit(bronze_table))
    .withColumn("record_key", F.col("payment_id"))
    .withColumn("failed_rule_name", F.lit("payment_silver_dq"))
    .withColumn("failure_reason",
                F.lit("mandatory field / type / range / allowed-value check failed"))
    .withColumn("severity", F.lit("QUARANTINE"))
    .withColumn("raw_payload", F.to_json(F.struct(
        "payment_id", "customer_id", "account_id", "amount",
        "currency_code", "payment_type", "transaction_date")))
    .withColumn("failed_at", F.current_timestamp())
    .withColumn("status", F.lit("OPEN"))
    .select("source_system", "source_table", "record_key", "failure_reason",
            "failed_rule_name", "severity", "raw_payload", "ingestion_timestamp",
            "failed_at", "run_id", "status"))

display(quarantine)
(quarantine.write.format("delta").mode("append")
    .option("mergeSchema", "true").saveAsTable(quarantine_table))
print("quarantined rows written:", quarantine.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Dedup the valid rows — keep the latest per payment_id
# MAGIC Bronze can hold duplicates (re-delivered files, retries). We keep one row
# MAGIC per `payment_id` (latest by `ingestion_timestamp`) — a `row_number()` window.

# COMMAND ----------
valid = checked.where(~F.col("dq_failed")).select(
    "payment_id", "customer_id", "account_id", "amount", "currency_code",
    "payment_type", "transaction_date", "source_system", "run_id",
    "ingestion_timestamp", "record_hash")

w = Window.partitionBy("payment_id").orderBy(F.col("ingestion_timestamp").desc())
clean = valid.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")

print("valid rows:", valid.count(), "-> after dedup:", clean.count())
display(clean)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. MERGE the clean rows into the trusted Silver table
# MAGIC `MERGE` upserts on `payment_id`: existing rows are updated, new rows inserted.
# MAGIC This is why re-running is **idempotent** — no duplicates on a second run.

# COMMAND ----------
from delta.tables import DeltaTable

if spark.catalog.tableExists(clean_table):
    (DeltaTable.forName(spark, clean_table).alias("t")
        .merge(clean.alias("s"), "t.payment_id = s.payment_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print("MERGEd into existing", clean_table)
else:
    clean.write.format("delta").mode("overwrite").saveAsTable(clean_table)
    print("created", clean_table)

display(spark.table(clean_table))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Prove MERGE is idempotent
# MAGIC Re-run cell 6 — the row count in `payment_clean` **does not change** (rows are
# MAGIC updated in place, not duplicated). That's the whole point of MERGE vs append.

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT count(*) AS clean_rows FROM ${catalog}.silver_clean.payment_clean;
# MAGIC -- SELECT * FROM ${catalog}.silver_quarantine.failed_records;

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. This is exactly what the job runs
# MAGIC `src/payments_platform/databricks/silver_payments.py::run()` does all of the
# MAGIC above; `pipelines/dag_task.py` calls it for the `silver_payments` task. The
# MAGIC bundle just packages it. **Next:** Silver → Gold (dbt) — see
# MAGIC `03_learn_gold_dbt`.

# Databricks notebook source
# MAGIC %md
# MAGIC # Generate synthetic BRONZE data for the InvestSphere enterprise lakehouse
# MAGIC
# MAGIC Run this **once** on a Databricks cluster (Spark) to stand up a fully demoable
# MAGIC pipeline **without** any real Oracle / Salesforce / SQL Server / SFTP / booking
# MAGIC REST source. It writes:
# MAGIC
# MAGIC 1. Every **Bronze** table the six Silver conformers read, with the *exact* raw
# MAGIC    business columns each conformer selects (as STRINGS — Bronze is raw) plus the
# MAGIC    Bronze audit contract (`source_system`, `run_id`, `ingestion_timestamp`,
# MAGIC    `record_hash`).
# MAGIC 2. The shared **customer SCD2** dimension (`silver_cdc.customer_scd2`) directly,
# MAGIC    so the CDC step can be skipped for a pure demo.
# MAGIC 3. The **control tables** (`silver_control.pipeline_run`,
# MAGIC    `silver_control.table_load_status`) that drive the `gold_ops_trust` trust marts.
# MAGIC
# MAGIC Each domain contains a few **intentionally DQ-failing rows** (marked `# BAD:`),
# MAGIC so the Silver DQ gate → quarantine (`silver_quarantine.failed_records`) → and the
# MAGIC trust score have something real to show.
# MAGIC
# MAGIC Everything is **deterministic** (fixed `run_id`, no random/uuid) so re-running is
# MAGIC idempotent and the numbers in the demo never move.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Parameters
# MAGIC `spark` already exists in a Databricks notebook. Two widgets: the target
# MAGIC `catalog` and a fixed `run_id` (matches what the Silver conformers filter Bronze on).

# COMMAND ----------
dbutils.widgets.text("catalog", "investsphere_dev")
dbutils.widgets.text("run_id", "demo_run_001")

catalog = dbutils.widgets.get("catalog")
run_id = dbutils.widgets.get("run_id")

# Deterministic ingest stamp for Bronze rows (string — Bronze is raw text).
INGEST_TS = "2026-07-05 06:00:00"
AUDIT_COLS = ["source_system", "run_id", "ingestion_timestamp", "record_hash"]

print("catalog :", catalog)
print("run_id  :", run_id)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Schemas
# MAGIC Create every schema this notebook writes to (`bronze`, `silver_cdc`,
# MAGIC `silver_control`, `silver_quarantine`) plus the trusted Silver domain schemas the
# MAGIC conformers MERGE into — so the whole pipeline runs end to end after this notebook.

# COMMAND ----------
for schema in [
    "bronze", "silver_cdc", "silver_control", "silver_quarantine",
    # trusted Silver targets written by the conformers (created here for convenience)
    "silver_clean", "silver_realestate", "silver_hospitality",
    "silver_entertainment", "silver_investment", "silver_customer", "silver_fx",
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    print("schema ready:", f"{catalog}.{schema}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Bronze writer helper
# MAGIC `make_bronze()` takes the raw business columns + rows (native Python values),
# MAGIC stringifies them (Bronze is raw text), appends the audit contract with a
# MAGIC deterministic `record_hash` over the sorted business columns, writes a managed
# MAGIC Delta table, and records load stats so the control tables can be seeded to match.

# COMMAND ----------
import hashlib
from pyspark.sql.types import StructType, StructField, StringType

# registry of what we wrote, so silver_control.table_load_status matches exactly
LOAD_STATS = []


def _record_hash(biz_cols, row):
    """sha256 over business columns (sorted names, '|' sep, null->'') — mirrors the
    platform's common.hashing.record_hash contract used by the real ingestors."""
    parts = ["" if row[biz_cols.index(c)] is None else str(row[biz_cols.index(c)])
             for c in sorted(biz_cols)]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def make_bronze(name, biz_cols, rows, source_system, target_table, load_type,
                records_rejected):
    """Create `{catalog}.bronze.{name}` as a Delta table with raw string business
    columns + the Bronze audit contract, and register its load stats."""
    full_cols = biz_cols + AUDIT_COLS
    schema = StructType([StructField(c, StringType(), True) for c in full_cols])

    data = []
    for r in rows:
        vals = [None if v is None else str(v) for v in r]
        data.append(tuple(vals) + (source_system, run_id, INGEST_TS,
                                    _record_hash(biz_cols, r)))

    df = spark.createDataFrame(data, schema)
    target = f"{catalog}.bronze.{name}"
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(target))

    LOAD_STATS.append({
        "source_system": source_system,
        "source_table": target,                       # full name (matches quarantine)
        "target_table": f"{catalog}.{target_table}",
        "load_type": load_type,
        "records_read": len(rows),
        "records_written": len(rows) - records_rejected,
        "records_rejected": records_rejected,
    })
    print(f"bronze  {target:<48} rows={len(rows):>3}  bad={records_rejected}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Real estate (Oracle PMS) → `silver_realestate.*`
# MAGIC Bronze: `oracle_properties`, `oracle_leases`, `oracle_occupancy_daily`,
# MAGIC `oracle_maintenance_orders`. `customer_id` 101-108 join the shared dimension.

# COMMAND ----------
# ---- oracle_properties (PK property_id) ------------------------------------
make_bronze(
    "oracle_properties",
    ["property_id", "property_name", "property_type", "city", "emirate",
     "gross_leasable_area_sqm", "units_total", "acquisition_date"],
    [
        ("PROP-001", "Dubai Hills Estate", "RESIDENTIAL", "Dubai", "Dubai", "85000.00", "320", "2016-03-01"),
        ("PROP-002", "Business Bay Tower", "COMMERCIAL", "Dubai", "Dubai", "42000.00", "180", "2018-07-15"),
        ("PROP-003", "City Walk Retail", "RETAIL", "Dubai", "Dubai", "30000.00", "95", "2017-11-20"),
        ("PROP-004", "Bluewaters Residences", "RESIDENTIAL", "Dubai", "Dubai", "60000.00", "240", "2019-02-10"),
        ("PROP-005", "Marina Mall Complex", "MIXED_USE", "Abu Dhabi", "Abu Dhabi", "120000.00", "410", "2015-09-05"),
        ("PROP-006", "JBR Beachfront Resort", "HOSPITALITY", "Dubai", "Dubai", "25000.00", "60", "2020-01-25"),
        # BAD: invalid property_type 'WAREHOUSE' (not in allowed set)
        ("PROP-BAD1", "Unknown Warehouse", "WAREHOUSE", "Sharjah", "Sharjah", "10000.00", "20", "2021-06-01"),
        # BAD: null PK (property_id)
        (None, "Ghost Property", "RESIDENTIAL", "Dubai", "Dubai", "5000.00", "10", "2022-01-01"),
    ],
    source_system="oracle_pms", target_table="silver_realestate.property_clean",
    load_type="FULL_SNAPSHOT", records_rejected=2)

# ---- oracle_leases (PK lease_id) ------------------------------------------
make_bronze(
    "oracle_leases",
    ["lease_id", "property_id", "customer_id", "unit_id", "monthly_rent",
     "currency_code", "lease_start", "lease_end", "status"],
    [
        ("LEASE-001", "PROP-001", "101", "U-101", "12000.00", "AED", "2023-01-01", "2024-12-31", "ACTIVE"),
        ("LEASE-002", "PROP-002", "102", "U-202", "45000.00", "AED", "2022-06-01", "2025-05-31", "ACTIVE"),
        ("LEASE-003", "PROP-003", "103", "U-303", "22000.00", "AED", "2021-03-01", "2023-02-28", "EXPIRED"),
        ("LEASE-004", "PROP-004", "104", "U-404", "18000.00", "AED", "2023-09-01", "2026-08-31", "ACTIVE"),
        ("LEASE-005", "PROP-005", "105", "U-505", "30000.00", "AED", "2020-01-01", "2022-12-31", "TERMINATED"),
        ("LEASE-006", "PROP-001", "106", "U-106", "13500.00", "AED", "2024-01-01", "2025-12-31", "ACTIVE"),
        # BAD: invalid currency 'XYZ'
        ("LEASE-BAD1", "PROP-002", "107", "U-207", "25000.00", "XYZ", "2023-01-01", "2024-12-31", "ACTIVE"),
        # BAD: negative monthly_rent
        ("LEASE-BAD2", "PROP-003", "108", "U-308", "-5000.00", "AED", "2023-01-01", "2024-12-31", "ACTIVE"),
    ],
    source_system="oracle_pms", target_table="silver_realestate.lease_clean",
    load_type="INCREMENTAL", records_rejected=2)

# ---- oracle_occupancy_daily (PK occupancy_id) -----------------------------
make_bronze(
    "oracle_occupancy_daily",
    ["occupancy_id", "property_id", "occupancy_date", "units_occupied",
     "units_total", "occupancy_rate"],
    [
        ("OCC-001", "PROP-001", "2026-07-01", "300", "320", "0.9375"),
        ("OCC-002", "PROP-002", "2026-07-01", "150", "180", "0.8333"),
        ("OCC-003", "PROP-003", "2026-07-01", "80", "95", "0.8421"),
        ("OCC-004", "PROP-004", "2026-07-01", "210", "240", "0.8750"),
        ("OCC-005", "PROP-005", "2026-07-01", "370", "410", "0.9024"),
        ("OCC-006", "PROP-001", "2026-07-02", "305", "320", "0.9531"),
        ("OCC-007", "PROP-002", "2026-07-02", "160", "180", "0.8889"),
        # BAD: occupancy_rate > 1
        ("OCC-BAD1", "PROP-003", "2026-07-02", "80", "95", "1.5000"),
    ],
    source_system="oracle_pms", target_table="silver_realestate.occupancy_clean",
    load_type="INCREMENTAL", records_rejected=1)

# ---- oracle_maintenance_orders (PK work_order_id) -------------------------
make_bronze(
    "oracle_maintenance_orders",
    ["work_order_id", "property_id", "category", "priority", "cost",
     "currency_code", "opened_date", "closed_date", "status"],
    [
        ("WO-001", "PROP-001", "HVAC", "HIGH", "3500.00", "AED", "2026-06-20", "2026-06-25", "CLOSED"),
        ("WO-002", "PROP-002", "Plumbing", "MEDIUM", "1200.00", "AED", "2026-06-22", None, "OPEN"),
        ("WO-003", "PROP-003", "Electrical", "CRITICAL", "8000.00", "AED", "2026-06-18", "2026-06-30", "CLOSED"),
        ("WO-004", "PROP-004", "Elevator", "HIGH", "15000.00", "AED", "2026-06-15", None, "IN_PROGRESS"),
        ("WO-005", "PROP-005", "Landscaping", "LOW", "600.00", "AED", "2026-06-10", "2026-06-12", "CLOSED"),
        # BAD: negative cost
        ("WO-BAD1", "PROP-001", "HVAC", "HIGH", "-500.00", "AED", "2026-06-20", "2026-06-25", "CLOSED"),
    ],
    source_system="oracle_pms", target_table="silver_realestate.maintenance_clean",
    load_type="INCREMENTAL", records_rejected=1)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Hospitality (booking REST API + CRM cases) → `silver_hospitality.*`
# MAGIC Bronze: `rest_hotels`, `rest_hotel_bookings`, `rest_hotel_revenue`, `sfdc_case`
# MAGIC (guest reviews). Bookings/reviews carry `customer_id` 101-108.

# COMMAND ----------
# ---- rest_hotels (PK hotel_id) --------------------------------------------
make_bronze(
    "rest_hotels",
    ["hotel_id", "hotel_name", "city", "emirate", "star_rating", "rooms_total", "brand"],
    [
        ("HOT-001", "Atlantis The Palm", "Dubai", "Dubai", "5", "1500", "Atlantis"),
        ("HOT-002", "Jumeirah Beach Hotel", "Dubai", "Dubai", "5", "600", "Jumeirah"),
        ("HOT-003", "Emirates Palace", "Abu Dhabi", "Abu Dhabi", "5", "390", "Mandarin Oriental"),
        ("HOT-004", "Rove Downtown", "Dubai", "Dubai", "3", "420", "Rove"),
        # BAD: star_rating out of range (> 5)
        ("HOT-BAD1", "Mystery Inn", "Sharjah", "Sharjah", "7", "100", "Unknown"),
    ],
    source_system="booking_rest_api", target_table="silver_hospitality.hotel_clean",
    load_type="FULL_SNAPSHOT", records_rejected=1)

# ---- rest_hotel_bookings (PK booking_id) ----------------------------------
make_bronze(
    "rest_hotel_bookings",
    ["booking_id", "hotel_id", "customer_id", "check_in_date", "check_out_date",
     "room_nights", "adr", "amount", "currency_code", "channel", "status", "booking_date"],
    [
        ("BK-001", "HOT-001", "101", "2026-06-01", "2026-06-05", "4", "1800.00", "7200.00", "AED", "DIRECT", "CONFIRMED", "2026-05-20"),
        ("BK-002", "HOT-002", "102", "2026-06-10", "2026-06-12", "2", "1200.00", "2400.00", "AED", "OTA", "CONFIRMED", "2026-05-25"),
        ("BK-003", "HOT-003", "103", "2026-06-15", "2026-06-20", "5", "2500.00", "12500.00", "AED", "CORPORATE", "CONFIRMED", "2026-05-30"),
        ("BK-004", "HOT-004", "104", "2026-06-18", "2026-06-19", "1", "450.00", "450.00", "AED", "DIRECT", "CONFIRMED", "2026-06-01"),
        ("BK-005", "HOT-001", "105", "2026-06-22", "2026-06-25", "3", "1900.00", "5700.00", "AED", "TRAVEL_AGENT", "CONFIRMED", "2026-06-05"),
        ("BK-006", "HOT-002", "106", "2026-07-01", "2026-07-03", "2", "1300.00", "2600.00", "AED", "OTA", "CANCELLED", "2026-06-10"),
        ("BK-007", "HOT-003", "107", "2026-07-02", "2026-07-04", "2", "2600.00", "5200.00", "AED", "DIRECT", "NO_SHOW", "2026-06-15"),
        # BAD: check_out_date before check_in_date
        ("BK-BAD1", "HOT-004", "108", "2026-06-20", "2026-06-18", "2", "400.00", "800.00", "AED", "DIRECT", "CONFIRMED", "2026-06-01"),
        # BAD: negative amount
        ("BK-BAD2", "HOT-001", "101", "2026-06-01", "2026-06-05", "4", "1800.00", "-7200.00", "AED", "DIRECT", "CONFIRMED", "2026-05-20"),
    ],
    source_system="booking_rest_api", target_table="silver_hospitality.booking_clean",
    load_type="INCREMENTAL", records_rejected=2)

# ---- rest_hotel_revenue (PK revenue_id) -----------------------------------
make_bronze(
    "rest_hotel_revenue",
    ["revenue_id", "hotel_id", "revenue_date", "rooms_available", "rooms_sold",
     "room_revenue", "fnb_revenue", "currency_code", "revpar", "occupancy_rate"],
    [
        ("REV-001", "HOT-001", "2026-07-01", "1500", "1300", "2340000.00", "480000.00", "AED", "1560.00", "0.8667"),
        ("REV-002", "HOT-002", "2026-07-01", "600", "520", "624000.00", "130000.00", "AED", "1040.00", "0.8667"),
        ("REV-003", "HOT-003", "2026-07-01", "390", "350", "875000.00", "210000.00", "AED", "2243.59", "0.8974"),
        ("REV-004", "HOT-004", "2026-07-01", "420", "380", "171000.00", "25000.00", "AED", "407.14", "0.9048"),
        ("REV-005", "HOT-001", "2026-07-02", "1500", "1280", "2304000.00", "460000.00", "AED", "1536.00", "0.8533"),
        # BAD: negative room_revenue
        ("REV-BAD1", "HOT-002", "2026-07-02", "600", "500", "-600000.00", "120000.00", "AED", "1000.00", "0.8333"),
    ],
    source_system="booking_rest_api", target_table="silver_hospitality.revenue_clean",
    load_type="INCREMENTAL", records_rejected=1)

# ---- sfdc_case = guest reviews (PK review_id) -----------------------------
make_bronze(
    "sfdc_case",
    ["review_id", "hotel_id", "customer_id", "review_date", "rating", "sentiment", "category"],
    [
        ("REVW-001", "HOT-001", "101", "2026-06-06", "5", "POSITIVE", "Service"),
        ("REVW-002", "HOT-002", "102", "2026-06-13", "4", "POSITIVE", "Cleanliness"),
        ("REVW-003", "HOT-003", "103", "2026-06-21", "3", "NEUTRAL", "Location"),
        ("REVW-004", "HOT-004", "104", "2026-06-19", "2", "NEGATIVE", "Value"),
        ("REVW-005", "HOT-001", "105", "2026-06-26", "5", "POSITIVE", "Amenities"),
        ("REVW-006", "HOT-002", "106", "2026-07-04", "4", "POSITIVE", "Staff"),
        # BAD: rating out of range (> 5)
        ("REVW-BAD1", "HOT-003", "107", "2026-07-05", "9", "POSITIVE", "Service"),
    ],
    source_system="salesforce_crm", target_table="silver_hospitality.guest_review_clean",
    load_type="INCREMENTAL", records_rejected=1)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Entertainment (ticketing SFTP + campaign file) → `silver_entertainment.*`
# MAGIC Bronze: `entertainment_venues`, `sftp_ticket_sales`, `sftp_footfall`,
# MAGIC `campaign_file` (composite PK `campaign_id` + `campaign_date`).

# COMMAND ----------
# ---- entertainment_venues (PK venue_id) -----------------------------------
make_bronze(
    "entertainment_venues",
    ["venue_id", "venue_name", "venue_type", "city", "emirate", "capacity"],
    [
        ("VEN-001", "IMG Worlds of Adventure", "THEME_PARK", "Dubai", "Dubai", "20000"),
        ("VEN-002", "Reel Cinemas Dubai Mall", "CINEMA", "Dubai", "Dubai", "2500"),
        ("VEN-003", "Coca-Cola Arena", "ARENA", "Dubai", "Dubai", "17000"),
        ("VEN-004", "Wild Wadi Waterpark", "WATERPARK", "Dubai", "Dubai", "8000"),
        # BAD: invalid venue_type 'CASINO'
        ("VEN-BAD1", "Desert Casino", "CASINO", "Abu Dhabi", "Abu Dhabi", "5000"),
    ],
    source_system="entertainment_ops", target_table="silver_entertainment.venue_clean",
    load_type="FULL_SNAPSHOT", records_rejected=1)

# ---- sftp_ticket_sales (PK ticket_id) -------------------------------------
make_bronze(
    "sftp_ticket_sales",
    ["ticket_id", "venue_id", "event_id", "quantity", "amount", "currency_code", "sale_date"],
    [
        ("TKT-001", "VEN-001", "EVT-001", "4", "600.00", "AED", "2026-06-15"),
        ("TKT-002", "VEN-002", "EVT-002", "2", "120.00", "AED", "2026-06-16"),
        ("TKT-003", "VEN-003", "EVT-003", "6", "1800.00", "AED", "2026-06-18"),
        ("TKT-004", "VEN-004", "EVT-004", "3", "450.00", "AED", "2026-06-20"),
        ("TKT-005", "VEN-001", "EVT-005", "8", "1200.00", "AED", "2026-06-22"),
        ("TKT-006", "VEN-003", "EVT-006", "10", "3000.00", "AED", "2026-06-25"),
        # BAD: invalid currency 'XYZ'
        ("TKT-BAD1", "VEN-002", "EVT-007", "2", "240.00", "XYZ", "2026-06-26"),
        # BAD: negative quantity
        ("TKT-BAD2", "VEN-004", "EVT-008", "-3", "450.00", "AED", "2026-06-27"),
    ],
    source_system="ticketing_sftp", target_table="silver_entertainment.ticket_sales_clean",
    load_type="INCREMENTAL", records_rejected=2)

# ---- sftp_footfall (PK footfall_id) ---------------------------------------
make_bronze(
    "sftp_footfall",
    ["footfall_id", "venue_id", "gate", "visitors", "footfall_date"],
    [
        ("FF-001", "VEN-001", "North", "12000", "2026-07-01"),
        ("FF-002", "VEN-002", "Main", "1800", "2026-07-01"),
        ("FF-003", "VEN-003", "East", "9500", "2026-07-01"),
        ("FF-004", "VEN-004", "West", "6000", "2026-07-01"),
        ("FF-005", "VEN-001", "South", "11000", "2026-07-02"),
        ("FF-006", "VEN-003", "West", "8800", "2026-07-02"),
        # BAD: negative visitors
        ("FF-BAD1", "VEN-002", "Main", "-500", "2026-07-02"),
    ],
    source_system="ticketing_sftp", target_table="silver_entertainment.footfall_clean",
    load_type="INCREMENTAL", records_rejected=1)

# ---- campaign_file (composite PK campaign_id + campaign_date) -------------
make_bronze(
    "campaign_file",
    ["campaign_id", "venue_id", "channel", "spend", "impressions", "clicks",
     "conversions", "currency_code", "campaign_date"],
    [
        ("CMP-001", "VEN-001", "SOCIAL", "50000.00", "1000000", "40000", "2000", "AED", "2026-06-01"),
        ("CMP-002", "VEN-002", "SEARCH", "20000.00", "500000", "25000", "1500", "AED", "2026-06-01"),
        ("CMP-003", "VEN-003", "EMAIL", "8000.00", "200000", "12000", "900", "AED", "2026-06-05"),
        ("CMP-004", "VEN-004", "OOH", "35000.00", "800000", "0", "0", "AED", "2026-06-10"),
        ("CMP-005", "VEN-001", "INFLUENCER", "60000.00", "1500000", "55000", "3000", "AED", "2026-06-15"),
        # BAD: clicks (150000) > impressions (100000)
        ("CMP-BAD1", "VEN-002", "SEARCH", "15000.00", "100000", "150000", "500", "AED", "2026-06-20"),
    ],
    source_system="marketing_campaigns", target_table="silver_entertainment.campaign_roi_clean",
    load_type="INCREMENTAL", records_rejected=1)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Investment / treasury (SQL Server) → `silver_investment.*`
# MAGIC Bronze: `sqlserver_assets`, `sqlserver_asset_performance`,
# MAGIC `sqlserver_risk_exposure`, `sqlserver_cashflow`.

# COMMAND ----------
# ---- sqlserver_assets (PK asset_id) ---------------------------------------
make_bronze(
    "sqlserver_assets",
    ["asset_id", "asset_name", "asset_class", "sector", "currency_code", "inception_date"],
    [
        ("AST-001", "DIFC Office Tower Fund", "REAL_ESTATE", "Real Estate", "AED", "2015-01-01"),
        ("AST-002", "Emaar Equity Holding", "EQUITY", "Consumer", "AED", "2016-03-01"),
        ("AST-003", "GCC Sovereign Bond", "FIXED_INCOME", "Government", "USD", "2017-06-01"),
        ("AST-004", "Tech Growth PE Fund", "PRIVATE_EQUITY", "Technology", "USD", "2018-09-01"),
        ("AST-005", "Dubai Metro Infra", "INFRASTRUCTURE", "Transport", "AED", "2014-01-01"),
        ("AST-006", "EU Bluechip Equity", "EQUITY", "Industrials", "EUR", "2019-02-01"),
        # BAD: invalid asset_class 'CRYPTO'
        ("AST-BAD1", "Crypto Basket", "CRYPTO", "Digital", "AED", "2021-01-01"),
    ],
    source_system="sqlserver_treasury", target_table="silver_investment.asset_clean",
    load_type="FULL_SNAPSHOT", records_rejected=1)

# ---- sqlserver_asset_performance (PK performance_id) ----------------------
make_bronze(
    "sqlserver_asset_performance",
    ["performance_id", "asset_id", "as_of_date", "nav", "mtd_return", "ytd_return", "benchmark_return"],
    [
        ("PERF-001", "AST-001", "2026-06-30", "250000000.00", "0.0120", "0.0850", "0.0700"),
        ("PERF-002", "AST-002", "2026-06-30", "120000000.00", "0.0200", "0.1100", "0.0950"),
        ("PERF-003", "AST-003", "2026-06-30", "80000000.00", "0.0050", "0.0300", "0.0280"),
        ("PERF-004", "AST-004", "2026-06-30", "150000000.00", "0.0300", "0.1800", "0.1200"),
        ("PERF-005", "AST-005", "2026-06-30", "300000000.00", "0.0080", "0.0600", "0.0550"),
        ("PERF-006", "AST-006", "2026-06-30", "95000000.00", "0.0150", "0.0750", "0.0680"),
        # BAD: negative nav
        ("PERF-BAD1", "AST-001", "2026-06-30", "-100.00", "0.0100", "0.0500", "0.0400"),
    ],
    source_system="sqlserver_treasury", target_table="silver_investment.asset_performance_clean",
    load_type="INCREMENTAL", records_rejected=1)

# ---- sqlserver_risk_exposure (PK exposure_id) -----------------------------
make_bronze(
    "sqlserver_risk_exposure",
    ["exposure_id", "asset_id", "as_of_date", "var_95", "volatility",
     "concentration_pct", "risk_rating", "risk_threshold_breached"],
    [
        ("RISK-001", "AST-001", "2026-06-30", "5000000.00", "0.1200", "0.2500", "MEDIUM", "false"),
        ("RISK-002", "AST-002", "2026-06-30", "8000000.00", "0.2200", "0.1500", "HIGH", "true"),
        ("RISK-003", "AST-003", "2026-06-30", "1500000.00", "0.0500", "0.1000", "LOW", "false"),
        ("RISK-004", "AST-004", "2026-06-30", "12000000.00", "0.3000", "0.2000", "HIGH", "true"),
        ("RISK-005", "AST-005", "2026-06-30", "4000000.00", "0.1000", "0.3000", "MEDIUM", "false"),
        # BAD: volatility > 1
        ("RISK-BAD1", "AST-006", "2026-06-30", "3000000.00", "1.5000", "0.1200", "MEDIUM", "false"),
    ],
    source_system="sqlserver_treasury", target_table="silver_investment.risk_exposure_clean",
    load_type="INCREMENTAL", records_rejected=1)

# ---- sqlserver_cashflow (PK cashflow_id) ----------------------------------
make_bronze(
    "sqlserver_cashflow",
    ["cashflow_id", "asset_id", "value_date", "cashflow_type", "amount", "currency_code"],
    [
        ("CF-001", "AST-001", "2026-06-15", "DISTRIBUTION", "2000000.00", "AED"),
        ("CF-002", "AST-002", "2026-06-20", "DIVIDEND", "500000.00", "AED"),
        ("CF-003", "AST-003", "2026-06-25", "COUPON", "300000.00", "USD"),
        ("CF-004", "AST-004", "2026-06-28", "CAPITAL_CALL", "5000000.00", "USD"),
        ("CF-005", "AST-005", "2026-06-30", "DISTRIBUTION", "1500000.00", "AED"),
        # BAD: invalid cashflow_type 'INTEREST'
        ("CF-BAD1", "AST-006", "2026-06-30", "INTEREST", "100000.00", "EUR"),
    ],
    source_system="sqlserver_treasury", target_table="silver_investment.cashflow_clean",
    load_type="INCREMENTAL", records_rejected=1)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Customer / CRM (Salesforce) → `silver_customer.*`
# MAGIC Bronze: `sfdc_account`, `sfdc_contact`, `sfdc_opportunity`. Contacts carry
# MAGIC `customer_id` 101-108 → the shared customer dimension.

# COMMAND ----------
# ---- sfdc_account (PK account_id) -----------------------------------------
make_bronze(
    "sfdc_account",
    ["account_id", "account_name", "segment", "industry", "region"],
    [
        ("ACC-001", "Al Habtoor Group", "CORPORATE", "Conglomerate", "Dubai"),
        ("ACC-002", "Sheikh Family Office", "HNW", "Wealth", "Abu Dhabi"),
        ("ACC-003", "Emirates NBD Retail", "RETAIL", "Banking", "Dubai"),
        ("ACC-004", "Dubai Tourism Board", "GOVERNMENT", "Public", "Dubai"),
        ("ACC-005", "GCC Traveller Group", "TOURIST", "Travel", "Dubai"),
        ("ACC-006", "Etisalat Corporate", "CORPORATE", "Telecom", "Abu Dhabi"),
        # BAD: invalid segment 'VIP'
        ("ACC-BAD1", "Mystery LLC", "VIP", "Unknown", "Sharjah"),
    ],
    source_system="salesforce_crm", target_table="silver_customer.account_clean",
    load_type="FULL_SNAPSHOT", records_rejected=1)

# ---- sfdc_contact (PK contact_id) -----------------------------------------
make_bronze(
    "sfdc_contact",
    ["contact_id", "account_id", "customer_id", "full_name", "email", "phone", "country", "is_active"],
    [
        ("CON-001", "ACC-001", "101", "Ahmed Al Habtoor", "ahmed@habtoor.ae", "+971501111111", "UAE", "true"),
        ("CON-002", "ACC-002", "102", "Fatima Al Nahyan", "fatima@familyoffice.ae", "+971502222222", "UAE", "true"),
        ("CON-003", "ACC-003", "103", "Rajesh Kumar", "rajesh@enbd.ae", "+971503333333", "India", "true"),
        ("CON-004", "ACC-004", "104", "Sara Johnson", "sara@dubaitourism.ae", "+971504444444", "UK", "true"),
        ("CON-005", "ACC-005", "105", "Mohammed Ali", "mohammed@traveller.com", "+971505555555", "Saudi Arabia", "true"),
        ("CON-006", "ACC-006", "106", "Layla Hassan", "layla@etisalat.ae", "+971506666666", "UAE", "false"),
        ("CON-007", "ACC-001", "107", "John Smith", "john@habtoor.ae", "+971507777777", "USA", "true"),
        ("CON-008", "ACC-002", "108", "Priya Nair", "priya@familyoffice.ae", "+971508888888", "India", "true"),
        # BAD: null account_id (mandatory FK missing)
        ("CON-BAD1", None, "101", "Ghost Contact", "ghost@example.com", "+971500000000", "UAE", "true"),
    ],
    source_system="salesforce_crm", target_table="silver_customer.contact_clean",
    load_type="INCREMENTAL", records_rejected=1)

# ---- sfdc_opportunity (PK opportunity_id) ---------------------------------
make_bronze(
    "sfdc_opportunity",
    ["opportunity_id", "account_id", "name", "stage", "amount", "currency_code", "close_date", "is_won"],
    [
        ("OPP-001", "ACC-001", "Tower Lease Renewal", "CLOSED_WON", "5000000.00", "AED", "2026-06-01", "true"),
        ("OPP-002", "ACC-002", "Wealth Mandate", "NEGOTIATION", "20000000.00", "AED", "2026-08-01", "false"),
        ("OPP-003", "ACC-003", "Retail Expansion", "PROPOSAL", "3000000.00", "AED", "2026-09-01", "false"),
        ("OPP-004", "ACC-004", "Tourism Package", "CLOSED_LOST", "1500000.00", "AED", "2026-05-15", "false"),
        ("OPP-005", "ACC-005", "Group Travel Deal", "QUALIFICATION", "800000.00", "AED", "2026-10-01", "false"),
        ("OPP-006", "ACC-006", "Telecom Services", "PROSPECTING", "2500000.00", "AED", "2026-11-01", "false"),
        # BAD: invalid stage 'DISCOVERY'
        ("OPP-BAD1", "ACC-001", "Bad Deal", "DISCOVERY", "1000000.00", "AED", "2026-07-01", "false"),
    ],
    source_system="salesforce_crm", target_table="silver_customer.opportunity_clean",
    load_type="INCREMENTAL", records_rejected=1)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Shared customer SCD2 dimension — `silver_cdc.customer_scd2`
# MAGIC Written **directly** (skipping the Bronze CDC + `silver_customer_scd2` MERGE) so
# MAGIC the demo runs without a Debezium feed. Columns match what
# MAGIC `dbt gold.dim_customer_history` reads. Customers 101-108; note:
# MAGIC - **103** has a historical version (`is_current=false`, nationality India→UAE),
# MAGIC - **106** is soft-deleted (`is_deleted=true`).

# COMMAND ----------
from datetime import datetime
from pyspark.sql.types import BooleanType, TimestampType


def _ts(s):
    return None if s is None else datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


scd2_cols = ["customer_id", "customer_name", "email", "phone_number", "nationality",
             "status", "source_system", "effective_from", "effective_to",
             "is_current", "is_deleted"]
scd2_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("status", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("effective_from", TimestampType(), True),
    StructField("effective_to", TimestampType(), True),
    StructField("is_current", BooleanType(), True),
    StructField("is_deleted", BooleanType(), True),
])

scd2_rows = [
    ("101", "Ahmed Al Habtoor", "ahmed@habtoor.ae", "+971501111111", "UAE", "ACTIVE",
     "salesforce_crm", _ts("2023-01-01 00:00:00"), None, True, False),
    ("102", "Fatima Al Nahyan", "fatima@familyoffice.ae", "+971502222222", "UAE", "ACTIVE",
     "salesforce_crm", _ts("2023-01-01 00:00:00"), None, True, False),
    # 103 — historical version (naturalised India -> UAE): expired, not current
    ("103", "Rajesh Kumar", "rajesh@enbd.ae", "+971503333333", "India", "ACTIVE",
     "salesforce_crm", _ts("2022-01-01 00:00:00"), _ts("2024-06-01 00:00:00"), False, False),
    # 103 — current version
    ("103", "Rajesh Kumar", "rajesh@enbd.ae", "+971503333333", "UAE", "ACTIVE",
     "salesforce_crm", _ts("2024-06-01 00:00:00"), None, True, False),
    ("104", "Sara Johnson", "sara@dubaitourism.ae", "+971504444444", "UK", "ACTIVE",
     "salesforce_crm", _ts("2023-03-01 00:00:00"), None, True, False),
    ("105", "Mohammed Ali", "mohammed@traveller.com", "+971505555555", "Saudi Arabia", "ACTIVE",
     "salesforce_crm", _ts("2023-05-01 00:00:00"), None, True, False),
    # 106 — soft-deleted (still the current version, flagged deleted)
    ("106", "Layla Hassan", "layla@etisalat.ae", "+971506666666", "UAE", "INACTIVE",
     "salesforce_crm", _ts("2023-06-01 00:00:00"), None, True, True),
    ("107", "John Smith", "john@habtoor.ae", "+971507777777", "USA", "ACTIVE",
     "salesforce_crm", _ts("2023-07-01 00:00:00"), None, True, False),
    ("108", "Priya Nair", "priya@familyoffice.ae", "+971508888888", "India", "ACTIVE",
     "salesforce_crm", _ts("2023-08-01 00:00:00"), None, True, False),
]

scd2_df = spark.createDataFrame(scd2_rows, scd2_schema)
scd2_target = f"{catalog}.silver_cdc.customer_scd2"
(scd2_df.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(scd2_target))
print(f"scd2    {scd2_target}  rows={len(scd2_rows)} (101-108; 103 has history, 106 soft-deleted)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10. Control tables — `silver_control.pipeline_run` + `table_load_status`
# MAGIC These drive the `gold_ops_trust` marts (`mart_pipeline_status`,
# MAGIC `mart_source_freshness`, `mart_dq_gate_status`). `table_load_status` is seeded
# MAGIC from the Bronze load registry, so `source_system` / `source_table` / `run_id`
# MAGIC line up with the rows the conformers will quarantine into
# MAGIC `silver_quarantine.failed_records`. Column names mirror
# MAGIC `src/payments_platform/config/control_tables.py`.

# COMMAND ----------
from datetime import timedelta
from pyspark.sql.types import IntegerType

_now = datetime.now()
_load_start = _now - timedelta(minutes=40)   # recent -> comfortably within the 26h SLA
WATERMARK = "2026-07-05"
JOB_NAME = "investsphere_enterprise_medallion"

# ---- pipeline_run (include one PARTIAL run) -------------------------------
pipeline_run_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("run_date", StringType(), True),
    StructField("environment", StringType(), True),
    StructField("job_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("triggered_by", StringType(), True),
    StructField("error_message", StringType(), True),
])
pipeline_run_rows = [
    # yesterday's clean run
    ("demo_run_prev", "2026-07-04", "dev", JOB_NAME, "SUCCESS",
     _ts("2026-07-04 06:00:00"), _ts("2026-07-04 06:45:00"), "service_principal", None),
    # today's run — PARTIAL because the DQ gate quarantined records above the warning threshold
    (run_id, _now.strftime("%Y-%m-%d"), "dev", JOB_NAME, "PARTIAL",
     _load_start, _now, "service_principal",
     "Silver DQ gate quarantined records above the warning threshold "
     "(e.g. oracle_leases 25%); downstream marts built on the passing rows."),
]
pr_df = spark.createDataFrame(pipeline_run_rows, pipeline_run_schema)
(pr_df.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver_control.pipeline_run"))
print(f"control {catalog}.silver_control.pipeline_run  rows={len(pipeline_run_rows)} (1 SUCCESS, 1 PARTIAL)")

# ---- table_load_status (one row per Bronze source, from the load registry) --
tls_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("target_table", StringType(), True),
    StructField("load_type", StringType(), True),
    StructField("records_read", IntegerType(), True),
    StructField("records_written", IntegerType(), True),
    StructField("records_rejected", IntegerType(), True),
    StructField("corrupt_record_count", IntegerType(), True),
    StructField("watermark_value", StringType(), True),
    StructField("status", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
])
tls_rows = []
for s in LOAD_STATS:
    # a source whose reject rate crosses 5% is flagged PARTIAL (drives dq_gate_passed=false)
    partial = s["records_rejected"] / max(s["records_read"], 1) >= 0.05
    tls_rows.append((
        run_id, s["source_system"], s["source_table"], s["target_table"],
        s["load_type"], s["records_read"], s["records_written"], s["records_rejected"],
        0, WATERMARK, "PARTIAL" if partial else "SUCCESS", None, _load_start, _now,
    ))
tls_df = spark.createDataFrame(tls_rows, tls_schema)
(tls_df.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver_control.table_load_status"))
print(f"control {catalog}.silver_control.table_load_status  rows={len(tls_rows)} (one per Bronze source)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 11. What was created
# MAGIC
# MAGIC **Bronze (19 tables)** — raw string business columns + audit contract
# MAGIC (`source_system`, `run_id`, `ingestion_timestamp`, `record_hash`):
# MAGIC
# MAGIC | Domain | Bronze tables |
# MAGIC |---|---|
# MAGIC | Real estate | `oracle_properties`, `oracle_leases`, `oracle_occupancy_daily`, `oracle_maintenance_orders` |
# MAGIC | Hospitality | `rest_hotels`, `rest_hotel_bookings`, `rest_hotel_revenue`, `sfdc_case` |
# MAGIC | Entertainment | `entertainment_venues`, `sftp_ticket_sales`, `sftp_footfall`, `campaign_file` |
# MAGIC | Investment | `sqlserver_assets`, `sqlserver_asset_performance`, `sqlserver_risk_exposure`, `sqlserver_cashflow` |
# MAGIC | Customer | `sfdc_account`, `sfdc_contact`, `sfdc_opportunity` |
# MAGIC
# MAGIC **Shared / control:** `silver_cdc.customer_scd2` (SCD2, 101-108; 103 has history,
# MAGIC 106 soft-deleted), `silver_control.pipeline_run` (1 SUCCESS + 1 PARTIAL),
# MAGIC `silver_control.table_load_status` (one row per source).
# MAGIC
# MAGIC **Intentionally DQ-failing rows** (each marked `# BAD:` above) so the DQ gate,
# MAGIC `silver_quarantine.failed_records`, and the `gold_ops_trust` trust score have
# MAGIC signal: invalid property_type, null PK (property), invalid currency `XYZ`,
# MAGIC negative rent, occupancy_rate > 1, negative maintenance cost, star_rating > 5,
# MAGIC check-out before check-in, negative booking amount, negative room_revenue,
# MAGIC rating out of range, invalid venue_type `CASINO`, negative quantity, negative
# MAGIC visitors, clicks > impressions, invalid asset_class `CRYPTO`, negative NAV,
# MAGIC volatility > 1, invalid cashflow_type, invalid segment `VIP`, null account_id,
# MAGIC invalid opportunity stage.
# MAGIC
# MAGIC ### Next steps
# MAGIC 1. Run the six Silver conformers (each `run(catalog, run_id)`), e.g.:
# MAGIC    ```python
# MAGIC    from payments_platform.databricks import (
# MAGIC        silver_realestate, silver_hospitality, silver_entertainment,
# MAGIC        silver_investment, silver_customer)
# MAGIC    for m in (silver_realestate, silver_hospitality, silver_entertainment,
# MAGIC              silver_investment, silver_customer):
# MAGIC        m.run(catalog="investsphere_dev", run_id="demo_run_001")
# MAGIC    ```
# MAGIC    (Bad rows land in `silver_quarantine.failed_records`; clean rows MERGE into the
# MAGIC    `silver_*` tables. `customer_scd2` is already populated by this notebook.)
# MAGIC 2. `dbt build --vars '{catalog: investsphere_dev}'` to build the Gold marts,
# MAGIC    including `gold_ops_trust`.

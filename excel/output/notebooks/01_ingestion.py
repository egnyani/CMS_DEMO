# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 1 — CMS Demo Dataset Ingestion
# MAGIC
# MAGIC Reads 7 CSV files with explicit schemas and writes them as Delta tables
# MAGIC into the configured Unity Catalog location. Safe to rerun (full overwrite).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog name")
dbutils.widgets.text("schema", "default", "Schema (database) name")
dbutils.widgets.text("source_path", "/Volumes/workspace/default/landing/", "DBFS/Volumes path containing CSVs")
dbutils.widgets.text("table_prefix", "", "Optional prefix for table names (e.g. 'dev_')")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_path = dbutils.widgets.get("source_path").rstrip("/")
table_prefix = dbutils.widgets.get("table_prefix")

print(f"catalog       = {catalog}")
print(f"schema        = {schema}")
print(f"source_path   = {source_path}")
print(f"table_prefix  = {table_prefix}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Ensure catalog and schema exist

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Explicit Spark schemas

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, IntegerType, LongType,
    StringType, DateType, TimestampType, DoubleType, BooleanType,
)

schema_dim_medicaid_recipient = StructType([
    StructField("medicaid_recipient_key", IntegerType(), False),
    StructField("msis_identification_num", StringType(), False),
    StructField("submitting_state_code", StringType(), True),
    StructField("eligible_state_code", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("sex_code", StringType(), True),
    StructField("enrollment_type_code", StringType(), True),
    StructField("registration_date", DateType(), True),
])

schema_dim_establishment = StructType([
    StructField("establishment_key", IntegerType(), False),
    StructField("submitting_state_prov_id", StringType(), True),
    StructField("prov_location_id", StringType(), True),
    StructField("service_facility_org_npi", StringType(), True),
    StructField("establishment_name", StringType(), True),
    StructField("facility_group_individual_code", StringType(), True),
    StructField("provider_classification_type", StringType(), True),
    StructField("provider_classification_code", StringType(), True),
    StructField("establishment_type", StringType(), True),
    StructField("establishment_state_code", StringType(), True),
])

schema_dim_calendar_month = StructType([
    StructField("calendar_month_key", IntegerType(), False),
    StructField("month_start_date", DateType(), True),
    StructField("month_end_date", DateType(), True),
    StructField("year_num", IntegerType(), True),
    StructField("month_num", IntegerType(), True),
    StructField("month_name", StringType(), True),
])

schema_dim_verification_method = StructType([
    StructField("verification_method_key", IntegerType(), False),
    StructField("verification_method", StringType(), True),
    StructField("verification_method_description", StringType(), True),
])

schema_fact_engagement_activity = StructType([
    StructField("engagement_activity_key", LongType(), False),
    StructField("medicaid_recipient_key", IntegerType(), False),
    StructField("establishment_key", IntegerType(), False),
    StructField("calendar_month_key", IntegerType(), False),
    StructField("verification_method_key", IntegerType(), False),
    StructField("icn_orig", StringType(), True),
    StructField("icn_adj", StringType(), True),
    StructField("line_num_orig", IntegerType(), True),
    StructField("line_num_adj", IntegerType(), True),
    StructField("source_record_id", StringType(), True),
    StructField("source_record_number", StringType(), True),
    StructField("claim_submitting_state_code", StringType(), True),
    StructField("beginning_date_of_service", DateType(), True),
    StructField("ending_date_of_service", DateType(), True),
    StructField("activity_type", StringType(), True),
    StructField("activity_start_ts", TimestampType(), True),
    StructField("activity_end_ts", TimestampType(), True),
    StructField("activity_duration_hours", DoubleType(), True),
    StructField("is_overlap_flag", BooleanType(), True),
])

schema_fact_recipient_monthly = StructType([
    StructField("medicaid_recipient_key", IntegerType(), False),
    StructField("calendar_month_key", IntegerType(), False),
    StructField("activity_count", IntegerType(), True),
    StructField("establishment_count", IntegerType(), True),
    StructField("age_group", StringType(), True),
    StructField("monthly_hours_completed", DoubleType(), True),
    StructField("required_monthly_hours", IntegerType(), True),
    StructField("is_exactly_80_hours_flag", BooleanType(), True),
    StructField("exact_80_consecutive_month_count", IntegerType(), True),
    StructField("is_exactly_80_hours_consecutive_flag", BooleanType(), True),
    StructField("is_single_establishment_dominant_flag", BooleanType(), True),
])

schema_fact_recipient_establishment_monthly = StructType([
    StructField("medicaid_recipient_key", IntegerType(), False),
    StructField("establishment_key", IntegerType(), False),
    StructField("calendar_month_key", IntegerType(), False),
    StructField("activity_count", IntegerType(), True),
    StructField("monthly_hours_completed", DoubleType(), True),
    StructField("establishment_share_of_monthly_hours", DoubleType(), True),
    StructField("is_dominant_establishment_for_month", BooleanType(), True),
    StructField("dominance_rank_in_month", IntegerType(), True),
])

print("All 7 schemas defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Table manifest (file → schema → target table)

# COMMAND ----------

TABLE_MANIFEST = [
    ("dim_medicaid_recipient",              schema_dim_medicaid_recipient,              10000),
    ("dim_establishment",                   schema_dim_establishment,                   500),
    ("dim_calendar_month",                  schema_dim_calendar_month,                  6),
    ("dim_verification_method",             schema_dim_verification_method,             3),
    ("fact_engagement_activity",            schema_fact_engagement_activity,            99369),
    ("fact_recipient_monthly_engagement",   schema_fact_recipient_monthly,              36097),
    ("fact_recipient_establishment_monthly", schema_fact_recipient_establishment_monthly, 89958),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Read CSVs and write Delta tables

# COMMAND ----------

from pyspark.sql.functions import col

results = []

for table_name, spark_schema, expected_rows in TABLE_MANIFEST:
    csv_path = f"{source_path}/{table_name}.csv"
    fq_table = f"{catalog}.{schema}.{table_prefix}{table_name}"

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(spark_schema)
        .load(csv_path)
    )

    actual_rows = df.count()

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(fq_table)
    )

    results.append((table_name, expected_rows, actual_rows, fq_table))
    print(f"✔ {fq_table}: wrote {actual_rows} rows (expected {expected_rows})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Validation: row counts

# COMMAND ----------

print(f"{'Table':<45} {'Expected':>10} {'Ingested':>10} {'Delta':>10} {'Match':>6}")
print("-" * 90)

all_passed = True

for table_name, expected_rows, ingested_rows, fq_table in results:
    delta_count = spark.table(fq_table).count()
    match = (expected_rows == ingested_rows == delta_count)
    all_passed = all_passed and match
    status = "OK" if match else "FAIL"
    print(f"{fq_table:<45} {expected_rows:>10} {ingested_rows:>10} {delta_count:>10} {status:>6}")

assert all_passed, "Row count mismatch detected — review output above."
print("\nAll row counts match.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — Validation: null checks on primary / foreign keys

# COMMAND ----------

KEY_CHECKS = {
    "dim_medicaid_recipient":              ["medicaid_recipient_key", "msis_identification_num"],
    "dim_establishment":                   ["establishment_key"],
    "dim_calendar_month":                  ["calendar_month_key"],
    "dim_verification_method":             ["verification_method_key"],
    "fact_engagement_activity":            ["engagement_activity_key", "medicaid_recipient_key",
                                            "establishment_key", "calendar_month_key",
                                            "verification_method_key"],
    "fact_recipient_monthly_engagement":   ["medicaid_recipient_key", "calendar_month_key"],
    "fact_recipient_establishment_monthly": ["medicaid_recipient_key", "establishment_key",
                                             "calendar_month_key"],
}

null_issues = []

for table_name, columns in KEY_CHECKS.items():
    fq_table = f"{catalog}.{schema}.{table_prefix}{table_name}"
    df = spark.table(fq_table)
    for c in columns:
        null_count = df.filter(col(c).isNull()).count()
        if null_count > 0:
            null_issues.append((fq_table, c, null_count))
            print(f"⚠ {fq_table}.{c} has {null_count} NULLs")

if null_issues:
    raise AssertionError(f"Null key violations found: {null_issues}")
else:
    print("All key columns are non-null. Null check passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 — Validation: referential integrity

# COMMAND ----------

ri_sql = f"""
SELECT
  (SELECT COUNT(*) FROM {catalog}.{schema}.{table_prefix}fact_engagement_activity f
   LEFT JOIN {catalog}.{schema}.{table_prefix}dim_medicaid_recipient d
     ON f.medicaid_recipient_key = d.medicaid_recipient_key
   WHERE d.medicaid_recipient_key IS NULL) AS orphan_recipients,

  (SELECT COUNT(*) FROM {catalog}.{schema}.{table_prefix}fact_engagement_activity f
   LEFT JOIN {catalog}.{schema}.{table_prefix}dim_establishment d
     ON f.establishment_key = d.establishment_key
   WHERE d.establishment_key IS NULL) AS orphan_establishments,

  (SELECT COUNT(*) FROM {catalog}.{schema}.{table_prefix}fact_engagement_activity f
   LEFT JOIN {catalog}.{schema}.{table_prefix}dim_calendar_month d
     ON f.calendar_month_key = d.calendar_month_key
   WHERE d.calendar_month_key IS NULL) AS orphan_months,

  (SELECT COUNT(*) FROM {catalog}.{schema}.{table_prefix}fact_engagement_activity f
   LEFT JOIN {catalog}.{schema}.{table_prefix}dim_verification_method d
     ON f.verification_method_key = d.verification_method_key
   WHERE d.verification_method_key IS NULL) AS orphan_verification
"""

ri_row = spark.sql(ri_sql).collect()[0]
issues = {k: v for k, v in ri_row.asDict().items() if v > 0}

if issues:
    raise AssertionError(f"Referential integrity violations: {issues}")
else:
    print("Referential integrity check passed — zero orphan keys.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 — Validation: confirm tables exist in catalog

# COMMAND ----------

existing_tables = [
    r.tableName
    for r in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
]

for table_name, _, _, _ in results:
    full_name = f"{table_prefix}{table_name}"
    assert full_name in existing_tables, f"Table {full_name} not found in {catalog}.{schema}"

print(f"All {len(results)} tables confirmed in {catalog}.{schema}.")
print("\n=== Ingestion notebook complete ===")

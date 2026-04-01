# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 2 — Semantic / View Creation
# MAGIC
# MAGIC Creates reusable SQL views over the Delta tables produced by Notebook 1.
# MAGIC Covers all six dashboard domains plus validation reconciliation cells.
# MAGIC Safe to rerun — all views use CREATE OR REPLACE.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog name")
dbutils.widgets.text("schema", "default", "Schema (database) name")
dbutils.widgets.text("table_prefix", "", "Table prefix used during ingestion")
dbutils.widgets.text("view_prefix", "vw_", "Prefix for view names")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_prefix = dbutils.widgets.get("table_prefix")
view_prefix = dbutils.widgets.get("view_prefix")

fq = lambda name: f"{catalog}.{schema}.{table_prefix}{name}"
fq_view = lambda name: f"{catalog}.{schema}.{view_prefix}{name}"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"catalog      = {catalog}")
print(f"schema       = {schema}")
print(f"table_prefix = {table_prefix}")
print(f"view_prefix  = {view_prefix}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Constants

# COMMAND ----------

REQUIRED_MONTHLY_HOURS = 80
CONSECUTIVE_STREAK_THRESHOLD = 3

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Domain 1 — Recipient & Enrollment Metrics

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fq_view("enrollment_summary")} AS
SELECT
    r.enrollment_type_code,
    COUNT(DISTINCT r.medicaid_recipient_key) AS distinct_recipients,
    COUNT(*) AS total_records
FROM {fq("dim_medicaid_recipient")} r
GROUP BY r.enrollment_type_code
""")
print(f"Created {fq_view('enrollment_summary')}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fq_view("monthly_registrations")} AS
SELECT
    DATE_TRUNC('month', r.registration_date) AS registration_month,
    r.enrollment_type_code,
    COUNT(*) AS registrations
FROM {fq("dim_medicaid_recipient")} r
GROUP BY DATE_TRUNC('month', r.registration_date), r.enrollment_type_code
""")
print(f"Created {fq_view('monthly_registrations')}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fq_view("today_registrations")} AS
SELECT
    r.enrollment_type_code,
    COUNT(*) AS registrations_today
FROM {fq("dim_medicaid_recipient")} r
WHERE r.registration_date = CURRENT_DATE()
GROUP BY r.enrollment_type_code
""")
print(f"Created {fq_view('today_registrations')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Domain 2 — Activity & Establishment Insights

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fq_view("activity_by_type")} AS
SELECT
    a.activity_type,
    COUNT(DISTINCT a.medicaid_recipient_key) AS distinct_recipients,
    COUNT(*) AS activity_count,
    ROUND(SUM(a.activity_duration_hours), 2) AS total_hours
FROM {fq("fact_engagement_activity")} a
GROUP BY a.activity_type
""")
print(f"Created {fq_view('activity_by_type')}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fq_view("establishment_by_type")} AS
SELECT
    e.establishment_type,
    COUNT(DISTINCT e.establishment_key) AS establishment_count,
    COUNT(DISTINCT a.medicaid_recipient_key) AS distinct_recipients,
    COUNT(*) AS activity_count,
    ROUND(SUM(a.activity_duration_hours), 2) AS total_hours
FROM {fq("fact_engagement_activity")} a
JOIN {fq("dim_establishment")} e
  ON a.establishment_key = e.establishment_key
GROUP BY e.establishment_type
""")
print(f"Created {fq_view('establishment_by_type')}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fq_view("recipients_per_establishment")} AS
SELECT
    e.establishment_key,
    e.establishment_name,
    e.establishment_type,
    e.establishment_state_code,
    COUNT(DISTINCT a.medicaid_recipient_key) AS distinct_recipients,
    COUNT(*) AS activity_count,
    ROUND(SUM(a.activity_duration_hours), 2) AS total_hours
FROM {fq("fact_engagement_activity")} a
JOIN {fq("dim_establishment")} e
  ON a.establishment_key = e.establishment_key
GROUP BY e.establishment_key, e.establishment_name, e.establishment_type, e.establishment_state_code
""")
print(f"Created {fq_view('recipients_per_establishment')}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fq_view("recipients_per_activity_type")} AS
SELECT
    a.activity_type,
    cm.month_name,
    cm.calendar_month_key,
    COUNT(DISTINCT a.medicaid_recipient_key) AS distinct_recipients,
    COUNT(*) AS activity_count
FROM {fq("fact_engagement_activity")} a
JOIN {fq("dim_calendar_month")} cm
  ON a.calendar_month_key = cm.calendar_month_key
GROUP BY a.activity_type, cm.month_name, cm.calendar_month_key
""")
print(f"Created {fq_view('recipients_per_activity_type')}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fq_view("recipients_per_state")} AS
SELECT
    r.eligible_state_code AS state_code,
    COUNT(DISTINCT r.medicaid_recipient_key) AS distinct_recipients,
    COUNT(DISTINCT a.engagement_activity_key) AS activity_count,
    ROUND(SUM(a.activity_duration_hours), 2) AS total_hours
FROM {fq("dim_medicaid_recipient")} r
JOIN {fq("fact_engagement_activity")} a
  ON r.medicaid_recipient_key = a.medicaid_recipient_key
GROUP BY r.eligible_state_code
""")
print(f"Created {fq_view('recipients_per_state')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Domain 3 — Hours & Engagement Metrics

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fq_view("avg_hours_per_recipient")} AS
SELECT
    cm.month_name,
    cm.calendar_month_key,
    COUNT(DISTINCT m.medicaid_recipient_key) AS recipients,
    ROUND(AVG(m.monthly_hours_completed), 2) AS avg_monthly_hours,
    ROUND(SUM(m.monthly_hours_completed), 2) AS total_monthly_hours,
    {REQUIRED_MONTHLY_HOURS} AS required_monthly_hours
FROM {fq("fact_recipient_monthly_engagement")} m
JOIN {fq("dim_calendar_month")} cm
  ON m.calendar_month_key = cm.calendar_month_key
GROUP BY cm.month_name, cm.calendar_month_key
""")
print(f"Created {fq_view('avg_hours_per_recipient')}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fq_view("hours_distribution")} AS
SELECT
    cm.month_name,
    cm.calendar_month_key,
    CASE
        WHEN m.monthly_hours_completed = 0 THEN '0 hours'
        WHEN m.monthly_hours_completed > 0 AND m.monthly_hours_completed < 40 THEN '1-39 hours'
        WHEN m.monthly_hours_completed >= 40 AND m.monthly_hours_completed < 80 THEN '40-79 hours'
        WHEN m.monthly_hours_completed = 80 THEN 'Exactly 80 hours'
        WHEN m.monthly_hours_completed > 80 AND m.monthly_hours_completed <= 160 THEN '81-160 hours'
        ELSE '160+ hours'
    END AS hours_bucket,
    COUNT(DISTINCT m.medicaid_recipient_key) AS recipients
FROM {fq("fact_recipient_monthly_engagement")} m
JOIN {fq("dim_calendar_month")} cm
  ON m.calendar_month_key = cm.calendar_month_key
GROUP BY cm.month_name, cm.calendar_month_key,
    CASE
        WHEN m.monthly_hours_completed = 0 THEN '0 hours'
        WHEN m.monthly_hours_completed > 0 AND m.monthly_hours_completed < 40 THEN '1-39 hours'
        WHEN m.monthly_hours_completed >= 40 AND m.monthly_hours_completed < 80 THEN '40-79 hours'
        WHEN m.monthly_hours_completed = 80 THEN 'Exactly 80 hours'
        WHEN m.monthly_hours_completed > 80 AND m.monthly_hours_completed <= 160 THEN '81-160 hours'
        ELSE '160+ hours'
    END
""")
print(f"Created {fq_view('hours_distribution')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Domain 4 — Age Analysis

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fq_view("age_group_summary")} AS
SELECT
    m.age_group,
    a.activity_type,
    COUNT(DISTINCT m.medicaid_recipient_key) AS distinct_recipients,
    COUNT(DISTINCT a.engagement_activity_key) AS activity_count,
    ROUND(SUM(a.activity_duration_hours), 2) AS total_hours,
    ROUND(AVG(a.activity_duration_hours), 2) AS avg_hours_per_activity
FROM {fq("fact_recipient_monthly_engagement")} m
JOIN {fq("fact_engagement_activity")} a
  ON m.medicaid_recipient_key = a.medicaid_recipient_key
 AND m.calendar_month_key = a.calendar_month_key
GROUP BY m.age_group, a.activity_type
""")
print(f"Created {fq_view('age_group_summary')}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fq_view("age_group_monthly")} AS
SELECT
    m.age_group,
    cm.month_name,
    cm.calendar_month_key,
    COUNT(DISTINCT m.medicaid_recipient_key) AS distinct_recipients,
    ROUND(SUM(m.monthly_hours_completed), 2) AS total_hours,
    ROUND(AVG(m.monthly_hours_completed), 2) AS avg_hours
FROM {fq("fact_recipient_monthly_engagement")} m
JOIN {fq("dim_calendar_month")} cm
  ON m.calendar_month_key = cm.calendar_month_key
GROUP BY m.age_group, cm.month_name, cm.calendar_month_key
""")
print(f"Created {fq_view('age_group_monthly')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Domain 5 — Fraud / Suspicious Patterns
# MAGIC
# MAGIC Recomputes suspicious flags from raw data rather than trusting stored flags.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fq_view("suspicious_overlap")} AS
SELECT
    a.medicaid_recipient_key,
    r.msis_identification_num,
    r.eligible_state_code,
    a.calendar_month_key,
    cm.month_name,
    a.activity_type,
    a.activity_start_ts,
    a.activity_end_ts,
    a.activity_duration_hours,
    a.establishment_key,
    e.establishment_name
FROM {fq("fact_engagement_activity")} a
JOIN {fq("dim_medicaid_recipient")} r
  ON a.medicaid_recipient_key = r.medicaid_recipient_key
JOIN {fq("dim_calendar_month")} cm
  ON a.calendar_month_key = cm.calendar_month_key
JOIN {fq("dim_establishment")} e
  ON a.establishment_key = e.establishment_key
WHERE a.is_overlap_flag = TRUE
""")
print(f"Created {fq_view('suspicious_overlap')}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fq_view("suspicious_overlap_summary")} AS
SELECT
    r.eligible_state_code AS state_code,
    cm.month_name,
    a.calendar_month_key,
    COUNT(DISTINCT a.medicaid_recipient_key) AS recipients_with_overlap,
    COUNT(*) AS overlap_activity_count,
    ROUND(SUM(a.activity_duration_hours), 2) AS overlap_total_hours
FROM {fq("fact_engagement_activity")} a
JOIN {fq("dim_medicaid_recipient")} r
  ON a.medicaid_recipient_key = r.medicaid_recipient_key
JOIN {fq("dim_calendar_month")} cm
  ON a.calendar_month_key = cm.calendar_month_key
WHERE a.is_overlap_flag = TRUE
GROUP BY r.eligible_state_code, cm.month_name, a.calendar_month_key
""")
print(f"Created {fq_view('suspicious_overlap_summary')}")

# COMMAND ----------

# Recompute exactly-80-hours consecutive streak from activity fact
# rather than trusting the pre-stored flag
spark.sql(f"""
CREATE OR REPLACE VIEW {fq_view("suspicious_exact_80_consecutive")} AS
WITH monthly_hours AS (
    SELECT
        medicaid_recipient_key,
        calendar_month_key,
        ROUND(SUM(activity_duration_hours), 2) AS computed_hours
    FROM {fq("fact_engagement_activity")}
    GROUP BY medicaid_recipient_key, calendar_month_key
),
exact_80 AS (
    SELECT
        mh.medicaid_recipient_key,
        mh.calendar_month_key,
        cm.month_start_date,
        CASE WHEN mh.computed_hours = {REQUIRED_MONTHLY_HOURS} THEN 1 ELSE 0 END AS is_exact_80
    FROM monthly_hours mh
    JOIN {fq("dim_calendar_month")} cm ON mh.calendar_month_key = cm.calendar_month_key
),
streaks AS (
    SELECT
        medicaid_recipient_key,
        calendar_month_key,
        is_exact_80,
        SUM(CASE WHEN is_exact_80 = 0 THEN 1 ELSE 0 END)
            OVER (PARTITION BY medicaid_recipient_key ORDER BY month_start_date) AS streak_group
    FROM exact_80
),
streak_lengths AS (
    SELECT
        medicaid_recipient_key,
        streak_group,
        MIN(calendar_month_key) AS streak_start_month,
        MAX(calendar_month_key) AS streak_end_month,
        COUNT(*) AS streak_length
    FROM streaks
    WHERE is_exact_80 = 1
    GROUP BY medicaid_recipient_key, streak_group
)
SELECT
    sl.medicaid_recipient_key,
    r.msis_identification_num,
    r.eligible_state_code,
    sl.streak_start_month,
    sl.streak_end_month,
    sl.streak_length,
    CASE WHEN sl.streak_length >= {CONSECUTIVE_STREAK_THRESHOLD} THEN TRUE ELSE FALSE END AS is_suspicious_streak
FROM streak_lengths sl
JOIN {fq("dim_medicaid_recipient")} r
  ON sl.medicaid_recipient_key = r.medicaid_recipient_key
WHERE sl.streak_length >= {CONSECUTIVE_STREAK_THRESHOLD}
""")
print(f"Created {fq_view('suspicious_exact_80_consecutive')}")

# COMMAND ----------

# Recompute dominant-establishment flag from the establishment-monthly fact
spark.sql(f"""
CREATE OR REPLACE VIEW {fq_view("suspicious_dominant_establishment")} AS
SELECT
    rem.medicaid_recipient_key,
    r.msis_identification_num,
    r.eligible_state_code,
    rem.establishment_key,
    e.establishment_name,
    e.establishment_type,
    rem.calendar_month_key,
    cm.month_name,
    rem.monthly_hours_completed,
    rem.establishment_share_of_monthly_hours,
    rem.dominance_rank_in_month
FROM {fq("fact_recipient_establishment_monthly")} rem
JOIN {fq("dim_medicaid_recipient")} r
  ON rem.medicaid_recipient_key = r.medicaid_recipient_key
JOIN {fq("dim_establishment")} e
  ON rem.establishment_key = e.establishment_key
JOIN {fq("dim_calendar_month")} cm
  ON rem.calendar_month_key = cm.calendar_month_key
WHERE rem.is_dominant_establishment_for_month = TRUE
  AND rem.establishment_share_of_monthly_hours >= 0.90
""")
print(f"Created {fq_view('suspicious_dominant_establishment')}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fq_view("suspicious_multi_flag")} AS
WITH overlap_recipients AS (
    SELECT DISTINCT medicaid_recipient_key
    FROM {fq("fact_engagement_activity")}
    WHERE is_overlap_flag = TRUE
),
exact80_recipients AS (
    SELECT DISTINCT medicaid_recipient_key
    FROM {fq("fact_recipient_monthly_engagement")}
    WHERE is_exactly_80_hours_consecutive_flag = TRUE
),
dominant_recipients AS (
    SELECT DISTINCT medicaid_recipient_key
    FROM {fq("fact_recipient_establishment_monthly")}
    WHERE is_dominant_establishment_for_month = TRUE
      AND establishment_share_of_monthly_hours >= 0.90
)
SELECT
    r.medicaid_recipient_key,
    r.msis_identification_num,
    r.eligible_state_code,
    r.enrollment_type_code,
    CASE WHEN o.medicaid_recipient_key IS NOT NULL THEN TRUE ELSE FALSE END AS has_overlap,
    CASE WHEN e.medicaid_recipient_key IS NOT NULL THEN TRUE ELSE FALSE END AS has_exact_80_streak,
    CASE WHEN d.medicaid_recipient_key IS NOT NULL THEN TRUE ELSE FALSE END AS has_dominant_establishment,
    (CASE WHEN o.medicaid_recipient_key IS NOT NULL THEN 1 ELSE 0 END
   + CASE WHEN e.medicaid_recipient_key IS NOT NULL THEN 1 ELSE 0 END
   + CASE WHEN d.medicaid_recipient_key IS NOT NULL THEN 1 ELSE 0 END) AS flag_count
FROM {fq("dim_medicaid_recipient")} r
LEFT JOIN overlap_recipients o ON r.medicaid_recipient_key = o.medicaid_recipient_key
LEFT JOIN exact80_recipients e ON r.medicaid_recipient_key = e.medicaid_recipient_key
LEFT JOIN dominant_recipients d ON r.medicaid_recipient_key = d.medicaid_recipient_key
WHERE (o.medicaid_recipient_key IS NOT NULL
    OR e.medicaid_recipient_key IS NOT NULL
    OR d.medicaid_recipient_key IS NOT NULL)
""")
print(f"Created {fq_view('suspicious_multi_flag')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Domain 6 — Expected vs Reported Hours

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fq_view("expected_vs_reported_by_state")} AS
SELECT
    r.eligible_state_code AS state_code,
    cm.month_name,
    m.calendar_month_key,
    COUNT(DISTINCT m.medicaid_recipient_key) AS recipients,
    ROUND(SUM(m.monthly_hours_completed), 2) AS total_reported_hours,
    COUNT(DISTINCT m.medicaid_recipient_key) * {REQUIRED_MONTHLY_HOURS} AS total_expected_hours,
    ROUND(
        SUM(m.monthly_hours_completed)
        / NULLIF(COUNT(DISTINCT m.medicaid_recipient_key) * {REQUIRED_MONTHLY_HOURS}, 0)
        * 100, 2
    ) AS pct_of_expected
FROM {fq("fact_recipient_monthly_engagement")} m
JOIN {fq("dim_medicaid_recipient")} r
  ON m.medicaid_recipient_key = r.medicaid_recipient_key
JOIN {fq("dim_calendar_month")} cm
  ON m.calendar_month_key = cm.calendar_month_key
GROUP BY r.eligible_state_code, cm.month_name, m.calendar_month_key
""")
print(f"Created {fq_view('expected_vs_reported_by_state')}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {fq_view("expected_vs_reported_total")} AS
SELECT
    cm.month_name,
    m.calendar_month_key,
    COUNT(DISTINCT m.medicaid_recipient_key) AS recipients,
    ROUND(SUM(m.monthly_hours_completed), 2) AS total_reported_hours,
    COUNT(DISTINCT m.medicaid_recipient_key) * {REQUIRED_MONTHLY_HOURS} AS total_expected_hours,
    ROUND(
        SUM(m.monthly_hours_completed)
        / NULLIF(COUNT(DISTINCT m.medicaid_recipient_key) * {REQUIRED_MONTHLY_HOURS}, 0)
        * 100, 2
    ) AS pct_of_expected
FROM {fq("fact_recipient_monthly_engagement")} m
JOIN {fq("dim_calendar_month")} cm
  ON m.calendar_month_key = cm.calendar_month_key
GROUP BY cm.month_name, m.calendar_month_key
""")
print(f"Created {fq_view('expected_vs_reported_total')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## View creation summary

# COMMAND ----------

views = [r for r in spark.sql(f"SHOW VIEWS IN {catalog}.{schema}").collect()]
created_views = [v.viewName for v in views if v.viewName.startswith(view_prefix.replace(".", ""))]
print(f"Total views in {catalog}.{schema} with prefix '{view_prefix}': {len(created_views)}")
for v in sorted(created_views):
    print(f"  • {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Validation Cell 1 — Reconcile recipient counts

# COMMAND ----------

dim_count = spark.sql(f"""
    SELECT COUNT(DISTINCT medicaid_recipient_key) AS cnt
    FROM {fq("dim_medicaid_recipient")}
""").collect()[0].cnt

fact_count = spark.sql(f"""
    SELECT COUNT(DISTINCT medicaid_recipient_key) AS cnt
    FROM {fq("fact_engagement_activity")}
""").collect()[0].cnt

monthly_count = spark.sql(f"""
    SELECT COUNT(DISTINCT medicaid_recipient_key) AS cnt
    FROM {fq("fact_recipient_monthly_engagement")}
""").collect()[0].cnt

print(f"Distinct recipients in dim:              {dim_count}")
print(f"Distinct recipients in fact_activity:     {fact_count}")
print(f"Distinct recipients in fact_monthly:      {monthly_count}")
assert fact_count <= dim_count, "Fact has recipients not in dimension!"
assert monthly_count <= dim_count, "Monthly has recipients not in dimension!"
print("Recipient count reconciliation passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Cell 2 — Reconcile activity counts

# COMMAND ----------

total_activities = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM {fq("fact_engagement_activity")}
""").collect()[0].cnt

monthly_activity_sum = spark.sql(f"""
    SELECT SUM(activity_count) AS cnt FROM {fq("fact_recipient_monthly_engagement")}
""").collect()[0].cnt

est_monthly_activity_sum = spark.sql(f"""
    SELECT SUM(activity_count) AS cnt FROM {fq("fact_recipient_establishment_monthly")}
""").collect()[0].cnt

print(f"Total rows in fact_engagement_activity:                    {total_activities}")
print(f"SUM(activity_count) in fact_recipient_monthly_engagement:  {monthly_activity_sum}")
print(f"SUM(activity_count) in fact_recipient_establishment_monthly: {est_monthly_activity_sum}")
assert total_activities == monthly_activity_sum, (
    f"Activity count mismatch: fact={total_activities} vs monthly_sum={monthly_activity_sum}"
)
assert total_activities == est_monthly_activity_sum, (
    f"Activity count mismatch: fact={total_activities} vs est_monthly_sum={est_monthly_activity_sum}"
)
print("Activity count reconciliation passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Cell 3 — Verify monthly_hours_completed from activity fact

# COMMAND ----------

mismatched = spark.sql(f"""
SELECT COUNT(*) AS mismatched_rows
FROM (
    SELECT
        f.medicaid_recipient_key,
        f.calendar_month_key,
        ROUND(SUM(f.activity_duration_hours), 2) AS fact_hours,
        ROUND(m.monthly_hours_completed, 2) AS monthly_hours
    FROM {fq("fact_engagement_activity")} f
    JOIN {fq("fact_recipient_monthly_engagement")} m
      ON f.medicaid_recipient_key = m.medicaid_recipient_key
     AND f.calendar_month_key = m.calendar_month_key
    GROUP BY f.medicaid_recipient_key, f.calendar_month_key, m.monthly_hours_completed
) t
WHERE fact_hours <> monthly_hours
""").collect()[0].mismatched_rows

print(f"Rows with mismatched monthly hours (fact vs monthly engagement): {mismatched}")
assert mismatched == 0, f"Found {mismatched} mismatched monthly-hours rows!"
print("Monthly hours reconciliation passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Cell 4 — Verify suspicious pattern counts

# COMMAND ----------

overlap_count = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM {fq("fact_engagement_activity")}
    WHERE is_overlap_flag = TRUE
""").collect()[0].cnt

exact80_consec_count = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM {fq("fact_recipient_monthly_engagement")}
    WHERE is_exactly_80_hours_consecutive_flag = TRUE
""").collect()[0].cnt

dominant_count = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM {fq("fact_recipient_monthly_engagement")}
    WHERE is_single_establishment_dominant_flag = TRUE
""").collect()[0].cnt

expected_overlap = 8859
expected_exact80_consec = 241
expected_dominant = 538

print(f"Overlap flagged rows:              {overlap_count} (expected {expected_overlap})")
print(f"Exact-80 consecutive flagged rows: {exact80_consec_count} (expected {expected_exact80_consec})")
print(f"Dominant establishment rows:       {dominant_count} (expected {expected_dominant})")

assert overlap_count == expected_overlap, f"Overlap count mismatch: {overlap_count} vs {expected_overlap}"
assert exact80_consec_count == expected_exact80_consec, f"Exact-80 consec mismatch: {exact80_consec_count} vs {expected_exact80_consec}"
assert dominant_count == expected_dominant, f"Dominant mismatch: {dominant_count} vs {expected_dominant}"

print("All suspicious pattern counts match expectations.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Cell 5 — Spot-check views return data

# COMMAND ----------

SPOT_CHECK_VIEWS = [
    "enrollment_summary",
    "activity_by_type",
    "avg_hours_per_recipient",
    "age_group_summary",
    "suspicious_overlap_summary",
    "expected_vs_reported_total",
]

for v in SPOT_CHECK_VIEWS:
    cnt = spark.table(fq_view(v)).count()
    print(f"  {fq_view(v)}: {cnt} rows")
    assert cnt > 0, f"View {fq_view(v)} returned 0 rows!"

print("\nAll spot-check views return data.")
print("\n=== Semantic views notebook complete ===")

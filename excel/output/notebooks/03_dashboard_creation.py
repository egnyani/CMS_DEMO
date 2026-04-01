# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 3 — Dashboard Creation (Lakeview / AI-BI)
# MAGIC
# MAGIC Creates a Lakeview dashboard over the semantic views produced by Notebook 2.
# MAGIC Uses the Databricks SDK `LakeviewAPI` which is the current supported interface.
# MAGIC
# MAGIC **Strategy:**
# MAGIC - Option A (preferred): programmatic dashboard creation via Databricks SDK
# MAGIC - Option B (fallback): queries are defined as standalone SQL — copy into the
# MAGIC   dashboard UI manually if the SDK is unavailable or the workspace blocks API creation.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog name")
dbutils.widgets.text("schema", "default", "Schema (database) name")
dbutils.widgets.text("view_prefix", "vw_", "View prefix used in Notebook 2")
dbutils.widgets.text("dashboard_name", "CMS Medicaid Engagement Dashboard", "Dashboard display name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
view_prefix = dbutils.widgets.get("view_prefix")
dashboard_name = dbutils.widgets.get("dashboard_name")

fq_view = lambda name: f"{catalog}.{schema}.{view_prefix}{name}"

print(f"catalog        = {catalog}")
print(f"schema         = {schema}")
print(f"view_prefix    = {view_prefix}")
print(f"dashboard_name = {dashboard_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Define dashboard query catalog
# MAGIC
# MAGIC Each entry: (query_name, display_title, sql_text, chart_type_hint)

# COMMAND ----------

DASHBOARD_QUERIES = [
    # --- Domain 1: Recipient & Enrollment ---
    (
        "enrollment_summary",
        "Recipients by Enrollment Type",
        f"SELECT enrollment_type_code, distinct_recipients, total_records FROM {fq_view('enrollment_summary')} ORDER BY distinct_recipients DESC",
        "bar",
    ),
    (
        "monthly_registrations",
        "Monthly Registrations by Enrollment Type",
        f"SELECT registration_month, enrollment_type_code, registrations FROM {fq_view('monthly_registrations')} ORDER BY registration_month",
        "line",
    ),

    # --- Domain 2: Activity & Establishment ---
    (
        "activity_by_type",
        "Activity Volume by Type",
        f"SELECT activity_type, distinct_recipients, activity_count, total_hours FROM {fq_view('activity_by_type')} ORDER BY activity_count DESC",
        "bar",
    ),
    (
        "establishment_by_type",
        "Establishment Summary by Type",
        f"SELECT establishment_type, establishment_count, distinct_recipients, activity_count, total_hours FROM {fq_view('establishment_by_type')} ORDER BY activity_count DESC",
        "bar",
    ),
    (
        "top_establishments",
        "Top 20 Establishments by Recipient Count",
        f"SELECT establishment_name, establishment_type, establishment_state_code, distinct_recipients, total_hours FROM {fq_view('recipients_per_establishment')} ORDER BY distinct_recipients DESC LIMIT 20",
        "table",
    ),
    (
        "recipients_by_state",
        "Recipients & Hours by State",
        f"SELECT state_code, distinct_recipients, activity_count, total_hours FROM {fq_view('recipients_per_state')} ORDER BY distinct_recipients DESC",
        "bar",
    ),

    # --- Domain 3: Hours & Engagement ---
    (
        "avg_hours_trend",
        "Average Monthly Hours per Recipient (Trend)",
        f"SELECT month_name, calendar_month_key, recipients, avg_monthly_hours, total_monthly_hours, required_monthly_hours FROM {fq_view('avg_hours_per_recipient')} ORDER BY calendar_month_key",
        "line",
    ),
    (
        "hours_distribution",
        "Hours Distribution Buckets by Month",
        f"SELECT month_name, hours_bucket, recipients FROM {fq_view('hours_distribution')} ORDER BY calendar_month_key, hours_bucket",
        "bar",
    ),

    # --- Domain 4: Age Analysis ---
    (
        "age_activity_summary",
        "Activity by Age Group and Type",
        f"SELECT age_group, activity_type, distinct_recipients, activity_count, total_hours, avg_hours_per_activity FROM {fq_view('age_group_summary')} ORDER BY age_group, activity_type",
        "bar",
    ),
    (
        "age_monthly_trend",
        "Monthly Hours by Age Group",
        f"SELECT age_group, month_name, calendar_month_key, distinct_recipients, total_hours, avg_hours FROM {fq_view('age_group_monthly')} ORDER BY calendar_month_key, age_group",
        "line",
    ),

    # --- Domain 5: Fraud / Suspicious Patterns ---
    (
        "overlap_summary",
        "Overlapping Activity by State and Month",
        f"SELECT state_code, month_name, recipients_with_overlap, overlap_activity_count, overlap_total_hours FROM {fq_view('suspicious_overlap_summary')} ORDER BY calendar_month_key, state_code",
        "table",
    ),
    (
        "exact_80_streaks",
        "Suspicious Exactly-80-Hours Consecutive Streaks",
        f"SELECT medicaid_recipient_key, msis_identification_num, eligible_state_code, streak_start_month, streak_end_month, streak_length FROM {fq_view('suspicious_exact_80_consecutive')} ORDER BY streak_length DESC",
        "table",
    ),
    (
        "dominant_establishment",
        "Dominant Single-Establishment Recipients (>= 90%)",
        f"SELECT medicaid_recipient_key, msis_identification_num, eligible_state_code, establishment_name, establishment_type, month_name, establishment_share_of_monthly_hours FROM {fq_view('suspicious_dominant_establishment')} ORDER BY establishment_share_of_monthly_hours DESC LIMIT 50",
        "table",
    ),
    (
        "multi_flag_recipients",
        "Recipients with Multiple Suspicious Flags",
        f"SELECT medicaid_recipient_key, msis_identification_num, eligible_state_code, enrollment_type_code, has_overlap, has_exact_80_streak, has_dominant_establishment, flag_count FROM {fq_view('suspicious_multi_flag')} WHERE flag_count >= 2 ORDER BY flag_count DESC",
        "table",
    ),

    # --- Domain 6: Expected vs Reported Hours ---
    (
        "expected_vs_reported_state",
        "Expected vs Reported Hours by State",
        f"SELECT state_code, month_name, recipients, total_reported_hours, total_expected_hours, pct_of_expected FROM {fq_view('expected_vs_reported_by_state')} ORDER BY calendar_month_key, state_code",
        "table",
    ),
    (
        "expected_vs_reported_total",
        "Expected vs Reported Hours — Overall Trend",
        f"SELECT month_name, recipients, total_reported_hours, total_expected_hours, pct_of_expected FROM {fq_view('expected_vs_reported_total')} ORDER BY calendar_month_key",
        "line",
    ),
]

print(f"Defined {len(DASHBOARD_QUERIES)} dashboard queries.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Validate all dashboard queries execute successfully

# COMMAND ----------

query_errors = []

for qname, qtitle, qsql, _ in DASHBOARD_QUERIES:
    try:
        cnt = spark.sql(qsql).count()
        print(f"  ✔ {qname}: {cnt} rows")
    except Exception as ex:
        query_errors.append((qname, str(ex)))
        print(f"  ✘ {qname}: {ex}")

if query_errors:
    raise AssertionError(f"{len(query_errors)} query(ies) failed: {[q[0] for q in query_errors]}")

print(f"\nAll {len(DASHBOARD_QUERIES)} queries validated successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Build Lakeview serialized dashboard definition

# COMMAND ----------

import json, uuid

def _dataset(name: str, sql: str) -> dict:
    return {"name": name, "query": sql}

def _page_for_query(qname: str, qtitle: str, chart_hint: str, dataset_name: str) -> dict:
    widget = {
        "name": qname,
        "queries": [{"name": qname, "query": {"datasetName": dataset_name, "disaggregated": False, "fields": []}}],
    }
    if chart_hint == "table":
        widget["spec"] = {"version": 3, "widgetType": "table", "encodings": {}}
    else:
        widget["spec"] = {"version": 3, "widgetType": chart_hint, "encodings": {}}
    return {
        "name": qname,
        "displayName": qtitle,
        "layout": [{"widget": widget, "position": {"x": 0, "y": 0, "width": 6, "height": 4}}],
    }

datasets = [_dataset(qn, qs) for qn, _, qs, _ in DASHBOARD_QUERIES]
pages = [_page_for_query(qn, qt, ch, qn) for qn, qt, _, ch in DASHBOARD_QUERIES]

serialized_dashboard = json.dumps({"datasets": datasets, "pages": pages}, indent=2)

print(f"Serialized dashboard JSON: {len(serialized_dashboard)} chars, "
      f"{len(datasets)} datasets, {len(pages)} pages")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Option A: Create dashboard via Databricks SDK (Lakeview API)

# COMMAND ----------

from databricks.sdk import WorkspaceClient

dashboard_created = False
dashboard_url = None

try:
    w = WorkspaceClient()

    existing = w.lakeview.list()
    found = None
    for d in existing:
        if d.display_name == dashboard_name:
            found = d
            break

    if found:
        updated = w.lakeview.update(
            dashboard_id=found.dashboard_id,
            display_name=dashboard_name,
            serialized_dashboard=serialized_dashboard,
            warehouse_id=spark.conf.get("spark.databricks.clusterUsageTags.sqlWarehouseId", ""),
        )
        dashboard_id = found.dashboard_id
        print(f"Updated existing dashboard: {dashboard_name} (id={dashboard_id})")
    else:
        created = w.lakeview.create(
            display_name=dashboard_name,
            serialized_dashboard=serialized_dashboard,
            warehouse_id=spark.conf.get("spark.databricks.clusterUsageTags.sqlWarehouseId", ""),
            parent_path=f"/Workspace/Users/{spark.sql('SELECT current_user()').collect()[0][0]}",
        )
        dashboard_id = created.dashboard_id
        print(f"Created new dashboard: {dashboard_name} (id={dashboard_id})")

    try:
        w.lakeview.publish(dashboard_id=dashboard_id)
        print(f"Dashboard published successfully.")
    except Exception as pub_ex:
        print(f"Dashboard created but publish step failed (may require manual publish): {pub_ex}")

    host = spark.conf.get("spark.databricks.workspaceUrl", dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get())
    dashboard_url = f"https://{host}/sql/dashboardsv3/{dashboard_id}"
    dashboard_created = True
    print(f"\nDashboard URL: {dashboard_url}")

except ImportError:
    print("Databricks SDK not available. Falling back to Option B (manual).")
except Exception as ex:
    print(f"SDK dashboard creation failed: {ex}")
    print("Falling back to Option B (manual).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Option B (Fallback): Print SQL queries for manual dashboard creation

# COMMAND ----------

if not dashboard_created:
    print("=" * 80)
    print("MANUAL DASHBOARD CREATION INSTRUCTIONS")
    print("=" * 80)
    print()
    print("The Lakeview API was not available or failed. Follow these steps:")
    print()
    print("1. Navigate to: SQL > Dashboards > Create Dashboard (Lakeview)")
    print(f"2. Name the dashboard: {dashboard_name}")
    print("3. Add a SQL warehouse as the data source")
    print("4. For each query below, click '+' > Add visualization, paste the SQL,")
    print("   choose the suggested chart type, and arrange on the canvas.")
    print()

    for i, (qname, qtitle, qsql, chart_hint) in enumerate(DASHBOARD_QUERIES, 1):
        print(f"--- Query {i}/{len(DASHBOARD_QUERIES)}: {qtitle} ---")
        print(f"-- Suggested chart: {chart_hint}")
        print(qsql)
        print()

    print("=" * 80)
    print("END OF MANUAL QUERIES")
    print("=" * 80)
else:
    print(f"Dashboard was created programmatically. URL: {dashboard_url}")
    print("No manual steps required.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — Final validation: confirm views are accessible from dashboard queries

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

all_views = [
    r.viewName
    for r in spark.sql(f"SHOW VIEWS IN {catalog}.{schema}").collect()
]

referenced_views = set()
for _, _, qsql, _ in DASHBOARD_QUERIES:
    for v_name in all_views:
        if v_name in qsql:
            referenced_views.add(v_name)

missing = []
for v in referenced_views:
    try:
        spark.table(f"{catalog}.{schema}.{v}").limit(1).collect()
    except Exception as ex:
        missing.append((v, str(ex)))

if missing:
    print(f"⚠ {len(missing)} view(s) could not be queried:")
    for v, err in missing:
        print(f"  {v}: {err}")
    raise AssertionError("Some dashboard views are not accessible.")
else:
    print(f"All {len(referenced_views)} referenced views are accessible.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 — Summary

# COMMAND ----------

print("=" * 60)
print("DASHBOARD CREATION NOTEBOOK — SUMMARY")
print("=" * 60)
print(f"  Dashboard name:    {dashboard_name}")
print(f"  Total queries:     {len(DASHBOARD_QUERIES)}")
print(f"  Views referenced:  {len(referenced_views)}")
print(f"  Programmatic:      {'Yes' if dashboard_created else 'No (manual fallback)'}")
if dashboard_url:
    print(f"  Dashboard URL:     {dashboard_url}")
print()
print("Query breakdown by domain:")
domains = {
    "Recipient & Enrollment": ["enrollment_summary", "monthly_registrations"],
    "Activity & Establishment": ["activity_by_type", "establishment_by_type", "top_establishments", "recipients_by_state"],
    "Hours & Engagement": ["avg_hours_trend", "hours_distribution"],
    "Age Analysis": ["age_activity_summary", "age_monthly_trend"],
    "Fraud / Suspicious": ["overlap_summary", "exact_80_streaks", "dominant_establishment", "multi_flag_recipients"],
    "Expected vs Reported": ["expected_vs_reported_state", "expected_vs_reported_total"],
}
for domain, queries in domains.items():
    print(f"  {domain}: {len(queries)} queries")

print("\n=== Dashboard creation notebook complete ===")

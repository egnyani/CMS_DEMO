# CMS Medicaid Engagement Dashboard — Deployment Guide

End-to-end instructions for deploying the demo dataset into Databricks,
creating the semantic layer, and building the Lakeview dashboard.

---

## Prerequisites

| Requirement | Details |
|---|---|
| **Databricks Workspace** | Unity Catalog enabled |
| **Permissions** | `CREATE CATALOG`, `CREATE SCHEMA`, `CREATE TABLE`, `CREATE VIEW` on the target catalog |
| **Cluster / SQL Warehouse** | Any Databricks Runtime 13.3 LTS+ or SQL Warehouse (Serverless recommended) |
| **Databricks SDK** | Pre-installed on DBR 13.3+; only needed for programmatic dashboard creation |
| **Source CSV files** | The 7 CSV files from the `output/` directory of this project |

---

## Step 0 — Upload CSV Files to Databricks

Before running any notebook, the 7 CSV files must be accessible from your cluster.

### Option A: Unity Catalog Volume (Recommended)

1. In the Databricks sidebar, go to **Catalog > workspace > default**.
2. Under **Volumes**, use the existing `landing` volume.
3. Upload all 7 CSV files into that volume:

   ```
   dim_medicaid_recipient.csv
   dim_establishment.csv
   dim_calendar_month.csv
   dim_verification_method.csv
   fact_engagement_activity.csv
   fact_recipient_monthly_engagement.csv
   fact_recipient_establishment_monthly.csv
   ```

4. The resulting path will be:
   ```
   /Volumes/workspace/default/landing/
   ```
   This is the default value for the `source_path` widget in Notebook 1.

### Option B: DBFS Upload

1. In the sidebar, go to **Data > DBFS > FileStore**.
2. Create a folder (e.g. `cms_demo_data`) and upload the 7 CSVs.
3. Set `source_path` to `/FileStore/cms_demo_data/` when running Notebook 1.

### Option C: Workspace Files / Repos

1. Clone or upload this project into a Databricks Repo.
2. Set `source_path` to the repo-relative path of the `output/` folder.

---

## Step 1 — Run Notebook 1: Ingestion

**File:** `01_ingestion.py`

### What it does

- Creates the Unity Catalog and schema if they don't exist.
- Reads all 7 CSV files using explicit Spark schemas (no inference).
- Writes each as a Delta table with full overwrite (safe to rerun).
- Validates row counts, null keys, and referential integrity.

### Widget parameters

| Widget | Default | Description |
|---|---|---|
| `catalog` | `workspace` | Unity Catalog name to create/use |
| `schema` | `default` | Schema (database) name |
| `source_path` | `/Volumes/workspace/default/landing/` | Path where CSVs were uploaded |
| `table_prefix` | *(empty)* | Optional prefix for table names (e.g. `dev_`) |

### How to run

1. Open `01_ingestion.py` in Databricks.
2. Attach to a cluster (DBR 13.3 LTS+).
3. Set widget values at the top of the notebook — at minimum, verify `source_path`
   points to where you uploaded the CSVs.
4. Click **Run All**.

### Expected output

```
✔ workspace.default.dim_medicaid_recipient: wrote 10000 rows (expected 10000)
✔ workspace.default.dim_establishment: wrote 500 rows (expected 500)
✔ workspace.default.dim_calendar_month: wrote 6 rows (expected 6)
✔ workspace.default.dim_verification_method: wrote 3 rows (expected 3)
✔ workspace.default.fact_engagement_activity: wrote 99369 rows (expected 99369)
✔ workspace.default.fact_recipient_monthly_engagement: wrote 36097 rows (expected 36097)
✔ workspace.default.fact_recipient_establishment_monthly: wrote 89958 rows (expected 89958)
```

### Validation checkpoints

The notebook will assert-fail and stop if:

- Any row count doesn't match expectations.
- Any primary/foreign key column contains NULLs.
- Any referential integrity violation exists (orphan keys).
- Any table is missing from the catalog after creation.

> **Troubleshooting:** If `source_path` is wrong, you'll get a
> `AnalysisException: Path does not exist` error. Double-check the volume path.

---

## Step 2 — Run Notebook 2: Semantic Views

**File:** `02_semantic_views.py`

### What it does

- Creates 19 SQL views across 6 business domains using `CREATE OR REPLACE VIEW`.
- Recomputes suspicious-pattern logic from raw data (does not blindly trust stored flags).
- Runs 5 validation cells that reconcile counts between tables and views.

### Widget parameters

| Widget | Default | Description |
|---|---|---|
| `catalog` | `workspace` | Must match Notebook 1 |
| `schema` | `default` | Must match Notebook 1 |
| `table_prefix` | *(empty)* | Must match Notebook 1 |
| `view_prefix` | `vw_` | Prefix for all view names |

### How to run

1. Open `02_semantic_views.py` in Databricks.
2. Attach to the same cluster used for Notebook 1.
3. Set widget values to match what you used in Notebook 1.
4. Click **Run All**.

### Views created (19 total)

| # | Domain | View Name | Purpose |
|---|---|---|---|
| 1 | Enrollment | `vw_enrollment_summary` | Recipients by enrollment type |
| 2 | Enrollment | `vw_monthly_registrations` | Monthly registration counts |
| 3 | Enrollment | `vw_today_registrations` | Registrations for current date |
| 4 | Activity | `vw_activity_by_type` | Activity volume by type |
| 5 | Activity | `vw_establishment_by_type` | Establishments by type |
| 6 | Activity | `vw_recipients_per_establishment` | Recipients per establishment |
| 7 | Activity | `vw_recipients_per_activity_type` | Recipients per activity type per month |
| 8 | Activity | `vw_recipients_per_state` | Recipients & hours by state |
| 9 | Hours | `vw_avg_hours_per_recipient` | Avg monthly hours trend |
| 10 | Hours | `vw_hours_distribution` | Hours bucketed (0, 1-39, 40-79, 80, 81-160, 160+) |
| 11 | Age | `vw_age_group_summary` | Activity by age group and type |
| 12 | Age | `vw_age_group_monthly` | Monthly hours by age group |
| 13 | Suspicious | `vw_suspicious_overlap` | Detail: overlapping activity rows |
| 14 | Suspicious | `vw_suspicious_overlap_summary` | Summary: overlap by state & month |
| 15 | Suspicious | `vw_suspicious_exact_80_consecutive` | Recomputed 80-hour streaks (threshold >= 3) |
| 16 | Suspicious | `vw_suspicious_dominant_establishment` | Single-establishment dominance (>= 90% share) |
| 17 | Suspicious | `vw_suspicious_multi_flag` | Recipients with 2+ suspicious flags |
| 18 | Expected vs Reported | `vw_expected_vs_reported_by_state` | Expected vs reported hours by state |
| 19 | Expected vs Reported | `vw_expected_vs_reported_total` | Expected vs reported hours overall trend |

### Validation checkpoints

The notebook will assert-fail and stop if:

- Recipient counts in fact tables exceed the dimension count.
- `SUM(activity_count)` in monthly/establishment-monthly facts doesn't match the activity fact row count.
- Any `monthly_hours_completed` disagrees with the sum of `activity_duration_hours`.
- Suspicious flag counts deviate from expected values (8,859 overlap / 241 exact-80-consecutive / 538 dominant).
- Any spot-check view returns zero rows.

### Business rules embedded

| Rule | Value | Notes |
|---|---|---|
| Required monthly hours | **80** | Fixed baseline for expected-vs-reported calculations |
| Consecutive streak threshold | **>= 3** | Months of exactly 80 hours to flag as suspicious |
| Dominant establishment | **>= 90%** | Share of monthly hours at a single establishment |
| State field | `eligible_state_code` | Used across all dashboard-facing views |

---

## Step 3 — Run Notebook 3: Dashboard Creation

**File:** `03_dashboard_creation.py`

### What it does

- Defines 16 dashboard queries across all 6 domains.
- Validates every query executes successfully.
- **Option A (automatic):** Uses the Databricks SDK Lakeview API to create or update
  the dashboard programmatically and publish it.
- **Option B (manual fallback):** If the SDK fails or is unavailable, prints all 16 SQL
  queries with chart-type suggestions for manual creation.

### Widget parameters

| Widget | Default | Description |
|---|---|---|
| `catalog` | `workspace` | Must match Notebooks 1 & 2 |
| `schema` | `default` | Must match Notebooks 1 & 2 |
| `view_prefix` | `vw_` | Must match Notebook 2 |
| `dashboard_name` | `CMS Medicaid Engagement Dashboard` | Display name for the dashboard |

### How to run

1. Open `03_dashboard_creation.py` in Databricks.
2. Attach to the same cluster or a SQL Warehouse.
3. Set widget values to match previous notebooks.
4. Click **Run All**.

### If Option A succeeds

The notebook outputs a dashboard URL like:

```
https://<workspace-host>/sql/dashboardsv3/<dashboard-id>
```

Click it to open your dashboard. You may need to arrange widgets and adjust
chart encodings in the Lakeview editor (see `DASHBOARD_LAYOUT_GUIDE.md`).

### If Option B is triggered

The notebook prints all 16 queries with their suggested chart types.
Follow the manual instructions in `DASHBOARD_LAYOUT_GUIDE.md` to build the
dashboard in the Databricks UI.

---

## Execution Order Summary

```
Step 0:  Upload 7 CSVs to a Databricks Volume / DBFS
            │
Step 1:  01_ingestion.py          ← Creates 7 Delta tables
            │
Step 2:  02_semantic_views.py     ← Creates 19 SQL views
            │
Step 3:  03_dashboard_creation.py ← Creates Lakeview dashboard (or prints SQL)
            │
Done:    Open dashboard URL and review
```

All three notebooks are **idempotent** — you can rerun any of them safely
without creating duplicates or corrupting data.

---

## Quick Validation Checklist

After all three notebooks complete, verify:

- [ ] 7 Delta tables exist in `workspace.default` with correct row counts
- [ ] 19 views exist in `workspace.default` with the `vw_` prefix
- [ ] All views return data when queried
- [ ] Suspicious flag counts match: 8,859 / 241 / 538
- [ ] Dashboard loads and shows 16 visualizations
- [ ] Monthly hours in views reconcile with activity-level sums

---

## File Inventory

| File | Purpose |
|---|---|
| `01_ingestion.py` | Notebook 1: CSV ingestion to Delta |
| `02_semantic_views.py` | Notebook 2: Semantic view creation |
| `03_dashboard_creation.py` | Notebook 3: Lakeview dashboard creation |
| `DEPLOYMENT_GUIDE.md` | This file — step-by-step instructions |
| `DATA_MODEL.md` | Schema reference for all tables and views |
| `DASHBOARD_LAYOUT_GUIDE.md` | Manual dashboard creation instructions |
| `TROUBLESHOOTING.md` | Common issues and resolutions |

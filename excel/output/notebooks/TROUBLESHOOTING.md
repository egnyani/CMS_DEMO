# CMS Medicaid Engagement — Troubleshooting Guide

Common issues, their causes, and resolutions for each notebook.

---

## Notebook 1: Ingestion

### "Path does not exist" on CSV read

**Symptom:**
```
AnalysisException: Path does not exist: /Volumes/workspace/default/landing/dim_medicaid_recipient.csv
```

**Cause:** The `source_path` widget doesn't point to where CSVs were uploaded.

**Fix:**
1. Verify the volume exists: go to **Catalog > workspace > default > Volumes > landing**.
2. Confirm all 7 CSV files are in the volume.
3. Update the `source_path` widget to the correct path and rerun.
4. If using DBFS instead of Volumes, set `source_path` to `/FileStore/<your-folder>/`.

---

### "Cannot create catalog" or permission error

**Symptom:**
```
PERMISSION_DENIED: User does not have CREATE CATALOG permission
```

**Cause:** Your account doesn't have metastore-level `CREATE CATALOG` rights.

**Fix:**
- Ask a workspace admin to create the catalog for you, or
- Change the `catalog` widget to an existing catalog you have access to.
- You still need `CREATE SCHEMA` and `CREATE TABLE` on that catalog.

---

### Row count mismatch assertion

**Symptom:**
```
AssertionError: Row count mismatch detected
```

**Cause:** A CSV file may be truncated or corrupted.

**Fix:**
1. Check the file on disk: `wc -l <filename>.csv` — subtract 1 for the header.
2. Compare against expected counts:
   - dim_medicaid_recipient: 10,000
   - dim_establishment: 500
   - dim_calendar_month: 6
   - dim_verification_method: 3
   - fact_engagement_activity: 99,369
   - fact_recipient_monthly_engagement: 36,097
   - fact_recipient_establishment_monthly: 89,958
3. Re-upload the CSV file and rerun.

---

### Boolean columns read as NULL

**Symptom:** Columns like `is_overlap_flag` are all NULL after ingestion.

**Cause:** CSV has `True`/`False` strings but the read may not parse them
if Spark version or locale settings interfere.

**Fix:** The notebooks use explicit `BooleanType()` in the schema definition.
Spark handles `True`/`False` (case-insensitive) by default. If still an issue:
- Verify no extra whitespace in the CSV.
- Read as `StringType` and cast: `.withColumn("is_overlap_flag", col("is_overlap_flag").cast("boolean"))`.

---

### Schema evolution errors on rerun

**Symptom:**
```
AnalysisException: The specified schema does not match the existing schema
```

**Cause:** A previous run wrote the table with a different schema.

**Fix:** The notebooks use `.option("overwriteSchema", "true")` which should handle
this automatically. If it persists:
```sql
DROP TABLE IF EXISTS workspace.default.dim_medicaid_recipient;
```
Then rerun the notebook.

---

## Notebook 2: Semantic Views

### "Table or view not found"

**Symptom:**
```
AnalysisException: Table or view 'workspace.default.dim_medicaid_recipient' not found
```

**Cause:** Notebook 1 wasn't run, or used different widget values.

**Fix:**
1. Run Notebook 1 first.
2. Ensure `catalog`, `schema`, and `table_prefix` widgets match between Notebooks 1 and 2.

---

### Suspicious pattern count assertion fails

**Symptom:**
```
AssertionError: Overlap count mismatch: 8800 vs 8859
```

**Cause:** Data was regenerated with a different seed, or CSVs were modified.

**Fix:**
- If you regenerated the dataset, update the expected values in Notebook 2's
  validation cell 4 (lines 635-637) to match the new `validation_summary.json`.
- If using the original dataset, re-upload the unmodified CSVs and rerun Notebook 1.

---

### View returns zero rows

**Symptom:**
```
AssertionError: View vw_today_registrations returned 0 rows!
```

**Cause:** `vw_today_registrations` filters on `registration_date = CURRENT_DATE()`.
Since the demo dataset has registration dates in Oct 2025 – Mar 2026, this view
will only return data if today falls within that range.

**Fix:** This is expected behavior. The spot-check validation in Notebook 2
deliberately excludes `vw_today_registrations` from its check list. If you added
it manually, remove it or adjust the date filter in the view definition.

---

### "Cannot create view" permissions

**Symptom:**
```
PERMISSION_DENIED: User does not have CREATE VIEW permission
```

**Fix:** You need `CREATE VIEW` (or `ALL PRIVILEGES`) on the target schema.
Ask a workspace admin to grant:
```sql
GRANT CREATE VIEW ON SCHEMA workspace.default TO `your-email@company.com`;
```

---

## Notebook 3: Dashboard Creation

### "Databricks SDK not available"

**Symptom:**
```
Databricks SDK not available. Falling back to Option B (manual).
```

**Cause:** The `databricks-sdk` package isn't installed on the cluster.

**Fix (option 1):** Install via cluster library:
```
%pip install databricks-sdk --upgrade
```
Then restart the Python interpreter and rerun.

**Fix (option 2):** Use DBR 13.3 LTS or later, which includes the SDK
pre-installed.

**Fix (option 3):** Use the manual fallback — follow `DASHBOARD_LAYOUT_GUIDE.md`
with the SQL queries printed in the notebook output.

---

### SDK dashboard creation fails with permission error

**Symptom:**
```
SDK dashboard creation failed: PERMISSION_DENIED
```

**Cause:** Your token or user doesn't have permission to create dashboards via API.

**Fix:**
- Ensure you have `CAN_MANAGE` permissions on dashboards.
- If using a service principal, it needs workspace-level dashboard access.
- Fall back to manual creation using `DASHBOARD_LAYOUT_GUIDE.md`.

---

### Dashboard created but publish fails

**Symptom:**
```
Dashboard created but publish step failed (may require manual publish)
```

**Cause:** Some workspaces require explicit credentials or a warehouse assignment
to publish.

**Fix:**
1. Open the dashboard URL printed in the notebook output.
2. In the dashboard editor, click **Publish** in the top-right corner.
3. If prompted, select a SQL warehouse for the published dashboard.

---

### Dashboard shows "Query failed" for widgets

**Symptom:** Dashboard loads but individual widgets show error states.

**Cause:** The SQL warehouse used by the dashboard can't access the views.

**Fix:**
1. Verify the SQL warehouse has access to the catalog:
   ```sql
   USE CATALOG workspace;
   SELECT * FROM workspace.default.vw_enrollment_summary LIMIT 1;
   ```
2. If the warehouse uses a different catalog default, ensure queries use
   fully qualified names (they do by default in Notebook 3's queries).
3. Grant the warehouse's service principal access:
   ```sql
   GRANT USAGE ON CATALOG workspace TO `warehouse-service-principal`;
   GRANT USAGE ON SCHEMA workspace.default TO `warehouse-service-principal`;
   GRANT SELECT ON SCHEMA workspace.default TO `warehouse-service-principal`;
   ```

---

### Charts show wrong sort order (months not chronological)

**Symptom:** X-axis shows months alphabetically (Dec, Feb, Jan...) instead of
chronologically.

**Fix:** In the Lakeview chart settings:
1. Click the widget to edit it.
2. In **X-axis** settings, change the sort field from `month_name` to
   `calendar_month_key` (ascending).
3. This ensures Oct 2025 → Nov 2025 → ... → Mar 2026 ordering.

---

## General Issues

### Cluster / warehouse disconnects mid-run

**Fix:** All notebooks are idempotent. Simply reconnect and click **Run All** again.
Tables are overwritten, views are replaced, and the dashboard is updated in place.

---

### Widgets don't appear at the top of the notebook

**Cause:** Databricks renders widgets after the first cell executes.

**Fix:**
1. Run the first cell (widget definitions) manually.
2. Set values in the widget bar that appears at the top.
3. Then click **Run All** or run remaining cells.

---

### "No such file" when using Repos / Workspace Files

**Cause:** Repo-relative paths differ from Volume/DBFS paths.

**Fix:** If your CSVs are in a Databricks Repo, set `source_path` to:
```
/Workspace/Repos/<your-email>/excel/output/
```
Or use the file browser to find the exact path.

---

## Support Contacts

For issues beyond this guide:
- **Databricks docs:** https://docs.databricks.com
- **Lakeview API:** https://docs.databricks.com/en/dashboards/lakeview-api.html
- **Unity Catalog permissions:** https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/index.html

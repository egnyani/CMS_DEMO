# CMS Medicaid Engagement — Dashboard Layout Guide

Step-by-step instructions for building the Lakeview dashboard manually in the
Databricks UI. Use this guide when Notebook 3's programmatic creation (Option A)
is not available, or when you want to fine-tune the layout.

---

## Getting Started

1. In the Databricks sidebar, navigate to **SQL > Dashboards**.
2. Click **Create dashboard** (this creates a Lakeview / AI-BI dashboard).
3. Name it: **CMS Medicaid Engagement Dashboard** (or your preferred name).
4. Select a SQL Warehouse as the compute source.

---

## Recommended Layout

The dashboard is organized into 6 sections, each as a canvas tab or as sections
within a single-page layout. The suggested approach is a **single-page layout
with section headers**.

### Canvas Grid

Lakeview uses a 12-column grid. Each visualization below includes a suggested
width and position. Adjust as needed.

---

## Section 1: Recipient & Enrollment Metrics

Add a **Text widget** at the top: `## Recipient & Enrollment Metrics`

### 1.1 — Recipients by Enrollment Type

| Property | Value |
|---|---|
| **Query name** | `enrollment_summary` |
| **Chart type** | Bar (horizontal) |
| **X-axis** | `enrollment_type_code` |
| **Y-axis** | `distinct_recipients` |
| **Grid position** | Row 1, Col 1-6 |

```sql
SELECT enrollment_type_code, distinct_recipients, total_records
FROM workspace.default.vw_enrollment_summary
ORDER BY distinct_recipients DESC
```

### 1.2 — Monthly Registrations by Enrollment Type

| Property | Value |
|---|---|
| **Query name** | `monthly_registrations` |
| **Chart type** | Line |
| **X-axis** | `registration_month` |
| **Y-axis** | `registrations` |
| **Color/group** | `enrollment_type_code` |
| **Grid position** | Row 1, Col 7-12 |

```sql
SELECT registration_month, enrollment_type_code, registrations
FROM workspace.default.vw_monthly_registrations
ORDER BY registration_month
```

---

## Section 2: Activity & Establishment Insights

Add a **Text widget**: `## Activity & Establishment Insights`

### 2.1 — Activity Volume by Type

| Property | Value |
|---|---|
| **Query name** | `activity_by_type` |
| **Chart type** | Bar (horizontal) |
| **X-axis** | `activity_type` |
| **Y-axis** | `activity_count` |
| **Grid position** | Row 2, Col 1-6 |

```sql
SELECT activity_type, distinct_recipients, activity_count, total_hours
FROM workspace.default.vw_activity_by_type
ORDER BY activity_count DESC
```

### 2.2 — Establishment Summary by Type

| Property | Value |
|---|---|
| **Query name** | `establishment_by_type` |
| **Chart type** | Bar (horizontal) |
| **X-axis** | `establishment_type` |
| **Y-axis** | `activity_count` |
| **Grid position** | Row 2, Col 7-12 |

```sql
SELECT establishment_type, establishment_count, distinct_recipients, activity_count, total_hours
FROM workspace.default.vw_establishment_by_type
ORDER BY activity_count DESC
```

### 2.3 — Top 20 Establishments by Recipient Count

| Property | Value |
|---|---|
| **Query name** | `top_establishments` |
| **Chart type** | Table |
| **Grid position** | Row 3, Col 1-6 |

```sql
SELECT establishment_name, establishment_type, establishment_state_code,
       distinct_recipients, total_hours
FROM workspace.default.vw_recipients_per_establishment
ORDER BY distinct_recipients DESC
LIMIT 20
```

### 2.4 — Recipients & Hours by State

| Property | Value |
|---|---|
| **Query name** | `recipients_by_state` |
| **Chart type** | Bar |
| **X-axis** | `state_code` |
| **Y-axis** | `distinct_recipients` |
| **Grid position** | Row 3, Col 7-12 |

```sql
SELECT state_code, distinct_recipients, activity_count, total_hours
FROM workspace.default.vw_recipients_per_state
ORDER BY distinct_recipients DESC
```

---

## Section 3: Hours & Engagement Metrics

Add a **Text widget**: `## Hours & Engagement Metrics`

### 3.1 — Average Monthly Hours per Recipient (Trend)

| Property | Value |
|---|---|
| **Query name** | `avg_hours_trend` |
| **Chart type** | Line (with reference line at 80) |
| **X-axis** | `month_name` (sort by `calendar_month_key`) |
| **Y-axis** | `avg_monthly_hours` |
| **Reference line** | `required_monthly_hours` = 80 |
| **Grid position** | Row 4, Col 1-6 |

```sql
SELECT month_name, calendar_month_key, recipients,
       avg_monthly_hours, total_monthly_hours, required_monthly_hours
FROM workspace.default.vw_avg_hours_per_recipient
ORDER BY calendar_month_key
```

> **Tip:** Add `required_monthly_hours` as a constant line on the Y-axis
> to visually show the 80-hour threshold.

### 3.2 — Hours Distribution Buckets by Month

| Property | Value |
|---|---|
| **Query name** | `hours_distribution` |
| **Chart type** | Stacked bar |
| **X-axis** | `month_name` |
| **Y-axis** | `recipients` |
| **Color/group** | `hours_bucket` |
| **Grid position** | Row 4, Col 7-12 |

```sql
SELECT month_name, hours_bucket, recipients
FROM workspace.default.vw_hours_distribution
ORDER BY calendar_month_key, hours_bucket
```

---

## Section 4: Age Analysis

Add a **Text widget**: `## Age Analysis`

### 4.1 — Activity by Age Group and Type

| Property | Value |
|---|---|
| **Query name** | `age_activity_summary` |
| **Chart type** | Grouped bar |
| **X-axis** | `age_group` |
| **Y-axis** | `total_hours` |
| **Color/group** | `activity_type` |
| **Grid position** | Row 5, Col 1-6 |

```sql
SELECT age_group, activity_type, distinct_recipients, activity_count,
       total_hours, avg_hours_per_activity
FROM workspace.default.vw_age_group_summary
ORDER BY age_group, activity_type
```

### 4.2 — Monthly Hours by Age Group

| Property | Value |
|---|---|
| **Query name** | `age_monthly_trend` |
| **Chart type** | Line |
| **X-axis** | `month_name` (sort by `calendar_month_key`) |
| **Y-axis** | `total_hours` |
| **Color/group** | `age_group` |
| **Grid position** | Row 5, Col 7-12 |

```sql
SELECT age_group, month_name, calendar_month_key, distinct_recipients,
       total_hours, avg_hours
FROM workspace.default.vw_age_group_monthly
ORDER BY calendar_month_key, age_group
```

---

## Section 5: Fraud / Suspicious Patterns

Add a **Text widget**: `## Fraud / Suspicious Patterns`

### 5.1 — Overlapping Activity by State and Month

| Property | Value |
|---|---|
| **Query name** | `overlap_summary` |
| **Chart type** | Table |
| **Grid position** | Row 6, Col 1-6 |

```sql
SELECT state_code, month_name, recipients_with_overlap,
       overlap_activity_count, overlap_total_hours
FROM workspace.default.vw_suspicious_overlap_summary
ORDER BY calendar_month_key, state_code
```

### 5.2 — Suspicious Exactly-80-Hours Consecutive Streaks

| Property | Value |
|---|---|
| **Query name** | `exact_80_streaks` |
| **Chart type** | Table |
| **Grid position** | Row 6, Col 7-12 |

```sql
SELECT medicaid_recipient_key, msis_identification_num, eligible_state_code,
       streak_start_month, streak_end_month, streak_length
FROM workspace.default.vw_suspicious_exact_80_consecutive
ORDER BY streak_length DESC
```

### 5.3 — Dominant Single-Establishment Recipients (>= 90%)

| Property | Value |
|---|---|
| **Query name** | `dominant_establishment` |
| **Chart type** | Table |
| **Grid position** | Row 7, Col 1-6 |

```sql
SELECT medicaid_recipient_key, msis_identification_num, eligible_state_code,
       establishment_name, establishment_type, month_name,
       establishment_share_of_monthly_hours
FROM workspace.default.vw_suspicious_dominant_establishment
ORDER BY establishment_share_of_monthly_hours DESC
LIMIT 50
```

### 5.4 — Recipients with Multiple Suspicious Flags

| Property | Value |
|---|---|
| **Query name** | `multi_flag_recipients` |
| **Chart type** | Table |
| **Highlight rule** | Conditionally color `flag_count >= 3` in red |
| **Grid position** | Row 7, Col 7-12 |

```sql
SELECT medicaid_recipient_key, msis_identification_num, eligible_state_code,
       enrollment_type_code, has_overlap, has_exact_80_streak,
       has_dominant_establishment, flag_count
FROM workspace.default.vw_suspicious_multi_flag
WHERE flag_count >= 2
ORDER BY flag_count DESC
```

---

## Section 6: Expected vs Reported Hours

Add a **Text widget**: `## Expected vs Reported Hours`

### 6.1 — Expected vs Reported Hours by State

| Property | Value |
|---|---|
| **Query name** | `expected_vs_reported_state` |
| **Chart type** | Table |
| **Grid position** | Row 8, Col 1-6 |

```sql
SELECT state_code, month_name, recipients, total_reported_hours,
       total_expected_hours, pct_of_expected
FROM workspace.default.vw_expected_vs_reported_by_state
ORDER BY calendar_month_key, state_code
```

### 6.2 — Expected vs Reported Hours — Overall Trend

| Property | Value |
|---|---|
| **Query name** | `expected_vs_reported_total` |
| **Chart type** | Line (dual axis recommended) |
| **X-axis** | `month_name` |
| **Y-axis (left)** | `total_reported_hours`, `total_expected_hours` |
| **Y-axis (right)** | `pct_of_expected` |
| **Grid position** | Row 8, Col 7-12 |

```sql
SELECT month_name, recipients, total_reported_hours,
       total_expected_hours, pct_of_expected
FROM workspace.default.vw_expected_vs_reported_total
ORDER BY calendar_month_key
```

---

## Final Steps

1. **Review all visualizations** — click each widget to verify data appears.
2. **Publish the dashboard** — click the **Publish** button in the top-right corner.
3. **Share** — use the **Share** button to grant access to other workspace users
   or groups. Viewers need at least `SELECT` permission on the views in
   `workspace.default`.

---

## Tips for Lakeview Dashboards

- **Sorting:** When the X-axis is `month_name`, always sort by `calendar_month_key`
  to get chronological order (Lakeview supports field-based sorting).
- **Filters:** Add a global filter on `calendar_month_key` or `month_name` to
  allow interactive month selection.
- **Conditional formatting:** For tables with `flag_count` or `pct_of_expected`,
  use conditional formatting rules to highlight outlier values.
- **Text widgets:** Use markdown-enabled text widgets for section headers and
  brief explanations of each section's purpose.
- **Refresh schedule:** Under dashboard settings, configure an automatic refresh
  (hourly, daily) if the underlying data will be updated.

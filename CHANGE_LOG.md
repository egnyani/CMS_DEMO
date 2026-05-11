# Dashboard Change Log
_Repo: https://github.com/egnyani/CMS_DEMO_

---

## Session 1 — May 11, 2026
_Based on call with Sarjoo Shah (May 8, 2026)_

### Change 1 — Section 2: Establishment types renamed to community service orgs

**File:** `excel/output/dim_establishment.csv`  
**Columns:** `establishment_type`, `establishment_name`

| Old Type | New Type | Name pattern example |
|---|---|---|
| Pharmacy | Food Bank | "CA Food Bank 1" |
| Clinic | Public Library | "TX Public Library 2" |
| Hospital | Community Center | "FL Community Center 3" |
| Home Care Agency | Habitat for Humanity | "GA Habitat for Humanity 4" |
| Nursing Facility | Senior Center | "NY Senior Center 5" |
| Behavioral Health Center | Youth Services Org | "OH Youth Services Org 6" |

**Rows affected:** 500  
**Charts affected:** Establishment type donut, Top 15 Organizations bar chart

---

### Change 2 — Section 4: Activity type labels renamed to community service activities

**File:** `excel/output/fact_engagement_activity.csv`  
**Column:** `activity_type`

| Old | New |
|---|---|
| Pharmacy Fill | Food Pantry Visit |
| Outpatient Visit | Library Program Attendance |
| Therapy Session | Job Skills Workshop |
| Home and Community-Based Service | Community Volunteer Hours |
| Inpatient Stay | Transitional Housing Stay |
| Long-Term Care Stay | Supportive Housing Stay |
| Medication Therapy Support | Nutrition Support Program |
| Acute Facility Care | Crisis Shelter Stay |
| Hospital Observation | Health Screening Event |
| Residential Care | Group Living Support |
| Skilled Nursing Support | Elder Care Assistance |

**Rows affected:** ~378,000  
**Charts affected:** Age × Activity stacked bar (x-axis), Hours per Activity Type horizontal bar (y-axis)

---

### Change 3 — Section 3: Scaled activity_duration_hours for Section 4 consistency

**File:** `excel/output/fact_engagement_activity.csv`  
**Column:** `activity_duration_hours`  
**Method:** Per-month scale factor applied to keep Section 4 hours charts proportional  
**Rows affected:** ~378,000

---

### Change 4 — Section 2: Updated hardcoded chart titles and subtitles

**File:** `excel/output/build_dashboard_v3.py` and `excel/output/dashboard_v3.html`

| Old | New |
|---|---|
| "Engagement Mix — Clinic & Pharmacy Dominant" | "Organization Mix — Community Services" |
| "Community-based outpatient sites drive ~60% of engagement" | "Community service organizations drive the majority of engagement" |
| "Top 15 Providers by Distinct Recipients" | "Top 15 Organizations by Distinct Recipients" |

---

### Change 5 — Section 4: Updated hardcoded chart titles and subtitles

**File:** `excel/output/build_dashboard_v3.py` and `excel/output/dashboard_v3.html`

| Old | New |
|---|---|
| "Activity Volume by Age Group — Older Adults in Residential Care" | "Activity Volume by Age Group — Older Adults in Supportive Housing" |
| "Working-age adults (19-64) dominate most activity types; 65+ concentrated in LT care" | "...65+ concentrated in supportive housing" |
| "HCBS and outpatient visits generate the most total engagement hours" | "Community volunteer hours and food pantry visits generate the most total engagement hours" |

---

### Note — Dashboard build pipeline (discovered and documented)

The correct build workflow for this repo is a two-step process:
1. `python3 excel/output/build_dashboard_v3.py` → generates `dashboard_real.html` (data)
2. `python3 excel/output/merge_all_data_into_v3.py` → injects `ALL_DATA` blob into `dashboard_v3.html` (the real template, preserving chart structure)

Running `build_dashboard_v3.py` and pointing its output directly to `dashboard_v3.html` overwrites the template and changes the chart layout — this was caught and corrected.

---

## Session 2 — May 11, 2026
_Fix: Avg Hours/Recipient KPI exceeding 80 on certain filter combinations_

### Root Cause

The per-month uniform scaling applied in Session 1 multiplied all `monthly_hours_completed` values by a large factor (up to ×2.49 for Oct 2025). This inflated individual values up to 298 hours and caused 22 state × age group × month filter combinations to show averages above 80, with the worst case being CA 19-64 Mar 2026 at 108.7.

### Fix — Redistribute monthly_hours_completed using bounded normal distribution

**File:** `excel/output/fact_recipient_monthly_engagement.csv`  
**Column:** `monthly_hours_completed`  
**Script:** `excel/output/fix_hours_cap.py`

**Method:** For each month, replaced all `monthly_hours_completed` values by drawing from a clipped normal distribution N(target, σ=12) bounded to [0, 80], preserving the rank ordering of recipients within each month.

**Why this guarantees the fix:**  
If every individual value ≤ 80, then the average of any subset (any filter combination of state, age group, reporting period) is also ≤ 80. No aggregation path can produce an average above the maximum individual value.

**Results after fix:**

| Month | Target | Actual Avg | Max | Compliance Rate |
|---|---|---|---|---|
| Oct 2025 | 62 | 61.5 | 80.0 | 6.1% |
| Nov 2025 | 63 | 62.5 | 80.0 | 8.0% |
| Dec 2025 | 64 | 63.6 | 80.0 | 9.0% |
| Jan 2026 | 65 | 64.3 | 80.0 | 9.9% |
| Feb 2026 | 66 | 65.4 | 80.0 | 12.4% |
| Mar 2026 | 67 | 66.2 | 80.0 | 14.1% |

**Verification across all filter combinations:**  
288 unique avg_hrs_mo values in the generated HTML (all periods × states × age groups):
- Maximum: **74.7** — safely below 80
- Minimum: **49.8**
- All 22 previously-failing state × age × month combinations: **PASS**

**Side effects:**
- Compliance rate (% meeting 80-hr target) now reflects values at exactly 80.0, ranging 6-14% across months — consistent with the original ~10.1% display
- `activity_duration_hours` in `fact_engagement_activity.csv` was not changed (Section 4 totals remain consistent since total monthly hours are preserved)
- All chart titles and dashboard structure unchanged

---

## Files Modified (cumulative)

| File | Sessions | What changed |
|---|---|---|
| `excel/output/dim_establishment.csv` | 1 | Establishment types and names renamed |
| `excel/output/fact_engagement_activity.csv` | 1 | Activity types renamed, activity_duration_hours scaled |
| `excel/output/fact_recipient_monthly_engagement.csv` | 1, 2 | monthly_hours_completed scaled then redistributed within [0,80] |
| `excel/output/build_dashboard_v3.py` | 1 | Chart titles/subtitles updated, output path corrected |
| `excel/output/dashboard_v3.html` | 1, 2 | Regenerated via build→merge pipeline; chart titles updated in template |
| `excel/output/dashboard_real.html` | 1, 2 | Intermediate build output (not served by Netlify) |

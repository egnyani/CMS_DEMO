# CMS Demo Dashboard — Change Plan
_Based on call with Sarjoo Shah, May 8 2026_

---

## Overview

Three changes are needed to the dashboard at https://community-engagement-dashboard.netlify.app/

**How the system works:**
Source CSVs live in `excel/output/`. The Python script `build_dashboard_v3.py` reads those CSVs and generates `dashboard_v3.html`, which Netlify serves directly (no build step). So the workflow for every change is:

```
Edit CSVs → run build_dashboard_v3.py → commit HTML + CSVs → push to main → Netlify deploys
```

---

## Change 1 — Section 2: Rename Establishments to Community Service Orgs

**Why:** Establishment types and names currently reflect healthcare (Pharmacy, Clinic, Hospital, etc.). They need to reflect community service organizations.

**File:** `excel/output/dim_establishment.csv` (500 rows)

**Columns to update:** `establishment_type` and `establishment_name`

| Current Type | New Type | Name Pattern |
|---|---|---|
| Pharmacy | Food Bank | "CA Food Bank 1" |
| Clinic | Public Library | "TX Public Library 2" |
| Hospital | Community Center | "FL Community Center 3" |
| Home Care Agency | Habitat for Humanity | "GA Habitat for Humanity 4" |
| Nursing Facility | Senior Center | "NY Senior Center 5" |
| Behavioral Health Center | Youth Services Org | "OH Youth Services Org 6" |

**Counts affected:**
- Pharmacy: 143 rows
- Clinic: 142 rows
- Home Care Agency: 78 rows
- Hospital: 65 rows
- Nursing Facility: 46 rows
- Behavioral Health Center: 26 rows

**Dashboard sections affected:**
- Establishment type donut chart (Section 2)
- Top 15 Providers chart (Section 2)
- Engagement mix display (Section 2)

---

## Change 2 — Section 4: Rename Activity Type Labels

**Why:** Activity types currently reflect medical services. They need to reflect community service activities.

**File:** `excel/output/fact_engagement_activity.csv` (~378,000 rows)

**Column to update:** `activity_type`

| Current | New |
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

**Dashboard sections affected:**
- Age × Activity stacked bar — x-axis labels (Section 4)
- Hours per Activity Type horizontal bar — y-axis labels (Section 4)

---

## Change 3 — Section 3: Scale Avg Hours / Recipient to 60–70 per Reporting Period

**Why:** The "Avg Hours / Recipient" KPI card currently shows ~45. Sarjoo requested it show ~60. The reporting period filter at the top changes which month's data is shown, so each individual month must average 60–70 independently.

**Files:**
- `excel/output/fact_recipient_monthly_engagement.csv` (43,780 rows) — column: `monthly_hours_completed`
- `excel/output/fact_engagement_activity.csv` (~378,000 rows) — column: `activity_duration_hours` (scaled by same factor to keep Section 4 hours charts consistent)

**Implementation:** Scale each month's rows by a per-month factor so the average lands in the 60–70 range. Using a gradual ramp-up pattern to reflect a program growing over time:

| Reporting Period | calendar_month_key | Current Avg | Target Avg | Scale Factor |
|---|---|---|---|---|
| Oct 2025 | 202510 | 24.85 | 62 | ×2.49 |
| Nov 2025 | 202511 | 30.93 | 63 | ×2.04 |
| Dec 2025 | 202512 | 34.41 | 64 | ×1.86 |
| Jan 2026 | 202601 | 37.61 | 65 | ×1.73 |
| Feb 2026 | 202602 | 40.11 | 66 | ×1.65 |
| Mar 2026 | 202603 | 41.74 | 67 | ×1.60 |

**Aggregated views** (Last 3 Months, Last 6 Months) are weighted averages of individual months — with all months in 60–70, these will naturally land around 65–66.

---

## Change 4 — Section 2: Update Hardcoded Chart Titles & Subtitles

**Why:** Several chart titles and subtitles in Section 2 contain hardcoded healthcare terminology that won't update automatically when the CSV data changes. These are static strings in `build_dashboard_v3.py`.

**File:** `excel/output/build_dashboard_v3.py`

| Element | Current Text | New Text |
|---|---|---|
| Chart title | "Provider Mix — Clinic & Pharmacy Dominant" | "Organization Mix — Community Services" |
| Chart subtitle | "Community-based outpatient sites drive ~60% of engagement" | "Community service organizations drive the majority of engagement" |
| Chart title | "Top 15 Providers by Distinct Recipients" | "Top 15 Organizations by Distinct Recipients" |

---

## Change 5 — Section 4: Update Hardcoded Chart Titles & Subtitles

**Why:** Same issue as Change 4 — chart titles and subtitles in Section 4 reference specific healthcare activity names (Residential Care, LT care, HCBS, outpatient visits) that will be renamed in the data but not in these static strings.

**File:** `excel/output/build_dashboard_v3.py`

| Element | Current Text | New Text |
|---|---|---|
| Chart title | "Activity Volume by Age Group — Older Adults in Residential Care" | "Activity Volume by Age Group — Older Adults in Supportive Housing" |
| Chart subtitle | "Working-age adults (19-64) dominate most activity types; 65+ concentrated in LT care" | "Working-age adults (19-64) dominate most activity types; 65+ concentrated in supportive housing" |
| Chart subtitle | "HCBS and outpatient visits generate the most total engagement hours" | "Community volunteer hours and food pantry visits generate the most total engagement hours" |

---

## Execution Steps

| Step | Action | Details |
|---|---|---|
| 1 | Clone repo | `git clone https://github.com/egnyani/CMS_DEMO.git` |
| 2 | Write migration script | Single Python script (`migrate_data.py`) applies all 5 CSV changes |
| 3 | Run migration script | Updates `dim_establishment.csv`, `fact_engagement_activity.csv`, `fact_recipient_monthly_engagement.csv` |
| 4 | Run builder | `python3 excel/output/build_dashboard_v3.py` — regenerates `dashboard_v3.html` |
| 5 | Verify | Open `dashboard_v3.html` locally, check all 3 sections |
| 6 | Commit & push | Commit updated CSVs + HTML to `main` |
| 7 | Netlify deploys | Auto-triggered on push, no build step needed |

---

## Files Changed Summary

| File | Change |
|---|---|
| `excel/output/dim_establishment.csv` | Rename establishment types and names (Change 1) |
| `excel/output/fact_engagement_activity.csv` | Rename activity types + scale activity_duration_hours (Changes 2 & 3) |
| `excel/output/fact_recipient_monthly_engagement.csv` | Scale monthly_hours_completed per month (Change 3) |
| `excel/output/build_dashboard_v3.py` | Update hardcoded chart titles and subtitles (Changes 4 & 5) |
| `excel/output/dashboard_v3.html` | Regenerated output — committed after builder runs |

when selecting certain reporting periods or states, the average hours KPI goes above 80, which should not happen.

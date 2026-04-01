# Medicaid Community Engagement Dashboard — Review & Improvement Guide

**Reviewed by:** Claude
**Data period:** Oct 2025 – Mar 2026
**Recipients:** 10,000 | **Activity Records:** 378,228 | **Establishments:** 500

---

## Summary

The dashboard covers all 6 intended question areas and is structurally well-organized. However, a handful of charts are redundant or low-signal and should be removed or replaced, several important visualizations are entirely missing, and two existing charts contain data display bugs. The sections below walk through every finding.

---

## 1. Charts That Should Be Removed (or Significantly Simplified)

### 1a. Recipient Status Breakdown Pie (Section 1)
**Why it's redundant:** This pie shows Active / Inactive / New — but the same numbers already appear in two KPI cards directly above it ("New Registrations" and "New vs Returning"). A third representation of the same three numbers adds no insight.
**Recommendation:** Remove it. Use the freed space for a more meaningful chart (see Section 3 below).

---

### 1b. Age Distribution Donut (Section 2 — "Activity & Establishment Insights")
**Why it's redundant / misplaced:** The age groups (0-18, 19-64, 65+) are already baked into the interactive age toggle filter at the top of the dashboard. The donut simply shows what percentage is in each bucket — information the user can infer from the toggle itself. More importantly, Section 4 already shows age broken down by activity type, which is far more informative.
**Why it's in the wrong section:** Age distribution belongs in Section 1 (Enrollment) or Section 4 (Age & Activity), not Section 2 (Establishment Insights).
**Recommendation:** Remove the standalone donut. If you want age context in Section 2, embed it as a small summary note or fold it into the stacked activity bar in Section 4.

---

### 1c. Enrollment Program by State Table (Section 7)
**Why it's low-value:** The data shows that every state has almost exactly the same program mix — 71–75% Medicaid, 16–19% Expansion CHIP, 8–12% Separate CHIP. There is essentially zero variation across the 8 states. A table with 8 identical rows teaches the reader nothing.

| State | Medicaid | Exp. CHIP | Sep. CHIP |
|-------|----------|-----------|-----------|
| CA | 71.9% | 17.5% | 10.7% |
| TX | 72.6% | 18.2% | 9.2% |
| OH | 71.0% | 19.0% | 10.0% |
| ... | ... | ... | ... |

**Recommendation:** Either remove this section entirely, or replace it with a grouped bar chart showing *absolute counts* (not percentages). The count differences between CA (2,224) and GA (735) are meaningful; the percentage mix is not.

---

### 1d. Concurrent Billing by Verification Method Chart (Section 5 — Fraud)
**Why it's low-value:** All three verification methods show almost identical overlap rates:
- Manual: 15.9%
- Electronic: 15.9%
- API: 16.0%

There is no signal here. The chart currently supports the narrative that "manual verification correlates with higher overlap flag rates" — but that is not what the data actually shows.
**Recommendation:** Replace this with "Concurrent Billing (Overlap Flags) by State," which does show variation (CA highest at 16.6%, FL lowest at 15.2%) and is more actionable for state-level oversight.

---

## 2. Charts That Have Bugs / Need Fixing

### 2a. State × Month Reporting Ratio Heatmap (Section 6) — DATA BUG
**The problem:** The heatmap only shows two columns — "Mar 2026" and "6-Mo Avg" — and both columns contain the same value for every state. This makes no sense and appears to be a rendering or data-generation bug.

The actual data shows a significant and interesting ramp-up trend across all 6 months:

| State | Oct 2025 | Nov 2025 | Dec 2025 | Jan 2026 | Feb 2026 | Mar 2026 |
|-------|----------|----------|----------|----------|----------|----------|
| CA | 33.4% | 42.0% | 46.6% | 51.2% | 54.1% | 56.7% |
| OH | 34.7% | 43.7% | 48.0% | 51.5% | 54.7% | 55.4% |
| NY | 32.4% | 41.7% | 46.3% | 51.6% | 54.7% | 57.3% |
| TX | 34.0% | 40.9% | 46.1% | 50.4% | 53.6% | 55.6% |

**Recommendation:** Fix the heatmap to show all 6 months as columns. The progression from ~33% in October to ~56% in March is one of the most compelling stories in the entire dataset. This is a very high-priority fix.

---

### 2b. 80-Hour Compliance Funnel (Section 2) — Wrong Section Placement
**The problem:** The compliance funnel is placed in the "Activity & Establishment Insights" section (Section 2), but it logically belongs in Section 3 (Hours & Engagement). A viewer browsing Section 2 for establishment data will be confused by an hours-compliance metric dropped in the middle.
**Recommendation:** Move this chart to Section 3 and replace its slot in Section 2 with the new "Recipients Reached per Activity Type" chart described below.

---

## 3. Charts That Are Missing and Should Be Added

### 3a. 🔴 HIGH PRIORITY — Monthly Activity Type Volume Trend
**What it shows:** How the volume of each activity type changes month over month.
**Why it matters:** The data shows a strong ramp-up across all activity types from Oct 2025 to Mar 2026 (e.g., Pharmacy Fill went from ~18K to ~25K/month). Spotting which types are growing faster than others helps identify engagement acceleration or anomalous spikes.
**Chart type:** Multi-line or stacked area chart. X-axis = month, Y-axis = activity count. One line per activity type (or top 5 types for readability).
**Section:** 2 — Activity & Establishment Insights.

---

### 3b. 🔴 HIGH PRIORITY — Distinct Recipients Reached per Activity Type
**What it shows:** How many unique recipients participated in each activity type.
**Why it matters:** Volume and reach are different things. Pharmacy Fill has the highest volume (~118K records) *and* the highest reach (9,856 distinct recipients). But Skilled Nursing Support has only 2,313 records and only reaches 1,954 recipients — meaning some populations are being served by a very narrow set of activity types. The current dashboard only shows volume, not reach.
**Data available:** Directly computable from `fact_engagement_activity` — `activities.groupby('activity_type')['medicaid_recipient_key'].nunique()`
**Chart type:** Horizontal bar chart sorted by recipient count.
**Section:** 2 — Activity & Establishment Insights.

---

### 3c. 🔴 HIGH PRIORITY — Compliance (≥80 hrs) by Age Group Over Time
**What it shows:** What % of recipients in each age group are meeting the 80-hr monthly target, tracked month by month.
**Why it matters:** The aggregate compliance rate (10.1%) hides whether specific populations are being left behind. This directly answers Question 4 from the requirements ("Ensure [age analysis] can be grouped by activity type") and extends it to compliance tracking.
**Note:** In the current dummy data, compliance is essentially the same across all three age groups (~2.9% each), which suggests the synthetic data doesn't differentiate age-based compliance. A real dataset should show variation here.
**Chart type:** Grouped bar or multi-line chart. X-axis = month, Y-axis = % meeting 80-hr target, grouped by age.
**Section:** 3 — Hours & Engagement, or fold into Section 4 alongside the stacked activity bar.

---

### 3d. 🟡 MEDIUM PRIORITY — Expected vs Reported Hours Gap (Full 6-Month View, Stacked)
**What it shows:** For each month, the expected total hours vs. reported hours side by side, with the gap clearly visualized as a separate "shortfall" bar or shaded area.
**Why it matters:** The current Section 6 chart shows this, but the story is even stronger when expressed as an absolute gap: the shortfall started at 180K hours in October and widened to 350K hours by March — meaning the program is serving more people but the *absolute underperformance* is getting larger even as the ratio improves. This nuance is hidden in the current chart.
**Chart type:** Stacked bar (reported + gap stacked to expected), with a line showing the reporting ratio %.
**Section:** 6 — Expected vs Reported Hours.

---

### 3e. 🟡 MEDIUM PRIORITY — Fraud: Concurrent Billing (Overlap Flags) by State
**What it shows:** The percentage of activity records flagged as concurrent/overlapping, broken down by state.
**Why it matters:** CA has the highest overlap rate (16.6%) and FL the lowest (15.2%). While the spread is modest in dummy data, in real data these state-level differences are the first place investigators look.
**Data available:** `activities.groupby('claim_submitting_state_code').agg(overlaps / total)`
**Chart type:** Horizontal bar chart with color coding (red = above threshold, yellow = borderline, green = low).
**Section:** 5 — Fraud & Suspicious Pattern Detection. Replace the Verification Method chart.

---

### 3f. 🟡 MEDIUM PRIORITY — Hours per Recipient Distribution by Age Group (Violin or Box Plot)
**What it shows:** The spread of monthly hours worked — not just the average — segmented by age group.
**Why it matters:** An average of 45 hrs/month hides huge variation. Some recipients are at 5 hours; others are at 180. The current "Hours Distribution by Bracket" bar chart shows the overall distribution but doesn't break it out by age, which is the most policy-relevant cut.
**Chart type:** Grouped box plot, violin plot, or — if keeping the bar format — a small multiples (3 side-by-side histograms) approach.
**Section:** 3 — Hours & Engagement.

---

### 3g. 🟢 LOWER PRIORITY — Gender/Sex Distribution
**What it shows:** Simple breakdown: Female (57%), Male (42%), Unknown (1%).
**Why it matters:** The data has `sex_code` in `dim_medicaid_recipient` but it's completely absent from the dashboard. While not one of the 6 required question areas, it's a basic demographic metric that CMS programs routinely track — a single KPI card or a small breakdown by enrollment type would suffice.
**Data available:** `recipients['sex_code'].value_counts()` → F: 5,702 / M: 4,209 / U: 89
**Chart type:** Add as a KPI card in Section 1, or a small donut alongside the Enrollment Program Mix.
**Section:** 1 — Recipient & Enrollment Overview.

---

### 3h. 🟢 LOWER PRIORITY — Activity Type by Enrollment Program
**What it shows:** Whether Medicaid, Medicaid Expansion CHIP, and Separate CHIP recipients tend to use different types of services.
**Why it matters:** Program type and activity pattern differences have compliance and billing implications. If CHIP recipients over-index on Inpatient stays vs. traditional Medicaid recipients, that's a reportable finding.
**Chart type:** 100% stacked bar (one bar per enrollment type, segments = activity types).
**Section:** 2 or 4.

---

## 4. Structural & UX Improvements

**Period filter doesn't update static tables.** The "Enrollment by State" table in Section 7 and parts of the Fraud table appear static and don't react to the period selector dropdown. If the filter is shown, all data should respond to it — or the table should be clearly marked "Full-period snapshot."

**"Declining Enrollment Curve" label may mislead.** The Section 1 registration chart is labeled as showing a "declining enrollment curve" as if enrollment is falling, when in reality it's simply that early months had more registrations because the program was new. Relabeling it to "Enrollment Ramp-Up: New Registrations by Month" or adding a note that says "Front-loaded by program start date" would remove the ambiguity.

**The compliance funnel (80-hr target) is in the wrong section.** See 2b above — move it from Section 2 to Section 3.

**Section 2 is currently overloaded with misplaced content.** Age distribution + compliance funnel both snuck in here. After removing those, the section becomes cleaner and focused on what it promises: establishment types, state distribution, provider reach.

---

## 5. Quick Reference: What to Keep, Change, Remove

| Chart | Keep As-Is | Fix / Move | Remove |
|-------|------------|------------|--------|
| Enrollment Program Mix donut | ✅ | | |
| Monthly Registrations line | ✅ | | |
| Recipient Status pie | | | ❌ Redundant with KPIs |
| Age Distribution donut (Sec 2) | | | ❌ Redundant + misplaced |
| Provider Mix donut | ✅ | | |
| Recipients per State bar | ✅ | | |
| Top 15 Providers bar | ✅ | | |
| 80-hr Compliance Funnel | | 🔁 Move to Section 3 | |
| Monthly Avg Hours line (age) | ✅ | | |
| Hours Distribution bracket bar | ✅ | | |
| Activity Volume by Age Group | ✅ | | |
| Hours by Activity Type bar | ✅ | | |
| Fraud Trend (multi-signal) | ✅ | | |
| Concurrent Billing by Verification Method | | | ❌ No variance in data; replace |
| Fraud Table (flagged recipients) | ✅ | | |
| State Comparison Expected vs Reported | ✅ | | |
| Reporting Trajectory line | ✅ | | |
| State × Month Heatmap | | 🛠 Fix to show all 6 months | |
| Enrollment Program by State table (Sec 7) | | | ❌ All states identical % |

**New charts to add:**
- Monthly Activity Type Volume Trend (HIGH)
- Distinct Recipients per Activity Type (HIGH)
- Compliance by Age Group Over Time (HIGH)
- Expected vs Reported Gap — Absolute Stacked View (MEDIUM)
- Concurrent Billing Flags by State (MEDIUM)
- Hours Distribution by Age Group (MEDIUM)
- Sex/Gender KPI card (LOW)
- Activity Type by Enrollment Program (LOW)

---

## 6. One-Sentence Summary for Each Required Question Area

| Question | Dashboard Status |
|----------|-----------------|
| Enrollment type, total recipients, record count, monthly registrations | ✅ Fully covered in Section 1 |
| Activity type, establishment type, recipients per establishment/activity/state | ⚠️ Covered but missing "distinct recipients per activity type" chart |
| Avg hours/recipient, total monthly hours | ✅ Covered in Section 3 |
| Age group distribution, grouped by activity type | ✅ Covered in Sections 3 & 4, but compliance by age is missing |
| Fraud / suspicious pattern detection | ✅ Covered in Section 5, but Verification Method chart needs replacement |
| Expected vs reported hours by state and total | ⚠️ Covered in Section 6, but heatmap has a display bug |

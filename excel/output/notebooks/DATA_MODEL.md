# CMS Medicaid Engagement — Data Model Reference

Complete schema reference for all Delta tables and semantic views.

---

## Entity-Relationship Overview

```
dim_medicaid_recipient ──┐
                         ├──► fact_engagement_activity
dim_establishment ───────┤         │
                         │         ├──► fact_recipient_monthly_engagement
dim_calendar_month ──────┤         │
                         │         └──► fact_recipient_establishment_monthly
dim_verification_method ─┘
```

---

## Dimension Tables

### dim_medicaid_recipient

Grain: one row per Medicaid recipient.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `medicaid_recipient_key` | INT | No | Surrogate key (PK) |
| `msis_identification_num` | STRING | No | MSIS ID — unique recipient identifier |
| `submitting_state_code` | STRING | Yes | State that submitted the enrollment |
| `eligible_state_code` | STRING | Yes | State where recipient is eligible — **used as the primary state field for dashboards** |
| `date_of_birth` | DATE | Yes | Date of birth |
| `sex_code` | STRING | Yes | Sex code (M, F, U) |
| `enrollment_type_code` | STRING | Yes | Enrollment type (Medicaid, Separate CHIP, Medicaid Expansion CHIP) |
| `registration_date` | DATE | Yes | Date of registration |

**Row count:** 10,000

---

### dim_establishment

Grain: one row per establishment (provider facility).

| Column | Type | Nullable | Description |
|---|---|---|---|
| `establishment_key` | INT | No | Surrogate key (PK) |
| `submitting_state_prov_id` | STRING | Yes | State-assigned provider ID |
| `prov_location_id` | STRING | Yes | Provider location identifier |
| `service_facility_org_npi` | STRING | Yes | NPI number |
| `establishment_name` | STRING | Yes | Display name |
| `facility_group_individual_code` | STRING | Yes | Facility/group/individual code |
| `provider_classification_type` | STRING | Yes | Classification system used |
| `provider_classification_code` | STRING | Yes | Classification code |
| `establishment_type` | STRING | Yes | Type (Hospital, Clinic, Nursing Facility, etc.) |
| `establishment_state_code` | STRING | Yes | State where establishment is located |

**Row count:** 500

---

### dim_calendar_month

Grain: one row per calendar month.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `calendar_month_key` | INT | No | Surrogate key (PK) — format YYYYMM |
| `month_start_date` | DATE | Yes | First day of month |
| `month_end_date` | DATE | Yes | Last day of month |
| `year_num` | INT | Yes | Year number |
| `month_num` | INT | Yes | Month number (1-12) |
| `month_name` | STRING | Yes | Display name (e.g. "Oct 2025") |

**Row count:** 6 (Oct 2025 – Mar 2026)

---

### dim_verification_method

Grain: one row per verification method.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `verification_method_key` | INT | No | Surrogate key (PK) |
| `verification_method` | STRING | Yes | Short name (Manual, Electronic, API) |
| `verification_method_description` | STRING | Yes | Full description |

**Row count:** 3

---

## Fact Tables

### fact_engagement_activity

Grain: one row per engagement activity event (claim line level).

| Column | Type | Nullable | Description |
|---|---|---|---|
| `engagement_activity_key` | LONG | No | Surrogate key (PK) |
| `medicaid_recipient_key` | INT | No | FK → dim_medicaid_recipient |
| `establishment_key` | INT | No | FK → dim_establishment |
| `calendar_month_key` | INT | No | FK → dim_calendar_month |
| `verification_method_key` | INT | No | FK → dim_verification_method |
| `icn_orig` | STRING | Yes | Original internal control number |
| `icn_adj` | STRING | Yes | Adjusted internal control number |
| `line_num_orig` | INT | Yes | Original line number |
| `line_num_adj` | INT | Yes | Adjusted line number |
| `source_record_id` | STRING | Yes | Source record type identifier |
| `source_record_number` | STRING | Yes | Source record sequence number |
| `claim_submitting_state_code` | STRING | Yes | State that submitted the claim |
| `beginning_date_of_service` | DATE | Yes | Service start date |
| `ending_date_of_service` | DATE | Yes | Service end date |
| `activity_type` | STRING | Yes | Activity category |
| `activity_start_ts` | TIMESTAMP | Yes | Precise start timestamp |
| `activity_end_ts` | TIMESTAMP | Yes | Precise end timestamp |
| `activity_duration_hours` | DOUBLE | Yes | Duration in hours |
| `is_overlap_flag` | BOOLEAN | Yes | TRUE if this activity overlaps with another for the same recipient |

**Row count:** 99,369

**Activity types:** Therapy Session, Home and Community-Based Service, Outpatient Visit,
Day Program, Long-Term Care Stay, Inpatient Stay, RX/Pharmacy

---

### fact_recipient_monthly_engagement

Grain: one row per recipient per calendar month.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `medicaid_recipient_key` | INT | No | FK → dim_medicaid_recipient (composite PK) |
| `calendar_month_key` | INT | No | FK → dim_calendar_month (composite PK) |
| `activity_count` | INT | Yes | Number of activities in the month |
| `establishment_count` | INT | Yes | Distinct establishments visited |
| `age_group` | STRING | Yes | Age bucket: `0-18`, `19-64`, `65+` |
| `monthly_hours_completed` | DOUBLE | Yes | Total hours for the month (derived from activity fact) |
| `required_monthly_hours` | INT | Yes | Always 80 |
| `is_exactly_80_hours_flag` | BOOLEAN | Yes | TRUE if `monthly_hours_completed == 80.0` |
| `exact_80_consecutive_month_count` | INT | Yes | Running count of consecutive exactly-80 months |
| `is_exactly_80_hours_consecutive_flag` | BOOLEAN | Yes | TRUE if streak >= 3 |
| `is_single_establishment_dominant_flag` | BOOLEAN | Yes | TRUE if one establishment accounts for >= 90% of monthly hours |

**Row count:** 36,097

---

### fact_recipient_establishment_monthly

Grain: one row per recipient per establishment per calendar month.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `medicaid_recipient_key` | INT | No | FK → dim_medicaid_recipient (composite PK) |
| `establishment_key` | INT | No | FK → dim_establishment (composite PK) |
| `calendar_month_key` | INT | No | FK → dim_calendar_month (composite PK) |
| `activity_count` | INT | Yes | Activities at this establishment in this month |
| `monthly_hours_completed` | DOUBLE | Yes | Hours at this establishment in this month |
| `establishment_share_of_monthly_hours` | DOUBLE | Yes | Fraction of the recipient's monthly hours at this establishment (0.0 – 1.0) |
| `is_dominant_establishment_for_month` | BOOLEAN | Yes | TRUE if this establishment has the highest share and share >= 90% |
| `dominance_rank_in_month` | INT | Yes | Rank by hours within the month (1 = highest) |

**Row count:** 89,958

---

## Semantic Views

All views are created in `<catalog>.<schema>` with the prefix `vw_`.

### Domain 1: Recipient & Enrollment

| View | Source Tables | Key Columns |
|---|---|---|
| `vw_enrollment_summary` | dim_medicaid_recipient | enrollment_type_code, distinct_recipients, total_records |
| `vw_monthly_registrations` | dim_medicaid_recipient | registration_month, enrollment_type_code, registrations |
| `vw_today_registrations` | dim_medicaid_recipient | enrollment_type_code, registrations_today |

### Domain 2: Activity & Establishment

| View | Source Tables | Key Columns |
|---|---|---|
| `vw_activity_by_type` | fact_engagement_activity | activity_type, distinct_recipients, activity_count, total_hours |
| `vw_establishment_by_type` | fact_engagement_activity + dim_establishment | establishment_type, establishment_count, distinct_recipients |
| `vw_recipients_per_establishment` | fact_engagement_activity + dim_establishment | establishment_name, establishment_type, distinct_recipients, total_hours |
| `vw_recipients_per_activity_type` | fact_engagement_activity + dim_calendar_month | activity_type, month_name, distinct_recipients |
| `vw_recipients_per_state` | dim_medicaid_recipient + fact_engagement_activity | state_code (eligible_state_code), distinct_recipients, total_hours |

### Domain 3: Hours & Engagement

| View | Source Tables | Key Columns |
|---|---|---|
| `vw_avg_hours_per_recipient` | fact_recipient_monthly_engagement + dim_calendar_month | month_name, avg_monthly_hours, total_monthly_hours, required_monthly_hours (80) |
| `vw_hours_distribution` | fact_recipient_monthly_engagement + dim_calendar_month | hours_bucket, recipients — buckets: 0, 1-39, 40-79, Exactly 80, 81-160, 160+ |

### Domain 4: Age Analysis

| View | Source Tables | Key Columns |
|---|---|---|
| `vw_age_group_summary` | fact_recipient_monthly_engagement + fact_engagement_activity | age_group, activity_type, distinct_recipients, total_hours |
| `vw_age_group_monthly` | fact_recipient_monthly_engagement + dim_calendar_month | age_group, month_name, total_hours, avg_hours |

### Domain 5: Fraud / Suspicious Patterns

| View | Source Tables | Key Columns |
|---|---|---|
| `vw_suspicious_overlap` | fact_engagement_activity + dims | Detail rows where is_overlap_flag = TRUE |
| `vw_suspicious_overlap_summary` | fact_engagement_activity + dims | state_code, month_name, recipients_with_overlap, overlap_activity_count |
| `vw_suspicious_exact_80_consecutive` | fact_engagement_activity + dim_calendar_month + dim_medicaid_recipient | **Recomputed** streaks of exactly 80 hours (threshold >= 3 months) |
| `vw_suspicious_dominant_establishment` | fact_recipient_establishment_monthly + dims | Recipients where one establishment >= 90% of monthly hours |
| `vw_suspicious_multi_flag` | All facts + dim_medicaid_recipient | Recipients with 2+ flags (overlap, exact-80-streak, dominant) |

### Domain 6: Expected vs Reported Hours

| View | Source Tables | Key Columns |
|---|---|---|
| `vw_expected_vs_reported_by_state` | fact_recipient_monthly_engagement + dims | state_code, total_reported_hours, total_expected_hours, pct_of_expected |
| `vw_expected_vs_reported_total` | fact_recipient_monthly_engagement + dim_calendar_month | month_name, total_reported_hours, total_expected_hours, pct_of_expected |

---

## Business Rules Quick Reference

| Rule | Value | Used In |
|---|---|---|
| Required monthly hours | 80 | Expected-vs-reported views, hours distribution, exact-80 logic |
| Consecutive streak threshold | >= 3 months | `vw_suspicious_exact_80_consecutive` |
| Dominant establishment threshold | >= 90% share | `vw_suspicious_dominant_establishment`, `vw_suspicious_multi_flag` |
| State field for reporting | `eligible_state_code` | All state-based views |
| Age group buckets | 0-18, 19-64, 65+ | `vw_age_group_summary`, `vw_age_group_monthly` |

---

## Expected Validation Counts

These counts come from the generated dataset and are hard-coded in the validation
cells of Notebook 2:

| Metric | Expected Value |
|---|---|
| dim_medicaid_recipient rows | 10,000 |
| dim_establishment rows | 500 |
| dim_calendar_month rows | 6 |
| dim_verification_method rows | 3 |
| fact_engagement_activity rows | 99,369 |
| fact_recipient_monthly_engagement rows | 36,097 |
| fact_recipient_establishment_monthly rows | 89,958 |
| Overlap-flagged activity rows | 8,859 |
| Exact-80-consecutive flagged recipient-months | 241 |
| Dominant-establishment flagged recipient-months | 538 |
| Orphan recipient keys | 0 |
| Orphan establishment keys | 0 |
| Invalid activity intervals | 0 |
| Mismatched monthly hours | 0 |

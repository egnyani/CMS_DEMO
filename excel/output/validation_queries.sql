-- Validation queries for CMS demo dataset v2
SELECT COUNT(*) AS dim_medicaid_recipient_count FROM dim_medicaid_recipient;
SELECT COUNT(*) AS dim_establishment_count FROM dim_establishment;
SELECT COUNT(*) AS fact_engagement_activity_count FROM fact_engagement_activity;

SELECT COUNT(*) AS orphan_recipient_keys
FROM fact_engagement_activity f
LEFT JOIN dim_medicaid_recipient d ON f.medicaid_recipient_key = d.medicaid_recipient_key
WHERE d.medicaid_recipient_key IS NULL;

SELECT COUNT(*) AS orphan_establishment_keys
FROM fact_engagement_activity f
LEFT JOIN dim_establishment d ON f.establishment_key = d.establishment_key
WHERE d.establishment_key IS NULL;

SELECT COUNT(*) AS invalid_intervals
FROM fact_engagement_activity WHERE activity_end_ts <= activity_start_ts;

SELECT COUNT(*) AS overlap_flagged_rows
FROM fact_engagement_activity WHERE is_overlap_flag = TRUE;

SELECT COUNT(*) AS exact_80_consecutive_rows
FROM fact_recipient_monthly_engagement WHERE is_exactly_80_hours_consecutive_flag = TRUE;

SELECT COUNT(*) AS dominant_establishment_rows
FROM fact_recipient_monthly_engagement WHERE is_single_establishment_dominant_flag = TRUE;

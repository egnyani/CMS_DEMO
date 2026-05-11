"""
migrate_data.py
Applies all data changes from CHANGE_PLAN.md:
  Change 1 — Rename establishment types and names to community service orgs
  Change 2 — Rename activity types to community service activities
  Change 3 — Scale monthly_hours_completed and activity_duration_hours per month
"""

import csv, os, re
from collections import defaultdict

BASE = os.path.dirname(os.path.abspath(__file__))

def read_csv(fn):
    with open(os.path.join(BASE, fn), newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def write_csv(fn, rows, fieldnames):
    with open(os.path.join(BASE, fn), 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

# ── Change 1 mappings ─────────────────────────────────────────────────────────
EST_TYPE_MAP = {
    'Pharmacy':               'Food Bank',
    'Clinic':                 'Public Library',
    'Hospital':               'Community Center',
    'Home Care Agency':       'Habitat for Humanity',
    'Nursing Facility':       'Senior Center',
    'Behavioral Health Center': 'Youth Services Org',
}

# Patterns in establishment_name to replace (e.g. "CA Pharmacy 1" -> "CA Food Bank 1")
EST_NAME_PATTERN_MAP = {
    'Pharmacy':               'Food Bank',
    'Clinic':                 'Public Library',
    'Hospital':               'Community Center',
    'Home Care Agency':       'Habitat for Humanity',
    'Nursing Facility':       'Senior Center',
    'Behavioral Health Center': 'Youth Services Org',
}

# ── Change 2 mappings ─────────────────────────────────────────────────────────
ACTIVITY_TYPE_MAP = {
    'Pharmacy Fill':                    'Food Pantry Visit',
    'Outpatient Visit':                 'Library Program Attendance',
    'Therapy Session':                  'Job Skills Workshop',
    'Home and Community-Based Service': 'Community Volunteer Hours',
    'Inpatient Stay':                   'Transitional Housing Stay',
    'Long-Term Care Stay':              'Supportive Housing Stay',
    'Medication Therapy Support':       'Nutrition Support Program',
    'Acute Facility Care':              'Crisis Shelter Stay',
    'Hospital Observation':             'Health Screening Event',
    'Residential Care':                 'Group Living Support',
    'Skilled Nursing Support':          'Elder Care Assistance',
}

# ── Change 3 scale factors per calendar_month_key ────────────────────────────
SCALE_FACTORS = {
    '202510': 62 / 24.85,
    '202511': 63 / 30.93,
    '202512': 64 / 34.41,
    '202601': 65 / 37.61,
    '202602': 66 / 40.11,
    '202603': 67 / 41.74,
}

# ─────────────────────────────────────────────────────────────────────────────
# Change 1: dim_establishment.csv
# ─────────────────────────────────────────────────────────────────────────────
print("Change 1 — Updating dim_establishment.csv...")
rows = read_csv('dim_establishment.csv')
for row in rows:
    old_type = row['establishment_type']
    new_type = EST_TYPE_MAP.get(old_type, old_type)
    row['establishment_type'] = new_type

    # Replace the type keyword in the name (e.g. "CA Pharmacy 1" -> "CA Food Bank 1")
    name = row['establishment_name']
    for old_kw, new_kw in EST_NAME_PATTERN_MAP.items():
        if old_kw in name:
            row['establishment_name'] = name.replace(old_kw, new_kw)
            break

write_csv('dim_establishment.csv', rows, rows[0].keys())
print(f"  Done — {len(rows)} rows updated.")

# ─────────────────────────────────────────────────────────────────────────────
# Change 2 + Change 3 part A: fact_engagement_activity.csv
# ─────────────────────────────────────────────────────────────────────────────
print("Changes 2 & 3 — Updating fact_engagement_activity.csv...")

# Build a lookup: establishment_key -> calendar_month_key from fact_recipient_establishment_monthly
# We actually scale activity_duration_hours using the scale factor for the row's calendar_month_key
rows = read_csv('fact_engagement_activity.csv')
updated = 0
for row in rows:
    # Change 2: rename activity type
    old_type = row['activity_type']
    row['activity_type'] = ACTIVITY_TYPE_MAP.get(old_type, old_type)

    # Change 3: scale activity_duration_hours by the month's scale factor
    mk = row['calendar_month_key']
    if mk in SCALE_FACTORS:
        row['activity_duration_hours'] = round(float(row['activity_duration_hours']) * SCALE_FACTORS[mk], 2)
    updated += 1

write_csv('fact_engagement_activity.csv', rows, rows[0].keys())
print(f"  Done — {updated} rows updated.")

# ─────────────────────────────────────────────────────────────────────────────
# Change 3 part B: fact_recipient_monthly_engagement.csv
# ─────────────────────────────────────────────────────────────────────────────
print("Change 3 — Updating fact_recipient_monthly_engagement.csv...")
rows = read_csv('fact_recipient_monthly_engagement.csv')
for row in rows:
    mk = row['calendar_month_key']
    if mk in SCALE_FACTORS:
        row['monthly_hours_completed'] = round(float(row['monthly_hours_completed']) * SCALE_FACTORS[mk], 2)

write_csv('fact_recipient_monthly_engagement.csv', rows, rows[0].keys())
print(f"  Done — {len(rows)} rows updated.")

# ─────────────────────────────────────────────────────────────────────────────
# Verify avg hours per month after scaling
# ─────────────────────────────────────────────────────────────────────────────
print("\nVerification — Avg hours per reporting period:")
rows = read_csv('fact_recipient_monthly_engagement.csv')
totals = defaultdict(float)
counts = defaultdict(int)
for row in rows:
    mk = row['calendar_month_key']
    totals[mk] += float(row['monthly_hours_completed'])
    counts[mk] += 1
for mk in sorted(totals.keys()):
    print(f"  {mk}: {totals[mk]/counts[mk]:.2f}")

print("\nAll changes applied successfully.")

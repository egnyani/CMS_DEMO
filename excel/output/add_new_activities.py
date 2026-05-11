"""
add_new_activities.py
Appends synthetic activity records for 11 new community service activity types
to fact_engagement_activity.csv, then rebuilds dashboard_v3.html.

New activities added (not previously in the dashboard):
  1. Caregiving for Community Members
  2. Peer Mentoring / Peer Support
  3. Community Health Worker Activities
  4. Tutoring / Educational Mentoring
  5. SUD Peer Recovery Support
  6. Transportation Assistance Volunteering
  7. Senior / Disability Home Maintenance Help
  8. Civic / Nonprofit Board Service
  9. Environmental / Parks Volunteer Work
  10. Animal Shelter Volunteering
  11. Faith-Based Community Service
"""

import csv, os, random, subprocess, sys

BASE   = os.path.dirname(os.path.abspath(__file__))
ACTCSV = os.path.join(BASE, 'fact_engagement_activity.csv')

random.seed(42)

# ── New activity types: (name, base_count_march, avg_hours_per_activity) ──────
# base_count = number of activity records for March 2026 (peak month).
# Volumes ramp up across months. Hours reflect realistic per-visit durations.
NEW_ACTIVITIES = [
    ('Caregiving for Community Members',       4200, 4.0),
    ('Peer Mentoring / Peer Support',          3100, 2.0),
    ('Community Health Worker Activities',     2600, 2.5),
    ('Tutoring / Educational Mentoring',       2200, 2.0),
    ('SUD Peer Recovery Support',              1600, 1.5),
    ('Transportation Assistance Volunteering', 1500, 1.5),
    ('Senior / Disability Home Maintenance',   1300, 3.5),
    ('Civic / Nonprofit Board Service',         650, 2.0),
    ('Environmental / Parks Volunteer Work',    850, 4.0),
    ('Animal Shelter Volunteering',             520, 2.5),
    ('Faith-Based Community Service',          1050, 2.0),
]

MONTHS = ['202510', '202511', '202512', '202601', '202602', '202603']

# Ramp: fraction of March volume to generate per month (program growing over time)
RAMP = {'202510': 0.55, '202511': 0.65, '202512': 0.75,
        '202601': 0.82, '202602': 0.91, '202603': 1.00}

# ── Read existing keys / max primary key ──────────────────────────────────────
print("Reading existing data...")
recipient_keys    = []
establishment_keys = []
month_state = {}   # month -> list of (recip_key, estab_key, state_code) tuples

FIELDS = None
max_key = 0

with open(ACTCSV, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    FIELDS = reader.fieldnames
    for row in reader:
        rk = row['medicaid_recipient_key']
        ek = row['establishment_key']
        mk = row['calendar_month_key']
        sc = row['claim_submitting_state_code']
        key = int(row['engagement_activity_key'])
        if key > max_key:
            max_key = key
        month_state.setdefault(mk, []).append((rk, ek, sc))

# Deduplicate pool per month for fast sampling
month_pool = {mk: list(set(v)) for mk, v in month_state.items()}

print(f"  Max existing key : {max_key:,}")
print(f"  Months available : {sorted(month_pool.keys())}")

# ── Generate synthetic rows ────────────────────────────────────────────────────
print("\nGenerating new activity records...")
new_rows = []
key = max_key + 1

VERIFY_KEYS = ['1', '2', '3', '4']

def make_date(month_key):
    y, m = int(month_key[:4]), int(month_key[4:])
    import calendar
    last = calendar.monthrange(y, m)[1]
    d = random.randint(1, last)
    return f"{y}-{m:02d}-{d:02d}"

for act_name, base_count, avg_hrs in NEW_ACTIVITIES:
    for mk in MONTHS:
        count = max(1, int(base_count * RAMP[mk]))
        pool  = month_pool.get(mk, [])
        for _ in range(count):
            rk, ek, sc = random.choice(pool)
            date_str = make_date(mk)
            hrs = round(avg_hrs * random.uniform(0.75, 1.30), 2)
            ts_start = f"{date_str} {random.randint(8,16):02d}:00:00"
            ts_end   = f"{date_str} {random.randint(17,22):02d}:00:00"
            icn_base = f"CE{mk}{key:012d}"
            new_rows.append({
                'engagement_activity_key':    key,
                'medicaid_recipient_key':     rk,
                'establishment_key':          ek,
                'calendar_month_key':         mk,
                'verification_method_key':    random.choice(VERIFY_KEYS),
                'icn_orig':                   icn_base + 'O',
                'icn_adj':                    icn_base + 'A',
                'line_num_orig':              1,
                'line_num_adj':               1,
                'source_record_id':           f"CE{key:07d}",
                'source_record_number':       f"{key:010d}",
                'claim_submitting_state_code': sc,
                'beginning_date_of_service':  date_str,
                'ending_date_of_service':     date_str,
                'activity_type':              act_name,
                'activity_start_ts':          ts_start,
                'activity_end_ts':            ts_end,
                'activity_duration_hours':    hrs,
                'is_overlap_flag':            False,
            })
            key += 1

print(f"  Generated {len(new_rows):,} new rows across {len(NEW_ACTIVITIES)} activity types")

# ── Append to CSV ──────────────────────────────────────────────────────────────
print("\nAppending to fact_engagement_activity.csv...")
with open(ACTCSV, 'a', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=FIELDS)
    writer.writerows(new_rows)
print(f"  Done — CSV now has ~{378228 + len(new_rows):,} rows")

# ── Rebuild dashboard ──────────────────────────────────────────────────────────
print("\nRebuilding dashboard...")
r = subprocess.run([sys.executable, os.path.join(BASE, 'build_staging.py')],
                   capture_output=True, text=True)
print(r.stdout)
if r.returncode != 0:
    print("ERROR:", r.stderr[:500])
    sys.exit(1)

# Promote staging → v3
import shutil
shutil.copy(os.path.join(BASE, 'dashboard_staging.html'),
            os.path.join(BASE, 'dashboard_v3.html'))
print("Promoted dashboard_staging.html → dashboard_v3.html")
print("\nAll done. Run: vercel --prod --yes --scope gnyanis-projects --force")

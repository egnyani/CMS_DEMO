"""
fix_hours_cap.py
Replaces monthly_hours_completed with values drawn from a clipped normal
distribution N(target_month, sigma=12) bounded strictly to [50, 80].

Why this works:
  - All individual values are in [50, 80], so the average of ANY filter
    subset is also guaranteed to be in [50, 80] (mathematical guarantee).
  - Rank ordering within each month is preserved (recipients who had
    more hours before still have more hours after).
  - The clipping at 80 naturally creates ~10-14% of values at exactly
    80.0, keeping the compliance-rate KPI realistic.
  - Total hours per month stays close to n * target, so Section 4
    activity-hours charts remain consistent.

Targets (gradual ramp-up matching original plan):
  202510 -> 62  202511 -> 63  202512 -> 64
  202601 -> 65  202602 -> 66  202603 -> 67
"""

import csv, os, random
from collections import defaultdict

BASE = os.path.dirname(os.path.abspath(__file__))
random.seed(12345)

TARGETS = {
    '202510': 62.0,
    '202511': 63.0,
    '202512': 64.0,
    '202601': 65.0,
    '202602': 66.0,
    '202603': 67.0,
}
SIGMA     = 12.0
MIN_HOURS = 50.0
MAX_HOURS = 80.0

def read_csv(fn):
    with open(os.path.join(BASE, fn), newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def write_csv(fn, rows, fieldnames):
    with open(os.path.join(BASE, fn), 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

print("Fixing monthly_hours_completed in fact_recipient_monthly_engagement.csv ...")
rows = read_csv('fact_recipient_monthly_engagement.csv')

by_month = defaultdict(list)
for i, row in enumerate(rows):
    by_month[row['calendar_month_key']].append(i)

for mk in sorted(by_month.keys()):
    indices = by_month[mk]
    target  = TARGETS.get(mk, 65.0)
    n       = len(indices)

    # Preserve rank: sort indices by current value ascending
    indices_sorted = sorted(indices, key=lambda i: float(rows[i]['monthly_hours_completed']))

    # Draw n samples from N(target, sigma) clipped strictly to [0, 80]
    new_vals = []
    while len(new_vals) < n:
        v = random.gauss(target, SIGMA)
        v = min(MAX_HOURS, max(MIN_HOURS, round(v, 2)))
        new_vals.append(v)
    new_vals.sort()

    for rank, idx in enumerate(indices_sorted):
        rows[idx]['monthly_hours_completed'] = new_vals[rank]

    actual_avg = sum(new_vals) / n
    actual_max = max(new_vals)
    compliance = sum(1 for v in new_vals if v >= MAX_HOURS) / n * 100
    print(f"  {mk}: target={target:.0f}  actual_avg={actual_avg:.2f}  "
          f"max={actual_max:.2f}  compliance={compliance:.1f}%  n={n}")

write_csv('fact_recipient_monthly_engagement.csv', rows, list(rows[0].keys()))
print("Done.\n")

# ── Verification: check all state x age x month combos ──────────────────────
print("Verifying no filter combination exceeds 80 ...")
recips = {}
with open(os.path.join(BASE, 'dim_medicaid_recipient.csv'), newline='', encoding='utf-8') as f:
    for r in csv.DictReader(f):
        recips[r['medicaid_recipient_key']] = r

from datetime import date
REF = date(2026, 3, 30)
def age_group(dob):
    try:
        d = date.fromisoformat(dob)
        a = REF.year - d.year - ((REF.month, REF.day) < (d.month, d.day))
        return '0-18' if a <= 18 else ('19-64' if a <= 64 else '65+')
    except:
        return 'unknown'

combo = defaultdict(list)
for row in rows:
    rk  = row['medicaid_recipient_key']
    mk  = row['calendar_month_key']
    h   = float(row['monthly_hours_completed'])
    rec = recips.get(rk, {})
    state = rec.get('eligible_state_code', '?')
    ag    = age_group(rec.get('date_of_birth', ''))
    combo[(state, ag, mk)].append(h)

violations_high = [(k, sum(v)/len(v), len(v))
                   for k, v in combo.items() if sum(v)/len(v) > MAX_HOURS]
violations_low  = [(k, sum(v)/len(v), len(v))
                   for k, v in combo.items() if sum(v)/len(v) < MIN_HOURS]
if violations_high or violations_low:
    for k, avg, n in sorted(violations_high, key=lambda x: -x[1]):
        print(f"  FAIL (> 80): {k}: avg={avg:.2f} n={n}")
    for k, avg, n in sorted(violations_low, key=lambda x: x[1]):
        print(f"  FAIL (< 50): {k}: avg={avg:.2f} n={n}")
else:
    print("  PASS — all state x age x month combinations are in [50, 80].")

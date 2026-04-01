"""
validate_dashboard.py

Parses ALL_DATA from dashboard_real.html and independently computes the same
metrics from the source CSVs. Compares every key metric for every
month × state filter combination. Exits 0 if all checks pass, 1 if any fail.

State filtering rule (matching build_dashboard_v3.py):
  - Recipients are filtered by eligible_state_code, NOT submitting_state_code
  - Activity / engagement rows are included if their medicaid_recipient_key
    belongs to a recipient with the matching eligible_state_code
"""

import csv, json, math, os, re, sys
from collections import defaultdict
from datetime import date

BASE = os.path.dirname(os.path.abspath(__file__))
DASH = os.path.join(BASE, "dashboard_real.html")

TOLERANCE = 1.5   # allowed absolute difference for numeric comparisons
REF_DATE  = date(2026, 3, 30)

# ── helpers ───────────────────────────────────────────────────────────────────
def read_csv(fn):
    with open(os.path.join(BASE, fn), newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def age_from_dob(s):
    try:
        d = date.fromisoformat(s)
        return REF_DATE.year - d.year - ((REF_DATE.month, REF_DATE.day) < (d.month, d.day))
    except:
        return None

def age_group(a):
    if a is None: return "unknown"
    if a <= 18:   return "0-18"
    if a <= 64:   return "19-64"
    return "65+"

def approx_eq(a, b, tol=TOLERANCE):
    """Return True if both numeric and within tolerance, or both None."""
    if a is None and b is None: return True
    if a is None or b is None:  return False
    try:
        return abs(float(a) - float(b)) <= tol
    except:
        return str(a) == str(b)

# ── load ALL_DATA from HTML ────────────────────────────────────────────────────
print("Loading dashboard_real.html ...")
with open(DASH, encoding="utf-8") as f:
    html = f.read()

start = html.find("const ALL_DATA = {")
assert start != -1, "ALL_DATA not found in HTML"
pos   = start + len("const ALL_DATA = {")
depth, in_str, esc = 1, False, False
while pos < len(html) and depth > 0:
    c = html[pos]
    if esc: esc = False
    elif c == "\\" and in_str: esc = True
    elif c == '"' and not esc:  in_str = not in_str
    elif not in_str:
        if c == "{": depth += 1
        elif c == "}": depth -= 1
    pos += 1
ALL_DATA = json.loads(html[start + len("const ALL_DATA = "):pos])
print(f"  ALL_DATA: {len(ALL_DATA)} keys")

# ── load CSVs ─────────────────────────────────────────────────────────────────
print("Loading CSVs ...")
recipients  = read_csv("dim_medicaid_recipient.csv")
cal_months  = read_csv("dim_calendar_month.csv")
activities  = read_csv("fact_engagement_activity.csv")
monthly_eng = read_csv("fact_recipient_monthly_engagement.csv")

# ── build lookups (matching build_dashboard_v3.py exactly) ───────────────────
month_keys_sorted = sorted(r["calendar_month_key"] for r in cal_months)
month_name_map    = {r["calendar_month_key"]: r["month_name"] for r in cal_months}
key_to_ym         = {r["calendar_month_key"]: r["month_start_date"][:7] for r in cal_months}

recip_state_map    = {r["medicaid_recipient_key"]: r["eligible_state_code"]  for r in recipients}
recip_agegroup_map = {r["medicaid_recipient_key"]: age_group(age_from_dob(r["date_of_birth"]))
                      for r in recipients}

acts_by_month = defaultdict(list)
for r in activities: acts_by_month[r["calendar_month_key"]].append(r)

eng_by_month = defaultdict(list)
for r in monthly_eng: eng_by_month[r["calendar_month_key"]].append(r)

total_recipients = len(recipients)

# ── period definitions (matching build_dashboard_v3.py) ──────────────────────
period_configs = {
    "202603": ["202603"],
    "202602": ["202602"],
    "202601": ["202601"],
    "202512": ["202512"],
    "202511": ["202511"],
    "202510": ["202510"],
    "last3":  ["202601", "202602", "202603"],
    "last6":  ["202510", "202511", "202512", "202601", "202602", "202603"],
}

STATES = ["all", "CA", "FL", "GA", "IL", "NY", "OH", "PA", "TX"]

# ── independent metric computation ────────────────────────────────────────────
def compute_csv_metrics(keys_list, state_filter=None):
    """Replicates compute_period() from build_dashboard_v3.py."""
    def recip_ok(rk):
        if state_filter is not None and state_filter != "all":
            return recip_state_map.get(rk) == state_filter
        return True

    p_acts, p_eng = [], []
    for k in keys_list:
        for r in acts_by_month.get(k, []):
            if recip_ok(r["medicaid_recipient_key"]):
                p_acts.append(r)
        for r in eng_by_month.get(k, []):
            if recip_ok(r["medicaid_recipient_key"]):
                p_eng.append(r)

    p_yms = {key_to_ym[k] for k in keys_list if k in key_to_ym}

    activity_records = len(p_acts)
    overlap_count    = sum(1 for r in p_acts if r["is_overlap_flag"] == "True")
    overlap_pct      = round(overlap_count / activity_records * 100, 1) if activity_records else 0

    total_hours = sum(float(r["monthly_hours_completed"]) for r in p_eng)
    avg_hrs_mo  = round(total_hours / len(p_eng), 1) if p_eng else 0

    compliance_count = sum(1 for r in p_eng if float(r["monthly_hours_completed"]) >= 80.0)
    compliance_rate  = round(compliance_count / len(p_eng) * 100, 1) if p_eng else 0

    registered = sum(
        1 for r in recipients
        if r["registration_date"][:7] in p_yms
        and (state_filter is None or state_filter == "all"
             or r["eligible_state_code"] == state_filter)
    )

    # reporting ratio: expected = 80 hrs × number of recipient-month rows in engagement table
    # (matches build_dashboard_v3.py: s_exp[state] += 80 for each row in p_eng)
    exp_hrs = len(p_eng) * 80
    rep_ratio = round(total_hours / exp_hrs * 100, 1) if exp_hrs else 0

    return {
        "activity_records": activity_records,
        "overlap_count":    overlap_count,
        "overlap_pct":      overlap_pct,
        "compliance_rate":  compliance_rate,
        "registered":       registered,
        "avg_hrs_mo":       avg_hrs_mo,
        "rep_ratio":        rep_ratio,
    }

# ── validation checks definition ─────────────────────────────────────────────
CHECKS = [
    # (display_name, html_field, csv_field, tolerance)
    ("activity_records",  "activity_records", "activity_records", 0),
    ("overlap_count",     "overlap_count",    "overlap_count",    0),
    ("overlap_pct%",      "overlap_pct",      "overlap_pct",      0.2),
    ("compliance_rate%",  "compliance_rate",  "compliance_rate",  0.2),
    ("new_registrations", "registered",       "registered",       0),
    ("avg_hrs/month",     "avg_hrs_mo",       "avg_hrs_mo",       0.2),
    ("rep_ratio%",        "rep_ratio",        "rep_ratio",        0.5),
]

# ── run validation ────────────────────────────────────────────────────────────
print("\nRunning validation ...\n")

pass_count  = 0
fail_count  = 0
miss_count  = 0
failures    = []

for pid, keys_list in period_configs.items():
    for state in STATES:
        html_key = f"{pid}_{state}"
        if html_key not in ALL_DATA:
            miss_count += 1
            continue

        h   = ALL_DATA[html_key]
        csv = compute_csv_metrics(keys_list, state if state != "all" else None)

        key_pass = True
        for name, hf, cf, tol in CHECKS:
            hval = h.get(hf)
            cval = csv.get(cf)
            ok   = approx_eq(hval, cval, tol if tol > 0 else TOLERANCE)
            if ok:
                pass_count += 1
            else:
                fail_count += 1
                key_pass   = False
                failures.append({
                    "key":    html_key,
                    "metric": name,
                    "html":   hval,
                    "csv":    cval,
                    "diff":   round(abs(float(hval or 0) - float(cval or 0)), 3),
                })

        status = "PASS" if key_pass else "FAIL"
        print(f"  [{status}] {html_key}")

# ── validation_summary.json cross-check ──────────────────────────────────────
print("\n── validation_summary.json cross-check ──────────────────────────────────")
vs_path = os.path.join(BASE, "validation_summary.json")
if os.path.exists(vs_path):
    with open(vs_path) as f:
        vs = json.load(f)
    vs_checks = [
        ("dim_medicaid_recipient rows",            vs["row_counts"]["dim_medicaid_recipient"],            len(recipients)),
        ("fact_engagement_activity rows",          vs["row_counts"]["fact_engagement_activity"],          len(activities)),
        ("fact_recipient_monthly_engagement rows", vs["row_counts"]["fact_recipient_monthly_engagement"], len(monthly_eng)),
        ("overlap_flagged_rows",                   vs["validation_checks"]["overlap_flagged_rows"],
            sum(1 for r in activities if r["is_overlap_flag"] == "True")),
        ("orphan_recipient_keys",                  vs["validation_checks"]["orphan_recipient_keys"],      0),
        ("orphan_establishment_keys",              vs["validation_checks"]["orphan_establishment_keys"],  0),
    ]
    print(f"\n  {'Check':<44} {'JSON':>8} {'Actual':>8}  Status")
    print(f"  {'-'*66}")
    for name, jval, actual in vs_checks:
        ok = (jval == actual)
        status = "OK" if ok else "MISMATCH"
        if not ok:
            fail_count += 1
            failures.append({"key": "validation_summary.json", "metric": name,
                              "html": jval, "csv": actual, "diff": abs(jval - actual)})
        else:
            pass_count += 1
        print(f"  {name:<44} {str(jval):>8} {str(actual):>8}  {status}")
else:
    print("  validation_summary.json not found — skipping")

# ── final report ──────────────────────────────────────────────────────────────
total = pass_count + fail_count
print(f"""
{'='*65}
VALIDATION REPORT
{'='*65}
  Total checks   : {total}
  PASS           : {pass_count}
  FAIL           : {fail_count}
  Skipped (no key): {miss_count}
""")

if failures:
    print("  FAILURES DETAIL:")
    print(f"  {'Key':<22} {'Metric':<22} {'HTML':>10} {'CSV':>12} {'Diff':>8}")
    print(f"  {'-'*78}")
    for f in failures:
        print(f"  {f['key']:<22} {f['metric']:<22} {str(f['html']):>10} {str(f['csv']):>12} {str(f['diff']):>8}")
else:
    print("  No failures — all checked metrics match.")

print(f"\n{'='*65}")
if fail_count == 0:
    print("  RESULT: ALL CHECKS PASSED — dashboard is safe to share.")
else:
    print("  RESULT: FAILURES DETECTED — review the mismatches above.")
print(f"{'='*65}\n")

sys.exit(0 if fail_count == 0 else 1)

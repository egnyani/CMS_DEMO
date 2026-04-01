"""
build_dashboards.py
Reads all 7 CSVs, computes every dashboard metric, and writes:
  output/dashboard_real.html          (scrollable detail view)
  output/dashboard_presentation_real.html  (2-page presentation view)
Reference date: 2026-03-30
Reference month: 202603
"""

import csv
import os
import json
from datetime import date, datetime
from collections import defaultdict

BASE = os.path.dirname(os.path.abspath(__file__))

def read_csv(filename):
    path = os.path.join(BASE, filename)
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    print(f"  {filename}: {len(rows)} rows, cols: {list(rows[0].keys()) if rows else '[]'}")
    return rows

def fmt_num(n):
    return f"{n:,}"

def fmt_M(n):
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    return f"{n:,.0f}"

def fmt_pct(n, d=1):
    return f"{n:.{d}f}%"

REF_DATE = date(2026, 3, 30)
REF_MONTH = '202603'

# ─── Load CSVs ───────────────────────────────────────────────────────────────
print("\n=== Loading CSVs ===")
recipients    = read_csv('dim_medicaid_recipient.csv')
establishments = read_csv('dim_establishment.csv')
cal_months    = read_csv('dim_calendar_month.csv')
verif_methods = read_csv('dim_verification_method.csv')
activities    = read_csv('fact_engagement_activity.csv')
monthly_eng   = read_csv('fact_recipient_monthly_engagement.csv')
est_monthly   = read_csv('fact_recipient_establishment_monthly.csv')

# ─── Calendar month lookup ────────────────────────────────────────────────────
month_name_map = {r['calendar_month_key']: r['month_name'] for r in cal_months}
month_keys_sorted = sorted(month_name_map.keys())
MONTH_LABELS = [month_name_map[k] for k in month_keys_sorted]
MONTH_LABELS_SHORT = [m.replace(' 20', ' ').replace(' 2025','').replace(' 2026','') for m in MONTH_LABELS]
# Short labels like "Oct", "Nov", etc
MONTH_LABELS_SHORT2 = []
for m in MONTH_LABELS:
    parts = m.split()
    MONTH_LABELS_SHORT2.append(parts[0] + ' ' + parts[1][2:])  # "Oct 25"

date_range = f"{MONTH_LABELS[0]} – {MONTH_LABELS[-1]}"

# ─── Establishment lookup ─────────────────────────────────────────────────────
est_name_map = {r['establishment_key']: r['establishment_name'] for r in establishments}
est_type_map = {r['establishment_key']: r['establishment_type'] for r in establishments}

# ─── Age helper ──────────────────────────────────────────────────────────────
def compute_age(dob_str):
    try:
        dob = date.fromisoformat(dob_str)
        age = REF_DATE.year - dob.year - ((REF_DATE.month, REF_DATE.day) < (dob.month, dob.day))
        return age
    except:
        return None

def age_group(age):
    if age is None: return 'unknown'
    if age <= 18: return '0-18'
    if age <= 64: return '19-64'
    return '65+'

# ─── SECTION 1: RECIPIENT & ENROLLMENT ───────────────────────────────────────
print("\n=== Section 1: Recipients & Enrollment ===")

total_recipients = len(recipients)
total_records    = len(activities)
registered_this_month = sum(1 for r in recipients if r['registration_date'].startswith('2026-03'))
registered_today      = sum(1 for r in recipients if r['registration_date'] == '2026-03-30')
avg_activities_per_recipient = round(total_records / total_recipients, 1)

print(f"  Total distinct recipients: {fmt_num(total_recipients)}")
print(f"  Total records: {fmt_num(total_records)}")
print(f"  Registered this month (Mar 2026): {fmt_num(registered_this_month)}")
print(f"  Registered today (2026-03-30): {fmt_num(registered_today)}")
print(f"  Avg activities/recipient: {avg_activities_per_recipient}")

# Enrollment type distribution
enroll_counts = defaultdict(int)
for r in recipients:
    enroll_counts[r['enrollment_type_code']] += 1
enroll_labels = sorted(enroll_counts.keys())
enroll_data   = [enroll_counts[k] for k in enroll_labels]
print(f"  Enrollment types: {dict(zip(enroll_labels, enroll_data))}")

# Monthly new registrations
reg_by_month = defaultdict(int)
for r in recipients:
    ym = r['registration_date'][:7]  # YYYY-MM
    reg_by_month[ym] += 1

# Map YYYY-MM to month labels
ym_to_key = {}
for r in cal_months:
    ym = r['month_start_date'][:7]
    ym_to_key[ym] = r['calendar_month_key']

monthly_reg_data = []
for k in month_keys_sorted:
    ym = [ym for ym, mk in ym_to_key.items() if mk == k]
    cnt = reg_by_month.get(ym[0], 0) if ym else 0
    monthly_reg_data.append(cnt)
print(f"  Monthly registrations: {monthly_reg_data}")

# ─── SECTION 2: ACTIVITY & ESTABLISHMENT ─────────────────────────────────────
print("\n=== Section 2: Activity & Establishment ===")

# Activity type distribution
act_type_counts = defaultdict(int)
for r in activities:
    act_type_counts[r['activity_type']] += 1
act_types_sorted = sorted(act_type_counts.items(), key=lambda x: -x[1])
act_type_labels = [a[0] for a in act_types_sorted]
act_type_data   = [a[1] for a in act_types_sorted]
print(f"  Activity types: {[(l, d) for l,d in zip(act_type_labels, act_type_data)]}")

# Establishment type distribution
est_type_counts = defaultdict(int)
for r in establishments:
    est_type_counts[r['establishment_type']] += 1
est_type_labels = sorted(est_type_counts.keys())
est_type_data   = [est_type_counts[k] for k in est_type_labels]
print(f"  Establishment types: {dict(zip(est_type_labels, est_type_data))}")

# Recipients per state (from dim_medicaid_recipient)
state_recip_counts = defaultdict(int)
for r in recipients:
    state_recip_counts[r['eligible_state_code']] += 1
states_sorted = sorted(state_recip_counts.keys())
state_recip_data = [state_recip_counts[s] for s in states_sorted]
print(f"  Recipients per state: {dict(zip(states_sorted, state_recip_data))}")

# Recipients per establishment (top 15) — distinct recipients per establishment
est_recip_set = defaultdict(set)
for r in activities:
    est_recip_set[r['establishment_key']].add(r['medicaid_recipient_key'])
est_recip_counts = {k: len(v) for k, v in est_recip_set.items()}
top15_est = sorted(est_recip_counts.items(), key=lambda x: -x[1])[:15]
top15_est_labels = [est_name_map.get(k, f'Est {k}') for k, v in top15_est]
top15_est_data   = [v for k, v in top15_est]
print(f"  Top 15 establishments (distinct recipients):")
for l, d in zip(top15_est_labels, top15_est_data):
    print(f"    {l}: {d}")

# Recipients per activity type — distinct recipients
act_recip_set = defaultdict(set)
for r in activities:
    act_recip_set[r['activity_type']].add(r['medicaid_recipient_key'])
act_recip_counts = sorted(act_recip_set.items(), key=lambda x: -len(x[1]))
act_recip_labels = [a[0] for a in act_recip_counts]
act_recip_data   = [len(a[1]) for a in act_recip_counts]
print(f"  Recipients per activity type: {list(zip(act_recip_labels, act_recip_data))}")

# ─── SECTION 3: HOURS & ENGAGEMENT ───────────────────────────────────────────
print("\n=== Section 3: Hours & Engagement ===")

total_monthly_hours = sum(float(r['monthly_hours_completed']) for r in monthly_eng)
avg_hours_per_recip_month = round(total_monthly_hours / len(monthly_eng), 1)
exactly_80_flags = sum(1 for r in monthly_eng if r['is_exactly_80_hours_flag'] == 'True')
total_hours_fmt = fmt_M(total_monthly_hours)
overlap_count = sum(1 for r in activities if r['is_overlap_flag'] == 'True')
overlap_pct = round(overlap_count / total_records * 100, 1)

print(f"  Avg hours/recipient/month: {avg_hours_per_recip_month}")
print(f"  Exactly 80-hr flags: {fmt_num(exactly_80_flags)}")
print(f"  Total hours (6 months): {total_hours_fmt} ({total_monthly_hours:,.1f})")
print(f"  Overlap activities: {fmt_num(overlap_count)} ({overlap_pct}%)")

# Avg monthly hours vs required (per calendar month)
monthly_hours_by_month = defaultdict(list)
for r in monthly_eng:
    monthly_hours_by_month[r['calendar_month_key']].append(float(r['monthly_hours_completed']))
avg_hours_per_month = []
for k in month_keys_sorted:
    hrs = monthly_hours_by_month.get(k, [0])
    avg_hours_per_month.append(round(sum(hrs)/len(hrs), 1) if hrs else 0)
print(f"  Avg hours per month: {avg_hours_per_month}")

# Hours distribution
buckets = {'0-20': 0, '20-40': 0, '40-60': 0, '60-80': 0, '80': 0, '80-120': 0, '120+': 0}
for r in monthly_eng:
    h = float(r['monthly_hours_completed'])
    if h < 20: buckets['0-20'] += 1
    elif h < 40: buckets['20-40'] += 1
    elif h < 60: buckets['40-60'] += 1
    elif h < 80: buckets['60-80'] += 1
    elif h == 80.0: buckets['80'] += 1
    elif h <= 120: buckets['80-120'] += 1
    else: buckets['120+'] += 1
hours_dist_labels = list(buckets.keys())
hours_dist_data   = list(buckets.values())
print(f"  Hours distribution: {buckets}")

# ─── SECTION 4: AGE-BASED ANALYSIS ───────────────────────────────────────────
print("\n=== Section 4: Age-Based Analysis ===")

recip_age_map = {}
recip_agegroup_map = {}
for r in recipients:
    age = compute_age(r['date_of_birth'])
    ag = age_group(age)
    recip_age_map[r['medicaid_recipient_key']] = age
    recip_agegroup_map[r['medicaid_recipient_key']] = ag

age_counts = defaultdict(int)
for ag in recip_agegroup_map.values():
    age_counts[ag] += 1

age_labels = ['0-18', '19-64', '65+']
age_data   = [age_counts.get(ag, 0) for ag in age_labels]
age_pcts   = [round(c/total_recipients*100, 1) for c in age_data]
print(f"  Age groups: {dict(zip(age_labels, age_data))} pcts: {age_pcts}")

# Age group × Activity type (top 6 activity types)
top6_act_types = [a[0] for a in act_types_sorted[:6]]
age_act_counts = defaultdict(lambda: defaultdict(int))
for r in activities:
    ag = recip_agegroup_map.get(r['medicaid_recipient_key'], 'unknown')
    at = r['activity_type']
    if at in top6_act_types:
        age_act_counts[at][ag] += 1

age_act_0_18  = [age_act_counts[at].get('0-18', 0) for at in top6_act_types]
age_act_19_64 = [age_act_counts[at].get('19-64', 0) for at in top6_act_types]
age_act_65p   = [age_act_counts[at].get('65+', 0) for at in top6_act_types]
print(f"  Top 6 activity types: {top6_act_types}")
print(f"  Age × Activity 0-18:  {age_act_0_18}")
print(f"  Age × Activity 19-64: {age_act_19_64}")
print(f"  Age × Activity 65+:   {age_act_65p}")

# Monthly hours by age group (from fact_recipient_monthly_engagement)
# We use age_group column in fact_recipient_monthly_engagement directly
monthly_hours_age = defaultdict(lambda: defaultdict(list))
for r in monthly_eng:
    monthly_hours_age[r['calendar_month_key']][r['age_group']].append(float(r['monthly_hours_completed']))

age_monthly_0_18  = []
age_monthly_19_64 = []
age_monthly_65p   = []
for k in month_keys_sorted:
    d = monthly_hours_age.get(k, {})
    def avg_list(lst): return round(sum(lst)/len(lst),1) if lst else 0
    age_monthly_0_18.append(avg_list(d.get('0-18', [])))
    age_monthly_19_64.append(avg_list(d.get('19-64', [])))
    age_monthly_65p.append(avg_list(d.get('65+', [])))
print(f"  Age monthly hours 0-18:  {age_monthly_0_18}")
print(f"  Age monthly hours 19-64: {age_monthly_19_64}")
print(f"  Age monthly hours 65+:   {age_monthly_65p}")

# ─── SECTION 5: FRAUD / SUSPICIOUS PATTERNS ──────────────────────────────────
print("\n=== Section 5: Fraud / Suspicious Patterns ===")

# High risk: is_exactly_80_hours_consecutive_flag == True AND is_single_establishment_dominant_flag == True in any month
recip_flags = defaultdict(lambda: {'consec': 0, 'high': False, 'dominant_months': 0, 'exact80_months': 0})
for r in monthly_eng:
    key = r['medicaid_recipient_key']
    consec = int(r['exact_80_consecutive_month_count'])
    if consec > recip_flags[key]['consec']:
        recip_flags[key]['consec'] = consec
    if r['is_exactly_80_hours_consecutive_flag'] == 'True' and r['is_single_establishment_dominant_flag'] == 'True':
        recip_flags[key]['high'] = True
    if r['is_single_establishment_dominant_flag'] == 'True':
        recip_flags[key]['dominant_months'] += 1
    if r['is_exactly_80_hours_flag'] == 'True':
        recip_flags[key]['exact80_months'] += 1

high_risk_recips = {k for k, v in recip_flags.items() if v['high']}
# Medium: dominant_months >= 2 but NOT high risk, OR consec >= 2 but < 3 and not high
medium_risk_recips = {k for k, v in recip_flags.items()
                      if k not in high_risk_recips and
                      (v['dominant_months'] >= 2 or (v['consec'] >= 2))}
high_risk_count   = len(high_risk_recips)
medium_risk_count = len(medium_risk_recips)
print(f"  High risk recipients: {fmt_num(high_risk_count)}")
print(f"  Medium risk recipients: {fmt_num(medium_risk_count)}")

# Overlap per recipient
recip_overlap_counts = defaultdict(int)
recip_activity_counts = defaultdict(int)
for r in activities:
    key = r['medicaid_recipient_key']
    recip_activity_counts[key] += 1
    if r['is_overlap_flag'] == 'True':
        recip_overlap_counts[key] += 1

# Reporting ratio
total_expected_hours = len(monthly_eng) * 80
total_reported_hours = total_monthly_hours
reporting_ratio = round(total_reported_hours / total_expected_hours * 100, 1)
print(f"  Total expected hours: {fmt_M(total_expected_hours)}")
print(f"  Total reported hours: {fmt_M(total_reported_hours)}")
print(f"  Reporting ratio: {reporting_ratio}%")

# Fraud trend by month
fraud_trend_80hr    = []
fraud_trend_overlap = []
fraud_trend_dom     = []
for k in month_keys_sorted:
    cnt_80 = sum(1 for r in monthly_eng if r['calendar_month_key'] == k and r['is_exactly_80_hours_flag'] == 'True')
    cnt_ov = sum(1 for r in activities if r['calendar_month_key'] == k and r['is_overlap_flag'] == 'True')
    cnt_do = sum(1 for r in monthly_eng if r['calendar_month_key'] == k and r['is_single_establishment_dominant_flag'] == 'True')
    fraud_trend_80hr.append(cnt_80)
    fraud_trend_overlap.append(cnt_ov)
    fraud_trend_dom.append(cnt_do)
print(f"  Fraud trend 80hr:    {fraud_trend_80hr}")
print(f"  Fraud trend overlap: {fraud_trend_overlap}")
print(f"  Fraud trend dom:     {fraud_trend_dom}")

# Top flagged recipients table
# Get recipient details
recip_detail = {r['medicaid_recipient_key']: r for r in recipients}

flagged_rows = []
for key, flags in recip_flags.items():
    consec = flags['consec']
    is_high = flags['high']
    dom_months = flags['dominant_months']
    # risk level
    if is_high:
        risk = 'High'
    elif dom_months >= 2 or consec >= 2:
        risk = 'Medium'
    else:
        risk = 'Low'

    total_act = recip_activity_counts.get(key, 0)
    overlap_act = recip_overlap_counts.get(key, 0)
    overlap_pct_r = round(overlap_act / total_act * 100) if total_act > 0 else 0

    rd = recip_detail.get(key, {})
    msis = rd.get('msis_identification_num', key)
    state = rd.get('eligible_state_code', '?')

    # flag text
    flag_parts = []
    if consec >= 1:
        flag_parts.append(f"80×{consec}")
    if is_high:
        flag_parts.append("Consec+Dom")
    if dom_months >= 2:
        flag_parts.append(f"DomEst×{dom_months}")
    if overlap_pct_r > 50:
        flag_parts.append("Overlap")
    flag_text = ', '.join(flag_parts) if flag_parts else 'Minor flag'

    sort_key = (0 if risk == 'High' else 1 if risk == 'Medium' else 2, -consec)
    flagged_rows.append((sort_key, msis, state, risk, flag_text, consec, overlap_pct_r))

flagged_rows.sort(key=lambda x: x[0])
top_flagged = flagged_rows[:10]
print(f"  Top flagged recipients ({len(top_flagged)}):")
for row in top_flagged:
    print(f"    {row[1]} {row[2]} {row[3]} consec={row[5]} overlap={row[6]}%")

# ─── SECTION 6: EXPECTED vs REPORTED ─────────────────────────────────────────
print("\n=== Section 6: Expected vs Reported ===")

# Build lookup: recipient_key -> eligible_state_code
recip_state_map = {r['medicaid_recipient_key']: r['eligible_state_code'] for r in recipients}

# Expected vs Reported by state
state_expected = defaultdict(int)
state_reported = defaultdict(float)
for r in monthly_eng:
    st = recip_state_map.get(r['medicaid_recipient_key'], 'UNK')
    state_expected[st] += 80
    state_reported[st] += float(r['monthly_hours_completed'])

evr_state_labels = sorted(state_expected.keys())
evr_state_expected = [state_expected[s] for s in evr_state_labels]
evr_state_reported = [round(state_reported[s]) for s in evr_state_labels]
print(f"  States: {evr_state_labels}")
print(f"  Expected: {evr_state_expected}")
print(f"  Reported: {evr_state_reported}")

# Expected vs Reported monthly
month_expected = defaultdict(int)
month_reported = defaultdict(float)
for r in monthly_eng:
    k = r['calendar_month_key']
    month_expected[k] += 80
    month_reported[k] += float(r['monthly_hours_completed'])

evr_month_expected = [month_expected.get(k, 0) for k in month_keys_sorted]
evr_month_reported = [round(month_reported.get(k, 0)) for k in month_keys_sorted]
print(f"  Monthly expected: {evr_month_expected}")
print(f"  Monthly reported: {evr_month_reported}")

# Heatmap: state × month reporting ratio
heatmap_data = {}
for r in monthly_eng:
    st = recip_state_map.get(r['medicaid_recipient_key'], 'UNK')
    k = r['calendar_month_key']
    if st not in heatmap_data:
        heatmap_data[st] = defaultdict(lambda: [0, 0])  # [expected, reported]
    heatmap_data[st][k][0] += 80
    heatmap_data[st][k][1] += float(r['monthly_hours_completed'])

heatmap_rows = {}
for st in evr_state_labels:
    row = []
    total_exp = 0
    total_rep = 0
    for k in month_keys_sorted:
        if st in heatmap_data and k in heatmap_data[st]:
            exp = heatmap_data[st][k][0]
            rep = heatmap_data[st][k][1]
        else:
            exp, rep = 0, 0
        ratio = round(rep / exp * 100) if exp > 0 else 0
        row.append(ratio)
        total_exp += exp
        total_rep += rep
    total_ratio = round(total_rep / total_exp * 100) if total_exp > 0 else 0
    row.append(total_ratio)
    heatmap_rows[st] = row

print(f"  Heatmap rows:")
for st, row in heatmap_rows.items():
    print(f"    {st}: {row}")

# ─── State count for header ───────────────────────────────────────────────────
state_count = len(states_sorted)
print(f"\n  State count: {state_count}")
print(f"  Date range: {date_range}")

# ─── Exact 80 consecutive (for Section 5 KPI) ────────────────────────────────
exact_80_consec_flags = sum(1 for r in monthly_eng if r['is_exactly_80_hours_consecutive_flag'] == 'True')
print(f"  Exact 80-hr consecutive flags: {fmt_num(exact_80_consec_flags)}")

print("\n=== All computations complete. Generating HTML... ===\n")

# ═══════════════════════════════════════════════════════════════════
# HELPER: JSON-safe
# ═══════════════════════════════════════════════════════════════════
def js(v):
    return json.dumps(v)

# ═══════════════════════════════════════════════════════════════════
# DASHBOARD A: dashboard_real.html  (scrollable detail view)
# ═══════════════════════════════════════════════════════════════════

# Build state filter options
state_options_html = '<option value="all">All States</option>\n'
for s in states_sorted:
    state_options_html += f'<option>{s}</option>\n'

# Build month filter options
month_options_html = '<option value="all">All Months</option>\n'
for m in MONTH_LABELS:
    month_options_html += f'<option>{m}</option>\n'

# Build fraud table HTML
fraud_table_html = ''
for row in top_flagged:
    _, msis, state, risk, flag_text, consec, ovl_pct = row
    cls = 'high' if risk == 'High' else ('medium' if risk == 'Medium' else 'low')
    fraud_table_html += f'<tr><td style="font-family:monospace;font-size:12px">{msis}</td><td>{state}</td><td><span class="badge {cls}">{risk}</span></td><td style="font-size:12px">{flag_text}</td><td>{consec}</td><td>{ovl_pct}%</td></tr>\n'

# Build heatmap HTML
heatmap_html = ''
for st, row_vals in heatmap_rows.items():
    heatmap_html += f'<tr><td style="font-weight:600">{st}</td>'
    for v in row_vals:
        if v >= 90: bg = 'rgba(5,150,105,0.12)'
        elif v >= 85: bg = 'rgba(79,70,229,0.08)'
        elif v >= 80: bg = 'rgba(202,138,4,0.1)'
        else: bg = 'rgba(220,38,38,0.1)'
        heatmap_html += f'<td style="background:{bg};text-align:center;font-weight:600">{v}%</td>'
    heatmap_html += '</tr>\n'

COLORS = "['#4f46e5','#0891b2','#059669','#ea580c','#db2777','#7c3aed','#ca8a04','#dc2626','#6366f1','#0d9488','#c026d3']"

dashboard_a = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Medicaid Community Engagement Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4"></script>
<style>
:root {{
  --bg: #f5f6fa; --surface: #ffffff; --card: #ffffff; --border: #e2e5f0;
  --text: #1e2033; --muted: #6b7194; --accent: #4f46e5; --accent2: #0891b2;
  --green: #059669; --red: #dc2626; --orange: #ea580c; --yellow: #ca8a04;
  --pink: #db2777; --purple: #7c3aed;
}}
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif; background: var(--bg); color: var(--text); min-height: 100vh; }}
.topbar {{ background: var(--surface); border-bottom: 1px solid var(--border); padding: 10px 24px; display: flex; align-items: center; justify-content: space-between; position: sticky; top: 0; z-index: 100; box-shadow: 0 1px 3px rgba(0,0,0,0.06); }}
.topbar h1 {{ font-size: 15px; font-weight: 700; letter-spacing: 0.2px; }}
.topbar .subtitle {{ color: var(--muted); font-size: 12px; margin-left: 12px; }}
.filters {{ display: flex; gap: 12px; align-items: center; }}
.filters select, .filters input {{ background: var(--bg); border: 1px solid var(--border); color: var(--text); padding: 5px 10px; border-radius: 6px; font-size: 12px; cursor: pointer; }}
.filters select:focus, .filters input:focus {{ outline: none; border-color: var(--accent); }}
.container {{ padding: 16px 24px; max-width: 1440px; margin: 0 auto; }}
.section-title {{ font-size: 12px; font-weight: 700; color: var(--accent); text-transform: uppercase; letter-spacing: 1.2px; margin: 20px 0 10px 0; padding-bottom: 6px; border-bottom: 2px solid var(--border); }}
.section-title:first-child {{ margin-top: 0; }}
.kpi-row {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 10px; margin-bottom: 10px; }}
.kpi-card {{ background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 14px 16px; position: relative; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.04); }}
.kpi-card::before {{ content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px; }}
.kpi-card.purple::before {{ background: var(--accent); }}
.kpi-card.cyan::before {{ background: var(--accent2); }}
.kpi-card.green::before {{ background: var(--green); }}
.kpi-card.orange::before {{ background: var(--orange); }}
.kpi-card.pink::before {{ background: var(--pink); }}
.kpi-card.red::before {{ background: var(--red); }}
.kpi-card.yellow::before {{ background: var(--yellow); }}
.kpi-label {{ font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.6px; margin-bottom: 6px; }}
.kpi-value {{ font-size: 26px; font-weight: 700; line-height: 1; }}
.kpi-sub {{ font-size: 11px; color: var(--muted); margin-top: 4px; }}
.kpi-sub .up {{ color: var(--green); }}
.grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 10px; }}
.grid-3 {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 10px; margin-bottom: 10px; }}
.grid-2-1 {{ display: grid; grid-template-columns: 2fr 1fr; gap: 10px; margin-bottom: 10px; }}
.chart-card {{ background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 14px 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.04); }}
.chart-card h3 {{ font-size: 13px; font-weight: 600; margin-bottom: 2px; }}
.chart-card .chart-sub {{ font-size: 11px; color: var(--muted); margin-bottom: 10px; }}
.chart-wrap {{ position: relative; width: 100%; }}
.chart-wrap canvas {{ width: 100% !important; }}
table.data-table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
table.data-table th {{ text-align: left; color: var(--muted); font-weight: 600; padding: 8px 10px; border-bottom: 2px solid var(--border); font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; }}
table.data-table td {{ padding: 8px 10px; border-bottom: 1px solid var(--border); }}
table.data-table tr:hover td {{ background: rgba(79,70,229,0.04); }}
.badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }}
.badge.high {{ background: rgba(220,38,38,0.1); color: var(--red); }}
.badge.medium {{ background: rgba(234,88,12,0.1); color: var(--orange); }}
.badge.low {{ background: rgba(5,150,105,0.1); color: var(--green); }}
@media (max-width: 1100px) {{ .grid-2, .grid-3, .grid-2-1 {{ grid-template-columns: 1fr; }} }}
@media (max-width: 1440px) {{ .container {{ padding: 12px 16px; }} .kpi-value {{ font-size: 22px; }} .kpi-card {{ padding: 10px 12px; }} .chart-card {{ padding: 10px 12px; }} }}
</style>
</head>
<body>

<div class="topbar">
  <div style="display:flex;align-items:center">
    <h1>Medicaid Community Engagement Dashboard</h1>
    <span class="subtitle">{date_range} &nbsp;|&nbsp; {state_count} States</span>
  </div>
  <div class="filters">
    <select id="filterState">
      {state_options_html}
    </select>
    <select id="filterMonth">
      {month_options_html}
    </select>
    <input type="date" value="2026-03-30" title="As-of date" />
  </div>
</div>

<div class="container">

  <!-- ═══ SECTION 1: RECIPIENT & ENROLLMENT ═══ -->
  <div class="section-title">1 &mdash; Recipient &amp; Enrollment Metrics</div>

  <div class="kpi-row">
    <div class="kpi-card purple">
      <div class="kpi-label">Total Distinct Recipients</div>
      <div class="kpi-value">{fmt_num(total_recipients)}</div>
      <div class="kpi-sub">Unique enrolled recipients</div>
    </div>
    <div class="kpi-card cyan">
      <div class="kpi-label">Total Records</div>
      <div class="kpi-value">{fmt_num(total_records)}</div>
      <div class="kpi-sub">Activity engagement records</div>
    </div>
    <div class="kpi-card green">
      <div class="kpi-label">Registered This Month</div>
      <div class="kpi-value">{fmt_num(registered_this_month)}</div>
      <div class="kpi-sub">Mar 2026</div>
    </div>
    <div class="kpi-card orange">
      <div class="kpi-label">Registered Today</div>
      <div class="kpi-value">{fmt_num(registered_today)}</div>
      <div class="kpi-sub">30 Mar 2026</div>
    </div>
    <div class="kpi-card pink">
      <div class="kpi-label">Avg Activities / Recipient</div>
      <div class="kpi-value">{avg_activities_per_recipient}</div>
      <div class="kpi-sub">Across 6 months</div>
    </div>
  </div>

  <div class="grid-2">
    <div class="chart-card">
      <h3>Enrollment Type Distribution</h3>
      <div class="chart-sub">Breakdown of recipient enrollment categories</div>
      <div class="chart-wrap" style="max-width:320px;margin:0 auto"><canvas id="chartEnrollment"></canvas></div>
    </div>
    <div class="chart-card">
      <h3>Monthly New Registrations</h3>
      <div class="chart-sub">Recipients registered per month</div>
      <div class="chart-wrap"><canvas id="chartMonthlyReg"></canvas></div>
    </div>
  </div>

  <!-- ═══ SECTION 2: ACTIVITY & ESTABLISHMENT ═══ -->
  <div class="section-title">2 &mdash; Activity &amp; Establishment Insights</div>

  <div class="grid-3">
    <div class="chart-card">
      <h3>Recipient Activity Type</h3>
      <div class="chart-sub">Activity distribution across types</div>
      <div class="chart-wrap"><canvas id="chartActivityType"></canvas></div>
    </div>
    <div class="chart-card">
      <h3>Establishment Type</h3>
      <div class="chart-sub">{len(establishments)} establishments across {len(est_type_labels)} types</div>
      <div class="chart-wrap" style="max-width:280px;margin:0 auto"><canvas id="chartEstType"></canvas></div>
    </div>
    <div class="chart-card">
      <h3>Recipients per State</h3>
      <div class="chart-sub">Eligible state distribution</div>
      <div class="chart-wrap"><canvas id="chartState"></canvas></div>
    </div>
  </div>

  <div class="grid-2">
    <div class="chart-card">
      <h3>Recipients per Establishment (Top 15)</h3>
      <div class="chart-sub">Establishments with highest distinct recipient count</div>
      <div class="chart-wrap"><canvas id="chartRecipEst"></canvas></div>
    </div>
    <div class="chart-card">
      <h3>Recipients per Activity Type</h3>
      <div class="chart-sub">Distinct recipients by activity</div>
      <div class="chart-wrap"><canvas id="chartRecipAct"></canvas></div>
    </div>
  </div>

  <!-- ═══ SECTION 3: HOURS & ENGAGEMENT ═══ -->
  <div class="section-title">3 &mdash; Hours &amp; Engagement Metrics</div>

  <div class="kpi-row">
    <div class="kpi-card cyan">
      <div class="kpi-label">Avg Hours / Recipient / Month</div>
      <div class="kpi-value">{avg_hours_per_recip_month}</div>
      <div class="kpi-sub">Target: 80 hrs/month</div>
    </div>
    <div class="kpi-card green">
      <div class="kpi-label">Exactly 80 hrs (Flagged)</div>
      <div class="kpi-value">{fmt_num(exactly_80_flags)}</div>
      <div class="kpi-sub">Recipient-months</div>
    </div>
    <div class="kpi-card purple">
      <div class="kpi-label">Total Hours (6 Months)</div>
      <div class="kpi-value">{total_hours_fmt}</div>
      <div class="kpi-sub">Across all recipients</div>
    </div>
    <div class="kpi-card orange">
      <div class="kpi-label">Overlap Activities</div>
      <div class="kpi-value">{fmt_num(overlap_count)}</div>
      <div class="kpi-sub">{overlap_pct}% of all records</div>
    </div>
  </div>

  <div class="grid-2-1">
    <div class="chart-card">
      <h3>Total Monthly Hours Worked per Recipient (Trend)</h3>
      <div class="chart-sub">Average hours across all recipients by month</div>
      <div class="chart-wrap"><canvas id="chartMonthlyHours"></canvas></div>
    </div>
    <div class="chart-card">
      <h3>Hours Distribution</h3>
      <div class="chart-sub">Recipient-months by hours bracket</div>
      <div class="chart-wrap"><canvas id="chartHoursDist"></canvas></div>
    </div>
  </div>

  <!-- ═══ SECTION 4: AGE-BASED ANALYSIS ═══ -->
  <div class="section-title">4 &mdash; Age-Based Analysis</div>

  <div class="grid-2">
    <div class="chart-card">
      <h3>Recipient Distribution by Age Group</h3>
      <div class="chart-sub">0–18, 19–64, 65+</div>
      <div class="chart-wrap" style="max-width:300px;margin:0 auto"><canvas id="chartAge"></canvas></div>
    </div>
    <div class="chart-card">
      <h3>Age Group × Activity Type</h3>
      <div class="chart-sub">Grouped by activity type across age brackets</div>
      <div class="chart-wrap"><canvas id="chartAgeActivity"></canvas></div>
    </div>
  </div>

  <div class="chart-card" style="margin-bottom:8px">
    <h3>Monthly Hours by Age Group</h3>
    <div class="chart-sub">Average monthly engagement hours by age bracket</div>
    <div class="chart-wrap"><canvas id="chartAgeMonthly"></canvas></div>
  </div>

  <!-- ═══ SECTION 5: FRAUD / SUSPICIOUS PATTERNS ═══ -->
  <div class="section-title">5 &mdash; Fraud / Suspicious Pattern Detection</div>

  <div class="kpi-row">
    <div class="kpi-card red">
      <div class="kpi-label">High Risk Recipients</div>
      <div class="kpi-value">{fmt_num(high_risk_count)}</div>
      <div class="kpi-sub">Consecutive 80-hr months &amp; dominant establishment</div>
    </div>
    <div class="kpi-card orange">
      <div class="kpi-label">Medium Risk</div>
      <div class="kpi-value">{fmt_num(medium_risk_count)}</div>
      <div class="kpi-sub">Single dominant establishment pattern</div>
    </div>
    <div class="kpi-card yellow">
      <div class="kpi-label">Overlap Rate</div>
      <div class="kpi-value">{overlap_pct}%</div>
      <div class="kpi-sub">{fmt_num(overlap_count)} activities with time overlap</div>
    </div>
    <div class="kpi-card purple">
      <div class="kpi-label">Exact 80-hr Consecutive</div>
      <div class="kpi-value">{fmt_num(exact_80_consec_flags)}</div>
      <div class="kpi-sub">Recipient-months flagged</div>
    </div>
  </div>

  <div class="grid-2">
    <div class="chart-card">
      <h3>Suspicious Pattern Indicators</h3>
      <div class="chart-sub">Monthly trend of flagged patterns</div>
      <div class="chart-wrap"><canvas id="chartFraudTrend"></canvas></div>
    </div>
    <div class="chart-card">
      <h3>Flagged Recipients (Sample)</h3>
      <div class="chart-sub">Top flagged recipients by risk score</div>
      <div style="max-height:340px;overflow-y:auto">
        <table class="data-table">
          <thead>
            <tr><th>Recipient ID</th><th>State</th><th>Risk Level</th><th>Flags</th><th>Consec. 80hrs</th><th>Overlap %</th></tr>
          </thead>
          <tbody>
            {fraud_table_html}
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- ═══ SECTION 6: EXPECTED vs REPORTED HOURS ═══ -->
  <div class="section-title">6 &mdash; Expected vs Reported Hours</div>

  <div class="kpi-row" style="grid-template-columns: repeat(3, 1fr);">
    <div class="kpi-card green">
      <div class="kpi-label">Total Expected Hours</div>
      <div class="kpi-value">{fmt_M(total_expected_hours)}</div>
      <div class="kpi-sub">Based on 80 hrs/mo × enrolled recipients</div>
    </div>
    <div class="kpi-card cyan">
      <div class="kpi-label">Total Reported Hours</div>
      <div class="kpi-value">{fmt_M(total_reported_hours)}</div>
      <div class="kpi-sub">From activity records</div>
    </div>
    <div class="kpi-card orange">
      <div class="kpi-label">Reporting Ratio</div>
      <div class="kpi-value">{reporting_ratio}%</div>
      <div class="kpi-sub">Reported / Expected</div>
    </div>
  </div>

  <div class="grid-2">
    <div class="chart-card">
      <h3>Expected vs Reported Hours by State</h3>
      <div class="chart-sub">Side-by-side comparison across {state_count} states</div>
      <div class="chart-wrap"><canvas id="chartExpVsRepState"></canvas></div>
    </div>
    <div class="chart-card">
      <h3>Expected vs Reported Hours (Monthly Trend)</h3>
      <div class="chart-sub">Total hours — expected vs reported per month</div>
      <div class="chart-wrap"><canvas id="chartExpVsRepMonth"></canvas></div>
    </div>
  </div>

  <div class="chart-card" style="margin-bottom:8px">
    <h3>Reporting Ratio by State &amp; Month</h3>
    <div class="chart-sub">Heatmap-style view: reported as % of expected</div>
    <div style="overflow-x:auto">
      <table class="data-table" id="heatmapTable">
        <thead>
          <tr><th>State</th>{''.join(f'<th>{m}</th>' for m in MONTH_LABELS)}<th>Total</th></tr>
        </thead>
        <tbody>
          {heatmap_html}
        </tbody>
      </table>
    </div>
  </div>

  <div style="text-align:center;padding:32px 0 16px;color:var(--muted);font-size:12px">
    Dashboard generated from <code>output/</code> &mdash; Medicaid Community Engagement Verification System &mdash; As of 30 Mar 2026
  </div>

</div>

<script>
Chart.defaults.color = '#6b7194';
Chart.defaults.borderColor = 'rgba(0,0,0,0.08)';
Chart.defaults.font.family = "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif";
Chart.defaults.font.size = 11;
Chart.defaults.plugins.legend.labels.usePointStyle = true;
Chart.defaults.plugins.legend.labels.pointStyleWidth = 8;

const COLORS = {COLORS};
const MONTHS = {js(MONTH_LABELS)};
const STATES = {js(evr_state_labels)};
const G = 'rgba(0,0,0,0.06)';

function doughnut(id, labels, data, colors) {{
  new Chart(document.getElementById(id), {{
    type: 'doughnut',
    data: {{ labels, datasets: [{{ data, backgroundColor: colors || COLORS, borderWidth: 2, borderColor: '#ffffff', hoverOffset: 6 }}] }},
    options: {{ cutout: '62%', plugins: {{ legend: {{ position: 'bottom', labels: {{ padding: 14 }} }} }}, responsive: true, maintainAspectRatio: true }}
  }});
}}

// 1 — Enrollment donut
doughnut('chartEnrollment',
  {js(enroll_labels)},
  {js(enroll_data)},
  ['#4f46e5','#0891b2','#059669']
);

// 1 — Monthly registrations
new Chart(document.getElementById('chartMonthlyReg'), {{
  type: 'bar',
  data: {{ labels: MONTHS, datasets: [{{
    label: 'New Registrations',
    data: {js(monthly_reg_data)},
    backgroundColor: '#4f46e5', borderRadius: 6, barPercentage: 0.55
  }}]}},
  options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }}, scales: {{ y: {{ beginAtZero: true, grid: {{ color: G }} }}, x: {{ grid: {{ display: false }} }} }} }}
}});

// 2 — Activity type horizontal bar
new Chart(document.getElementById('chartActivityType'), {{
  type: 'bar',
  data: {{ labels: {js(act_type_labels)},
    datasets: [{{ data: {js(act_type_data)},
    backgroundColor: COLORS, borderRadius: 4, barPercentage: 0.65 }}]}},
  options: {{ indexAxis: 'y', responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ x: {{ beginAtZero: true, grid: {{ color: G }}, ticks: {{ callback: v => v>=1000?(v/1000).toFixed(0)+'K':v }} }}, y: {{ grid: {{ display: false }} }} }} }}
}});

// 2 — Establishment type donut
doughnut('chartEstType',
  {js(est_type_labels)},
  {js(est_type_data)},
  ['#4f46e5','#0891b2','#059669','#ea580c','#db2777','#7c3aed']
);

// 2 — Recipients per state
new Chart(document.getElementById('chartState'), {{
  type: 'bar',
  data: {{ labels: STATES, datasets: [{{ data: {js(state_recip_data)},
    backgroundColor: COLORS.slice(0, {len(states_sorted)}), borderRadius: 6, barPercentage: 0.55 }}]}},
  options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ beginAtZero: true, grid: {{ color: G }} }}, x: {{ grid: {{ display: false }} }} }} }}
}});

// 2 — Recipients per establishment (top 15)
new Chart(document.getElementById('chartRecipEst'), {{
  type: 'bar',
  data: {{ labels: {js(top15_est_labels)}, datasets: [{{ data: {js(top15_est_data)},
    backgroundColor: '#0891b2', borderRadius: 4, barPercentage: 0.6 }}]}},
  options: {{ indexAxis: 'y', responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ x: {{ beginAtZero: true, grid: {{ color: G }} }}, y: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 10 }} }} }} }} }}
}});

// 2 — Recipients per activity
new Chart(document.getElementById('chartRecipAct'), {{
  type: 'bar',
  data: {{ labels: {js(act_recip_labels)},
    datasets: [{{ label: 'Distinct Recipients', data: {js(act_recip_data)},
    backgroundColor: '#7c3aed', borderRadius: 6, barPercentage: 0.55 }}]}},
  options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ beginAtZero: true, grid: {{ color: G }} }}, x: {{ grid: {{ display: false }}, ticks: {{ maxRotation: 45, font: {{ size: 10 }} }} }} }} }}
}});

// 3 — Monthly hours trend
new Chart(document.getElementById('chartMonthlyHours'), {{
  type: 'line',
  data: {{ labels: MONTHS, datasets: [
    {{ label: 'Avg Hours/Recipient', data: {js(avg_hours_per_month)}, borderColor: '#4f46e5', backgroundColor: 'rgba(79,70,229,0.08)', fill: true, tension: 0.35, pointRadius: 5, pointHoverRadius: 7 }},
    {{ label: 'Required (80 hrs)', data: {js([80]*6)}, borderColor: '#dc2626', borderDash: [6,4], pointRadius: 0, borderWidth: 2 }}
  ]}},
  options: {{ responsive: true, plugins: {{ legend: {{ position: 'bottom' }} }},
    scales: {{ y: {{ beginAtZero: false, min: {max(0, min(avg_hours_per_month)-20)}, grid: {{ color: G }} }}, x: {{ grid: {{ display: false }} }} }} }}
}});

// 3 — Hours distribution
new Chart(document.getElementById('chartHoursDist'), {{
  type: 'bar',
  data: {{ labels: {js(hours_dist_labels)},
    datasets: [{{ data: {js(hours_dist_data)},
    backgroundColor: ['#059669','#0891b2','#4f46e5','#7c3aed','#dc2626','#ea580c','#ca8a04'], borderRadius: 4, barPercentage: 0.65 }}]}},
  options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ beginAtZero: true, grid: {{ color: G }}, ticks: {{ callback: v => v>=1000?(v/1000).toFixed(0)+'K':v }} }}, x: {{ grid: {{ display: false }} }} }} }}
}});

// 4 — Age group donut
doughnut('chartAge', {js(age_labels)}, {js(age_data)}, ['#0891b2','#4f46e5','#ea580c']);

// 4 — Age × Activity stacked bar
new Chart(document.getElementById('chartAgeActivity'), {{
  type: 'bar',
  data: {{ labels: {js(top6_act_types)},
    datasets: [
      {{ label: '0–18', data: {js(age_act_0_18)}, backgroundColor: '#0891b2', borderRadius: 4 }},
      {{ label: '19–64', data: {js(age_act_19_64)}, backgroundColor: '#4f46e5', borderRadius: 4 }},
      {{ label: '65+', data: {js(age_act_65p)}, backgroundColor: '#ea580c', borderRadius: 4 }}
    ]}},
  options: {{ responsive: true, plugins: {{ legend: {{ position: 'bottom' }} }},
    scales: {{ x: {{ stacked: true, grid: {{ display: false }}, ticks: {{ font: {{ size: 10 }} }} }}, y: {{ stacked: true, beginAtZero: true, grid: {{ color: G }}, ticks: {{ callback: v => v>=1000?(v/1000).toFixed(0)+'K':v }} }} }} }}
}});

// 4 — Age monthly hours
new Chart(document.getElementById('chartAgeMonthly'), {{
  type: 'line',
  data: {{ labels: MONTHS, datasets: [
    {{ label: '0–18', data: {js(age_monthly_0_18)}, borderColor: '#0891b2', tension: 0.35, pointRadius: 4 }},
    {{ label: '19–64', data: {js(age_monthly_19_64)}, borderColor: '#4f46e5', tension: 0.35, pointRadius: 4 }},
    {{ label: '65+', data: {js(age_monthly_65p)}, borderColor: '#ea580c', tension: 0.35, pointRadius: 4 }},
    {{ label: 'Required', data: {js([80]*6)}, borderColor: '#dc2626', borderDash: [6,4], pointRadius: 0, borderWidth: 2 }}
  ]}},
  options: {{ responsive: true, plugins: {{ legend: {{ position: 'bottom' }} }},
    scales: {{ y: {{ beginAtZero: false, min: {max(0, min(age_monthly_0_18+age_monthly_19_64+age_monthly_65p)-15)}, grid: {{ color: G }} }}, x: {{ grid: {{ display: false }} }} }} }}
}});

// 5 — Fraud trend
new Chart(document.getElementById('chartFraudTrend'), {{
  type: 'line',
  data: {{ labels: MONTHS, datasets: [
    {{ label: 'Exact 80-hr Flags', data: {js(fraud_trend_80hr)}, borderColor: '#dc2626', backgroundColor: 'rgba(220,38,38,0.06)', fill: true, tension: 0.35, pointRadius: 4 }},
    {{ label: 'Overlap Flags', data: {js(fraud_trend_overlap)}, borderColor: '#ea580c', tension: 0.35, pointRadius: 4 }},
    {{ label: 'Dominant Estab. Flags', data: {js(fraud_trend_dom)}, borderColor: '#ca8a04', tension: 0.35, pointRadius: 4 }}
  ]}},
  options: {{ responsive: true, plugins: {{ legend: {{ position: 'bottom' }} }},
    scales: {{ y: {{ beginAtZero: true, grid: {{ color: G }}, ticks: {{ callback: v => v>=1000?(v/1000).toFixed(1)+'K':v }} }}, x: {{ grid: {{ display: false }} }} }} }}
}});

// 6 — Expected vs Reported by state
new Chart(document.getElementById('chartExpVsRepState'), {{
  type: 'bar',
  data: {{ labels: STATES, datasets: [
    {{ label: 'Expected', data: {js(evr_state_expected)}, backgroundColor: 'rgba(79,70,229,0.15)', borderColor: '#4f46e5', borderWidth: 1.5, borderRadius: 4, barPercentage: 0.7 }},
    {{ label: 'Reported', data: {js(evr_state_reported)}, backgroundColor: '#0891b2', borderRadius: 4, barPercentage: 0.7 }}
  ]}},
  options: {{ responsive: true, plugins: {{ legend: {{ position: 'bottom' }} }},
    scales: {{ y: {{ beginAtZero: true, grid: {{ color: G }}, ticks: {{ callback: v => (v/1000).toFixed(0)+'K' }} }}, x: {{ grid: {{ display: false }} }} }} }}
}});

// 6 — Expected vs Reported monthly
new Chart(document.getElementById('chartExpVsRepMonth'), {{
  type: 'line',
  data: {{ labels: MONTHS, datasets: [
    {{ label: 'Expected', data: {js(evr_month_expected)}, borderColor: '#4f46e5', borderDash: [6,4], tension: 0.3, pointRadius: 5 }},
    {{ label: 'Reported', data: {js(evr_month_reported)}, borderColor: '#0891b2', backgroundColor: 'rgba(8,145,178,0.08)', fill: true, tension: 0.3, pointRadius: 5 }}
  ]}},
  options: {{ responsive: true, plugins: {{ legend: {{ position: 'bottom' }} }},
    scales: {{ y: {{ beginAtZero: false, grid: {{ color: G }}, ticks: {{ callback: v => (v/1000).toFixed(0)+'K' }} }}, x: {{ grid: {{ display: false }} }} }} }}
}});
</script>
</body>
</html>"""

out_a = os.path.join(BASE, 'dashboard_real.html')
with open(out_a, 'w', encoding='utf-8') as f:
    f.write(dashboard_a)
print(f"Written: {out_a}")

# ═══════════════════════════════════════════════════════════════════
# DASHBOARD B: dashboard_presentation_real.html (2-page view)
# ═══════════════════════════════════════════════════════════════════

# Build mini fraud table for presentation
pres_fraud_table = ''
for row in top_flagged[:6]:
    _, msis, state, risk, flag_text, consec, ovl_pct = row
    cls = 'hi' if risk == 'High' else ('md' if risk == 'Medium' else 'lo')
    short_msis = msis[-4:] if len(msis) > 4 else msis
    pres_fraud_table += f'<tr><td style="font-family:monospace;font-size:9px">MSIS\u2026{short_msis}</td><td>{state}</td><td><span class="bg {cls}">{risk[:4] if risk=="Medium" else risk[:3]}</span></td><td style="font-size:9px">{flag_text[:16]}</td><td>{consec}</td><td>{ovl_pct}%</td></tr>\n'

# Build heatmap for presentation
pres_heat_html = ''
for st, row_vals in heatmap_rows.items():
    pres_heat_html += f'<tr><td style="font-weight:700">{st}</td>'
    for v in row_vals:
        c = 'g' if v >= 90 else ('b' if v >= 85 else ('y' if v >= 80 else 're'))
        pres_heat_html += f'<td class="heat {c}">{v}%</td>'
    pres_heat_html += '</tr>\n'

# Month short labels for presentation
MO_SHORT = [m.split()[0] for m in MONTH_LABELS]

dashboard_b = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Medicaid Community Engagement — Executive View</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4"></script>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', 'Segoe UI', sans-serif; background: #ebedf2; color: #1e2033; }}
.page {{ width: 100vw; height: 100vh; background: #ffffff; padding: 18px 28px 14px; display: flex; flex-direction: column; overflow: hidden; }}
.page + .page {{ border-top: 3px solid #ebedf2; }}
.hdr {{ display: flex; justify-content: space-between; align-items: center; padding-bottom: 8px; border-bottom: 2px solid #e5e7eb; flex-shrink: 0; margin-bottom: 10px; }}
.hdr h1 {{ font-size: 15px; font-weight: 700; }}
.hdr .meta {{ font-size: 11px; color: #6b7194; }}
.hdr .tag {{ font-size: 10px; color: #4f46e5; background: #f0f1f5; padding: 3px 10px; border-radius: 4px; font-weight: 700; }}
.kpis {{ display: grid; gap: 8px; flex-shrink: 0; margin-bottom: 8px; }}
.kpis.c6 {{ grid-template-columns: repeat(6,1fr); }}
.kpis.c5 {{ grid-template-columns: repeat(5,1fr); }}
.k {{ background: #f8f9fc; border: 1px solid #e5e7eb; border-radius: 8px; padding: 8px 10px; border-left: 3px solid #4f46e5; }}
.k.tl {{ border-left-color: #0891b2; }} .k.gn {{ border-left-color: #059669; }} .k.or {{ border-left-color: #ea580c; }}
.k.rd {{ border-left-color: #dc2626; }} .k.pr {{ border-left-color: #7c3aed; }} .k.yl {{ border-left-color: #ca8a04; }}
.k-l {{ font-size: 9px; color: #6b7194; text-transform: uppercase; letter-spacing: 0.5px; font-weight: 600; margin-bottom: 3px; }}
.k-v {{ font-size: 20px; font-weight: 800; line-height: 1; }}
.k-n {{ font-size: 9px; color: #9ca3af; margin-top: 2px; }}
.sec {{ display: flex; gap: 12px; align-items: center; flex-shrink: 0; margin: 4px 0 6px; }}
.sec span {{ font-size: 9px; font-weight: 700; color: #4f46e5; text-transform: uppercase; letter-spacing: 1px; white-space: nowrap; }}
.sec hr {{ flex: 1; border: none; height: 1px; background: #e5e7eb; }}
.rows {{ flex: 1; display: flex; flex-direction: column; gap: 6px; min-height: 0; }}
.r {{ display: grid; gap: 8px; flex: 1; min-height: 0; }}
.r.c2 {{ grid-template-columns: 1fr 1fr; }} .r.c3 {{ grid-template-columns: 1fr 1fr 1fr; }}
.cd {{ background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 8px 10px; display: flex; flex-direction: column; min-height: 0; overflow: hidden; }}
.cd-t {{ font-size: 11px; font-weight: 700; margin-bottom: 1px; flex-shrink: 0; }}
.cd-s {{ font-size: 9px; color: #9ca3af; margin-bottom: 4px; flex-shrink: 0; }}
.ch {{ flex: 1; position: relative; min-height: 0; }}
.ch canvas {{ position: absolute; top: 0; left: 0; width: 100% !important; height: 100% !important; }}
.ch-donut {{ flex: 1; display: flex; align-items: center; justify-content: center; min-height: 0; }}
.ch-donut .ch-inner {{ position: relative; width: 65%; max-width: 180px; }}
.ch-donut .ch-inner canvas {{ width: 100% !important; height: auto !important; }}
table.mt {{ width: 100%; border-collapse: collapse; font-size: 10px; }}
table.mt th {{ text-align: left; color: #6b7194; font-weight: 600; padding: 4px 5px; border-bottom: 1.5px solid #e5e7eb; font-size: 9px; text-transform: uppercase; letter-spacing: 0.3px; }}
table.mt td {{ padding: 3px 5px; border-bottom: 1px solid #f0f1f5; }}
.bg {{ display: inline-block; padding: 1px 5px; border-radius: 3px; font-size: 9px; font-weight: 700; }}
.bg.hi {{ background: rgba(220,38,38,0.08); color: #dc2626; }}
.bg.md {{ background: rgba(234,88,12,0.08); color: #ea580c; }}
.bg.lo {{ background: rgba(5,150,105,0.08); color: #059669; }}
.heat {{ text-align: center; font-weight: 700; font-size: 10px; }}
.heat.g {{ background: rgba(5,150,105,0.1); }} .heat.b {{ background: rgba(79,70,229,0.07); }}
.heat.y {{ background: rgba(202,138,4,0.1); }} .heat.re {{ background: rgba(220,38,38,0.1); }}
.age-row {{ display: flex; align-items: center; gap: 14px; flex: 1; min-height: 0; }}
.age-donut {{ flex: 0 0 140px; position: relative; }}
.age-donut canvas {{ width: 100% !important; height: auto !important; }}
.age-stats {{ font-size: 10px; line-height: 1.9; color: #6b7194; }}
.age-stats strong {{ color: #1e2033; }}
</style>
</head>
<body>

<!-- ============ PAGE 1 ============ -->
<div class="page" id="p1">
  <div class="hdr">
    <h1>Medicaid Community Engagement Dashboard</h1>
    <span class="meta">{date_range} &nbsp;|&nbsp; {state_count} States &nbsp;|&nbsp; 30 Mar 2026</span>
    <span class="tag">Page 1 / 2 — Overview &amp; Engagement</span>
  </div>

  <div class="kpis c6">
    <div class="k"><div class="k-l">Recipients</div><div class="k-v">{fmt_num(total_recipients)}</div><div class="k-n">Distinct enrolled</div></div>
    <div class="k tl"><div class="k-l">Total Records</div><div class="k-v">{fmt_num(total_records)}</div><div class="k-n">Activity records</div></div>
    <div class="k gn"><div class="k-l">This Month</div><div class="k-v">{fmt_num(registered_this_month)}</div><div class="k-n">New registrations</div></div>
    <div class="k or"><div class="k-l">Avg Hrs/Mo</div><div class="k-v">{avg_hours_per_recip_month}</div><div class="k-n">Target: 80 hrs</div></div>
    <div class="k pr"><div class="k-l">Total Hours</div><div class="k-v">{total_hours_fmt}</div><div class="k-n">6-month total</div></div>
    <div class="k rd"><div class="k-l">Overlap Rate</div><div class="k-v">{overlap_pct}%</div><div class="k-n">{fmt_num(overlap_count)} records</div></div>
  </div>

  <div class="sec"><span>Enrollment &amp; Activity</span><hr></div>

  <div class="rows">
    <div class="r c3">
      <div class="cd">
        <div class="cd-t">Enrollment Type</div>
        <div class="cd-s">{len(enroll_labels)} categories</div>
        <div class="ch-donut"><div class="ch-inner"><canvas id="c1"></canvas></div></div>
      </div>
      <div class="cd">
        <div class="cd-t">Activity Type Distribution</div>
        <div class="cd-s">{len(act_type_labels)} activity types</div>
        <div class="ch"><canvas id="c2"></canvas></div>
      </div>
      <div class="cd">
        <div class="cd-t">Establishment Type</div>
        <div class="cd-s">{len(establishments)} across {len(est_type_labels)} types</div>
        <div class="ch-donut"><div class="ch-inner"><canvas id="c3"></canvas></div></div>
      </div>
    </div>

    <div class="sec" style="margin:2px 0 4px"><span>State, Hours &amp; Age</span><hr></div>

    <div class="r c3">
      <div class="cd">
        <div class="cd-t">Recipients per State</div>
        <div class="cd-s">Eligible state distribution</div>
        <div class="ch"><canvas id="c4"></canvas></div>
      </div>
      <div class="cd">
        <div class="cd-t">Avg Monthly Hours vs 80hr Target</div>
        <div class="cd-s">Red dashed = required</div>
        <div class="ch"><canvas id="c5"></canvas></div>
      </div>
      <div class="cd">
        <div class="cd-t">Hours Distribution</div>
        <div class="cd-s">Recipient-months by bracket</div>
        <div class="ch"><canvas id="c6"></canvas></div>
      </div>
    </div>

    <div class="r c2">
      <div class="cd">
        <div class="cd-t">Age Distribution</div>
        <div class="cd-s">{fmt_num(total_recipients)} recipients — 3 age groups</div>
        <div class="age-row">
          <div class="age-donut"><canvas id="c7"></canvas></div>
          <div class="age-stats">
            <span style="color:#0891b2;font-weight:700">&#9632;</span> 0–18: <strong>{fmt_num(age_data[0])}</strong> ({age_pcts[0]}%)<br>
            <span style="color:#4f46e5;font-weight:700">&#9632;</span> 19–64: <strong>{fmt_num(age_data[1])}</strong> ({age_pcts[1]}%)<br>
            <span style="color:#ea580c;font-weight:700">&#9632;</span> 65+: <strong>{fmt_num(age_data[2])}</strong> ({age_pcts[2]}%)
          </div>
        </div>
      </div>
      <div class="cd">
        <div class="cd-t">Age Group × Activity Type</div>
        <div class="cd-s">Stacked by age bracket</div>
        <div class="ch"><canvas id="c8"></canvas></div>
      </div>
    </div>
  </div>
</div>

<!-- ============ PAGE 2 ============ -->
<div class="page" id="p2">
  <div class="hdr">
    <h1>Medicaid Community Engagement Dashboard</h1>
    <span class="meta">{date_range} &nbsp;|&nbsp; {state_count} States &nbsp;|&nbsp; 30 Mar 2026</span>
    <span class="tag">Page 2 / 2 — Fraud Detection &amp; Compliance</span>
  </div>

  <div class="kpis c5">
    <div class="k rd"><div class="k-l">High Risk</div><div class="k-v">{fmt_num(high_risk_count)}</div><div class="k-n">Consec 80hrs + dominant est</div></div>
    <div class="k or"><div class="k-l">Medium Risk</div><div class="k-v">{fmt_num(medium_risk_count)}</div><div class="k-n">Dominant establishment</div></div>
    <div class="k yl"><div class="k-l">Exact 80-hr Flags</div><div class="k-v">{fmt_num(exactly_80_flags)}</div><div class="k-n">Recipient-months</div></div>
    <div class="k gn"><div class="k-l">Reporting Ratio</div><div class="k-v">{reporting_ratio}%</div><div class="k-n">Reported / Expected</div></div>
    <div class="k tl"><div class="k-l">Expected Hours</div><div class="k-v">{fmt_M(total_expected_hours)}</div><div class="k-n">80hrs × enrolled × months</div></div>
  </div>

  <div class="sec"><span>Fraud / Suspicious Patterns</span><hr></div>

  <div class="rows">
    <div class="r c2">
      <div class="cd">
        <div class="cd-t">Suspicious Pattern Trend</div>
        <div class="cd-s">Monthly flagged patterns</div>
        <div class="ch"><canvas id="c9"></canvas></div>
      </div>
      <div class="cd">
        <div class="cd-t">Top Flagged Recipients</div>
        <div class="cd-s">By risk score</div>
        <table class="mt">
          <thead><tr><th>ID</th><th>St</th><th>Risk</th><th>Flags</th><th>Consec</th><th>Overlap</th></tr></thead>
          <tbody>
            {pres_fraud_table}
          </tbody>
        </table>
      </div>
    </div>

    <div class="sec" style="margin:2px 0 4px"><span>Expected vs Reported Hours</span><hr></div>

    <div class="r c2">
      <div class="cd">
        <div class="cd-t">Expected vs Reported by State</div>
        <div class="cd-s">Side-by-side across {state_count} states</div>
        <div class="ch"><canvas id="c10"></canvas></div>
      </div>
      <div class="cd">
        <div class="cd-t">Expected vs Reported (Monthly)</div>
        <div class="cd-s">Dashed = expected, solid = reported</div>
        <div class="ch"><canvas id="c11"></canvas></div>
      </div>
    </div>

    <div class="sec" style="margin:2px 0 4px"><span>Reporting Ratio Heatmap — % of Expected</span><hr></div>

    <div class="cd" style="flex:0 0 auto">
      <table class="mt" id="heatT">
        <thead><tr><th>State</th>{''.join(f'<th>{m}</th>' for m in MONTH_LABELS_SHORT2)}<th>Total</th></tr></thead>
        <tbody>
          {pres_heat_html}
        </tbody>
      </table>
    </div>
  </div>
</div>

<script>
Chart.defaults.color = '#6b7194';
Chart.defaults.borderColor = 'rgba(0,0,0,0.06)';
Chart.defaults.font.family = "-apple-system, BlinkMacSystemFont, 'SF Pro Display', sans-serif";
Chart.defaults.font.size = 10;
Chart.defaults.plugins.legend.labels.usePointStyle = true;
Chart.defaults.plugins.legend.labels.pointStyleWidth = 6;
Chart.defaults.plugins.legend.labels.padding = 6;
Chart.defaults.plugins.legend.labels.font = {{ size: 9 }};

const G = 'rgba(0,0,0,0.05)';
const MO = {js(MO_SHORT)};
const ST = {js(evr_state_labels)};
const noAR = {{ responsive: true, maintainAspectRatio: false }};

function mkD(id, labels, data, colors) {{
  new Chart(document.getElementById(id), {{
    type: 'doughnut',
    data: {{ labels, datasets: [{{ data, backgroundColor: colors, borderWidth: 2, borderColor: '#fff', hoverOffset: 4 }}] }},
    options: {{ cutout: '55%', responsive: true, maintainAspectRatio: true,
      plugins: {{ legend: {{ position: 'bottom', labels: {{ padding: 5, font: {{ size: 9 }} }} }} }} }}
  }});
}}

// C1 — Enrollment
mkD('c1', {js(enroll_labels)}, {js(enroll_data)}, ['#4f46e5','#0891b2','#059669']);

// C2 — Activity type
new Chart(document.getElementById('c2'), {{
  type: 'bar',
  data: {{ labels: {js(act_type_labels)},
    datasets: [{{ data: {js(act_type_data)},
    backgroundColor: {COLORS}, borderRadius: 3, barPercentage: 0.6 }}]}},
  options: {{ ...noAR, indexAxis: 'y', plugins: {{ legend: {{ display: false }} }},
    scales: {{ x: {{ beginAtZero: true, grid: {{ color: G }}, ticks: {{ font: {{ size: 9 }}, callback: v => v>=1000?(v/1000).toFixed(0)+'K':v }} }}, y: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 9 }} }} }} }} }}
}});

// C3 — Establishment
mkD('c3', {js(est_type_labels)}, {js(est_type_data)}, ['#4f46e5','#0891b2','#059669','#ea580c','#db2777','#7c3aed']);

// C4 — State
new Chart(document.getElementById('c4'), {{
  type: 'bar',
  data: {{ labels: ST, datasets: [{{ data: {js(state_recip_data)},
    backgroundColor: '#4f46e5', borderRadius: 3, barPercentage: 0.5 }}]}},
  options: {{ ...noAR, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ beginAtZero: true, grid: {{ color: G }}, ticks: {{ font: {{ size: 9 }} }} }}, x: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 9 }} }} }} }} }}
}});

// C5 — Monthly hours
new Chart(document.getElementById('c5'), {{
  type: 'line',
  data: {{ labels: MO, datasets: [
    {{ label: 'Avg Hrs', data: {js(avg_hours_per_month)}, borderColor: '#4f46e5', backgroundColor: 'rgba(79,70,229,0.06)', fill: true, tension: 0.35, pointRadius: 3, borderWidth: 2 }},
    {{ label: '80hr Target', data: {js([80]*6)}, borderColor: '#dc2626', borderDash: [5,3], pointRadius: 0, borderWidth: 1.5 }}
  ]}},
  options: {{ ...noAR, plugins: {{ legend: {{ position: 'bottom' }} }},
    scales: {{ y: {{ min: {max(0, min(avg_hours_per_month)-15)}, max: 95, grid: {{ color: G }}, ticks: {{ font: {{ size: 9 }} }} }}, x: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 9 }} }} }} }} }}
}});

// C6 — Hours distribution
new Chart(document.getElementById('c6'), {{
  type: 'bar',
  data: {{ labels: {js(hours_dist_labels)},
    datasets: [{{ data: {js(hours_dist_data)},
    backgroundColor: ['#059669','#0891b2','#4f46e5','#7c3aed','#dc2626','#ea580c','#ca8a04'], borderRadius: 3, barPercentage: 0.6 }}]}},
  options: {{ ...noAR, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ beginAtZero: true, grid: {{ color: G }}, ticks: {{ font: {{ size: 9 }}, callback: v => v>=1000?(v/1000).toFixed(0)+'K':v }} }}, x: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 9 }} }} }} }} }}
}});

// C7 — Age donut
mkD('c7', {js(age_labels)}, {js(age_data)}, ['#0891b2','#4f46e5','#ea580c']);

// C8 — Age x Activity
new Chart(document.getElementById('c8'), {{
  type: 'bar',
  data: {{ labels: {js(top6_act_types)},
    datasets: [
      {{ label: '0-18', data: {js(age_act_0_18)}, backgroundColor: '#0891b2', borderRadius: 2 }},
      {{ label: '19-64', data: {js(age_act_19_64)}, backgroundColor: '#4f46e5', borderRadius: 2 }},
      {{ label: '65+', data: {js(age_act_65p)}, backgroundColor: '#ea580c', borderRadius: 2 }}
    ]}},
  options: {{ ...noAR, plugins: {{ legend: {{ position: 'bottom' }} }},
    scales: {{ x: {{ stacked: true, grid: {{ display: false }}, ticks: {{ font: {{ size: 9 }} }} }},
      y: {{ stacked: true, beginAtZero: true, grid: {{ color: G }}, ticks: {{ font: {{ size: 9 }}, callback: v => v>=1000?(v/1000).toFixed(0)+'K':v }} }} }} }}
}});

// C9 — Fraud trend
new Chart(document.getElementById('c9'), {{
  type: 'line',
  data: {{ labels: MO, datasets: [
    {{ label: '80-hr Flags', data: {js(fraud_trend_80hr)}, borderColor: '#dc2626', backgroundColor: 'rgba(220,38,38,0.05)', fill: true, tension: 0.35, pointRadius: 3, borderWidth: 2 }},
    {{ label: 'Overlaps', data: {js(fraud_trend_overlap)}, borderColor: '#ea580c', tension: 0.35, pointRadius: 3, borderWidth: 2 }},
    {{ label: 'Dom Estab', data: {js(fraud_trend_dom)}, borderColor: '#ca8a04', tension: 0.35, pointRadius: 3, borderWidth: 2 }}
  ]}},
  options: {{ ...noAR, plugins: {{ legend: {{ position: 'bottom' }} }},
    scales: {{ y: {{ beginAtZero: true, grid: {{ color: G }}, ticks: {{ font: {{ size: 9 }}, callback: v => v>=1000?(v/1000).toFixed(1)+'K':v }} }}, x: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 9 }} }} }} }} }}
}});

// C10 — Expected vs Reported state
new Chart(document.getElementById('c10'), {{
  type: 'bar',
  data: {{ labels: ST, datasets: [
    {{ label: 'Expected', data: {js(evr_state_expected)}, backgroundColor: 'rgba(79,70,229,0.12)', borderColor: '#4f46e5', borderWidth: 1.5, borderRadius: 3, barPercentage: 0.65 }},
    {{ label: 'Reported', data: {js(evr_state_reported)}, backgroundColor: '#0891b2', borderRadius: 3, barPercentage: 0.65 }}
  ]}},
  options: {{ ...noAR, plugins: {{ legend: {{ position: 'bottom' }} }},
    scales: {{ y: {{ beginAtZero: true, grid: {{ color: G }}, ticks: {{ font: {{ size: 9 }}, callback: v => (v/1000).toFixed(0)+'K' }} }}, x: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 9 }} }} }} }} }}
}});

// C11 — Expected vs Reported monthly
new Chart(document.getElementById('c11'), {{
  type: 'line',
  data: {{ labels: MO, datasets: [
    {{ label: 'Expected', data: {js(evr_month_expected)}, borderColor: '#4f46e5', borderDash: [5,3], tension: 0.3, pointRadius: 3, borderWidth: 2 }},
    {{ label: 'Reported', data: {js(evr_month_reported)}, borderColor: '#0891b2', backgroundColor: 'rgba(8,145,178,0.06)', fill: true, tension: 0.3, pointRadius: 3, borderWidth: 2 }}
  ]}},
  options: {{ ...noAR, plugins: {{ legend: {{ position: 'bottom' }} }},
    scales: {{ y: {{ grid: {{ color: G }}, ticks: {{ font: {{ size: 9 }}, callback: v => (v/1000).toFixed(0)+'K' }} }}, x: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 9 }} }} }} }} }}
}});
</script>
</body>
</html>"""

out_b = os.path.join(BASE, 'dashboard_presentation_real.html')
with open(out_b, 'w', encoding='utf-8') as f:
    f.write(dashboard_b)
print(f"Written: {out_b}")
print("\n=== DONE ===")

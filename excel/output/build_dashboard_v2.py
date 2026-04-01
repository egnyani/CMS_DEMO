"""
build_dashboard_v2.py
Generates an updated dashboard_real.html with:
 - Period dropdown (individual months + Last 3 / Last 6 rollups)
 - All charts/KPIs respond to period selection
 - Activity Type and Age Distribution charts swapped
 - Pie charts: legend on right, vertically centred
 - Square legend markers globally
 - Distinct colors per state
 - Age x Activity x-axis horizontal (no slant), wrapped labels
 - Cleaned header (title + dropdown only)
 - Renamed KPIs
"""

import csv, os, json
from datetime import date
from collections import defaultdict, OrderedDict

BASE = os.path.dirname(os.path.abspath(__file__))
REF_DATE = date(2026, 3, 30)

# ── helpers ────────────────────────────────────────────────────────────────────
def read_csv(fn):
    with open(os.path.join(BASE, fn), newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def fmt_num(n): return f"{int(n):,}"

def fmt_M(n):
    n = float(n)
    if n >= 1_000_000: return f"{n/1_000_000:.2f}M"
    if n >= 1_000:     return f"{n/1_000:.1f}K"
    return f"{n:.0f}"

def age_from_dob(dob_str):
    try:
        dob = date.fromisoformat(dob_str)
        return REF_DATE.year - dob.year - ((REF_DATE.month, REF_DATE.day) < (dob.month, dob.day))
    except: return None

def age_group(age):
    if age is None: return 'unknown'
    if age <= 18: return '0-18'
    if age <= 64: return '19-64'
    return '65+'

def js(v): return json.dumps(v, ensure_ascii=False)

def wrap_label(s, max_len=12):
    """Return a JS-safe label: list of strings if long, else plain string."""
    if len(s) <= max_len: return s
    words, lines, cur = s.split(), [], ''
    for w in words:
        if cur and len(cur) + 1 + len(w) > max_len:
            lines.append(cur); cur = w
        else:
            cur = (cur + ' ' + w).strip()
    if cur: lines.append(cur)
    return lines

# ── load CSVs ──────────────────────────────────────────────────────────────────
print("Loading CSVs...")
recipients     = read_csv('dim_medicaid_recipient.csv')
establishments = read_csv('dim_establishment.csv')
cal_months     = read_csv('dim_calendar_month.csv')
activities     = read_csv('fact_engagement_activity.csv')
monthly_eng    = read_csv('fact_recipient_monthly_engagement.csv')
print(f"  recipients={len(recipients)}  activities={len(activities)}  monthly_eng={len(monthly_eng)}")

# ── static lookups ─────────────────────────────────────────────────────────────
month_keys_sorted = sorted(r['calendar_month_key'] for r in cal_months)
month_name_map    = {r['calendar_month_key']: r['month_name']       for r in cal_months}
key_to_ym         = {r['calendar_month_key']: r['month_start_date'][:7] for r in cal_months}
est_name_map      = {r['establishment_key']:  r['establishment_name']    for r in establishments}
recip_state_map   = {r['medicaid_recipient_key']: r['eligible_state_code'] for r in recipients}
recip_detail      = {r['medicaid_recipient_key']: r                       for r in recipients}

# age group per recipient (from dim table)
recip_agegroup_map = {}
for r in recipients:
    recip_agegroup_map[r['medicaid_recipient_key']] = age_group(age_from_dob(r['date_of_birth']))

# registration counts by YYYY-MM
reg_by_month = defaultdict(int)
for r in recipients:
    reg_by_month[r['registration_date'][:7]] += 1

# states (sorted, distinct)
states_sorted = sorted({r['eligible_state_code'] for r in recipients})

# pre-index activities and monthly_eng by calendar_month_key
acts_by_month = defaultdict(list)
for r in activities: acts_by_month[r['calendar_month_key']].append(r)

eng_by_month = defaultdict(list)
for r in monthly_eng: eng_by_month[r['calendar_month_key']].append(r)

# ── constant (non-time-filtered) data ─────────────────────────────────────────
total_recipients = len(recipients)

enroll_counts = defaultdict(int)
for r in recipients: enroll_counts[r['enrollment_type_code']] += 1
enroll_labels = sorted(enroll_counts.keys())
enroll_data   = [enroll_counts[k] for k in enroll_labels]

est_type_counts = defaultdict(int)
for r in establishments: est_type_counts[r['establishment_type']] += 1
est_type_labels = sorted(est_type_counts.keys())
est_type_data   = [est_type_counts[k] for k in est_type_labels]

age_counts = defaultdict(int)
for ag in recip_agegroup_map.values(): age_counts[ag] += 1
AGE_LABELS = ['0-18', '19-64', '65+']
age_data = [age_counts.get(ag, 0) for ag in AGE_LABELS]
age_pcts = [round(c / total_recipients * 100, 1) for c in age_data]

state_recip_data = [sum(1 for r in recipients if r['eligible_state_code'] == s) for s in states_sorted]

PALETTE = ['#4f46e5','#0891b2','#059669','#ea580c','#db2777','#7c3aed',
           '#ca8a04','#dc2626','#6366f1','#0d9488','#c026d3']
STATE_COLORS = PALETTE[:len(states_sorted)]

# ── period definitions ─────────────────────────────────────────────────────────
period_configs = OrderedDict([
    ('202603', {'label': 'March 2026',    'keys': ['202603']}),
    ('202602', {'label': 'February 2026', 'keys': ['202602']}),
    ('202601', {'label': 'January 2026',  'keys': ['202601']}),
    ('202512', {'label': 'December 2025', 'keys': ['202512']}),
    ('202511', {'label': 'November 2025', 'keys': ['202511']}),
    ('202510', {'label': 'October 2025',  'keys': ['202510']}),
    ('last3',  {'label': 'Last 3 Months', 'keys': ['202601','202602','202603']}),
    ('last6',  {'label': 'Last 6 Months',
                'keys': ['202510','202511','202512','202601','202602','202603']}),
])

# ── per-period computation ─────────────────────────────────────────────────────
def compute_period(keys_list):
    ks = set(keys_list)
    p_acts = []
    p_eng  = []
    for k in keys_list:
        p_acts.extend(acts_by_month.get(k, []))
        p_eng.extend(eng_by_month.get(k, []))

    p_month_keys   = [k for k in month_keys_sorted if k in ks]
    p_month_labels = [month_name_map[k] for k in p_month_keys]

    # ── KPIs ──
    activity_records = len(p_acts)
    p_yms = {key_to_ym[k] for k in keys_list if k in key_to_ym}
    registered_in_period = sum(1 for r in recipients if r['registration_date'][:7] in p_yms)

    total_hours_raw = sum(float(r['monthly_hours_completed']) for r in p_eng) if p_eng else 0
    avg_hrs_mo = round(total_hours_raw / len(p_eng), 1) if p_eng else 0
    overlap_count = sum(1 for r in p_acts if r['is_overlap_flag'] == 'True')
    overlap_pct   = round(overlap_count / activity_records * 100, 1) if activity_records else 0

    # ── monthly registrations ──
    monthly_reg = [reg_by_month.get(key_to_ym.get(k,''), 0) for k in p_month_keys]

    # ── activity type distribution ──
    act_cnt = defaultdict(int)
    for r in p_acts: act_cnt[r['activity_type']] += 1
    act_sorted = sorted(act_cnt.items(), key=lambda x: -x[1])
    act_labels = [a[0] for a in act_sorted]
    act_data   = [a[1] for a in act_sorted]

    # ── top 15 establishments ──
    est_rset = defaultdict(set)
    for r in p_acts: est_rset[r['establishment_key']].add(r['medicaid_recipient_key'])
    top15 = sorted(est_rset.items(), key=lambda x: -len(x[1]))[:15]
    top15_labels = [est_name_map.get(k, f'Est {k}') for k, v in top15]
    top15_data   = [len(v) for k, v in top15]

    # ── recipients per activity type ──
    act_rset = defaultdict(set)
    for r in p_acts: act_rset[r['activity_type']].add(r['medicaid_recipient_key'])
    act_recip_s = sorted(act_rset.items(), key=lambda x: -len(x[1]))
    act_recip_labels = [a[0] for a in act_recip_s]
    act_recip_data   = [len(a[1]) for a in act_recip_s]

    # ── avg monthly hours ──
    hrs_by_mk = defaultdict(list)
    for r in p_eng: hrs_by_mk[r['calendar_month_key']].append(float(r['monthly_hours_completed']))
    avg_hrs_per_month = [round(sum(hrs_by_mk.get(k,[]))/max(1,len(hrs_by_mk.get(k,[1]))),1)
                         for k in p_month_keys]

    # ── hours distribution ──
    bkt = {'0-20':0,'20-40':0,'40-60':0,'60-80':0,'80':0,'80-120':0,'120+':0}
    for r in p_eng:
        h = float(r['monthly_hours_completed'])
        if   h < 20:  bkt['0-20']   += 1
        elif h < 40:  bkt['20-40']  += 1
        elif h < 60:  bkt['40-60']  += 1
        elif h < 80:  bkt['60-80']  += 1
        elif h == 80: bkt['80']     += 1
        elif h <= 120:bkt['80-120'] += 1
        else:         bkt['120+']   += 1
    hrs_dist_data = list(bkt.values())

    # ── age × activity (top 6 by count this period) ──
    top6 = [a[0] for a in act_sorted[:6]]
    aa_cnt = defaultdict(lambda: defaultdict(int))
    for r in p_acts:
        ag = recip_agegroup_map.get(r['medicaid_recipient_key'],'unknown')
        if r['activity_type'] in top6: aa_cnt[r['activity_type']][ag] += 1
    aa_018  = [aa_cnt[t].get('0-18',  0) for t in top6]
    aa_1964 = [aa_cnt[t].get('19-64', 0) for t in top6]
    aa_65p  = [aa_cnt[t].get('65+',   0) for t in top6]

    # wrapped x-axis labels for age×activity chart
    top6_wrapped = [wrap_label(t) for t in top6]

    # ── monthly hours by age group ──
    age_m = defaultdict(lambda: defaultdict(list))
    for r in p_eng: age_m[r['calendar_month_key']][r['age_group']].append(float(r['monthly_hours_completed']))
    def avg_l(lst): return round(sum(lst)/len(lst),1) if lst else 0
    age_m_018  = [avg_l(age_m.get(k,{}).get('0-18', [])) for k in p_month_keys]
    age_m_1964 = [avg_l(age_m.get(k,{}).get('19-64',[])) for k in p_month_keys]
    age_m_65p  = [avg_l(age_m.get(k,{}).get('65+',  [])) for k in p_month_keys]

    # ── fraud trend ──
    fr_80hr = [sum(1 for r in eng_by_month.get(k,[]) if r['is_exactly_80_hours_flag']=='True') for k in p_month_keys]
    fr_ov   = [sum(1 for r in acts_by_month.get(k,[]) if r['is_overlap_flag']=='True') for k in p_month_keys]
    fr_dom  = [sum(1 for r in eng_by_month.get(k,[]) if r['is_single_establishment_dominant_flag']=='True') for k in p_month_keys]

    # ── section 5 KPIs ──
    ex80     = sum(1 for r in p_eng if r['is_exactly_80_hours_flag']=='True')
    ex80c    = sum(1 for r in p_eng if r['is_exactly_80_hours_consecutive_flag']=='True')

    # risk
    rf = defaultdict(lambda:{'consec':0,'high':False,'dom':0})
    for r in p_eng:
        k2 = r['medicaid_recipient_key']
        c = int(r['exact_80_consecutive_month_count'])
        if c > rf[k2]['consec']: rf[k2]['consec'] = c
        if r['is_exactly_80_hours_consecutive_flag']=='True' and r['is_single_establishment_dominant_flag']=='True':
            rf[k2]['high'] = True
        if r['is_single_establishment_dominant_flag']=='True': rf[k2]['dom'] += 1
    high_risk   = len({k2 for k2,v in rf.items() if v['high']})
    medium_risk = len({k2 for k2,v in rf.items() if not v['high'] and (v['dom']>=2 or v['consec']>=2)})

    # flagged table
    p_ro = defaultdict(int); p_ra = defaultdict(int)
    for r in p_acts:
        p_ra[r['medicaid_recipient_key']] += 1
        if r['is_overlap_flag']=='True': p_ro[r['medicaid_recipient_key']] += 1

    flagged = []
    for k2,v in rf.items():
        risk = 'High' if v['high'] else ('Medium' if v['dom']>=2 or v['consec']>=2 else None)
        if risk is None: continue
        ta = p_ra.get(k2,0)
        ov = round(p_ro.get(k2,0)/ta*100) if ta else 0
        rd = recip_detail.get(k2,{})
        msis = rd.get('msis_identification_num', k2)
        st   = rd.get('eligible_state_code','?')
        fp = []
        if v['consec']>=1: fp.append(f"80×{v['consec']}")
        if v['high']:       fp.append("Consec+Dom")
        if v['dom']>=2:     fp.append(f"DomEst×{v['dom']}")
        flagged.append(((0 if risk=='High' else 1, -v['consec']), msis, st, risk, ', '.join(fp) or 'Pattern', v['consec'], ov))
    flagged.sort(key=lambda x:x[0])
    flagged_html = ''
    for row in flagged[:10]:
        _,msis,st,risk,fp,consec,ov = row
        cls = 'high' if risk=='High' else 'medium'
        flagged_html += (f'<tr><td style="font-family:monospace;font-size:12px">{msis}</td>'
                         f'<td>{st}</td><td><span class="badge {cls}">{risk}</span></td>'
                         f'<td style="font-size:12px">{fp}</td><td>{consec}</td><td>{ov}%</td></tr>\n')

    # ── section 6 ──
    s_exp = defaultdict(int); s_rep = defaultdict(float)
    for r in p_eng:
        st = recip_state_map.get(r['medicaid_recipient_key'],'UNK')
        s_exp[st] += 80
        s_rep[st] += float(r['monthly_hours_completed'])
    evr_exp = [s_exp.get(s,0) for s in states_sorted]
    evr_rep = [round(s_rep.get(s,0)) for s in states_sorted]
    m_exp = [sum(80 for r in eng_by_month.get(k,[]) if r['calendar_month_key']==k) for k in p_month_keys]
    m_rep = [round(sum(float(r['monthly_hours_completed']) for r in eng_by_month.get(k,[]))) for k in p_month_keys]
    tot_exp = sum(evr_exp); tot_rep = sum(evr_rep)
    rep_ratio = round(tot_rep/tot_exp*100,1) if tot_exp else 0

    # heatmap: state × month in period
    sm_exp = defaultdict(int); sm_rep = defaultdict(float)
    for r in p_eng:
        st = recip_state_map.get(r['medicaid_recipient_key'],'UNK')
        sm_exp[(st, r['calendar_month_key'])] += 80
        sm_rep[(st, r['calendar_month_key'])] += float(r['monthly_hours_completed'])

    heat_head = '<th>State</th>' + ''.join(f'<th>{month_name_map[k]}</th>' for k in p_month_keys) + '<th>Total</th>'
    heat_body = ''
    for st in states_sorted:
        heat_body += f'<tr><td style="font-weight:600">{st}</td>'
        te=0; tr2=0
        for k in p_month_keys:
            e = sm_exp.get((st,k),0); rp = sm_rep.get((st,k),0)
            te+=e; tr2+=rp
            v = round(rp/e*100) if e else 0
            bg = 'rgba(5,150,105,0.12)' if v>=90 else ('rgba(79,70,229,0.08)' if v>=85 else ('rgba(202,138,4,0.1)' if v>=80 else 'rgba(220,38,38,0.1)'))
            heat_body += f'<td style="background:{bg};text-align:center;font-weight:600">{v}%</td>'
        tv = round(tr2/te*100) if te else 0
        tbg = 'rgba(5,150,105,0.12)' if tv>=90 else ('rgba(79,70,229,0.08)' if tv>=85 else ('rgba(202,138,4,0.1)' if tv>=80 else 'rgba(220,38,38,0.1)'))
        heat_body += f'<td style="background:{tbg};text-align:center;font-weight:600">{tv}%</td>'
        heat_body += '</tr>\n'

    return {
        # KPIs
        'activity_records': activity_records,
        'activity_records_fmt': fmt_num(activity_records),
        'registered': registered_in_period,
        'registered_fmt': fmt_num(registered_in_period),
        'avg_hrs_mo': avg_hrs_mo,
        'total_hours_fmt': fmt_M(total_hours_raw),
        'overlap_count': overlap_count,
        'overlap_count_fmt': fmt_num(overlap_count),
        'overlap_pct': overlap_pct,
        # S5
        'ex80': ex80, 'ex80_fmt': fmt_num(ex80),
        'ex80c': ex80c, 'ex80c_fmt': fmt_num(ex80c),
        'high_risk': high_risk, 'high_risk_fmt': fmt_num(high_risk),
        'medium_risk': medium_risk, 'medium_risk_fmt': fmt_num(medium_risk),
        # S6
        'tot_exp_fmt': fmt_M(tot_exp),
        'tot_rep_fmt': fmt_M(tot_rep),
        'rep_ratio': rep_ratio,
        # chart data
        'month_labels': p_month_labels,
        'monthly_reg': monthly_reg,
        'act_labels': act_labels,
        'act_data': act_data,
        'top15_labels': top15_labels,
        'top15_data': top15_data,
        'act_recip_labels': act_recip_labels,
        'act_recip_data': act_recip_data,
        'avg_hrs_per_month': avg_hrs_per_month,
        'hrs_dist_data': hrs_dist_data,
        'top6': top6,
        'top6_wrapped': top6_wrapped,
        'aa_018': aa_018, 'aa_1964': aa_1964, 'aa_65p': aa_65p,
        'age_m_018': age_m_018, 'age_m_1964': age_m_1964, 'age_m_65p': age_m_65p,
        'fr_80hr': fr_80hr, 'fr_ov': fr_ov, 'fr_dom': fr_dom,
        'evr_exp': evr_exp, 'evr_rep': evr_rep,
        'm_exp': m_exp, 'm_rep': m_rep,
        # HTML
        'flagged_html': flagged_html,
        'heat_head': heat_head,
        'heat_body': heat_body,
    }

print("Computing period data...")
all_data = {pid: compute_period(pc['keys']) for pid, pc in period_configs.items()}
print(f"  Done. Periods: {list(all_data.keys())}")

# ── build HTML ─────────────────────────────────────────────────────────────────
# pre-render default period for non-JS fallback initial HTML
d0 = all_data['202603']

period_options_html = '\n'.join(
    f'<option value="{pid}"{" selected" if pid=="202603" else ""}>{pc["label"]}</option>'
    for pid, pc in period_configs.items()
)

heatmap_months_all = ''.join(f'<th>{month_name_map[k]}</th>' for k in month_keys_sorted)

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Medicaid Community Engagement Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4"></script>
<style>
:root{{
  --bg:#f5f6fa;--surface:#fff;--card:#fff;--border:#e2e5f0;
  --text:#1e2033;--muted:#6b7194;--accent:#4f46e5;--accent2:#0891b2;
  --green:#059669;--red:#dc2626;--orange:#ea580c;--yellow:#ca8a04;
  --pink:#db2777;--purple:#7c3aed;
}}
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{font-family:'Segoe UI',-apple-system,BlinkMacSystemFont,sans-serif;background:var(--bg);color:var(--text);min-height:100vh;}}
.topbar{{background:var(--surface);border-bottom:1px solid var(--border);padding:10px 24px;display:flex;align-items:center;position:sticky;top:0;z-index:100;box-shadow:0 1px 3px rgba(0,0,0,0.06);}}
.topbar h1{{font-size:15px;font-weight:700;letter-spacing:0.2px;white-space:nowrap;}}
.period-select{{background:var(--bg);border:1px solid var(--border);color:var(--text);padding:5px 10px;border-radius:6px;font-size:13px;cursor:pointer;font-weight:600;margin-left:14px;}}
.period-select:focus{{outline:none;border-color:var(--accent);}}
.container{{padding:16px 24px;max-width:1440px;margin:0 auto;}}
.section-title{{font-size:12px;font-weight:700;color:var(--accent);text-transform:uppercase;letter-spacing:1.2px;margin:20px 0 10px;padding-bottom:6px;border-bottom:2px solid var(--border);}}
.section-title:first-child{{margin-top:0;}}
.kpi-row{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin-bottom:10px;}}
.kpi-card{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px 16px;position:relative;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.04);}}
.kpi-card::before{{content:'';position:absolute;top:0;left:0;right:0;height:3px;}}
.kpi-card.purple::before{{background:var(--accent);}}
.kpi-card.cyan::before{{background:var(--accent2);}}
.kpi-card.green::before{{background:var(--green);}}
.kpi-card.orange::before{{background:var(--orange);}}
.kpi-card.pink::before{{background:var(--pink);}}
.kpi-card.red::before{{background:var(--red);}}
.kpi-card.yellow::before{{background:var(--yellow);}}
.kpi-label{{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:0.6px;margin-bottom:6px;line-height:1.3;}}
.kpi-value{{font-size:26px;font-weight:700;line-height:1;}}
.kpi-sub{{font-size:11px;color:var(--muted);margin-top:4px;}}
.grid-2{{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px;}}
.grid-3{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-bottom:10px;}}
.grid-2-1{{display:grid;grid-template-columns:2fr 1fr;gap:10px;margin-bottom:10px;}}
.chart-card{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px 16px;box-shadow:0 1px 3px rgba(0,0,0,0.04);}}
.chart-card h3{{font-size:13px;font-weight:600;margin-bottom:2px;}}
.chart-card .chart-sub{{font-size:11px;color:var(--muted);margin-bottom:10px;}}
.chart-wrap{{position:relative;width:100%;}}
.chart-wrap canvas{{width:100% !important;}}
table.data-table{{width:100%;border-collapse:collapse;font-size:13px;}}
table.data-table th{{text-align:left;color:var(--muted);font-weight:600;padding:8px 10px;border-bottom:2px solid var(--border);font-size:11px;text-transform:uppercase;letter-spacing:0.5px;}}
table.data-table td{{padding:8px 10px;border-bottom:1px solid var(--border);}}
table.data-table tr:hover td{{background:rgba(79,70,229,0.04);}}
.badge{{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;}}
.badge.high{{background:rgba(220,38,38,0.1);color:var(--red);}}
.badge.medium{{background:rgba(234,88,12,0.1);color:var(--orange);}}
.badge.low{{background:rgba(5,150,105,0.1);color:var(--green);}}
@media(max-width:1100px){{.grid-2,.grid-3,.grid-2-1{{grid-template-columns:1fr;}}}}
@media(max-width:1440px){{.container{{padding:12px 16px;}}.kpi-value{{font-size:22px;}}.kpi-card{{padding:10px 12px;}}.chart-card{{padding:10px 12px;}}}}
</style>
</head>
<body>

<div class="topbar">
  <h1>Medicaid Community Engagement Dashboard</h1>
  <select id="periodSelector" class="period-select" onchange="updateDashboard(this.value)">
    {period_options_html}
  </select>
</div>

<div class="container">

  <!-- ═══ SECTION 1 ═══ -->
  <div class="section-title">1 &mdash; Recipient &amp; Enrollment Metrics</div>
  <div class="kpi-row">
    <div class="kpi-card purple">
      <div class="kpi-label">Total Number of Distinct Recipients Enrolled</div>
      <div class="kpi-value">{fmt_num(total_recipients)}</div>
    </div>
    <div class="kpi-card cyan">
      <div class="kpi-label">Total Activity Records</div>
      <div class="kpi-value" id="kpi-records">{d0['activity_records_fmt']}</div>
    </div>
    <div class="kpi-card green">
      <div class="kpi-label">New Registrations</div>
      <div class="kpi-value" id="kpi-registered">{d0['registered_fmt']}</div>
      <div class="kpi-sub" id="kpi-registered-sub">March 2026</div>
    </div>
    <div class="kpi-card orange">
      <div class="kpi-label">Registered Today</div>
      <div class="kpi-value">{fmt_num(sum(1 for r in recipients if r['registration_date']=='2026-03-30'))}</div>
      <div class="kpi-sub">30 Mar 2026</div>
    </div>
    <div class="kpi-card pink">
      <div class="kpi-label">Avg Activities / Recipient</div>
      <div class="kpi-value">{round(len(activities)/len(recipients),1)}</div>
      <div class="kpi-sub">Across 6 months</div>
    </div>
  </div>

  <div class="grid-2">
    <div class="chart-card">
      <h3>Enrollment Type Distribution</h3>
      <div class="chart-sub">Breakdown of recipient enrollment categories</div>
      <div class="chart-wrap"><canvas id="chartEnrollment"></canvas></div>
    </div>
    <div class="chart-card">
      <h3>Monthly New Registrations</h3>
      <div class="chart-sub">Recipients registered per month</div>
      <div class="chart-wrap"><canvas id="chartMonthlyReg"></canvas></div>
    </div>
  </div>

  <!-- ═══ SECTION 2 ═══ -->
  <div class="section-title">2 &mdash; Activity &amp; Establishment Insights</div>
  <div class="grid-3">
    <div class="chart-card">
      <h3>Age Distribution</h3>
      <div class="chart-sub">{fmt_num(total_recipients)} recipients &mdash; 3 age groups</div>
      <div class="chart-wrap"><canvas id="chartAge"></canvas></div>
    </div>
    <div class="chart-card">
      <h3>Establishment Type</h3>
      <div class="chart-sub">{len(establishments)} establishments across {len(est_type_labels)} types</div>
      <div class="chart-wrap"><canvas id="chartEstType"></canvas></div>
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

  <!-- ═══ SECTION 3 ═══ -->
  <div class="section-title">3 &mdash; Hours &amp; Engagement Metrics</div>
  <div class="kpi-row">
    <div class="kpi-card cyan">
      <div class="kpi-label">Avg Hrs / Recipient / Month</div>
      <div class="kpi-value" id="kpi-avghrs">{d0['avg_hrs_mo']}</div>
      <div class="kpi-sub">Target: 80 hrs/month</div>
    </div>
    <div class="kpi-card green">
      <div class="kpi-label">Exactly 80 hrs (Flagged)</div>
      <div class="kpi-value" id="kpi-ex80">{d0['ex80_fmt']}</div>
      <div class="kpi-sub">Recipient-months</div>
    </div>
    <div class="kpi-card purple">
      <div class="kpi-label">Total Hours (Selected Period)</div>
      <div class="kpi-value" id="kpi-totalhrs">{d0['total_hours_fmt']}</div>
    </div>
    <div class="kpi-card orange">
      <div class="kpi-label">Overlap Activities</div>
      <div class="kpi-value" id="kpi-overlap">{d0['overlap_count_fmt']}</div>
      <div class="kpi-sub" id="kpi-overlap-sub">{d0['overlap_pct']}% of records</div>
    </div>
  </div>

  <div class="grid-2-1">
    <div class="chart-card">
      <h3>Average Monthly Hours vs 80hr Target</h3>
      <div class="chart-sub">Average hours across all recipients by month</div>
      <div class="chart-wrap"><canvas id="chartMonthlyHours"></canvas></div>
    </div>
    <div class="chart-card">
      <h3>Hours Distribution</h3>
      <div class="chart-sub">Recipient-months by hours bracket</div>
      <div class="chart-wrap"><canvas id="chartHoursDist"></canvas></div>
    </div>
  </div>

  <!-- ═══ SECTION 4 ═══ -->
  <div class="section-title">4 &mdash; Age-Based Analysis</div>
  <div class="grid-2">
    <div class="chart-card">
      <h3>Activity Type Distribution</h3>
      <div class="chart-sub">Activity distribution across types</div>
      <div class="chart-wrap"><canvas id="chartActivityType"></canvas></div>
    </div>
    <div class="chart-card">
      <h3>Age Group &times; Activity Type</h3>
      <div class="chart-sub">Grouped by activity type across age brackets</div>
      <div class="chart-wrap"><canvas id="chartAgeActivity"></canvas></div>
    </div>
  </div>

  <div class="chart-card" style="margin-bottom:8px">
    <h3>Monthly Hours by Age Group</h3>
    <div class="chart-sub">Average monthly engagement hours by age bracket</div>
    <div class="chart-wrap"><canvas id="chartAgeMonthly"></canvas></div>
  </div>

  <!-- ═══ SECTION 5 ═══ -->
  <div class="section-title">5 &mdash; Fraud / Suspicious Pattern Detection</div>
  <div class="kpi-row">
    <div class="kpi-card red">
      <div class="kpi-label">High Risk Recipients</div>
      <div class="kpi-value" id="kpi-highrisk">{d0['high_risk_fmt']}</div>
      <div class="kpi-sub">Consecutive 80-hr &amp; dominant establishment</div>
    </div>
    <div class="kpi-card orange">
      <div class="kpi-label">Medium Risk</div>
      <div class="kpi-value" id="kpi-medrisk">{d0['medium_risk_fmt']}</div>
      <div class="kpi-sub">Single dominant establishment pattern</div>
    </div>
    <div class="kpi-card yellow">
      <div class="kpi-label">Overlap Rate</div>
      <div class="kpi-value" id="kpi-overlaprate">{d0['overlap_pct']}%</div>
      <div class="kpi-sub" id="kpi-overlaprate-sub">{d0['overlap_count_fmt']} activities</div>
    </div>
    <div class="kpi-card purple">
      <div class="kpi-label">Exact 80-hr Consecutive</div>
      <div class="kpi-value" id="kpi-ex80c">{d0['ex80c_fmt']}</div>
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
      <h3>Flagged Recipients</h3>
      <div class="chart-sub">Top flagged recipients by risk score</div>
      <div style="max-height:340px;overflow-y:auto">
        <table class="data-table" id="fraudTable">
          <thead><tr><th>Recipient ID</th><th>State</th><th>Risk Level</th><th>Flags</th><th>Consec. 80hrs</th><th>Overlap %</th></tr></thead>
          <tbody id="fraudTableBody">{d0['flagged_html']}</tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- ═══ SECTION 6 ═══ -->
  <div class="section-title">6 &mdash; Expected vs Reported Hours</div>
  <div class="kpi-row" style="grid-template-columns:repeat(3,1fr);">
    <div class="kpi-card green">
      <div class="kpi-label">Total Expected Hours</div>
      <div class="kpi-value" id="kpi-totexp">{d0['tot_exp_fmt']}</div>
      <div class="kpi-sub">80 hrs/mo × enrolled × months</div>
    </div>
    <div class="kpi-card cyan">
      <div class="kpi-label">Total Reported Hours</div>
      <div class="kpi-value" id="kpi-totrep">{d0['tot_rep_fmt']}</div>
    </div>
    <div class="kpi-card orange">
      <div class="kpi-label">Reporting Ratio</div>
      <div class="kpi-value" id="kpi-repratio">{d0['rep_ratio']}%</div>
      <div class="kpi-sub">Reported / Expected</div>
    </div>
  </div>

  <div class="grid-2">
    <div class="chart-card">
      <h3>Expected vs Reported Hours by State</h3>
      <div class="chart-wrap"><canvas id="chartExpVsRepState"></canvas></div>
    </div>
    <div class="chart-card">
      <h3>Expected vs Reported Hours (Monthly Trend)</h3>
      <div class="chart-wrap"><canvas id="chartExpVsRepMonth"></canvas></div>
    </div>
  </div>

  <div class="chart-card" style="margin-bottom:8px">
    <h3>Reporting Ratio by State &amp; Month</h3>
    <div class="chart-sub">Heatmap: reported as % of expected</div>
    <div style="overflow-x:auto">
      <table class="data-table" id="heatmapTable">
        <thead id="heatmapHead"><tr id="heatmapHeadRow">{d0['heat_head']}</tr></thead>
        <tbody id="heatmapBody">{d0['heat_body']}</tbody>
      </table>
    </div>
  </div>

  <div style="text-align:center;padding:32px 0 16px;color:var(--muted);font-size:12px">
    Medicaid Community Engagement Verification System &mdash; As of 30 Mar 2026
  </div>
</div>

<script>
// ── Global Chart defaults: square legend markers ────────────────────────────
Chart.defaults.color = '#6b7194';
Chart.defaults.borderColor = 'rgba(0,0,0,0.08)';
Chart.defaults.font.family = "-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif";
Chart.defaults.font.size = 11;
Chart.defaults.plugins.legend.labels.usePointStyle = false;
Chart.defaults.plugins.legend.labels.boxWidth  = 12;
Chart.defaults.plugins.legend.labels.boxHeight = 12;
Chart.defaults.plugins.legend.labels.padding = 12;

const G = 'rgba(0,0,0,0.06)';
const COLORS = {js(PALETTE)};
const STATES = {js(states_sorted)};
const STATE_COLORS = {js(STATE_COLORS)};

// ── Constant data (enrollment, establishment, age, state) ───────────────────
const CONST = {{
  enroll_labels: {js(enroll_labels)},
  enroll_data:   {js(enroll_data)},
  est_labels: {js(est_type_labels)},
  est_data:   {js(est_type_data)},
  age_labels: {js(AGE_LABELS)},
  age_data:   {js(age_data)},
  age_pcts:   {js(age_pcts)},
  state_data: {js(state_recip_data)},
}};

// ── All period data ─────────────────────────────────────────────────────────
const ALL_DATA = {js(all_data)};

// ── Pie / Donut factory ─────────────────────────────────────────────────────
function makePie(id, labels, data, colors, aspectRatio) {{
  return new Chart(document.getElementById(id), {{
    type: 'doughnut',
    data: {{ labels, datasets: [{{ data, backgroundColor: colors || COLORS,
      borderWidth: 2, borderColor: '#fff', hoverOffset: 6 }}] }},
    options: {{
      cutout: '62%', responsive: true, maintainAspectRatio: true,
      aspectRatio: aspectRatio || 2,
      plugins: {{
        legend: {{
          position: 'right', align: 'center',
          labels: {{ usePointStyle: false, boxWidth: 12, boxHeight: 12, padding: 10, font: {{ size: 11 }} }}
        }}
      }}
    }}
  }});
}}

// ── Create all charts ───────────────────────────────────────────────────────
const d = ALL_DATA['202603'];

// Section 1
const cEnroll = makePie('chartEnrollment', CONST.enroll_labels, CONST.enroll_data,
  ['#4f46e5','#0891b2','#059669'], 2.2);

const cMonthReg = new Chart(document.getElementById('chartMonthlyReg'), {{
  type: 'bar',
  data: {{ labels: d.month_labels, datasets: [{{ label:'New Registrations', data: d.monthly_reg,
    backgroundColor: '#4f46e5', borderRadius: 6, barPercentage: 0.55 }}] }},
  options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ beginAtZero: true, grid: {{ color: G }} }}, x: {{ grid: {{ display: false }} }} }} }}
}});

// Section 2 — Age donut (moved here)
const cAge = makePie('chartAge', CONST.age_labels, CONST.age_data,
  ['#0891b2','#4f46e5','#ea580c'], 2.2);

// Section 2 — Establishment donut
const cEstType = makePie('chartEstType', CONST.est_labels, CONST.est_data,
  ['#4f46e5','#0891b2','#059669','#ea580c','#db2777','#7c3aed'], 2.2);

// Section 2 — State bar (distinct color per state)
const cState = new Chart(document.getElementById('chartState'), {{
  type: 'bar',
  data: {{ labels: STATES, datasets: [{{ data: CONST.state_data,
    backgroundColor: STATE_COLORS, borderRadius: 6, barPercentage: 0.55 }}] }},
  options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ beginAtZero: true, grid: {{ color: G }} }}, x: {{ grid: {{ display: false }} }} }} }}
}});

// Section 2 — Top 15 establishments
const cRecipEst = new Chart(document.getElementById('chartRecipEst'), {{
  type: 'bar',
  data: {{ labels: d.top15_labels, datasets: [{{ data: d.top15_data,
    backgroundColor: '#0891b2', borderRadius: 4, barPercentage: 0.6 }}] }},
  options: {{ indexAxis: 'y', responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ x: {{ beginAtZero: true, grid: {{ color: G }} }},
      y: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 10 }} }} }} }} }}
}});

// Section 2 — Recipients per activity
const cRecipAct = new Chart(document.getElementById('chartRecipAct'), {{
  type: 'bar',
  data: {{ labels: d.act_recip_labels, datasets: [{{ label:'Distinct Recipients', data: d.act_recip_data,
    backgroundColor: '#7c3aed', borderRadius: 6, barPercentage: 0.55 }}] }},
  options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ beginAtZero: true, grid: {{ color: G }} }},
      x: {{ grid: {{ display: false }}, ticks: {{ maxRotation: 45, font: {{ size: 10 }} }} }} }} }}
}});

// Section 3 — Monthly hours line
const cMonthlyHrs = new Chart(document.getElementById('chartMonthlyHours'), {{
  type: 'line',
  data: {{ labels: d.month_labels, datasets: [
    {{ label:'Avg Hours/Recipient', data: d.avg_hrs_per_month,
      borderColor:'#4f46e5', backgroundColor:'rgba(79,70,229,0.08)', fill:true, tension:0.35, pointRadius:5 }},
    {{ label:'Required (80 hrs)', data: d.month_labels.map(()=>80),
      borderColor:'#dc2626', borderDash:[6,4], pointRadius:0, borderWidth:2 }}
  ]}},
  options: {{ responsive: true, plugins: {{ legend: {{ position:'bottom' }} }},
    scales: {{ y: {{ beginAtZero:false, grid: {{ color: G }} }}, x: {{ grid: {{ display: false }} }} }} }}
}});

// Section 3 — Hours distribution
const cHoursDist = new Chart(document.getElementById('chartHoursDist'), {{
  type: 'bar',
  data: {{ labels: ['0-20','20-40','40-60','60-80','80','80-120','120+'],
    datasets: [{{ data: d.hrs_dist_data,
    backgroundColor: ['#059669','#0891b2','#4f46e5','#7c3aed','#dc2626','#ea580c','#ca8a04'],
    borderRadius: 4, barPercentage: 0.65 }}] }},
  options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ beginAtZero: true, grid: {{ color: G }}, ticks: {{ callback: v => v>=1000?(v/1000).toFixed(0)+'K':v }} }},
      x: {{ grid: {{ display: false }} }} }} }}
}});

// Section 4 — Activity type horizontal bar (moved here from S2)
const cActivityType = new Chart(document.getElementById('chartActivityType'), {{
  type: 'bar',
  data: {{ labels: d.act_labels, datasets: [{{ data: d.act_data,
    backgroundColor: COLORS, borderRadius: 4, barPercentage: 0.65 }}] }},
  options: {{ indexAxis: 'y', responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ x: {{ beginAtZero: true, grid: {{ color: G }}, ticks: {{ callback: v => v>=1000?(v/1000).toFixed(0)+'K':v }} }},
      y: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 10 }} }} }} }} }}
}});

// Section 4 — Age x Activity stacked bar (horizontal labels, wrapped)
const cAgeAct = new Chart(document.getElementById('chartAgeActivity'), {{
  type: 'bar',
  data: {{ labels: d.top6_wrapped, datasets: [
    {{ label:'0-18',  data: d.aa_018,  backgroundColor:'#0891b2', borderRadius:4 }},
    {{ label:'19-64', data: d.aa_1964, backgroundColor:'#4f46e5', borderRadius:4 }},
    {{ label:'65+',   data: d.aa_65p,  backgroundColor:'#ea580c', borderRadius:4 }}
  ]}},
  options: {{ responsive: true, plugins: {{ legend: {{ position:'bottom' }} }},
    scales: {{
      x: {{ stacked:true, grid: {{ display:false }},
        ticks: {{ maxRotation:0, minRotation:0, font: {{ size:10 }}, autoSkip:false }} }},
      y: {{ stacked:true, beginAtZero:true, grid: {{ color: G }},
        ticks: {{ callback: v => v>=1000?(v/1000).toFixed(0)+'K':v }} }}
    }}
  }}
}});

// Section 4 — Age monthly hours
const cAgeMonthly = new Chart(document.getElementById('chartAgeMonthly'), {{
  type: 'line',
  data: {{ labels: d.month_labels, datasets: [
    {{ label:'0-18',  data: d.age_m_018,  borderColor:'#0891b2', tension:0.35, pointRadius:4 }},
    {{ label:'19-64', data: d.age_m_1964, borderColor:'#4f46e5', tension:0.35, pointRadius:4 }},
    {{ label:'65+',   data: d.age_m_65p,  borderColor:'#ea580c', tension:0.35, pointRadius:4 }},
    {{ label:'Required', data: d.month_labels.map(()=>80),
      borderColor:'#dc2626', borderDash:[6,4], pointRadius:0, borderWidth:2 }}
  ]}},
  options: {{ responsive:true, plugins: {{ legend: {{ position:'bottom' }} }},
    scales: {{ y: {{ beginAtZero:false, grid: {{ color:G }} }}, x: {{ grid: {{ display:false }} }} }} }}
}});

// Section 5 — Fraud trend
const cFraud = new Chart(document.getElementById('chartFraudTrend'), {{
  type: 'line',
  data: {{ labels: d.month_labels, datasets: [
    {{ label:'Exact 80-hr Flags', data: d.fr_80hr,
      borderColor:'#dc2626', backgroundColor:'rgba(220,38,38,0.06)', fill:true, tension:0.35, pointRadius:4 }},
    {{ label:'Overlap Flags', data: d.fr_ov,
      borderColor:'#ea580c', tension:0.35, pointRadius:4 }},
    {{ label:'Dominant Estab. Flags', data: d.fr_dom,
      borderColor:'#ca8a04', tension:0.35, pointRadius:4 }}
  ]}},
  options: {{ responsive:true, plugins: {{ legend: {{ position:'bottom' }} }},
    scales: {{ y: {{ beginAtZero:true, grid: {{ color:G }},
      ticks: {{ callback: v => v>=1000?(v/1000).toFixed(1)+'K':v }} }},
      x: {{ grid: {{ display:false }} }} }} }}
}});

// Section 6 — Expected vs Reported by state
const cEVRState = new Chart(document.getElementById('chartExpVsRepState'), {{
  type: 'bar',
  data: {{ labels: STATES, datasets: [
    {{ label:'Expected', data: d.evr_exp,
      backgroundColor:'rgba(79,70,229,0.15)', borderColor:'#4f46e5', borderWidth:1.5, borderRadius:4, barPercentage:0.7 }},
    {{ label:'Reported', data: d.evr_rep,
      backgroundColor:'#0891b2', borderRadius:4, barPercentage:0.7 }}
  ]}},
  options: {{ responsive:true, plugins: {{ legend: {{ position:'bottom' }} }},
    scales: {{ y: {{ beginAtZero:true, grid: {{ color:G }}, ticks: {{ callback: v => (v/1000).toFixed(0)+'K' }} }},
      x: {{ grid: {{ display:false }} }} }} }}
}});

// Section 6 — Expected vs Reported monthly
const cEVRMonth = new Chart(document.getElementById('chartExpVsRepMonth'), {{
  type: 'line',
  data: {{ labels: d.month_labels, datasets: [
    {{ label:'Expected', data: d.m_exp,
      borderColor:'#4f46e5', borderDash:[6,4], tension:0.3, pointRadius:5 }},
    {{ label:'Reported', data: d.m_rep,
      borderColor:'#0891b2', backgroundColor:'rgba(8,145,178,0.08)', fill:true, tension:0.3, pointRadius:5 }}
  ]}},
  options: {{ responsive:true, plugins: {{ legend: {{ position:'bottom' }} }},
    scales: {{ y: {{ beginAtZero:false, grid: {{ color:G }}, ticks: {{ callback: v => (v/1000).toFixed(0)+'K' }} }},
      x: {{ grid: {{ display:false }} }} }} }}
}});

// ── updateDashboard ─────────────────────────────────────────────────────────
function updateDashboard(pid) {{
  const d = ALL_DATA[pid];
  const lbl = document.querySelector('#periodSelector option[value="'+pid+'"]').textContent;

  // KPIs
  document.getElementById('kpi-records').textContent    = d.activity_records_fmt;
  document.getElementById('kpi-registered').textContent = d.registered_fmt;
  document.getElementById('kpi-registered-sub').textContent = lbl;
  document.getElementById('kpi-avghrs').textContent     = d.avg_hrs_mo;
  document.getElementById('kpi-ex80').textContent       = d.ex80_fmt;
  document.getElementById('kpi-totalhrs').textContent   = d.total_hours_fmt;
  document.getElementById('kpi-overlap').textContent    = d.overlap_count_fmt;
  document.getElementById('kpi-overlap-sub').textContent = d.overlap_pct + '% of records';
  document.getElementById('kpi-highrisk').textContent   = d.high_risk_fmt;
  document.getElementById('kpi-medrisk').textContent    = d.medium_risk_fmt;
  document.getElementById('kpi-overlaprate').textContent     = d.overlap_pct + '%';
  document.getElementById('kpi-overlaprate-sub').textContent = d.overlap_count_fmt + ' activities';
  document.getElementById('kpi-ex80c').textContent      = d.ex80c_fmt;
  document.getElementById('kpi-totexp').textContent     = d.tot_exp_fmt;
  document.getElementById('kpi-totrep').textContent     = d.tot_rep_fmt;
  document.getElementById('kpi-repratio').textContent   = d.rep_ratio + '%';

  // Monthly Reg
  cMonthReg.data.labels = d.month_labels;
  cMonthReg.data.datasets[0].data = d.monthly_reg;
  cMonthReg.update();

  // Activity Type (S4)
  cActivityType.data.labels = d.act_labels;
  cActivityType.data.datasets[0].data = d.act_data;
  cActivityType.update();

  // Top 15 Establishments
  cRecipEst.data.labels = d.top15_labels;
  cRecipEst.data.datasets[0].data = d.top15_data;
  cRecipEst.update();

  // Recipients per Activity
  cRecipAct.data.labels = d.act_recip_labels;
  cRecipAct.data.datasets[0].data = d.act_recip_data;
  cRecipAct.update();

  // Monthly Hours line
  cMonthlyHrs.data.labels = d.month_labels;
  cMonthlyHrs.data.datasets[0].data = d.avg_hrs_per_month;
  cMonthlyHrs.data.datasets[1].data = d.month_labels.map(()=>80);
  cMonthlyHrs.update();

  // Hours Distribution
  cHoursDist.data.datasets[0].data = d.hrs_dist_data;
  cHoursDist.update();

  // Age x Activity
  cAgeAct.data.labels = d.top6_wrapped;
  cAgeAct.data.datasets[0].data = d.aa_018;
  cAgeAct.data.datasets[1].data = d.aa_1964;
  cAgeAct.data.datasets[2].data = d.aa_65p;
  cAgeAct.update();

  // Age Monthly
  cAgeMonthly.data.labels = d.month_labels;
  cAgeMonthly.data.datasets[0].data = d.age_m_018;
  cAgeMonthly.data.datasets[1].data = d.age_m_1964;
  cAgeMonthly.data.datasets[2].data = d.age_m_65p;
  cAgeMonthly.data.datasets[3].data = d.month_labels.map(()=>80);
  cAgeMonthly.update();

  // Fraud Trend
  cFraud.data.labels = d.month_labels;
  cFraud.data.datasets[0].data = d.fr_80hr;
  cFraud.data.datasets[1].data = d.fr_ov;
  cFraud.data.datasets[2].data = d.fr_dom;
  cFraud.update();

  // Fraud Table
  document.getElementById('fraudTableBody').innerHTML = d.flagged_html;

  // EVR State
  cEVRState.data.datasets[0].data = d.evr_exp;
  cEVRState.data.datasets[1].data = d.evr_rep;
  cEVRState.update();

  // EVR Monthly
  cEVRMonth.data.labels = d.month_labels;
  cEVRMonth.data.datasets[0].data = d.m_exp;
  cEVRMonth.data.datasets[1].data = d.m_rep;
  cEVRMonth.update();

  // Heatmap
  document.getElementById('heatmapHeadRow').innerHTML = d.heat_head;
  document.getElementById('heatmapBody').innerHTML    = d.heat_body;
}}
</script>
</body>
</html>"""

out = os.path.join(BASE, 'dashboard_real.html')
with open(out, 'w', encoding='utf-8') as f:
    f.write(html)
print(f"\nWritten: {out}  ({len(html):,} bytes)")

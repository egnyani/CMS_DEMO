"""
build_dashboard_v3.py
Generates dashboard_real.html (v3) with:
 - Realistic synthetic data (v2 generator outputs)
 - Period + State dual filters (pre-computed, no page reload)
 - Age-group toggle (client-side JS)
 - New KPIs: compliance rate, active rate, new vs returning, multistate mismatch
 - New charts: compliance funnel, hours/activity type, recipient status,
               verification method × fraud flag, state × enrollment heatmap
 - Removed duplicate "recipients per activity type" chart
 - Improved heatmap: green ≥60%, yellow 40-60%, red <40%
 - Fraud section: higher flagged counts, red-accent styling
 - Insight-focused chart titles
"""

import csv, json, os, math
from datetime import date, timedelta
from collections import defaultdict, OrderedDict

BASE     = os.path.dirname(os.path.abspath(__file__))
REF_DATE = date(2026, 3, 30)

# ── helpers ─────────────────────────────────────────────────────────────────
def read_csv(fn):
    with open(os.path.join(BASE, fn), newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def fmt_num(n): return f"{int(n):,}"

def fmt_M(n):
    n = float(n)
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000:     return f"{n/1_000:.1f}K"
    return f"{n:.0f}"

def fmt_pct(n): return f"{n:.1f}%"

def age_from_dob(s):
    try:
        d = date.fromisoformat(s)
        return REF_DATE.year - d.year - ((REF_DATE.month, REF_DATE.day) < (d.month, d.day))
    except: return None

def age_group(a):
    if a is None: return 'unknown'
    if a <= 18:   return '0-18'
    if a <= 64:   return '19-64'
    return '65+'

def wrap_label(s, max_len=12):
    if len(s) <= max_len: return s
    words, lines, cur = s.split(), [], ''
    for w in words:
        if cur and len(cur) + 1 + len(w) > max_len: lines.append(cur); cur = w
        else: cur = (cur + ' ' + w).strip()
    if cur: lines.append(cur)
    return lines

def js(v): return json.dumps(v, ensure_ascii=False)

def month_end(d):
    return ((d.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1))

# ── load CSVs ─────────────────────────────────────────────────────────────────
print("Loading CSVs...")
recipients     = read_csv('dim_medicaid_recipient.csv')
establishments = read_csv('dim_establishment.csv')
cal_months     = read_csv('dim_calendar_month.csv')
activities     = read_csv('fact_engagement_activity.csv')
monthly_eng    = read_csv('fact_recipient_monthly_engagement.csv')
veri_methods   = read_csv('dim_verification_method.csv')
print(f"  recipients={len(recipients)}  activities={len(activities)}  monthly={len(monthly_eng)}")

# ── lookups ───────────────────────────────────────────────────────────────────
month_keys_sorted = sorted(r['calendar_month_key'] for r in cal_months)
month_name_map    = {r['calendar_month_key']: r['month_name'] for r in cal_months}
key_to_ym         = {r['calendar_month_key']: r['month_start_date'][:7] for r in cal_months}
est_name_map      = {r['establishment_key']:  r['establishment_name'] for r in establishments}
est_type_by_key   = {r['establishment_key']: r['establishment_type'] for r in establishments}
veri_name_map     = {r['verification_method_key']: r['verification_method'] for r in veri_methods}

recip_state_map   = {r['medicaid_recipient_key']: r['eligible_state_code'] for r in recipients}
recip_sub_map     = {r['medicaid_recipient_key']: r['submitting_state_code'] for r in recipients}
recip_detail      = {r['medicaid_recipient_key']: r for r in recipients}
recip_enroll_map  = {r['medicaid_recipient_key']: r['enrollment_type_code'] for r in recipients}
recip_reg_map     = {r['medicaid_recipient_key']: r['registration_date'] for r in recipients}
recip_agegroup_map= {r['medicaid_recipient_key']: age_group(age_from_dob(r['date_of_birth']))
                     for r in recipients}

total_recipients = len(recipients)
all_states       = sorted({r['eligible_state_code'] for r in recipients})
PALETTE = ['#4f46e5','#0891b2','#059669','#ea580c','#db2777','#7c3aed',
           '#ca8a04','#dc2626','#6366f1','#0d9488','#c026d3']
STATE_COLORS = {s: PALETTE[i % len(PALETTE)] for i, s in enumerate(all_states)}

# ── first-activity dates (for days-to-first metric) ───────────────────────────
recip_first_act_date = {}
for r in activities:
    rk = r['medicaid_recipient_key']
    d_str = r['beginning_date_of_service']
    if rk not in recip_first_act_date or d_str < recip_first_act_date[rk]:
        recip_first_act_date[rk] = d_str

# multistate mismatch count
multistate_count = sum(
    1 for r in recipients
    if r['submitting_state_code'] != r['eligible_state_code']
)

# ── constant data (enrollment, establishment type, age) ────────────────────────
enroll_cnt = defaultdict(int)
for r in recipients: enroll_cnt[r['enrollment_type_code']] += 1
enroll_labels = ['Medicaid', 'Medicaid Expansion CHIP', 'Separate CHIP']
enroll_data   = [enroll_cnt.get(k, 0) for k in enroll_labels]

est_type_cnt = defaultdict(int)
for r in establishments: est_type_cnt[r['establishment_type']] += 1
est_type_labels = sorted(est_type_cnt.keys())
est_type_data   = [est_type_cnt[k] for k in est_type_labels]

AGE_LABELS = ['0-18', '19-64', '65+']
age_cnt    = defaultdict(int)
for ag in recip_agegroup_map.values(): age_cnt[ag] += 1
age_data   = [age_cnt.get(ag, 0) for ag in AGE_LABELS]
age_pcts   = [round(c / total_recipients * 100, 1) for c in age_data]

# per-state enrollment type matrix (for state × enrollment chart)
state_enroll = defaultdict(lambda: defaultdict(int))
for r in recipients:
    state_enroll[r['eligible_state_code']][r['enrollment_type_code']] += 1

# reg counts by YYYY-MM
reg_by_month = defaultdict(int)
for r in recipients: reg_by_month[r['registration_date'][:7]] += 1

# pre-index
acts_by_month = defaultdict(list)
for r in activities: acts_by_month[r['calendar_month_key']].append(r)

eng_by_month = defaultdict(list)
for r in monthly_eng: eng_by_month[r['calendar_month_key']].append(r)

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

# ── core compute function ─────────────────────────────────────────────────────
def compute_period(keys_list, state_filter=None, age_filter=None):
    """age_filter: None (all ages) or one of AGE_LABELS — filters all recipient-level metrics."""

    def recip_ok(rk):
        if state_filter is not None and recip_state_map.get(rk) != state_filter:
            return False
        if age_filter is not None and recip_agegroup_map.get(rk) != age_filter:
            return False
        return True

    ks        = set(keys_list)
    p_acts    = []
    p_eng     = []
    for k in keys_list:
        for r in acts_by_month.get(k, []):
            if recip_ok(r['medicaid_recipient_key']):
                p_acts.append(r)
        for r in eng_by_month.get(k, []):
            if recip_ok(r['medicaid_recipient_key']):
                p_eng.append(r)

    p_month_keys   = [k for k in month_keys_sorted if k in ks]
    p_month_labels = [month_name_map[k] for k in p_month_keys]

    # ── KPIs ──
    activity_records = len(p_acts)
    p_yms = {key_to_ym[k] for k in keys_list if k in key_to_ym}

    # new registrations in period (filtered by state / age if needed)
    registered_in_period = sum(
        1 for r in recipients
        if r['registration_date'][:7] in p_yms
        and (state_filter is None or r['eligible_state_code'] == state_filter)
        and (age_filter is None or recip_agegroup_map.get(r['medicaid_recipient_key']) == age_filter)
    )

    total_hours_raw = sum(float(r['monthly_hours_completed']) for r in p_eng)
    avg_hrs_mo = round(total_hours_raw / len(p_eng), 1) if p_eng else 0

    overlap_count = sum(1 for r in p_acts if r['is_overlap_flag'] == 'True')
    overlap_pct   = round(overlap_count / activity_records * 100, 1) if activity_records else 0

    active_recip_keys_ms = {r['medicaid_recipient_key'] for r in p_acts}
    multistate_count = sum(
        1 for rk in active_recip_keys_ms
        if recip_sub_map.get(rk) != recip_state_map.get(rk)
    )

    # ── compliance rate (% recipient-months ≥ 80 hrs) ──
    compliance_count = sum(1 for r in p_eng if float(r['monthly_hours_completed']) >= 80.0)
    compliance_rate  = round(compliance_count / len(p_eng) * 100, 1) if p_eng else 0

    # ── active recipient rate ──
    active_recips = len({r['medicaid_recipient_key'] for r in p_acts})
    if age_filter is None:
        enrolled_in_state = total_recipients if state_filter is None else sum(
            1 for r in recipients if r['eligible_state_code'] == state_filter)
    else:
        enrolled_in_state = sum(
            1 for r in recipients
            if (state_filter is None or r['eligible_state_code'] == state_filter)
            and recip_agegroup_map.get(r['medicaid_recipient_key']) == age_filter)
    active_rate = round(active_recips / enrolled_in_state * 100, 1) if enrolled_in_state else 0
    avg_acts_per_recip = round(activity_records / active_recips, 1) if active_recips else 0

    # ── new vs returning ──
    recips_active_prior = set()
    prior_keys = [k for k in month_keys_sorted if k < min(ks)]
    for k in prior_keys:
        for r in acts_by_month.get(k, []):
            if recip_ok(r['medicaid_recipient_key']):
                recips_active_prior.add(r['medicaid_recipient_key'])
    recips_in_period = {r['medicaid_recipient_key'] for r in p_acts}
    new_recipients     = len(recips_in_period - recips_active_prior)
    returning_recipients = len(recips_in_period & recips_active_prior)

    # ── avg days to first activity ──
    days_list = []
    for rk in recips_in_period:
        reg_str = recip_reg_map.get(rk)
        fad_str = recip_first_act_date.get(rk)
        if reg_str and fad_str:
            try:
                delta = (date.fromisoformat(fad_str) - date.fromisoformat(reg_str)).days
                if 0 <= delta <= 180: days_list.append(delta)
            except: pass
    avg_days_first = round(sum(days_list) / len(days_list), 1) if days_list else 0

    # ── monthly registrations (respect state + age filters) ──
    monthly_reg = []
    for k in p_month_keys:
        ym = key_to_ym.get(k, '')
        monthly_reg.append(sum(
            1 for r in recipients
            if r['registration_date'][:7] == ym
            and (state_filter is None or r['eligible_state_code'] == state_filter)
            and (age_filter is None or recip_agegroup_map.get(r['medicaid_recipient_key']) == age_filter)
        ))

    # ── activity type distribution ──
    act_cnt = defaultdict(int)
    for r in p_acts: act_cnt[r['activity_type']] += 1
    act_sorted = sorted(act_cnt.items(), key=lambda x: -x[1])
    act_labels = [a[0] for a in act_sorted]
    act_data   = [a[1] for a in act_sorted]

    # ── hours per activity type ──
    act_hrs = defaultdict(float)
    for r in p_acts: act_hrs[r['activity_type']] += float(r['activity_duration_hours'])
    hrs_sorted = sorted(act_hrs.items(), key=lambda x: -x[1])
    act_hrs_labels = [a[0] for a in hrs_sorted]
    act_hrs_data   = [round(a[1] / 1000, 1) for a in hrs_sorted]  # in thousands

    # ── compliance funnel ──
    funnel_bkts = {'0-25%': 0, '25-50%': 0, '50-75%': 0, '75-<100%': 0, '≥100%': 0}
    funnel_exact80 = 0
    for r in p_eng:
        h = float(r['monthly_hours_completed'])
        pct = h / 80.0 * 100
        if math.isclose(h, 80.0, abs_tol=1e-9): funnel_exact80 += 1
        elif pct < 25:  funnel_bkts['0-25%'] += 1
        elif pct < 50:  funnel_bkts['25-50%'] += 1
        elif pct < 75:  funnel_bkts['50-75%'] += 1
        elif pct < 100: funnel_bkts['75-<100%'] += 1
        else:           funnel_bkts['≥100%'] += 1
    funnel_labels = list(funnel_bkts.keys()) + ['= 80hrs']
    funnel_data   = list(funnel_bkts.values()) + [funnel_exact80]

    # ── top 15 establishments ──
    est_rset = defaultdict(set)
    for r in p_acts: est_rset[r['establishment_key']].add(r['medicaid_recipient_key'])
    top15 = sorted(est_rset.items(), key=lambda x: -len(x[1]))[:15]
    top15_labels = [est_name_map.get(k, f'Est {k}') for k, v in top15]
    top15_data   = [len(v) for k, v in top15]

    # ── avg monthly hours ──
    hrs_by_mk = defaultdict(list)
    for r in p_eng: hrs_by_mk[r['calendar_month_key']].append(float(r['monthly_hours_completed']))
    avg_hrs_per_month = [
        round(sum(hrs_by_mk.get(k, [])) / max(1, len(hrs_by_mk.get(k, [0]))), 1)
        for k in p_month_keys
    ]

    # ── hours distribution (for funnel + histogram) ──
    hist_bkt = {'0-20': 0, '20-40': 0, '40-60': 0, '60-80': 0, '80': 0, '80-120': 0, '120+': 0}
    for r in p_eng:
        h = float(r['monthly_hours_completed'])
        if   h < 20:  hist_bkt['0-20']   += 1
        elif h < 40:  hist_bkt['20-40']  += 1
        elif h < 60:  hist_bkt['40-60']  += 1
        elif h < 80:  hist_bkt['60-80']  += 1
        elif math.isclose(h, 80.0, abs_tol=1e-9): hist_bkt['80'] += 1
        elif h <= 120: hist_bkt['80-120'] += 1
        else:          hist_bkt['120+']  += 1
    hrs_dist_data = list(hist_bkt.values())

    # ── recipient status (active / inactive / new) ──
    # Active = had activity in period; New = first period of activity
    # Inactive = enrolled but no activity in period
    all_enrolled_in_filter = [
        r['medicaid_recipient_key'] for r in recipients
        if (state_filter is None or r['eligible_state_code'] == state_filter)
        and (age_filter is None or recip_agegroup_map.get(r['medicaid_recipient_key']) == age_filter)
    ]
    inactive_count = max(0, len(all_enrolled_in_filter) - active_recips)
    status_labels = ['Active', 'Inactive', 'New This Period']
    status_data   = [active_recips - new_recipients, inactive_count, new_recipients]

    # ── age × activity (top 6) ──
    top6 = [a[0] for a in act_sorted[:6]]
    aa_cnt = defaultdict(lambda: defaultdict(int))
    for r in p_acts:
        ag_val = recip_agegroup_map.get(r['medicaid_recipient_key'], 'unknown')
        if r['activity_type'] in top6: aa_cnt[r['activity_type']][ag_val] += 1
    aa_018  = [aa_cnt[t].get('0-18',  0) for t in top6]
    aa_1964 = [aa_cnt[t].get('19-64', 0) for t in top6]
    aa_65p  = [aa_cnt[t].get('65+',   0) for t in top6]
    top6_wrapped = [wrap_label(t) for t in top6]

    # ── monthly hours by age group ──
    age_m = defaultdict(lambda: defaultdict(list))
    for r in p_eng:
        age_m[r['calendar_month_key']][r['age_group']].append(float(r['monthly_hours_completed']))
    def avg_l(lst): return round(sum(lst) / len(lst), 1) if lst else 0
    age_m_018  = [avg_l(age_m.get(k, {}).get('0-18',  [])) for k in p_month_keys]
    age_m_1964 = [avg_l(age_m.get(k, {}).get('19-64', [])) for k in p_month_keys]
    age_m_65p  = [avg_l(age_m.get(k, {}).get('65+',   [])) for k in p_month_keys]

    # ── fraud trend ──
    fr_80hr = [sum(1 for r in eng_by_month.get(k, [])
                   if r['is_exactly_80_hours_flag'] == 'True'
                   and recip_ok(r['medicaid_recipient_key']))
               for k in p_month_keys]
    fr_ov   = [sum(1 for r in acts_by_month.get(k, [])
                   if r['is_overlap_flag'] == 'True'
                   and recip_ok(r['medicaid_recipient_key']))
               for k in p_month_keys]
    fr_dom  = [sum(1 for r in eng_by_month.get(k, [])
                   if r['is_single_establishment_dominant_flag'] == 'True'
                   and recip_ok(r['medicaid_recipient_key']))
               for k in p_month_keys]

    # ── fraud KPIs ──
    ex80  = sum(1 for r in p_eng if r['is_exactly_80_hours_flag'] == 'True')
    ex80c = sum(1 for r in p_eng if r['is_exactly_80_hours_consecutive_flag'] == 'True')

    # Risk scoring: high = consecutive 80hr flag triggered;  medium = dominant est flag triggered
    rf = defaultdict(lambda: {'consec': 0, 'dom': 0})
    for r in p_eng:
        k2 = r['medicaid_recipient_key']
        c  = int(r['exact_80_consecutive_month_count'])
        if c > rf[k2]['consec']: rf[k2]['consec'] = c
        if r['is_exactly_80_hours_consecutive_flag'] == 'True': rf[k2]['consec_flag'] = True
        if r['is_single_establishment_dominant_flag'] == 'True': rf[k2]['dom'] += 1

    high_risk   = len({k2 for k2, v in rf.items() if v.get('consec_flag')})
    medium_risk = len({k2 for k2, v in rf.items()
                       if not v.get('consec_flag') and v['dom'] >= 1})

    # ── flagged table ──
    p_ro = defaultdict(int); p_ra = defaultdict(int)
    for r in p_acts:
        p_ra[r['medicaid_recipient_key']] += 1
        if r['is_overlap_flag'] == 'True': p_ro[r['medicaid_recipient_key']] += 1

    flagged = []
    for k2, v in rf.items():
        risk = 'High' if v.get('consec_flag') else ('Medium' if v['dom'] >= 1 else None)
        if risk is None: continue
        ta = p_ra.get(k2, 0)
        ov = round(p_ro.get(k2, 0) / ta * 100) if ta else 0
        rd = recip_detail.get(k2, {})
        msis = rd.get('msis_identification_num', k2)
        st   = rd.get('eligible_state_code', '?')
        enr  = rd.get('enrollment_type_code', '?')
        fp = []
        if v.get('consec_flag'): fp.append(f"80×{v['consec']}")
        if v['dom'] >= 1:         fp.append(f"DomEst×{v['dom']}")
        if ov > 0:                fp.append(f"Overlap")
        flagged.append(
            ((0 if risk == 'High' else 1, -v['consec']),
             msis, st, enr, risk, ', '.join(fp) or 'Pattern', v['consec'], ov)
        )
    flagged.sort(key=lambda x: x[0])
    flagged_html = ''
    for row in flagged[:12]:
        _, msis, st, enr, risk, fp, consec, ov = row
        cls = 'high' if risk == 'High' else 'medium'
        ov_str = f'{ov}%' if ov else '—'
        flagged_html += (
            f'<tr>'
            f'<td style="font-family:monospace;font-size:12px">{msis}</td>'
            f'<td>{st}</td>'
            f'<td style="font-size:11px;color:#6b7194">{enr[:8]}</td>'
            f'<td><span class="badge {cls}">{risk}</span></td>'
            f'<td style="font-size:11px">{fp}</td>'
            f'<td style="text-align:center">{consec}</td>'
            f'<td style="text-align:center">{ov_str}</td>'
            f'</tr>\n'
        )

    # ── verification method distribution ──
    veri_cnt = defaultdict(int)
    veri_flag_cnt = defaultdict(lambda: defaultdict(int))
    for r in p_acts:
        vk  = r['verification_method_key']
        vm  = veri_name_map.get(vk, f'Method {vk}')
        veri_cnt[vm] += 1
        flag = 'Overlap' if r['is_overlap_flag'] == 'True' else 'Normal'
        veri_flag_cnt[vm][flag] += 1
    veri_labels  = sorted(veri_cnt.keys())
    veri_normal  = [veri_flag_cnt[v].get('Normal',  0) for v in veri_labels]
    veri_overlap = [veri_flag_cnt[v].get('Overlap', 0) for v in veri_labels]

    # ── Section 6: Expected vs Reported ──
    # Improved denominator: use active recipient-months only
    s_exp = defaultdict(float); s_rep = defaultdict(float)
    for r in p_eng:
        st = recip_state_map.get(r['medicaid_recipient_key'], 'UNK')
        s_exp[st] += 80.0
        s_rep[st] += float(r['monthly_hours_completed'])
    evr_exp = [round(s_exp.get(s, 0)) for s in all_states]
    evr_rep = [round(s_rep.get(s, 0)) for s in all_states]

    m_exp = [sum(80 for r in eng_by_month.get(k, [])
                 if recip_ok(r['medicaid_recipient_key']))
             for k in p_month_keys]
    m_rep = [round(sum(float(r['monthly_hours_completed'])
                       for r in eng_by_month.get(k, [])
                       if recip_ok(r['medicaid_recipient_key'])))
             for k in p_month_keys]

    tot_exp = sum(s_exp.values())
    tot_rep = sum(s_rep.values())
    rep_ratio = round(tot_rep / tot_exp * 100, 1) if tot_exp else 0

    # rolling 3-month compliance (use last 3 available months in period)
    last3_keys = p_month_keys[-3:] if len(p_month_keys) >= 3 else p_month_keys
    r3_comp = sum(1 for k in last3_keys
                  for r in eng_by_month.get(k, [])
                  if float(r['monthly_hours_completed']) >= 80.0
                  and recip_ok(r['medicaid_recipient_key']))
    r3_tot  = sum(len([r for r in eng_by_month.get(k, [])
                       if recip_ok(r['medicaid_recipient_key'])])
                  for k in last3_keys)
    rolling3_compliance = round(r3_comp / r3_tot * 100, 1) if r3_tot else 0

    # ── state × month heatmap ──
    sm_exp = defaultdict(float); sm_rep = defaultdict(float)
    for r in p_eng:
        st = recip_state_map.get(r['medicaid_recipient_key'], 'UNK')
        sm_exp[(st, r['calendar_month_key'])] += 80.0
        sm_rep[(st, r['calendar_month_key'])] += float(r['monthly_hours_completed'])

    heat_head = '<th>State</th>' + ''.join(f'<th>{month_name_map[k]}</th>' for k in p_month_keys) + '<th>6-mo Avg</th>'
    heat_body = ''
    for st in all_states:
        heat_body += f'<tr><td style="font-weight:600">{st}</td>'
        te = tr2 = 0
        for k in p_month_keys:
            e = sm_exp.get((st, k), 0); rp = sm_rep.get((st, k), 0)
            te += e; tr2 += rp
            v = round(rp / e * 100) if e else 0
            bg = _heat_color(v)
            heat_body += f'<td style="background:{bg};text-align:center;font-weight:600;color:{_heat_text(v)}">{v}%</td>'
        tv = round(tr2 / te * 100) if te else 0
        tbg = _heat_color(tv)
        heat_body += f'<td style="background:{tbg};text-align:center;font-weight:700;color:{_heat_text(tv)}">{tv}%</td></tr>\n'

    # ── enrollment program mix (filtered recipients) ──
    enroll_cnt_f = defaultdict(int)
    for r in recipients:
        if state_filter is not None and r['eligible_state_code'] != state_filter:
            continue
        if age_filter is not None and recip_agegroup_map.get(r['medicaid_recipient_key']) != age_filter:
            continue
        enroll_cnt_f[r['enrollment_type_code']] += 1
    enroll_data_period = [enroll_cnt_f.get(k, 0) for k in enroll_labels]

    # ── establishment type mix from activity in period (filtered) ──
    est_cnt_act = defaultdict(int)
    for r in p_acts:
        et = est_type_by_key.get(r['establishment_key'])
        if et:
            est_cnt_act[et] += 1
    est_data_period = [est_cnt_act.get(k, 0) for k in est_type_labels]

    # ── cross-state activity recipients (among those with activity in period) ──
    active_rks = {r['medicaid_recipient_key'] for r in p_acts}
    ms_count = sum(
        1 for rk in active_rks
        if recip_state_map.get(rk) != recip_sub_map.get(rk)
    )

    return {
        # KPIs
        'activity_records':         activity_records,
        'activity_records_fmt':     fmt_num(activity_records),
        'registered':               registered_in_period,
        'registered_fmt':           fmt_num(registered_in_period),
        'active_recips_fmt':        fmt_num(active_recips),
        'active_rate':              active_rate,
        'active_rate_fmt':          f'{active_rate}%',
        'avg_acts_per_recip':       avg_acts_per_recip,
        'avg_acts_fmt':             f'{avg_acts_per_recip}',
        'multistate_fmt':           fmt_num(ms_count),
        'multistate_count':         ms_count,
        'avg_hrs_mo':               avg_hrs_mo,
        'total_hours_fmt':          fmt_M(total_hours_raw),
        'compliance_rate':          compliance_rate,
        'compliance_rate_fmt':      f'{compliance_rate}%',
        'rolling3_compliance_fmt':  f'{rolling3_compliance}%',
        'overlap_count':            overlap_count,
        'overlap_count_fmt':        fmt_num(overlap_count),
        'overlap_pct':              overlap_pct,
        'new_recips_fmt':           fmt_num(new_recipients),
        'returning_fmt':            fmt_num(returning_recipients),
        'avg_days_first_fmt':       f'{avg_days_first:.0f} days',
        # S5 fraud
        'ex80':                     ex80,
        'ex80_fmt':                 fmt_num(ex80),
        'ex80c':                    ex80c,
        'ex80c_fmt':                fmt_num(ex80c),
        'high_risk_fmt':            fmt_num(high_risk),
        'medium_risk_fmt':          fmt_num(medium_risk),
        # S6
        'tot_exp_fmt':              fmt_M(tot_exp),
        'tot_rep_fmt':              fmt_M(tot_rep),
        'rep_ratio':                rep_ratio,
        'rep_ratio_fmt':            f'{rep_ratio}%',
        # chart data
        'month_labels':             p_month_labels,
        'monthly_reg':              monthly_reg,
        'act_labels':               act_labels,
        'act_data':                 act_data,
        'act_hrs_labels':           act_hrs_labels,
        'act_hrs_data':             act_hrs_data,
        'funnel_labels':            funnel_labels,
        'funnel_data':              funnel_data,
        'top15_labels':             top15_labels,
        'top15_data':               top15_data,
        'avg_hrs_per_month':        avg_hrs_per_month,
        'hrs_dist_data':            hrs_dist_data,
        'status_labels':            status_labels,
        'status_data':              status_data,
        'top6':                     top6,
        'top6_wrapped':             top6_wrapped,
        'aa_018':   aa_018,   'aa_1964': aa_1964,   'aa_65p':  aa_65p,
        'age_m_018': age_m_018, 'age_m_1964': age_m_1964, 'age_m_65p': age_m_65p,
        'fr_80hr':  fr_80hr,  'fr_ov': fr_ov,  'fr_dom': fr_dom,
        'evr_exp':  evr_exp,  'evr_rep': evr_rep,
        'm_exp':    m_exp,    'm_rep': m_rep,
        'veri_labels': veri_labels, 'veri_normal': veri_normal, 'veri_overlap': veri_overlap,
        'flagged_html': flagged_html,
        'heat_head': heat_head,
        'heat_body': heat_body,
        'enroll_labels':            list(enroll_labels),
        'enroll_data':              enroll_data_period,
        'est_labels':               list(est_type_labels),
        'est_data':                 est_data_period,
    }


def _heat_color(v):
    """Background color for heatmap cell based on reporting ratio."""
    if v >= 60:  return 'rgba(5,150,105,0.15)'
    if v >= 45:  return 'rgba(202,138,4,0.12)'
    return 'rgba(220,38,38,0.12)'

def _heat_text(v):
    if v >= 60:  return '#065f46'
    if v >= 45:  return '#713f12'
    return '#991b1b'


# ── compute all data ─────────────────────────────────────────────────────────
print("Computing period data (all states)...")
all_data = {}
for pid, pc in period_configs.items():
    all_data[f'{pid}_all'] = compute_period(pc['keys'])
    print(f"  {pid} done")

print("Computing per-state period data...")
for pid, pc in period_configs.items():
    for st in all_states:
        all_data[f'{pid}_{st}'] = compute_period(pc['keys'], state_filter=st)
print("Computing age-segmented period data...")
for pid, pc in period_configs.items():
    for ag in AGE_LABELS:
        all_data[f'{pid}_all_{ag}'] = compute_period(pc['keys'], age_filter=ag)
for pid, pc in period_configs.items():
    for st in all_states:
        for ag in AGE_LABELS:
            all_data[f'{pid}_{st}_{ag}'] = compute_period(pc['keys'], state_filter=st, age_filter=ag)
print("All data computed.")

d0 = all_data['202603_all']

# ── state × enrollment heatmap (static) ──────────────────────────────────────
se_head = '<th>State</th>' + ''.join(f'<th>{e[:8]}</th>' for e in ['Medicaid','Exp. CHIP','Sep. CHIP']) + '<th>Total</th>'
se_body = ''
for st in all_states:
    totst = sum(state_enroll[st].values())
    se_body += f'<tr><td style="font-weight:600">{st}</td>'
    for et in ['Medicaid', 'Medicaid Expansion CHIP', 'Separate CHIP']:
        cnt = state_enroll[st].get(et, 0)
        pct = round(cnt / totst * 100) if totst else 0
        se_body += f'<td style="text-align:center">{cnt:,} <small style="color:#6b7194">({pct}%)</small></td>'
    se_body += f'<td style="text-align:center;font-weight:600">{totst:,}</td></tr>\n'

period_options_html = '\n'.join(
    f'<option value="{pid}"{" selected" if pid == "202603" else ""}>{pc["label"]}</option>'
    for pid, pc in period_configs.items()
)
state_options_html = '<option value="all" selected>All States</option>\n' + '\n'.join(
    f'<option value="{s}">{s}</option>' for s in all_states
)

# ── HTML ─────────────────────────────────────────────────────────────────────
html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Medicaid Community Engagement Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4"></script>
<style>
:root{{
  --bg:#f4f5fb;--surface:#fff;--card:#fff;--border:#e2e5f0;
  --text:#1e2033;--muted:#6b7194;--accent:#4f46e5;--accent2:#0891b2;
  --green:#059669;--red:#dc2626;--orange:#ea580c;--yellow:#ca8a04;
  --pink:#db2777;--purple:#7c3aed;--fraud-bg:#fff5f5;--fraud-border:#fca5a5;
}}
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{font-family:'Segoe UI',-apple-system,BlinkMacSystemFont,sans-serif;background:var(--bg);color:var(--text);min-height:100vh;}}
.topbar{{background:var(--surface);border-bottom:2px solid var(--border);padding:10px 24px;display:flex;align-items:center;gap:10px;position:sticky;top:0;z-index:100;box-shadow:0 2px 8px rgba(0,0,0,0.06);flex-wrap:wrap;}}
.topbar h1{{font-size:14px;font-weight:700;letter-spacing:0.2px;white-space:nowrap;flex:1;min-width:200px;}}
.topbar-filters{{display:flex;gap:8px;align-items:center;flex-wrap:wrap;}}
.filter-label{{font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:0.6px;}}
.filter-select{{background:var(--bg);border:1.5px solid var(--border);color:var(--text);padding:5px 10px;border-radius:6px;font-size:13px;cursor:pointer;font-weight:600;transition:border-color 0.15s;}}
.filter-select:focus{{outline:none;border-color:var(--accent);}}
.age-toggle{{display:flex;gap:4px;align-items:center;}}
.age-btn{{padding:4px 9px;border-radius:5px;font-size:11px;font-weight:700;border:1.5px solid var(--border);background:var(--surface);cursor:pointer;transition:all 0.15s;}}
.age-btn.active{{color:#fff;border-color:transparent;}}
.age-btn[data-age="0-18"].active{{background:#0891b2;}}
.age-btn[data-age="19-64"].active{{background:#4f46e5;}}
.age-btn[data-age="65+"].active{{background:#ea580c;}}
.container{{padding:16px 24px;max-width:1600px;margin:0 auto;}}
.section-title{{font-size:11px;font-weight:700;color:var(--accent);text-transform:uppercase;letter-spacing:1.4px;margin:22px 0 10px;padding-bottom:6px;border-bottom:2px solid var(--border);display:flex;align-items:center;gap:8px;}}
.section-title:first-child{{margin-top:0;}}
.section-icon{{font-size:14px;}}
.kpi-row{{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:10px;margin-bottom:10px;}}
.kpi-card{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:13px 15px;position:relative;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,0.05);transition:box-shadow 0.15s;}}
.kpi-card:hover{{box-shadow:0 3px 10px rgba(0,0,0,0.10);}}
.kpi-card::before{{content:'';position:absolute;top:0;left:0;right:0;height:3px;}}
.kpi-card.purple::before{{background:var(--accent);}}
.kpi-card.cyan::before{{background:var(--accent2);}}
.kpi-card.green::before{{background:var(--green);}}
.kpi-card.orange::before{{background:var(--orange);}}
.kpi-card.pink::before{{background:var(--pink);}}
.kpi-card.red::before{{background:var(--red);}}
.kpi-card.yellow::before{{background:var(--yellow);}}
.kpi-label{{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:0.7px;margin-bottom:5px;line-height:1.3;font-weight:600;}}
.kpi-value{{font-size:24px;font-weight:800;line-height:1;letter-spacing:-0.5px;}}
.kpi-sub{{font-size:10px;color:var(--muted);margin-top:4px;}}
.kpi-trend{{font-size:10px;margin-top:3px;}}
.kpi-trend.up{{color:var(--green);}}
.kpi-trend.down{{color:var(--red);}}
.grid-2{{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px;}}
.grid-3{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-bottom:10px;}}
.grid-4{{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:10px;}}
.grid-2-1{{display:grid;grid-template-columns:2fr 1fr;gap:10px;margin-bottom:10px;}}
.grid-1-2{{display:grid;grid-template-columns:1fr 2fr;gap:10px;margin-bottom:10px;}}
.chart-card{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px 16px;box-shadow:0 1px 4px rgba(0,0,0,0.05);}}
.chart-card h3{{font-size:13px;font-weight:700;margin-bottom:2px;}}
.chart-card .chart-sub{{font-size:10px;color:var(--muted);margin-bottom:8px;line-height:1.4;}}
.chart-wrap{{position:relative;width:100%;}}
.chart-wrap canvas{{width:100% !important;}}
.fraud-section .section-title{{color:var(--red);border-bottom-color:var(--fraud-border);}}
.fraud-section .kpi-card{{background:var(--fraud-bg);border-color:var(--fraud-border);}}
.fraud-section .chart-card{{background:var(--fraud-bg);border-color:var(--fraud-border);}}
table.data-table{{width:100%;border-collapse:collapse;font-size:12px;}}
table.data-table th{{text-align:left;color:var(--muted);font-weight:700;padding:7px 8px;border-bottom:2px solid var(--border);font-size:10px;text-transform:uppercase;letter-spacing:0.5px;}}
table.data-table td{{padding:7px 8px;border-bottom:1px solid var(--border);}}
table.data-table tr:last-child td{{border-bottom:none;}}
table.data-table tr:hover td{{background:rgba(79,70,229,0.04);}}
.badge{{display:inline-block;padding:2px 8px;border-radius:4px;font-size:10px;font-weight:700;letter-spacing:0.3px;}}
.badge.high{{background:rgba(220,38,38,0.12);color:#991b1b;border:1px solid rgba(220,38,38,0.2);}}
.badge.medium{{background:rgba(234,88,12,0.12);color:#9a3412;border:1px solid rgba(234,88,12,0.2);}}
.badge.low{{background:rgba(5,150,105,0.12);color:#065f46;border:1px solid rgba(5,150,105,0.2);}}
.snapshot-note{{font-size:10px;color:var(--muted);font-style:italic;margin-top:4px;}}
@media(max-width:1200px){{.grid-2,.grid-3,.grid-4,.grid-2-1,.grid-1-2{{grid-template-columns:1fr 1fr;}}}}
@media(max-width:800px){{.grid-2,.grid-3,.grid-4,.grid-2-1,.grid-1-2{{grid-template-columns:1fr;}}}}
</style>
</head>
<body>

<div class="topbar">
  <h1>Medicaid Community Engagement Dashboard</h1>
  <div class="topbar-filters">
    <span class="filter-label">Period</span>
    <select id="periodSelector" class="filter-select" onchange="updateFilters()">
      {period_options_html}
    </select>
    <span class="filter-label">State</span>
    <select id="stateSelector" class="filter-select" onchange="updateFilters()">
      {state_options_html}
    </select>
    <span class="filter-label">Age</span>
    <div class="age-toggle">
      <button class="age-btn active" data-age="0-18"  onclick="toggleAge(this)">0-18</button>
      <button class="age-btn active" data-age="19-64" onclick="toggleAge(this)">19-64</button>
      <button class="age-btn active" data-age="65+"   onclick="toggleAge(this)">65+</button>
    </div>
  </div>
</div>

<div class="container">

<!-- ═══ SECTION 1: Recipient & Enrollment ═══ -->
<div class="section-title"><span class="section-icon">👥</span>1 — Recipient & Enrollment Overview</div>
<div class="kpi-row">
  <div class="kpi-card purple">
    <div class="kpi-label">Total Enrolled Recipients</div>
    <div class="kpi-value">{fmt_num(total_recipients)}</div>
    <div class="kpi-sub">Across all 6 months</div>
  </div>
  <div class="kpi-card cyan">
    <div class="kpi-label">Activity Records (Period)</div>
    <div class="kpi-value" id="kpi-records">{d0['activity_records_fmt']}</div>
  </div>
  <div class="kpi-card green">
    <div class="kpi-label">New Registrations</div>
    <div class="kpi-value" id="kpi-registered">{d0['registered_fmt']}</div>
    <div class="kpi-sub" id="kpi-registered-sub">March 2026</div>
  </div>
  <div class="kpi-card orange">
    <div class="kpi-label">Avg Activities / Recipient</div>
    <div class="kpi-value" id="kpi-avg-acts">{d0['avg_acts_fmt']}</div>
    <div class="kpi-sub">In selected period</div>
  </div>
  <div class="kpi-card pink">
    <div class="kpi-label">New vs Returning</div>
    <div class="kpi-value" id="kpi-new-ret">{d0['new_recips_fmt']} / {d0['returning_fmt']}</div>
    <div class="kpi-sub">New / Returning this period</div>
  </div>
  <div class="kpi-card yellow">
    <div class="kpi-label">Cross-State Recipients</div>
    <div class="kpi-value">{d0['multistate_fmt']}</div>
    <div class="kpi-sub">Submitting ≠ Eligible state</div>
  </div>
</div>

<div class="grid-3">
  <div class="chart-card">
    <h3>Enrollment Program Mix</h3>
    <div class="chart-sub">Traditional Medicaid dominates at ~72% — typical of high-complexity state programs</div>
    <div class="chart-wrap"><canvas id="chartEnrollment"></canvas></div>
    <div class="snapshot-note">Enrollment snapshot — does not change with period</div>
  </div>
  <div class="chart-card">
    <h3>Monthly Registrations — Declining Enrollment Curve</h3>
    <div class="chart-sub">Front-loaded registration wave; later months show tapering new sign-ups</div>
    <div class="chart-wrap"><canvas id="chartMonthlyReg"></canvas></div>
  </div>
  <div class="chart-card">
    <h3>Recipient Status Breakdown</h3>
    <div class="chart-sub">Active / Inactive / New this period</div>
    <div class="chart-wrap"><canvas id="chartStatus"></canvas></div>
  </div>
</div>

<!-- ═══ SECTION 2: Activity & Establishment ═══ -->
<div class="section-title"><span class="section-icon">🏥</span>2 — Activity & Establishment Insights</div>
<div class="grid-3">
  <div class="chart-card">
    <h3>Age Distribution</h3>
    <div class="chart-sub">{age_pcts[0]}% under 19 — higher child share reflects broader Medicaid eligibility</div>
    <div class="chart-wrap"><canvas id="chartAge"></canvas></div>
    <div class="snapshot-note">Age snapshot — as of enrollment date</div>
  </div>
  <div class="chart-card">
    <h3>Provider Mix — Clinic & Pharmacy Dominant</h3>
    <div class="chart-sub">Community-based outpatient sites drive ~60% of engagement</div>
    <div class="chart-wrap"><canvas id="chartEstType"></canvas></div>
    <div class="snapshot-note">500 establishments — distribution snapshot</div>
  </div>
  <div class="chart-card">
    <h3>Recipients per State — Larger States Lead</h3>
    <div class="chart-sub">CA and TX account for ~39% of total enrollment</div>
    <div class="chart-wrap"><canvas id="chartState"></canvas></div>
    <div class="snapshot-note">Enrollment snapshot</div>
  </div>
</div>

<div class="grid-2">
  <div class="chart-card">
    <h3>Top 15 Providers by Distinct Recipients</h3>
    <div class="chart-sub">Establishments with highest reach across the enrolled population</div>
    <div class="chart-wrap"><canvas id="chartRecipEst"></canvas></div>
  </div>
  <div class="chart-card">
    <h3>80-Hour Compliance Funnel</h3>
    <div class="chart-sub">How close are recipients to the 80-hr monthly target?</div>
    <div class="chart-wrap"><canvas id="chartFunnel"></canvas></div>
  </div>
</div>

<!-- ═══ SECTION 3: Hours & Engagement ═══ -->
<div class="section-title"><span class="section-icon">⏱</span>3 — Hours & Engagement Metrics</div>
<div class="kpi-row">
  <div class="kpi-card cyan">
    <div class="kpi-label">Avg Hrs / Recipient / Month</div>
    <div class="kpi-value" id="kpi-avghrs">{d0['avg_hrs_mo']}</div>
    <div class="kpi-sub">Target: 80 hrs/month</div>
  </div>
  <div class="kpi-card green">
    <div class="kpi-label">% Meeting 80-hr Target</div>
    <div class="kpi-value" id="kpi-compliance">{d0['compliance_rate_fmt']}</div>
    <div class="kpi-sub" id="kpi-compliance-sub">Recipient-months ≥ 80 hrs</div>
  </div>
  <div class="kpi-card purple">
    <div class="kpi-label">Rolling 3-Mo Compliance</div>
    <div class="kpi-value" id="kpi-rolling3">{d0['rolling3_compliance_fmt']}</div>
    <div class="kpi-sub">Last 3 months in view</div>
  </div>
  <div class="kpi-card orange">
    <div class="kpi-label">Total Hours (Period)</div>
    <div class="kpi-value" id="kpi-totalhrs">{d0['total_hours_fmt']}</div>
  </div>
  <div class="kpi-card pink">
    <div class="kpi-label">Recipients Below 80-hr Target</div>
    <div class="kpi-value" id="kpi-below-target">{round(100-d0['compliance_rate'],1)}%</div>
    <div class="kpi-sub" id="kpi-below-sub">% not meeting monthly goal</div>
  </div>
</div>

<div class="grid-2-1">
  <div class="chart-card">
    <h3>Engagement Ramp-Up — Monthly Avg Hours vs 80-hr Target</h3>
    <div class="chart-sub">Avg hours climb from ~27 in Oct toward ~44 by Mar, with age-group variation</div>
    <div class="chart-wrap"><canvas id="chartAgeMonthly"></canvas></div>
  </div>
  <div class="chart-card">
    <h3>Hours Distribution by Bracket</h3>
    <div class="chart-sub">Recipient-months by hours bracket</div>
    <div class="chart-wrap"><canvas id="chartHoursDist"></canvas></div>
  </div>
</div>

<!-- ═══ SECTION 4: Age & Activity Breakdown ═══ -->
<div class="section-title"><span class="section-icon">📊</span>4 — Age & Activity Breakdown</div>
<div class="grid-2">
  <div class="chart-card">
    <h3>Activity Volume by Age Group — Older Adults in Residential Care</h3>
    <div class="chart-sub">Working-age adults (19-64) dominate most activity types; 65+ concentrated in LT care</div>
    <div class="chart-wrap"><canvas id="chartAgeActivity"></canvas></div>
  </div>
  <div class="chart-card">
    <h3>Hours Logged by Activity Type (Thousands)</h3>
    <div class="chart-sub">HCBS and outpatient visits generate the most total engagement hours</div>
    <div class="chart-wrap"><canvas id="chartHoursPerActivity"></canvas></div>
  </div>
</div>

<!-- ═══ SECTION 5: Fraud & Suspicious Patterns ═══ -->
<div class="fraud-section">
<div class="section-title"><span class="section-icon">🚨</span>5 — Fraud & Suspicious Pattern Detection</div>
<div class="kpi-row">
  <div class="kpi-card red">
    <div class="kpi-label">High Risk Recipients</div>
    <div class="kpi-value" id="kpi-highrisk">{d0['high_risk_fmt']}</div>
    <div class="kpi-sub">Consecutive exact 80-hr pattern</div>
  </div>
  <div class="kpi-card orange">
    <div class="kpi-label">Medium Risk</div>
    <div class="kpi-value" id="kpi-medrisk">{d0['medium_risk_fmt']}</div>
    <div class="kpi-sub">Single dominant provider pattern</div>
  </div>
  <div class="kpi-card yellow">
    <div class="kpi-label">Exact 80-hr Flags</div>
    <div class="kpi-value" id="kpi-ex80">{d0['ex80_fmt']}</div>
    <div class="kpi-sub" id="kpi-ex80c-sub">Consec flag: {d0['ex80c_fmt']} months</div>
  </div>
  <div class="kpi-card red">
    <div class="kpi-label">Concurrent Billing Flags</div>
    <div class="kpi-value" id="kpi-overlap">{d0['overlap_count_fmt']}</div>
    <div class="kpi-sub" id="kpi-overlap-sub">{d0['overlap_pct']}% of activity records</div>
  </div>
</div>

<div class="grid-2">
  <div class="chart-card">
    <h3>Suspicious Pattern Trend — Monthly Flag Volume</h3>
    <div class="chart-sub">Tracking three independent fraud signals across the 6-month period</div>
    <div class="chart-wrap"><canvas id="chartFraudTrend"></canvas></div>
  </div>
  <div class="chart-card">
    <h3>Concurrent Billing by Verification Method</h3>
    <div class="chart-sub">Manual verification correlates with higher overlap flag rates</div>
    <div class="chart-wrap"><canvas id="chartVeriMethod"></canvas></div>
  </div>
</div>

<div class="chart-card" style="margin-bottom:10px">
  <h3>Flagged Recipients — Risk Summary</h3>
  <div class="chart-sub">Top flagged recipients by risk tier and pattern indicators</div>
  <div style="max-height:320px;overflow-y:auto">
    <table class="data-table" id="fraudTable">
      <thead><tr>
        <th>Recipient ID</th><th>State</th><th>Program</th>
        <th>Risk</th><th>Pattern Flags</th><th>Consec. Months</th><th>Overlap %</th>
      </tr></thead>
      <tbody id="fraudTableBody">{d0['flagged_html']}</tbody>
    </table>
  </div>
</div>
</div><!-- end fraud-section -->

<!-- ═══ SECTION 6: Expected vs Reported ═══ -->
<div class="section-title"><span class="section-icon">📈</span>6 — Expected vs Reported Hours</div>
<div class="kpi-row" style="grid-template-columns:repeat(3,1fr);">
  <div class="kpi-card green">
    <div class="kpi-label">Expected Hours (Active Enrollees)</div>
    <div class="kpi-value" id="kpi-totexp">{d0['tot_exp_fmt']}</div>
    <div class="kpi-sub">80 hrs × active recipient-months</div>
  </div>
  <div class="kpi-card cyan">
    <div class="kpi-label">Total Reported Hours</div>
    <div class="kpi-value" id="kpi-totrep">{d0['tot_rep_fmt']}</div>
  </div>
  <div class="kpi-card orange">
    <div class="kpi-label">Overall Reporting Ratio</div>
    <div class="kpi-value" id="kpi-repratio">{d0['rep_ratio_fmt']}</div>
    <div class="kpi-sub">Reported / Expected</div>
  </div>
</div>

<div class="grid-2">
  <div class="chart-card">
    <h3>State Comparison — High vs Low Reporting States</h3>
    <div class="chart-sub">CA and NY exceed 55%; OH and TX lag below 40%</div>
    <div class="chart-wrap"><canvas id="chartExpVsRepState"></canvas></div>
  </div>
  <div class="chart-card">
    <h3>Reporting Trajectory — Hours Gap Narrowing Over Time</h3>
    <div class="chart-sub">Reported hours climb each month as recipients become more active</div>
    <div class="chart-wrap"><canvas id="chartExpVsRepMonth"></canvas></div>
  </div>
</div>

<div class="chart-card" style="margin-bottom:8px">
  <h3>State × Month Reporting Ratio Heatmap</h3>
  <div class="chart-sub">
    <span style="display:inline-block;width:12px;height:12px;background:rgba(5,150,105,0.15);border-radius:2px;margin-right:4px"></span>≥60%&nbsp;&nbsp;
    <span style="display:inline-block;width:12px;height:12px;background:rgba(202,138,4,0.12);border-radius:2px;margin-right:4px"></span>40-59%&nbsp;&nbsp;
    <span style="display:inline-block;width:12px;height:12px;background:rgba(220,38,38,0.12);border-radius:2px;margin-right:4px"></span>&lt;40%
  </div>
  <div style="overflow-x:auto">
    <table class="data-table" id="heatmapTable">
      <thead id="heatmapHead"><tr id="heatmapHeadRow">{d0['heat_head']}</tr></thead>
      <tbody id="heatmapBody">{d0['heat_body']}</tbody>
    </table>
  </div>
</div>

<!-- ═══ SECTION 7: Enrollment Deep Dive ═══ -->
<div class="section-title"><span class="section-icon">📋</span>7 — Enrollment Program by State</div>
<div class="chart-card" style="margin-bottom:8px">
  <h3>Medicaid vs CHIP Distribution by State</h3>
  <div class="chart-sub">Traditional Medicaid dominant across all states; CHIP share varies by state expansion decisions</div>
  <div style="overflow-x:auto">
    <table class="data-table">
      <thead><tr>{se_head}</tr></thead>
      <tbody>{se_body}</tbody>
    </table>
  </div>
  <div class="snapshot-note" style="margin-top:6px">Enrollment snapshot — does not change with period filter</div>
</div>

</div><!-- .container -->

<script>
Chart.defaults.color = '#6b7194';
Chart.defaults.borderColor = 'rgba(0,0,0,0.07)';
Chart.defaults.font.family = "-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif";
Chart.defaults.font.size = 11;

const G = 'rgba(0,0,0,0.07)';
const COLORS = ["#4f46e5","#0891b2","#059669","#ea580c","#db2777","#7c3aed","#ca8a04","#dc2626","#6366f1","#0d9488","#c026d3"];
const STATES  = {js(all_states)};
const STATE_COLORS = {js([STATE_COLORS[s] for s in all_states])};
const AGE_COLORS   = ['#0891b2','#4f46e5','#ea580c'];

// ── Embedded data ──────────────────────────────────────────────────────────
const ALL_DATA = {js(all_data)};

// ── Constant dimension data ────────────────────────────────────────────────
const CONST = {{
  enroll_labels: {js(enroll_labels)},
  enroll_data:   {js(enroll_data)},
  est_labels:    {js(est_type_labels)},
  est_data:      {js(est_type_data)},
  age_labels:    {js(AGE_LABELS)},
  age_data:      {js(age_data)},
  state_data:    {js([sum(1 for r in recipients if r['eligible_state_code']==s) for s in all_states])},
}};

// ── Age toggle state ───────────────────────────────────────────────────────
let activeAges = new Set(['0-18','19-64','65+']);

function toggleAge(btn) {{
  const age = btn.dataset.age;
  if (activeAges.has(age)) {{
    if (activeAges.size <= 1) return;  // keep at least one
    activeAges.delete(age);
    btn.classList.remove('active');
  }} else {{
    activeAges.add(age);
    btn.classList.add('active');
  }}
  refreshAgeCharts();
}}

function refreshAgeCharts() {{
  const d = currentData();
  // Age × Activity
  cAgeAct.data.datasets[0].hidden = !activeAges.has('0-18');
  cAgeAct.data.datasets[1].hidden = !activeAges.has('19-64');
  cAgeAct.data.datasets[2].hidden = !activeAges.has('65+');
  cAgeAct.update();
  // Age Monthly
  cAgeMonthly.data.datasets[0].hidden = !activeAges.has('0-18');
  cAgeMonthly.data.datasets[1].hidden = !activeAges.has('19-64');
  cAgeMonthly.data.datasets[2].hidden = !activeAges.has('65+');
  cAgeMonthly.update();
}}

function currentData() {{
  const pid = document.getElementById('periodSelector').value;
  const st  = document.getElementById('stateSelector').value;
  return ALL_DATA[pid + '_' + st];
}}

// ── Pie factory ────────────────────────────────────────────────────────────
function makePie(id, labels, data, colors, aspectRatio) {{
  return new Chart(document.getElementById(id), {{
    type: 'doughnut',
    data: {{ labels, datasets: [{{ data, backgroundColor: colors || COLORS,
              borderWidth: 2, borderColor: '#fff', hoverOffset: 6 }}] }},
    options: {{
      cutout: '60%', responsive: true, maintainAspectRatio: true,
      aspectRatio: aspectRatio || 2,
      plugins: {{ legend: {{
        position: 'right', align: 'center',
        labels: {{ usePointStyle: false, boxWidth: 12, boxHeight: 12, padding: 10, font: {{ size: 10 }} }}
      }} }}
    }}
  }});
}}

// ── Build initial charts ───────────────────────────────────────────────────
const d0 = ALL_DATA['202603_all'];

// S1 — Enrollment donut
makePie('chartEnrollment', CONST.enroll_labels, CONST.enroll_data,
  ['#4f46e5','#0891b2','#059669'], 2.0);

// S1 — Monthly registrations
const cMonthReg = new Chart(document.getElementById('chartMonthlyReg'), {{
  type: 'bar',
  data: {{ labels: d0.month_labels, datasets: [{{
    label: 'New Registrations', data: d0.monthly_reg,
    backgroundColor: '#4f46e5', borderRadius: 6, barPercentage: 0.6
  }}] }},
  options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ beginAtZero: true, grid: {{ color: G }} }}, x: {{ grid: {{ display: false }} }} }} }}
}});

// S1 — Recipient status donut
const cStatus = makePie('chartStatus', d0.status_labels, d0.status_data,
  ['#4f46e5','#e2e5f0','#059669'], 2.0);

// S2 — Age donut
makePie('chartAge', CONST.age_labels, CONST.age_data, AGE_COLORS, 2.0);

// S2 — Establishment type donut
makePie('chartEstType', CONST.est_labels, CONST.est_data,
  ['#4f46e5','#0891b2','#059669','#ea580c','#db2777','#7c3aed'], 2.0);

// S2 — State bar
const cState = new Chart(document.getElementById('chartState'), {{
  type: 'bar',
  data: {{ labels: STATES, datasets: [{{ data: CONST.state_data,
    backgroundColor: STATE_COLORS, borderRadius: 5, barPercentage: 0.6 }}] }},
  options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ beginAtZero: true, grid: {{ color: G }}, ticks: {{ callback: v => v>=1000?(v/1000).toFixed(0)+'K':v }} }},
      x: {{ grid: {{ display: false }} }} }} }}
}});

// S2 — Top 15 establishments
const cRecipEst = new Chart(document.getElementById('chartRecipEst'), {{
  type: 'bar',
  data: {{ labels: d0.top15_labels, datasets: [{{
    data: d0.top15_data, backgroundColor: '#0891b2', borderRadius: 3, barPercentage: 0.65
  }}] }},
  options: {{ indexAxis: 'y', responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ x: {{ beginAtZero: true, grid: {{ color: G }} }},
      y: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 10 }} }} }} }} }}
}});

// S2 — Compliance funnel
const cFunnel = new Chart(document.getElementById('chartFunnel'), {{
  type: 'bar',
  data: {{ labels: d0.funnel_labels, datasets: [{{
    data: d0.funnel_data,
    backgroundColor: ['#dc2626','#ea580c','#ca8a04','#0891b2','#059669','#4f46e5'],
    borderRadius: 5, barPercentage: 0.65
  }}] }},
  options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ beginAtZero: true, grid: {{ color: G }},
      ticks: {{ callback: v => v>=1000?(v/1000).toFixed(0)+'K':v }} }},
      x: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 10 }} }} }} }} }}
}});

// S3 — Monthly hours by age group
const cAgeMonthly = new Chart(document.getElementById('chartAgeMonthly'), {{
  type: 'line',
  data: {{ labels: d0.month_labels, datasets: [
    {{ label:'0-18',     data: d0.age_m_018,  borderColor:'#0891b2', tension:0.35, pointRadius:4 }},
    {{ label:'19-64',    data: d0.age_m_1964, borderColor:'#4f46e5', tension:0.35, pointRadius:4 }},
    {{ label:'65+',      data: d0.age_m_65p,  borderColor:'#ea580c', tension:0.35, pointRadius:4 }},
    {{ label:'Target (80 hrs)', data: d0.month_labels.map(()=>80),
       borderColor:'#dc2626', borderDash:[6,4], pointRadius:0, borderWidth:2 }}
  ]}},
  options: {{ responsive: true, plugins: {{ legend: {{ position:'bottom' }} }},
    scales: {{ y: {{ beginAtZero:false, grid:{{ color:G }} }}, x: {{ grid:{{ display:false }} }} }} }}
}});

// S3 — Hours distribution histogram
const cHoursDist = new Chart(document.getElementById('chartHoursDist'), {{
  type: 'bar',
  data: {{ labels: ['0-20','20-40','40-60','60-80','=80','80-120','120+'],
    datasets: [{{ data: d0.hrs_dist_data,
      backgroundColor:['#dc2626','#ea580c','#ca8a04','#0891b2','#4f46e5','#059669','#7c3aed'],
      borderRadius: 4, barPercentage: 0.7 }}] }},
  options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ beginAtZero: true, grid:{{ color:G }},
      ticks:{{ callback: v => v>=1000?(v/1000).toFixed(0)+'K':v }} }},
      x: {{ grid:{{ display:false }} }} }} }}
}});

// S4 — Age × Activity stacked bar
const cAgeAct = new Chart(document.getElementById('chartAgeActivity'), {{
  type: 'bar',
  data: {{ labels: d0.top6_wrapped, datasets: [
    {{ label:'0-18',  data: d0.aa_018,  backgroundColor:'#0891b2', borderRadius:3 }},
    {{ label:'19-64', data: d0.aa_1964, backgroundColor:'#4f46e5', borderRadius:3 }},
    {{ label:'65+',   data: d0.aa_65p,  backgroundColor:'#ea580c', borderRadius:3 }}
  ]}},
  options: {{ responsive: true, plugins: {{ legend: {{ position:'bottom' }} }},
    scales: {{
      x: {{ stacked:true, grid:{{ display:false }},
        ticks:{{ maxRotation:0, minRotation:0, font:{{ size:10 }}, autoSkip:false }} }},
      y: {{ stacked:true, beginAtZero:true, grid:{{ color:G }},
        ticks:{{ callback: v => v>=1000?(v/1000).toFixed(0)+'K':v }} }}
    }} }}
}});

// S4 — Hours per activity type
const cHoursPerAct = new Chart(document.getElementById('chartHoursPerActivity'), {{
  type: 'bar',
  data: {{ labels: d0.act_hrs_labels, datasets: [{{
    data: d0.act_hrs_data, backgroundColor: COLORS, borderRadius: 4, barPercentage: 0.65
  }}] }},
  options: {{ indexAxis: 'y', responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ x: {{ beginAtZero: true, grid:{{ color:G }},
      ticks:{{ callback: v => v+'K hrs' }} }},
      y: {{ grid:{{ display:false }}, ticks:{{ font:{{ size:10 }} }} }} }} }}
}});

// S5 — Fraud trend
const cFraud = new Chart(document.getElementById('chartFraudTrend'), {{
  type: 'line',
  data: {{ labels: d0.month_labels, datasets: [
    {{ label:'Exact 80-hr Flags',        data: d0.fr_80hr,
       borderColor:'#dc2626', backgroundColor:'rgba(220,38,38,0.06)', fill:true, tension:0.35, pointRadius:4 }},
    {{ label:'Concurrent Billing Flags', data: d0.fr_ov,
       borderColor:'#ea580c', tension:0.35, pointRadius:4 }},
    {{ label:'Dominant Provider Flags',  data: d0.fr_dom,
       borderColor:'#ca8a04', tension:0.35, pointRadius:4 }}
  ]}},
  options: {{ responsive: true, plugins: {{ legend: {{ position:'bottom' }} }},
    scales: {{ y: {{ beginAtZero:true, grid:{{ color:G }},
      ticks:{{ callback: v => v>=1000?(v/1000).toFixed(1)+'K':v }} }},
      x: {{ grid:{{ display:false }} }} }} }}
}});

// S5 — Verification method × flag
const cVeri = new Chart(document.getElementById('chartVeriMethod'), {{
  type: 'bar',
  data: {{ labels: d0.veri_labels, datasets: [
    {{ label:'Normal',  data: d0.veri_normal,  backgroundColor:'#4f46e5', borderRadius:4 }},
    {{ label:'Overlap', data: d0.veri_overlap, backgroundColor:'#ea580c', borderRadius:4 }}
  ]}},
  options: {{ responsive: true, plugins: {{ legend: {{ position:'bottom' }} }},
    scales: {{ x: {{ stacked:false, grid:{{ display:false }} }},
      y: {{ beginAtZero:true, grid:{{ color:G }},
        ticks:{{ callback: v => v>=1000?(v/1000).toFixed(0)+'K':v }} }} }} }}
}});

// S6 — Expected vs Reported by state
const cEVRState = new Chart(document.getElementById('chartExpVsRepState'), {{
  type: 'bar',
  data: {{ labels: STATES, datasets: [
    {{ label:'Expected', data: d0.evr_exp,
       backgroundColor:'rgba(79,70,229,0.12)', borderColor:'#4f46e5', borderWidth:1.5,
       borderRadius:4, barPercentage:0.65 }},
    {{ label:'Reported', data: d0.evr_rep,
       backgroundColor:'#0891b2', borderRadius:4, barPercentage:0.65 }}
  ]}},
  options: {{ responsive: true, plugins: {{ legend: {{ position:'bottom' }} }},
    scales: {{ y: {{ beginAtZero:true, grid:{{ color:G }},
      ticks:{{ callback: v => (v/1000).toFixed(0)+'K' }} }},
      x: {{ grid:{{ display:false }} }} }} }}
}});

// S6 — Expected vs Reported monthly
const cEVRMonth = new Chart(document.getElementById('chartExpVsRepMonth'), {{
  type: 'line',
  data: {{ labels: d0.month_labels, datasets: [
    {{ label:'Expected', data: d0.m_exp,
       borderColor:'#4f46e5', borderDash:[6,4], tension:0.3, pointRadius:5 }},
    {{ label:'Reported', data: d0.m_rep,
       borderColor:'#0891b2', backgroundColor:'rgba(8,145,178,0.08)', fill:true, tension:0.3, pointRadius:5 }}
  ]}},
  options: {{ responsive: true, plugins: {{ legend: {{ position:'bottom' }} }},
    scales: {{ y: {{ beginAtZero:false, grid:{{ color:G }},
      ticks:{{ callback: v => (v/1000).toFixed(0)+'K' }} }},
      x: {{ grid:{{ display:false }} }} }} }}
}});

// ── updateFilters ──────────────────────────────────────────────────────────
function updateFilters() {{
  const pid = document.getElementById('periodSelector').value;
  const st  = document.getElementById('stateSelector').value;
  const key = pid + '_' + st;
  const d   = ALL_DATA[key];
  if (!d) return;

  const lbl = document.querySelector('#periodSelector option[value="'+pid+'"]').textContent;

  // KPIs
  document.getElementById('kpi-records').textContent         = d.activity_records_fmt;
  document.getElementById('kpi-registered').textContent      = d.registered_fmt;
  document.getElementById('kpi-registered-sub').textContent  = lbl;
  document.getElementById('kpi-avg-acts').textContent        = d.avg_acts_fmt;
  document.getElementById('kpi-new-ret').textContent         = d.new_recips_fmt + ' / ' + d.returning_fmt;
  document.getElementById('kpi-avghrs').textContent          = d.avg_hrs_mo;
  document.getElementById('kpi-compliance').textContent      = d.compliance_rate_fmt;
  document.getElementById('kpi-rolling3').textContent        = d.rolling3_compliance_fmt;
  document.getElementById('kpi-totalhrs').textContent        = d.total_hours_fmt;
  const belowPct = (100 - parseFloat(d.compliance_rate)).toFixed(1) + '%';
  document.getElementById('kpi-below-target').textContent    = belowPct;
  document.getElementById('kpi-below-sub').textContent       = 'Not meeting 80-hr goal';
  document.getElementById('kpi-highrisk').textContent        = d.high_risk_fmt;
  document.getElementById('kpi-medrisk').textContent         = d.medium_risk_fmt;
  document.getElementById('kpi-ex80').textContent            = d.ex80_fmt;
  document.getElementById('kpi-ex80c-sub').textContent       = 'Consec flag: ' + d.ex80c_fmt + ' months';
  document.getElementById('kpi-overlap').textContent         = d.overlap_count_fmt;
  document.getElementById('kpi-overlap-sub').textContent     = d.overlap_pct + '% of activity records';
  document.getElementById('kpi-totexp').textContent          = d.tot_exp_fmt;
  document.getElementById('kpi-totrep').textContent          = d.tot_rep_fmt;
  document.getElementById('kpi-repratio').textContent        = d.rep_ratio_fmt;

  // S1 — Monthly Reg
  cMonthReg.data.labels = d.month_labels;
  cMonthReg.data.datasets[0].data = d.monthly_reg;
  cMonthReg.update();

  // S1 — Status donut
  cStatus.data.datasets[0].data = d.status_data;
  cStatus.update();

  // S2 — Top 15
  cRecipEst.data.labels = d.top15_labels;
  cRecipEst.data.datasets[0].data = d.top15_data;
  cRecipEst.update();

  // S2 — Compliance funnel
  cFunnel.data.labels = d.funnel_labels;
  cFunnel.data.datasets[0].data = d.funnel_data;
  cFunnel.update();

  // S3 — Monthly hours by age
  cAgeMonthly.data.labels = d.month_labels;
  cAgeMonthly.data.datasets[0].data = d.age_m_018;
  cAgeMonthly.data.datasets[1].data = d.age_m_1964;
  cAgeMonthly.data.datasets[2].data = d.age_m_65p;
  cAgeMonthly.data.datasets[3].data = d.month_labels.map(()=>80);
  cAgeMonthly.update();
  refreshAgeCharts();

  // S3 — Hours distribution
  cHoursDist.data.datasets[0].data = d.hrs_dist_data;
  cHoursDist.update();

  // S4 — Age × Activity
  cAgeAct.data.labels = d.top6_wrapped;
  cAgeAct.data.datasets[0].data = d.aa_018;
  cAgeAct.data.datasets[1].data = d.aa_1964;
  cAgeAct.data.datasets[2].data = d.aa_65p;
  cAgeAct.update();

  // S4 — Hours per activity
  cHoursPerAct.data.labels = d.act_hrs_labels;
  cHoursPerAct.data.datasets[0].data = d.act_hrs_data;
  cHoursPerAct.update();

  // S5 — Fraud trend
  cFraud.data.labels = d.month_labels;
  cFraud.data.datasets[0].data = d.fr_80hr;
  cFraud.data.datasets[1].data = d.fr_ov;
  cFraud.data.datasets[2].data = d.fr_dom;
  cFraud.update();

  // S5 — Veri method
  cVeri.data.labels = d.veri_labels;
  cVeri.data.datasets[0].data = d.veri_normal;
  cVeri.data.datasets[1].data = d.veri_overlap;
  cVeri.update();

  // S5 — Fraud table
  document.getElementById('fraudTableBody').innerHTML = d.flagged_html;

  // S6 — EVR State
  cEVRState.data.datasets[0].data = d.evr_exp;
  cEVRState.data.datasets[1].data = d.evr_rep;
  cEVRState.update();

  // S6 — EVR Monthly
  cEVRMonth.data.labels = d.month_labels;
  cEVRMonth.data.datasets[0].data = d.m_exp;
  cEVRMonth.data.datasets[1].data = d.m_rep;
  cEVRMonth.update();

  // S6 — Heatmap
  document.getElementById('heatmapHeadRow').innerHTML = d.heat_head;
  document.getElementById('heatmapBody').innerHTML    = d.heat_body;
}}
</script>
</body>
</html>"""

out_path = os.path.join(BASE, 'dashboard_real.html')
with open(out_path, 'w', encoding='utf-8') as f:
    f.write(html)
print(f"Written: {out_path}  ({len(html)//1024}KB)")

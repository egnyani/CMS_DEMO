"""
patch_age_filter.py

Adds per-age-group data entries to dashboard_v3.html's ALL_DATA so the age
filter truly re-segments every chart and KPI, and updates the JS accordingly.

New ALL_DATA keys:  <period>_<state>_0-18   /  _19-64  /  _65+
e.g.  202603_all_65+,  202603_CA_0-18, last3_all_19-64, …

JS changes injected:
  1. ageSelector onchange -> updateFilters()  (was refreshAgeCharts())
  2. currentData() / updateFilters() include age in key lookup
  3. refreshAgeCharts() kept for multi-series chart visibility only
"""

import csv, json, os, math, re
from datetime import date, timedelta
from collections import defaultdict, OrderedDict

BASE     = os.path.dirname(os.path.abspath(__file__))
REF_DATE = date(2026, 3, 30)

# ── helpers ──────────────────────────────────────────────────────────────────
def read_csv(fn):
    with open(os.path.join(BASE, fn), newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def fmt_num(n): return f"{int(n):,}"

def fmt_M(n):
    n = float(n)
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000:     return f"{n/1_000:.1f}K"
    return f"{n:.0f}"

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
est_name_map      = {r['establishment_key']: r['establishment_name'] for r in establishments}
veri_name_map     = {r['verification_method_key']: r['verification_method'] for r in veri_methods}

recip_state_map    = {r['medicaid_recipient_key']: r['eligible_state_code'] for r in recipients}
recip_detail       = {r['medicaid_recipient_key']: r for r in recipients}
recip_enroll_map   = {r['medicaid_recipient_key']: r['enrollment_type_code'] for r in recipients}
recip_reg_map      = {r['medicaid_recipient_key']: r['registration_date'] for r in recipients}
recip_agegroup_map = {r['medicaid_recipient_key']: age_group(age_from_dob(r['date_of_birth']))
                      for r in recipients}

all_states = sorted({r['eligible_state_code'] for r in recipients})

recip_first_act_date = {}
for r in activities:
    rk, d_str = r['medicaid_recipient_key'], r['beginning_date_of_service']
    if rk not in recip_first_act_date or d_str < recip_first_act_date[rk]:
        recip_first_act_date[rk] = d_str

acts_by_month = defaultdict(list)
for r in activities: acts_by_month[r['calendar_month_key']].append(r)

eng_by_month = defaultdict(list)
for r in monthly_eng: eng_by_month[r['calendar_month_key']].append(r)

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

def _heat_color(v):
    if v >= 60:  return 'rgba(5,150,105,0.15)'
    if v >= 45:  return 'rgba(202,138,4,0.12)'
    return 'rgba(220,38,38,0.12)'

def _heat_text(v):
    if v >= 60:  return '#065f46'
    if v >= 45:  return '#713f12'
    return '#991b1b'

# ── core compute function (now supports age_filter) ───────────────────────────
def compute_period(keys_list, state_filter=None, age_filter=None):
    ks = set(keys_list)
    p_acts, p_eng = [], []
    for k in keys_list:
        for r in acts_by_month.get(k, []):
            rk = r['medicaid_recipient_key']
            if state_filter and recip_state_map.get(rk) != state_filter:
                continue
            if age_filter and recip_agegroup_map.get(rk) != age_filter:
                continue
            p_acts.append(r)
        for r in eng_by_month.get(k, []):
            rk = r['medicaid_recipient_key']
            if state_filter and recip_state_map.get(rk) != state_filter:
                continue
            if age_filter and recip_agegroup_map.get(rk) != age_filter:
                continue
            p_eng.append(r)

    p_month_keys   = [k for k in month_keys_sorted if k in ks]
    p_month_labels = [month_name_map[k] for k in p_month_keys]
    p_yms          = {key_to_ym[k] for k in keys_list if k in key_to_ym}

    activity_records = len(p_acts)

    registered_in_period = sum(
        1 for r in recipients
        if r['registration_date'][:7] in p_yms
        and (not state_filter or r['eligible_state_code'] == state_filter)
        and (not age_filter  or recip_agegroup_map.get(r['medicaid_recipient_key']) == age_filter)
    )

    total_hours_raw = sum(float(r['monthly_hours_completed']) for r in p_eng)
    avg_hrs_mo = round(total_hours_raw / len(p_eng), 1) if p_eng else 0

    overlap_count = sum(1 for r in p_acts if r['is_overlap_flag'] == 'True')
    overlap_pct   = round(overlap_count / activity_records * 100, 1) if activity_records else 0

    compliance_count = sum(1 for r in p_eng if float(r['monthly_hours_completed']) >= 80.0)
    compliance_rate  = round(compliance_count / len(p_eng) * 100, 1) if p_eng else 0

    active_recips = len({r['medicaid_recipient_key'] for r in p_acts})
    all_enrolled_in_filter = [
        r['medicaid_recipient_key'] for r in recipients
        if (not state_filter or r['eligible_state_code'] == state_filter)
        and (not age_filter  or recip_agegroup_map.get(r['medicaid_recipient_key']) == age_filter)
    ]
    enrolled_count = len(all_enrolled_in_filter)
    active_rate = round(active_recips / enrolled_count * 100, 1) if enrolled_count else 0
    avg_acts_per_recip = round(activity_records / active_recips, 1) if active_recips else 0

    recips_active_prior = set()
    prior_keys = [k for k in month_keys_sorted if k < min(ks)]
    for k in prior_keys:
        for r in acts_by_month.get(k, []):
            rk = r['medicaid_recipient_key']
            if state_filter and recip_state_map.get(rk) != state_filter: continue
            if age_filter  and recip_agegroup_map.get(rk) != age_filter:  continue
            recips_active_prior.add(rk)
    recips_in_period      = {r['medicaid_recipient_key'] for r in p_acts}
    new_recipients        = len(recips_in_period - recips_active_prior)
    returning_recipients  = len(recips_in_period & recips_active_prior)

    days_list = []
    for rk in recips_in_period:
        reg_str = recip_reg_map.get(rk); fad_str = recip_first_act_date.get(rk)
        if reg_str and fad_str:
            try:
                delta = (date.fromisoformat(fad_str) - date.fromisoformat(reg_str)).days
                if 0 <= delta <= 180: days_list.append(delta)
            except: pass
    avg_days_first = round(sum(days_list) / len(days_list), 1) if days_list else 0

    monthly_reg = []
    for k in p_month_keys:
        ym = key_to_ym.get(k, '')
        monthly_reg.append(sum(
            1 for r in recipients
            if r['registration_date'][:7] == ym
            and (not state_filter or r['eligible_state_code'] == state_filter)
            and (not age_filter  or recip_agegroup_map.get(r['medicaid_recipient_key']) == age_filter)
        ))

    act_cnt = defaultdict(int)
    for r in p_acts: act_cnt[r['activity_type']] += 1
    act_sorted = sorted(act_cnt.items(), key=lambda x: -x[1])
    act_labels = [a[0] for a in act_sorted]
    act_data   = [a[1] for a in act_sorted]

    act_hrs = defaultdict(float)
    for r in p_acts: act_hrs[r['activity_type']] += float(r['activity_duration_hours'])
    hrs_sorted     = sorted(act_hrs.items(), key=lambda x: -x[1])
    act_hrs_labels = [a[0] for a in hrs_sorted]
    act_hrs_data   = [round(a[1] / 1000, 1) for a in hrs_sorted]

    funnel_bkts = {'0-25%': 0, '25-50%': 0, '50-75%': 0, '75-<100%': 0, '≥100%': 0}
    funnel_exact80 = 0
    for r in p_eng:
        h = float(r['monthly_hours_completed']); pct = h / 80.0 * 100
        if math.isclose(h, 80.0, abs_tol=1e-9): funnel_exact80 += 1
        elif pct < 25:  funnel_bkts['0-25%'] += 1
        elif pct < 50:  funnel_bkts['25-50%'] += 1
        elif pct < 75:  funnel_bkts['50-75%'] += 1
        elif pct < 100: funnel_bkts['75-<100%'] += 1
        else:           funnel_bkts['≥100%'] += 1
    funnel_labels = list(funnel_bkts.keys()) + ['= 80hrs']
    funnel_data   = list(funnel_bkts.values()) + [funnel_exact80]

    est_rset = defaultdict(set)
    for r in p_acts: est_rset[r['establishment_key']].add(r['medicaid_recipient_key'])
    top15 = sorted(est_rset.items(), key=lambda x: -len(x[1]))[:15]
    top15_labels = [est_name_map.get(k, f'Est {k}') for k, v in top15]
    top15_data   = [len(v) for k, v in top15]

    hrs_by_mk = defaultdict(list)
    for r in p_eng: hrs_by_mk[r['calendar_month_key']].append(float(r['monthly_hours_completed']))
    avg_hrs_per_month = [
        round(sum(hrs_by_mk.get(k, [])) / max(1, len(hrs_by_mk.get(k, [0]))), 1)
        for k in p_month_keys
    ]

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

    inactive_count = max(0, enrolled_count - active_recips)
    status_labels  = ['Active', 'Inactive', 'New This Period']
    status_data    = [active_recips - new_recipients, inactive_count, new_recipients]

    top6 = [a[0] for a in act_sorted[:6]]
    aa_cnt = defaultdict(lambda: defaultdict(int))
    for r in p_acts:
        ag_val = recip_agegroup_map.get(r['medicaid_recipient_key'], 'unknown')
        if r['activity_type'] in top6: aa_cnt[r['activity_type']][ag_val] += 1
    aa_018  = [aa_cnt[t].get('0-18',  0) for t in top6]
    aa_1964 = [aa_cnt[t].get('19-64', 0) for t in top6]
    aa_65p  = [aa_cnt[t].get('65+',   0) for t in top6]
    top6_wrapped = [wrap_label(t) for t in top6]

    age_m = defaultdict(lambda: defaultdict(list))
    for r in p_eng:
        age_m[r['calendar_month_key']][r['age_group']].append(float(r['monthly_hours_completed']))
    def avg_l(lst): return round(sum(lst) / len(lst), 1) if lst else 0
    age_m_018  = [avg_l(age_m.get(k, {}).get('0-18',  [])) for k in p_month_keys]
    age_m_1964 = [avg_l(age_m.get(k, {}).get('19-64', [])) for k in p_month_keys]
    age_m_65p  = [avg_l(age_m.get(k, {}).get('65+',   [])) for k in p_month_keys]

    fr_80hr = [sum(1 for r in eng_by_month.get(k, [])
                   if r['is_exactly_80_hours_flag'] == 'True'
                   and (not state_filter or recip_state_map.get(r['medicaid_recipient_key']) == state_filter)
                   and (not age_filter  or recip_agegroup_map.get(r['medicaid_recipient_key']) == age_filter))
               for k in p_month_keys]
    fr_ov   = [sum(1 for r in acts_by_month.get(k, [])
                   if r['is_overlap_flag'] == 'True'
                   and (not state_filter or recip_state_map.get(r['medicaid_recipient_key']) == state_filter)
                   and (not age_filter  or recip_agegroup_map.get(r['medicaid_recipient_key']) == age_filter))
               for k in p_month_keys]
    fr_dom  = [sum(1 for r in eng_by_month.get(k, [])
                   if r['is_single_establishment_dominant_flag'] == 'True'
                   and (not state_filter or recip_state_map.get(r['medicaid_recipient_key']) == state_filter)
                   and (not age_filter  or recip_agegroup_map.get(r['medicaid_recipient_key']) == age_filter))
               for k in p_month_keys]

    ex80  = sum(1 for r in p_eng if r['is_exactly_80_hours_flag'] == 'True')
    ex80c = sum(1 for r in p_eng if r['is_exactly_80_hours_consecutive_flag'] == 'True')

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

    p_ro = defaultdict(int); p_ra = defaultdict(int)
    for r in p_acts:
        p_ra[r['medicaid_recipient_key']] += 1
        if r['is_overlap_flag'] == 'True': p_ro[r['medicaid_recipient_key']] += 1
    flagged = []
    for k2, v in rf.items():
        risk = 'High' if v.get('consec_flag') else ('Medium' if v['dom'] >= 1 else None)
        if risk is None: continue
        ta = p_ra.get(k2, 0); ov = round(p_ro.get(k2, 0) / ta * 100) if ta else 0
        rd = recip_detail.get(k2, {})
        msis = rd.get('msis_identification_num', k2); st2 = rd.get('eligible_state_code', '?')
        enr = rd.get('enrollment_type_code', '?')
        fp = []
        if v.get('consec_flag'): fp.append(f"80×{v['consec']}")
        if v['dom'] >= 1:         fp.append(f"DomEst×{v['dom']}")
        if ov > 0:                fp.append('Overlap')
        flagged.append(((0 if risk=='High' else 1, -v['consec']),
                        msis, st2, enr, risk, ', '.join(fp) or 'Pattern', v['consec'], ov))
    flagged.sort(key=lambda x: x[0])
    flagged_html = ''
    for row in flagged[:12]:
        _, msis, st2, enr, risk, fp, consec, ov = row
        cls = 'high' if risk == 'High' else 'medium'
        ov_str = f'{ov}%' if ov else '—'
        flagged_html += (
            f'<tr><td style="font-family:monospace;font-size:12px">{msis}</td>'
            f'<td>{st2}</td>'
            f'<td style="font-size:11px;color:#6b7194">{enr[:8]}</td>'
            f'<td><span class="badge {cls}">{risk}</span></td>'
            f'<td style="font-size:11px">{fp}</td>'
            f'<td style="text-align:center">{consec}</td>'
            f'<td style="text-align:center">{ov_str}</td></tr>\n'
        )

    veri_cnt = defaultdict(int); veri_flag_cnt = defaultdict(lambda: defaultdict(int))
    for r in p_acts:
        vm = veri_name_map.get(r['verification_method_key'], f'Method {r["verification_method_key"]}')
        veri_cnt[vm] += 1
        veri_flag_cnt[vm]['Overlap' if r['is_overlap_flag']=='True' else 'Normal'] += 1
    veri_labels  = sorted(veri_cnt.keys())
    veri_normal  = [veri_flag_cnt[v].get('Normal',  0) for v in veri_labels]
    veri_overlap = [veri_flag_cnt[v].get('Overlap', 0) for v in veri_labels]

    s_exp = defaultdict(float); s_rep = defaultdict(float)
    for r in p_eng:
        st3 = recip_state_map.get(r['medicaid_recipient_key'], 'UNK')
        s_exp[st3] += 80.0; s_rep[st3] += float(r['monthly_hours_completed'])
    evr_exp = [round(s_exp.get(s, 0)) for s in all_states]
    evr_rep = [round(s_rep.get(s, 0)) for s in all_states]

    m_exp = [sum(80 for r in eng_by_month.get(k, [])
                 if (not state_filter or recip_state_map.get(r['medicaid_recipient_key']) == state_filter)
                 and (not age_filter  or recip_agegroup_map.get(r['medicaid_recipient_key']) == age_filter))
             for k in p_month_keys]
    m_rep = [round(sum(float(r['monthly_hours_completed'])
                       for r in eng_by_month.get(k, [])
                       if (not state_filter or recip_state_map.get(r['medicaid_recipient_key']) == state_filter)
                       and (not age_filter  or recip_agegroup_map.get(r['medicaid_recipient_key']) == age_filter)))
             for k in p_month_keys]

    tot_exp = sum(s_exp.values()); tot_rep = sum(s_rep.values())
    rep_ratio = round(tot_rep / tot_exp * 100, 1) if tot_exp else 0

    last3_keys = p_month_keys[-3:] if len(p_month_keys) >= 3 else p_month_keys
    r3_comp = sum(1 for k in last3_keys
                  for r in eng_by_month.get(k, [])
                  if float(r['monthly_hours_completed']) >= 80.0
                  and (not state_filter or recip_state_map.get(r['medicaid_recipient_key']) == state_filter)
                  and (not age_filter  or recip_agegroup_map.get(r['medicaid_recipient_key']) == age_filter))
    r3_tot  = sum(len([r for r in eng_by_month.get(k, [])
                       if (not state_filter or recip_state_map.get(r['medicaid_recipient_key']) == state_filter)
                       and (not age_filter  or recip_agegroup_map.get(r['medicaid_recipient_key']) == age_filter)])
                  for k in last3_keys)
    rolling3_compliance = round(r3_comp / r3_tot * 100, 1) if r3_tot else 0

    sm_exp = defaultdict(float); sm_rep = defaultdict(float)
    for r in p_eng:
        st4 = recip_state_map.get(r['medicaid_recipient_key'], 'UNK')
        sm_exp[(st4, r['calendar_month_key'])] += 80.0
        sm_rep[(st4, r['calendar_month_key'])] += float(r['monthly_hours_completed'])
    heat_head = '<th>State</th>' + ''.join(f'<th>{month_name_map[k]}</th>' for k in p_month_keys) + '<th>6-mo Avg</th>'
    heat_body = ''
    for st5 in all_states:
        heat_body += f'<tr><td style="font-weight:600">{st5}</td>'
        te = tr2 = 0
        for k in p_month_keys:
            e = sm_exp.get((st5, k), 0); rp2 = sm_rep.get((st5, k), 0)
            te += e; tr2 += rp2
            v = round(rp2 / e * 100) if e else 0
            bg = _heat_color(v)
            heat_body += f'<td style="background:{bg};text-align:center;font-weight:600;color:{_heat_text(v)}">{v}%</td>'
        tv = round(tr2 / te * 100) if te else 0
        heat_body += f'<td style="background:{_heat_color(tv)};text-align:center;font-weight:700;color:{_heat_text(tv)}">{tv}%</td></tr>\n'

    multistate_count_filtered = sum(
        1 for r in recipients
        if r['submitting_state_code'] != r['eligible_state_code']
        and (not state_filter or r['eligible_state_code'] == state_filter)
        and (not age_filter  or recip_agegroup_map.get(r['medicaid_recipient_key']) == age_filter)
    )

    return {
        'activity_records': activity_records,
        'activity_records_fmt': fmt_num(activity_records),
        'registered': registered_in_period,
        'registered_fmt': fmt_num(registered_in_period),
        'active_recips_fmt': fmt_num(active_recips),
        'active_rate': active_rate,
        'active_rate_fmt': f'{active_rate}%',
        'avg_acts_per_recip': avg_acts_per_recip,
        'avg_acts_fmt': f'{avg_acts_per_recip}',
        'multistate_fmt': fmt_num(multistate_count_filtered),
        'avg_hrs_mo': avg_hrs_mo,
        'total_hours_fmt': fmt_M(total_hours_raw),
        'compliance_rate': compliance_rate,
        'compliance_rate_fmt': f'{compliance_rate}%',
        'rolling3_compliance_fmt': f'{rolling3_compliance}%',
        'overlap_count': overlap_count,
        'overlap_count_fmt': fmt_num(overlap_count),
        'overlap_pct': overlap_pct,
        'new_recips_fmt': fmt_num(new_recipients),
        'returning_fmt': fmt_num(returning_recipients),
        'avg_days_first_fmt': f'{avg_days_first:.0f} days',
        'ex80': ex80, 'ex80_fmt': fmt_num(ex80),
        'ex80c': ex80c, 'ex80c_fmt': fmt_num(ex80c),
        'high_risk_fmt': fmt_num(high_risk),
        'medium_risk_fmt': fmt_num(medium_risk),
        'tot_exp_fmt': fmt_M(tot_exp),
        'tot_rep_fmt': fmt_M(tot_rep),
        'rep_ratio': rep_ratio,
        'rep_ratio_fmt': f'{rep_ratio}%',
        'month_labels': p_month_labels,
        'monthly_reg': monthly_reg,
        'act_labels': act_labels,
        'act_data': act_data,
        'act_hrs_labels': act_hrs_labels,
        'act_hrs_data': act_hrs_data,
        'funnel_labels': funnel_labels,
        'funnel_data': funnel_data,
        'top15_labels': top15_labels,
        'top15_data': top15_data,
        'avg_hrs_per_month': avg_hrs_per_month,
        'hrs_dist_data': hrs_dist_data,
        'status_labels': status_labels,
        'status_data': status_data,
        'top6': top6,
        'top6_wrapped': top6_wrapped,
        'aa_018': aa_018, 'aa_1964': aa_1964, 'aa_65p': aa_65p,
        'age_m_018': age_m_018, 'age_m_1964': age_m_1964, 'age_m_65p': age_m_65p,
        'fr_80hr': fr_80hr, 'fr_ov': fr_ov, 'fr_dom': fr_dom,
        'evr_exp': evr_exp, 'evr_rep': evr_rep,
        'm_exp': m_exp, 'm_rep': m_rep,
        'veri_labels': veri_labels,
        'veri_normal': veri_normal,
        'veri_overlap': veri_overlap,
        'flagged_html': flagged_html,
        'heat_head': heat_head,
        'heat_body': heat_body,
    }


# ── compute age-filtered data ─────────────────────────────────────────────────
AGE_GROUPS = ['0-18', '19-64', '65+']
new_data = {}

print("Computing age-filtered data...")
for pid, pc in period_configs.items():
    for ag in AGE_GROUPS:
        key = f'{pid}_all_{ag}'
        print(f"  {key}...")
        new_data[key] = compute_period(pc['keys'], age_filter=ag)
    for st in all_states:
        for ag in AGE_GROUPS:
            key = f'{pid}_{st}_{ag}'
            print(f"  {key}...")
            new_data[key] = compute_period(pc['keys'], state_filter=st, age_filter=ag)

print(f"Computed {len(new_data)} new age-filtered entries.")

# ── read dashboard_v3.html ────────────────────────────────────────────────────
dash_path = os.path.join(BASE, 'dashboard_v3.html')
with open(dash_path, 'r', encoding='utf-8') as f:
    html = f.read()

# ── inject new entries into ALL_DATA ─────────────────────────────────────────
# ALL_DATA is a JS object literal: const ALL_DATA = { ... };
# We find the closing }; of ALL_DATA and insert new entries before it.

new_entries_json = ',\n'.join(
    f'{json.dumps(k)}: {json.dumps(v, ensure_ascii=False)}'
    for k, v in new_data.items()
)

# Find the ALL_DATA block end - it ends before the next `const ` declaration after it
# The pattern is: const ALL_DATA = { ... };\n\nconst OVERLAP_STATE_NORMAL
# We need to insert our new entries inside the outer { } of ALL_DATA

# Strategy: find the last entry's closing brace before "const OVERLAP_STATE_NORMAL"
# and append our new entries
marker = 'const OVERLAP_STATE_NORMAL'
marker_idx = html.find(marker)
if marker_idx == -1:
    raise ValueError("Could not find OVERLAP_STATE_NORMAL marker in HTML")

# Walk back from marker to find the closing }; of ALL_DATA
# It will be on the line just before the marker
pre = html[:marker_idx]
# Find the last "};" before the marker
closing_idx = pre.rfind('};')
if closing_idx == -1:
    raise ValueError("Could not find closing }; of ALL_DATA")

# Insert new entries: replace "};" with ",\n<new entries>\n};"
html = html[:closing_idx] + ',\n' + new_entries_json + '\n}' + html[closing_idx+2:]

print("Injected age-filtered entries into ALL_DATA.")

# ── update JS: ageSelector onchange ──────────────────────────────────────────
# Change onchange="refreshAgeCharts()" to onchange="updateFilters()"
html = html.replace(
    'id="ageSelector" class="filter-select" onchange="refreshAgeCharts()"',
    'id="ageSelector" class="filter-select" onchange="updateFilters()"'
)

# ── update JS: currentData() to include age in key ───────────────────────────
old_current_data = '''function currentData() {
  const pid = document.getElementById('periodSelector').value;
  const st  = document.getElementById('stateSelector').value;
  return ALL_DATA[pid + '_' + st];
}'''
new_current_data = '''function currentData() {
  const pid = document.getElementById('periodSelector').value;
  const st  = document.getElementById('stateSelector').value;
  const ag  = document.getElementById('ageSelector') ? document.getElementById('ageSelector').value : '';
  const key = pid + '_' + st + (ag ? '_' + ag : '');
  return ALL_DATA[key] || ALL_DATA[pid + '_' + st];
}'''
html = html.replace(old_current_data, new_current_data)

# ── update JS: updateFilters() key construction to include age ────────────────
old_key_line = '''  const key = pid + '_' + st;
  let d = ALL_DATA[key];
  const notice = document.getElementById('stateNotice');
  if (!d && (pid === 'last3' || pid === 'last6')) {
    d = ALL_DATA[pid + '_all'];
    if (notice) { notice.style.display = 'block'; notice.textContent = '⚠ Multi-month view shows All States data. Select a single month for state-level breakdown.'; }
  } else {
    if (notice) notice.style.display = 'none';
  }
  if (!d) return;'''

new_key_line = '''  const ag  = document.getElementById('ageSelector') ? document.getElementById('ageSelector').value : '';
  const key = pid + '_' + st + (ag ? '_' + ag : '');
  let d = ALL_DATA[key];
  const notice = document.getElementById('stateNotice');
  if (!d && (pid === 'last3' || pid === 'last6')) {
    d = ALL_DATA[pid + '_all' + (ag ? '_' + ag : '')];
    if (notice) { notice.style.display = 'block'; notice.textContent = '⚠ Multi-month view shows All States data. Select a single month for state-level breakdown.'; }
  } else {
    if (notice) notice.style.display = 'none';
  }
  if (!d) return;'''

html = html.replace(old_key_line, new_key_line)

# ── update refreshAgeCharts() — remove KPI update (now handled by updateFilters) ──
# The KPI update block we added previously uses currentData() which now already
# respects the age filter, so it's redundant. We just remove that addition so
# there's no double-update. The visibility toggling stays.
old_age_kpi_block = '''
  // Update avg-hrs KPIs to reflect selected age group
  var d = currentData();
  if (!d) return;
  var ageArr = !sel ? null : sel === '0-18' ? d.age_m_018 : sel === '19-64' ? d.age_m_1964 : d.age_m_65p;
  var ageHrs = ageArr
    ? (ageArr.reduce(function(a,b){return a+b;},0) / ageArr.length).toFixed(1)
    : d.avg_hrs_mo;
  var a1 = document.getElementById('kpi-avghrs-s1');
  var a2 = document.getElementById('kpi-avghrs');
  if (a1) a1.textContent = ageHrs;
  if (a2) a2.textContent = ageHrs;
}'''
new_age_kpi_end = '\n}'
if old_age_kpi_block in html:
    html = html.replace(old_age_kpi_block, new_age_kpi_end)

# ── write patched dashboard_v3.html ──────────────────────────────────────────
with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(html)

print(f"Patched: {dash_path}  ({len(html)//1024}KB)")
print("Done! Age filter now drives true per-segment data across all charts and KPIs.")

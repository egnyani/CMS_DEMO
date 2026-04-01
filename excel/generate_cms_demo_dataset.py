#!/usr/bin/env python3
"""
generate_cms_demo_dataset.py  — v2 (realistic distributions)

Key fixes vs v1:
  A. Weighted state distribution (CA/TX/NY/FL dominant)
  B. Weighted establishment types (Clinic + Pharmacy dominant)
  C. Weighted enrollment types (Medicaid ~72%)
  D. Weighted sex codes (F > M >> U)
  E. Age-bucket DOB generation (~35% 0-18, ~45% 19-64, ~20% 65+)
  F. Front-loaded monthly registrations
  G. Per-recipient engagement tier + state×month duration multipliers → ramp-up trajectory
  H. State-level duration multipliers → real variance in reporting ratios
  I. Overlap: scattered days/times instead of fixed 12th/9am pattern
  J. Exact-80: varied hour combos that still sum to exactly 80
  K. Larger fraud cohorts + lower flag thresholds (streak >= 2)
"""
from __future__ import annotations

import json
import math
import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


SEED = 20260330
random.seed(SEED)

ROOT   = Path(__file__).parent
OUTDIR = ROOT / "output"

TARGET_RECIPIENTS    = 10_000
TARGET_ESTABLISHMENTS = 500

# ── Fraud cohort rates (larger → non-trivial KPI counts) ────────────────────
OVERLAP_RECIPIENT_RATE  = 0.025   # 250 recipients
EXACT_80_RECIPIENT_RATE = 0.025   # 250 recipients
DOMINANT_RECIPIENT_RATE = 0.035   # 350 recipients

# ── State definitions with weighted recipient assignment ─────────────────────
STATES        = ["CA", "FL", "GA", "IL", "NY", "OH", "PA", "TX"]
STATE_WEIGHTS = [0.18, 0.11, 0.06, 0.08, 0.12, 0.07, 0.07, 0.14]   # unnormalized; random.choices normalises

# ── State-level activity-duration multiplier (controls reporting ratio) ──────
#   Ratio ≈ multiplier × baseline_ratio.  Baseline (PA=1.0) ~ 60%.
STATE_DURATION_MULT = {
    "CA": 1.30,   # ~78% ratio
    "FL": 0.90,   # ~54%
    "GA": 0.85,   # ~51%
    "IL": 1.10,   # ~66%
    "NY": 1.25,   # ~75%
    "OH": 0.60,   # ~36%
    "PA": 1.00,   # ~60% (baseline)
    "TX": 0.70,   # ~42%
}

# ── Month-level progression multiplier (ramp-up over 6 months) ───────────────
MONTH_PROGRESSION = {
    202510: 0.52,
    202511: 0.65,
    202512: 0.78,
    202601: 0.88,
    202602: 0.95,
    202603: 1.00,
}

# ── Per-recipient engagement tier ────────────────────────────────────────────
ENGAGEMENT_TIERS        = ["low", "medium", "high"]
ENGAGEMENT_TIER_WEIGHTS = [0.22, 0.48, 0.30]
TIER_ACT_RANGE          = {"low": (3, 6), "medium": (6, 10), "high": (10, 15)}

# ── Enrollment types ─────────────────────────────────────────────────────────
ENROLLMENT_TYPES   = ["Medicaid", "Medicaid Expansion CHIP", "Separate CHIP"]
ENROLLMENT_WEIGHTS = [0.72, 0.18, 0.10]

# ── Sex codes ────────────────────────────────────────────────────────────────
SEX_CODES   = ["M", "F", "U"]
SEX_WEIGHTS = [0.42, 0.57, 0.01]

# ── Establishment types with realistic weights ───────────────────────────────
ESTABLISHMENT_TYPES = [
    "Clinic", "Pharmacy", "Home Care Agency",
    "Hospital", "Nursing Facility", "Behavioral Health Center",
]
EST_TYPE_WEIGHTS = [0.30, 0.30, 0.15, 0.12, 0.08, 0.05]

FACILITY_GROUP_CODES = ["F", "G", "I"]
PROVIDER_CLASSIFICATION_TYPES = ["State Provider Category", "Taxonomy Family"]
PROVIDER_CLASSIFICATION_CODES = {"Clinic": "CLIN", "Pharmacy": "RX",
    "Home Care Agency": "HHA", "Hospital": "HOSP",
    "Nursing Facility": "NF", "Behavioral Health Center": "BH"}

VERIFICATION_METHODS = [
    ("Manual",     "Demo-only manually verified workflow"),
    ("Electronic", "Demo-only batch/electronic verification workflow"),
    ("API",        "Demo-only API-based verification workflow"),
]
VERIFICATION_WEIGHTS = [0.30, 0.50, 0.20]   # Electronic most common

CLAIM_FAMILIES = ["IP", "LT", "OT", "RX"]

ACTIVITY_TYPE_MAP = {
    "IP": ["Inpatient Stay", "Hospital Observation", "Acute Facility Care"],
    "LT": ["Long-Term Care Stay", "Skilled Nursing Support", "Residential Care"],
    "OT": ["Outpatient Visit", "Home and Community-Based Service", "Therapy Session"],
    "RX": ["Pharmacy Fill", "Medication Therapy Support"],
}

# ── Monthly registration shape (front-loaded) ────────────────────────────────
REG_MONTH_STARTS = [
    date(2025, 10, 1), date(2025, 11, 1), date(2025, 12, 1),
    date(2026,  1, 1), date(2026,  2, 1), date(2026,  3, 1),
]
REG_MONTH_WEIGHTS = [0.28, 0.22, 0.18, 0.15, 0.11, 0.06]

# ── Exact-80 hour combos (all sum to exactly 80) ─────────────────────────────
EXACT_80_COMBOS: List[List[int]] = [
    [4,  6,  8,  8,  8,  8,  8,  8, 10, 12],
    [3,  5,  7,  8,  8,  8,  9, 10, 11, 11],
    [6,  6,  6,  8,  8,  8,  8, 10, 10, 10],
    [5,  6,  7,  8,  8,  8,  8,  9, 10, 11],
    [4,  4,  6,  8,  8,  8, 10, 10, 10, 12],
    [7,  7,  7,  8,  8,  8,  8,  9,  9,  9],
    [5,  5,  7,  7,  8,  8,  8,  8, 12, 12],
    [3,  4,  5,  8,  8,  8,  8, 10, 12, 14],
]
assert all(sum(c) == 80 for c in EXACT_80_COMBOS), "EXACT_80_COMBOS must all sum to 80"


# ── Dataclasses ──────────────────────────────────────────────────────────────
@dataclass
class Establishment:
    establishment_key: int
    submitting_state_prov_id: str
    prov_location_id: str
    service_facility_org_npi: str
    establishment_name: str
    facility_group_individual_code: str
    provider_classification_type: str
    provider_classification_code: str
    establishment_type: str
    establishment_state_code: str


@dataclass
class Recipient:
    medicaid_recipient_key: int
    msis_identification_num: str
    submitting_state_code: str
    eligible_state_code: str
    date_of_birth: date
    sex_code: str
    enrollment_type_code: str
    registration_date: date
    engagement_tier: str   # internal; not persisted to CSV


# ── Calendar / date helpers ───────────────────────────────────────────────────
def month_range(start_month: date, end_month: date) -> List[date]:
    months, current = [], start_month
    while current <= end_month:
        months.append(current)
        current = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
    return months


def month_end(d: date) -> date:
    return ((d.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1))


def calc_age(dob: date, on_date: date) -> int:
    years = on_date.year - dob.year
    return years - int((on_date.month, on_date.day) < (dob.month, dob.day))


def age_group_for_date(dob: date, on_date: date) -> str:
    age = calc_age(dob, on_date)
    if age <= 18:  return "0-18"
    if age <= 64:  return "19-64"
    return "65+"


# ── Dimension builders ───────────────────────────────────────────────────────
def build_calendar_months() -> pd.DataFrame:
    months = month_range(date(2025, 10, 1), date(2026, 3, 1))
    return pd.DataFrame([{
        "calendar_month_key": m.year * 100 + m.month,
        "month_start_date":   m,
        "month_end_date":     month_end(m),
        "year_num":           m.year,
        "month_num":          m.month,
        "month_name":         m.strftime("%b %Y"),
    } for m in months])


def build_verification_methods() -> pd.DataFrame:
    return pd.DataFrame([{
        "verification_method_key": idx,
        "verification_method": m,
        "verification_method_description": d,
    } for idx, (m, d) in enumerate(VERIFICATION_METHODS, 1)])


def build_establishments(n: int = TARGET_ESTABLISHMENTS) -> Tuple[pd.DataFrame, List[Establishment]]:
    rows, ests = [], []
    est_types = random.choices(ESTABLISHMENT_TYPES, weights=EST_TYPE_WEIGHTS, k=n)
    for idx in range(1, n + 1):
        state = random.choices(STATES, weights=STATE_WEIGHTS)[0]
        etype = est_types[idx - 1]
        pcode = PROVIDER_CLASSIFICATION_CODES[etype]
        ptype = random.choice(PROVIDER_CLASSIFICATION_TYPES)
        fgi   = FACILITY_GROUP_CODES[(idx - 1) % len(FACILITY_GROUP_CODES)]
        est = Establishment(
            establishment_key=idx,
            submitting_state_prov_id=f"{state}P{idx:05d}",
            prov_location_id=f"L{(idx % 7) + 1:03d}",
            service_facility_org_npi=f"{10_000_000_000 + idx:010d}",
            establishment_name=f"{state} {etype} {idx}",
            facility_group_individual_code=fgi,
            provider_classification_type=ptype,
            provider_classification_code=pcode,
            establishment_type=etype,
            establishment_state_code=state,
        )
        ests.append(est)
        rows.append({k: v for k, v in est.__dict__.items()})
    return pd.DataFrame(rows), ests


def random_dob() -> date:
    """Weighted DOB so ~35% age 0-18, ~45% 19-64, ~20% 65+."""
    bucket = random.choices([0, 1, 2], weights=[35, 45, 20])[0]
    if bucket == 0:   yr_range = (2007, 2024)   # age 1-18 approx
    elif bucket == 1: yr_range = (1962, 2006)   # age 19-64
    else:             yr_range = (1930, 1960)   # age 65+
    return date(random.randint(*yr_range), random.randint(1, 12), random.randint(1, 28))


def random_registration_date(is_protected: bool, idx: int) -> date:
    if is_protected:
        return date(2025, 10, 1) + timedelta(days=(idx - 1) % 12)
    month_start = random.choices(REG_MONTH_STARTS, weights=REG_MONTH_WEIGHTS)[0]
    days_in_month = (month_end(month_start) - month_start).days
    return month_start + timedelta(days=random.randint(0, days_in_month - 1))


def build_recipients(n: int = TARGET_RECIPIENTS,
                     protected_prefix: int = 0) -> Tuple[pd.DataFrame, List[Recipient]]:
    rows, recipients = [], []
    for idx in range(1, n + 1):
        is_protected = idx <= protected_prefix
        sub_state  = random.choices(STATES, weights=STATE_WEIGHTS)[0]
        elig_state = random.choices(STATES, weights=STATE_WEIGHTS)[0]
        tier       = random.choices(ENGAGEMENT_TIERS, weights=ENGAGEMENT_TIER_WEIGHTS)[0]
        rec = Recipient(
            medicaid_recipient_key=idx,
            msis_identification_num=f"MSIS{idx:08d}",
            submitting_state_code=sub_state,
            eligible_state_code=elig_state,
            date_of_birth=random_dob(),
            sex_code=random.choices(SEX_CODES, weights=SEX_WEIGHTS)[0],
            enrollment_type_code=random.choices(ENROLLMENT_TYPES, weights=ENROLLMENT_WEIGHTS)[0],
            registration_date=random_registration_date(is_protected, idx),
            engagement_tier=tier,
        )
        recipients.append(rec)
        d = rec.__dict__.copy()
        d.pop("engagement_tier")   # not in CSV schema
        rows.append(d)
    return pd.DataFrame(rows), recipients


# ── Activity helpers ─────────────────────────────────────────────────────────
def choose_claim_family(establishment_type: str) -> str:
    if establishment_type == "Hospital":
        return random.choices(["IP", "OT"], weights=[0.65, 0.35])[0]
    if establishment_type == "Nursing Facility":
        return random.choices(["LT", "OT"], weights=[0.75, 0.25])[0]
    if establishment_type == "Pharmacy":
        return "RX"
    if establishment_type == "Home Care Agency":
        return random.choices(["OT", "LT"], weights=[0.88, 0.12])[0]
    return random.choices(["OT", "RX", "IP"], weights=[0.75, 0.15, 0.10])[0]


def choose_activity_type(claim_family: str, establishment_type: str) -> str:
    options = ACTIVITY_TYPE_MAP[claim_family]
    if claim_family == "IP" and establishment_type == "Hospital":   return options[0]
    if claim_family == "LT" and establishment_type == "Nursing Facility": return options[0]
    if claim_family == "RX" and establishment_type == "Pharmacy":  return options[0]
    return random.choice(options)


def synthesize_interval(
    claim_family: str, month_start: date,
    duration_mult: float = 1.0,
) -> Tuple[datetime, datetime, date, date]:
    day = random.randint(1, min(month_end(month_start).day - 2, 26))
    base_date = month_start.replace(day=day)

    if claim_family == "IP":
        span_days     = random.randint(1, 4)
        start_hour    = random.randint(6, 14)
        # Timestamp spans same-day session only (service dates cover full stay)
        raw_dur       = random.choice([4, 6, 8, 10, 12])
    elif claim_family == "LT":
        span_days     = random.randint(4, 14)
        start_hour    = random.randint(7, 11)
        # Timestamp = daily assessment session; span via service dates
        raw_dur       = random.choice([4, 6, 8, 10])
    elif claim_family == "RX":
        span_days     = 0
        start_hour    = random.randint(8, 18)
        raw_dur       = random.choice([0.25, 0.5, 1.0])
    else:  # OT / HCBS  (longer sessions for community engagement context)
        span_days     = random.randint(0, 2)
        start_hour    = random.randint(7, 17)
        raw_dur       = random.choice([3, 4, 6, 8, 10, 12])

    # Apply multiplier (skip for RX — pharmacy visits don't scale)
    duration_hours = raw_dur if claim_family == "RX" else round(raw_dur * duration_mult, 2)
    duration_hours = max(0.25, duration_hours)

    start_dt = datetime.combine(base_date, time(start_hour, random.choice([0, 15, 30, 45])))
    end_dt   = start_dt + timedelta(hours=duration_hours)
    ending_service_date = min(base_date + timedelta(days=span_days), month_end(month_start))
    if end_dt.date() > ending_service_date:
        ending_service_date = end_dt.date()
    return start_dt, end_dt, base_date, ending_service_date


def exact_80_plan(month_start: date) -> List[Tuple[datetime, datetime, date, date]]:
    """Generate activities that sum to EXACTLY 80 hours with natural variation."""
    combo = random.choice(EXACT_80_COMBOS)
    max_day = month_end(month_start).day - 2
    days = sorted(random.sample(range(2, max_day + 1), len(combo)))
    rows = []
    for day, hours in zip(days, combo):
        h    = random.choice([7, 8, 9, 10, 11])
        m    = random.choice([0, 15, 30])
        sdt  = datetime.combine(month_start.replace(day=day), time(h, m))
        edt  = sdt + timedelta(hours=hours)
        rows.append((sdt, edt, sdt.date(), edt.date()))
    return rows


def scattered_overlap_pair(month_start: date,
                            est1: "Establishment",
                            est2: "Establishment"
                            ) -> List[Tuple[int, "Establishment", datetime, datetime]]:
    """Produce two overlapping activities on a random day with randomised times."""
    day     = random.randint(3, month_end(month_start).day - 3)
    h1      = random.randint(8, 14)
    m1      = random.choice([0, 15, 30, 45])
    dur1    = random.choice([2, 3, 4, 5])
    overlap_offset = random.randint(1, dur1 - 1)   # starts while first is running
    h2      = h1 + overlap_offset
    m2      = random.choice([0, 15, 30, 45])
    dur2    = random.choice([2, 3])
    sdt1 = datetime.combine(month_start.replace(day=day), time(h1, m1))
    edt1 = sdt1 + timedelta(hours=dur1)
    sdt2 = datetime.combine(month_start.replace(day=day), time(h2, m2))
    edt2 = sdt2 + timedelta(hours=dur2)
    return [(90, est1, sdt1, edt1), (91, est2, sdt2, edt2)]


# ── Activity fact builder ─────────────────────────────────────────────────────
def build_activity_fact(
    recipients: List[Recipient],
    establishments: List[Establishment],
    calendar_months: pd.DataFrame,
    verification_methods: pd.DataFrame,
) -> pd.DataFrame:
    months            = [pd.Timestamp(d).date() for d in calendar_months["month_start_date"].tolist()]
    est_lookup        = {e.establishment_key: e for e in establishments}
    verification_keys = verification_methods["verification_method_key"].tolist()
    verification_wts  = VERIFICATION_WEIGHTS
    rows: List[Dict] = []
    activity_key = 1

    n = len(recipients)
    overlap_count  = max(15, int(n * OVERLAP_RECIPIENT_RATE))
    exact_80_count = max(15, int(n * EXACT_80_RECIPIENT_RATE))
    dominant_count = max(20, int(n * DOMINANT_RECIPIENT_RATE))

    overlap_set  = set(range(1, overlap_count + 1))
    exact80_set  = set(range(overlap_count + 1,
                              overlap_count + exact_80_count + 1))
    dominant_set = set(range(overlap_count + exact_80_count + 1,
                              overlap_count + exact_80_count + dominant_count + 1))

    for recipient in recipients:
        active_months = [m for m in months if m >= recipient.registration_date.replace(day=1)]
        if not active_months:
            continue

        state_mult = STATE_DURATION_MULT.get(recipient.eligible_state_code, 1.0)
        tier_lo, tier_hi = TIER_ACT_RANGE[recipient.engagement_tier]

        dom_est = random.choice(establishments)
        alt_est = random.choice([e for e in establishments
                                  if e.establishment_key != dom_est.establishment_key])

        for month_start in active_months:
            mk         = month_start.year * 100 + month_start.month
            month_mult = MONTH_PROGRESSION.get(mk, 1.0)
            eff_mult   = state_mult * month_mult

            rid = recipient.medicaid_recipient_key

            # ── exact-80 cohort ──────────────────────────────────────────────
            if rid in exact80_set and month_start in months[1:6]:
                plan = exact_80_plan(month_start)
                for ix, (sdt, edt, bsd, esd) in enumerate(plan, 1):
                    est = dom_est if ix <= 8 else alt_est
                    cf  = choose_claim_family(est.establishment_type)
                    rows.append(make_activity_row(
                        activity_key, recipient, est, verification_keys,
                        verification_wts, mk, cf, bsd, esd, sdt, edt,
                        line_num=ix, adj_suffix="A", est_lookup=est_lookup,
                    ))
                    activity_key += 1
                # Overlap injection after exact-80 plan
                if rid in overlap_set and month_start in months[2:5]:
                    for ln_off, est, sdt, edt in scattered_overlap_pair(month_start, dom_est, alt_est):
                        rows.append(make_activity_row(
                            activity_key, recipient, est, verification_keys,
                            verification_wts, mk, "OT", sdt.date(), edt.date(),
                            sdt, edt, line_num=ln_off, adj_suffix="O", est_lookup=est_lookup,
                        ))
                        activity_key += 1
                continue

            # ── dominant cohort ──────────────────────────────────────────────
            if rid in dominant_set and month_start in months[0:6]:
                base_count = 8
                for ln in range(1, base_count + 1):
                    est = dom_est if ln <= 7 else alt_est
                    cf  = choose_claim_family(est.establishment_type)
                    # Fix durations for dominant pattern
                    dur_h = 8.0 if est.establishment_key == dom_est.establishment_key else 1.5
                    day   = random.randint(1, 26)
                    sh    = random.randint(7, 17)
                    sm    = random.choice([0, 15, 30, 45])
                    sdt   = datetime.combine(month_start.replace(day=day), time(sh, sm))
                    edt   = sdt + timedelta(hours=dur_h)
                    rows.append(make_activity_row(
                        activity_key, recipient, est, verification_keys,
                        verification_wts, mk, cf, sdt.date(), edt.date(),
                        sdt, edt, line_num=ln, adj_suffix="A", est_lookup=est_lookup,
                    ))
                    activity_key += 1
            else:
                # ── regular recipients ───────────────────────────────────────
                base_count = random.randint(tier_lo, tier_hi)
                for ln in range(1, base_count + 1):
                    est = random.choice(establishments)
                    cf  = choose_claim_family(est.establishment_type)
                    sdt, edt, bsd, esd = synthesize_interval(cf, month_start,
                                                               duration_mult=eff_mult)
                    rows.append(make_activity_row(
                        activity_key, recipient, est, verification_keys,
                        verification_wts, mk, cf, bsd, esd, sdt, edt,
                        line_num=ln, adj_suffix="A", est_lookup=est_lookup,
                    ))
                    activity_key += 1

            # ── overlap injection for overlap cohort ─────────────────────────
            if rid in overlap_set and month_start in months[2:5]:
                for ln_off, est, sdt, edt in scattered_overlap_pair(month_start, dom_est, alt_est):
                    rows.append(make_activity_row(
                        activity_key, recipient, est, verification_keys,
                        verification_wts, mk, "OT", sdt.date(), edt.date(),
                        sdt, edt, line_num=ln_off, adj_suffix="O", est_lookup=est_lookup,
                    ))
                    activity_key += 1

    df = pd.DataFrame(rows)
    df = derive_overlap_flags(df)
    return df.sort_values("engagement_activity_key").reset_index(drop=True)


def make_activity_row(
    activity_key: int,
    recipient: Recipient,
    establishment: "Establishment",
    verification_keys: List[int],
    verification_wts: List[float],
    calendar_month_key: int,
    claim_family: str,
    beginning_service_date: date,
    ending_service_date: date,
    start_dt: datetime,
    end_dt: datetime,
    line_num: int,
    adj_suffix: str,
    est_lookup: Dict[int, "Establishment"],
) -> Dict:
    activity_type = choose_activity_type(claim_family, establishment.establishment_type)
    icn_base = (f"{claim_family}{calendar_month_key}"
                f"{recipient.medicaid_recipient_key:05d}{activity_key:06d}")
    vkey = random.choices(verification_keys, weights=verification_wts)[0]
    return {
        "engagement_activity_key":    activity_key,
        "medicaid_recipient_key":     recipient.medicaid_recipient_key,
        "establishment_key":          establishment.establishment_key,
        "calendar_month_key":         calendar_month_key,
        "verification_method_key":    vkey,
        "icn_orig":                   f"{icn_base}O",
        "icn_adj":                    f"{icn_base}A",
        "line_num_orig":              line_num,
        "line_num_adj":               line_num,
        "source_record_id":           f"{claim_family}000{1 if claim_family in ('IP','LT') else 2}",
        "source_record_number":       f"{activity_key:010d}",
        "claim_submitting_state_code": establishment.establishment_state_code,
        "beginning_date_of_service":  beginning_service_date,
        "ending_date_of_service":     ending_service_date,
        "activity_type":              activity_type,
        "activity_start_ts":          start_dt,
        "activity_end_ts":            end_dt,
        "activity_duration_hours":    round((end_dt - start_dt).total_seconds() / 3600, 2),
        "is_overlap_flag":            False,
    }


def derive_overlap_flags(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    overlap_idx = set()
    for _, grp in df.sort_values("activity_start_ts").groupby("medicaid_recipient_key"):
        rows = grp.to_dict("records")
        for i, left in enumerate(rows):
            for right in rows[i + 1:]:
                if left["establishment_key"] == right["establishment_key"]:
                    continue
                if left["activity_end_ts"] <= right["activity_start_ts"]:
                    break
                ls = max(left["activity_start_ts"], right["activity_start_ts"])
                le = min(left["activity_end_ts"],   right["activity_end_ts"])
                if ls < le:
                    overlap_idx.add(left["engagement_activity_key"])
                    overlap_idx.add(right["engagement_activity_key"])
    df.loc[df["engagement_activity_key"].isin(overlap_idx), "is_overlap_flag"] = True
    return df


# ── Monthly aggregation tables ────────────────────────────────────────────────
def build_monthly_tables(
    activity_df: pd.DataFrame,
    recipients_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rec_lu = recipients_df.set_index("medicaid_recipient_key").to_dict("index")

    # recipient × establishment × month
    re_agg = (
        activity_df
        .groupby(["medicaid_recipient_key", "establishment_key", "calendar_month_key"],
                 as_index=False)
        .agg(activity_count=("engagement_activity_key", "count"),
             monthly_hours_completed=("activity_duration_hours", "sum"))
        .sort_values(["medicaid_recipient_key", "calendar_month_key",
                      "monthly_hours_completed"], ascending=[True, True, False])
        .reset_index(drop=True)
    )

    # share per recipient-month
    share_map: Dict = {}
    re_rows = []
    for (rk, mk), grp in re_agg.groupby(["medicaid_recipient_key", "calendar_month_key"]):
        total_h = grp["monthly_hours_completed"].sum()
        ranked  = grp.sort_values(["monthly_hours_completed", "establishment_key"],
                                   ascending=[False, True]).reset_index(drop=True)
        for idx, row in ranked.iterrows():
            share = round(row["monthly_hours_completed"] / total_h, 4) if total_h else 0.0
            share_map[(rk, mk, row["establishment_key"])] = share
            re_rows.append({
                "medicaid_recipient_key":                rk,
                "establishment_key":                     int(row["establishment_key"]),
                "calendar_month_key":                    int(mk),
                "activity_count":                        int(row["activity_count"]),
                "monthly_hours_completed":               round(float(row["monthly_hours_completed"]), 2),
                "establishment_share_of_monthly_hours":  share,
                "is_dominant_establishment_for_month":   idx == 0,
                "dominance_rank_in_month":               idx + 1,
            })
    re_df = pd.DataFrame(re_rows)

    # recipient × month
    rm_agg = (
        activity_df
        .groupby(["medicaid_recipient_key", "calendar_month_key"], as_index=False)
        .agg(activity_count=("engagement_activity_key", "count"),
             establishment_count=("establishment_key", "nunique"),
             monthly_hours_completed=("activity_duration_hours", "sum"))
        .reset_index(drop=True)
    )

    streak_state:  Dict[int, int]           = defaultdict(int)
    dominant_state: Dict[int, Tuple]        = defaultdict(lambda: (None, 0))
    m_rows = []

    for _, row in rm_agg.sort_values(["medicaid_recipient_key", "calendar_month_key"]).iterrows():
        rk = int(row["medicaid_recipient_key"])
        mk = int(row["calendar_month_key"])
        rec       = rec_lu[rk]
        me_date   = month_end(date(mk // 100, mk % 100, 1))
        age_grp   = age_group_for_date(rec["date_of_birth"], me_date)
        mh        = round(float(row["monthly_hours_completed"]), 2)
        is_ex80   = math.isclose(mh, 80.0, abs_tol=1e-9)
        streak_state[rk] = streak_state[rk] + 1 if is_ex80 else 0

        dom_row = re_df[
            (re_df["medicaid_recipient_key"] == rk) &
            (re_df["calendar_month_key"]     == mk) &
            re_df["is_dominant_establishment_for_month"]
        ].iloc[0]
        dom_est   = int(dom_row["establishment_key"])
        dom_share = float(dom_row["establishment_share_of_monthly_hours"])

        prev_est, prev_streak = dominant_state[rk]
        # Lowered threshold: share >= 0.80 and streak >= 2 (was 0.85 / 3)
        if dom_share >= 0.80 and dom_est == prev_est:
            d_streak = prev_streak + 1
        elif dom_share >= 0.80:
            d_streak = 1
        else:
            d_streak = 0
        dominant_state[rk] = (dom_est, d_streak)

        m_rows.append({
            "medicaid_recipient_key":              rk,
            "calendar_month_key":                  mk,
            "activity_count":                      int(row["activity_count"]),
            "establishment_count":                 int(row["establishment_count"]),
            "age_group":                           age_grp,
            "monthly_hours_completed":             mh,
            "required_monthly_hours":              80,
            "is_exactly_80_hours_flag":            is_ex80,
            "exact_80_consecutive_month_count":    streak_state[rk],
            "is_exactly_80_hours_consecutive_flag": streak_state[rk] >= 2,   # was 3
            "is_single_establishment_dominant_flag": d_streak >= 2,           # was 3
        })

    return pd.DataFrame(m_rows), re_df


# ── Validation ────────────────────────────────────────────────────────────────
def write_validation_sql() -> str:
    return """-- Validation queries for CMS demo dataset v2
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
"""


def build_validation_summary(recip_df, est_df, cal_df, veri_df,
                               act_df, monthly_df, re_df) -> Dict:
    orphan_r = int((~act_df["medicaid_recipient_key"].isin(recip_df["medicaid_recipient_key"])).sum())
    orphan_e = int((~act_df["establishment_key"].isin(est_df["establishment_key"])).sum())
    invalid  = int((act_df["activity_end_ts"] <= act_df["activity_start_ts"]).sum())
    return {
        "seed": SEED,
        "output_format": "csv",
        "business_rules": {
            "exact_80_threshold":      "streak >= 2",
            "dominant_est_threshold":  "share >= 0.80, streak >= 2",
        },
        "row_counts": {
            "dim_medicaid_recipient":         int(len(recip_df)),
            "dim_establishment":              int(len(est_df)),
            "dim_calendar_month":             int(len(cal_df)),
            "dim_verification_method":        int(len(veri_df)),
            "fact_engagement_activity":       int(len(act_df)),
            "fact_recipient_monthly_engagement": int(len(monthly_df)),
            "fact_recipient_establishment_monthly": int(len(re_df)),
        },
        "validation_checks": {
            "orphan_recipient_keys":        orphan_r,
            "orphan_establishment_keys":    orphan_e,
            "invalid_intervals":            invalid,
            "overlap_flagged_rows":         int(act_df["is_overlap_flag"].sum()),
            "exact_80_consecutive_rows":    int(monthly_df["is_exactly_80_hours_consecutive_flag"].sum()),
            "dominant_establishment_rows":  int(monthly_df["is_single_establishment_dominant_flag"].sum()),
        },
    }


def write_csv(df: pd.DataFrame, path: Path) -> None:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    out.to_csv(path, index=False)


def main() -> None:
    OUTDIR.mkdir(parents=True, exist_ok=True)
    print("Building calendar months...")
    cal_df   = build_calendar_months()
    veri_df  = build_verification_methods()

    protected_prefix = max(20, int(TARGET_RECIPIENTS * (
        OVERLAP_RECIPIENT_RATE + EXACT_80_RECIPIENT_RATE + DOMINANT_RECIPIENT_RATE)))
    print(f"  protected_prefix={protected_prefix}")

    print("Building establishments...")
    est_df, establishments = build_establishments()

    print("Building recipients...")
    rec_df, recipients = build_recipients(protected_prefix=protected_prefix)

    print("Building activity facts (this may take a minute)...")
    act_df = build_activity_fact(recipients, establishments, cal_df, veri_df)
    print(f"  {len(act_df):,} activity rows")

    print("Building monthly aggregation tables...")
    monthly_df, re_df = build_monthly_tables(act_df, rec_df)

    outputs = {
        "dim_medicaid_recipient.csv":         rec_df,
        "dim_establishment.csv":              est_df,
        "dim_calendar_month.csv":             cal_df,
        "dim_verification_method.csv":        veri_df,
        "fact_engagement_activity.csv":       act_df,
        "fact_recipient_monthly_engagement.csv": monthly_df,
        "fact_recipient_establishment_monthly.csv": re_df,
    }
    for fn, df in outputs.items():
        write_csv(df, OUTDIR / fn)
        print(f"  wrote {fn}  ({len(df):,} rows)")

    (OUTDIR / "validation_queries.sql").write_text(write_validation_sql(), encoding="utf-8")
    summary = build_validation_summary(rec_df, est_df, cal_df, veri_df, act_df, monthly_df, re_df)
    (OUTDIR / "validation_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print("Done.")
    print(json.dumps(summary["validation_checks"], indent=2))


if __name__ == "__main__":
    main()

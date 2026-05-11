"""
update_org_types.py
Reassigns 3 unsupported org types to 4 CMS-backed types in dim_establishment.csv,
updates establishment names to match, then rebuilds dashboard_v3.html.

Mapping:
  Community Center (65)   → 33 → Community Health Center (FQHC)
                            32 → Area Agency on Aging
  Habitat for Humanity (78) → Housing Support Organization
  Youth Services Org (26)   → SUD Treatment Program

Kept unchanged: Food Bank, Public Library, Senior Center
"""

import csv, os, random, subprocess, sys, shutil

BASE = os.path.dirname(os.path.abspath(__file__))
random.seed(77777)

# ── Name keyword map: old type keyword → new type keyword (for name replacement) ──
TYPE_MAP = {
    'Community Health Center (FQHC)': 'Community Health Center',
    'Area Agency on Aging':           'Area Agency on Aging',
    'Housing Support Organization':   'Housing Support Organization',
    'SUD Treatment Program':          'SUD Treatment Program',
}

# ── Read establishments ────────────────────────────────────────────────────────
print("Reading dim_establishment.csv...")
with open(os.path.join(BASE, 'dim_establishment.csv'), newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    FIELDS = reader.fieldnames
    rows   = list(reader)

# Collect rows by old type
community_centers  = [r for r in rows if r['establishment_type'] == 'Community Center']
habitats           = [r for r in rows if r['establishment_type'] == 'Habitat for Humanity']
youth_orgs         = [r for r in rows if r['establishment_type'] == 'Youth Services Org']

print(f"  Community Center rows   : {len(community_centers)}")
print(f"  Habitat for Humanity rows: {len(habitats)}")
print(f"  Youth Services Org rows : {len(youth_orgs)}")

# ── Reassign Community Center → split between two new types ───────────────────
random.shuffle(community_centers)
split = len(community_centers) // 2
cc_fqhc = community_centers[:split]
cc_aaa  = community_centers[split:]

def reassign(row_list, new_type):
    keyword = TYPE_MAP[new_type]
    for r in row_list:
        old_name = r['establishment_name']
        # Replace old keyword in name (Community Center, Habitat for Humanity, Youth Services Org)
        for old_kw in ['Community Center', 'Habitat for Humanity', 'Youth Services Org']:
            if old_kw in old_name:
                r['establishment_name'] = old_name.replace(old_kw, keyword)
                break
        r['establishment_type'] = new_type
    return row_list

reassign(cc_fqhc, 'Community Health Center (FQHC)')
reassign(cc_aaa,  'Area Agency on Aging')
reassign(habitats, 'Housing Support Organization')
reassign(youth_orgs, 'SUD Treatment Program')

# ── Write updated CSV ──────────────────────────────────────────────────────────
print("\nWriting updated dim_establishment.csv...")
with open(os.path.join(BASE, 'dim_establishment.csv'), 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(rows)

# Verify counts
counts = {}
for r in rows:
    counts[r['establishment_type']] = counts.get(r['establishment_type'], 0) + 1
print("  New distribution:")
for t, n in sorted(counts.items(), key=lambda x: -x[1]):
    print(f"    {n:3d}  {t}")

# ── Rebuild dashboard ──────────────────────────────────────────────────────────
print("\nRebuilding dashboard...")
r = subprocess.run([sys.executable, os.path.join(BASE, 'build_staging.py')],
                   capture_output=True, text=True)
print(r.stdout)
if r.returncode != 0:
    print("ERROR:", r.stderr[:400])
    sys.exit(1)

shutil.copy(os.path.join(BASE, 'dashboard_staging.html'),
            os.path.join(BASE, 'dashboard_v3.html'))
print("Promoted staging → dashboard_v3.html")

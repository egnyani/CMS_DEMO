"""
build_production_preview.py
Reads dashboard_v3.html and produces production_preview.html with:
  1. Green "PRODUCTION PREVIEW" banner (replaces yellow demo notice)
  2. Data freshness strip below banner
  3. Updated page title + "● T-MSIS Connected" status dot
  4. Export CSV button in filter bar
  5. Realistic org names for top15_labels (JS post-processing)

Hard rule: NEVER modifies dashboard_v3.html or any CSV/builder files.
"""
import os, re

BASE = os.path.dirname(os.path.abspath(__file__))
SRC  = os.path.join(BASE, 'dashboard_v3.html')
DST  = os.path.join(BASE, 'production_preview.html')

with open(SRC, encoding='utf-8') as f:
    html = f.read()

# ── 1. Add new CSS (before </style>) ──────────────────────────────────────────
PREVIEW_CSS = """
/* ── Production Preview Additions ─────────────────────────────────────────── */
.preview-notice{background:#e8f5e9;border-bottom:2px solid #2e8540;padding:6px 20px;font-size:11px;color:#1a3c24;font-weight:600;letter-spacing:0.2px;display:flex;align-items:center;gap:8px;}
.preview-notice::before{content:"✓";font-size:14px;color:#2e8540;}
.freshness-strip{background:#f0f7ff;border-bottom:1px solid #b3d4f5;padding:4px 20px;font-size:10px;color:#1a4480;font-weight:600;letter-spacing:0.3px;display:flex;gap:16px;}
.status-dot{display:inline-flex;align-items:center;gap:5px;font-size:10px;color:rgba(255,255,255,0.85);font-weight:600;white-space:nowrap;padding-left:12px;border-left:1px solid rgba(255,255,255,0.2);margin-left:4px;}
.status-dot::before{content:"●";color:#4ade80;font-size:13px;line-height:1;}
.export-btn{background:#003a6b;border:1px solid rgba(255,255,255,0.35);color:#fff;padding:4px 12px;border-radius:2px;font-size:11px;cursor:pointer;font-weight:700;display:inline-flex;align-items:center;gap:5px;white-space:nowrap;}
.export-btn:hover{background:#1a4480;border-color:rgba(255,255,255,0.6);}
"""
html = html.replace('</style>', PREVIEW_CSS + '</style>', 1)

# ── 2. Replace demo banner with preview banner + freshness strip ───────────────
OLD_BANNER = '<div class="demo-notice">DEMONSTRATION DATA ONLY — This dashboard uses synthetic data for analytical methodology review. Not for official policy use.</div>'
NEW_BANNER = (
    '<div class="preview-notice">PRODUCTION PREVIEW — T-MSIS Data Integration Proposal &nbsp;·&nbsp; '
    'Demonstrates proposed production design; KPI values are based on synthetic data.</div>\n'
    '<div class="freshness-strip">'
    '<span>Data as of: Q4 2025</span>'
    '<span>|</span>'
    '<span>Refresh Cycle: Quarterly (T-MSIS TAF)</span>'
    '<span>|</span>'
    '<span>Next Update: Q1 2026</span>'
    '</div>'
)
html = html.replace(OLD_BANNER, NEW_BANNER, 1)

# ── 3. Update page title ────────────────────────────────────────────────────────
html = html.replace(
    '<title>T-MSIS Community Engagement Analytics | CMS</title>',
    '<title>T-MSIS Community Engagement Analytics — Production Preview | CMS</title>',
    1
)

# ── 4. Update h1 + add status dot ──────────────────────────────────────────────
OLD_H1 = '<h1>Medicaid Community Engagement — T-MSIS Analytics Dashboard</h1>'
NEW_H1 = ('<h1>T-MSIS Community Engagement Analytics — Production Preview</h1>'
           '<span class="status-dot">T-MSIS Connected</span>')
html = html.replace(OLD_H1, NEW_H1, 1)

# ── 5. Add Export button at end of filter bar ──────────────────────────────────
# The filter bar ends with the ageSelector </select> followed by </div> </div> </div>
OLD_FILTER_END = (
    '      <option value="65+">65+</option>\n'
    '      </select>\n'
    '    </div>\n'
    '  </div>\n'
    '</div>'
)
NEW_FILTER_END = (
    '      <option value="65+">65+</option>\n'
    '      </select>\n'
    '      <button class="export-btn" onclick="exportDashboardCSV()" title="Download current view as CSV">&#8595; Export</button>\n'
    '    </div>\n'
    '  </div>\n'
    '</div>'
)
html = html.replace(OLD_FILTER_END, NEW_FILTER_END, 1)

# ── 6. Insert JS: org name remapping + export function (before </script>) ──────
EXTRA_JS = r"""
// ── Production Preview: realistic org names ────────────────────────────────
(function remapOrgNames() {
  var STATE_CITY = {CA:'Los Angeles',FL:'Miami',GA:'Atlanta',IL:'Chicago',NY:'New York',OH:'Columbus',PA:'Philadelphia',TX:'Houston'};
  var TYPE_LABELS = {
    'Food Bank':              'Regional Food Bank',
    'Public Library':         'Public Library System',
    'Community Center':       'Community Services Center',
    'Habitat for Humanity':   'Habitat for Humanity',
    'Senior Center':          'Senior Resource Center',
    'Youth Services Org':     'Youth & Family Services'
  };
  function prettyName(label) {
    var m = label.match(/^([A-Z]{2})\s+(.+?)\s+(\d+)$/);
    if (!m) return label;
    var state = m[1], orgType = m[2];
    var city = STATE_CITY[state] || state;
    var pretty = TYPE_LABELS[orgType] || orgType;
    return city + ' ' + pretty;
  }
  for (var key in ALL_DATA) {
    var d = ALL_DATA[key];
    if (Array.isArray(d.top15_labels)) {
      var seen = {};
      d.top15_labels = d.top15_labels.map(function(l) {
        var name = prettyName(l);
        if (seen[name]) { name = name + ' – ' + (seen[name] + 1); }
        seen[name] = (seen[name] || 0) + 1;
        return name;
      });
    }
  }
})();

// ── Production Preview: CSV export ────────────────────────────────────────
function exportDashboardCSV() {
  var period = document.getElementById('periodSelector');
  var state  = document.getElementById('stateSelector');
  var age    = document.getElementById('ageSelector');
  var rows = [
    ['Metric', 'Value', 'Period', 'State', 'Age Group'],
    ['New Recipients Registered', document.getElementById('kpi-registered').textContent,
      period.options[period.selectedIndex].text,
      state.options[state.selectedIndex].text,
      age.options[age.selectedIndex].text || 'All Ages'],
    ['Total Activity Records',   document.getElementById('kpi-records').textContent],
    ['Avg Hours / Recipient',    document.getElementById('kpi-s1-avghrs').textContent],
    ['Recipients with 2+ Flags', document.getElementById('kpi-multi-flag').textContent],
    ['Overlapping Activity',     document.getElementById('kpi-overlap').textContent],
    ['Exactly 80 hrs for 3+ Mo', document.getElementById('kpi-ex80').textContent],
    ['Single Establishment Reliance', document.getElementById('kpi-medrisk').textContent],
  ];
  var csv = rows.map(function(r){ return r.map(function(c){ return '"'+(c||'').replace(/"/g,'""')+'"'; }).join(','); }).join('\n');
  var blob = new Blob([csv], {type:'text/csv'});
  var url  = URL.createObjectURL(blob);
  var a    = document.createElement('a');
  a.href   = url;
  a.download = 'cms_dashboard_export.csv';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}
"""

# Insert before last </script>
html = html[:html.rfind('</script>')] + EXTRA_JS + '\n</script>\n</body>\n</html>'
# Remove the old closing tags that were after the rfind point
# (The rfind found the last </script>, everything from there to end is replaced)
# But we need to be careful — rfind gives us the position of the LAST </script>
# The original ends with: ...updateFilters();\n...\n</script>\n</body>\n</html>
# After rfind of </script> we get just the index of the <, so everything from that index
# onwards in the original is replaced by our new ending.

with open(DST, 'w', encoding='utf-8') as f:
    f.write(html)

print(f"Written: {DST}")
print(f"Size: {len(html):,} bytes")

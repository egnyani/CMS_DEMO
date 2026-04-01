"""
repair_injection.py

Fixes the bad injection from patch_age_filter.py:
  - Removes the age-filtered entries that were wrongly placed inside const CONST
  - Re-injects them correctly inside ALL_DATA using brace-depth tracking
"""

import json, os, re

BASE      = os.path.dirname(os.path.abspath(__file__))
DASH_PATH = os.path.join(BASE, 'dashboard_v3.html')

with open(DASH_PATH, 'r', encoding='utf-8') as f:
    html = f.read()

print(f"Read {len(html)//1024}KB")

# ── Step 1: locate ALL_DATA's true closing brace via brace tracking ───────────
all_data_start = html.find('const ALL_DATA = {')
assert all_data_start != -1, "Cannot find const ALL_DATA"

pos = all_data_start + len('const ALL_DATA = {')
depth = 1
in_string = False
escape = False
while pos < len(html) and depth > 0:
    c = html[pos]
    if escape:
        escape = False
    elif c == '\\' and in_string:
        escape = True
    elif c == '"' and not escape:
        in_string = not in_string
    elif not in_string:
        if c == '{':
            depth += 1
        elif c == '}':
            depth -= 1
    pos += 1

all_data_close_pos = pos - 1   # points to the closing } of ALL_DATA
print(f"ALL_DATA closes at position {all_data_close_pos}")
print(f"Context: {repr(html[all_data_close_pos-20:all_data_close_pos+20])}")

# ── Step 2: locate the injected entries inside CONST ─────────────────────────
# They start just after "state_data: [...]," in CONST with a spurious ",\n"
# Look for the first age-key entry after ALL_DATA closes
inject_start_marker = ',\n"202603_all_0-18":'
inject_start = html.find(inject_start_marker, all_data_close_pos)
assert inject_start != -1, "Cannot find injected entries start"

# The injected block ends just before the } that closes what was originally CONST
# We find it by looking for the pattern: last age entry's closing }} followed by \n}
# The last injected key is last6_TX_65+
last_key_marker = '"last6_TX_65+":'
last_key_pos = html.rfind(last_key_marker, inject_start, inject_start + 6_000_000)
assert last_key_pos != -1, "Cannot find last injected entry"

# Find closing }} of that entry (end of the JSON value object)
# Scan forward from the last_key_pos to find depth-0 closing
scan = last_key_pos + len(last_key_marker)
# skip whitespace to {
while html[scan] in ' \t\n': scan += 1
assert html[scan] == '{', f"Expected {{ at {scan}, got {repr(html[scan])}"

depth2 = 1
scan += 1
in_s = esc2 = False
while scan < len(html) and depth2 > 0:
    c = html[scan]
    if esc2: esc2 = False
    elif c == '\\' and in_s: esc2 = True
    elif c == '"' and not esc2: in_s = not in_s
    elif not in_s:
        if c == '{': depth2 += 1
        elif c == '}': depth2 -= 1
    scan += 1
inject_entry_end = scan   # one past the closing } of last entry value

# The injected block in html is: inject_start .. inject_entry_end
# That is: ',\n"202603_all_0-18": {...}, ... "last6_TX_65+": {...}'
# Plus there should be '\n}' right after that ends the wrongly-placed block
# Consume that \n}
tail = html[inject_entry_end:]
# tail should start with '\n}' (the wrong closing brace we added)
if tail.startswith('\n}'):
    wrong_close_end = inject_entry_end + 2
elif tail.startswith('}'):
    wrong_close_end = inject_entry_end + 1
else:
    wrong_close_end = inject_entry_end

print(f"Injected block: {inject_start} .. {wrong_close_end}")
print(f"Context before: {repr(html[inject_start-30:inject_start])}")
print(f"Context after:  {repr(html[wrong_close_end:wrong_close_end+40])}")

# Extract the new entries JSON (without the leading ",\n" and without trailing garbage)
injected_json_raw = html[inject_start + len(',\n') : inject_entry_end]
print(f"Extracted {len(injected_json_raw)//1024}KB of new entries")

# Verify it parses by wrapping in {}
try:
    test = json.loads('{' + injected_json_raw + '}')
    print(f"Injected JSON valid: {len(test)} keys")
except Exception as e:
    print(f"Injected JSON parse error: {e}")

# ── Step 3: remove injected block from CONST area ────────────────────────────
# Replace the region [inject_start, wrong_close_end] with nothing
# This restores CONST to its original form (trailing comma already on state_data line)
# We need the CONST closing }; back: look at what's left after wrong_close_end
# The original had: ...state_data:[...],\n};\n\nconst OVERLAP_STATE_NORMAL
# After injection: ...state_data:[...],\n,\n<entries>\n}\n\nconst OVERLAP_STATE_NORMAL
# Removing [inject_start, wrong_close_end]:
# html[:inject_start] + html[wrong_close_end:]
# html[:inject_start] ends with: ...state_data:[...],\n
# html[wrong_close_end:] starts with: \n\nconst OVERLAP_STATE_NORMAL
# We need to re-add "};" between them

html_repaired = html[:inject_start] + '};\n' + html[wrong_close_end:].lstrip('\n')

# ── Step 4: re-inject new entries correctly inside ALL_DATA ──────────────────
# Find ALL_DATA's closing } in the repaired HTML using brace tracking again
ad_start2 = html_repaired.find('const ALL_DATA = {')
pos2 = ad_start2 + len('const ALL_DATA = {')
depth3 = 1
in_s3 = esc3 = False
while pos2 < len(html_repaired) and depth3 > 0:
    c = html_repaired[pos2]
    if esc3: esc3 = False
    elif c == '\\' and in_s3: esc3 = True
    elif c == '"' and not esc3: in_s3 = not in_s3
    elif not in_s3:
        if c == '{': depth3 += 1
        elif c == '}': depth3 -= 1
    pos2 += 1
ad_close2 = pos2 - 1
print(f"ALL_DATA closing brace in repaired html at: {ad_close2}")
print(f"Context: {repr(html_repaired[ad_close2-30:ad_close2+10])}")

# Insert before the closing }
html_final = (
    html_repaired[:ad_close2]
    + ',\n'
    + injected_json_raw
    + '\n'
    + html_repaired[ad_close2:]
)

# ── Step 5: validate ──────────────────────────────────────────────────────────
# Quick sanity: ALL_DATA should now contain the new keys
assert '"202603_all_0-18"' in html_final, "Missing new key after repair!"
assert '"last6_TX_65+"' in html_final, "Missing last key after repair!"
assert 'const CONST = {' in html_final, "CONST block missing!"

# Verify ALL_DATA JSON is valid
ad_start3 = html_final.find('const ALL_DATA = {')
# find end via brace tracking
pos3 = ad_start3 + len('const ALL_DATA = {')
depth4 = 1; in_s4 = esc4 = False
while pos3 < len(html_final) and depth4 > 0:
    c = html_final[pos3]
    if esc4: esc4 = False
    elif c == '\\' and in_s4: esc4 = True
    elif c == '"' and not esc4: in_s4 = not in_s4
    elif not in_s4:
        if c == '{': depth4 += 1
        elif c == '}': depth4 -= 1
    pos3 += 1
ad_close3 = pos3 - 1
json_str = html_final[ad_start3 + len('const ALL_DATA = '):ad_close3 + 1]
try:
    data = json.loads(json_str)
    print(f"ALL_DATA JSON valid: {len(data)} keys")
    age_keys = [k for k in data if '_0-18' in k or '_19-64' in k or '_65+' in k]
    print(f"  Age-filtered keys: {len(age_keys)}")
except Exception as e:
    print(f"ALL_DATA JSON ERROR: {e}")

# ── Step 6: write ─────────────────────────────────────────────────────────────
with open(DASH_PATH, 'w', encoding='utf-8') as f:
    f.write(html_final)
print(f"Written: {DASH_PATH}  ({len(html_final)//1024}KB)")

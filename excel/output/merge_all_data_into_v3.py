#!/usr/bin/env python3
"""Replace const ALL_DATA = {...} in dashboard_v3.html with the blob from dashboard_real.html."""
import os

BASE = os.path.dirname(os.path.abspath(__file__))


def extract_json_object_after(s, marker):
    i = s.find(marker)
    if i < 0:
        raise ValueError(f"marker not found: {marker!r}")
    i = s.find("{", i)
    depth = 0
    start = i
    while i < len(s):
        c = s[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return s[start : i + 1]
        i += 1
    raise ValueError("unbalanced braces")


def main():
    real_path = os.path.join(BASE, "dashboard_real.html")
    v3_path = os.path.join(BASE, "dashboard_v3.html")
    with open(real_path, encoding="utf-8") as f:
        dr = f.read()
    obj = extract_json_object_after(dr, "const ALL_DATA = ")
    if '"202603_all_0-18"' not in dr:
        print("WARNING: expected age keys not found in dashboard_real.html")

    with open(v3_path, encoding="utf-8") as f:
        v3 = f.read()

    start = v3.find("const ALL_DATA = ")
    if start < 0:
        raise ValueError("dashboard_v3.html has no ALL_DATA")
    brace = v3.find("{", start)
    depth = 0
    i = brace
    while i < len(v3):
        c = v3[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                if end < len(v3) and v3[end] == ";":
                    end += 1
                break
        i += 1
    else:
        raise ValueError("could not find end of ALL_DATA in v3")

    new_v3 = v3[:start] + "const ALL_DATA = " + obj + ";" + v3[end:]
    with open(v3_path, "w", encoding="utf-8") as f:
        f.write(new_v3)
    print("OK: merged ALL_DATA into dashboard_v3.html (", len(obj), "chars)")


if __name__ == "__main__":
    main()

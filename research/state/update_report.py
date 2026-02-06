#!/usr/bin/env python3
"""Update STATE_ANALYSIS_REPORT.md: sort leaderboard by best speedup across all sources."""

import json
from pathlib import Path

PROJECT = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
lb = json.loads((PROJECT / "research/state/leaderboard.json").read_text())
report = (PROJECT / "research/state/STATE_ANALYSIS_REPORT.md").read_text()

# Build DSR1 lookup
dsr1 = {}
for q in lb["queries"]:
    qnum = int(q["query"].replace("q", ""))
    dsr1[qnum] = q

# Parse existing table rows
lines = report.split("\n")
rows = []
in_table = False
for line in lines:
    if "| Rank | Query |" in line:
        in_table = True
        continue
    if in_table and line.startswith("|---"):
        continue
    if in_table and line.startswith("|"):
        parts = [p.strip() for p in line.split("|")]
        if len(parts) >= 8:
            query = parts[2]
            runtime_str = parts[3]
            prior_str = parts[4]
            savings_str = parts[7]

            qnum = int(query.replace("Q", ""))
            runtime_ms = int(runtime_str.replace("ms", ""))
            try:
                prior = float(prior_str.replace("x", ""))
            except Exception:
                prior = 1.0

            d = dsr1.get(qnum, {})
            dsr1_sp = d.get("speedup", 0)
            dsr1_st = d.get("status", "-")

            best = max(prior, dsr1_sp)

            if best >= 1.1:
                best_status = "WIN"
            elif best >= 1.05:
                best_status = "IMPROVED"
            elif best >= 0.95:
                best_status = "NEUTRAL"
            else:
                best_status = "REGRESSION"

            star = " ⭐ TOP 20" if "⭐" in savings_str else ""
            savings_ms = runtime_ms // 2

            rows.append({
                "qnum": qnum,
                "runtime_ms": runtime_ms,
                "runtime_str": runtime_str,
                "prior": prior,
                "dsr1_sp": dsr1_sp,
                "dsr1_st": dsr1_st,
                "best": best,
                "best_status": best_status,
                "savings_ms": savings_ms,
                "star": star,
            })
    elif in_table and not line.startswith("|"):
        in_table = False

# Sort by best speedup descending
rows.sort(key=lambda r: r["best"], reverse=True)

# Rebuild table
table_lines = []
table_lines.append("| Rank | Query | Runtime | Best | Prior | DSR1 | Status | Savings @2x |")
table_lines.append("|------|-------|---------|------|-------|------|--------|-------------|")
for i, r in enumerate(rows):
    rank = i + 1
    dsr1_str = f"{r['dsr1_sp']:.2f}x" if r["dsr1_sp"] else "-"
    table_lines.append(
        f"| {rank} | Q{r['qnum']} | {r['runtime_str']} | {r['best']:.2f}x | {r['prior']:.2f}x | {dsr1_str} | {r['best_status']} | {r['savings_ms']}ms{r['star']} |"
    )

# Replace table in report
new_lines = []
skip_table = False
for line in lines:
    if "| Rank | Query |" in line:
        skip_table = True
        for tl in table_lines:
            new_lines.append(tl)
        continue
    if skip_table and line.startswith("|"):
        continue
    if skip_table and not line.startswith("|"):
        skip_table = False
    new_lines.append(line)

(PROJECT / "research/state/STATE_ANALYSIS_REPORT.md").write_text("\n".join(new_lines))

print(f"Done - {len(rows)} queries sorted by best speedup")
for r in rows[:10]:
    print(f"  Q{r['qnum']}: {r['best']:.2f}x (prior={r['prior']:.2f}x, dsr1={r['dsr1_sp']:.2f}x) {r['runtime_str']}")

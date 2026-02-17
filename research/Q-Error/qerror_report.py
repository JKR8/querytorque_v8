"""Generate Q-Error Engineering Report (charts + tables → PDF-ready markdown + PNGs)."""

import json
import csv
from pathlib import Path
from collections import Counter, defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# ── Config ──────────────────────────────────────────────────────────────
DATA = Path(__file__).parent.parent / "packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds/qerror_analysis.json"
OUT = Path(__file__).parent / "report"
OUT.mkdir(exist_ok=True)

COLORS = {
    "CATASTROPHIC_BLINDNESS": "#d32f2f",
    "MAJOR_HALLUCINATION": "#f57c00",
    "MODERATE_GUESS": "#fbc02d",
    "MINOR_DRIFT": "#81c784",
    "ACCURATE": "#4caf50",
    "NO_JSON": "#bdbdbd",
}
DIR_COLORS = {"OVER_EST": "#1976d2", "UNDER_EST": "#d32f2f", "ZERO_EST": "#7b1fa2", "ACCURATE": "#4caf50"}
STATUS_COLORS = {"WIN": "#2e7d32", "IMPROVED": "#66bb6a", "NEUTRAL": "#9e9e9e", "REGRESSION": "#ef5350", "ERROR": "#bdbdbd"}

rows = json.loads(DATA.read_text())

# ── Figure 1: Severity distribution (pie) ───────────────────────────────
sev_counts = Counter(r["severity"] for r in rows)
labels = ["CATASTROPHIC_BLINDNESS", "MAJOR_HALLUCINATION", "MODERATE_GUESS", "MINOR_DRIFT", "ACCURATE", "NO_JSON"]
labels = [l for l in labels if sev_counts.get(l, 0) > 0]
sizes = [sev_counts[l] for l in labels]
colors = [COLORS[l] for l in labels]
short = [l.replace("_", "\n").title() for l in labels]

fig, ax = plt.subplots(figsize=(6, 4.5))
wedges, texts, autotexts = ax.pie(
    sizes, labels=short, autopct=lambda p: f"{p:.0f}%\n({int(p*sum(sizes)/100)})",
    colors=colors, startangle=90, textprops={"fontsize": 9},
)
for t in autotexts:
    t.set_fontsize(8)
ax.set_title("Q-Error Severity Distribution\n(101 TPC-DS queries, DuckDB SF10)", fontsize=11, fontweight="bold")
fig.tight_layout()
fig.savefig(OUT / "fig1_severity_pie.png", dpi=150, bbox_inches="tight")
plt.close()

# ── Figure 2: Max Q-Error per query (log bar, colored by benchmark outcome) ──
valid = [(r["query_id"], r["max_q_error"], r.get("benchmark_status", ""))
         for r in rows if r["max_q_error"] and r["max_q_error"] > 1]
valid.sort(key=lambda x: -x[1])

fig, ax = plt.subplots(figsize=(14, 4))
x = range(len(valid))
bar_colors = [STATUS_COLORS.get(v[2], "#bdbdbd") for v in valid]
ax.bar(x, [v[1] for v in valid], color=bar_colors, width=0.8)
ax.set_yscale("log")
ax.set_ylabel("Max Q-Error (log)")
ax.set_title("Max Q-Error per Query (colored by optimization outcome)", fontsize=11, fontweight="bold")
ax.set_xticks(x)
ax.set_xticklabels([v[0].replace("query_", "Q") for v in valid], rotation=90, fontsize=5.5)
# Horizontal threshold lines
ax.axhline(y=10000, color="#d32f2f", linestyle="--", alpha=0.5, linewidth=0.8)
ax.axhline(y=100, color="#f57c00", linestyle="--", alpha=0.5, linewidth=0.8)
ax.axhline(y=10, color="#fbc02d", linestyle="--", alpha=0.5, linewidth=0.8)
ax.text(len(valid)+0.5, 10000, "CATASTROPHIC", fontsize=7, color="#d32f2f", va="center")
ax.text(len(valid)+0.5, 100, "MAJOR", fontsize=7, color="#f57c00", va="center")
ax.text(len(valid)+0.5, 10, "MODERATE", fontsize=7, color="#fbc02d", va="center")
# Legend
from matplotlib.patches import Patch
legend_elements = [Patch(facecolor=c, label=l) for l, c in STATUS_COLORS.items() if l in {v[2] for v in valid}]
ax.legend(handles=legend_elements, loc="upper right", fontsize=7, title="Benchmark outcome", title_fontsize=8)
fig.tight_layout()
fig.savefig(OUT / "fig2_qerror_per_query.png", dpi=150, bbox_inches="tight")
plt.close()

# ── Figure 3: Direction × Locus heatmap ─────────────────────────────────
dir_locus = defaultdict(lambda: defaultdict(int))
for r in rows:
    if r["direction"] and r["locus"]:
        dir_locus[r["direction"]][r["locus"]] += 1

directions = ["OVER_EST", "UNDER_EST", "ZERO_EST"]
loci = ["PROJECTION", "FILTER", "JOIN", "SCAN", "AGGREGATE", "CTE"]
matrix = [[dir_locus[d].get(l, 0) for l in loci] for d in directions]

fig, ax = plt.subplots(figsize=(7, 3))
im = ax.imshow(matrix, cmap="YlOrRd", aspect="auto")
ax.set_xticks(range(len(loci)))
ax.set_xticklabels(loci, fontsize=9)
ax.set_yticks(range(len(directions)))
ax.set_yticklabels(directions, fontsize=9)
for i in range(len(directions)):
    for j in range(len(loci)):
        v = matrix[i][j]
        if v > 0:
            ax.text(j, i, str(v), ha="center", va="center", fontsize=10, fontweight="bold",
                    color="white" if v > 8 else "black")
ax.set_title("Direction × Locus Distribution (query count)", fontsize=11, fontweight="bold")
fig.colorbar(im, ax=ax, shrink=0.8)
fig.tight_layout()
fig.savefig(OUT / "fig3_direction_locus.png", dpi=150, bbox_inches="tight")
plt.close()

# ── Figure 4: Structural flags frequency ────────────────────────────────
flag_counts = Counter()
for r in rows:
    if r["structural_flags"]:
        for f in r["structural_flags"].split("|"):
            if f:
                flag_counts[f] += 1

fig, ax = plt.subplots(figsize=(6, 3))
items = flag_counts.most_common()
ax.barh([i[0] for i in items], [i[1] for i in items], color="#5c6bc0")
ax.set_xlabel("Queries with flag")
ax.set_title("Structural Signal Frequency\n(free signals, no execution needed)", fontsize=11, fontweight="bold")
for i, (name, count) in enumerate(items):
    ax.text(count + 0.5, i, str(count), va="center", fontsize=9)
fig.tight_layout()
fig.savefig(OUT / "fig4_structural_flags.png", dpi=150, bbox_inches="tight")
plt.close()

# ── Figure 5: Pathology routing coverage ────────────────────────────────
path_counts = Counter()
for r in rows:
    if r["pathology_routing"]:
        for p in r["pathology_routing"].split(","):
            if p:
                path_counts[p.strip()] += 1

fig, ax = plt.subplots(figsize=(6, 3.5))
pathologies = [f"P{i}" for i in range(10)]
counts = [path_counts.get(p, 0) for p in pathologies]
bars = ax.bar(pathologies, counts, color="#26a69a")
ax.set_ylabel("Queries routed")
ax.set_title("Pathology Routing from Q-Error\n(how many queries each pathology covers)", fontsize=11, fontweight="bold")
for bar, c in zip(bars, counts):
    if c > 0:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, str(c),
                ha="center", fontsize=9)
fig.tight_layout()
fig.savefig(OUT / "fig5_pathology_routing.png", dpi=150, bbox_inches="tight")
plt.close()

# ── Figure 6: Q-Error vs Speedup scatter ────────────────────────────────
scatter_data = [(r["max_q_error"], r["benchmark_speedup"], r["benchmark_status"], r["query_id"])
                for r in rows if r["max_q_error"] and r["max_q_error"] > 1 and r["benchmark_speedup"]]

fig, ax = plt.subplots(figsize=(7, 5))
for qe, sp, st, qid in scatter_data:
    c = STATUS_COLORS.get(st, "#bdbdbd")
    ax.scatter(qe, sp, c=c, s=30, alpha=0.7, edgecolors="none")
    if sp > 2.5 or qe > 100000:
        ax.annotate(qid.replace("query_", "Q"), (qe, sp), fontsize=6, alpha=0.8,
                    xytext=(4, 4), textcoords="offset points")
ax.set_xscale("log")
ax.axhline(y=1.0, color="gray", linestyle="-", alpha=0.3)
ax.axhline(y=1.1, color="#2e7d32", linestyle="--", alpha=0.3, linewidth=0.8)
ax.set_xlabel("Max Q-Error (log)")
ax.set_ylabel("Optimization Speedup (x)")
ax.set_title("Q-Error vs Achieved Speedup", fontsize=11, fontweight="bold")
legend_elements = [Patch(facecolor=c, label=l) for l, c in STATUS_COLORS.items()
                   if l in {s[2] for s in scatter_data}]
ax.legend(handles=legend_elements, fontsize=7, title="Outcome", title_fontsize=8)
fig.tight_layout()
fig.savefig(OUT / "fig6_qerror_vs_speedup.png", dpi=150, bbox_inches="tight")
plt.close()

# ── Generate markdown report ────────────────────────────────────────────
md = []
md.append("# Q-Error Analysis Report: DuckDB TPC-DS SF10")
md.append("")
md.append("**101 queries | DuckDB 1.4.3 | EXPLAIN (ANALYZE, FORMAT JSON)**")
md.append("")
md.append("## Key Finding")
md.append("")
md.append("Q-Error (cardinality estimation error) identifies **where** and **how** the DuckDB")
md.append("optimizer misjudges row counts. This is directly actionable: each (locus, direction)")
md.append("pair maps to specific rewrite pathologies (P0-P9).")
md.append("")

# Coverage table
md.append("## 1. Coverage")
md.append("")
md.append(f"| Metric | Value |")
md.append(f"|--------|-------|")
md.append(f"| Queries with Q-Error signals | **{sum(1 for r in rows if r['severity'] not in ('NO_JSON', 'ACCURATE'))}** / 101 |")
md.append(f"| Missing plan_json (DuckDB limitation) | {sum(1 for r in rows if r['severity'] == 'NO_JSON')} |")
md.append(f"| Accurate (no misestimates Q>2) | {sum(1 for r in rows if r['severity'] == 'ACCURATE')} |")
md.append("")

# Severity table
md.append("## 2. Severity Distribution")
md.append("")
md.append("![Severity](report/fig1_severity_pie.png)")
md.append("")
md.append("| Severity | Count | % | Meaning |")
md.append("|----------|------:|--:|---------|")
for sev in ["CATASTROPHIC_BLINDNESS", "MAJOR_HALLUCINATION", "MODERATE_GUESS", "MINOR_DRIFT", "ACCURATE"]:
    c = sev_counts.get(sev, 0)
    pct = c * 100 // max(sum(sev_counts.values()), 1)
    meaning = {
        "CATASTROPHIC_BLINDNESS": "Q > 10,000 — planner off by 4+ orders of magnitude",
        "MAJOR_HALLUCINATION": "Q 100-10K — planner off by 2-4 orders",
        "MODERATE_GUESS": "Q 10-100 — planner off by 1-2 orders",
        "MINOR_DRIFT": "Q 2-10 — small but measurable mismatch",
        "ACCURATE": "Q < 2 — estimates are reasonable",
    }[sev]
    md.append(f"| {sev} | {c} | {pct}% | {meaning} |")
md.append("")

# Per-query bar chart
md.append("## 3. Q-Error per Query")
md.append("")
md.append("![Q-Error per query](report/fig2_qerror_per_query.png)")
md.append("")
md.append("Bar color = benchmark optimization outcome (green = WIN, red = REGRESSION).")
md.append("Dashed lines mark severity thresholds.")
md.append("")

# Direction x Locus
md.append("## 4. Where is the Optimizer Wrong? (Direction × Locus)")
md.append("")
md.append("![Direction × Locus](report/fig3_direction_locus.png)")
md.append("")
md.append("**How to read**: Each cell = number of queries where the worst Q-Error is at that")
md.append("(direction, locus) combination.")
md.append("")
md.append("| Direction | Meaning | Count | Actionable signal |")
md.append("|-----------|---------|------:|-------------------|")
dir_counts = Counter(r["direction"] for r in rows if r["direction"] and r["direction"] != "ACCURATE")
md.append(f"| OVER_EST | Planner thinks more rows than actual | {dir_counts.get('OVER_EST', 0)} | Redundant work, missed pruning → P1, P4, P7 |")
md.append(f"| UNDER_EST | Planner thinks fewer rows than actual | {dir_counts.get('UNDER_EST', 0)} | Under-provisioned join/scan → P2, P0 |")
md.append(f"| ZERO_EST | Planner estimated 0 (gave up) | {dir_counts.get('ZERO_EST', 0)} | CTE boundary blocks stats → P0, P2, P7 |")
md.append("")
md.append("| Locus | Where the worst mismatch is | Count | Typical pathology |")
md.append("|-------|---------------------------|------:|-------------------|")
locus_counts = Counter(r["locus"] for r in rows if r["locus"])
for l in ["PROJECTION", "FILTER", "JOIN", "SCAN", "AGGREGATE", "CTE"]:
    routing = {
        "PROJECTION": "P7 (CTE split), P0 (pushback), P4 (OR decomp)",
        "FILTER": "P9 (shared expr), P0 (pushback)",
        "JOIN": "P2 (decorrelate), P0 (pushback), P5 (LEFT→INNER)",
        "SCAN": "P1 (repeated scans), P4 (OR decomp), P2 (DELIM_SCAN)",
        "AGGREGATE": "P3 (agg below join)",
        "CTE": "P0 (pushback), P7 (CTE split), P2 (decorrelate)",
    }[l]
    md.append(f"| {l} | Worst Q-Error on {l.lower()} node | {locus_counts.get(l, 0)} | {routing} |")
md.append("")

# Structural flags
md.append("## 5. Structural Signals (Free — No Execution Needed)")
md.append("")
md.append("![Structural flags](report/fig4_structural_flags.png)")
md.append("")
md.append("| Flag | Queries | Meaning | Routes to |")
md.append("|------|--------:|---------|-----------|")
flag_desc = {
    "EST_ZERO": ("Planner estimated 0 rows on non-trivial node", "P0, P7"),
    "EST_ONE_NONLEAF": ("Planner estimated 1 row on non-leaf — guessing", "P2, P0"),
    "DELIM_SCAN": ("Correlated subquery marker (optimizer couldn't decorrelate)", "P2"),
    "LEFT_JOIN": ("LEFT JOIN present (INNER conversion candidate)", "P5"),
    "REPEATED_TABLE": ("Same table scanned 2+ times", "P1"),
    "INTERSECT_EXCEPT": ("INTERSECT/EXCEPT operator (EXISTS candidate)", "P6"),
}
for name, count in flag_counts.most_common():
    desc, routes = flag_desc.get(name, (name, ""))
    md.append(f"| {name} | {count} | {desc} | {routes} |")
md.append("")

# Pathology routing
md.append("## 6. Pathology Routing from Q-Error")
md.append("")
md.append("![Pathology routing](report/fig5_pathology_routing.png)")
md.append("")
md.append("| Pathology | Description | Queries routed | Safety |")
md.append("|-----------|-------------|---------------:|--------|")
path_desc = {
    "P0": ("Predicate chain pushback", "Safe with gates"),
    "P1": ("Repeated scans → single pass", "Zero regressions"),
    "P2": ("Correlated subquery decorrelation", "Check EXISTS first"),
    "P3": ("Aggregate below join", "Zero regressions"),
    "P4": ("Cross-column OR decomposition", "Max 3 branches"),
    "P5": ("LEFT JOIN → INNER conversion", "Zero regressions"),
    "P6": ("INTERSECT → EXISTS", "Zero regressions"),
    "P7": ("Self-joined CTE split", "Check orphan CTE"),
    "P8": ("Deferred window aggregation", "Zero regressions"),
    "P9": ("Shared subexpression extraction", "Never on EXISTS"),
}
for p in [f"P{i}" for i in range(10)]:
    desc, safety = path_desc[p]
    c = path_counts.get(p, 0)
    md.append(f"| {p} | {desc} | {c} | {safety} |")
md.append("")

# Correlation with benchmark outcomes
md.append("## 7. Q-Error vs Optimization Outcomes")
md.append("")
md.append("![Q-Error vs Speedup](report/fig6_qerror_vs_speedup.png)")
md.append("")

# Win rate by severity
md.append("| Severity | Queries | Wins | Win Rate | Avg Speedup (wins) |")
md.append("|----------|--------:|-----:|---------:|-------------------:|")
for sev in ["CATASTROPHIC_BLINDNESS", "MAJOR_HALLUCINATION", "MODERATE_GUESS", "MINOR_DRIFT"]:
    sev_rows = [r for r in rows if r["severity"] == sev and r["benchmark_status"]]
    wins = [r for r in sev_rows if r["benchmark_status"] == "WIN"]
    avg_sp = sum(r["benchmark_speedup"] for r in wins) / max(len(wins), 1)
    wr = len(wins) * 100 // max(len(sev_rows), 1)
    md.append(f"| {sev} | {len(sev_rows)} | {len(wins)} | {wr}% | {avg_sp:.2f}x |")
md.append("")

# Top Q-Error queries with outcomes
md.append("## 8. Top 15 Highest Q-Error Queries")
md.append("")
md.append("| Query | Max Q-Error | Locus | Direction | Routing | Outcome | Speedup |")
md.append("|-------|------------:|-------|-----------|---------|---------|--------:|")
top = sorted([r for r in rows if r["max_q_error"]], key=lambda r: -r["max_q_error"])[:15]
for r in top:
    qe = f"{r['max_q_error']:,.0f}" if r["max_q_error"] else ""
    md.append(f"| {r['query_id']} | {qe} | {r['locus']} | {r['direction']} | {r['pathology_routing']} | {r['benchmark_status']} | {r['benchmark_speedup']:.2f}x |")
md.append("")

# Actionable routing table
md.append("## 9. Actionable Routing Table")
md.append("")
md.append("This is the decision table that maps Q-Error signals to interventions:")
md.append("")
md.append("| Q-Error Locus | Direction | Intervention | Transform | Example wins |")
md.append("|---------------|-----------|-------------|-----------|-------------|")
md.append("| JOIN | UNDER_EST | Decorrelate to CTE + hash join | `decorrelate` | Q1 2.92x, Q35 2.42x |")
md.append("| JOIN | ZERO_EST | Push predicate into CTE | `predicate_pushback` | Q6 4.00x, Q11 4.00x |")
md.append("| JOIN | OVER_EST | Convert LEFT→INNER if WHERE on right | `inner_join_conversion` | Q93 3.44x |")
md.append("| SCAN | OVER_EST | Consolidate repeated scans | `single_pass_agg` | Q88 6.24x, Q9 4.47x |")
md.append("| SCAN | ZERO_EST | Decorrelate (DELIM_SCAN) | `decorrelate` | Q35 2.42x |")
md.append("| AGGREGATE | OVER_EST | Push GROUP BY below join | `aggregate_pushdown` | Q22 42.90x |")
md.append("| CTE | ZERO_EST | Push selective predicate into CTE | `date_cte_isolate` | Q63 3.77x |")
md.append("| PROJECTION | OVER_EST | Split self-joined CTE | `self_join_decomp` | Q39 4.76x |")
md.append("| PROJECTION | UNDER_EST | Replace INTERSECT with EXISTS | `intersect_to_exists` | Q14 2.72x |")
md.append("")

md.append("---")
md.append("")
md.append("*Generated by `qerror_report.py` from EXPLAIN (ANALYZE, FORMAT JSON) on 101 TPC-DS queries.*")

report_path = Path(__file__).parent / "QERROR_REPORT.md"
report_path.write_text("\n".join(md))
print(f"Report: {report_path}")
print(f"Figures: {OUT}/")

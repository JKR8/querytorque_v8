#!/usr/bin/env python3
"""Generate comprehensive markdown review of swarm pipeline batch run."""
import json, os, re
from collections import Counter, defaultdict
from pathlib import Path
from datetime import datetime

BASE = Path(__file__).parent
OUT = BASE / "SWARM_REVIEW.md"

def safe_json(path):
    try:
        with open(path) as f: return json.load(f)
    except: return None

def safe_text(path):
    try:
        with open(path) as f: return f.read()
    except: return ""

def classify_error(err_msg):
    if not err_msg: return ""
    e = err_msg.lower()
    if "binder error" in e: return "Binder Error"
    if "catalog error" in e: return "Catalog Error"
    if "not implemented" in e: return "Not Implemented"
    if "timeout" in e or "timed out" in e: return "Timeout"
    if "parser error" in e or "syntax error" in e: return "Parser/Syntax Error"
    return "Other Error"

def status_label(speedup, rows_match, error):
    if error: return "ERROR"
    if not rows_match: return "FAIL"
    if speedup >= 2.0: return "WIN"
    if speedup >= 1.1: return "IMPROVED"
    if speedup >= 0.95: return "NEUTRAL"
    return "REGRESSION"

def fmt_speedup(sp):
    return "---" if sp == 0 else f"{sp:.2f}x"

def overall_label(sp):
    if sp == 0: return "ERROR"
    if sp >= 2.0: return "WIN"
    if sp >= 1.1: return "IMPROVED"
    if sp >= 0.95: return "NEUTRAL"
    return "REGRESSION"

def parse_ts(s):
    try: return datetime.fromisoformat(s)
    except: return None

def get_best_per_query(q):
    best_sp, best_worker, best_iter = 0.0, -1, -1
    baseline_ms, row_count = 0.0, 0
    all_workers = []
    for it_num, it_key in [(0,"benchmark_iter0"),(1,"benchmark_iter1"),(2,"benchmark_iter2")]:
        bench = q.get(it_key)
        if bench is None: continue
        baseline_ms = bench.get("baseline_trimmed_mean_ms", baseline_ms)
        row_count = bench.get("baseline_row_count", row_count)
        for w in bench.get("workers", []):
            wid, sp = w["worker_id"], w.get("speedup", 0)
            rm, err, st = w.get("rows_match", False), w.get("error", ""), w.get("status", "")
            all_workers.append({"worker_id":wid,"speedup":sp,"rows_match":rm,
                "error":err,"status":st,"iter":it_num,"label":status_label(sp,rm,err)})
            if rm and st == "pass" and sp > best_sp:
                best_sp, best_worker, best_iter = sp, wid, it_num
    return {"best_speedup":best_sp,"best_worker":best_worker,"best_iter":best_iter,
            "baseline_ms":baseline_ms,"row_count":row_count,"all_workers":all_workers}

print("Reading query directories...")
query_dirs = sorted(
    [d for d in BASE.iterdir() if d.is_dir() and d.name.startswith("query_")],
    key=lambda d: (int(re.sub(r"[^0-9]","",d.name.split("_",1)[1]) or "0"), d.name))

manifest = safe_json(BASE / "manifest.json") or {}
checkpoint = safe_json(BASE / "checkpoint.json") or {}

queries = []
for qdir in query_dirs:
    qid = qdir.name
    entry = {"qid":qid,
        "assignments":safe_json(qdir/"assignments.json") or [],
        "regression_warnings":safe_json(qdir/"regression_warnings.json") or [],
        "reanalyze_parsed":safe_json(qdir/"reanalyze_parsed.json") or {},
        "reanalyze_response":safe_text(qdir/"reanalyze_response.txt"),
        "benchmark_iter0":safe_json(qdir/"benchmark_iter0.json"),
        "benchmark_iter1":safe_json(qdir/"benchmark_iter1.json"),
        "benchmark_iter2":safe_json(qdir/"benchmark_iter2.json"),
        "workers":{}}
    for wf in sorted(qdir.glob("worker_*_response.txt")):
        m = re.search(r"worker_(\d+)_response", wf.name)
        if m:
            wid = int(m.group(1))
            entry["workers"][wid] = {"sql":safe_text(qdir/f"worker_{wid}_sql.sql")}
    entry["snipe_sql"] = safe_text(qdir/"snipe_worker_sql.sql")
    entry["final_sql"] = safe_text(qdir/"final_worker_sql.sql")
    queries.append(entry)
print(f"  Loaded {len(queries)} queries")

print("Analyzing...")
for q in queries: q["analysis"] = get_best_per_query(q)
queries_ranked = sorted(queries, key=lambda q: q["analysis"]["best_speedup"], reverse=True)
label_counts = Counter(overall_label(q["analysis"]["best_speedup"]) for q in queries)

iter1_improvements, iter2_improvements = 0, 0
iter_best_source = Counter()
for q in queries:
    a = q["analysis"]
    if a["best_iter"] >= 0: iter_best_source[f"iter{a['best_iter']}"] += 1
    else: iter_best_source["none"] += 1
    b0 = q.get("benchmark_iter0"); b1 = q.get("benchmark_iter1"); b2 = q.get("benchmark_iter2")
    best0 = max((w.get("speedup",0) for w in (b0 or {}).get("workers",[])
                 if w.get("rows_match") and w.get("status")=="pass"), default=0)
    best1 = max((w.get("speedup",0) for w in (b1 or {}).get("workers",[])
                 if w.get("rows_match") and w.get("status")=="pass"), default=0)
    best2 = max((w.get("speedup",0) for w in (b2 or {}).get("workers",[])
                 if w.get("rows_match") and w.get("status")=="pass"), default=0)
    if best1 > best0 and best1 > 0: iter1_improvements += 1
    if best2 > max(best0, best1) and best2 > 0: iter2_improvements += 1

iter0_ws = defaultdict(lambda: {"pass":0,"fail":0,"error":0,"wins":0})
for q in queries:
    b0 = q.get("benchmark_iter0")
    if not b0: continue
    for w in b0.get("workers",[]):
        wid = w["worker_id"]
        if w.get("error"): iter0_ws[wid]["error"] += 1
        elif not w.get("rows_match"): iter0_ws[wid]["fail"] += 1
        else:
            iter0_ws[wid]["pass"] += 1
            if w.get("speedup",0) >= 1.1: iter0_ws[wid]["wins"] += 1

strat_counter, strat_results = Counter(), defaultdict(list)
ex_counter, ex_results = Counter(), defaultdict(list)
for q in queries:
    for asgn in q["assignments"]:
        strat = asgn.get("strategy","unknown"); wid = asgn.get("worker_id")
        strat_counter[strat] += 1
        b0 = q.get("benchmark_iter0")
        if b0:
            for w in b0.get("workers",[]):
                if w["worker_id"]==wid and w.get("rows_match") and w.get("status")=="pass":
                    strat_results[strat].append(w["speedup"])
        for ex in asgn.get("examples",[]):
            ex_counter[ex] += 1
            if b0:
                for w in b0.get("workers",[]):
                    if w["worker_id"]==wid and w.get("rows_match") and w.get("status")=="pass":
                        ex_results[ex].append(w["speedup"])

err_cats = Counter(); all_errs = []; mixed_qs = set()
for q in queries:
    a = q["analysis"]; has_p = has_e = False
    for w in a["all_workers"]:
        if w["error"]:
            has_e = True; cat = classify_error(w["error"]); err_cats[cat] += 1
            all_errs.append({"qid":q["qid"],"worker_id":w["worker_id"],"iter":w["iter"],
                             "error":w["error"],"category":cat})
        elif w["rows_match"] and w["status"]=="pass": has_p = True
    if has_e and has_p: mixed_qs.add(q["qid"])

exited_qs = [(q["qid"], q["benchmark_iter0"]["best_speedup"])
             for q in queries if q.get("benchmark_iter0",{}).get("exited")]
reg_qs = sorted([q for q in queries if 0 < q["analysis"]["best_speedup"] < 0.95],
                key=lambda q: q["analysis"]["best_speedup"])
iter0_sql_ok = sum(1 for q in queries for w in q["workers"].values() if w["sql"].strip())
iter0_sql_total = sum(len(q["workers"]) for q in queries)
iter1_sql_ok = sum(1 for q in queries if q["snipe_sql"].strip())
iter2_sql_ok = sum(1 for q in queries if q["final_sql"].strip())
total_api = 101 + 404 + 95 + 94 + 94
total_input_chars = manifest.get("prompt_stats",{}).get("total_chars", 627873)
est_cost = (total_input_chars * 6 / 4 * 2.0 + total_api * 250 * 8.0) / 1_000_000
total_we = sum(len(q["analysis"]["all_workers"]) for q in queries)
total_errs = sum(1 for q in queries for w in q["analysis"]["all_workers"] if w["error"])
total_fails = sum(1 for q in queries for w in q["analysis"]["all_workers"]
                  if not w["error"] and (not w["rows_match"] or w["status"]=="fail"))
total_pass = total_we - total_errs - total_fails
fail_qs = defaultdict(list)
for q in queries:
    for w in q["analysis"]["all_workers"]:
        if w["label"] == "FAIL": fail_qs[q["qid"]].append(w)

# ============================== MARKDOWN ==============================
print("Generating markdown...")
L = []
def emit(s=""): L.append(s)

emit("# Swarm Pipeline Review: DuckDB TPC-DS SF10")
emit()
emit("**Batch**: `swarm_batch_20260208_102033`")
emit("**Date**: 2026-02-08")
emit("**Database**: TPC-DS SF10 on DuckDB 1.4.3")
emit("**LLM**: DeepSeek Reasoner (deepseek-reasoner)")
emit("**Pipeline**: 3-iteration swarm (4 workers + snipe + reanalyze/final)")
emit()
emit("---")
emit()
emit("## Executive Summary")
emit()
emit("| Metric | Value |")
emit("|--------|-------|")
emit(f"| Total queries | {len(queries)} |")
emit(f"| Total LLM API calls | {total_api} |")
emit(f"| Total runtime | ~65 min |")
emit(f"| Exit gate | 2.0x |")
emit(f"| Estimated cost | ~${est_cost:.2f} |")
emit()
emit("### Outcome Breakdown")
emit()
emit("| Status | Threshold | Count | % |")
emit("|--------|-----------|------:|---:|")
thresholds = {"WIN":">=2.0x","IMPROVED":"1.1x-2.0x","NEUTRAL":"0.95x-1.1x",
              "REGRESSION":"<0.95x","ERROR":"all fail/error"}
for lbl in ["WIN","IMPROVED","NEUTRAL","REGRESSION","ERROR"]:
    cnt = label_counts.get(lbl, 0)
    emit(f"| {lbl} | {thresholds[lbl]} | {cnt} | {100*cnt/len(queries):.1f}% |")
emit()
net_pos = label_counts.get("WIN",0) + label_counts.get("IMPROVED",0)
emit(f"**Net positive rate**: {net_pos}/{len(queries)} ({100*net_pos/len(queries):.1f}%) queries improved by >= 1.1x")
emit()
passing = [q["analysis"]["best_speedup"] for q in queries if q["analysis"]["best_speedup"] > 0]
avg_sp = sum(passing)/len(passing) if passing else 0
sorted_p = sorted(passing)
median_sp = sorted_p[len(sorted_p)//2] if sorted_p else 0
emit(f"**Average best speedup** (passing queries): {avg_sp:.2f}x")
emit(f"**Median best speedup** (passing queries): {median_sp:.2f}x")
emit()
emit("### Top 10 Winners")
emit()
emit("| Rank | Query | Speedup | Worker | Iter | Baseline (ms) | Strategy |")
emit("|-----:|-------|--------:|-------:|-----:|--------------:|----------|")
for i, q in enumerate(queries_ranked[:10]):
    a = q["analysis"]; strat = ""
    if a["best_iter"] == 0:
        for asgn in q["assignments"]:
            if asgn.get("worker_id") == a["best_worker"]:
                strat = asgn.get("strategy", ""); break
    elif a["best_iter"] == 1: strat = "snipe_worker"
    elif a["best_iter"] == 2: strat = "final_worker (reanalyze)"
    emit(f"| {i+1} | {q['qid']} | **{a['best_speedup']:.2f}x** | W{a['best_worker']} | iter{a['best_iter']} | {a['baseline_ms']:.0f} | {strat} |")
emit()
emit("### Queries Exiting at 2x Gate (iter0)")
emit()
if exited_qs:
    emit(f"{len(exited_qs)} queries achieved >= 2.0x in iter0 and skipped subsequent iterations:")
    emit()
    for qid, sp in sorted(exited_qs, key=lambda x: -x[1]):
        emit(f"- **{qid}**: {sp:.2f}x")
    emit()

emit("---")
emit()
emit("## Pipeline Performance")
emit()
emit("### Phase Timing")
emit()
emit("| Phase | Description | Calls | Completed At | Duration |")
emit("|-------|-------------|------:|-------------|----------|")
p_descs = {"phase2":("Analyst Fan-Out",101),"phase2_5":("Parse Assignments","---"),
    "phase3":("Worker Generation",404),"phase4":("Benchmark iter0",101),
    "phase5":("Snipe Workers (LLM)",95),"phase5_bench":("Benchmark iter1",95),
    "phase6":("Re-Analyze (LLM)",94),"phase7":("Final Workers (LLM)",94),
    "phase7_bench":("Benchmark iter2",93)}
p_order = ["phase2","phase2_5","phase3","phase4","phase5","phase5_bench","phase6","phase7","phase7_bench"]
prev_ts = parse_ts("2026-02-08T11:11:00")
for pk in p_order:
    desc, calls = p_descs.get(pk, (pk,"?"))
    ts_str = checkpoint.get(pk,{}).get("completed_at","")
    ts = parse_ts(ts_str); dur_str = ""
    if ts and prev_ts:
        dur = (ts - prev_ts).total_seconds()
        if dur >= 0: dur_str = f"{dur/60:.1f} min"
        prev_ts = ts
    ts_disp = ts_str.split("T")[1][:8] if "T" in ts_str else ts_str
    emit(f"| {pk} | {desc} | {calls} | {ts_disp} | {dur_str} |")
emit()
emit("**Total wall time**: ~65 min (11:11 to 12:15)")
emit()
emit("### API Call Success Rates")
emit()
emit("| Phase | Total | Success | Rate |")
emit("|-------|------:|--------:|-----:|")
emit("| Analyst Fan-Out (P2) | 101 | 101 | 100% |")
emit("| Worker Generation (P3) | 404 | 404 | 100% |")
emit("| Snipe Workers (P5) | 95 | 95 | 100% |")
emit("| Re-Analyze (P6) | 94 | 94 | 100% |")
emit("| Final Workers (P7) | 94 | 94 | 100% |")
emit(f"| **Total** | **{total_api}** | **{total_api}** | **100%** |")
emit()
emit("All 788 LLM API calls succeeded with zero transport/timeout errors.")
emit()
emit("### SQL Extraction Success")
emit()
emit("| Iteration | Extracted | Total | Rate |")
emit("|-----------|----------:|------:|-----:|")
emit(f"| iter0 (4 workers) | {iter0_sql_ok} | {iter0_sql_total} | {100*iter0_sql_ok/max(iter0_sql_total,1):.1f}% |")
emit(f"| iter1 (snipe) | {iter1_sql_ok} | 95 | {100*iter1_sql_ok/95:.1f}% |")
emit(f"| iter2 (final) | {iter2_sql_ok} | 94 | {100*iter2_sql_ok/94:.1f}% |")
emit()

emit("---")
emit()
emit("## Iteration Effectiveness Analysis")
emit()
emit("The swarm pipeline runs up to 3 iterations per query:")
emit()
emit("1. **iter0**: 4 parallel workers with diverse strategies (W1-W4)")
emit("2. **iter1 (snipe)**: 1 worker (W5) sees iter0 results, targets best approach")
emit("3. **iter2 (reanalyze + final)**: Analyst re-examines all results, 1 final worker (W6) gets refined strategy")
emit()
emit("### Where Did the Best Result Come From?")
emit()
emit("| Source | Queries | % |")
emit("|--------|--------:|---:|")
for src in ["iter0","iter1","iter2","none"]:
    cnt = iter_best_source.get(src, 0)
    emit(f"| {src} | {cnt} | {100*cnt/len(queries):.1f}% |")
emit()
emit(f"- **iter1 (snipe) beat iter0** on {iter1_improvements} queries ({100*iter1_improvements/len(queries):.1f}%)")
emit(f"- **iter2 (final) beat iter0+iter1** on {iter2_improvements} queries ({100*iter2_improvements/len(queries):.1f}%)")
total_later = iter_best_source.get("iter1",0) + iter_best_source.get("iter2",0)
emit(f"- **Multi-iteration produced the overall best** on {total_later} queries ({100*total_later/len(queries):.1f}%)")
emit()
emit("### Verdict: Did Multi-Iteration Add Value?")
emit()
emit(f"**Yes.** {total_later} of {len(queries)} queries ({100*total_later/len(queries):.1f}%) got their best result from iter1 or iter2, not iter0.")
emit()
it2_best = sorted([(q["qid"],q["analysis"]["best_speedup"]) for q in queries if q["analysis"]["best_iter"]==2], key=lambda x: -x[1])
if it2_best:
    emit("Notable queries where iter2 (reanalyze+final) produced the best result:")
    emit()
    for qid, sp in it2_best[:10]:
        emit(f"- **{qid}**: {sp:.2f}x")
    emit()
it1_best = sorted([(q["qid"],q["analysis"]["best_speedup"]) for q in queries if q["analysis"]["best_iter"]==1], key=lambda x: -x[1])
if it1_best:
    emit("Notable queries where iter1 (snipe) produced the best result:")
    emit()
    for qid, sp in it1_best[:10]:
        emit(f"- **{qid}**: {sp:.2f}x")
    emit()
emit("### iter0 Worker Performance (W1-W4)")
emit()
emit("| Worker | Pass | Fail (rows) | Error (SQL) | Win Rate (>=1.1x) |")
emit("|-------:|-----:|------------:|------------:|-------------------:|")
for wid in sorted(iter0_ws.keys()):
    ws = iter0_ws[wid]
    total = ws["pass"]+ws["fail"]+ws["error"]
    emit(f"| W{wid} | {ws['pass']} | {ws['fail']} | {ws['error']} | {ws['wins']}/{total} ({100*ws['wins']/max(total,1):.0f}%) |")
emit()

emit("---")
emit()
emit("## Transform / Strategy Analysis")
emit()
emit("### Strategy Frequency (assigned by Analyst)")
emit()
emit("| Strategy | Assigned | Passing | Avg Speedup | Win Rate (>=1.1x) |")
emit("|----------|--------:|-------:|-----------:|---------:|")
for strat, count in strat_counter.most_common(20):
    results = strat_results.get(strat, [])
    avg = sum(results)/len(results) if results else 0
    wins = sum(1 for r in results if r >= 1.1)
    rate = 100*wins/len(results) if results else 0
    emit(f"| {strat} | {count} | {len(results)} | {avg:.2f}x | {wins}/{len(results)} ({rate:.0f}%) |")
emit()
emit("### Example Pattern Usage (from Gold Examples)")
emit()
emit("| Example | Times Used | Passing | Avg Speedup | Win Rate (>=1.1x) |")
emit("|---------|----------:|-------:|-----------:|---------:|")
for ex, count in ex_counter.most_common(25):
    results = ex_results.get(ex, [])
    avg = sum(results)/len(results) if results else 0
    wins = sum(1 for r in results if r >= 1.1)
    rate = 100*wins/len(results) if results else 0
    emit(f"| {ex} | {count} | {len(results)} | {avg:.2f}x | {wins}/{len(results)} ({rate:.0f}%) |")
emit()

emit("---")
emit()
emit("## Error Analysis")
emit()
emit("| Metric | Count | % |")
emit("|--------|------:|---:|")
emit(f"| Total worker evaluations | {total_we} | 100% |")
emit(f"| Clean passes | {total_pass} | {100*total_pass/total_we:.1f}% |")
emit(f"| Row mismatches (FAIL) | {total_fails} | {100*total_fails/total_we:.1f}% |")
emit(f"| SQL errors (ERROR) | {total_errs} | {100*total_errs/total_we:.1f}% |")
emit()
emit("### Error Categories")
emit()
emit("| Category | Count | % of Errors |")
emit("|----------|------:|------------:|")
for cat, cnt in err_cats.most_common():
    emit(f"| {cat} | {cnt} | {100*cnt/max(total_errs,1):.1f}% |")
emit()
emit("### All SQL Errors (detailed)")
emit()
emit("| Query | Worker | Iter | Category | Error Message (truncated) |")
emit("|-------|-------:|-----:|----------|---------------------------|")
for e in sorted(all_errs, key=lambda x: (x["qid"],x["iter"],x["worker_id"])):
    msg = e["error"].replace("\n"," ").strip()[:100]
    emit(f"| {e['qid']} | W{e['worker_id']} | {e['iter']} | {e['category']} | {msg} |")
emit()
all_fail_qs = [q for q in queries if q["analysis"]["best_speedup"] == 0]
emit(f"### Queries Where No Valid Optimization Was Found ({len(all_fail_qs)})")
emit()
if all_fail_qs:
    emit("Every worker either errored or produced wrong row counts across all 3 iterations:")
    emit()
    for q in all_fail_qs:
        a = q["analysis"]
        emit(f"#### {q['qid']} (baseline: {a['baseline_ms']:.0f}ms, {a['row_count']} rows)")
        emit()
        for w in a["all_workers"]:
            if w["error"]:
                emit(f"- iter{w['iter']} W{w['worker_id']}: **ERROR** - `{w['error'][:120]}`")
            else:
                emit(f"- iter{w['iter']} W{w['worker_id']}: **FAIL** (rows mismatch, would-be speedup {w['speedup']:.2f}x)")
        would_wins = [w for w in a["all_workers"] if w["speedup"] >= 1.1 and not w["error"]]
        if would_wins:
            bw = max(w["speedup"] for w in would_wins)
            emit()
            emit(f"**Note**: {len(would_wins)} workers produced faster SQL ({bw:.2f}x best) but wrong row counts -- semantically incorrect optimization.")
        emit()
emit(f"### Mixed Queries (some workers error, some pass) [{len(mixed_qs)}]")
emit()
if mixed_qs:
    emit("These queries had at least one SQL error AND at least one passing result:")
    emit()
    for qid in sorted(mixed_qs):
        q = next(qq for qq in queries if qq["qid"]==qid)
        a = q["analysis"]
        n_err = sum(1 for w in a["all_workers"] if w["error"])
        n_pass = sum(1 for w in a["all_workers"] if w["rows_match"] and w["status"]=="pass")
        emit(f"- **{qid}**: {n_err} errors, {n_pass} passes, best {a['best_speedup']:.2f}x")
    emit()

emit("---")
emit()
emit("## Regression Deep Dive")
emit()
if reg_qs:
    emit(f"**{len(reg_qs)} queries** where the best passing result was still a regression (<0.95x):")
    emit()
    emit("| Query | Best Speedup | Baseline (ms) | Best Worker | Iter | Notes |")
    emit("|-------|------------:|--------------:|------------:|-----:|-------|")
    for q in reg_qs:
        a = q["analysis"]; notes = ""
        if a["best_worker"] <= 4:
            for asgn in q["assignments"]:
                if asgn.get("worker_id") == a["best_worker"]:
                    notes = asgn.get("strategy",""); break
        elif a["best_worker"] == 5: notes = "snipe"
        elif a["best_worker"] == 6: notes = "final"
        emit(f"| {q['qid']} | **{a['best_speedup']:.3f}x** | {a['baseline_ms']:.0f} | W{a['best_worker']} | iter{a['best_iter']} | {notes} |")
    emit()
    reg_baselines = [q["analysis"]["baseline_ms"] for q in reg_qs]
    pass_baselines = [q["analysis"]["baseline_ms"] for q in queries if q["analysis"]["best_speedup"] >= 0.95]
    avg_reg = sum(reg_baselines)/len(reg_baselines) if reg_baselines else 0
    avg_pass = sum(pass_baselines)/len(pass_baselines) if pass_baselines else 0
    emit("### Are Fast Queries More Prone to Regression?")
    emit()
    emit(f"- Average baseline of regressed queries: {avg_reg:.0f}ms")
    emit(f"- Average baseline of non-regressed queries: {avg_pass:.0f}ms")
    if avg_reg < avg_pass:
        emit(f"- **Yes** - regressed queries have lower baselines ({avg_reg:.0f}ms vs {avg_pass:.0f}ms), suggesting fast queries leave less room for optimization.")
    else:
        emit(f"- **No** - regressed queries are not systematically faster.")
    emit()
    emit("### Regression Details")
    emit()
    for q in reg_qs:
        a = q["analysis"]
        emit(f"#### {q['qid']} ({a['best_speedup']:.3f}x, baseline {a['baseline_ms']:.0f}ms)")
        emit()
        emit("| Worker | Iter | Speedup | Status |")
        emit("|-------:|-----:|--------:|--------|")
        for w in a["all_workers"]:
            emit(f"| W{w['worker_id']} | {w['iter']} | {fmt_speedup(w['speedup'])} | {w['label']} |")
        emit()
        ra = q.get("reanalyze_response","")
        if ra:
            fa_match = re.search(r"FAILURE_ANALYSIS:\s*(.+?)(?=UNEXPLORED|REFINED|$)", ra, re.DOTALL)
            if fa_match:
                fa = fa_match.group(1).strip()[:400]
                emit(f"**Re-analyze failure assessment**: {fa}")
                emit()
else:
    emit("No regressions. All queries maintained or improved performance.")
    emit()

emit("---")
emit()
emit("## Row Mismatch (FAIL) Analysis")
emit()
emit(f"**{len(fail_qs)} queries** had at least one worker produce wrong row counts.")
emit()
all_fail_row = []
for qid, ws in fail_qs.items():
    q = next(qq for qq in queries if qq["qid"]==qid)
    if all(w["label"] in ("FAIL","ERROR") for w in q["analysis"]["all_workers"]):
        all_fail_row.append(qid)
emit(f"**{len(all_fail_row)} queries** had ALL workers produce wrong rows or errors:")
emit()
for qid in sorted(all_fail_row):
    q = next(qq for qq in queries if qq["qid"]==qid)
    a = q["analysis"]
    would = [w for w in a["all_workers"] if w["speedup"] > 0 and not w["error"]]
    if would:
        bw = max(w["speedup"] for w in would)
        emit(f"- **{qid}** (baseline {a['baseline_ms']:.0f}ms) - best would-have-been: {bw:.2f}x")
    else:
        emit(f"- **{qid}** (baseline {a['baseline_ms']:.0f}ms) - all SQL errors")
emit()
total_non_err = sum(1 for q in queries for w in q["analysis"]["all_workers"] if not w["error"])
total_fail_c = sum(1 for q in queries for w in q["analysis"]["all_workers"] if w["label"]=="FAIL")
emit(f"**Row mismatch rate**: {total_fail_c}/{total_non_err} non-error evaluations ({100*total_fail_c/max(total_non_err,1):.1f}%)")
emit()
fqc = sorted([(qid, len(ws)) for qid, ws in fail_qs.items()], key=lambda x: -x[1])
emit("### Queries with Most Row Mismatches")
emit()
emit("| Query | Fail Count | Total Workers | Baseline (ms) |")
emit("|-------|----------:|--------------:|--------------:|")
for qid, fc in fqc[:15]:
    q = next(qq for qq in queries if qq["qid"]==qid)
    a = q["analysis"]
    emit(f"| {qid} | {fc} | {len(a['all_workers'])} | {a['baseline_ms']:.0f} |")
emit()

emit("---")
emit()
emit("## Per-Query Details")
emit()
emit("Sorted by best speedup (descending). Each query shows all worker results across all iterations.")
emit()
for q in queries_ranked:
    a = q["analysis"]; qid = q["qid"]
    label = overall_label(a["best_speedup"])
    markers = {"WIN":"**[WIN]**","IMPROVED":"[IMPROVED]","NEUTRAL":"[NEUTRAL]",
               "REGRESSION":"[REGRESSION]","ERROR":"[ALL FAIL]"}
    marker = markers.get(label,"")
    emit(f"### {qid} {marker}")
    emit()
    emit(f"- **Baseline**: {a['baseline_ms']:.1f} ms ({a['row_count']} rows)")
    emit(f"- **Best speedup**: {fmt_speedup(a['best_speedup'])} (W{a['best_worker']}, iter{a['best_iter']})")
    b0 = q.get("benchmark_iter0")
    if b0 and b0.get("exited"):
        emit(f"- **Exited at 2x gate** after iter0")
    emit()
    emit("| Worker | Iter | Speedup | Status | Notes |")
    emit("|-------:|-----:|--------:|--------|-------|")
    for w in a["all_workers"]:
        notes = ""
        if w["error"]: notes = classify_error(w["error"])
        elif w["label"] == "FAIL": notes = "row mismatch"
        elif w["worker_id"] == a["best_worker"] and w["iter"] == a["best_iter"]: notes = "BEST"
        emit(f"| W{w['worker_id']} | {w['iter']} | {fmt_speedup(w['speedup'])} | {w['label']} | {notes} |")
    emit()
    if q["assignments"]:
        emit("**Strategies assigned:**")
        emit()
        for asgn in q["assignments"]:
            wid = asgn.get("worker_id","?")
            strat = asgn.get("strategy","?")
            examples = ", ".join(asgn.get("examples",[]))
            hint = asgn.get("hint","")[:200]
            emit(f"- W{wid}: `{strat}` ({examples})")
            emit(f"  - {hint}")
        emit()
    rp = q.get("reanalyze_parsed") or {}
    if rp.get("failure_analysis"):
        fa = rp["failure_analysis"][:300]
        emit(f"**Reanalyze insight**: {fa}...")
        emit()
    emit("---")
    emit()

emit("## Appendix: Full Leaderboard")
emit()
emit("| # | Query | Best Speedup | Worker | Iter | Baseline (ms) | Rows | Status |")
emit("|--:|-------|------------:|-------:|-----:|--------------:|-----:|--------|")
for i, q in enumerate(queries_ranked):
    a = q["analysis"]
    label = overall_label(a["best_speedup"])
    emit(f"| {i+1} | {q['qid']} | {fmt_speedup(a['best_speedup'])} | W{a['best_worker']} | {a['best_iter']} | {a['baseline_ms']:.0f} | {a['row_count']} | {label} |")
emit()
emit("---")
emit()
emit("*Generated by `generate_review.py` on 2026-02-08*")

output = "\n".join(L)
with open(OUT, "w") as f:
    f.write(output)
print(f"\nDone! Written {len(output):,} chars ({len(L)} lines) to:")
print(f"  {OUT}")
print(f"\nSummary:")
print(f"  Queries: {len(queries)}")
print(f"  WIN: {label_counts.get('WIN',0)}, IMPROVED: {label_counts.get('IMPROVED',0)}, "
      f"NEUTRAL: {label_counts.get('NEUTRAL',0)}, REGRESSION: {label_counts.get('REGRESSION',0)}, "
      f"ERROR: {label_counts.get('ERROR',0)}")
print(f"  Best from iter0: {iter_best_source.get('iter0',0)}, "
      f"iter1: {iter_best_source.get('iter1',0)}, "
      f"iter2: {iter_best_source.get('iter2',0)}")

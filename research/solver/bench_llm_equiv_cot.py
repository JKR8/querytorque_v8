"""
CoT LLM-as-judge SQL equivalence — rerun on misses only.
Targets the two failure modes:
  FP (16): model falsely rejects safe structural rewrites
  FN (17): model misses subtle semantic breaks
"""
import json
import subprocess
import sys
import time
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import sqlglot

MODEL = "qwen2.5-coder:7b"
VOTES = 3
PARALLEL = 8
GT_FILE = Path(__file__).parent / "witness_ground_truth_results.json"
PREV_FILE = Path(__file__).parent / "llm_equiv_bench_results.json"
OUT_FILE = Path(__file__).parent / "llm_equiv_cot_results.json"

SYSTEM_PROMPT = """You are an SQL equivalence verifier. You must determine if two queries return identical results on any possible database state.

Think step by step, then give your final answer."""

USER_TEMPLATE = """Are Query A and Query B semantically equivalent?

## Step-by-step checklist

Work through each check. If ANY check fails, the queries are NOT equivalent.

1. **JOIN type and position**: Are all JOINs the same type (INNER/LEFT/RIGHT/CROSS)?
   - CRITICAL: Moving a filter from WHERE to a LEFT JOIN's ON clause CHANGES semantics.
     WHERE filters rows AFTER the join (removes NULLs). ON filters DURING the join (preserves NULLs).
   - Comma-joins are implicit INNER JOINs. Converting comma-join to explicit INNER JOIN is SAFE.

2. **Filter predicates**: Are all WHERE/HAVING conditions present in both queries?
   - Predicates may move into CTEs or subqueries — that is SAFE if the same rows are filtered.
   - A predicate on a LEFT-JOIN table in WHERE vs ON is NOT safe (see rule 1).

3. **Correlated vs uncorrelated subqueries**: If a correlated subquery was converted to a CTE or derived table:
   - The correlated version computes per-row (e.g., AVG per group matching the outer row).
   - The uncorrelated version computes globally (e.g., AVG across ALL groups).
   - This changes results if the correlation produced different values per outer row.

4. **Aggregation scope**: Do GROUP BY columns match? Are aggregate functions (SUM, AVG, COUNT) computed over the same set of rows?

5. **Column output**: Do both queries return the same columns in the same order?

6. **Safe transformations** (these do NOT change semantics):
   - Extracting filters into CTEs or derived tables
   - Rewriting comma-joins as explicit INNER JOINs
   - Reordering JOIN clauses (for INNER joins only)
   - Renaming aliases
   - Adding redundant CTEs that are joined back equivalently

## Queries

Query A:
{sql_a}

Query B:
{sql_b}

## Your analysis

For each check (1-6), write one line. Then write your final answer.

VERDICT: TRUE or FALSE"""


def normalize_sql(sql: str) -> str:
    try:
        parsed = sqlglot.parse(sql)
        parts = []
        for stmt in parsed:
            normalized = stmt.sql(
                dialect="duckdb",
                normalize=True,
                pretty=True,
                comments=False,
            ).lower()
            parts.append(normalized)
        return "\n".join(parts)
    except Exception:
        return sql.lower().strip()


def call_ollama(system: str, user: str) -> str:
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "stream": False,
        "options": {"temperature": 0.3, "num_predict": 512},
    }
    result = subprocess.run(
        ["curl", "-s", "http://localhost:11434/api/chat", "-d", json.dumps(payload)],
        capture_output=True, text=True, timeout=300,
    )
    resp = json.loads(result.stdout)
    return resp.get("message", {}).get("content", "").strip()


def parse_verdict(raw: str) -> str:
    """Extract final VERDICT from CoT response."""
    upper = raw.upper()
    # Look for explicit VERDICT: line (last one wins)
    lines = upper.split("\n")
    for line in reversed(lines):
        if "VERDICT:" in line:
            after = line.split("VERDICT:")[-1].strip()
            if after.startswith("TRUE"):
                return "TRUE"
            if after.startswith("FALSE"):
                return "FALSE"
    # Fallback: last TRUE/FALSE in text
    for line in reversed(lines):
        stripped = line.strip()
        if stripped.startswith("TRUE"):
            return "TRUE"
        if stripped.startswith("FALSE"):
            return "FALSE"
    if "TRUE" in upper and "FALSE" not in upper:
        return "TRUE"
    if "FALSE" in upper and "TRUE" not in upper:
        return "FALSE"
    return "UNCLEAR"


def process_entry(idx: int, entry: dict) -> dict:
    qid = entry["query_id"]
    pid = entry["patch_id"]
    it = entry["iteration"]
    gt = entry["ground_truth"]
    expected = "TRUE" if gt == "EQUIVALENT" else "FALSE"

    try:
        norm_a = normalize_sql(entry["original_sql"])
        norm_b = normalize_sql(entry["patch_sql"])
        user_msg = USER_TEMPLATE.format(sql_a=norm_a, sql_b=norm_b)

        votes = []
        raw_responses = []
        for _ in range(VOTES):
            raw = call_ollama(SYSTEM_PROMPT, user_msg)
            votes.append(parse_verdict(raw))
            raw_responses.append(raw)

        tc = votes.count("TRUE")
        fc = votes.count("FALSE")
        if tc > fc:
            verdict = "TRUE"
        elif fc > tc:
            verdict = "FALSE"
        else:
            verdict = "SPLIT"

        correct = verdict == expected
        return {
            "idx": idx, "query_id": qid, "patch_id": pid, "iteration": it,
            "ground_truth": gt, "expected": expected,
            "verdict": verdict, "votes": votes, "correct": correct,
            "raw_responses": raw_responses,
        }
    except Exception as ex:
        return {
            "idx": idx, "query_id": qid, "patch_id": pid, "iteration": it,
            "ground_truth": gt, "expected": expected,
            "verdict": "ERROR", "votes": [], "correct": False,
            "error": str(ex),
        }


def main():
    # Load previous results, extract misses
    with open(PREV_FILE) as f:
        prev = json.load(f)

    misses = [r for r in prev["results"] if not r["correct"] and r["verdict"] != "ERROR"]
    miss_keys = {(r["query_id"], r["patch_id"], r["iteration"]) for r in misses}

    # Load ground truth to get SQL
    with open(GT_FILE) as f:
        gt_data = json.load(f)

    gt_lookup = {}
    for r in gt_data:
        key = (r["query_id"], r["patch_id"], r["iteration"])
        if key not in gt_lookup:
            gt_lookup[key] = r

    entries = []
    for key in miss_keys:
        if key in gt_lookup:
            entries.append(gt_lookup[key])
    entries.sort(key=lambda r: (r["query_id"], r["patch_id"], r["iteration"]))

    total = len(entries)
    fp_count = sum(1 for e in entries if e["ground_truth"] == "EQUIVALENT")
    fn_count = sum(1 for e in entries if e["ground_truth"] == "CHECKSUM_FAIL")
    print(f"Model: {MODEL} | CoT prompt | Votes: {VOTES} | Parallel: {PARALLEL}")
    print(f"Misses only: {total} ({fp_count} false positives, {fn_count} false negatives)")
    print(f"Total LLM calls: {total * VOTES}")
    print("=" * 70)

    t_start = time.time()
    results = [None] * total
    done_count = 0
    lock = threading.Lock()

    def on_done(future):
        nonlocal done_count
        r = future.result()
        results[r["idx"]] = r
        with lock:
            done_count += 1
            elapsed = time.time() - t_start
            rate = done_count / elapsed if elapsed > 0 else 0
            eta = (total - done_count) / rate if rate > 0 else 0
            tag = "FIXED" if r["correct"] else "still wrong"
            print(
                f"  [{done_count:3d}/{total}] {r['query_id']:30s} {r['patch_id']:10s} "
                f"gt={r['ground_truth']:15s} llm={r['verdict']:5s} "
                f"votes={r.get('votes',[])} {tag}"
            )

    with ThreadPoolExecutor(max_workers=PARALLEL) as pool:
        futures = []
        for i, entry in enumerate(entries):
            f = pool.submit(process_entry, i, entry)
            f.add_done_callback(on_done)
            futures.append(f)
        for f in futures:
            f.result()

    elapsed_total = time.time() - t_start

    # Tally
    tp = fp = tn = fn = 0
    for r in results:
        gt = r["ground_truth"]
        v = r["verdict"]
        if v == "ERROR":
            continue
        if gt == "CHECKSUM_FAIL" and v == "FALSE":
            tp += 1
        elif gt == "EQUIVALENT" and v == "FALSE":
            fp += 1
        elif gt == "EQUIVALENT" and v == "TRUE":
            tn += 1
        elif gt == "CHECKSUM_FAIL" and v == "TRUE":
            fn += 1

    fixed = sum(1 for r in results if r["correct"])

    print("\n" + "=" * 70)
    print(f"COT RESULTS — {MODEL} — misses-only rerun")
    print("=" * 70)
    print(f"Previously wrong:  {total}")
    print(f"Now correct:       {fixed}/{total} ({fixed/total:.1%})")
    print(f"Time:              {elapsed_total:.1f}s")
    print()
    print(f"  Was FP (equiv→FALSE), now fixed: {tn}/{fp_count}")
    print(f"  Was FN (fail→TRUE),   now fixed: {tp}/{fn_count}")
    print()
    print(f"  Still FP: {fp}")
    print(f"  Still FN: {fn}")
    print()

    # What would full-set accuracy be with CoT?
    orig_correct = prev["correct"]
    new_total_correct = orig_correct + fixed
    full_total = prev["total"]
    print(f"Projected full-set accuracy: {new_total_correct}/{full_total} ({new_total_correct/full_total:.1%})")
    print(f"  (was {orig_correct}/{full_total} = {orig_correct/full_total:.1%})")

    # Save with raw CoT for inspection
    summary = {
        "model": MODEL,
        "prompt": "CoT",
        "votes_per_pair": VOTES,
        "parallel": PARALLEL,
        "total_misses": total,
        "fixed": fixed,
        "tp": tp, "fp": fp, "tn": tn, "fn": fn,
        "elapsed_s": elapsed_total,
        "projected_accuracy": new_total_correct / full_total,
        "results": results,
    }
    with open(OUT_FILE, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nResults saved to: {OUT_FILE}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

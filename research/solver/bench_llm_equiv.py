"""
Batch LLM-as-judge SQL equivalence benchmark.
Runs all ground truth pairs through qwen2.5-coder:7b via Ollama.
3 votes per pair, majority wins. Parallel via ThreadPoolExecutor.
"""
import json
import subprocess
import sys
import time
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import sqlglot

MODEL = "qwen2.5-coder:7b"
VOTES = 3
PARALLEL = 8  # concurrent ollama requests
GT_FILE = Path(__file__).parent / "witness_ground_truth_results.json"
OUT_FILE = Path(__file__).parent / "llm_equiv_bench_results.json"

SYSTEM_PROMPT = (
    "You are an SQL expert. Your task is to determine if two SQL queries "
    "will return identical results in the same database state."
)

USER_TEMPLATE = """Compare Query A and Query B.

Focus on logic: joins, filters, grouping, and aggregations.

If they are semantically equivalent (return same rows and columns), output "TRUE".

If they differ in logic or expected output, output "FALSE".

Query A:
{sql_a}

Query B:
{sql_b}

Output (TRUE/FALSE):"""


def normalize_sql(sql: str) -> str:
    """Normalize SQL via sqlglot: lowercase, no comments, consistent format."""
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
        "options": {"temperature": 0.3, "num_predict": 64},
    }
    result = subprocess.run(
        ["curl", "-s", "http://localhost:11434/api/chat", "-d", json.dumps(payload)],
        capture_output=True, text=True, timeout=300,
    )
    resp = json.loads(result.stdout)
    return resp.get("message", {}).get("content", "").strip()


def parse_verdict(raw: str) -> str:
    upper = raw.upper().strip()
    if upper.startswith("TRUE"):
        return "TRUE"
    if upper.startswith("FALSE"):
        return "FALSE"
    if "TRUE" in upper and "FALSE" not in upper:
        return "TRUE"
    if "FALSE" in upper and "TRUE" not in upper:
        return "FALSE"
    return "UNCLEAR"


def single_vote(user_msg: str) -> str:
    """One LLM call, returns parsed verdict."""
    raw = call_ollama(SYSTEM_PROMPT, user_msg)
    return parse_verdict(raw)


def process_entry(idx: int, entry: dict, total: int) -> dict:
    """Process one ground truth entry: normalize, 3 votes, majority."""
    qid = entry["query_id"]
    pid = entry["patch_id"]
    it = entry["iteration"]
    gt = entry["ground_truth"]
    expected = "TRUE" if gt == "EQUIVALENT" else "FALSE"

    try:
        norm_a = normalize_sql(entry["original_sql"])
        norm_b = normalize_sql(entry["patch_sql"])
        user_msg = USER_TEMPLATE.format(sql_a=norm_a, sql_b=norm_b)

        # 3 votes sequentially per entry (parallelism is across entries)
        votes = []
        for _ in range(VOTES):
            votes.append(single_vote(user_msg))

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
        }
    except Exception as ex:
        return {
            "idx": idx, "query_id": qid, "patch_id": pid, "iteration": it,
            "ground_truth": gt, "expected": expected,
            "verdict": "ERROR", "votes": [], "correct": False,
            "error": str(ex),
        }


def main():
    with open(GT_FILE) as f:
        data = json.load(f)

    # Deduplicate by (query_id, patch_id, iteration), skip errors
    seen = set()
    entries = []
    for r in data:
        key = (r["query_id"], r["patch_id"], r["iteration"])
        if key in seen:
            continue
        seen.add(key)
        if r.get("orig_err") or r.get("patch_err"):
            continue
        entries.append(r)

    total = len(entries)
    equiv_count = sum(1 for e in entries if e["ground_truth"] == "EQUIVALENT")
    fail_count = sum(1 for e in entries if e["ground_truth"] == "CHECKSUM_FAIL")
    print(f"Model: {MODEL} | Votes: {VOTES} | Parallel: {PARALLEL}")
    print(f"Entries: {total} ({equiv_count} EQUIVALENT, {fail_count} CHECKSUM_FAIL)")
    print(f"Total LLM calls: {total * VOTES}")
    print("=" * 70)

    t_start = time.time()
    results = [None] * total
    done_count = 0
    lock = threading.Lock()

    def on_done(future):
        nonlocal done_count
        r = future.result()
        idx = r["idx"]
        results[idx] = r
        with lock:
            done_count += 1
            elapsed = time.time() - t_start
            rate = done_count / elapsed if elapsed > 0 else 0
            eta = (total - done_count) / rate if rate > 0 else 0
            tag = "ok" if r["correct"] else "MISS"
            print(
                f"  [{done_count:3d}/{total}] {r['query_id']:30s} {r['patch_id']:8s} "
                f"gt={r['ground_truth']:15s} llm={r['verdict']:5s} "
                f"votes={r.get('votes',[])} {tag:4s}  "
                f"({rate:.1f}/s, ETA {eta:.0f}s)"
            )

    with ThreadPoolExecutor(max_workers=PARALLEL) as pool:
        futures = []
        for i, entry in enumerate(entries):
            f = pool.submit(process_entry, i, entry, total)
            f.add_done_callback(on_done)
            futures.append(f)
        # Wait for all
        for f in futures:
            f.result()

    elapsed_total = time.time() - t_start

    # --- Confusion matrix ---
    tp = fp = tn = fn = errors = 0
    for r in results:
        gt = r["ground_truth"]
        v = r["verdict"]
        if v == "ERROR":
            errors += 1
            continue
        if gt == "CHECKSUM_FAIL" and v == "FALSE":
            tp += 1
        elif gt == "EQUIVALENT" and v == "FALSE":
            fp += 1
        elif gt == "EQUIVALENT" and v == "TRUE":
            tn += 1
        elif gt == "CHECKSUM_FAIL" and v == "TRUE":
            fn += 1

    correct_count = sum(1 for r in results if r["correct"])
    accuracy = correct_count / total if total else 0
    precision = tp / (tp + fp) if (tp + fp) else 0
    recall = tp / (tp + fn) if (tp + fn) else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0

    print("\n" + "=" * 70)
    print(f"RESULTS — {MODEL} — {VOTES}-vote majority — {PARALLEL} parallel")
    print("=" * 70)
    print(f"Total pairs:       {total}")
    print(f"Correct:           {correct_count}/{total} ({accuracy:.1%})")
    print(f"Errors:            {errors}")
    print(f"Time:              {elapsed_total:.1f}s ({elapsed_total/total:.2f}s/pair)")
    print()
    print(f"Confusion matrix (detecting NON-EQUIVALENCE):")
    print(f"  True Positives:  {tp:3d}  (correctly caught semantic error)")
    print(f"  True Negatives:  {tn:3d}  (correctly passed equivalent)")
    print(f"  False Positives: {fp:3d}  (wrongly flagged equivalent as different)")
    print(f"  False Negatives: {fn:3d}  (missed semantic error)")
    print()
    print(f"  Precision:       {precision:.1%}")
    print(f"  Recall:          {recall:.1%}")
    print(f"  F1:              {f1:.1%}")
    print()

    equiv_correct = sum(1 for r in results if r["ground_truth"] == "EQUIVALENT" and r["correct"])
    fail_correct = sum(1 for r in results if r["ground_truth"] == "CHECKSUM_FAIL" and r["correct"])
    equiv_total = sum(1 for r in results if r["ground_truth"] == "EQUIVALENT")
    fail_total = sum(1 for r in results if r["ground_truth"] == "CHECKSUM_FAIL")
    if equiv_total:
        print(f"  EQUIVALENT pairs:     {equiv_correct}/{equiv_total} correct ({equiv_correct/equiv_total:.1%})")
    if fail_total:
        print(f"  CHECKSUM_FAIL pairs:  {fail_correct}/{fail_total} correct ({fail_correct/fail_total:.1%})")

    # Vote distribution
    vote_patterns = Counter(tuple(r["votes"]) for r in results if r["votes"])
    print(f"\nVote patterns:")
    for pattern, count in vote_patterns.most_common():
        print(f"  {pattern}: {count}")

    # Misses detail
    misses = [r for r in results if not r["correct"] and r["verdict"] != "ERROR"]
    if misses:
        print(f"\nMisses ({len(misses)}):")
        for r in misses:
            print(f"  {r['query_id']:30s} {r['patch_id']:8s} gt={r['ground_truth']:15s} llm={r['verdict']} votes={r['votes']}")

    # Save
    summary = {
        "model": MODEL,
        "votes_per_pair": VOTES,
        "parallel": PARALLEL,
        "total": total,
        "correct": correct_count,
        "accuracy": accuracy,
        "tp": tp, "fp": fp, "tn": tn, "fn": fn,
        "precision": precision, "recall": recall, "f1": f1,
        "elapsed_s": elapsed_total,
        "results": results,
    }
    with open(OUT_FILE, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nFull results saved to: {OUT_FILE}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

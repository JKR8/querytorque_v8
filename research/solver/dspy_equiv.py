"""
DSPy-optimized SQL equivalence checker.
BootstrapFewShot with balanced 50/50 trainset and weighted metric.
Uses qwen2.5-coder:7b via Ollama.

Based on literature review:
- LLM-SQL-Solver (NAACL 2025): Miniature & Mull technique
- EDBT 2025: FPs dominate, complexity (8+ tables) drives errors
- Taming SQL Complexity (ICML 2025): normalization + 3-5x consensus
- Ragas: Explain & Compare structured output

Our approach: DSPy BootstrapFewShot to mine successful FALSE reasoning
traces from balanced trainset, then inject as few-shot demos. Combined
with sqlglot normalization (already ahead of Ragas) and 3-vote consensus.
"""
import json
import random
import sys
import time
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import dspy
import sqlglot

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MODEL = "ollama/qwen2.5-coder:7b"
VOTES = 3              # consensus voting per pair in final eval
PARALLEL_EVAL = 4      # concurrent eval workers
GT_FILE = Path(__file__).parent / "witness_ground_truth_results.json"
OUT_FILE = Path(__file__).parent / "dspy_equiv_results.json"
PROGRAM_DIR = Path(__file__).parent / "dspy_optimized_equiv"

random.seed(42)


# ---------------------------------------------------------------------------
# 1. SQL normalizer (run once, cache results)
# ---------------------------------------------------------------------------
_norm_cache: dict[str, str] = {}


def normalize_sql(sql: str) -> str:
    """Normalize via sqlglot: lowercase, no comments, consistent format."""
    if sql in _norm_cache:
        return _norm_cache[sql]
    try:
        parsed = sqlglot.parse(sql)
        parts = []
        for stmt in parsed:
            normalized = stmt.sql(
                dialect="duckdb", normalize=True, pretty=True, comments=False,
            ).lower()
            parts.append(normalized)
        result = "\n".join(parts)
    except Exception:
        result = sql.lower().strip()
    _norm_cache[sql] = result
    return result


# ---------------------------------------------------------------------------
# 2. DSPy Signature & Module
# ---------------------------------------------------------------------------
class SQLEquivalence(dspy.Signature):
    """Determine if two SQL queries are semantically equivalent.
    Two queries are equivalent if and only if they return identical rows and
    columns on EVERY possible database state.

    SAFE transformations that preserve equivalence:
    - Extracting filters into CTEs or derived tables
    - Converting comma-joins (FROM a, b WHERE a.id = b.id) to explicit INNER JOINs
    - Reordering INNER JOIN clauses
    - Renaming table/column aliases
    - Splitting a single query into multiple CTEs joined back together

    UNSAFE transformations that BREAK equivalence:
    - Moving a WHERE predicate on a LEFT/RIGHT JOIN table into the ON clause
      (WHERE filters AFTER join removing NULLs; ON filters DURING join preserving NULLs)
    - Converting a correlated subquery to an uncorrelated CTE/derived table
      (correlated computes per outer row; uncorrelated computes once globally)
    - Changing INNER JOIN to LEFT JOIN or vice versa
    - Adding or removing DISTINCT
    - Changing GROUP BY columns or aggregate function scope
    - Dropping a filter predicate during CTE extraction
    - Changing AND to OR or vice versa in WHERE clauses"""

    query_a: str = dspy.InputField(desc="First SQL query (normalized, lowercase)")
    query_b: str = dspy.InputField(desc="Second SQL query (normalized, lowercase)")
    is_equivalent: str = dspy.OutputField(
        desc="Exactly TRUE or FALSE. TRUE means the queries return identical "
             "results on any database. FALSE means there exists some database "
             "where they return different results."
    )


class SQLEquivChecker(dspy.Module):
    def __init__(self):
        self.judge = dspy.ChainOfThought(SQLEquivalence)

    def forward(self, query_a, query_b):
        return self.judge(query_a=query_a, query_b=query_b)


# ---------------------------------------------------------------------------
# 3. Metric — weighted to penalize affirmative bias
# ---------------------------------------------------------------------------
def strict_sql_metric(example, pred, trace=None):
    """Weighted metric: catching FALSE is worth 2x catching TRUE.
    This biases the DSPy optimizer toward prompts that break affirmative bias."""
    expected = example.is_equivalent.upper().strip()
    predicted = pred.is_equivalent.upper().strip()

    # Normalize predicted to clean TRUE/FALSE
    if predicted.startswith("TRUE"):
        predicted = "TRUE"
    elif predicted.startswith("FALSE"):
        predicted = "FALSE"
    elif "FALSE" in predicted:
        predicted = "FALSE"
    elif "TRUE" in predicted:
        predicted = "TRUE"
    else:
        return 0.0

    is_correct = expected == predicted

    if is_correct and expected == "FALSE":
        return 1.0   # Full credit for catching semantic errors
    elif is_correct and expected == "TRUE":
        return 0.5   # Half credit for correct equivalence
    else:
        return 0.0   # No credit for wrong answer


# ---------------------------------------------------------------------------
# 4. Data loading — balanced splits
# ---------------------------------------------------------------------------
def load_data():
    with open(GT_FILE) as f:
        raw = json.load(f)

    # Deduplicate by (query_id, patch_id, iteration), skip execution errors
    seen = set()
    entries = []
    for r in raw:
        key = (r["query_id"], r["patch_id"], r["iteration"])
        if key in seen:
            continue
        seen.add(key)
        if r.get("orig_err") or r.get("patch_err"):
            continue
        entries.append(r)

    # Pre-normalize all SQL once
    for e in entries:
        e["_norm_a"] = normalize_sql(e["original_sql"])
        e["_norm_b"] = normalize_sql(e["patch_sql"])

    # Split by ground truth
    true_entries = [e for e in entries if e["ground_truth"] == "EQUIVALENT"]
    false_entries = [e for e in entries if e["ground_truth"] == "CHECKSUM_FAIL"]
    random.shuffle(true_entries)
    random.shuffle(false_entries)

    print(f"Loaded: {len(true_entries)} EQUIVALENT, {len(false_entries)} CHECKSUM_FAIL")

    def make_example(e, with_meta=False):
        ex = dspy.Example(
            query_a=e["_norm_a"],
            query_b=e["_norm_b"],
            is_equivalent="TRUE" if e["ground_truth"] == "EQUIVALENT" else "FALSE",
        ).with_inputs("query_a", "query_b")
        if with_meta:
            ex._query_id = e["query_id"]
            ex._patch_id = e["patch_id"]
            ex._iteration = e["iteration"]
        return ex

    # Balanced trainset: 15 FALSE + 15 TRUE = 30
    # Balanced devset:   6 FALSE + 6 TRUE = 12
    # Evalset: everything (129)
    n_train_false = min(15, len(false_entries))
    n_dev_false = len(false_entries) - n_train_false

    train_false = false_entries[:n_train_false]
    dev_false = false_entries[n_train_false:]

    train_true = true_entries[:n_train_false]  # match count
    dev_true = true_entries[n_train_false:n_train_false + n_dev_false]

    trainset = [make_example(e) for e in train_false + train_true]
    random.shuffle(trainset)

    devset = [make_example(e) for e in dev_false + dev_true]
    random.shuffle(devset)

    evalset = [make_example(e, with_meta=True) for e in entries]

    print(f"Trainset: {len(trainset)} ({sum(1 for e in trainset if e.is_equivalent=='FALSE')} FALSE, "
          f"{sum(1 for e in trainset if e.is_equivalent=='TRUE')} TRUE)")
    print(f"Devset:   {len(devset)} ({sum(1 for e in devset if e.is_equivalent=='FALSE')} FALSE, "
          f"{sum(1 for e in devset if e.is_equivalent=='TRUE')} TRUE)")
    print(f"Evalset:  {len(evalset)} (all entries)")

    return trainset, devset, evalset


# ---------------------------------------------------------------------------
# 5. Verdict parsing helper
# ---------------------------------------------------------------------------
def parse_verdict(raw: str) -> str:
    upper = raw.upper().strip()
    if upper.startswith("TRUE"):
        return "TRUE"
    if upper.startswith("FALSE"):
        return "FALSE"
    if "FALSE" in upper and "TRUE" not in upper:
        return "FALSE"
    if "TRUE" in upper and "FALSE" not in upper:
        return "TRUE"
    # Both present — check last occurrence
    last_true = upper.rfind("TRUE")
    last_false = upper.rfind("FALSE")
    if last_false > last_true:
        return "FALSE"
    if last_true > last_false:
        return "TRUE"
    return "UNCLEAR"


# ---------------------------------------------------------------------------
# 6. Eval with 3-vote consensus (per "Taming SQL Complexity" paper)
# ---------------------------------------------------------------------------
def eval_single(program, ex):
    """Run one example through the optimized program with 3-vote majority."""
    votes = []
    reasoning_samples = []
    for _ in range(VOTES):
        try:
            pred = program(query_a=ex.query_a, query_b=ex.query_b)
            v = parse_verdict(pred.is_equivalent)
            votes.append(v)
            reasoning_samples.append(getattr(pred, "reasoning", "")[:300])
        except Exception as e:
            votes.append("ERROR")
            reasoning_samples.append(str(e)[:200])

    tc = votes.count("TRUE")
    fc = votes.count("FALSE")
    if tc > fc:
        verdict = "TRUE"
    elif fc > tc:
        verdict = "FALSE"
    elif tc == fc and tc > 0:
        verdict = "SPLIT"
    else:
        verdict = "ERROR"

    expected = ex.is_equivalent
    correct = verdict == expected

    return {
        "query_id": getattr(ex, "_query_id", ""),
        "patch_id": getattr(ex, "_patch_id", ""),
        "iteration": getattr(ex, "_iteration", 0),
        "expected": expected,
        "verdict": verdict,
        "votes": votes,
        "correct": correct,
        "reasoning_sample": reasoning_samples[0] if reasoning_samples else "",
    }


# ---------------------------------------------------------------------------
# 7. Main pipeline
# ---------------------------------------------------------------------------
def main():
    # ── Configure DSPy with Ollama ──
    lm = dspy.LM(
        MODEL,
        api_base="http://localhost:11434",
        temperature=0.3,
        max_tokens=512,
        cache=False,  # disable cache so votes are independent
    )
    dspy.configure(lm=lm)

    trainset, devset, evalset = load_data()
    checker = SQLEquivChecker()

    # ── STEP 1: Baseline on devset ──
    print("\n" + "=" * 70)
    print("STEP 1: Baseline (unoptimized CoT) on devset")
    print("=" * 70)
    baseline_eval = dspy.Evaluate(
        devset=devset,
        metric=strict_sql_metric,
        num_threads=PARALLEL_EVAL,
        display_progress=True,
    )
    baseline_score = baseline_eval(checker)
    print(f"Baseline devset score: {baseline_score:.3f}")

    # ── STEP 2: BootstrapFewShot optimization ──
    print("\n" + "=" * 70)
    print("STEP 2: BootstrapFewShot optimization (balanced trainset)")
    print("=" * 70)
    optimizer = dspy.BootstrapFewShot(
        metric=strict_sql_metric,
        max_bootstrapped_demos=6,   # max CoT traces to bootstrap
        max_labeled_demos=6,        # max labeled examples in prompt
        max_rounds=2,               # optimization rounds
    )
    optimized = optimizer.compile(checker, trainset=trainset)

    # ── STEP 3: Optimized on devset ──
    print("\n" + "=" * 70)
    print("STEP 3: Optimized model on devset")
    print("=" * 70)
    opt_eval = dspy.Evaluate(
        devset=devset,
        metric=strict_sql_metric,
        num_threads=PARALLEL_EVAL,
        display_progress=True,
    )
    opt_score = opt_eval(optimized)
    print(f"Optimized devset score: {opt_score:.3f}")

    # ── STEP 4: Full eval with 3-vote consensus ──
    print("\n" + "=" * 70)
    print(f"STEP 4: Full evaluation ({len(evalset)} pairs, {VOTES}-vote consensus)")
    print("=" * 70)
    t_start = time.time()
    results = [None] * len(evalset)
    done_count = 0
    lock = threading.Lock()

    def process_and_report(idx):
        nonlocal done_count
        r = eval_single(optimized, evalset[idx])
        r["idx"] = idx
        results[idx] = r
        with lock:
            done_count += 1
            elapsed = time.time() - t_start
            rate = done_count / elapsed if elapsed > 0 else 0
            eta = (len(evalset) - done_count) / rate if rate > 0 else 0
            tag = "ok" if r["correct"] else "MISS"
            print(
                f"  [{done_count:3d}/{len(evalset)}] {r['query_id']:30s} {r['patch_id']:10s} "
                f"gt={r['expected']:5s} llm={r['verdict']:5s} votes={r['votes']} "
                f"{tag}  ({rate:.1f}/s ETA {eta:.0f}s)"
            )
        return r

    with ThreadPoolExecutor(max_workers=PARALLEL_EVAL) as pool:
        futures = [pool.submit(process_and_report, i) for i in range(len(evalset))]
        for f in as_completed(futures):
            f.result()  # propagate exceptions

    elapsed_total = time.time() - t_start

    # ── Confusion matrix ──
    tp = fp = tn = fn = errors = 0
    for r in results:
        if r["verdict"] == "ERROR":
            errors += 1
            continue
        if r["expected"] == "FALSE" and r["verdict"] == "FALSE":
            tp += 1
        elif r["expected"] == "TRUE" and r["verdict"] == "FALSE":
            fp += 1
        elif r["expected"] == "TRUE" and r["verdict"] == "TRUE":
            tn += 1
        elif r["expected"] == "FALSE" and r["verdict"] == "TRUE":
            fn += 1

    correct_count = sum(1 for r in results if r["correct"])
    total = len(results)
    accuracy = correct_count / total if total else 0
    precision = tp / (tp + fp) if (tp + fp) else 0
    recall = tp / (tp + fn) if (tp + fn) else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0

    print("\n" + "=" * 70)
    print(f"FINAL RESULTS — DSPy BootstrapFewShot — {MODEL}")
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
    print(f"  Baseline devset:  {baseline_score:.3f}")
    print(f"  Optimized devset: {opt_score:.3f}")
    print()

    equiv_correct = sum(1 for r in results if r["expected"] == "TRUE" and r["correct"])
    fail_correct = sum(1 for r in results if r["expected"] == "FALSE" and r["correct"])
    equiv_total = sum(1 for r in results if r["expected"] == "TRUE")
    fail_total = sum(1 for r in results if r["expected"] == "FALSE")
    if equiv_total:
        print(f"  EQUIVALENT:      {equiv_correct}/{equiv_total} ({equiv_correct/equiv_total:.1%})")
    if fail_total:
        print(f"  CHECKSUM_FAIL:   {fail_correct}/{fail_total} ({fail_correct/fail_total:.1%})")

    # Vote distribution
    vote_patterns = Counter(tuple(r["votes"]) for r in results if r["votes"])
    print(f"\nVote patterns:")
    for pattern, count in vote_patterns.most_common(10):
        print(f"  {pattern}: {count}")

    # List misses
    misses = [r for r in results if not r["correct"] and r["verdict"] != "ERROR"]
    if misses:
        print(f"\nMisses ({len(misses)}):")
        for r in sorted(misses, key=lambda r: r["query_id"]):
            print(f"  {r['query_id']:30s} {r['patch_id']:10s} "
                  f"gt={r['expected']:5s} llm={r['verdict']:5s} votes={r['votes']}")

    # ── Save results ──
    summary = {
        "model": MODEL,
        "method": "DSPy BootstrapFewShot + 3-vote consensus",
        "votes_per_pair": VOTES,
        "parallel": PARALLEL_EVAL,
        "total": total,
        "correct": correct_count,
        "accuracy": accuracy,
        "tp": tp, "fp": fp, "tn": tn, "fn": fn,
        "precision": precision, "recall": recall, "f1": f1,
        "baseline_dev_score": baseline_score,
        "optimized_dev_score": opt_score,
        "elapsed_s": elapsed_total,
        "results": results,
    }
    with open(OUT_FILE, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nResults saved to: {OUT_FILE}")

    # Save optimized program
    PROGRAM_DIR.mkdir(exist_ok=True)
    optimized.save(str(PROGRAM_DIR))
    print(f"Optimized program saved to: {PROGRAM_DIR}/")

    # Dump the actual optimized prompt for inspection
    prompt_file = Path(__file__).parent / "dspy_optimized_prompt.txt"
    try:
        # Get the compiled prompt by inspecting the demos
        demos = optimized.judge.demos
        prompt_text = f"=== DSPy Optimized Prompt ===\n"
        prompt_text += f"Bootstrapped demos: {len([d for d in demos if hasattr(d, 'reasoning')])}\n"
        prompt_text += f"Labeled demos: {len([d for d in demos if not hasattr(d, 'reasoning')])}\n\n"
        for i, d in enumerate(demos):
            prompt_text += f"--- Demo {i+1} ---\n"
            prompt_text += f"is_equivalent: {d.get('is_equivalent', 'N/A')}\n"
            if hasattr(d, 'reasoning') or 'reasoning' in d:
                prompt_text += f"reasoning: {d.get('reasoning', '')[:500]}\n"
            prompt_text += f"query_a length: {len(d.get('query_a', ''))}\n"
            prompt_text += f"query_b length: {len(d.get('query_b', ''))}\n\n"
        with open(prompt_file, "w") as f:
            f.write(prompt_text)
        print(f"Optimized prompt demos saved to: {prompt_file}")
    except Exception as e:
        print(f"Could not dump prompt demos: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

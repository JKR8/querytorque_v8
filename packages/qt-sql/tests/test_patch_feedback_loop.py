#!/usr/bin/env python3
"""Feedback loop: generate 4 patch plans, apply, EXPLAIN, feed back, refine."""
import copy
import json
import re
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, "packages/qt-shared")
sys.path.insert(0, "packages/qt-sql")

from qt_sql.ir import build_script_ir, render_ir_node_map, Dialect, dict_to_plan, apply_patch_plan
from qt_shared.llm import create_llm_client

# ── Config ────────────────────────────────────────────────────────────
QUERY_FILE = Path("packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/queries/query001_multi_i1.sql")
OUT_DIR = Path("test_patch_logs/feedback")
MAX_ROUNDS = 3
DSN_ARGS = ["-h", "127.0.0.1", "-p", "5434", "-U", "jakc9", "-d", "dsb_sf10"]

OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Load query + build IR ─────────────────────────────────────────────
original_sql = QUERY_FILE.read_text().strip()
script_ir = build_script_ir(original_sql, Dialect.POSTGRES)
ir_node_map = render_ir_node_map(script_ir)
client = create_llm_client()


# ── Helpers ───────────────────────────────────────────────────────────
def run_query(sql: str, label: str) -> dict:
    """Run EXPLAIN ANALYZE + plain execution, return timing + row count + plan."""
    # EXPLAIN ANALYZE
    explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {sql}"
    r = subprocess.run(
        ["psql", *DSN_ARGS, "-c", explain_sql],
        capture_output=True, text=True, timeout=120,
    )
    explain_text = r.stdout

    # Extract timing
    m = re.search(r'Execution Time:\s*([\d.]+)\s*ms', explain_text)
    time_ms = float(m.group(1)) if m else None

    # Row count from plain execution
    r2 = subprocess.run(
        ["psql", *DSN_ARGS, "-t", "-c", f"SELECT COUNT(*) FROM ({sql}) _q"],
        capture_output=True, text=True, timeout=120,
    )
    try:
        row_count = int(r2.stdout.strip())
    except (ValueError, AttributeError):
        row_count = None

    # Top 8 lines of EXPLAIN for feedback
    plan_lines = [l for l in explain_text.split('\n') if l.strip()][:8]
    plan_summary = '\n'.join(plan_lines)

    return {
        "label": label,
        "time_ms": time_ms,
        "row_count": row_count,
        "plan_summary": plan_summary,
        "full_explain": explain_text,
    }


def try_apply_plan(plan_data: dict, idx: int) -> dict:
    """Apply a patch plan, return result dict."""
    plan_id = plan_data.get("plan_id", f"P{idx}")
    steps = plan_data.get("steps", [])
    result_d = {"plan_id": plan_id, "steps": len(steps), "applied": False,
                "sql": None, "error": None, "step_details": []}

    for j, s in enumerate(steps):
        result_d["step_details"].append(f"s{j+1}: {s.get('op','?')} — {s.get('description','')}")

    ir_copy = copy.deepcopy(script_ir)
    try:
        plan = dict_to_plan(plan_data)
        result = apply_patch_plan(ir_copy, plan)
    except Exception as e:
        result_d["error"] = str(e)
        return result_d

    if not result.success:
        result_d["error"] = "; ".join(result.errors[:3])
        # Try partial application
        for n in range(len(steps) - 1, 0, -1):
            partial = {"plan_id": plan_id, "dialect": "postgres", "steps": steps[:n]}
            ir_copy = copy.deepcopy(script_ir)
            try:
                p = dict_to_plan(partial)
                r = apply_patch_plan(ir_copy, p)
                if r.success:
                    result_d["sql"] = r.output_sql
                    result_d["applied"] = True
                    result_d["error"] = f"partial {n}/{len(steps)}: {result_d['error']}"
                    break
            except Exception:
                continue
        return result_d

    result_d["applied"] = True
    result_d["sql"] = result.output_sql
    return result_d


def parse_plans(response: str) -> list[dict]:
    """Extract patch plan JSON array from LLM response."""
    # Try JSON array
    m = re.search(r'\[\s*\{.*\}\s*\]', response, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            raw = re.sub(r',\s*([}\]])', r'\1', m.group(0))
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                pass

    # Try individual ```json blocks
    blocks = re.findall(r'```json\s*\n(.*?)```', response, re.DOTALL)
    plans = []
    for block in blocks:
        try:
            obj = json.loads(block.strip())
            if isinstance(obj, list):
                plans.extend(obj)
            elif isinstance(obj, dict) and "steps" in obj:
                plans.append(obj)
        except json.JSONDecodeError:
            pass
    return plans


def build_initial_prompt() -> str:
    return f"""You are a SQL rewrite engine for PostgreSQL v14.3.
Preserve exact semantic equivalence (same rows, same columns, same ordering).

## Original SQL

```sql
{original_sql}
```

## IR Node Map

```
{ir_node_map}
```

## Task

Produce **4 patch plans** — one per strategy slot below. Each plan is a self-contained
JSON object applied to the ORIGINAL IR (not chained).

### Strategy Slots (you MUST use a different strategy for each)

**P1 DECORRELATE**: Pre-compute the correlated subquery (e.g. AVG/MAX subquery in WHERE)
into a CTE, then use `replace_from` to JOIN that CTE instead, and `replace_expr_subtree`
or `delete_expr_subtree` to swap the old subquery predicate for a simple column reference.
Typical steps: `insert_cte` + `replace_from` + `replace_expr_subtree`.

**P2 PREDICATE PUSHDOWN**: Push selective WHERE filters into CTE definitions or earlier
in the FROM chain to reduce intermediate result sizes before expensive joins/subqueries.
Typical steps: `insert_cte` + `replace_expr_subtree`.

**P3 RESTRUCTURE**: Convert comma-joins to explicit JOINs via `replace_from`, or
restructure the FROM clause for better optimizer join ordering.
Typical steps: `replace_from` (possibly combined with `replace_expr_subtree` to move
predicates from WHERE into JOIN ON).

**P4 COMBINED**: Best elements of P1+P2+P3 together — decorrelate AND push predicates
AND restructure joins in a single plan.

## Available Operations

| op | required fields | description |
|---|---|---|
| `insert_cte` | `by_node_id`, `cte_name`, `cte_query_sql` | Add a new CTE |
| `replace_expr_subtree` | `by_node_id`, `by_anchor_hash`, `expr_sql` | Replace one expression |
| `replace_where_predicate` | `by_node_id`, `by_anchor_hash`, `expr_sql` | Replace ENTIRE WHERE clause |
| `replace_from` | `by_node_id`, `from_sql` | Replace entire FROM+JOINs of a SELECT |
| `delete_expr_subtree` | `by_node_id`, `by_anchor_hash` | Delete an expression |

`replace_from`: `from_sql` is everything after FROM, before WHERE. Example:
```
"from_sql": "customer_total_return AS ctr1 JOIN store_avg sa ON sa.ctr_store_sk = ctr1.ctr_store_sk JOIN store ON s_store_sk = ctr1.ctr_store_sk"
```

## CRITICAL rules

- `by_anchor_hash`: copy the 16-char hex from `[...]` in the IR Node Map. Do NOT compute your own.
- `replace_where_predicate` replaces the ENTIRE WHERE clause. If you only want to replace one predicate within a multi-AND WHERE, use `replace_expr_subtree` targeting that predicate's anchor hash.
- Every `cte_query_sql`, `expr_sql`, and `from_sql` must be complete, syntactically valid SQL.
- Each plan must preserve ALL existing predicates unless intentionally restructured.
- Fields can be flat at step level (no need to nest in target/payload).

## Output: JSON array of 4 plans

```json
[
  {{"plan_id": "P1_decorrelate", "dialect": "postgres", "steps": [...]}},
  {{"plan_id": "P2_pushdown", "dialect": "postgres", "steps": [...]}},
  {{"plan_id": "P3_restructure", "dialect": "postgres", "steps": [...]}},
  {{"plan_id": "P4_combined", "dialect": "postgres", "steps": [...]}}
]
```

After the JSON, write one line per plan: `P1: <strategy> — expected speedup: <estimate>`"""


def build_feedback_prompt(round_num: int, prev_results: list[dict], baseline: dict) -> str:
    feedback_lines = []
    feedback_lines.append(f"## Round {round_num} Results (baseline: {baseline['time_ms']:.0f}ms, {baseline['row_count']} rows)\n")

    for r in prev_results:
        pid = r["plan_id"]
        if not r["applied"]:
            feedback_lines.append(f"### {pid}: FAILED TO APPLY")
            feedback_lines.append(f"Error: {r['error']}")
            feedback_lines.append(f"Steps: {'; '.join(r['step_details'])}")
        elif r.get("row_count") != baseline["row_count"]:
            feedback_lines.append(f"### {pid}: WRONG RESULTS ({r['row_count']} rows, expected {baseline['row_count']})")
            feedback_lines.append(f"Time: {r['time_ms']:.0f}ms ({baseline['time_ms']/r['time_ms']:.2f}x)")
            feedback_lines.append(f"Steps: {'; '.join(r['step_details'])}")
            feedback_lines.append(f"EXPLAIN top:\n```\n{r['plan_summary']}\n```")
            feedback_lines.append(f"BUG: Semantic mismatch — some predicates were dropped or the rewrite changed semantics.")
        elif r["time_ms"] and r["time_ms"] < baseline["time_ms"] * 0.9:
            speedup = baseline["time_ms"] / r["time_ms"]
            feedback_lines.append(f"### {pid}: WIN {speedup:.2f}x ({r['time_ms']:.0f}ms)")
            feedback_lines.append(f"Steps: {'; '.join(r['step_details'])}")
            feedback_lines.append(f"EXPLAIN top:\n```\n{r['plan_summary']}\n```")
        else:
            feedback_lines.append(f"### {pid}: NO IMPROVEMENT ({r['time_ms']:.0f}ms)")
            feedback_lines.append(f"Steps: {'; '.join(r['step_details'])}")
            feedback_lines.append(f"EXPLAIN top:\n```\n{r['plan_summary']}\n```")
        feedback_lines.append("")

    feedback_text = '\n'.join(feedback_lines)

    return f"""You are a SQL rewrite engine for PostgreSQL v14.3.
Preserve exact semantic equivalence (same rows, same columns, same ordering).

## Original SQL

```sql
{original_sql}
```

## IR Node Map

```
{ir_node_map}
```

## Previous Round Feedback

{feedback_text}

## Task

Based on the feedback above, produce **4 improved patch plans** — one per strategy slot.
- If a plan WON, try to improve it further or combine its strategy with others.
- If a plan had WRONG RESULTS, fix the semantic error (usually: predicates dropped by replace_where_predicate — use replace_expr_subtree instead to target only the specific predicate).
- If a plan FAILED TO APPLY, simplify it or use different operations.
- If a plan had NO IMPROVEMENT, try a fundamentally different approach.

### Strategy Slots (you MUST use a different strategy for each)

**P1 DECORRELATE**: Pre-compute correlated subqueries into CTEs, use `replace_from` to JOIN them.
**P2 PREDICATE PUSHDOWN**: Push selective filters into CTE definitions or earlier FROM.
**P3 RESTRUCTURE**: Convert comma-joins to explicit JOINs via `replace_from`.
**P4 COMBINED**: Best elements of P1+P2+P3 together.

## Available Operations

| op | required fields | description |
|---|---|---|
| `insert_cte` | `by_node_id`, `cte_name`, `cte_query_sql` | Add a new CTE |
| `replace_expr_subtree` | `by_node_id`, `by_anchor_hash`, `expr_sql` | Replace one expression |
| `replace_where_predicate` | `by_node_id`, `by_anchor_hash`, `expr_sql` | Replace ENTIRE WHERE clause |
| `replace_from` | `by_node_id`, `from_sql` | Replace entire FROM+JOINs of a SELECT |
| `delete_expr_subtree` | `by_node_id`, `by_anchor_hash` | Delete an expression |

`replace_from`: `from_sql` is everything after FROM, before WHERE.

## CRITICAL rules

- `by_anchor_hash`: copy the 16-char hex from `[...]` in the IR Node Map. Do NOT invent hashes.
- `replace_where_predicate` replaces the ENTIRE WHERE clause. To replace just ONE predicate in a multi-AND WHERE, use `replace_expr_subtree` with that predicate's anchor hash.
- Every `cte_query_sql`, `expr_sql`, and `from_sql` must be complete, syntactically valid SQL.
- Each plan must preserve ALL existing predicates unless intentionally restructured.
- Fields can be flat at step level (no need for target/payload nesting).

## Output: JSON array of 4 plans

```json
[
  {{"plan_id": "P1_...", "dialect": "postgres", "steps": [...]}},
  {{"plan_id": "P2_...", "dialect": "postgres", "steps": [...]}},
  {{"plan_id": "P3_...", "dialect": "postgres", "steps": [...]}},
  {{"plan_id": "P4_...", "dialect": "postgres", "steps": [...]}}
]
```

After the JSON, write one line per plan: `P1: <strategy> — expected speedup: <estimate>`"""


# ── Main loop ─────────────────────────────────────────────────────────
print("=" * 70)
print("  PATCH FEEDBACK LOOP")
print(f"  Query: {QUERY_FILE.name}")
print(f"  Max rounds: {MAX_ROUNDS}")
print("=" * 70)

# Baseline
print("\nRunning baseline...")
baseline = run_query(original_sql, "original")
print(f"  Baseline: {baseline['time_ms']:.0f}ms, {baseline['row_count']} rows")
(OUT_DIR / "baseline_explain.txt").write_text(baseline["full_explain"])

best_overall = {"plan_id": "original", "time_ms": baseline["time_ms"], "speedup": 1.0}

for round_num in range(1, MAX_ROUNDS + 1):
    print(f"\n{'='*70}")
    print(f"  ROUND {round_num}")
    print(f"{'='*70}")

    # Build prompt
    if round_num == 1:
        prompt = build_initial_prompt()
    else:
        prompt = build_feedback_prompt(round_num, prev_results, baseline)

    prompt_file = OUT_DIR / f"r{round_num}_prompt.txt"
    prompt_file.write_text(prompt)
    print(f"  Prompt: {len(prompt)} chars")

    # LLM call (with cache)
    cache_file = OUT_DIR / f"r{round_num}_response.txt"
    if cache_file.exists() and "--no-cache" not in sys.argv:
        response = cache_file.read_text()
        print(f"  Cached response: {len(response)} chars")
    else:
        print(f"  Sending to LLM...")
        t0 = time.time()
        response = client.analyze(prompt)
        elapsed = time.time() - t0
        print(f"  Response: {len(response)} chars in {elapsed:.1f}s")
        cache_file.write_text(response)

    # Parse plans
    plans = parse_plans(response)
    print(f"  Parsed: {len(plans)} plans")

    if not plans:
        print("  ERROR: No plans parsed. Response preview:")
        print(response[:1000])
        break

    # Apply + evaluate each plan
    prev_results = []
    for i, plan_data in enumerate(plans):
        pid = plan_data.get("plan_id", f"P{i+1}")
        print(f"\n  --- {pid} ---")

        # Apply patch
        r = try_apply_plan(plan_data, i)
        print(f"    Applied: {r['applied']}", end="")
        if r["error"]:
            print(f" (error: {r['error'][:80]})")
        else:
            print()

        if r["applied"] and r["sql"]:
            # Save SQL
            sql_file = OUT_DIR / f"r{round_num}_{pid}.sql"
            sql_file.write_text(r["sql"] + ";\n")

            # Run on PG
            print(f"    Running on PG...")
            pg_result = run_query(r["sql"], pid)
            r["time_ms"] = pg_result["time_ms"]
            r["row_count"] = pg_result["row_count"]
            r["plan_summary"] = pg_result["plan_summary"]

            # Save EXPLAIN
            (OUT_DIR / f"r{round_num}_{pid}_explain.txt").write_text(pg_result["full_explain"])

            # Verdict
            correct = r["row_count"] == baseline["row_count"]
            if r["time_ms"] and correct:
                speedup = baseline["time_ms"] / r["time_ms"]
                tag = f"{'WIN' if speedup > 1.1 else 'NEUTRAL'} {speedup:.2f}x"
                if speedup > best_overall["speedup"]:
                    best_overall = {"plan_id": pid, "time_ms": r["time_ms"],
                                    "speedup": speedup, "round": round_num}
            elif not correct:
                tag = f"WRONG ROWS ({r['row_count']} vs {baseline['row_count']})"
            else:
                tag = "TIMING ERROR"

            print(f"    Result: {r['time_ms']:.0f}ms, {r['row_count']} rows → {tag}")
        else:
            r["time_ms"] = None
            r["row_count"] = None
            r["plan_summary"] = ""

        prev_results.append(r)

    # Round summary
    print(f"\n  Round {round_num} summary:")
    for r in prev_results:
        status = "APPLIED" if r["applied"] else "FAILED"
        timing = f"{r['time_ms']:.0f}ms" if r.get("time_ms") else "n/a"
        rows = r.get("row_count", "?")
        print(f"    {r['plan_id']}: {status}, {timing}, {rows} rows")

    # Save round log
    round_log = {"round": round_num, "baseline_ms": baseline["time_ms"],
                 "baseline_rows": baseline["row_count"], "results": []}
    for r in prev_results:
        round_log["results"].append({
            "plan_id": r["plan_id"], "applied": r["applied"],
            "time_ms": r.get("time_ms"), "row_count": r.get("row_count"),
            "error": r.get("error"), "steps": r["step_details"],
        })
    (OUT_DIR / f"r{round_num}_log.json").write_text(json.dumps(round_log, indent=2))

# ── Final summary ─────────────────────────────────────────────────────
print(f"\n{'='*70}")
print(f"  FINAL SUMMARY")
print(f"{'='*70}")
print(f"  Baseline: {baseline['time_ms']:.0f}ms, {baseline['row_count']} rows")
print(f"  Best: {best_overall['plan_id']} — {best_overall['time_ms']:.0f}ms ({best_overall['speedup']:.2f}x)")
print(f"  Rounds: {min(round_num, MAX_ROUNDS)}")
print(f"  All logs: {OUT_DIR}/")

#!/usr/bin/env python3
"""PoC: Convert gold example to patch format, send to LLM, apply sequentially with EXPLAIN."""
import copy
import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, "packages/qt-shared")
sys.path.insert(0, "packages/qt-sql")

from qt_sql.ir import build_script_ir, render_ir_node_map, Dialect, dict_to_plan, apply_patch_plan

# ── Load query + build IR ──────────────────────────────────────────────
BENCH_DIR = Path("packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76")
sql_path = BENCH_DIR / "queries" / "query001_multi_i1.sql"
original_sql = sql_path.read_text().strip()
script_ir = build_script_ir(original_sql, Dialect.POSTGRES)
ir_node_map = render_ir_node_map(script_ir)

out_dir = Path("test_patch_logs/sequential")
out_dir.mkdir(parents=True, exist_ok=True)

# ── Convert gold example to patch format ───────────────────────────────
# The gold example (early_filter_decorrelate) does 3 things:
#   1. Decorrelate: pre-compute AVG into store_thresholds CTE
#   2. Replace WHERE: swap correlated subquery for CTE lookup
#   3. Push s_state filter into CTE WHERE (requires FROM change too — skip)
#
# We express what our engine CAN do as a compact patch example:
patch_example = {
    "id": "early_filter_decorrelate",
    "speedup": "27.80x",
    "principle": "Decorrelate correlated AVG subquery into pre-computed CTE, then replace WHERE with CTE lookup",
    "steps": [
        {"op": "insert_cte", "cte_name": "store_thresholds",
         "sql": "SELECT ctr_store_sk, AVG(ctr_total_return) * 1.2 AS avg_limit FROM customer_total_return GROUP BY ctr_store_sk"},
        {"op": "replace_where_predicate",
         "desc": "Replace correlated subquery with threshold CTE lookup",
         "before": "ctr1.ctr_total_return > (SELECT AVG(...) FROM ctr2 WHERE correlated)",
         "after": "ctr1.ctr_total_return > (SELECT avg_limit FROM store_thresholds WHERE ...)"}
    ]
}

# ── Build prompt ───────────────────────────────────────────────────────
prompt = f"""You are a SQL rewrite engine for PostgreSQL v14.3.
Preserve exact semantic equivalence (same rows, same columns, same ordering).

## Reference Example (patch format)

**{patch_example['id']}** ({patch_example['speedup']}): {patch_example['principle']}

Steps that produced the speedup:
1. `insert_cte` name=`{patch_example['steps'][0]['cte_name']}`: `{patch_example['steps'][0]['sql']}`
2. `replace_where_predicate`: {patch_example['steps'][1]['desc']}
   BEFORE: `{patch_example['steps'][1]['before']}`
   AFTER: `{patch_example['steps'][1]['after']}`

## Original SQL

```sql
{original_sql}
```

## IR Node Map

```
{ir_node_map}
```

## Task

Apply the decorrelation pattern from the example above to this query.
Emit a single patch plan JSON with steps applied in order.

Available ops: `insert_cte`, `replace_where_predicate`, `replace_expr_subtree`, `delete_expr_subtree`
- `by_node_id`: Statement ID (e.g. `"S0"`). Required for all.
- `by_anchor_hash`: Copy the 16-char hex hash from `[...]` in the IR Node Map. Required for expression ops.
- Payload: `cte_name`+`cte_query_sql` for insert_cte, `expr_sql` for replace ops.

```json
{{"plan_id": "...", "dialect": "postgres", "steps": [...]}}
```

After JSON, write: `Changes: <summary>` and `Expected speedup: <estimate>`

Now output your Patch Plan JSON:"""

print(f"Prompt: {len(prompt)} chars ({prompt.count(chr(10))+1} lines)")
print(f"  (compare: full-SQL example would add ~60 lines; patch example adds ~6)")
(out_dir / "prompt.txt").write_text(prompt)

# ── Send to LLM ───────────────────────────────────────────────────────
from qt_shared.llm import create_llm_client

cache_path = out_dir / "response_latest.txt"
if cache_path.exists() and "--no-cache" not in sys.argv:
    response = cache_path.read_text()
    print(f"Using cached response: {len(response)} chars")
else:
    client = create_llm_client()
    print("Sending to LLM...")
    t0 = time.time()
    response = client.analyze(prompt)
    elapsed = time.time() - t0
    print(f"Response: {len(response)} chars in {elapsed:.1f}s")
    cache_path.write_text(response)

(out_dir / "response.txt").write_text(response)
print(f"\nLLM Response:\n{response}\n")

# ── Parse patch plan ───────────────────────────────────────────────────
json_match = re.search(r'```json\s*\n(.*?)```', response, re.DOTALL)
if not json_match:
    json_match = re.search(r'\{[^{}]*"plan_id".*\}', response, re.DOTALL)

if not json_match:
    print("ERROR: No JSON found")
    sys.exit(1)

raw_json = json_match.group(1) if '```' in json_match.group(0) else json_match.group(0)
patch_data = json.loads(raw_json.strip())
steps = patch_data.get("steps", [])
print(f"Parsed: {patch_data.get('plan_id', '?')} with {len(steps)} steps")

# ── Sequential application with EXPLAIN ────────────────────────────────
import subprocess

DSN = "postgres://jakc9:jakc9@127.0.0.1:5434/dsb_sf10"

def run_explain(sql: str, label: str) -> str:
    """Run EXPLAIN ANALYZE on PG and return the plan text."""
    explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {sql}"
    result = subprocess.run(
        ["psql", "-h", "127.0.0.1", "-p", "5434", "-U", "jakc9", "-d", "dsb_sf10",
         "-c", explain_sql],
        capture_output=True, text=True, timeout=120,
    )
    return result.stdout

def extract_timing(explain_text: str) -> str:
    """Extract execution time from EXPLAIN ANALYZE output."""
    match = re.search(r'Execution Time:\s*([\d.]+)\s*ms', explain_text)
    return match.group(1) if match else "?"

# Step 0: Original
print("\n" + "=" * 70)
print("  STEP 0: Original (baseline)")
print("=" * 70)

sql_file = out_dir / "step0.sql"
sql_file.write_text(original_sql + ";\n")

explain_0 = run_explain(original_sql, "original")
time_0 = extract_timing(explain_0)
(out_dir / "explain_step0.txt").write_text(explain_0)
print(f"  Execution Time: {time_0}ms")

# Show top-level plan node
for line in explain_0.split('\n')[:5]:
    if line.strip():
        print(f"  {line.strip()}")

# Apply patches one at a time
for n in range(1, len(steps) + 1):
    partial = {
        "plan_id": patch_data["plan_id"],
        "dialect": patch_data.get("dialect", "postgres"),
        "steps": steps[:n],
    }

    ir_copy = copy.deepcopy(script_ir)
    try:
        plan = dict_to_plan(partial)
        result = apply_patch_plan(ir_copy, plan)
    except Exception as e:
        print(f"\n  Step {n} FAILED: {e}")
        break

    if not result.success:
        print(f"\n  Step {n} FAILED: {'; '.join(result.errors[:3])}")
        break

    step_info = steps[n - 1]
    op = step_info.get("op", "?")
    desc = step_info.get("description", "")

    print("\n" + "=" * 70)
    print(f"  STEP {n}: +{op} — {desc}")
    print("=" * 70)

    sql = result.output_sql
    sql_file = out_dir / f"step{n}.sql"
    sql_file.write_text(sql + ";\n")

    explain_n = run_explain(sql, f"step{n}")
    time_n = extract_timing(explain_n)
    (out_dir / f"explain_step{n}.txt").write_text(explain_n)

    delta = f" ({float(time_0)/float(time_n):.2f}x vs baseline)" if time_n != "?" and time_0 != "?" else ""
    print(f"  Execution Time: {time_n}ms{delta}")

    for line in explain_n.split('\n')[:5]:
        if line.strip():
            print(f"  {line.strip()}")

# ── Summary ────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("  SUMMARY")
print("=" * 70)
print(f"  Query: query001_multi_i1")
print(f"  Example: early_filter_decorrelate (patch format: 6 lines vs 60 lines full SQL)")
print(f"  Baseline: {time_0}ms")
print(f"  All EXPLAIN plans saved to: {out_dir}/")

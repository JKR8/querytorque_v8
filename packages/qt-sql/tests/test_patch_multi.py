#!/usr/bin/env python3
"""Test: single LLM call producing multiple patch plans."""
import copy
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, "packages/qt-shared")
sys.path.insert(0, "packages/qt-sql")

from qt_sql.ir import build_script_ir, render_ir_node_map, Dialect, dict_to_plan, apply_patch_plan
from qt_sql.sql_rewriter import SQLRewriter

# ── Load query + build IR ──────────────────────────────────────────────
sql_path = Path("packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/queries/query001_multi_i1.sql")
original_sql = sql_path.read_text().strip()
script_ir = build_script_ir(original_sql, Dialect.POSTGRES)
ir_node_map = render_ir_node_map(script_ir)

out_dir = Path("test_patch_logs")
out_dir.mkdir(exist_ok=True)

# ── Build multi-plan prompt ────────────────────────────────────────────
prompt = f"""You are a SQL rewrite engine for PostgreSQL v14.3.
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

Produce **4 independent patch plans** — each a different optimization strategy.
Each plan is a self-contained JSON object. Emit them as a JSON array.

Strategies to try (one per plan):
1. **Decorrelate**: Pre-compute the correlated subquery avg into a CTE
2. **Explicit JOINs**: Convert comma-joins to explicit JOIN...ON syntax for better plan selection
3. **Early filter**: Push selective filters (birth_month, birth_year, marital_status) into a CTE to reduce join input
4. **Combined**: Decorrelate + explicit JOINs together

## Available Operations

| op | target fields | payload fields |
|---|---|---|
| `insert_cte` | `by_node_id` | `cte_name`, `cte_query_sql` |
| `replace_expr_subtree` | `by_node_id` + `by_anchor_hash` | `expr_sql` |
| `replace_where_predicate` | `by_node_id` + `by_anchor_hash` | `expr_sql` |
| `delete_expr_subtree` | `by_node_id` + `by_anchor_hash` | _(none)_ |

## Targeting

- `by_node_id`: Statement ID (e.g. `"S0"`). Required for all ops.
- `by_anchor_hash`: Copy the 16-char hex hash from the IR Node Map `[...]` brackets. Do NOT compute your own.

## Output Format

```json
[
  {{"plan_id": "P1_decorrelate", "dialect": "postgres", "steps": [...]}},
  {{"plan_id": "P2_explicit_joins", "dialect": "postgres", "steps": [...]}},
  {{"plan_id": "P3_early_filter", "dialect": "postgres", "steps": [...]}},
  {{"plan_id": "P4_combined", "dialect": "postgres", "steps": [...]}}
]
```

Rules:
- Every `cte_query_sql` and `expr_sql` must be complete, executable SQL.
- Each plan is independent (applied to the original IR, not chained).
- After the JSON array, write a brief summary of each plan's expected speedup.

Now output the JSON array of 4 patch plans:"""

print(f"Prompt: {len(prompt)} chars, {prompt.count(chr(10))+1} lines")
(out_dir / "multi_prompt.txt").write_text(prompt)

# ── Send to LLM ───────────────────────────────────────────────────────
from qt_shared.llm import create_llm_client
client = create_llm_client()

cache_path = out_dir / "multi_response_latest.txt"
if cache_path.exists() and "--no-cache" not in sys.argv:
    response = cache_path.read_text()
    print(f"Using cached response: {len(response)} chars")
else:
    print("Sending to LLM...")
    t0 = time.time()
    response = client.analyze(prompt)
    elapsed = time.time() - t0
    print(f"Response: {len(response)} chars in {elapsed:.1f}s")
    cache_path.write_text(response)

(out_dir / "multi_response.txt").write_text(response)

# ── Parse response: extract JSON array ─────────────────────────────────
import re

# Find JSON array in response
json_match = re.search(r'\[\s*\{.*\}\s*\]', response, re.DOTALL)
if not json_match:
    # Try individual JSON objects in ```json blocks
    blocks = re.findall(r'```json\s*\n(.*?)```', response, re.DOTALL)
    if blocks:
        # Try to parse each block
        plans_data = []
        for block in blocks:
            try:
                obj = json.loads(block.strip())
                if isinstance(obj, list):
                    plans_data.extend(obj)
                elif isinstance(obj, dict) and "plan_id" in obj:
                    plans_data.append(obj)
            except json.JSONDecodeError:
                pass
    else:
        print("ERROR: No JSON array found in response")
        print("Response preview:")
        print(response[:2000])
        sys.exit(1)
else:
    try:
        plans_data = json.loads(json_match.group(0))
    except json.JSONDecodeError as e:
        print(f"JSON parse error: {e}")
        # Try to fix common issues
        raw = json_match.group(0)
        # Sometimes LLMs add trailing commas
        raw = re.sub(r',\s*([}\]])', r'\1', raw)
        plans_data = json.loads(raw)

print(f"\nParsed {len(plans_data)} patch plans")
print()

# ── Apply each plan independently ──────────────────────────────────────
for i, plan_data in enumerate(plans_data):
    plan_id = plan_data.get("plan_id", f"P{i+1}")
    steps = plan_data.get("steps", [])

    print("=" * 70)
    print(f"  Plan {i+1}: {plan_id} ({len(steps)} steps)")
    print("=" * 70)

    for j, step in enumerate(steps):
        op = step.get("op", "?")
        desc = step.get("description", "")
        print(f"    s{j+1}: {op} — {desc}")

    # Apply patch
    ir_copy = copy.deepcopy(script_ir)
    try:
        plan = dict_to_plan(plan_data)
        result = apply_patch_plan(ir_copy, plan)
    except Exception as e:
        print(f"  FAILED: {e}")
        print()
        continue

    if not result.success:
        print(f"  FAILED: {'; '.join(result.errors[:3])}")
        print()
        continue

    sql = result.output_sql
    sql_file = out_dir / f"multi_plan{i+1}.sql"
    sql_file.write_text(sql + "\n")

    print(f"  Applied: {result.steps_applied}/{result.steps_total} steps")
    print(f"  Output: {len(sql)} chars → {sql_file}")
    print()

print("=" * 70)
print("  All plans saved. Run each with psql to compare timings.")
print("=" * 70)

#!/usr/bin/env python3
"""Apply patches one at a time and show the SQL after each step."""
import copy
import json
import sys
from pathlib import Path

sys.path.insert(0, "packages/qt-shared")
sys.path.insert(0, "packages/qt-sql")

from qt_sql.ir import build_script_ir, render_ir_node_map, Dialect, dict_to_plan, apply_patch_plan, render_script

# Load original SQL
sql_path = Path("packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/queries/query001_multi_i1.sql")
original_sql = sql_path.read_text().strip()

# Build IR once
script_ir = build_script_ir(original_sql, Dialect.POSTGRES)

# The patch plan from the LLM
patch_data = {
    "plan_id": "W1_S0",
    "dialect": "postgres",
    "steps": [
        {
            "step_id": "s1",
            "op": "insert_cte",
            "target": {"by_node_id": "S0"},
            "payload": {
                "cte_name": "store_avg_return",
                "cte_query_sql": "SELECT ctr_store_sk, AVG(ctr_total_return) * 1.2 AS avg_return FROM customer_total_return GROUP BY ctr_store_sk"
            },
            "description": "Precompute per-store average return amounts"
        },
        {
            "step_id": "s2",
            "op": "replace_where_predicate",
            "target": {"by_node_id": "S0", "by_anchor_hash": "a05952426f9ba20b"},
            "payload": {
                "expr_sql": "ctr1.ctr_total_return > (SELECT avg_return FROM store_avg_return WHERE ctr_store_sk = ctr1.ctr_store_sk) AND ctr1.ctr_reason_sk BETWEEN 43 AND 46 AND s_store_sk = ctr1.ctr_store_sk AND s_state IN ('IL', 'KY', 'TX') AND ctr1.ctr_customer_sk = c_customer_sk AND c_current_cdemo_sk = cd_demo_sk AND cd_marital_status IN ('M', 'M') AND cd_education_status IN ('Advanced Degree', 'College') AND cd_gender = 'F' AND c_birth_month = 2 AND c_birth_year BETWEEN 1965 AND 1971"
            },
            "description": "Replace correlated subquery with lookup into precomputed CTE"
        }
    ]
}

out_dir = Path("test_patch_logs")
out_dir.mkdir(exist_ok=True)

print("=" * 70)
print("  STEP 0: Original SQL")
print("=" * 70)
print()
print(original_sql)
print()

# Write original for psql
(out_dir / "step0_original.sql").write_text(original_sql + "\n")

# Apply patches one at a time
for n_steps in range(1, len(patch_data["steps"]) + 1):
    print("=" * 70)
    print(f"  STEP {n_steps}: Apply first {n_steps} patch(es)")
    print("=" * 70)

    # Build a partial plan with only the first n_steps
    partial_plan_data = {
        "plan_id": patch_data["plan_id"],
        "dialect": patch_data["dialect"],
        "steps": patch_data["steps"][:n_steps],
    }

    # Deepcopy IR, apply partial plan
    ir_copy = copy.deepcopy(script_ir)
    plan = dict_to_plan(partial_plan_data)
    result = apply_patch_plan(ir_copy, plan)

    if not result.success:
        print(f"  FAILED: {result.errors}")
        continue

    print(f"  Applied: {result.steps_applied}/{result.steps_total} steps")
    print(f"  Step: {patch_data['steps'][n_steps-1]['description']}")
    print()

    # Pretty-print the output SQL
    sql = result.output_sql
    out_file = out_dir / f"step{n_steps}_patched.sql"
    out_file.write_text(sql + "\n")

    print(sql)
    print()
    print(f"  Saved: {out_file}")
    print()

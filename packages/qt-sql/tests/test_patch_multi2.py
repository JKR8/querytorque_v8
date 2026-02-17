#!/usr/bin/env python3
"""Apply only the working steps from each multi-plan, run on PG."""
import copy
import json
import sys
from pathlib import Path

sys.path.insert(0, "packages/qt-shared")
sys.path.insert(0, "packages/qt-sql")

from qt_sql.ir import build_script_ir, render_ir_node_map, Dialect, dict_to_plan, apply_patch_plan

sql_path = Path("packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/queries/query001_multi_i1.sql")
original_sql = sql_path.read_text().strip()
script_ir = build_script_ir(original_sql, Dialect.POSTGRES)
out_dir = Path("test_patch_logs")

response = (out_dir / "multi_response_latest.txt").read_text()

import re
json_match = re.search(r'\[\s*\{.*\}\s*\]', response, re.DOTALL)
plans_data = json.loads(json_match.group(0))

print(f"Loaded {len(plans_data)} plans\n")

succeeded = []
for i, plan_data in enumerate(plans_data):
    plan_id = plan_data.get("plan_id", f"P{i+1}")
    all_steps = plan_data.get("steps", [])

    # Try full plan first
    ir_copy = copy.deepcopy(script_ir)
    try:
        plan = dict_to_plan(plan_data)
        result = apply_patch_plan(ir_copy, plan)
        if result.success:
            sql_file = out_dir / f"multi_p{i+1}_full.sql"
            sql_file.write_text(result.output_sql + "\n")
            print(f"  {plan_id}: FULL SUCCESS ({result.steps_applied} steps) → {sql_file.name}")
            succeeded.append((plan_id, "full", sql_file))
            continue
    except Exception:
        pass

    # Try progressive subsets: steps 1, steps 1-2, etc.
    best_n = 0
    best_sql = None
    for n in range(1, len(all_steps) + 1):
        partial = {
            "plan_id": plan_data["plan_id"],
            "dialect": plan_data.get("dialect", "postgres"),
            "steps": all_steps[:n],
        }
        ir_copy = copy.deepcopy(script_ir)
        try:
            plan = dict_to_plan(partial)
            result = apply_patch_plan(ir_copy, plan)
            if result.success:
                best_n = n
                best_sql = result.output_sql
        except Exception:
            break

    if best_sql:
        sql_file = out_dir / f"multi_p{i+1}_partial{best_n}.sql"
        sql_file.write_text(best_sql + "\n")
        failed_step = all_steps[best_n] if best_n < len(all_steps) else None
        fail_op = failed_step.get("op", "?") if failed_step else "n/a"
        print(f"  {plan_id}: {best_n}/{len(all_steps)} steps OK (failed on: {fail_op}) → {sql_file.name}")
        succeeded.append((plan_id, f"{best_n}/{len(all_steps)}", sql_file))
    else:
        print(f"  {plan_id}: ALL STEPS FAILED")

print(f"\n{'='*70}")
print(f"  {len(succeeded)}/{len(plans_data)} plans produced valid SQL")
print(f"{'='*70}\n")

for plan_id, status, sql_file in succeeded:
    print(f"  {plan_id} [{status}]: {sql_file}")

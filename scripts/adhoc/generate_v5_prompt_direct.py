#!/usr/bin/env python3
"""Generate JSON_V5 prompt for Q1 - direct imports to avoid __init__ issues."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "packages/qt-sql"))

# Direct imports
from qt_sql.optimization.dag_v2 import DagV2Pipeline
from qt_sql.optimization.dag_v3 import get_matching_examples, build_prompt_with_examples
from qt_sql.optimization.plan_analyzer import analyze_plan_for_optimization
from qt_sql.execution.database_utils import run_explain_analyze

# Q1 SQL
Q1_SQL = """WITH customer_total_return AS (
  SELECT sr_customer_sk AS ctr_customer_sk,
         sr_store_sk AS ctr_store_sk,
         SUM(SR_FEE) AS ctr_total_return
  FROM store_returns, date_dim
  WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
  GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id
FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return > (
        SELECT avg(ctr_total_return) * 1.2
        FROM customer_total_return ctr2
        WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk
      )
  AND s_store_sk = ctr1.ctr_store_sk
  AND s_state = 'SD'
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100"""

SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"

def format_plan_summary(ctx):
    """Format plan summary (copied from adaptive_rewriter_v5)."""
    lines = []

    top_ops = ctx.get_top_operators(5)
    if top_ops:
        lines.append("Operators by cost:")
        for op in top_ops:
            lines.append(f"- {op['operator']}: {op['cost_pct']}% cost, {op['rows']:,} rows")
        lines.append("")

    if ctx.table_scans:
        lines.append("Scans:")
        for scan in ctx.table_scans[:8]:
            if scan.has_filter:
                lines.append(f"- {scan.table}: {scan.rows_scanned:,} → {scan.rows_out:,} rows (filtered)")
            else:
                lines.append(f"- {scan.table}: {scan.rows_scanned:,} rows (no filter)")
        lines.append("")

    if ctx.joins:
        lines.append("Joins:")
        for j in ctx.joins[:5]:
            late = " (late)" if j.is_late else ""
            lines.append(f"- {j.join_type}: {j.left_table} x {j.right_table} -> {j.output_rows:,} rows{late}")
        lines.append("")

    return "\n".join(lines).strip() or "(execution plan not available)"

def main():
    print("Generating JSON_V5 prompt for Q1...")
    print("=" * 60)

    # Get plan context
    print("\n1. Getting execution plan context...")
    result = run_explain_analyze(SAMPLE_DB, Q1_SQL) or {}
    plan_json = result.get("plan_json")
    plan_text = result.get("plan_text") or "(execution plan not available)"

    print(f"   Plan JSON: {'Present' if plan_json else 'MISSING'}")

    if plan_json:
        ctx = analyze_plan_for_optimization(plan_json, Q1_SQL)
        plan_summary = format_plan_summary(ctx)
    else:
        plan_summary = "(execution plan not available)"

    print(f"   Plan summary length: {len(plan_summary)} chars")

    # Build base DAG prompt
    print("\n2. Building base DAG prompt...")
    pipeline = DagV2Pipeline(Q1_SQL, plan_context=ctx if plan_json else None)
    base_prompt = pipeline.get_prompt()
    print(f"   Base prompt length: {len(base_prompt)} chars")

    # Get matching examples
    print("\n3. Getting matching examples...")
    examples = get_matching_examples(Q1_SQL)
    print(f"   Total examples: {len(examples)}")
    if examples:
        print(f"   Example IDs: {[ex.id for ex in examples[:5]]}")

    # Take first 3 for worker 1
    worker1_examples = examples[:3]
    print(f"   Worker 1 will use: {[ex.id for ex in worker1_examples]}")

    # Build full prompt
    print("\n4. Building full prompt (worker 1)...")
    full_prompt = build_prompt_with_examples(
        base_prompt,
        worker1_examples,
        plan_summary,
        history_section=""
    )
    print(f"   Full prompt length: {len(full_prompt)} chars")
    print(f"   Full prompt lines: {len(full_prompt.splitlines())}")

    # Save prompt
    output_dir = Path(__file__).parent / "packages/qt-sql/prompts"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "v5_q1_prompt_fresh.txt"
    output_file.write_text(full_prompt)
    print(f"\n✓ Prompt saved to: {output_file}")

    # Show key sections
    print("\n" + "=" * 60)
    print("KEY SECTIONS:")
    print("=" * 60)

    lines = full_prompt.splitlines()
    for i, line in enumerate(lines, 1):
        if line.startswith("## "):
            print(f"Line {i}: {line}")

    print("\n" + "=" * 60)
    print("COST ATTRIBUTION SECTION:")
    print("=" * 60)

    in_cost = False
    for line in lines:
        if line.startswith("## Cost Attribution"):
            in_cost = True
        elif in_cost and line.startswith("## "):
            break
        elif in_cost:
            print(line)

if __name__ == "__main__":
    main()

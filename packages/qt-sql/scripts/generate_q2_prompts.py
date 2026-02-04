#!/usr/bin/env python3
"""Generate real prompts for Q2 across all 3 optimization modes."""

import sys
from pathlib import Path

# Add packages to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from qt_sql.optimization.dag_v2 import DagV2Pipeline
from qt_sql.optimization.dag_v3 import (
    build_prompt_with_examples,
    get_matching_examples,
    load_all_examples,
    load_example,
)
from qt_sql.optimization.query_recommender import get_query_recommendations
from qt_sql.optimization.plan_analyzer import analyze_plan_for_optimization
from qt_sql.execution.database_utils import run_explain_analyze

SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
FULL_DB = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"
QUERIES_DIR = Path("/mnt/d/TPC-DS/queries_duckdb_converted")
OUTPUT_DIR = Path(__file__).parent.parent / "prompts" / "q2"


def load_query(query_num: int) -> str:
    path = QUERIES_DIR / f"query_{query_num}.sql"
    return path.read_text()


def get_plan_context(db_path: str, sql: str):
    """Get plan summary and context."""
    result = run_explain_analyze(db_path, sql) or {}
    plan_json = result.get("plan_json")
    plan_text = result.get("plan_text") or "(execution plan not available)"
    if not plan_json:
        return "(execution plan not available)", plan_text, None

    ctx = analyze_plan_for_optimization(plan_json, sql)
    return format_plan_summary(ctx), plan_text, ctx


def format_plan_summary(ctx) -> str:
    """Compact plan summary."""
    lines = []

    scan_counts = {}
    scan_by_table = {}
    for scan in ctx.table_scans:
        scan_counts[scan.table] = scan_counts.get(scan.table, 0) + 1
        scan_by_table.setdefault(scan.table, []).append(scan)

    for table in scan_by_table:
        scan_by_table[table].sort(key=lambda s: (s.rows_scanned, s.rows_out), reverse=True)

    top_ops = ctx.get_top_operators(5)
    if top_ops:
        lines.append("Operators by cost:")
        for op in top_ops:
            label = op["operator"]
            if "SCAN" in label.upper() and scan_by_table:
                top_table = max(scan_by_table.items(), key=lambda kv: (kv[1][0].rows_scanned, kv[1][0].rows_out))[0]
                label = f"{label}({top_table})"
            lines.append(f"- {label}: {op['cost_pct']}% cost, {op['rows']:,} rows")
        lines.append("")

    if scan_by_table:
        lines.append("Scans:")
        for table, scans in sorted(scan_by_table.items(), key=lambda kv: (kv[1][0].rows_scanned, kv[1][0].rows_out), reverse=True)[:8]:
            s = scans[0]
            count = scan_counts[table]
            if s.has_filter:
                lines.append(f"- {table} x{count}: {s.rows_scanned:,} → {s.rows_out:,} rows (filtered)")
            else:
                lines.append(f"- {table} x{count}: {s.rows_scanned:,} rows (no filter)")
        lines.append("")

    return "\n".join(lines).strip() or "(execution plan not available)"


def generate_mode2_prompts(sql: str, query_id: str, plan_summary: str, plan_context):
    """Generate Mode 2 (Parallel) prompts - 4 workers with different example batches."""
    base_prompt = DagV2Pipeline(sql, plan_context=plan_context).get_prompt()

    # Get ML recommendations
    ml_recs = get_query_recommendations(query_id, top_n=12)
    print(f"ML recommendations for {query_id}: {ml_recs}")

    # Load examples
    all_examples = load_all_examples()
    all_example_ids = [ex.id for ex in all_examples]

    padded_recs = ml_recs.copy()
    for ex_id in all_example_ids:
        if len(padded_recs) >= 12:
            break
        if ex_id not in padded_recs:
            padded_recs.append(ex_id)

    example_objects = []
    for ex_id in padded_recs[:12]:
        ex = load_example(ex_id)
        if ex:
            example_objects.append(ex)

    # Create batches
    batches = [
        example_objects[0:3],
        example_objects[3:6],
        example_objects[6:9],
        example_objects[9:12],
    ]

    prompts = {}
    for i, batch in enumerate(batches):
        worker_id = i + 1
        prompt = build_prompt_with_examples(base_prompt, batch, plan_summary, "")
        prompts[f"worker_{worker_id}"] = prompt
        print(f"  Worker {worker_id}: {len(batch)} examples - {[e.id for e in batch]}")

    # Worker 5: Full SQL mode (no examples)
    worker5_prompt = f"""You are a SQL optimizer. Rewrite the ENTIRE query for maximum performance.

## Adversarial Explore Mode
Be creative and aggressive. Try radical structural rewrites that the database
engine is unlikely to do automatically. Don't be constrained by incremental changes.

## Original Query
```sql
{sql}
```

## Full Execution Plan (EXPLAIN ANALYZE)
```
{plan_summary}
```

## Instructions
1. Analyze the execution plan bottlenecks
2. Rewrite the entire query for maximum performance
3. Try transforms like:
   - Decorrelating subqueries
   - Converting OR to UNION ALL
   - Pushing down filters aggressively
   - Materializing CTEs strategically
   - Reordering joins
   - Eliminating redundant operations

## Output Format
Return ONLY the complete optimized SQL query. No JSON. No explanation. Just SQL.
"""
    prompts["worker_5_full_sql"] = worker5_prompt

    return prompts


def generate_mode1_prompt(sql: str, query_id: str, plan_summary: str, plan_context):
    """Generate Mode 1 (Retry) prompt."""
    base_prompt = DagV2Pipeline(sql, plan_context=plan_context).get_prompt()

    # Get ML recommendations
    ml_recs = get_query_recommendations(query_id, top_n=6)
    print(f"Mode 1 ML recommendations: {ml_recs}")

    examples = []
    for ex_id in ml_recs:
        if len(examples) >= 3:
            break
        ex = load_example(ex_id)
        if ex:
            examples.append(ex)

    # Pad with pattern-matched if needed
    if len(examples) < 3:
        fallback = get_matching_examples(sql)
        for ex in fallback:
            if len(examples) >= 3:
                break
            if ex not in examples:
                examples.append(ex)

    print(f"  Using examples: {[e.id for e in examples]}")
    prompt = build_prompt_with_examples(base_prompt, examples, plan_summary, "")
    return prompt


def generate_mode3_prompt(sql: str, query_id: str, plan_summary: str, plan_context):
    """Generate Mode 3 (Evolutionary) iteration 1 prompt."""
    base_prompt = DagV2Pipeline(sql, plan_context=plan_context).get_prompt()

    # Get ML recommendations
    ml_recs = get_query_recommendations(query_id, top_n=12)
    print(f"Mode 3 ML recommendations: {ml_recs}")

    all_examples = load_all_examples()
    example_by_id = {ex.id: ex for ex in all_examples}

    prioritized_examples = []
    for rec_id in ml_recs:
        if rec_id in example_by_id:
            prioritized_examples.append(example_by_id[rec_id])
        else:
            for ex_id, ex in example_by_id.items():
                if rec_id in ex_id or ex_id.startswith(rec_id):
                    if ex not in prioritized_examples:
                        prioritized_examples.append(ex)
                        break

    for ex in all_examples:
        if ex not in prioritized_examples:
            prioritized_examples.append(ex)

    # Iteration 1 uses first 3 examples
    examples = prioritized_examples[0:3]
    print(f"  Iteration 1 examples: {[e.id for e in examples]}")

    prompt = build_prompt_with_examples(base_prompt, examples, plan_summary, "")
    return prompt


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    query_num = 2
    query_id = "q2"
    sql = load_query(query_num)

    print(f"Loaded Q{query_num}: {len(sql)} chars")
    print(f"Getting execution plan from sample DB...")

    plan_summary, plan_text, plan_context = get_plan_context(SAMPLE_DB, sql)
    print(f"Plan summary: {len(plan_summary)} chars")

    # Save original query
    (OUTPUT_DIR / "original_query.sql").write_text(sql)
    (OUTPUT_DIR / "plan_summary.txt").write_text(plan_summary)

    # Mode 2: Parallel
    print("\n=== Mode 2 (Parallel) ===")
    mode2_prompts = generate_mode2_prompts(sql, query_id, plan_summary, plan_context)
    for name, prompt in mode2_prompts.items():
        path = OUTPUT_DIR / f"mode2_{name}.txt"
        path.write_text(prompt)
        print(f"  Saved: {path.name} ({len(prompt)} chars)")

    # Mode 1: Retry
    print("\n=== Mode 1 (Retry) ===")
    mode1_prompt = generate_mode1_prompt(sql, query_id, plan_summary, plan_context)
    path = OUTPUT_DIR / "mode1_retry.txt"
    path.write_text(mode1_prompt)
    print(f"  Saved: {path.name} ({len(mode1_prompt)} chars)")

    # Mode 3: Evolutionary
    print("\n=== Mode 3 (Evolutionary) ===")
    mode3_prompt = generate_mode3_prompt(sql, query_id, plan_summary, plan_context)
    path = OUTPUT_DIR / "mode3_evolutionary_iter1.txt"
    path.write_text(mode3_prompt)
    print(f"  Saved: {path.name} ({len(mode3_prompt)} chars)")

    print(f"\nAll prompts saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Capture DSPy API Payload - Save real LLM request/response to folder

This script runs a DSPy optimization on a query and saves the complete
API payload (prompt inputs and LLM response) to a folder for inspection.

Usage:
    python scripts/capture_dspy_payload.py <query_file.sql> [output_folder]

Example:
    python scripts/capture_dspy_payload.py /mnt/d/TPC-DS/queries_duckdb_converted/query_3.sql dspy_payloads/q3
"""

import sys
import json
from pathlib import Path
from datetime import datetime

# Add packages to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "packages" / "qt-sql"))
sys.path.insert(0, str(project_root / "packages" / "qt-shared"))

from qt_sql.optimization.adaptive_rewriter_v5 import _get_plan_context
from qt_sql.optimization.dag_v2 import DagBuilder
from qt_sql.optimization.dag_prompts import build_dag_structure_string, build_node_sql_string
from qt_sql.optimization.dspy_optimizer import SQLDagOptimizer, configure_lm, detect_knowledge_patterns
from qt_sql.optimization.dag_v3 import get_matching_examples
import dspy


def capture_payload(sql: str, sample_db: str, output_folder: Path):
    """Run a DSPy call and save all inputs/outputs to folder."""
    output_folder.mkdir(parents=True, exist_ok=True)

    print(f"Output folder: {output_folder}")
    print()

    # Configure LM
    print("Configuring LLM...")
    configure_lm(provider="deepseek")
    print("✅ LLM configured")
    print()

    # Build DAG
    print("Building DAG...")
    dag = DagBuilder(sql).build()
    query_dag = build_dag_structure_string(dag)
    node_sql = build_node_sql_string(dag)
    print(f"✅ DAG built: {len(dag.nodes)} nodes, {len(dag.edges)} edges")
    print()

    # Get plan
    print("Getting execution plan...")
    plan_summary, plan_text, plan_json = _get_plan_context(sample_db, sql)
    print(f"✅ Plan: {len(plan_summary)} chars")
    print()

    # Detect hints
    print("Detecting optimization hints...")
    hints = detect_knowledge_patterns(sql, dag=dag)
    print(f"✅ Hints: {len(hints)} chars")
    print()

    # Get examples
    print("Getting KB-matched examples...")
    examples = get_matching_examples(sql)
    print(f"✅ Found {len(examples)} matching examples")
    print()

    # Convert first 3 examples to DSPy demos
    demos = []
    for ex in examples[:3]:
        demo = dspy.Example(
            query_dag=ex.example.get("input_slice", ""),
            node_sql="",
            execution_plan="",
            optimization_hints=ex.example.get("opportunity", ""),
            constraints="",
            rewrites=json.dumps(ex.example.get("output", {})),
            explanation=ex.example.get("key_insight", "")
        ).with_inputs("query_dag", "node_sql", "execution_plan", "optimization_hints", "constraints")
        demos.append(demo)

    # Build optimizer with demos
    print("Creating DSPy optimizer with demos...")
    optimizer = dspy.ChainOfThought(SQLDagOptimizer)
    if demos:
        if hasattr(optimizer, "predict") and hasattr(optimizer.predict, "demos"):
            optimizer.predict.demos = demos
    print(f"✅ Optimizer configured with {len(demos)} demos")
    print()

    # Save original SQL
    (output_folder / "original.sql").write_text(sql)
    print(f"✅ Saved: original.sql")

    # Save DAG structure
    (output_folder / "dag_structure.txt").write_text(query_dag)
    print(f"✅ Saved: dag_structure.txt")

    # Save node SQL
    (output_folder / "node_sql.txt").write_text(node_sql)
    print(f"✅ Saved: node_sql.txt")

    # Save execution plan
    (output_folder / "execution_plan.txt").write_text(plan_summary)
    print(f"✅ Saved: execution_plan.txt")

    # Save full plan (detailed)
    (output_folder / "execution_plan_full.txt").write_text(plan_text)
    print(f"✅ Saved: execution_plan_full.txt")

    # Save optimization hints
    (output_folder / "optimization_hints.txt").write_text(hints)
    print(f"✅ Saved: optimization_hints.txt")

    # Save demos
    demos_list = []
    for i, demo in enumerate(demos):
        demo_dict = {
            "input_slice": demo.query_dag,
            "opportunity": demo.optimization_hints,
            "output": json.loads(demo.rewrites) if demo.rewrites else {},
            "key_insight": demo.explanation
        }
        demos_list.append(demo_dict)

    (output_folder / "demos.json").write_text(json.dumps(demos_list, indent=2))
    print(f"✅ Saved: demos.json ({len(demos)} examples)")
    print()

    # Make LLM call
    print("=" * 80)
    print("CALLING LLM...")
    print("=" * 80)
    print()

    response = optimizer(
        query_dag=query_dag,
        node_sql=node_sql,
        execution_plan=plan_summary,
        optimization_hints=hints,
        constraints="",
    )

    # Save LLM response
    (output_folder / "llm_rewrites.json").write_text(response.rewrites)
    print(f"✅ Saved: llm_rewrites.json")

    (output_folder / "llm_explanation.txt").write_text(response.explanation)
    print(f"✅ Saved: llm_explanation.txt")
    print()

    # Validate JSON
    print("=" * 80)
    print("VALIDATION")
    print("=" * 80)
    print()

    try:
        rewrites_json = json.loads(response.rewrites)
        (output_folder / "rewrites_parsed.json").write_text(json.dumps(rewrites_json, indent=2))
        print(f"✅ Rewrites is valid JSON with {len(rewrites_json)} keys")
        print(f"✅ Saved: rewrites_parsed.json")
    except json.JSONDecodeError as e:
        error_msg = f"JSON decode error: {e}"
        (output_folder / "error.txt").write_text(error_msg)
        print(f"❌ {error_msg}")
        print(f"✅ Saved: error.txt")
    print()

    # Create summary
    summary = f"""DSPy Payload Capture Summary
{'=' * 80}

Timestamp: {datetime.now().isoformat()}
Query: {Path(sys.argv[1]).name if len(sys.argv) > 1 else 'unknown'}

DAG Statistics:
- Nodes: {len(dag.nodes)}
- Edges: {len(dag.edges)}
- CTEs: {sum(1 for n in dag.nodes.values() if n.node_type == 'cte')}

Input Sizes:
- DAG Structure: {len(query_dag)} chars
- Node SQL: {len(node_sql)} chars
- Execution Plan: {len(plan_summary)} chars
- Optimization Hints: {len(hints)} chars
- Demos: {len(demos)} examples

Output:
- Rewrites: {len(response.rewrites)} chars
- Explanation: {len(response.explanation)} chars

Files Saved:
1. original.sql - Original query
2. dag_structure.txt - DAG topology and node metadata
3. node_sql.txt - SQL code for each node
4. execution_plan.txt - Compact EXPLAIN summary
5. execution_plan_full.txt - Full EXPLAIN output
6. optimization_hints.txt - Knowledge base patterns detected
7. demos.json - Gold examples used for few-shot learning
8. llm_rewrites.json - Raw LLM response (rewrites field)
9. llm_explanation.txt - LLM explanation of optimizations
10. rewrites_parsed.json - Parsed rewrites (if valid JSON)

This payload represents a complete DSPy optimization request/response cycle.
"""

    (output_folder / "README.txt").write_text(summary)
    print(summary)
    print(f"✅ Saved: README.txt")
    print()
    print(f"All files saved to: {output_folder.absolute()}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/capture_dspy_payload.py <query_file.sql> [output_folder]")
        return 1

    query_file = Path(sys.argv[1])
    if not query_file.exists():
        print(f"❌ File not found: {query_file}")
        return 1

    # Default output folder: dspy_payloads/<query_name>_<timestamp>
    if len(sys.argv) >= 3:
        output_folder = Path(sys.argv[2])
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        query_name = query_file.stem
        output_folder = Path("dspy_payloads") / f"{query_name}_{timestamp}"

    sql = query_file.read_text()
    sample_db = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"

    capture_payload(sql, sample_db, output_folder)
    return 0


if __name__ == "__main__":
    sys.exit(main())

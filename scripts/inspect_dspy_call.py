#!/usr/bin/env python3
"""
DSPy Call Inspector - Debug DSPy API calls

Shows the actual prompt sent to the LLM and the response received.
Useful for catching errors in DSPy module configuration.

Usage:
    python scripts/inspect_dspy_call.py <query_file.sql>
    python scripts/inspect_dspy_call.py <query_file.sql> --no-llm --out <path>
"""

import sys
import json
import argparse
from pathlib import Path

# Add packages to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "packages" / "qt-sql"))
sys.path.insert(0, str(project_root / "packages" / "qt-shared"))

from qt_sql.optimization.adaptive_rewriter_v5 import _get_plan_context
from qt_sql.optimization.dag_v2 import DagBuilder
from qt_sql.optimization.dag_prompts import build_dag_structure_string, build_node_sql_string


def _ensure_fake_dspy():
    """Install a minimal fake dspy module for no-LLM prompt rendering."""
    import types
    if "dspy" in sys.modules:
        return
    fake = types.ModuleType("dspy")

    class Signature:
        pass

    class Module:
        pass

    def InputField(*, desc="", default=None):
        return default

    def OutputField(*, desc="", default=None):
        return default

    class Example:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def with_inputs(self, *args):
            self.inputs = args
            return self

    class ChainOfThought:
        def __init__(self, signature):
            self.signature = signature
            self.predict = types.SimpleNamespace(demos=None)

        def __call__(self, **kwargs):
            raise RuntimeError("ChainOfThought should not be called in --no-llm mode")

    fake.Signature = Signature
    fake.Module = Module
    fake.InputField = InputField
    fake.OutputField = OutputField
    fake.Example = Example
    fake.ChainOfThought = ChainOfThought
    fake.settings = types.SimpleNamespace(lm=None)
    sys.modules["dspy"] = fake


def _demo_to_dict(demo):
    raw = getattr(demo, "kwargs", None)
    if raw is None:
        raw = getattr(demo, "_store", None)
    if raw is None and hasattr(demo, "__dict__"):
        raw = demo.__dict__
    if raw is None:
        raw = {}
    keys = [
        "query_dag",
        "node_sql",
        "execution_plan",
        "optimization_hints",
        "constraints",
        "rewrites",
        "explanation",
    ]
    out = {k: raw.get(k) for k in keys if k in raw}
    return out or raw


def _render_prompt_bundle(
    query_dag: str,
    node_sql: str,
    execution_plan: str,
    optimization_hints: str,
    constraints: str,
    demos: list,
) -> str:
    parts: list[str] = []
    parts.append("=" * 80)
    parts.append("DSPy PROMPT BUNDLE (NO LLM CALL)")
    parts.append("=" * 80)
    parts.append("")
    parts.append("## query_dag:")
    parts.append(query_dag)
    parts.append("")
    parts.append("## node_sql:")
    parts.append(node_sql)
    parts.append("")
    parts.append("## execution_plan:")
    parts.append(execution_plan)
    parts.append("")
    parts.append("## optimization_hints:")
    parts.append(optimization_hints)
    parts.append("")
    parts.append("## constraints:")
    parts.append(constraints)
    parts.append("")
    parts.append("## demos:")
    for i, demo in enumerate(demos, 1):
        d = _demo_to_dict(demo)
        parts.append(f"--- Demo {i} ---")
        parts.append("Inputs:")
        parts.append(f"query_dag:\n{d.get('query_dag','')}")
        parts.append(f"node_sql:\n{d.get('node_sql','')}")
        parts.append(f"execution_plan:\n{d.get('execution_plan','')}")
        parts.append(f"optimization_hints:\n{d.get('optimization_hints','')}")
        parts.append(f"constraints:\n{d.get('constraints','')}")
        parts.append("Outputs:")
        parts.append(f"rewrites:\n{d.get('rewrites','')}")
        parts.append(f"explanation:\n{d.get('explanation','')}")
        parts.append("")
    return "\n".join(parts).strip() + "\n"


def inspect_dspy_call(sql: str, sample_db: str, use_llm: bool = True, out_path: str | None = None):
    """Run a DSPy call and show all internals."""
    if not use_llm:
        _ensure_fake_dspy()

    from qt_sql.optimization.dspy_optimizer import (
        SQLDagOptimizer,
        configure_lm,
        detect_knowledge_patterns,
        build_dag_constraints,
        load_dag_gold_examples,
    )
    import dspy

    if use_llm:
        # Configure LM with verbose logging
        configure_lm(provider="deepseek")

    # Build DAG
    dag = DagBuilder(sql).build()
    query_dag = build_dag_structure_string(dag)
    node_sql = build_node_sql_string(dag)

    # Get plan
    plan_summary, plan_text, plan_json = _get_plan_context(sample_db, sql)

    # Detect hints
    hints = detect_knowledge_patterns(sql, dag=dag)
    constraints = build_dag_constraints()
    demos = load_dag_gold_examples(3)

    if not use_llm:
        prompt_text = _render_prompt_bundle(
            query_dag=query_dag,
            node_sql=node_sql,
            execution_plan=plan_summary,
            optimization_hints=hints,
            constraints=constraints,
            demos=demos,
        )
        if out_path:
            Path(out_path).write_text(prompt_text)
        else:
            print(prompt_text)
        return

    # Show inputs
    print("=" * 80)
    print("INPUTS TO LLM")
    print("=" * 80)
    print()
    print("## query_dag:")
    print(query_dag)
    print()
    print("## node_sql:")
    print(node_sql)
    print()
    print("## execution_plan:")
    print(plan_summary)
    print()
    print("## optimization_hints:")
    print(hints)
    print()

    # Make call
    print("=" * 80)
    print("CALLING LLM...")
    print("=" * 80)
    print()

    # Build optimizer (no demos for inspector)
    print("Creating DSPy optimizer...")
    optimizer = dspy.ChainOfThought(SQLDagOptimizer)
    if demos:
        if hasattr(optimizer, "predict") and hasattr(optimizer.predict, "demos"):
            optimizer.predict.demos = demos
        elif hasattr(optimizer, "demos"):
            optimizer.demos = demos
    print("✅ Optimizer configured")
    print()

    response = optimizer(
        query_dag=query_dag,
        node_sql=node_sql,
        execution_plan=plan_summary,
        optimization_hints=hints,
        constraints=constraints,
    )

    # Show outputs
    print("=" * 80)
    print("OUTPUTS FROM LLM")
    print("=" * 80)
    print()
    print("## rewrites:")
    print(response.rewrites)
    print()
    print("## explanation:")
    print(response.explanation)
    print()

    # Try to parse rewrites as JSON
    print("=" * 80)
    print("VALIDATION")
    print("=" * 80)
    print()

    try:
        rewrites_json = json.loads(response.rewrites)
        print(f"✅ Rewrites is valid JSON with {len(rewrites_json)} keys")
    except json.JSONDecodeError as e:
        print(f"❌ Rewrites is NOT valid JSON: {e}")
        print("   This will likely cause validation to fail!")
    print()


def main():
    parser = argparse.ArgumentParser(description="DSPy Call Inspector")
    parser.add_argument("query_file", help="Path to SQL file")
    parser.add_argument("--no-llm", action="store_true", help="Do not call the LLM; render prompt bundle only")
    parser.add_argument("--out", help="Write prompt bundle to file (use with --no-llm)")
    parser.add_argument("--sample-db", default="/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb")
    args = parser.parse_args()

    query_file = Path(args.query_file)
    if not query_file.exists():
        print(f"❌ File not found: {query_file}")
        return 1

    sql = query_file.read_text()
    sample_db = args.sample_db

    inspect_dspy_call(sql, sample_db, use_llm=not args.no_llm, out_path=args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
DSPy V5 API Call Example

This demonstrates the correct way to call the DSPy v5 optimizer
with DAG-based optimization and validation.

Usage:
    export DEEPSEEK_API_KEY=your_key_here
    python dspy_v5_api_example.py
"""

import os
import sys
from pathlib import Path

# Add packages to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "packages" / "qt-sql"))
sys.path.insert(0, str(project_root / "packages" / "qt-shared"))

def check_environment():
    """Verify required environment variables and dependencies."""
    if not os.getenv("DEEPSEEK_API_KEY"):
        print("ERROR: Set DEEPSEEK_API_KEY environment variable")
        print("  export DEEPSEEK_API_KEY=your_key_here")
        sys.exit(1)

    try:
        import dspy
    except ImportError:
        print("ERROR: dspy-ai not installed")
        print("  pip install dspy-ai")
        sys.exit(1)

    print("✓ Environment check passed")


def simple_dspy_v5_call():
    """Simple DSPy v5 API call example - demonstrates the core pattern."""
    import dspy
    from qt_sql.optimization.dspy_optimizer import (
        configure_lm,
        SQLDagOptimizer,
        load_dag_gold_examples,
        detect_knowledge_patterns
    )
    from qt_sql.optimization.sql_dag import SQLDag
    from qt_sql.optimization.dag_v2 import DagV2Pipeline

    print("\n" + "="*70)
    print("SIMPLE DSPy V5 API CALL")
    print("="*70)

    # Step 1: Configure the language model
    print("\n[1/5] Configuring DeepSeek LLM...")
    configure_lm(provider="deepseek")

    # Step 2: Load the SQL query
    sql = """
    WITH customer_total_return AS (
        SELECT
            sr_customer_sk AS ctr_customer_sk,
            sr_store_sk AS ctr_store_sk,
            SUM(SR_FEE) AS ctr_total_return
        FROM store_returns, date_dim
        WHERE sr_returned_date_sk = d_date_sk
            AND d_year = 2000
        GROUP BY sr_customer_sk, sr_store_sk
    )
    SELECT c_customer_id
    FROM customer_total_return ctr1, store, customer
    WHERE ctr1.ctr_total_return > (
        SELECT AVG(ctr_total_return) * 1.2
        FROM customer_total_return ctr2
        WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk
    )
    AND s_store_sk = ctr1.ctr_store_sk
    AND s_state = 'SD'
    AND ctr1.ctr_customer_sk = c_customer_sk
    ORDER BY c_customer_id
    LIMIT 100;
    """.strip()

    print(f"✓ Query loaded ({len(sql)} chars)")

    # Step 3: Build DAG representation
    print("\n[2/5] Building DAG representation...")
    dag = SQLDag.from_sql(sql)

    # Format DAG structure
    dag_structure = ["Nodes:"]
    for node_id in dag.topological_order():
        node = dag.nodes[node_id]
        parts = [f"  [{node_id}]", f"type={node.node_type}"]
        if node.tables:
            parts.append(f"tables={node.tables}")
        if node.cte_refs:
            parts.append(f"refs={node.cte_refs}")
        if node.is_correlated:
            parts.append("CORRELATED")
        dag_structure.append(" ".join(parts))

    dag_structure.append("\nEdges:")
    for edge in dag.edges:
        dag_structure.append(f"  {edge.source} → {edge.target}")

    query_dag = "\n".join(dag_structure)
    print(f"✓ DAG built: {len(dag.nodes)} nodes, {len(dag.edges)} edges")

    # Step 4: Format node SQL
    print("\n[3/5] Formatting node SQL...")
    node_sql_parts = []
    for node_id in dag.topological_order():
        node = dag.nodes[node_id]
        if node.sql:
            node_sql_parts.append(f"### {node_id}\n```sql\n{node.sql.strip()}\n```")
    node_sql = "\n\n".join(node_sql_parts)
    print(f"✓ {len(node_sql_parts)} nodes formatted")

    # Step 5: Detect optimization opportunities
    print("\n[4/5] Detecting optimization opportunities...")
    hints = detect_knowledge_patterns(sql, dag=dag)
    print(f"✓ {len(hints) if hints else 0} optimization hints detected")

    # Step 6: Load few-shot examples and call DSPy
    print("\n[5/5] Calling DSPy optimizer...")
    demos = load_dag_gold_examples(3)
    print(f"✓ Loaded {len(demos)} few-shot examples")

    # Create optimizer with ChainOfThought
    optimizer = dspy.ChainOfThought(SQLDagOptimizer)

    # Set few-shot demos
    if demos:
        if hasattr(optimizer, 'predict') and hasattr(optimizer.predict, 'demos'):
            optimizer.predict.demos = demos
        elif hasattr(optimizer, 'demos'):
            optimizer.demos = demos

    # Execution plan (simplified for this example)
    execution_plan = """
    Operators by cost:
    - SEQ_SCAN (customer): 73.4% cost, 1,999,335 rows
    - HASH_JOIN: 11.0% cost, 7,986 rows
    - SEQ_SCAN (store_returns): 5.2% cost, 56,138 rows
    """

    # Make the API call
    print("\n→ Sending request to DeepSeek...")
    result = optimizer(
        query_dag=query_dag,
        node_sql=node_sql,
        execution_plan=execution_plan,
        optimization_hints=hints,
        constraints=""
    )

    print("✓ Response received")

    # Step 7: Parse and apply rewrites
    print("\n" + "="*70)
    print("RESULT")
    print("="*70)

    print("\n[Rewrites JSON]")
    print(result.rewrites)

    print("\n[Explanation]")
    print(result.explanation)

    # Apply rewrites using DagV2Pipeline
    try:
        pipeline = DagV2Pipeline(sql)
        optimized_sql = pipeline.apply_response(result.rewrites)

        print("\n[Optimized SQL]")
        print(optimized_sql)
    except Exception as e:
        print(f"\n⚠ Could not apply rewrites: {e}")

    return result


def full_dspy_v5_pipeline():
    """Full DSPy v5 pipeline with validation - production-ready pattern."""
    from qt_sql.optimization.adaptive_rewriter_v5 import optimize_v5_dspy

    print("\n" + "="*70)
    print("FULL DSPy V5 PIPELINE (With Validation)")
    print("="*70)

    # Check if sample database exists
    sample_db = "D:/TPC-DS/tpcds_sf1.duckdb"
    if not Path(sample_db).exists():
        print(f"\n⚠ Sample database not found: {sample_db}")
        print("  Skipping validation example")
        return None

    sql = """
    WITH customer_total_return AS (
        SELECT
            sr_customer_sk AS ctr_customer_sk,
            sr_store_sk AS ctr_store_sk,
            SUM(SR_FEE) AS ctr_total_return
        FROM store_returns, date_dim
        WHERE sr_returned_date_sk = d_date_sk
            AND d_year = 2000
        GROUP BY sr_customer_sk, sr_store_sk
    )
    SELECT c_customer_id
    FROM customer_total_return ctr1, store, customer
    WHERE ctr1.ctr_total_return > (
        SELECT AVG(ctr_total_return) * 1.2
        FROM customer_total_return ctr2
        WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk
    )
    AND s_store_sk = ctr1.ctr_store_sk
    AND s_state = 'SD'
    AND ctr1.ctr_customer_sk = c_customer_sk
    ORDER BY c_customer_id
    LIMIT 100;
    """.strip()

    print("\nRunning optimize_v5_dspy with validation...")
    print("  - 5 parallel workers")
    print("  - Automatic retry on failure")
    print("  - Result validation on sample DB")

    result = optimize_v5_dspy(
        sql=sql,
        sample_db=sample_db,
        max_workers=5,
        provider="deepseek"
    )

    print("\n" + "="*70)
    print("VALIDATION RESULT")
    print("="*70)

    print(f"\nWorker ID: {result.worker_id}")
    print(f"Status: {result.status.value}")
    print(f"Speedup: {result.speedup:.2f}x")

    if result.error:
        print(f"Error: {result.error}")

    print("\n[Optimized SQL]")
    print(result.optimized_sql[:500] + "..." if len(result.optimized_sql) > 500 else result.optimized_sql)

    print("\n[Response]")
    print(result.response[:300] + "..." if len(result.response) > 300 else result.response)

    return result


def main():
    """Main entry point."""
    check_environment()

    print("\n" + "="*70)
    print("DSPy V5 API EXAMPLES")
    print("="*70)
    print("\nThis script demonstrates two patterns:")
    print("  1. Simple API call - shows core DSPy v5 pattern")
    print("  2. Full pipeline - production-ready with validation")

    # Example 1: Simple API call
    try:
        simple_dspy_v5_call()
    except Exception as e:
        print(f"\n✗ Simple call failed: {e}")
        import traceback
        traceback.print_exc()

    # Example 2: Full pipeline (skip if no database)
    try:
        full_dspy_v5_pipeline()
    except Exception as e:
        print(f"\n✗ Full pipeline failed: {e}")
        import traceback
        traceback.print_exc()

    print("\n" + "="*70)
    print("DONE")
    print("="*70)


if __name__ == "__main__":
    main()

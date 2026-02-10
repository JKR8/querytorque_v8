#!/usr/bin/env python3
"""
Test Query 9 Optimization (Dry Run)

Validates all components work without making API call.
"""

import sys
from pathlib import Path

# Add packages to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "packages" / "qt-sql"))
sys.path.insert(0, str(project_root / "packages" / "qt-shared"))

print("=" * 80)
print("Query 9 DSPy Optimization - Dry Run Test")
print("=" * 80)
print()

# 1. Load query
print("Step 1: Load query file")
query_file = Path("/mnt/d/TPC-DS/queries_duckdb_converted/query_9.sql")
if not query_file.exists():
    print(f"❌ File not found: {query_file}")
    sys.exit(1)

sql = query_file.read_text()
print(f"✅ Loaded query: {len(sql)} chars")
print(f"   First 100 chars: {sql[:100]}...")
print()

# 2. Test imports
print("Step 2: Test imports")
try:
    from qt_sql.optimization.adaptive_rewriter_v5 import optimize_v5_dspy, _get_plan_context
    from qt_sql.optimization.dag_v2 import DagBuilder
    from qt_sql.optimization.dag_prompts import build_dag_structure_string, build_node_sql_string
    from qt_sql.optimization.dspy_optimizer import detect_knowledge_patterns
    from qt_sql.optimization.dag_v3 import get_matching_examples
    print("✅ All imports successful")
except Exception as e:
    print(f"❌ Import failed: {e}")
    sys.exit(1)
print()

# 3. Build DAG
print("Step 3: Build DAG with dag_v2")
try:
    dag = DagBuilder(sql).build()
    print(f"✅ DAG built successfully")
    print(f"   Nodes: {len(dag.nodes)}")
    print(f"   Edges: {len(dag.edges)}")
    print(f"   CTEs: {sum(1 for n in dag.nodes.values() if n.node_type == 'cte')}")
    print()
    print("   Node IDs:")
    for node_id in dag.nodes:
        print(f"     - {node_id} (type={dag.nodes[node_id].node_type})")
except Exception as e:
    print(f"❌ DAG build failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
print()

# 4. Test DAG prompt builders
print("Step 4: Test DAG prompt builders")
try:
    dag_structure = build_dag_structure_string(dag)
    node_sql = build_node_sql_string(dag)
    print(f"✅ DAG prompts built")
    print(f"   Structure: {len(dag_structure)} chars")
    print(f"   Node SQL: {len(node_sql)} chars")
    print()
    print("   DAG Structure Preview:")
    print("   " + "\n   ".join(dag_structure.split("\n")[:10]))
except Exception as e:
    print(f"❌ DAG prompt building failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
print()

# 5. Get execution plan
print("Step 5: Get execution plan")
sample_db = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
try:
    plan_summary, plan_text, plan_json = _get_plan_context(sample_db, sql)
    print(f"✅ Execution plan retrieved")
    print(f"   Summary: {len(plan_summary)} chars")
    print(f"   Full plan: {len(plan_text)} chars")
    print()
    print("   Plan Summary Preview:")
    print("   " + "\n   ".join(plan_summary.split("\n")[:5]))
except Exception as e:
    print(f"❌ Plan retrieval failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
print()

# 6. Detect KB patterns
print("Step 6: Detect knowledge base patterns")
try:
    hints = detect_knowledge_patterns(sql, dag=dag)
    print(f"✅ KB patterns detected")
    print(f"   Hints: {len(hints)} chars")
    print()
    if hints:
        print("   Detected Patterns:")
        print("   " + "\n   ".join(hints.split("\n")[:15]))
    else:
        print("   (No patterns detected)")
except Exception as e:
    print(f"❌ Pattern detection failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
print()

# 7. Get matching examples
print("Step 7: Get KB-matched examples")
try:
    examples = get_matching_examples(sql)
    print(f"✅ Examples retrieved: {len(examples)} matches")
    if examples:
        print()
        print("   Top 3 examples:")
        for i, ex in enumerate(examples[:3], 1):
            opportunity = ex.example.get("opportunity", "N/A")[:60]
            print(f"     {i}. {opportunity}...")
except Exception as e:
    print(f"❌ Example retrieval failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
print()

# 8. Test example batching
print("Step 8: Test example batching")
try:
    from qt_sql.optimization.adaptive_rewriter_v5 import _split_example_batches
    batches = _split_example_batches(examples, batch_size=3)
    print(f"✅ Example batching works")
    print(f"   Total batches: {len(batches)}")
    for i, batch in enumerate(batches[:4], 1):
        print(f"     Batch {i}: {len(batch)} examples")
except Exception as e:
    print(f"❌ Example batching failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
print()

# Summary
print("=" * 80)
print("DRY RUN SUMMARY")
print("=" * 80)
print()
print("✅ All components validated successfully!")
print()
print("Ready to optimize with the following configuration:")
print(f"  - Query: query_9.sql ({len(sql)} chars)")
print(f"  - DAG: {len(dag.nodes)} nodes, {len(dag.edges)} edges")
print(f"  - KB Patterns: {len(hints)} chars of hints")
print(f"  - Examples: {len(examples)} gold examples, {len(batches)} batches")
print(f"  - Workers: 5 parallel (4 with examples, 1 explore mode)")
print()
print("To run the actual optimization:")
print("  export DEEPSEEK_API_KEY=$(cat DeepseekV3.txt)")
print("  .venv/bin/python -c \"")
print("from qt_sql.optimization.adaptive_rewriter_v5 import optimize_v5_dspy")
print("result = optimize_v5_dspy(")
print("    sql=open('/mnt/d/TPC-DS/queries_duckdb_converted/query_9.sql').read(),")
print("    sample_db='/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb',")
print("    max_workers=5,")
print("    provider='deepseek'")
print(")")
print("print(f'Status: {result.status.value}')")
print("print(f'Speedup: {result.speedup:.2f}x')")
print("\"")
print()

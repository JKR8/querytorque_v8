#!/usr/bin/env python3
"""Test DSPy V5 optimizer on Q1"""
import os, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "packages" / "qt-shared"))
sys.path.insert(0, str(ROOT / "packages" / "qt-sql"))

from dotenv import load_dotenv
load_dotenv()

api_key = os.environ.get("DEEPSEEK_API_KEY") or os.environ.get("QT_DEEPSEEK_API_KEY")
if not api_key:
    print("❌ No API key"); sys.exit(1)
os.environ["DEEPSEEK_API_KEY"] = api_key

q1_sql = """with customer_total_return as
(select sr_customer_sk as ctr_customer_sk
,sr_store_sk as ctr_store_sk
,sum(SR_FEE) as ctr_total_return
from store_returns
,date_dim
where sr_returned_date_sk = d_date_sk
and d_year =2000
group by sr_customer_sk
,sr_store_sk)
 select c_customer_id
from customer_total_return ctr1
,store
,customer
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
from customer_total_return ctr2
where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'SD'
and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
 LIMIT 100"""

print("="*80)
print("DSPy V5 Test - Q1 with DeepSeek")
print("="*80)

import dspy
from qt_sql.optimization.dspy_optimizer import configure_lm, SQLDagOptimizer, load_dag_gold_examples
from qt_sql.optimization.dag_v2 import DagBuilder
from qt_sql.optimization.dag_prompts import build_dag_structure_string, build_node_sql_string

print("\n1. Configuring DeepSeek...")
configure_lm(provider="deepseek")
print("✅ Done")

print("\n2. Building DAG...")
dag = DagBuilder(q1_sql).build()
query_dag = build_dag_structure_string(dag)
node_sql = build_node_sql_string(dag)
print(f"✅ DAG has {len(dag.nodes)} nodes")

print("\n3. Loading gold examples...")
demos = load_dag_gold_examples(3)
print(f"✅ Loaded {len(demos)} examples (Q15, Q39, Q23)")

print("\n4. Creating optimizer with ChainOfThought...")
optimizer = dspy.ChainOfThought(SQLDagOptimizer)
if hasattr(optimizer, 'predict') and hasattr(optimizer.predict, 'demos'):
    optimizer.predict.demos = demos
    print(f"✅ Attached {len(demos)} demos to optimizer")

print("\n5. Calling DeepSeek API...")
print("   (This takes 10-30 seconds)")
response = optimizer(
    query_dag=query_dag,
    node_sql=node_sql,
    execution_plan="GROUP_BY cost=45%, HASH_JOIN cost=30%, SEQ_SCAN cost=20%",
    optimization_hints="SQL-CORR-001: Correlated subquery, SQL-JOIN-001: Multiple joins",
    constraints=""
)

print("\n" + "="*80)
print("RESULT")
print("="*80)
print(f"\nRewrites:\n{response.rewrites}\n")
print(f"Explanation:\n{response.explanation}\n")
print("="*80)
print("✅ Test complete!")

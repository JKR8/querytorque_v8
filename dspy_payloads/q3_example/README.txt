DSPy Payload Capture Summary
================================================================================

Timestamp: 2026-02-04T16:16:05.273788
Query: query_3.sql

DAG Statistics:
- Nodes: 1
- Edges: 0
- CTEs: 0

Input Sizes:
- DAG Structure: 82 chars
- Node SQL: 466 chars
- Execution Plan: 545 chars
- Optimization Hints: 294 chars
- Demos: 3 examples

Output:
- Rewrites: 547 chars
- Explanation: 657 chars

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

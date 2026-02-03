# SQL Optimization Prompts

This folder contains all prompts used by the SQL optimization system.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    OPTIMIZATION MODES                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   DSPy Pipeline                    MCTS Pipeline                │
│   ─────────────                    ─────────────                │
│                                                                 │
│   ┌─────────────┐                  ┌─────────────┐              │
│   │  Normal     │                  │  Regular    │              │
│   │  (full SQL) │                  │  (full SQL) │              │
│   └──────┬──────┘                  └──────┬──────┘              │
│          │                                │                     │
│   ┌──────┴──────┐                  ┌──────┴──────┐              │
│   │  DAG        │                  │  DAG        │              │
│   │  (node-level│                  │  (node-level│              │
│   │   rewrites) │                  │   rewrites) │              │
│   └─────────────┘                  └─────────────┘              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Folder Structure

```
packages/qt-sql/qt_sql/optimization/prompts/
├── mcts_regular/         # MCTS: Full SQL rewrite templates
│   ├── push_pred.txt
│   ├── reorder_join.txt
│   ├── materialize_cte.txt
│   ├── inline_cte.txt
│   ├── flatten_subq.txt
│   ├── opt_agg.txt
│   ├── remove_redundant.txt
│   ├── opt_window.txt
│   └── multi_push_pred.txt
│
├── mcts_dag/             # MCTS: DAG node-level rewrite templates
│   └── (same 9 transforms)
│
├── dspy/                 # DSPy: Signature definitions
│   ├── sql_optimizer.txt              # Normal mode
│   ├── sql_optimizer_with_feedback.txt # Normal + retry
│   ├── sql_dag_optimizer.txt          # DAG mode
│   └── sql_dag_optimizer_with_feedback.txt # DAG + retry
│
└── examples/             # Fully expanded examples (q67)
    ├── regular_push_pred_q67.txt     # MCTS regular
    ├── dag_context_q67.txt           # DAG structure only
    ├── dag_push_pred_q67.txt         # MCTS DAG full prompt
    ├── dag_multi_push_pred_q67.txt   # MCTS DAG + pushdown
    ├── dspy_normal_q67.txt           # DSPy normal mode
    └── dspy_dag_q67.txt              # DSPy DAG mode
```

## Transform Types (9 total)

| ID | Name | Description |
|----|------|-------------|
| push_pred | Predicate Pushdown | Push WHERE closer to base tables |
| reorder_join | Join Reordering | Put selective tables first |
| materialize_cte | CTE Materialization | Extract repeated subqueries to CTEs |
| inline_cte | CTE Inlining | Inline single-use CTEs |
| flatten_subq | Subquery Flattening | Convert correlated subqueries to JOINs |
| opt_agg | Aggregation Optimization | Optimize GROUP BY, push aggregates |
| remove_redundant | Redundancy Removal | Remove unnecessary DISTINCT, columns |
| opt_window | Window Optimization | Combine window functions, fix frames |
| multi_push_pred | Multi-Node Pushdown | Push predicates through multiple CTE layers |

## Mode Comparison

### Regular Mode (MCTS)
- Input: `{query}` placeholder
- Output: Plain SQL
- Simpler, less tokens
- Good for small-medium queries

### DAG Mode (MCTS)
- Input: `{dag_prompt}` with full DAG structure
- Output: JSON with node rewrites
- Better for large queries
- Preserves unchanged parts

### DSPy Normal Mode
- Input: query + execution_plan + row_estimates + hints + constraints
- Output: optimized_query + rationale
- Uses ChainOfThought reasoning
- Few-shot examples loaded
- Validation + retry loop

### DSPy DAG Mode
- Input: query_dag + node_sql + execution_plan + hints + constraints
- Output: JSON rewrites + explanation
- Node-level targeting
- Fewer tokens for large queries

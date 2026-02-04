# SQL Optimization Prompts

This folder contains all prompts used by the SQL optimization system.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    OPTIMIZATION MODES                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   JSON v5 Pipeline                                                │
│   ───────────────                                                │
│                                                                 │
│   ┌─────────────┐                                                │
│   │  DAG        │                                                │
│   │  (node-level│                                                │
│   │   rewrites) │                                                │
│   └─────────────┘                                                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Folder Structure

```
packages/qt-sql/qt_sql/optimization/prompts/
└── README.md
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

### JSON v5 (DAG v2)
- Input: DAG v2 prompt + plan summary + gold JSON examples
- Output: JSON rewrites applied to DAG v2
- Node-level targeting
- Validation on sample DB with optional retry

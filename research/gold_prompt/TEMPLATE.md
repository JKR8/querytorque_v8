# Gold Standard Optimization Prompt Template

> **Proven:** 2.18x speedup on Q23 with Gemini (2025-02-01)

---

## Structure

```
Optimize this SQL query.

## Execution Plan              ← From EXPLAIN ANALYZE (optional, adds signal)
## Block Map                   ← AST-derived clause structure
## Optimization Patterns       ← Known patterns that produce >2x speedups
## SQL                         ← Original query
## Output                      ← Operations JSON format + rules
```

---

## Key Elements

### 1. Block Map (AST-derived)
Shows CTEs and clauses with:
- Content summaries
- CTE references (`main_query.from → cte_name`)
- Repeated scans (`date_dim: 4×`)
- Filter gaps (`scans X WITHOUT filter, refs Y which HAS filter`)

### 2. Operations Format
| Op | Fields | Description |
|----|--------|-------------|
| `add_cte` | `after`, `name`, `sql` | Insert new CTE |
| `delete_cte` | `name` | Remove CTE |
| `replace_cte` | `name`, `sql` | Replace entire CTE body |
| `replace_clause` | `target`, `sql` | Replace clause |
| `patch` | `target`, `patches[]` | Snippet search/replace |

### 3. Critical Rules
1. **1-5 operations maximum** - focus on highest-impact changes
2. When removing a join, add `WHERE fk_column IS NOT NULL`
3. When removing a table, alias the FK column (e.g., `ss_customer_sk AS c_customer_sk`)

---

## Generation

```python
from qt_sql.optimization import build_full_prompt

# Basic (no execution plan)
prompt = build_full_prompt(sql)

# With execution plan (adds operator cost %, row counts)
prompt = build_full_prompt(sql, plan_summary)
```

See `generate.py` for full example with plan extraction.

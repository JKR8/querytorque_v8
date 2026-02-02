#!/usr/bin/env python3
"""
Prompt Builder V2 - Uses plan_analyzer for proper plan parsing

Usage:
    .venv/bin/python research/scripts/build_prompt_v2.py q1
    .venv/bin/python research/scripts/build_prompt_v2.py q1 --output prompt.txt
"""

import argparse
import sys
import re
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import duckdb
import sqlglot
from sqlglot import exp

from qt_sql.optimization.plan_analyzer import (
    analyze_plan_for_optimization,
    OptimizationContext,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Build V2 optimization prompt")
    parser.add_argument("query", help="Query ID (e.g., q1, q15)")
    parser.add_argument("--output", "-o", help="Output file (default: stdout)")
    parser.add_argument("--db", default="/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb",
                        help="Database path")
    return parser.parse_args()


def load_sql(query_id: str) -> str:
    """Load SQL from batch prompt file."""
    prompt_path = Path(__file__).parent.parent / "prompts" / "batch" / f"{query_id}_prompt.txt"
    if not prompt_path.exists():
        raise FileNotFoundError(f"No prompt file for {query_id}")

    content = prompt_path.read_text()
    sql_match = re.search(r'```sql\n(.*?)```', content, re.DOTALL)
    if sql_match:
        return sql_match.group(1).strip()
    raise ValueError(f"No SQL found in {prompt_path}")


def get_plan_json(sql: str, db_path: str) -> dict:
    """Get execution plan as JSON from DuckDB using profiling."""
    import tempfile
    import os

    # Need non-read-only for profiling
    conn = duckdb.connect(db_path, read_only=False)

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        profile_path = f.name

    try:
        conn.execute("PRAGMA enable_profiling='json'")
        conn.execute(f"PRAGMA profiling_output='{profile_path}'")

        # Run the query to generate profile
        conn.execute(sql).fetchall()

        # Read the JSON profile
        if os.path.exists(profile_path) and os.path.getsize(profile_path) > 0:
            with open(profile_path) as f:
                return json.load(f)
    except Exception as e:
        pass
    finally:
        if os.path.exists(profile_path):
            os.unlink(profile_path)
        conn.close()

    return {}


def annotate_sql_with_blocks(sql: str) -> str:
    """Parse SQL and annotate with block markers."""
    try:
        parsed = sqlglot.parse_one(sql, error_level=sqlglot.ErrorLevel.IGNORE)
    except:
        return sql

    blocks = []

    # Extract CTEs
    for cte in parsed.find_all(exp.CTE):
        cte_name = str(cte.alias) if cte.alias else "unnamed_cte"
        cte_sql = cte.this.sql() if cte.this else ""

        inner = cte.this
        tables = [str(t.name) for t in inner.find_all(exp.Table)] if inner else []
        has_group = bool(inner.find(exp.Group)) if inner else False
        has_window = bool(inner.find(exp.Window)) if inner else False

        tags = []
        if tables:
            tags.append(f"tables={','.join(tables[:3])}")
        if has_group:
            tags.append("GROUP BY")
        if has_window:
            tags.append("WINDOW")

        blocks.append(f"-- [BLOCK: {cte_name}] {' | '.join(tags)}")
        blocks.append(cte_sql)
        blocks.append("")

    # Main query
    main_select = None
    for select in parsed.find_all(exp.Select):
        if not select.find_ancestor(exp.CTE):
            main_select = select
            break

    if main_select:
        # Check for correlated subquery
        has_correlated = False
        where = main_select.find(exp.Where)
        if where:
            for subq in where.find_all(exp.Subquery):
                inner = subq.find(exp.Select)
                if inner and inner.find(exp.Where):
                    # Has subquery with WHERE - likely correlated
                    for eq in inner.find(exp.Where).find_all(exp.EQ):
                        cols = list(eq.find_all(exp.Column))
                        tables = {str(c.table).lower() for c in cols if c.table}
                        if len(tables) >= 2:
                            has_correlated = True
                            break

        tags = []
        if has_correlated:
            tags.append("CORRELATED SUBQUERY ⚠️")

        blocks.append(f"-- [BLOCK: main_query] {' | '.join(tags)}")
        blocks.append(main_select.sql())

    return "\n".join(blocks) if blocks else sql


def detect_opportunities(sql: str) -> str:
    """Detect optimization opportunities."""
    try:
        from qt_sql.analyzers.opportunity_detector import detect_opportunities as detect
        opportunities = detect(sql)

        if not opportunities:
            return "None auto-detected. Analyze plan manually."

        lines = []
        for opp in opportunities:
            lines.append(f">>> {opp.pattern_id}: {opp.pattern_name}")
            lines.append(f"    Detected: {opp.trigger}")
            lines.append(f"    Fix: {opp.rewrite_hint}")
            lines.append(f"    Expected: {opp.expected_benefit}")

        return "\n".join(lines)
    except ImportError:
        return "Detector not available."


def format_plan_section(ctx: OptimizationContext) -> str:
    """Format the plan analysis into readable text."""
    lines = []

    # Top operators by cost
    top_ops = ctx.get_top_operators(5)
    if top_ops:
        lines.append("**Operators by cost:**")
        for op in top_ops:
            lines.append(f"  {op['operator']}: {op['cost_pct']}% | {op['rows']:,} rows")
        lines.append("")

    # Table scans
    if ctx.table_scans:
        lines.append("**Table scans:**")
        for scan in ctx.table_scans:
            filter_status = "FILTERED" if scan.has_filter else "NO FILTER"
            selectivity = f"({scan.selectivity:.1%} selectivity)" if scan.has_filter else ""
            lines.append(f"  {scan.table}: {scan.rows_scanned:,} rows [{filter_status}] {selectivity}")
        lines.append("")

    # Joins
    if ctx.joins:
        lines.append("**Joins:**")
        for join in ctx.joins:
            late_flag = " ⚠️ LATE JOIN" if join.is_late else ""
            lines.append(f"  {join.join_type}: {join.left_table} × {join.right_table} → {join.output_rows:,} rows{late_flag}")
        lines.append("")

    # Cardinality misestimates
    if ctx.cardinality_misestimates:
        lines.append("**Cardinality misestimates:**")
        for mis in ctx.cardinality_misestimates:
            lines.append(f"  {mis['operator']}: est {mis['estimated']:,} vs actual {mis['actual']:,} ({mis['ratio']}x off)")
        lines.append("")

    return "\n".join(lines) if lines else "Plan analysis not available."


def format_data_flow(ctx: OptimizationContext) -> str:
    """Format data flow section."""
    lines = []

    if ctx.data_flow.ctes:
        for name, cte in ctx.data_flow.ctes.items():
            inputs = ", ".join(cte.input_tables) if cte.input_tables else "?"
            agg = " → GROUP BY" if cte.has_aggregation else ""
            rows = f" → {cte.output_rows:,} rows" if cte.output_rows else ""
            lines.append(f"  [{name}]: {inputs}{agg}{rows}")

    if ctx.data_flow.main_query_tables:
        tables = ", ".join(ctx.data_flow.main_query_tables)
        lines.append(f"  [main_query]: {tables}")

    return "\n".join(lines) if lines else ""


def build_prompt(query_id: str, db_path: str) -> str:
    """Build complete V2 prompt for a query."""
    sql = load_sql(query_id)

    # Get plan analysis
    plan_json = get_plan_json(sql, db_path)
    ctx = analyze_plan_for_optimization(plan_json, sql)

    plan_section = format_plan_section(ctx)
    data_flow_section = format_data_flow(ctx)
    detected_opportunities = detect_opportunities(sql)
    annotated_sql = annotate_sql_with_blocks(sql)

    prompt = f"""# SQL Optimizer

You are optimizing a DuckDB query. Analyze the plan, apply the detected pattern, output patches.

## Algorithm

1. **READ** the Detected Opportunities - these are high-confidence patterns found by static analysis
2. **VERIFY** against the execution plan - confirm the bottleneck exists
3. **APPLY** the suggested fix using the patch format below
4. **OUTPUT** JSON patches only (not full SQL)

## Optimization Patterns

| Pattern | Fix | Speedup |
|---------|-----|---------|
| Correlated Subquery | Window function in CTE: `AVG(x) OVER (PARTITION BY key)` | 2.5x |
| Late Filter | Push filter INTO CTE before GROUP BY | 2.1x |
| OR on Different Cols | Split into UNION ALL branches | 2-3x |
| IN to EXISTS | `EXISTS (SELECT 1 FROM ... WHERE ...)` | 1.5x |

---

## Execution Plan

{plan_section}

## Data Flow

{data_flow_section}

## Detected Opportunities

{detected_opportunities}

---

## SQL Query (Annotated)

```sql
{annotated_sql}
```

---

## Output Format

```json
{{
  "patches": [
    {{
      "op": "replace_cte",
      "name": "cte_name",
      "sql": "full rewritten CTE body"
    }},
    {{
      "op": "replace_clause",
      "target": "main_query.where",
      "search": "exact text to find",
      "replace": "replacement text"
    }}
  ],
  "explanation": "what was optimized and why"
}}
```

**Patch ops:** `replace_cte`, `replace_clause`, `add_cte`, `delete_join`

**Targets:** `{{cte_name}}.where`, `main_query.where`, `main_query.from`

---

## Example: Correlated Subquery → Window

**Pattern:** `WHERE x > (SELECT avg(x) FROM t WHERE t.key = outer.key)`

**Fix:**
```json
{{
  "patches": [
    {{
      "op": "replace_cte",
      "name": "customer_total_return",
      "sql": "SELECT ..., AVG(SUM(SR_FEE)) OVER (PARTITION BY sr_store_sk) * 1.2 AS threshold FROM ... GROUP BY ..."
    }},
    {{
      "op": "replace_clause",
      "target": "main_query.where",
      "search": "> (select avg(ctr_total_return)*1.2 from customer_total_return ctr2 where ctr1.ctr_store_sk = ctr2.ctr_store_sk)",
      "replace": "> ctr1.threshold"
    }}
  ],
  "explanation": "Replaced correlated subquery with window function computed once in CTE"
}}
```

Now output your patches:
"""

    return prompt


def main():
    args = parse_args()

    try:
        prompt = build_prompt(args.query, args.db)

        if args.output:
            Path(args.output).write_text(prompt)
            print(f"Prompt written to {args.output}")
        else:
            print(prompt)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

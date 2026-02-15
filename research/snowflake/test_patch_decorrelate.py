"""Test: can qwen3-coder emit valid PatchPlan JSON for decorrelation?

Same transform as test_small_llm_decorrelate.py but using patch mode output
instead of full SQL rewrite.
"""

import json
import os
import sys
import time
from pathlib import Path


SYSTEM = """\
You rewrite Snowflake SQL queries to eliminate correlated scalar subqueries.
You output structured Patch Plans (JSON), NOT full SQL.

## DIALECT: SNOWFLAKE

Snowflake-specific rules:
- NO `AS MATERIALIZED` or `AS NOT MATERIALIZED` — Snowflake does not support this.
- EXISTS/NOT EXISTS must STAY as EXISTS.
- Do NOT wrap filter columns in functions — kills micro-partition pruning.
- INTERVAL syntax: `INTERVAL '90 DAY'` or `cast('date' as date) + interval '90 day'`.

## TRANSFORM: DECORRELATE CORRELATED SCALAR SUBQUERY

Pattern: `WHERE col > (SELECT agg(col) FROM table WHERE key = outer.key)`

Steps:
1. Extract shared scans into CTEs (date-filtered fact rows, dimension filters)
2. Compute per-key aggregates via GROUP BY in a CTE (the threshold)
3. Replace the correlated subquery with an INNER JOIN to the threshold CTE
4. Convert comma joins to explicit JOINs
5. If inner and outer query scan the SAME table with SAME filters, use a single shared-scan CTE

## CORRECTNESS REQUIREMENTS

1. SAME SELECT columns — identical names, order, and expressions
2. SAME row semantics — INNER JOIN excludes rows with no threshold match (equivalent to NULL)
3. SAME ORDER BY and LIMIT — preserved exactly
4. SAME aggregate multiplier — e.g., `1.3 * avg(...)` not `avg(...)`
5. ALL predicates preserved — every WHERE condition must appear somewhere

## OUTPUT FORMAT — Patch Plan

You receive an IR Node Map showing the query structure with anchor hashes.
Emit a single JSON object describing ONLY the changes (not the full SQL).

```json
{"plan_id": "W1_<query_id>", "dialect": "snowflake",
 "steps": [
   {"step_id": "s1", "op": "<operation>",
    "target": {"by_node_id": "<statement_id>"},
    "payload": { ... },
    "description": "<what this step does>"}
 ]}
```

### Available Operations

| op | target fields | payload fields | description |
|---|---|---|---|
| `insert_cte` | `by_node_id` | `cte_name`, `cte_query_sql` | Add a new CTE to a statement |
| `replace_from` | `by_node_id` | `from_sql` | Replace FROM + JOINs of main query |
| `replace_where_predicate` | `by_node_id` | `expr_sql` | Replace entire WHERE clause |
| `replace_body` | `by_node_id` | `sql_fragment` | Replace the main SELECT+FROM+WHERE+ORDER+LIMIT, keeping CTEs |
| `replace_expr_subtree` | `by_node_id` + `by_anchor_hash` | `expr_sql` | Replace specific expression by hash |
| `delete_expr_subtree` | `by_node_id` + `by_anchor_hash` | _(none)_ | Remove expression by hash |

### Targeting

- `by_node_id`: Selects the statement (e.g. `"S0"`). Required for all ops.
- `by_anchor_hash`: Copy the 16-char hex hash from `[...]` in the IR Node Map.

### Rules
- Every `cte_query_sql`, `expr_sql`, `from_sql`, `sql_fragment` must be complete SQL.
- Steps are applied in order. Later steps see the IR state after earlier steps.
- For decorrelation, the easiest approach is: insert_cte (x2-3) + replace_body.

Output ONLY the JSON. No explanation, no markdown fences."""


def build_ir_node_map(sql: str) -> str:
    """Build IR node map for a query."""
    project_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(project_root / "packages" / "qt-shared"))
    sys.path.insert(0, str(project_root / "packages" / "qt-sql"))
    from qt_sql.ir import build_script_ir, render_ir_node_map, Dialect

    script_ir = build_script_ir(sql, Dialect.SNOWFLAKE)
    return render_ir_node_map(script_ir)


def build_prompt(test_sql: str, ir_node_map: str, demo_input: str, demo_node_map: str, demo_output: str) -> list[dict]:
    """Build chat messages: system + 1 demo + test query."""
    demo_user = f"## IR Node Map\n\n```\n{demo_node_map}\n```\n\n## Original SQL\n\n{demo_input}"
    test_user = f"## IR Node Map\n\n```\n{ir_node_map}\n```\n\n## Original SQL\n\n{test_sql}"

    return [
        {"role": "system", "content": SYSTEM},
        {"role": "user", "content": demo_user},
        {"role": "assistant", "content": demo_output},
        {"role": "user", "content": test_user},
    ]


# ── Demo: Q92 gold patch ──────────────────────────────────────────────
# We know the gold rewrite for Q92 — express it as a PatchPlan.

DEMO_SQL = """\
select
   sum(ws_ext_discount_amt)  as "Excess Discount Amount"
from
    web_sales
   ,item
   ,date_dim
where
(i_manufact_id BETWEEN 341 and 540
or i_category IN ('Home', 'Men', 'Music'))
and i_item_sk = ws_item_sk
and d_date between '1998-03-13' and
        cast('1998-03-13' as date) + interval '90 day'
and d_date_sk = ws_sold_date_sk
and ws_wholesale_cost BETWEEN 26 AND 46
and ws_ext_discount_amt
     > (
         SELECT
            1.3 * avg(ws_ext_discount_amt)
         FROM
            web_sales
           ,date_dim
         WHERE
              ws_item_sk = i_item_sk
          and d_date between '1998-03-13' and
                             cast('1998-03-13' as date) + interval '90 day'
          and d_date_sk = ws_sold_date_sk
          and ws_wholesale_cost BETWEEN 26 AND 46
          and ws_sales_price / ws_list_price BETWEEN 34 * 0.01 AND 49 * 0.01
      )
order by sum(ws_ext_discount_amt)
limit 100;"""

DEMO_PATCH = json.dumps({
    "plan_id": "W1_query_demo",
    "dialect": "snowflake",
    "steps": [
        {
            "step_id": "s1",
            "op": "insert_cte",
            "target": {"by_node_id": "S0"},
            "payload": {
                "cte_name": "common_scan",
                "cte_query_sql": (
                    "SELECT ws_item_sk, ws_ext_discount_amt, ws_sales_price, ws_list_price "
                    "FROM web_sales "
                    "INNER JOIN date_dim ON d_date_sk = ws_sold_date_sk "
                    "WHERE d_date BETWEEN '1998-03-13' AND CAST('1998-03-13' AS DATE) + INTERVAL '90 DAY' "
                    "AND ws_wholesale_cost BETWEEN 26 AND 46"
                )
            },
            "description": "Shared scan CTE: date-filtered web_sales with cost filter"
        },
        {
            "step_id": "s2",
            "op": "insert_cte",
            "target": {"by_node_id": "S0"},
            "payload": {
                "cte_name": "threshold_computation",
                "cte_query_sql": (
                    "SELECT ws_item_sk, 1.3 * AVG(ws_ext_discount_amt) AS threshold "
                    "FROM common_scan "
                    "WHERE ws_sales_price / ws_list_price BETWEEN 34 * 0.01 AND 49 * 0.01 "
                    "GROUP BY ws_item_sk"
                )
            },
            "description": "Per-item threshold from shared scan"
        },
        {
            "step_id": "s3",
            "op": "replace_body",
            "target": {"by_node_id": "S0"},
            "payload": {
                "sql_fragment": (
                    "SELECT SUM(cs.ws_ext_discount_amt) AS \"Excess Discount Amount\" "
                    "FROM common_scan cs "
                    "INNER JOIN item ON i_item_sk = cs.ws_item_sk "
                    "INNER JOIN threshold_computation t ON cs.ws_item_sk = t.ws_item_sk "
                    "WHERE (i_manufact_id BETWEEN 341 AND 540 OR i_category IN ('Home', 'Men', 'Music')) "
                    "AND cs.ws_ext_discount_amt > t.threshold "
                    "ORDER BY SUM(cs.ws_ext_discount_amt) "
                    "LIMIT 100"
                )
            },
            "description": "Replace main query: JOIN threshold CTE instead of correlated subquery"
        }
    ]
}, indent=2)


def call_openai_compatible(messages, model, api_key, base_url):
    """Call any OpenAI-compatible API."""
    import openai
    client = openai.OpenAI(api_key=api_key, base_url=base_url)
    t0 = time.perf_counter()
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0.0,
        max_tokens=2048,
    )
    elapsed = time.perf_counter() - t0
    content = response.choices[0].message.content
    usage = response.usage
    return content, elapsed, usage


def validate_patch_json(content: str) -> dict:
    """Try to parse and validate the patch plan JSON."""
    # Strip markdown fences if present
    text = content.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)

    data = json.loads(text)

    # Validate structure
    assert "plan_id" in data, "Missing plan_id"
    assert "steps" in data, "Missing steps"
    assert isinstance(data["steps"], list), "steps must be a list"
    assert len(data["steps"]) > 0, "steps must not be empty"

    for step in data["steps"]:
        assert "step_id" in step, f"Missing step_id in step"
        assert "op" in step, f"Missing op in step {step.get('step_id')}"
        assert "target" in step, f"Missing target in step {step.get('step_id')}"
        assert "by_node_id" in step["target"], f"Missing by_node_id in step {step.get('step_id')}"

    return data


def main():
    # Load .env
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

    api_key = os.environ.get("QT_OPENROUTER_API_KEY", "")
    if not api_key:
        print("Missing QT_OPENROUTER_API_KEY")
        sys.exit(1)

    # ── Load test queries ──────────────────────────────────────────
    project_root = Path(__file__).resolve().parents[2]
    qdir = project_root / "packages" / "qt-sql" / "qt_sql" / "benchmarks" / "snowflake_tpcds" / "queries"

    # Build demo IR node map
    demo_node_map = build_ir_node_map(DEMO_SQL)
    print("=== DEMO IR NODE MAP ===")
    print(demo_node_map)
    print()

    # Test queries: Q32 and Q92 (the FACT-tagged P3 queries)
    test_queries = ["query_92", "query_32"]
    query_filter = sys.argv[1:] if len(sys.argv) > 1 else None
    if query_filter:
        test_queries = [q for q in test_queries if q in query_filter]

    out_dir = Path(__file__).parent / "decorrelate_results"
    out_dir.mkdir(exist_ok=True)

    for qid in test_queries:
        sql = (qdir / f"{qid}.sql").read_text()
        ir_node_map = build_ir_node_map(sql)

        print(f"\n{'─'*50}")
        print(f"Query: {qid}")
        print(f"{'─'*50}")
        print(f"IR Node Map:\n{ir_node_map}")

        messages = build_prompt(sql, ir_node_map, DEMO_SQL, demo_node_map, DEMO_PATCH)
        prompt_chars = sum(len(m["content"]) for m in messages)
        print(f"Prompt: {prompt_chars} chars")

        try:
            content, elapsed, usage = call_openai_compatible(
                messages, "qwen/qwen3-coder", api_key,
                "https://openrouter.ai/api/v1"
            )
        except Exception as e:
            print(f"  ERROR: {e}")
            continue

        print(f"Response: {len(content)} chars in {elapsed:.1f}s")
        print(f"Usage: {usage}")
        print()
        print(content)
        print()

        # Validate JSON
        try:
            patch_data = validate_patch_json(content)
            print(f"✓ Valid PatchPlan: {len(patch_data['steps'])} steps")
            for step in patch_data["steps"]:
                print(f"  {step['step_id']}: {step['op']} → {step.get('description', '')[:80]}")
        except Exception as e:
            print(f"✗ Invalid PatchPlan: {e}")

        # Try to apply the patch
        try:
            sys.path.insert(0, str(project_root / "packages" / "qt-shared"))
            sys.path.insert(0, str(project_root / "packages" / "qt-sql"))
            from qt_sql.ir import build_script_ir, Dialect
            from qt_sql.ir.patch_schema import PatchPlan
            from qt_sql.ir.patch_engine import apply_patch_plan
            from qt_sql.ir import dict_to_plan
            import copy

            script_ir = build_script_ir(sql, Dialect.SNOWFLAKE)
            ir_copy = copy.deepcopy(script_ir)
            plan = dict_to_plan(patch_data)
            result = apply_patch_plan(ir_copy, plan)

            if result.success:
                print(f"\n✓ Patch applied successfully!")
                print(f"Output SQL ({len(result.output_sql)} chars):")
                print(result.output_sql)
            else:
                print(f"\n✗ Patch failed: {result.errors}")
        except Exception as e:
            print(f"\n✗ Patch engine error: {e}")

        # Save result
        out_file = out_dir / f"patch_{qid}_{int(time.time())}.json"
        out_file.write_text(json.dumps({
            "query_id": qid,
            "mode": "patch",
            "model": "qwen/qwen3-coder",
            "elapsed_s": round(elapsed, 2),
            "prompt_chars": prompt_chars,
            "response": content,
            "original_sql": sql,
            "ir_node_map": ir_node_map,
            "usage": str(usage),
        }, indent=2))


if __name__ == "__main__":
    main()

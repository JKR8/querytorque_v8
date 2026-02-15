"""Mini test: can a small LLM decorrelate Snowflake correlated subqueries?

DSPy-style: signature + 2 few-shot demos + test query.
No framework, just a prompt string and an API call.
"""

import json
import sys
import time
from pathlib import Path

# ── DSPy-style signature ────────────────────────────────────────────────
#
#   Decorrelate: original_sql -> optimized_sql
#
#   Given a SQL query containing a correlated scalar subquery in WHERE,
#   rewrite it to eliminate the correlation using CTEs and JOINs.
#

SYSTEM = """\
You rewrite Snowflake SQL queries to eliminate correlated scalar subqueries.

## DIALECT: SNOWFLAKE

Snowflake-specific rules:
- NO `AS MATERIALIZED` or `AS NOT MATERIALIZED` — Snowflake does not support this syntax. CTEs are auto-materialized when referenced 2+ times.
- EXISTS/NOT EXISTS must STAY as EXISTS — never materialize into CTEs (kills SemiJoin early termination).
- NOT IN → rewrite as NOT EXISTS for NULL safety.
- Do NOT wrap filter columns in functions — kills micro-partition pruning.
- INTERVAL syntax: `INTERVAL '90 DAY'` or `cast('date' as date) + interval '90 day'`.
- Column names are case-insensitive. Quoted identifiers ("col") preserve case.
- QUALIFY is supported for window function filtering (prefer over subquery).

## TRANSFORM: DECORRELATE CORRELATED SCALAR SUBQUERY

Pattern: `WHERE col > (SELECT agg(col) FROM table WHERE key = outer.key)`

Steps:
1. Extract shared scans into CTEs (date-filtered fact rows, dimension filters)
2. Compute per-key aggregates via GROUP BY in a CTE (the threshold)
3. Replace the correlated subquery with an INNER JOIN to the threshold CTE
4. Convert comma joins to explicit JOINs
5. If inner and outer query scan the SAME table with SAME filters, use a single shared-scan CTE

## CORRECTNESS REQUIREMENTS (MUST ALL HOLD)

Before outputting, mentally verify:
1. SAME SELECT columns — identical names, order, and expressions
2. SAME row semantics — INNER JOIN excludes rows with no threshold match, which is equivalent to the correlated subquery returning NULL (comparison is FALSE → row excluded)
3. SAME ORDER BY and LIMIT — preserved exactly
4. SAME aggregate multiplier — e.g., `1.2 * avg(...)` not `avg(...)`
5. SAME GROUP BY key — must match the correlated column (e.g., ctr_store_sk)
6. ALL predicates preserved — every WHERE condition from the original must appear somewhere in the rewrite (in a CTE or final query)
7. NO extra rows — do not accidentally broaden the result set

## OUTPUT FORMAT

Output ONLY the rewritten SQL. No explanation, no markdown fences."""

# ── Few-shot demos (from verified gold examples) ────────────────────────

DEMO_1_INPUT = """\
select  sum(cs_ext_discount_amt)  as "excess discount amount"
from
   catalog_sales
   ,item
   ,date_dim
where
(i_manufact_id in (1, 78, 97, 516, 521)
or i_manager_id BETWEEN 25 and 54)
and i_item_sk = cs_item_sk
and d_date between '1999-03-07' and
        cast('1999-03-07' as date) + interval '90 day'
and d_date_sk = cs_sold_date_sk
and cs_ext_discount_amt
     > (
         select
            1.3 * avg(cs_ext_discount_amt)
         from
            catalog_sales
           ,date_dim
         where
              cs_item_sk = i_item_sk
          and d_date between '1999-03-07' and
                             cast('1999-03-07' as date) + interval '90 day'
          and d_date_sk = cs_sold_date_sk
          and cs_list_price between 16 and 45
          and cs_sales_price / cs_list_price BETWEEN 63 * 0.01 AND 83 * 0.01
      )
order by sum(cs_ext_discount_amt)
limit 100;"""

DEMO_1_OUTPUT = """\
WITH filtered_items AS (
    SELECT i_item_sk
    FROM item
    WHERE i_manufact_id IN (1, 78, 97, 516, 521)
       OR i_manager_id BETWEEN 25 AND 54
),
date_filtered_sales AS (
    SELECT cs.cs_item_sk, cs.cs_ext_discount_amt,
           cs.cs_list_price, cs.cs_sales_price
    FROM catalog_sales cs
    JOIN date_dim d ON d.d_date_sk = cs.cs_sold_date_sk
    WHERE d.d_date BETWEEN '1999-03-07' AND cast('1999-03-07' as date) + interval '90 day'
),
item_avg_discount AS (
    SELECT dfs.cs_item_sk,
           1.3 * avg(dfs.cs_ext_discount_amt) AS threshold
    FROM date_filtered_sales dfs
    JOIN filtered_items fi ON fi.i_item_sk = dfs.cs_item_sk
    WHERE dfs.cs_list_price BETWEEN 16 AND 45
      AND dfs.cs_sales_price / dfs.cs_list_price BETWEEN 63 * 0.01 AND 83 * 0.01
    GROUP BY dfs.cs_item_sk
)
SELECT sum(dfs.cs_ext_discount_amt) AS "excess discount amount"
FROM date_filtered_sales dfs
JOIN item_avg_discount iad ON iad.cs_item_sk = dfs.cs_item_sk
WHERE dfs.cs_ext_discount_amt > iad.threshold
ORDER BY 1
LIMIT 100;"""

DEMO_2_INPUT = """\
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

DEMO_2_OUTPUT = """\
WITH common_scan AS (
  SELECT ws_item_sk, ws_ext_discount_amt, ws_sales_price, ws_list_price
  FROM web_sales
  INNER JOIN date_dim ON d_date_sk = ws_sold_date_sk
  WHERE d_date BETWEEN '1998-03-13' AND CAST('1998-03-13' AS DATE) + INTERVAL '90 DAY'
    AND ws_wholesale_cost BETWEEN 26 AND 46
),
threshold_computation AS (
  SELECT ws_item_sk, 1.3 * AVG(ws_ext_discount_amt) AS threshold
  FROM common_scan
  WHERE ws_sales_price / ws_list_price BETWEEN 34 * 0.01 AND 49 * 0.01
  GROUP BY ws_item_sk
),
outer_rows AS (
  SELECT cs.ws_item_sk, cs.ws_ext_discount_amt
  FROM common_scan cs
  INNER JOIN item ON i_item_sk = cs.ws_item_sk
  WHERE i_manufact_id BETWEEN 341 AND 540
     OR i_category IN ('Home', 'Men', 'Music')
),
join_filter AS (
  SELECT o.ws_ext_discount_amt
  FROM outer_rows o
  INNER JOIN threshold_computation t ON o.ws_item_sk = t.ws_item_sk
  WHERE o.ws_ext_discount_amt > t.threshold
)
SELECT SUM(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM join_filter
ORDER BY SUM(ws_ext_discount_amt)
LIMIT 100;"""

# ── Auto-detect P3 candidates from benchmark queries ───────────────────

def find_p3_queries(query_dir: Path) -> list[tuple[str, str]]:
    """Find queries with correlated scalar aggregate subqueries (P3 pattern).

    Returns list of (query_id, sql) tuples.
    """
    project_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(project_root / "packages" / "qt-shared"))
    sys.path.insert(0, str(project_root / "packages" / "qt-sql"))
    from qt_sql.tag_index import extract_tags

    hits = []
    for qf in sorted(query_dir.glob("*.sql")):
        sql = qf.read_text()
        tags = extract_tags(sql)
        # P3 pattern: correlated subquery + scalar aggregate
        if "correlated_sub" in tags and "scalar_agg_sub" in tags:
            hits.append((qf.stem, sql))
    return hits


def build_prompt(test_sql: str) -> list[dict]:
    """Build chat messages: system + 2 demos + test query."""
    return [
        {"role": "system", "content": SYSTEM},
        {"role": "user", "content": DEMO_1_INPUT},
        {"role": "assistant", "content": DEMO_1_OUTPUT},
        {"role": "user", "content": DEMO_2_INPUT},
        {"role": "assistant", "content": DEMO_2_OUTPUT},
        {"role": "user", "content": test_sql},
    ]


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


def call_anthropic(messages, model, api_key):
    """Call Anthropic API."""
    import anthropic

    client = anthropic.Anthropic(api_key=api_key)

    # Extract system message
    system_msg = next(m["content"] for m in messages if m["role"] == "system")
    chat_messages = [m for m in messages if m["role"] != "system"]

    t0 = time.perf_counter()
    response = client.messages.create(
        model=model,
        system=system_msg,
        messages=chat_messages,
        temperature=0.0,
        max_tokens=2048,
    )
    elapsed = time.perf_counter() - t0
    content = response.content[0].text
    usage = response.usage
    return content, elapsed, usage


def main():
    import os

    # Load .env from project root
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

    # ── Choose model ────────────────────────────────────────────────
    # Override with CLI args: python test_small_llm_decorrelate.py haiku
    model_key = sys.argv[1] if len(sys.argv) > 1 else "haiku"

    MODELS = {
        "qwen3-coder": {
            "provider": "openai",
            "model": "qwen/qwen3-coder",
            "api_key_env": "QT_OPENROUTER_API_KEY",
            "base_url": "https://openrouter.ai/api/v1",
        },
        "haiku": {
            "provider": "anthropic",
            "model": "claude-haiku-4-5-20251001",
            "api_key_env": "ANTHROPIC_API_KEY",
        },
        "sonnet": {
            "provider": "anthropic",
            "model": "claude-sonnet-4-5-20250929",
            "api_key_env": "ANTHROPIC_API_KEY",
        },
        "gpt4o-mini": {
            "provider": "openai",
            "model": "gpt-4o-mini",
            "api_key_env": "OPENAI_API_KEY",
            "base_url": "https://api.openai.com/v1",
        },
        "deepseek-chat": {
            "provider": "openai",
            "model": "deepseek-chat",
            "api_key_env": "QT_DEEPSEEK_API_KEY",
            "base_url": "https://api.deepseek.com",
        },
    }

    if model_key not in MODELS:
        print(f"Unknown model: {model_key}")
        print(f"Available: {', '.join(MODELS.keys())}")
        sys.exit(1)

    cfg = MODELS[model_key]
    api_key = os.environ.get(cfg["api_key_env"], "")
    if not api_key:
        print(f"Missing API key: {cfg['api_key_env']}")
        sys.exit(1)

    # ── Find P3 queries ───────────────────────────────────────────────
    project_root = Path(__file__).resolve().parents[2]
    query_dir = project_root / "packages" / "qt-sql" / "qt_sql" / "benchmarks" / "snowflake_tpcds" / "queries"
    p3_queries = find_p3_queries(query_dir)

    # Optional query filter: python script.py qwen3-coder query_1 query_32
    query_filter = sys.argv[2:] if len(sys.argv) > 2 else None
    if query_filter:
        p3_queries = [(qid, sql) for qid, sql in p3_queries if qid in query_filter]

    print(f"Model: {cfg['model']}")
    print(f"P3 candidates: {len(p3_queries)} queries")
    print(f"Queries: {', '.join(qid for qid, _ in p3_queries)}")
    print("=" * 60)

    out_dir = Path(__file__).parent / "decorrelate_results"
    out_dir.mkdir(exist_ok=True)

    results = []
    for qid, sql in p3_queries:
        print(f"\n{'─'*40}")
        print(f"Query: {qid}")
        print(f"{'─'*40}")

        messages = build_prompt(sql)
        prompt_chars = sum(len(m["content"]) for m in messages)
        print(f"Prompt: {prompt_chars} chars")

        # ── Call LLM ──────────────────────────────────────────────
        try:
            if cfg["provider"] == "anthropic":
                content, elapsed, usage = call_anthropic(
                    messages, cfg["model"], api_key
                )
            else:
                content, elapsed, usage = call_openai_compatible(
                    messages, cfg["model"], api_key, cfg.get("base_url")
                )
        except Exception as e:
            print(f"  ERROR: {e}")
            results.append({"query_id": qid, "status": "ERROR", "error": str(e)})
            continue

        print(f"Response: {len(content)} chars in {elapsed:.1f}s")
        print(f"Usage: {usage}")
        print()
        print(content[:500])
        if len(content) > 500:
            print(f"  ... ({len(content) - 500} more chars)")

        result = {
            "query_id": qid,
            "model": cfg["model"],
            "model_key": model_key,
            "elapsed_s": round(elapsed, 2),
            "prompt_chars": prompt_chars,
            "response": content,
            "original_sql": sql,
            "usage": str(usage),
            "status": "OK",
        }
        results.append(result)

        # Save individual result
        out_file = out_dir / f"{model_key}_{qid}_{int(time.time())}.json"
        out_file.write_text(json.dumps(result, indent=2))

    # ── Summary ─────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"Summary: {len(results)} queries processed")
    for r in results:
        status = r.get("status", "?")
        elapsed = r.get("elapsed_s", 0)
        print(f"  {r['query_id']}: {status} ({elapsed:.1f}s)")
    print(f"\nResults saved to: {out_dir}")


if __name__ == "__main__":
    main()

"""
Quick PoC: LLM-as-judge SQL equivalence check via Ollama (qwen2.5-coder:7b).
Pre-normalizes SQL with sqlglot (lowercase, no comments, consistent formatting).
Runs each pair 3 times, takes majority vote.
"""
import json
import subprocess
import sys
import time

import sqlglot

MODEL = "qwen2.5-coder:7b"
VOTES = 3

SYSTEM_PROMPT = (
    "You are an SQL expert. Your task is to determine if two SQL queries "
    "will return identical results in the same database state."
)

USER_TEMPLATE = """Compare Query A and Query B.

Focus on logic: joins, filters, grouping, and aggregations.

If they are semantically equivalent (return same rows and columns), output "TRUE".

If they differ in logic or expected output, output "FALSE".

Query A:
{sql_a}

Query B:
{sql_b}

Output (TRUE/FALSE):"""


def normalize_sql(sql: str) -> str:
    """Normalize SQL via sqlglot: lowercase, no comments, consistent format."""
    try:
        parsed = sqlglot.parse(sql)
        parts = []
        for stmt in parsed:
            normalized = stmt.sql(
                dialect="duckdb",
                normalize=True,
                pretty=True,
                comments=False,
            ).lower()
            parts.append(normalized)
        return "\n".join(parts)
    except Exception:
        # Fallback: just lowercase and strip
        return sql.lower().strip()


def call_ollama(system: str, user: str) -> str:
    """Call ollama via CLI and return raw response text."""
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "stream": False,
        "options": {"temperature": 0.3, "num_predict": 64},
    }
    result = subprocess.run(
        ["curl", "-s", "http://localhost:11434/api/chat", "-d", json.dumps(payload)],
        capture_output=True, text=True, timeout=300,
    )
    resp = json.loads(result.stdout)
    return resp.get("message", {}).get("content", "").strip()


def parse_verdict(raw: str) -> str:
    """Extract TRUE/FALSE from model response."""
    upper = raw.upper().strip()
    if upper.startswith("TRUE"):
        return "TRUE"
    if upper.startswith("FALSE"):
        return "FALSE"
    if "TRUE" in upper and "FALSE" not in upper:
        return "TRUE"
    if "FALSE" in upper and "TRUE" not in upper:
        return "FALSE"
    return "UNCLEAR"


def majority_vote(sql_a: str, sql_b: str, label: str) -> dict:
    """Run VOTES times and return majority verdict."""
    # Normalize both queries
    norm_a = normalize_sql(sql_a)
    norm_b = normalize_sql(sql_b)

    print(f"  Normalized A ({len(norm_a)} chars), B ({len(norm_b)} chars)")

    votes = []
    raw_responses = []
    for i in range(VOTES):
        user_msg = USER_TEMPLATE.format(sql_a=norm_a, sql_b=norm_b)
        t0 = time.time()
        raw = call_ollama(SYSTEM_PROMPT, user_msg)
        elapsed = time.time() - t0
        verdict = parse_verdict(raw)
        votes.append(verdict)
        raw_responses.append(raw)
        print(f"  Vote {i+1}: {verdict} ({elapsed:.1f}s) — raw: {raw[:120]}")

    true_count = votes.count("TRUE")
    false_count = votes.count("FALSE")
    if true_count > false_count:
        final = "TRUE"
    elif false_count > true_count:
        final = "FALSE"
    else:
        final = "SPLIT"

    return {
        "label": label,
        "votes": votes,
        "final": final,
        "raw_responses": raw_responses,
    }


# ---------- Test cases ----------

# PASS case: query019_agg_i1/t1 — decorrelate subquery into CTE, semantically equivalent
PASS_ORIGINAL = """select  i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
 	sum(ss_ext_sales_price) ext_price
  from date_dim, store_sales, item,customer,customer_address,store
  where  d_date_sk = ss_sold_date_sk
    and ss_item_sk = i_item_sk
    and ss_customer_sk = c_customer_sk
    and c_current_addr_sk = ca_address_sk
    and ss_store_sk = s_store_sk
    AND i_category  = 'Jewelry'
    and d_year=2002
    and d_moy = 4
    and substring(ca_zip,1,5) <> substring(s_zip,1,5)
    and ca_state  = 'IL'
    and c_birth_month = 1
    and ss_wholesale_cost BETWEEN 35 AND 55
 group by i_brand
      ,i_brand_id
      ,i_manufact_id
      ,i_manufact
 order by ext_price desc
         ,i_brand
         ,i_brand_id
         ,i_manufact_id
         ,i_manufact
limit 100 ;"""

PASS_PATCH = """WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2002 AND d_moy = 4), filtered_geo_pairs AS (SELECT ca_address_sk, s_store_sk FROM customer_address, store WHERE ca_state = 'IL' AND SUBSTRING(ca_zip FROM 1 FOR 5) <> SUBSTRING(s_zip FROM 1 FOR 5)), filtered_sales AS (SELECT ss_ext_sales_price, ss_item_sk, ss_customer_sk, ss_store_sk FROM store_sales JOIN filtered_dates ON d_date_sk = ss_sold_date_sk WHERE ss_wholesale_cost BETWEEN 35 AND 55) SELECT i_brand_id AS brand_id, i_brand AS brand, i_manufact_id, i_manufact, SUM(ss_ext_sales_price) AS ext_price FROM filtered_sales AS fs JOIN item AS i ON fs.ss_item_sk = i.i_item_sk JOIN customer AS c ON fs.ss_customer_sk = c.c_customer_sk JOIN filtered_geo_pairs AS fg ON c.c_current_addr_sk = fg.ca_address_sk AND fs.ss_store_sk = fg.s_store_sk WHERE i_category = 'Jewelry' AND c_birth_month = 1 GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact ORDER BY ext_price DESC, i_brand, i_brand_id, i_manufact_id, i_manufact LIMIT 100;"""

# FAIL case: query040_agg_i2/t1 — moved cr_reason_sk=40 from WHERE to LEFT JOIN ON
# This changes semantics: WHERE filters out NULLs (no return), ON keeps them
FAIL_ORIGINAL = """select
   w_state
  ,i_item_id
  ,sum(case when (cast(d_date as date) < cast ('2002-02-20' as date))
 		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_before
  ,sum(case when (cast(d_date as date) >= cast ('2002-02-20' as date))
 		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_after
 from
   catalog_sales left outer join catalog_returns on
       (cs_order_number = cr_order_number
        and cs_item_sk = cr_item_sk)
  ,warehouse
  ,item
  ,date_dim
 where
 i_item_sk          = cs_item_sk
 and cs_warehouse_sk    = w_warehouse_sk
 and cs_sold_date_sk    = d_date_sk
 and d_date between  (cast ('2002-02-20' as date) - interval '30 day')
                and (cast ('2002-02-20' as date) + interval '30 day')
 and i_category  = 'Shoes'
 and i_manager_id between 42 and 81
 and cs_wholesale_cost between 68 and 87
 and cr_reason_sk = 40
 group by
    w_state,i_item_id
 order by w_state,i_item_id
limit 100;"""

FAIL_PATCH = """SELECT w_state, i_item_id, SUM(CASE WHEN (CAST(d_date AS DATE) < CAST('2002-02-20' AS DATE)) THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END) AS sales_before, SUM(CASE WHEN (CAST(d_date AS DATE) >= CAST('2002-02-20' AS DATE)) THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END) AS sales_after FROM catalog_sales LEFT JOIN catalog_returns ON (cs_order_number = cr_order_number AND cs_item_sk = cr_item_sk AND cr_reason_sk = 40) INNER JOIN warehouse ON cs_warehouse_sk = w_warehouse_sk INNER JOIN item ON i_item_sk = cs_item_sk INNER JOIN date_dim ON cs_sold_date_sk = d_date_sk WHERE d_date BETWEEN (CAST('2002-02-20' AS DATE) - INTERVAL '30 DAY') AND (CAST('2002-02-20' AS DATE) + INTERVAL '30 DAY') AND i_category = 'Shoes' AND i_manager_id BETWEEN 42 AND 81 AND cs_wholesale_cost BETWEEN 68 AND 87 GROUP BY w_state, i_item_id ORDER BY w_state, i_item_id LIMIT 100;"""


def main():
    print(f"Model: {MODEL}")
    print(f"Votes per pair: {VOTES}")
    print(f"{'='*60}")

    # Test 1: PASS case (should say TRUE — queries are equivalent)
    print("\n[TEST 1] EQUIVALENT pair (expected: TRUE)")
    print("  Original: comma-join with inline filters")
    print("  Patch: CTE-based early filtering (same logic)")
    r1 = majority_vote(PASS_ORIGINAL, PASS_PATCH, "EQUIVALENT")
    correct_1 = r1["final"] == "TRUE"
    print(f"  -> Majority: {r1['final']} (expected TRUE) {'CORRECT' if correct_1 else 'WRONG'}")

    # Test 2: FAIL case (should say FALSE — queries differ)
    print("\n[TEST 2] NON-EQUIVALENT pair (expected: FALSE)")
    print("  Original: cr_reason_sk=40 in WHERE (filters LEFT JOIN nulls)")
    print("  Patch: cr_reason_sk=40 in ON clause (preserves LEFT JOIN nulls)")
    r2 = majority_vote(FAIL_ORIGINAL, FAIL_PATCH, "NON_EQUIVALENT")
    correct_2 = r2["final"] == "FALSE"
    print(f"  -> Majority: {r2['final']} (expected FALSE) {'CORRECT' if correct_2 else 'WRONG'}")

    print(f"\n{'='*60}")
    print(f"Results: {int(correct_1) + int(correct_2)}/2 correct")

    return 0 if (correct_1 and correct_2) else 1


if __name__ == "__main__":
    sys.exit(main())

"""
DAG-Format Gold Examples for Few-Shot Learning

Uses VERIFIED optimizations from Full-SQL mode (Q15, Q39, Q23) reformatted for DAG output.
These are real speedups validated on SF100.

Used by DagOptimizationPipeline to guide LLM output format.
"""

import dspy
from typing import List


# ============================================================
# Q15 - 2.98x speedup (VERIFIED)
# Key: Early date filtering CTE, OR decomposition to UNION ALL
# ============================================================

Q15_DAG = """Nodes:
  [filtered_dates] type=cte tables=[date_dim] FILTER
  [filtered_catalog_sales] type=cte tables=[catalog_sales, customer, customer_address] UNION_ALL
  [main_query] type=main refs=[filtered_catalog_sales] GROUP_BY

Edges:
  filtered_dates -> filtered_catalog_sales
  filtered_catalog_sales -> main_query"""

Q15_NODE_SQL = """[main_query]:
SELECT ca_zip, sum(cs_sales_price)
FROM catalog_sales, customer, customer_address, date_dim
WHERE cs_bill_customer_sk = c_customer_sk
  AND c_current_addr_sk = ca_address_sk
  AND (substr(ca_zip,1,5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')
       OR ca_state IN ('CA','WA','GA')
       OR cs_sales_price > 500)
  AND cs_sold_date_sk = d_date_sk
  AND d_qoy = 1 AND d_year = 2001
GROUP BY ca_zip
ORDER BY ca_zip
LIMIT 100"""

Q15_PLAN = """HASH_GROUP_BY[ca_zip] cost=35%
HASH_JOIN[cs_bill_customer_sk=c_customer_sk] cost=25%
HASH_JOIN[c_current_addr_sk=ca_address_sk] cost=15%
SEQ_SCAN[catalog_sales] rows=143M
SEQ_SCAN[date_dim] rows=365 filtered=91"""

Q15_HINTS = """SQL-OR-001: OR on Different Columns
  Trigger: OR condition across ca_zip, ca_state, cs_sales_price
  Fix: Split into UNION ALL branches for parallel execution
  Expected: 2-3x speedup"""

Q15_REWRITES = """{
  "filtered_dates": "SELECT d_date_sk FROM date_dim WHERE d_qoy = 1 AND d_year = 2001",
  "filtered_catalog_sales": "SELECT cs_bill_customer_sk, cs_sales_price, ca_zip FROM catalog_sales INNER JOIN filtered_dates ON cs_sold_date_sk = d_date_sk INNER JOIN customer ON cs_bill_customer_sk = c_customer_sk INNER JOIN customer_address ON c_current_addr_sk = ca_address_sk WHERE substr(ca_zip,1,5) IN ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792') UNION ALL SELECT cs_bill_customer_sk, cs_sales_price, ca_zip FROM catalog_sales INNER JOIN filtered_dates ON cs_sold_date_sk = d_date_sk INNER JOIN customer ON cs_bill_customer_sk = c_customer_sk INNER JOIN customer_address ON c_current_addr_sk = ca_address_sk WHERE ca_state IN ('CA','WA','GA') UNION ALL SELECT cs_bill_customer_sk, cs_sales_price, ca_zip FROM catalog_sales INNER JOIN filtered_dates ON cs_sold_date_sk = d_date_sk INNER JOIN customer ON cs_bill_customer_sk = c_customer_sk INNER JOIN customer_address ON c_current_addr_sk = ca_address_sk WHERE cs_sales_price > 500",
  "main_query": "SELECT ca_zip, SUM(cs_sales_price) AS total_sales FROM filtered_catalog_sales GROUP BY ca_zip ORDER BY ca_zip LIMIT 100"
}"""

Q15_EXPLANATION = """1. Created filtered_dates CTE for early date filtering (d_qoy=1, d_year=2001) reducing to 91 rows.
2. Split OR condition into three UNION ALL branches in filtered_catalog_sales for parallel execution.
3. Each branch joins through filtered_dates first to reduce cardinality early.
4. Main query just aggregates the pre-filtered results."""


# ============================================================
# Q39 - 2.44x speedup (VERIFIED)
# Key: Push d_moy filter into CTE before GROUP BY
# ============================================================

Q39_DAG = """Nodes:
  [inv] type=cte tables=[inventory, item, warehouse, date_dim] GROUP_BY FILTER
  [main_query] type=main refs=[inv] SELF_JOIN

Edges:
  inv -> main_query"""

Q39_NODE_SQL = """[inv]:
SELECT w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy,
       stdev, mean, CASE mean WHEN 0 THEN NULL ELSE stdev/mean END cov
FROM (SELECT w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy,
             stddev_samp(inv_quantity_on_hand) stdev, avg(inv_quantity_on_hand) mean
      FROM inventory, item, warehouse, date_dim
      WHERE inv_item_sk = i_item_sk AND inv_warehouse_sk = w_warehouse_sk
        AND inv_date_sk = d_date_sk AND d_year = 1998
      GROUP BY w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy) foo
WHERE CASE mean WHEN 0 THEN 0 ELSE stdev/mean END > 1

[main_query]:
SELECT inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy, inv1.mean, inv1.cov,
       inv2.w_warehouse_sk, inv2.i_item_sk, inv2.d_moy, inv2.mean, inv2.cov
FROM inv inv1, inv inv2
WHERE inv1.i_item_sk = inv2.i_item_sk AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
  AND inv1.d_moy = 1 AND inv2.d_moy = 1+1
ORDER BY inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy, inv1.mean, inv1.cov, inv2.d_moy, inv2.mean, inv2.cov"""

Q39_PLAN = """HASH_GROUP_BY[warehouse,item,d_moy] cost=60.6%
HASH_JOIN[inv_item_sk=i_item_sk] cost=15%
SEQ_SCAN[inventory] rows=400M
SEQ_SCAN[date_dim] rows=365"""

Q39_HINTS = """SQL-FILTER-001: Late Filter Pushdown
  Trigger: CTE groups all 12 months but main query only uses months 1,2
  Fix: Push d_moy IN (1,2) into CTE before GROUP BY
  Expected: 2x+ speedup by reducing rows before aggregation"""

Q39_REWRITES = """{
  "inv": "SELECT w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy, stdev, mean, CASE mean WHEN 0 THEN NULL ELSE stdev/mean END AS cov FROM (SELECT w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy, STDDEV_SAMP(inv_quantity_on_hand) AS stdev, AVG(inv_quantity_on_hand) AS mean FROM inventory JOIN item ON inv_item_sk = i_item_sk JOIN warehouse ON inv_warehouse_sk = w_warehouse_sk JOIN date_dim ON inv_date_sk = d_date_sk WHERE d_year = 1998 AND d_moy IN (1, 2) GROUP BY w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy) foo WHERE mean != 0 AND stdev/mean > 1",
  "main_query": "SELECT inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy, inv1.mean, inv1.cov, inv2.w_warehouse_sk, inv2.i_item_sk, inv2.d_moy, inv2.mean, inv2.cov FROM inv inv1 JOIN inv inv2 ON inv1.i_item_sk = inv2.i_item_sk AND inv1.w_warehouse_sk = inv2.w_warehouse_sk WHERE inv1.d_moy = 1 AND inv2.d_moy = 2 ORDER BY inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy, inv1.mean, inv1.cov, inv2.d_moy, inv2.mean, inv2.cov"
}"""

Q39_EXPLANATION = """1. Pushed d_moy IN (1, 2) filter into CTE - original grouped all 12 months but only used 2.
2. Simplified CASE to mean != 0 AND stdev/mean > 1 for efficiency.
3. Used explicit JOIN syntax for better optimizer understanding.
4. Replaced 1+1 with literal 2."""


# ============================================================
# Q23 - 2.33x speedup (VERIFIED)
# Key: EXISTS instead of IN for semi-joins
# ============================================================

Q23_DAG = """Nodes:
  [frequent_ss_items] type=cte tables=[store_sales, date_dim, item] GROUP_BY HAVING
  [max_store_sales] type=cte tables=[store_sales, customer, date_dim] AGG
  [best_ss_customer] type=cte tables=[store_sales, customer] GROUP_BY HAVING
  [main_query] type=main tables=[catalog_sales, web_sales, customer, date_dim] UNION_ALL IN_SUBQUERY

Edges:
  frequent_ss_items -> main_query
  max_store_sales -> best_ss_customer
  best_ss_customer -> main_query"""

Q23_NODE_SQL = """[frequent_ss_items]:
SELECT substr(i_item_desc,1,30) itemdesc, i_item_sk item_sk, d_date solddate, count(*) cnt
FROM store_sales, date_dim, item
WHERE ss_sold_date_sk = d_date_sk AND ss_item_sk = i_item_sk AND d_year IN (2000,2001,2002,2003)
GROUP BY substr(i_item_desc,1,30), i_item_sk, d_date
HAVING count(*) > 4

[main_query]:
SELECT c_last_name, c_first_name, sales FROM (
  SELECT c_last_name, c_first_name, sum(cs_quantity*cs_list_price) sales
  FROM catalog_sales, customer, date_dim
  WHERE d_year = 2000 AND d_moy = 5 AND cs_sold_date_sk = d_date_sk
    AND cs_item_sk IN (SELECT item_sk FROM frequent_ss_items)
    AND cs_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer)
    AND cs_bill_customer_sk = c_customer_sk
  GROUP BY c_last_name, c_first_name
  UNION ALL
  SELECT c_last_name, c_first_name, sum(ws_quantity*ws_list_price) sales
  FROM web_sales, customer, date_dim
  WHERE d_year = 2000 AND d_moy = 5 AND ws_sold_date_sk = d_date_sk
    AND ws_item_sk IN (SELECT item_sk FROM frequent_ss_items)
    AND ws_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer)
    AND ws_bill_customer_sk = c_customer_sk
  GROUP BY c_last_name, c_first_name
) ORDER BY c_last_name, c_first_name, sales LIMIT 100"""

Q23_PLAN = """HASH_GROUP_BY[c_customer_sk] cost=40%
HASH_JOIN[ss_customer_sk=c_customer_sk] cost=20%
SEQ_SCAN[store_sales] rows=287M
SEQ_SCAN[catalog_sales] rows=143M"""

Q23_HINTS = """SQL-IN-001: IN to EXISTS Conversion
  Trigger: IN subquery on large result sets
  Fix: Convert to EXISTS for early termination
  Expected: 1.5-2x speedup"""

Q23_REWRITES = """{
  "frequent_ss_items": "SELECT substr(i_item_desc, 1, 30) AS itemdesc, i_item_sk AS item_sk, d_date AS solddate, COUNT(*) AS cnt FROM store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk JOIN item ON ss_item_sk = i_item_sk WHERE d_year BETWEEN 2000 AND 2003 GROUP BY substr(i_item_desc, 1, 30), i_item_sk, d_date HAVING COUNT(*) > 4",
  "main_query": "SELECT c_last_name, c_first_name, sales FROM (SELECT c_last_name, c_first_name, SUM(cs_quantity * cs_list_price) AS sales FROM catalog_sales JOIN date_dim ON cs_sold_date_sk = d_date_sk JOIN customer ON cs_bill_customer_sk = c_customer_sk WHERE d_year = 2000 AND d_moy = 5 AND EXISTS (SELECT 1 FROM frequent_ss_items f WHERE f.item_sk = cs_item_sk) AND EXISTS (SELECT 1 FROM best_ss_customer b WHERE b.c_customer_sk = cs_bill_customer_sk) GROUP BY c_last_name, c_first_name UNION ALL SELECT c_last_name, c_first_name, SUM(ws_quantity * ws_list_price) AS sales FROM web_sales JOIN date_dim ON ws_sold_date_sk = d_date_sk JOIN customer ON ws_bill_customer_sk = c_customer_sk WHERE d_year = 2000 AND d_moy = 5 AND EXISTS (SELECT 1 FROM frequent_ss_items f WHERE f.item_sk = ws_item_sk) AND EXISTS (SELECT 1 FROM best_ss_customer b WHERE b.c_customer_sk = ws_bill_customer_sk) GROUP BY c_last_name, c_first_name) ORDER BY c_last_name, c_first_name, sales LIMIT 100"
}"""

Q23_EXPLANATION = """1. Converted IN subqueries to EXISTS - stops searching after first match.
2. Used explicit JOIN syntax for better optimizer understanding.
3. Changed d_year IN (2000,2001,2002,2003) to BETWEEN 2000 AND 2003.
4. Each EXISTS check terminates early rather than materializing full subquery result."""


# ============================================================
# Build DSPy Examples
# ============================================================

def get_dag_gold_examples(num_examples: int = 3) -> List[dspy.Example]:
    """Get DAG-format gold examples for few-shot learning.

    Uses VERIFIED optimizations from Full-SQL benchmarks.

    Args:
        num_examples: Number of examples to return (max 3)

    Returns:
        List of dspy.Example objects with inputs marked
    """
    all_examples = [
        # Q15 - 2.98x (best)
        dspy.Example(
            query_dag=Q15_DAG,
            node_sql=Q15_NODE_SQL,
            execution_plan=Q15_PLAN,
            optimization_hints=Q15_HINTS,
            rewrites=Q15_REWRITES,
            explanation=Q15_EXPLANATION
        ).with_inputs("query_dag", "node_sql", "execution_plan", "optimization_hints"),

        # Q39 - 2.44x
        dspy.Example(
            query_dag=Q39_DAG,
            node_sql=Q39_NODE_SQL,
            execution_plan=Q39_PLAN,
            optimization_hints=Q39_HINTS,
            rewrites=Q39_REWRITES,
            explanation=Q39_EXPLANATION
        ).with_inputs("query_dag", "node_sql", "execution_plan", "optimization_hints"),

        # Q23 - 2.33x
        dspy.Example(
            query_dag=Q23_DAG,
            node_sql=Q23_NODE_SQL,
            execution_plan=Q23_PLAN,
            optimization_hints=Q23_HINTS,
            rewrites=Q23_REWRITES,
            explanation=Q23_EXPLANATION
        ).with_inputs("query_dag", "node_sql", "execution_plan", "optimization_hints"),
    ]

    return all_examples[:num_examples]

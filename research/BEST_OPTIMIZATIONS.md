# Best TPC-DS Query Optimization Pairs

**Last Updated:** 2026-02-05
**Collated from:** v5_benchmark_run, kimi_benchmark, deepseek_adaptive, mcts_llm

---

## Tier 1: Exceptional Wins (>= 2.5x speedup)

### Q11 - 4.00x (v5_run_20260205)
**Transform:** `early_filter` + `date_cte_isolate`
**Source:** `research/benchmarks/qt-sql/runs/benchmark_output/q11/iteration_1_validated_4.00x.sql`

```sql
WITH filtered_dates_2001 AS (
  SELECT d_date_sk FROM date_dim WHERE d_year = 2001
),
filtered_dates_2002 AS (
  SELECT d_date_sk FROM date_dim WHERE d_year = 2002
),
store_sales_2001 AS (
  SELECT ss_customer_sk, SUM(ss_ext_list_price - ss_ext_discount_amt) AS year_total
  FROM store_sales JOIN filtered_dates_2001 ON ss_sold_date_sk = d_date_sk
  GROUP BY ss_customer_sk
  HAVING SUM(ss_ext_list_price - ss_ext_discount_amt) > 0
),
store_sales_2002 AS (
  SELECT ss_customer_sk, SUM(ss_ext_list_price - ss_ext_discount_amt) AS year_total
  FROM store_sales JOIN filtered_dates_2002 ON ss_sold_date_sk = d_date_sk
  GROUP BY ss_customer_sk
),
web_sales_2001 AS (
  SELECT ws_bill_customer_sk, SUM(ws_ext_list_price - ws_ext_discount_amt) AS year_total
  FROM web_sales JOIN filtered_dates_2001 ON ws_sold_date_sk = d_date_sk
  GROUP BY ws_bill_customer_sk
  HAVING SUM(ws_ext_list_price - ws_ext_discount_amt) > 0
),
web_sales_2002 AS (
  SELECT ws_bill_customer_sk, SUM(ws_ext_list_price - ws_ext_discount_amt) AS year_total
  FROM web_sales JOIN filtered_dates_2002 ON ws_sold_date_sk = d_date_sk
  GROUP BY ws_bill_customer_sk
)
SELECT c.c_customer_id AS customer_id, c.c_first_name AS customer_first_name,
       c.c_last_name AS customer_last_name, c.c_birth_country AS customer_birth_country
FROM store_sales_2001 AS s1
JOIN store_sales_2002 AS s2 ON s1.ss_customer_sk = s2.ss_customer_sk
JOIN web_sales_2001 AS w1 ON s1.ss_customer_sk = w1.ws_bill_customer_sk
JOIN web_sales_2002 AS w2 ON s1.ss_customer_sk = w2.ws_bill_customer_sk
JOIN customer AS c ON s1.ss_customer_sk = c.c_customer_sk
WHERE w2.year_total / w1.year_total > s2.year_total / s1.year_total
ORDER BY c.c_customer_id, c.c_first_name, c.c_last_name, c.c_birth_country
LIMIT 100
```

**Key Insight:** Split year_total CTE into 4 specialized CTEs (store_2001, store_2002, web_2001, web_2002) with HAVING > 0 to eliminate zero-sum customers early. This avoids computing and joining rows that will be filtered out later.

---

### Q15 - 3.17x (v5_run_20260205)
**Transform:** `or_to_union`
**Source:** `research/benchmarks/qt-sql/runs/benchmark_output/q15/iteration_1_validated_3.17x.sql`

```sql
WITH filtered_dates AS (
  SELECT d_date_sk FROM date_dim WHERE d_qoy = 1 AND d_year = 2001
),
filtered_sales AS (
  SELECT cs_sales_price, ca_zip
  FROM catalog_sales
  JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
  JOIN customer ON cs_bill_customer_sk = c_customer_sk
  JOIN customer_address ON c_current_addr_sk = ca_address_sk
  WHERE SUBSTRING(ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')
  UNION ALL
  SELECT cs_sales_price, ca_zip
  FROM catalog_sales
  JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
  JOIN customer ON cs_bill_customer_sk = c_customer_sk
  JOIN customer_address ON c_current_addr_sk = ca_address_sk
  WHERE ca_state IN ('CA', 'WA', 'GA')
  UNION ALL
  SELECT cs_sales_price, ca_zip
  FROM catalog_sales
  JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
  JOIN customer ON cs_bill_customer_sk = c_customer_sk
  JOIN customer_address ON c_current_addr_sk = ca_address_sk
  WHERE cs_sales_price > 500
)
SELECT ca_zip, SUM(cs_sales_price)
FROM filtered_sales
GROUP BY ca_zip
ORDER BY ca_zip
LIMIT 100
```

**Key Insight:** Split OR condition (zip IN (...) OR state IN (...) OR price > 500) into 3 UNION ALL branches. Each branch can use separate index access paths and be optimized independently.

---

### Q1 - 2.92x (kimi_benchmark)
**Transform:** `decorrelate` + `early_filter`
**Source:** `research/experiments/optimizations/kimi_q1-q30_20260202_213955/benchmark_ready/q1_optimized.sql`

```sql
WITH filtered_returns AS (
  SELECT sr.sr_customer_sk, sr.sr_store_sk, sr.SR_FEE
  FROM store_returns AS sr
  JOIN date_dim AS d ON sr.sr_returned_date_sk = d.d_date_sk
  JOIN store AS s ON sr.sr_store_sk = s.s_store_sk
  WHERE d.d_year = 2000 AND s.s_state = 'SD'
),
customer_total_return AS (
  SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk,
         SUM(SR_FEE) AS ctr_total_return
  FROM filtered_returns
  GROUP BY sr_customer_sk, sr_store_sk
),
store_avg_return AS (
  SELECT ctr_store_sk, AVG(ctr_total_return) * 1.2 AS avg_return_threshold
  FROM customer_total_return
  GROUP BY ctr_store_sk
)
SELECT c.c_customer_id
FROM customer_total_return AS ctr1
JOIN store_avg_return AS sar ON ctr1.ctr_store_sk = sar.ctr_store_sk
JOIN customer AS c ON ctr1.ctr_customer_sk = c.c_customer_sk
WHERE ctr1.ctr_total_return > sar.avg_return_threshold
ORDER BY c.c_customer_id
LIMIT 100
```

**Key Insight:** Push date and store filters into the first CTE (filtered_returns) to reduce intermediate result size before aggregation.

---

### Q93 - 2.73x (kimi_benchmark)
**Transform:** `early_filter`
**Source:** `research/experiments/optimizations/kimi_q31-q99_20260202_215203/benchmark_ready/q93_optimized.sql`

```sql
WITH filtered_reason AS (
  SELECT r_reason_sk FROM reason WHERE r_reason_desc = 'duplicate purchase'
),
filtered_returns AS (
  SELECT sr_item_sk, sr_ticket_number, sr_return_quantity
  FROM store_returns
  JOIN filtered_reason ON sr_reason_sk = r_reason_sk
)
SELECT ss_customer_sk, SUM(act_sales) AS sumsales
FROM (
  SELECT ss.ss_customer_sk,
         CASE WHEN NOT fr.sr_return_quantity IS NULL
              THEN (ss.ss_quantity - fr.sr_return_quantity) * ss.ss_sales_price
              ELSE (ss.ss_quantity * ss.ss_sales_price)
         END AS act_sales
  FROM store_sales AS ss
  JOIN filtered_returns AS fr ON (fr.sr_item_sk = ss.ss_item_sk
                                  AND fr.sr_ticket_number = ss.ss_ticket_number)
) AS t
GROUP BY ss_customer_sk
ORDER BY sumsales, ss_customer_sk
LIMIT 100
```

**Key Insight:** Push the reason filter ('duplicate purchase') to filter store_returns BEFORE joining with store_sales. Also converts LEFT JOIN to INNER JOIN since the WHERE clause filters out non-matching rows anyway.

---

## Tier 2: Strong Wins (2.0x - 2.5x speedup)

### Q9 - 2.11x (deepseek_20260204) / 2.07x (v5_run)
**Transform:** `pushdown` (quantity_range)
**Source:** `research/benchmarks/qt-sql/runs/benchmark_output/q9/iteration_2_validated_2.07x.sql`

```sql
WITH quantity_1_20_stats AS (
  SELECT COUNT(*) AS cnt, AVG(ss_ext_sales_price) AS avg_ext_price,
         AVG(ss_net_profit) AS avg_net_profit
  FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20
),
quantity_21_40_stats AS (
  SELECT COUNT(*) AS cnt, AVG(ss_ext_sales_price) AS avg_ext_price,
         AVG(ss_net_profit) AS avg_net_profit
  FROM store_sales WHERE ss_quantity BETWEEN 21 AND 40
),
quantity_41_60_stats AS (
  SELECT COUNT(*) AS cnt, AVG(ss_ext_sales_price) AS avg_ext_price,
         AVG(ss_net_profit) AS avg_net_profit
  FROM store_sales WHERE ss_quantity BETWEEN 41 AND 60
),
quantity_61_80_stats AS (
  SELECT COUNT(*) AS cnt, AVG(ss_ext_sales_price) AS avg_ext_price,
         AVG(ss_net_profit) AS avg_net_profit
  FROM store_sales WHERE ss_quantity BETWEEN 61 AND 80
),
quantity_81_100_stats AS (
  SELECT COUNT(*) AS cnt, AVG(ss_ext_sales_price) AS avg_ext_price,
         AVG(ss_net_profit) AS avg_net_profit
  FROM store_sales WHERE ss_quantity BETWEEN 81 AND 100
)
SELECT
  CASE WHEN q1.cnt > 2972190 THEN q1.avg_ext_price ELSE q1.avg_net_profit END AS bucket1,
  CASE WHEN q2.cnt > 4505785 THEN q2.avg_ext_price ELSE q2.avg_net_profit END AS bucket2,
  CASE WHEN q3.cnt > 1575726 THEN q3.avg_ext_price ELSE q3.avg_net_profit END AS bucket3,
  CASE WHEN q4.cnt > 3188917 THEN q4.avg_ext_price ELSE q4.avg_net_profit END AS bucket4,
  CASE WHEN q5.cnt > 3525216 THEN q5.avg_ext_price ELSE q5.avg_net_profit END AS bucket5
FROM reason
CROSS JOIN quantity_1_20_stats AS q1
CROSS JOIN quantity_21_40_stats AS q2
CROSS JOIN quantity_41_60_stats AS q3
CROSS JOIN quantity_61_80_stats AS q4
CROSS JOIN quantity_81_100_stats AS q5
WHERE r_reason_sk = 1
```

**Key Insight:** Original query scans store_sales 10 times (5 ranges × 2 aggregates). By computing all 3 aggregates (COUNT, AVG ext_price, AVG net_profit) per range in one scan, reduces to 5 scans.

---

## Tier 3: Good Wins (1.3x - 2.0x speedup)

### Q90 - 1.57x (kimi_benchmark)
**Transform:** `early_filter`
**Source:** `research/experiments/optimizations/kimi_q31-q99_20260202_215203/benchmark_ready/q90_optimized.sql`

```sql
WITH filtered_web_data AS (
  SELECT CASE WHEN t.t_hour BETWEEN 10 AND 11 THEN 1 END AS am_flag,
         CASE WHEN t.t_hour BETWEEN 16 AND 17 THEN 1 END AS pm_flag
  FROM web_sales AS ws
  JOIN household_demographics AS hd ON ws.ws_ship_hdemo_sk = hd.hd_demo_sk
  JOIN time_dim AS t ON ws.ws_sold_time_sk = t.t_time_sk
  JOIN web_page AS wp ON ws.ws_web_page_sk = wp.wp_web_page_sk
  WHERE hd.hd_dep_count = 2
    AND wp.wp_char_count BETWEEN 5000 AND 5200
    AND (t.t_hour BETWEEN 10 AND 11 OR t.t_hour BETWEEN 16 AND 17)
),
counts AS (
  SELECT COUNT(am_flag) AS amc, COUNT(pm_flag) AS pmc FROM filtered_web_data
)
SELECT CAST(amc AS DECIMAL(15, 4)) / CAST(pmc AS DECIMAL(15, 4)) AS am_pm_ratio
FROM counts
ORDER BY am_pm_ratio
LIMIT 100
```

---

### Q95 - 1.37x (kimi_benchmark)
**Transform:** `materialize_cte`
**Source:** `research/experiments/optimizations/kimi_q31-q99_20260202_215203/benchmark_ready/q95_optimized.sql`

```sql
WITH ws_wh AS (
  SELECT ws1.ws_order_number, ws1.ws_warehouse_sk AS wh1, ws2.ws_warehouse_sk AS wh2
  FROM web_sales AS ws1, web_sales AS ws2
  WHERE ws1.ws_order_number = ws2.ws_order_number
    AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
)
SELECT COUNT(DISTINCT ws_order_number) AS "order count",
       SUM(ws_ext_ship_cost) AS "total shipping cost",
       SUM(ws_net_profit) AS "total net profit"
FROM web_sales AS ws1
JOIN date_dim ON ws1.ws_ship_date_sk = d_date_sk
JOIN customer_address ON ws1.ws_ship_addr_sk = ca_address_sk
JOIN web_site ON ws1.ws_web_site_sk = web_site_sk
WHERE d_date BETWEEN '1999-2-01' AND (CAST('1999-2-01' AS DATE) + INTERVAL '60' DAY)
  AND ca_state = 'NC'
  AND web_company_name = 'pri'
  AND EXISTS(SELECT 1 FROM ws_wh WHERE ws_wh.ws_order_number = ws1.ws_order_number)
  AND EXISTS(SELECT 1 FROM web_returns
             JOIN ws_wh ON wr_order_number = ws_wh.ws_order_number
             WHERE wr_order_number = ws1.ws_order_number)
ORDER BY COUNT(DISTINCT ws_order_number)
LIMIT 100
```

---

### Q74 - 1.36x (kimi_benchmark)
**Transform:** `union_cte_split`
**Source:** `research/experiments/optimizations/kimi_q31-q99_20260202_215203/benchmark_ready/q74_optimized.sql`

```sql
WITH year_total_store AS (
  SELECT c_customer_id AS customer_id, c_first_name, c_last_name, d_year AS year,
         STDDEV_SAMP(ss_net_paid) AS year_total
  FROM customer, store_sales, date_dim
  WHERE c_customer_sk = ss_customer_sk
    AND ss_sold_date_sk = d_date_sk
    AND d_year IN (1999, 2000)
  GROUP BY c_customer_id, c_first_name, c_last_name, d_year
),
year_total_web AS (
  SELECT c_customer_id AS customer_id, c_first_name, c_last_name, d_year AS year,
         STDDEV_SAMP(ws_net_paid) AS year_total
  FROM customer, web_sales, date_dim
  WHERE c_customer_sk = ws_bill_customer_sk
    AND ws_sold_date_sk = d_date_sk
    AND d_year IN (1999, 2000)
  GROUP BY c_customer_id, c_first_name, c_last_name, d_year
)
SELECT t_s_secyear.customer_id, t_s_secyear.customer_first_name, t_s_secyear.customer_last_name
FROM year_total_store AS t_s_firstyear, year_total_store AS t_s_secyear,
     year_total_web AS t_w_firstyear, year_total_web AS t_w_secyear
WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_w_secyear.customer_id
  AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
  AND t_s_firstyear.year = 1999 AND t_s_secyear.year = 2000
  AND t_w_firstyear.year = 1999 AND t_w_secyear.year = 2000
  AND t_s_firstyear.year_total > 0 AND t_w_firstyear.year_total > 0
  AND CASE WHEN t_w_firstyear.year_total > 0
           THEN t_w_secyear.year_total / t_w_firstyear.year_total ELSE NULL END
    > CASE WHEN t_s_firstyear.year_total > 0
           THEN t_s_secyear.year_total / t_s_firstyear.year_total ELSE NULL END
ORDER BY 2, 1, 3
LIMIT 100
```

**Key Insight:** Split the generic year_total CTE (with UNION ALL of store and web) into separate year_total_store and year_total_web CTEs. Eliminates redundant scans since each source type is now queried independently.

---

### Q6 - 1.33x (kimi_benchmark)
**Transform:** `date_cte_isolate` + `decorrelate`
**Source:** `research/experiments/optimizations/kimi_q1-q30_20260202_213955/benchmark_ready/q6_optimized.sql`

```sql
WITH target_month AS (
  SELECT DISTINCT d_month_seq FROM date_dim WHERE d_year = 2002 AND d_moy = 3
),
category_avg AS (
  SELECT i_category, AVG(i_current_price) * 1.2 AS price_threshold
  FROM item GROUP BY i_category
)
SELECT a.ca_state AS state, COUNT(*) AS cnt
FROM customer_address AS a
JOIN customer AS c ON a.ca_address_sk = c.c_current_addr_sk
JOIN store_sales AS s ON c.c_customer_sk = s.ss_customer_sk
JOIN date_dim AS d ON s.ss_sold_date_sk = d.d_date_sk
JOIN target_month AS tm ON d.d_month_seq = tm.d_month_seq
JOIN item AS i ON s.ss_item_sk = i.i_item_sk
JOIN category_avg AS ca ON i.i_category = ca.i_category
WHERE i.i_current_price > ca.price_threshold
GROUP BY a.ca_state
HAVING COUNT(*) >= 10
ORDER BY cnt, a.ca_state
LIMIT 100
```

**Key Insight:** Isolate the date filter into a CTE (target_month) and pre-compute category averages. Join to these small CTEs instead of repeating correlated subqueries.

---

## Summary by Transform

| Transform | Best Query | Speedup | Count |
|-----------|------------|---------|-------|
| `early_filter` + `date_cte_isolate` | Q11 | 4.00x | 1 |
| `or_to_union` | Q15 | 3.17x | 2 |
| `decorrelate` | Q1 | 2.92x | 3 |
| `early_filter` | Q93 | 2.73x | 4 |
| `pushdown` | Q9 | 2.11x | 2 |
| `union_cte_split` | Q74 | 1.36x | 1 |
| `materialize_cte` | Q95 | 1.37x | 1 |
| `date_cte_isolate` | Q6 | 1.33x | 2 |

---

## File Locations

| Query | Speedup | Source | Path |
|-------|---------|--------|------|
| Q11 | 4.00x | v5_run | `research/benchmarks/qt-sql/runs/benchmark_output/q11/iteration_1_validated_4.00x.sql` |
| Q15 | 3.17x | v5_run | `research/benchmarks/qt-sql/runs/benchmark_output/q15/iteration_1_validated_3.17x.sql` |
| Q9 | 2.07x | v5_run | `research/benchmarks/qt-sql/runs/benchmark_output/q9/iteration_2_validated_2.07x.sql` |
| Q1 | 2.92x | kimi | `research/experiments/optimizations/kimi_q1-q30_20260202_213955/benchmark_ready/q1_optimized.sql` |
| Q93 | 2.73x | kimi | `research/experiments/optimizations/kimi_q31-q99_20260202_215203/benchmark_ready/q93_optimized.sql` |
| Q90 | 1.57x | kimi | `research/experiments/optimizations/kimi_q31-q99_20260202_215203/benchmark_ready/q90_optimized.sql` |
| Q95 | 1.37x | kimi | `research/experiments/optimizations/kimi_q31-q99_20260202_215203/benchmark_ready/q95_optimized.sql` |
| Q74 | 1.36x | kimi | `research/experiments/optimizations/kimi_q31-q99_20260202_215203/benchmark_ready/q74_optimized.sql` |
| Q6 | 1.33x | kimi | `research/experiments/optimizations/kimi_q1-q30_20260202_213955/benchmark_ready/q6_optimized.sql` |

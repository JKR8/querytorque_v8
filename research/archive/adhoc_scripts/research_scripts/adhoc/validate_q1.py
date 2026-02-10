#!/usr/bin/env python3
"""Validate DSPy V5 optimized Q1 on full TPC-DS database"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "packages" / "qt-shared"))
sys.path.insert(0, str(Path(__file__).parent / "packages" / "qt-sql"))

from qt_sql.validation.sql_validator import SQLValidator

original_sql = """with customer_total_return as
(select sr_customer_sk as ctr_customer_sk
,sr_store_sk as ctr_store_sk
,sum(SR_FEE) as ctr_total_return
from store_returns
,date_dim
where sr_returned_date_sk = d_date_sk
and d_year =2000
group by sr_customer_sk
,sr_store_sk)
 select c_customer_id
from customer_total_return ctr1
,store
,customer
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
from customer_total_return ctr2
where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'SD'
and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
 LIMIT 100"""

optimized_sql = """WITH customer_total_return AS (
  SELECT sr_customer_sk AS ctr_customer_sk,
         sr_store_sk AS ctr_store_sk,
         SUM(SR_FEE) AS ctr_total_return
  FROM store_returns
  JOIN date_dim ON sr_returned_date_sk = d_date_sk
  WHERE d_year = 2000
  GROUP BY sr_customer_sk, sr_store_sk
),
store_avg_return AS (
  SELECT ctr_store_sk,
         AVG(ctr_total_return) * 1.2 AS avg_threshold
  FROM customer_total_return
  GROUP BY ctr_store_sk
)
SELECT c_customer_id
FROM customer_total_return ctr1
JOIN store ON s_store_sk = ctr1.ctr_store_sk
JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
JOIN store_avg_return sar ON ctr1.ctr_store_sk = sar.ctr_store_sk
WHERE ctr1.ctr_total_return > sar.avg_threshold
  AND s_state = 'SD'
ORDER BY c_customer_id
LIMIT 100"""

full_db = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"

print("="*80)
print("Validating DSPy V5 Optimized Q1 on Full TPC-DS SF100")
print("="*80)
print(f"\nDatabase: {full_db} (28GB)")
print("\nRunning validation (this may take 1-5 minutes)...\n")

validator = SQLValidator(database=full_db)
result = validator.validate(original_sql, optimized_sql)

print("="*80)
print("VALIDATION RESULT")
print("="*80)
print(f"Status: {result.status.value}")
print(f"Speedup: {result.speedup:.2f}x")
print(f"Original time: {result.original_timing_ms/1000:.2f}s")
print(f"Optimized time: {result.optimized_timing_ms/1000:.2f}s")
print(f"Row counts match: {result.row_counts_match}")
print(f"Original rows: {result.original_row_count}")
print(f"Optimized rows: {result.optimized_row_count}")
if result.errors:
    print(f"Errors: {result.errors}")
print("="*80)

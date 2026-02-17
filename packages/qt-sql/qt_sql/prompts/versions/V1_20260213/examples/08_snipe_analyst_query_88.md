You are the diagnostic analyst for query query_88. You've seen 4 parallel attempts at >=2.0x speedup on this query. Your job: diagnose what worked, what didn't, and WHY — then design a strategy the sniper couldn't have known without these empirical results.

## Target: >=2.0x speedup
Anything below 2.0x is a miss. The sniper you deploy must be given a strategy with genuine headroom to reach this bar.

## Previous Optimization Attempts
Target: **>=2.0x** | 4 workers tried | 4 reached target

### W2: moderate_dimension_isolation → 6.2373882475101565x ★ BEST [WIN (6.2373882475101565x)]
- **Examples**: dimension_cte_isolate, date_cte_isolate, shared_dimension_multi_channel
- **Transforms**: dimension_cte_isolate, date_cte_isolate
- **Approach**: Isolate filtered dimension tables (store, household_demographics) and time ranges into separate CTEs before joining, enabling predicate pushdown and reusing shared dimension filters across time windows.
- **Optimized SQL:**
```sql
WITH filtered_store AS (
    SELECT s_store_sk
    FROM store
    WHERE s_store_name = 'ese'
),
filtered_hd AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE (
        (hd_dep_count = -1 AND hd_vehicle_count <= -1 + 2)
        OR (hd_dep_count = 4 AND hd_vehicle_count <= 4 + 2)
        OR (hd_dep_count = 3 AND hd_vehicle_count <= 3 + 2)
    )
),
time_ranges AS (
    SELECT 
        t_time_sk,
        CASE 
            WHEN t_hour = 8 AND t_minute >= 30 THEN 1
            WHEN t_hour = 9 AND t_minute < 30 THEN 2
            WHEN t_hour = 9 AND t_minute >= 30 THEN 3
            WHEN t_hour = 10 AND t_minute < 30 THEN 4
            WHEN t_hour = 10 AND t_minute >= 30 THEN 5
            WHEN t_hour = 11 AND t_minute < 30 THEN 6
            WHEN t_hour = 11 AND t_minute >= 30 THEN 7
            WHEN t_hour = 12 AND t_minute < 30 THEN 8
        END AS time_window
    FROM time_dim
    WHERE (
        (t_hour = 8 AND t_minute >= 30) OR
        (t_hour = 9 AND t_minute < 30) OR
        (t_hour = 9 AND t_minute >= 30) OR
        (t_hour = 10 AND t_minute < 30) OR
        (t_hour = 10 AND t_minute >= 30) OR
        (t_hour = 11 AND t_minute < 30) OR
        (t_hour = 11 AND t_minute >= 30) OR
        (t_hour = 12 AND t_minute < 30)
    )
),
sales_with_time AS (
    SELECT 
        tr.time_window,
        ss.ss_sold_time_sk
    FROM store_sales ss
    JOIN filtered_store fs ON ss.ss_store_sk = fs.s_store_sk
    JOIN filtered_hd fhd ON ss.ss_hdemo_sk = fhd.hd_demo_sk
    JOIN time_ranges tr ON ss.ss_sold_time_sk = tr.t_time_sk
)
SELECT 
    COUNT(CASE WHEN time_window = 1 THEN 1 END) AS h8_30_to_9,
    COUNT(CASE WHEN time_window = 2 THEN 1 END) AS h9_to_9_30,
    COUNT(CASE WHEN time_window = 3 THEN 1 END) AS h9_30_to_10,
    COUNT(CASE WHEN time_window = 4 THEN 1 END) AS h10_to_10_30,
    COUNT(CASE WHEN time_window = 5 THEN 1 END) AS h10_30_to_11,
    COUNT(CASE WHEN time_window = 6 THEN 1 END) AS h11_to_11_30,
    COUNT(CASE WHEN time_window = 7 THEN 1 END) AS h11_30_to_12,
    COUNT(CASE WHEN time_window = 8 THEN 1 END) AS h12_to_12_30
FROM sales_with_time;
```

### W4: novel_structural_transform → 6.104619135142192x [WIN (6.104619135142192x)]
- **Examples**: or_to_union, union_cte_split, composite_decorrelate_union
- **Transforms**: or_to_union, union_cte_split
- **Approach**: Transform OR conditions on household_demographics into UNION ALL branches for better index usage, split into specialized CTEs, and apply decorrelation techniques to restructure the entire query flow.
- **Optimized SQL:**
```sql
WITH filtered_store AS (
    SELECT s_store_sk
    FROM store
    WHERE s_store_name = 'ese'
),
filtered_household AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE (hd_dep_count = -1 AND hd_vehicle_count <= 1)
       OR (hd_dep_count = 4 AND hd_vehicle_count <= 6)
       OR (hd_dep_count = 3 AND hd_vehicle_count <= 5)
),
time_slices AS (
    SELECT 
        t_time_sk,
        CASE 
            WHEN t_hour = 8 AND t_minute >= 30 THEN 1
            WHEN t_hour = 9 AND t_minute < 30 THEN 2
            WHEN t_hour = 9 AND t_minute >= 30 THEN 3
            WHEN t_hour = 10 AND t_minute < 30 THEN 4
            WHEN t_hour = 10 AND t_minute >= 30 THEN 5
            WHEN t_hour = 11 AND t_minute < 30 THEN 6
            WHEN t_hour = 11 AND t_minute >= 30 THEN 7
            WHEN t_hour = 12 AND t_minute < 30 THEN 8
            ELSE 0
        END AS slice_id
    FROM time_dim
    WHERE slice_id > 0
),
qualified_sales AS (
    SELECT 
        ss.ss_sold_time_sk,
        ts.slice_id
    FROM store_sales ss
    JOIN filtered_store fs ON ss.ss_store_sk = fs.s_store_sk
    JOIN filtered_household fh ON ss.ss_hdemo_sk = fh.hd_demo_sk
    JOIN time_slices ts ON ss.ss_sold_time_sk = ts.t_time_sk
)
SELECT 
    COUNT(CASE WHEN slice_id = 1 THEN 1 END) AS h8_30_to_9,
    COUNT(CASE WHEN slice_id = 2 THEN 1 END) AS h9_to_9_30,
    COUNT(CASE WHEN slice_id = 3 THEN 1 END) AS h9_30_to_10,
    COUNT(CASE WHEN slice_id = 4 THEN 1 END) AS h10_to_10_30,
    COUNT(CASE WHEN slice_id = 5 THEN 1 END) AS h10_30_to_11,
    COUNT(CASE WHEN slice_id = 6 THEN 1 END) AS h11_to_11_30,
    COUNT(CASE WHEN slice_id = 7 THEN 1 END) AS h11_30_to_12,
    COUNT(CASE WHEN slice_id = 8 THEN 1 END) AS h12_to_12_30
FROM qualified_sales;
```

### W3: aggressive_single_pass_restructure → 5.853343016368592x [WIN (5.853343016368592x)]
- **Examples**: single_pass_aggregation, prefetch_fact_join, multi_date_range_cte
- **Transforms**: single_pass_aggregation, prefetch_fact_join
- **Approach**: Consolidate all time-window subqueries into a single CTE that scans store_sales once with conditional aggregation, prefetch filtered dimensions, and handle multiple time ranges in a unified structure.
- **Optimized SQL:**
```sql
WITH filtered_data AS (
  SELECT
    time_dim.t_hour,
    time_dim.t_minute
  FROM store_sales
  JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
  JOIN time_dim ON store_sales.ss_sold_time_sk = time_dim.t_time_sk
  JOIN store ON store_sales.ss_store_sk = store.s_store_sk
  WHERE store.s_store_name = 'ese'
    AND (
      (household_demographics.hd_dep_count = -1 AND household_demographics.hd_vehicle_count <= 1)
      OR (household_demographics.hd_dep_count = 4 AND household_demographics.hd_vehicle_count <= 6)
      OR (household_demographics.hd_dep_count = 3 AND household_demographics.hd_vehicle_count <= 5)
    )
    AND (
      (time_dim.t_hour = 8 AND time_dim.t_minute >= 30)
      OR (time_dim.t_hour = 9)
      OR (time_dim.t_hour = 10)
      OR (time_dim.t_hour = 11)
      OR (time_dim.t_hour = 12 AND time_dim.t_minute < 30)
    )
)
SELECT
  COUNT(CASE WHEN t_hour = 8 AND t_minute >= 30 THEN 1 END) AS h8_30_to_9,
  COUNT(CASE WHEN t_hour = 9 AND t_minute < 30 THEN 1 END) AS h9_to_9_30,
  COUNT(CASE WHEN t_hour = 9 AND t_minute >= 30 THEN 1 END) AS h9_30_to_10,
  COUNT(CASE WHEN t_hour = 10 AND t_minute < 30 THEN 1 END) AS h10_to_10_30,
  COUNT(CASE WHEN t_hour = 10 AND t_minute >= 30 THEN 1 END) AS h10_30_to_11,
  COUNT(CASE WHEN t_hour = 11 AND t_minute < 30 THEN 1 END) AS h11_to_11_30,
  COUNT(CASE WHEN t_hour = 11 AND t_minute >= 30 THEN 1 END) AS h11_30_to_12,
  COUNT(CASE WHEN t_hour = 12 AND t_minute < 30 THEN 1 END) AS h12_to_12_30
FROM filtered_data
```

### W1: conservative_early_reduction → 5.269233093530888x [WIN (5.269233093530888x)]
- **Examples**: early_filter, pushdown, materialize_cte
- **Transforms**: early_filter, pushdown
- **Approach**: Apply aggressive early filtering to dimension tables before joining, push predicates down into subqueries, and materialize repeated filter patterns into CTEs to avoid recomputation.
- **Optimized SQL:**
```sql
WITH filtered_store AS (
    SELECT s_store_sk
    FROM store
    WHERE s_store_name = 'ese'
),
filtered_household AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE (
        (hd_dep_count = -1 AND hd_vehicle_count <= 1)  -- -1 + 2 = 1
        OR (hd_dep_count = 4 AND hd_vehicle_count <= 6)  -- 4 + 2 = 6
        OR (hd_dep_count = 3 AND hd_vehicle_count <= 5)  -- 3 + 2 = 5
    )
),
filtered_time AS (
    SELECT t_time_sk, t_hour, t_minute
    FROM time_dim
    WHERE t_hour IN (8, 9, 10, 11, 12)
),
qualified_sales AS (
    SELECT 
        ss_sold_time_sk,
        t_hour,
        t_minute
    FROM store_sales
    INNER JOIN filtered_time ON ss_sold_time_sk = t_time_sk
    INNER JOIN filtered_household ON ss_hdemo_sk = hd_demo_sk
    INNER JOIN filtered_store ON ss_store_sk = s_store_sk
)
SELECT
    (SELECT COUNT(*) FROM qualified_sales WHERE t_hour = 8 AND t_minute >= 30) AS h8_30_to_9,
    (SELECT COUNT(*) FROM qualified_sales WHERE t_hour = 9 AND t_minute < 30) AS h9_to_9_30,
    (SELECT COUNT(*) FROM qualified_sales WHERE t_hour = 9 AND t_minute >= 30) AS h9_30_to_10,
    (SELECT COUNT(*) FROM qualified_sales WHERE t_hour = 10 AND t_minute < 30) AS h10_to_10_30,
    (SELECT COUNT(*) FROM qualified_sales WHERE t_hour = 10 AND t_minute >= 30) AS h10_30_to_11,
    (SELECT COUNT(*) FROM qualified_sales WHERE t_hour = 11 AND t_minute < 30) AS h11_to_11_30,
    (SELECT COUNT(*) FROM qualified_sales WHERE t_hour = 11 AND t_minute >= 30) AS h11_30_to_12,
    (SELECT COUNT(*) FROM qualified_sales WHERE t_hour = 12 AND t_minute < 30) AS h12_to_12_30
```

## Original SQL (query_88, duckdb v1.4.3)

```sql
 1 | select  *
 2 | from
 3 |  (select count(*) h8_30_to_9
 4 |  from store_sales, household_demographics , time_dim, store
 5 |  where ss_sold_time_sk = time_dim.t_time_sk   
 6 |      and ss_hdemo_sk = household_demographics.hd_demo_sk 
 7 |      and ss_store_sk = s_store_sk
 8 |      and time_dim.t_hour = 8
 9 |      and time_dim.t_minute >= 30
10 |      and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
11 |           (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
12 |           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2)) 
13 |      and store.s_store_name = 'ese') s1,
14 |  (select count(*) h9_to_9_30 
15 |  from store_sales, household_demographics , time_dim, store
16 |  where ss_sold_time_sk = time_dim.t_time_sk
17 |      and ss_hdemo_sk = household_demographics.hd_demo_sk
18 |      and ss_store_sk = s_store_sk 
19 |      and time_dim.t_hour = 9 
20 |      and time_dim.t_minute < 30
21 |      and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
22 |           (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
23 |           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
24 |      and store.s_store_name = 'ese') s2,
25 |  (select count(*) h9_30_to_10 
26 |  from store_sales, household_demographics , time_dim, store
27 |  where ss_sold_time_sk = time_dim.t_time_sk
28 |      and ss_hdemo_sk = household_demographics.hd_demo_sk
29 |      and ss_store_sk = s_store_sk
30 |      and time_dim.t_hour = 9
31 |      and time_dim.t_minute >= 30
32 |      and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
33 |           (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
34 |           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
35 |      and store.s_store_name = 'ese') s3,
36 |  (select count(*) h10_to_10_30
37 |  from store_sales, household_demographics , time_dim, store
38 |  where ss_sold_time_sk = time_dim.t_time_sk
39 |      and ss_hdemo_sk = household_demographics.hd_demo_sk
40 |      and ss_store_sk = s_store_sk
41 |      and time_dim.t_hour = 10 
42 |      and time_dim.t_minute < 30
43 |      and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
44 |           (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
45 |           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
46 |      and store.s_store_name = 'ese') s4,
47 |  (select count(*) h10_30_to_11
48 |  from store_sales, household_demographics , time_dim, store
49 |  where ss_sold_time_sk = time_dim.t_time_sk
50 |      and ss_hdemo_sk = household_demographics.hd_demo_sk
51 |      and ss_store_sk = s_store_sk
52 |      and time_dim.t_hour = 10 
53 |      and time_dim.t_minute >= 30
54 |      and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
55 |           (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
56 |           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
57 |      and store.s_store_name = 'ese') s5,
58 |  (select count(*) h11_to_11_30
59 |  from store_sales, household_demographics , time_dim, store
60 |  where ss_sold_time_sk = time_dim.t_time_sk
61 |      and ss_hdemo_sk = household_demographics.hd_demo_sk
62 |      and ss_store_sk = s_store_sk 
63 |      and time_dim.t_hour = 11
64 |      and time_dim.t_minute < 30
65 |      and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
66 |           (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
67 |           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
68 |      and store.s_store_name = 'ese') s6,
69 |  (select count(*) h11_30_to_12
70 |  from store_sales, household_demographics , time_dim, store
71 |  where ss_sold_time_sk = time_dim.t_time_sk
72 |      and ss_hdemo_sk = household_demographics.hd_demo_sk
73 |      and ss_store_sk = s_store_sk
74 |      and time_dim.t_hour = 11
75 |      and time_dim.t_minute >= 30
76 |      and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
77 |           (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
78 |           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
79 |      and store.s_store_name = 'ese') s7,
80 |  (select count(*) h12_to_12_30
81 |  from store_sales, household_demographics , time_dim, store
82 |  where ss_sold_time_sk = time_dim.t_time_sk
83 |      and ss_hdemo_sk = household_demographics.hd_demo_sk
84 |      and ss_store_sk = s_store_sk
85 |      and time_dim.t_hour = 12
86 |      and time_dim.t_minute < 30
87 |      and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
88 |           (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
89 |           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
90 |      and store.s_store_name = 'ese') s8
91 | ;
```

## EXPLAIN ANALYZE Plan

```
┌─────────────┐
│  PROJECTION │
│    ──────   │
│  h8_30_to_9 │
│  h9_to_9_30 │
│ h9_30_to_10 │
│ h10_to_10_30│
│ h10_30_to_11│
│ h11_to_11_30│
│ h11_30_to_12│
│ h12_to_12_30│
│             │
│    ~1 row   │
└──────┬──────┘
┌──────┴──────┐
│CROSS_PRODUCT├─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
└──────┬──────┘                                                                                                                                                                                                                                 
┌──────┴──────┐                                                                                                                                                                                                                                 
│CROSS_PRODUCT│                                                                                                                                                                                                                                 
│             │                                                                                                                                                                                                                                 
│             ├─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
│             │                                                                                                                                                                                                                                 
└──────┬──────┘                                                                                                                                                                                                                                 
┌──────┴──────┐                                                                                                                                                                                                                                 
│CROSS_PRODUCT│                                                                                                                                                                                                                                 
│             │                                                                                                                                                                                                                                 
│             │                                                                                                                                                                                                                                 
│             │                                                                                                                                                                                                                                 
│             │                                                                                                                                                                                                                                 
│             ├─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
│             │                                                                                                                                                                                                                                 
│             │                                                                                                                                                                                                                                 
│             │                                                                                                                                                                                                                                 
│             │                                                                                                                                                                                                                                 
└──────┬──────┘                                                                                                                                                                                                                                 
┌──────┴──────┐                                                                                                                                                                                                                                 
│CROSS_PRODUCT│                                                                                                                                                                                                                                 
│             │                                                                                                                                                                                                                                 
│             │                                                                                                                                                                                                                                 
│             │                                                                                                                                                                                                                                 
│             │                                                                                                                                                                                                                                 
│             ├─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
│             │                                                                                                                                                                                                                                 
│             │                                                                                                                                                                                                                                 
│             │                                                                                                                                                                                                                                 
│             │                                                                                                                                                                                                                                 
└──────┬──────┘                                                                                                                                                                                                                                 
┌──────┴──────┐                                                                                                                                                                                                                                 
│CROSS_PRODUCT│                                                                                                                                                                                                                                 
│             │                                                                                                                                                                                                                                 
│             │                                                                                                                                                                                                                                 
│             │                                                                                                                                                                                                                                 
│             │                                                                                                                                                                                                                                 
│             ├────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐                                                    
│             │                                                                                                                                                                            │                                                    
│             │                                                                                                                                                                            │                                                    
│             │                                                                                                                                                                            │                                                    
│             │                                                                                                                                                                            │                                                    
│             │                                                                                                                                                                            │                                                    
└──────┬──────┘                                                                                                                                                                            │                                                    
┌──────┴──────┐                                                                                                                                                                     ┌──────┴──────┐                                             
│CROSS_PRODUCT│                                                                                                                                                                     │UNGROUPE...  │                                             
│             │                                                                                                                                                                     │    ──────   │                                             
│             │                                                                                                                                                                     │ Aggregates: │                                             
│             │                                                                                                                                                                     │ count_star()│                                             
│             │                                                                                                                                                                     │             │                                             
│             │                                                                                                                                                                     │             │                                             
│             │                                                                                                                                                                     │             │                                             
│             │                                                                                                                                                                     │             │                                             
│             ├────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐                                                    │             │                                             
│             │                                                                                                                │                                                    │             │                                             
│             │                                                                                                                │                                                    │             │                                             
│             │                                                                                                                │                                                    │             │                                             
│             │                                                                                                                │                                                    │             │                                             
│             │                                                                                                                │                                                    │             │                                             
│             │                                                                                                                │                                                    │             │                                             
│             │                                                                                                                │                                                    │             │                                             
│             │                                                                                                                │                                                    │             │                                             
└──────┬──────┘                                                                                                                │                                                    └──────┬──────┘                                             
┌──────┴──────┐                                                                                                         ┌──────┴──────┐                                             ┌──────┴──────┐                                             
│CROSS_PRODUCT│                                                                                                         │UNGROUPE...  │                                             │  HASH_JOIN  │                                             
│             │                                                                                                         │    ──────   │                                             │    ──────   │                                             
│             │                                                                                                         │ Aggregates: │                                             │  Join Type: │                                             
│             │                                                                                                         │ count_star()│                                             │    INNER    │                                             
│             │                                                                                                         │             │                                             │             │                                             
│             │                                                                                                         │             │                                             │ Conditions: │                                             
│             │                                                                                                         │             │                                             │ ss_hdemo_sk │                                             
│             │                                                                                                         │             │                                             │ = hd_demo_sk│                                             
│             ├────────────────────────────────────────────────────┐                                                    │             │                                             │             ├─────────────────────────────────────┐       
│             │                                                    │                                                    │             │                                             │             │                                     │       
│             │                                                    │                                                    │             │                                             │             │                                     │       
│             │                                                    │                                                    │             │                                             │             │                                     │       
│             │                                                    │                                                    │             │                                             │             │                                     │       
│             │                                                    │                                                    │             │                                             │             │                                     │       
│             │                                                    │                                                    │             │                                             │             │                                     │       
│             │                                                    │                                                    │             │                                             │             │                                     │       
│             │                                                    │                                                    │             │                                             │ ~30,905 rows│                                     │       
└──────┬──────┘                                                    │                                                    └──────┬──────┘                                             └──────┬──────┘                                     │       
┌──────┴──────┐                                             ┌──────┴──────┐                                             ┌──────┴──────┐                                             ┌──────┴──────┐                              ┌──────┴──────┐
│UNGROUPE...  │                                             │UNGROUPE...  │                                             │  HASH_JOIN  │                                             │  HASH_JOIN  │                              │  PROJECTION │
│    ──────   │                                             │    ──────   │                                             │    ──────   │                                             │    ──────   │                              │    ──────   │
│ Aggregates: │                                             │ Aggregates: │                                             │  Join Type: │                                             │  Join Type: │                              │      #0     │
│ count_star()│                                             │ count_star()│                                             │    INNER    │                                             │    INNER    │                              │             │
│             │                                             │             │                                             │             │                                             │             │                              │             │
│             │                                             │             │                                             │ Conditions: │                                             │ Conditions: │                              │             │
│             │                                             │             │                                             │ ss_hdemo_sk │                                             │ ss_store_sk │                              │             │
│             │                                             │             │                                             │ = hd_demo_sk│                                             │ = s_store_sk│                              │             │
│             │                                             │             │                                             │             ├─────────────────────────────────────┐       │             ├──────────────────────┐       │             │
│             │                                             │             │                                             │             │                                     │       │             │                      │       │             │
│             │                                             │             │                                             │             │                                     │       │             │                      │       │             │
│             │                                             │             │                                             │             │                                     │       │             │                      │       │             │
│             │                                             │             │                                             │             │                                     │       │             │                      │       │             │
│             │                                             │             │                                             │             │                                     │       │             │                      │       │             │
│             │                                             │             │                                             │             │                                     │       │             │                      │       │             │
│             │                                             │             │                                             │             │                                     │       │             │                      │       │             │
│             │                                             │             │                                             │ ~30,905 rows│                                     │       │~154,525 rows│                      │       │ ~1,440 rows │
└──────┬──────┘                                             └──────┬──────┘                                             └──────┬──────┘                                     │       └──────┬──────┘                      │       └──────┬──────┘
┌──────┴──────┐                                             ┌──────┴──────┐                                             ┌──────┴──────┐                              ┌──────┴──────┐┌──────┴──────┐               ┌──────┴──────┐┌──────┴──────┐
│  HASH_JOIN  │                                             │  HASH_JOIN  │                                             │  HASH_JOIN  │                              │  PROJECTION ││  HASH_JOIN  │               │    FILTER   ││    FILTER   │
│    ──────   │                                             │    ──────   │                                             │    ──────   │                              │    ──────   ││    ──────   │               │    ──────   ││    ──────   │
│  Join Type: │                                             │  Join Type: │                                             │  Join Type: │                              │      #0     ││  Join Type: │               │ (s_store_sk ││((hd_dep_coun│
│    INNER    │                                             │    INNER    │                                             │    INNER    │                              │             ││    INNER    │               │   <= 100)   ││  t = 4) OR  │
│             │                                             │             │                                             │             │                              │             ││             │               │             ││(hd_dep_count│
│ Conditions: │                                             │ Conditions: │                                             │ Conditions: │                              │             ││ Conditions: │               │             ││     = 3))   │
│ ss_hdemo_sk │                                             │ ss_hdemo_sk │                                             │ ss_store_sk │                              │             ││ss_sold_time_│               │             ││             │
│ = hd_demo_sk│                                             │ = hd_demo_sk│                                             │ = s_store_sk│                              │             ││sk = t_time_s│               │             ││             │
│             ├─────────────────────────────────────┐       │             ├─────────────────────────────────────┐       │             ├──────────────────────┐       │             ││      k      ├───────┐       │             ││             │
│             │                                     │       │             │                                     │       │             │                      │       │             ││             │       │       │             ││             │
│             │                                     │       │             │                                     │       │             │                      │       │             ││             │       │       │             ││             │
│             │                                     │       │             │                                     │       │             │                      │       │             ││             │       │       │             ││             │
│             │                                     │       │             │                                     │       │             │                      │       │             ││             │       │       │             ││             │
│             │                                     │       │             │                                     │       │             │                      │       │             ││             │       │       │             ││             │
│             │                                     │       │             │                                     │       │             │                      │       │             ││             │       │       │             ││             │
│             │                                     │       │             │                                     │       │             │                      │       │             ││             │       │       │             ││             │
│ ~30,905 rows│                                     │       │ ~30,905 rows│                                     │       │~154,525 rows│                      │       │ ~1,440 rows ││~1,404,7...  │       │       │   ~11 rows  ││ ~1,440 rows │
└──────┬──────┘                                     │       └──────┬──────┘                                     │       └──────┬──────┘                      │       └──────┬──────┘└──────┬──────┘       │       └──────┬──────┘└──────┬──────┘
┌──────┴──────┐                              ┌──────┴──────┐┌──────┴──────┐                              ┌──────┴──────┐┌──────┴──────┐               ┌──────┴──────┐┌──────┴──────┐┌──────┴──────┐┌──────┴──────┐┌──────┴──────┐┌──────┴──────┐
│  HASH_JOIN  │                              │  PROJECTION ││  HASH_JOIN  │                              │  PROJECTION ││  HASH_JOIN  │               │    FILTER   ││    FILTER   ││  SEQ_SCAN   ││    FILTER   ││  SEQ_SCAN   ││  SEQ_SCAN   │
│    ──────   │                              │    ──────   ││    ──────   │                              │    ──────   ││    ──────   │               │    ──────   ││    ──────   ││    ──────   ││    ──────   ││    ──────   ││    ──────   │
│  Join Type: │                              │      #0     ││  Join Type: │                              │      #0     ││  Join Type: │               │ (s_store_sk ││((hd_dep_coun││    Table:   ││  (t_time_sk ││    Table:   ││    Table:   │
│    INNER    │                              │             ││    INNER    │                              │             ││    INNER    │               │   <= 100)   ││  t = 4) OR  ││ store_sales ││    BETWEEN  ││    store    ││household_dem│
│             │                              │             ││             │                              │             ││             │               │             ││(hd_dep_count││             ││   28800 AND ││             ││  ographics  │
│ Conditions: │                              │             ││ Conditions: │                              │             ││ Conditions: │               │             ││     = 3))   ││    Type:    ││    75599)   ││    Type:    ││             │
│ ss_store_sk │                              │             ││ ss_store_sk │                              │             ││ss_sold_time_│               │             ││             ││  Sequential ││             ││  Sequential ││    Type:    │
│ = s_store_sk│                              │             ││ = s_store_sk│                              │             ││sk = t_time_s│               │             ││             ││     Scan    ││             ││     Scan    ││  Sequential │
│             ├──────────────────────┐       │             ││             ├──────────────────────┐       │             ││      k      ├───────┐       │             ││             ││             ││             ││             ││     Scan    │
│             │                      │       │             ││             │                      │       │             ││             │       │       │             ││             ││ Projections:││             ││ Projections:││             │
│             │                      │       │             ││             │                      │       │             ││             │       │       │             ││             ││ss_sold_time_││             ││  s_store_sk ││ Projections:│
│             │                      │       │             ││             │                      │       │             ││             │       │       │             ││             ││      sk     ││             ││             ││  hd_demo_sk │
│             │                      │       │             ││             │                      │       │             ││             │       │       │             ││             ││ ss_hdemo_sk ││             ││   Filters:  ││ hd_dep_count│
... (63 more lines truncated)
```

## Query Structure (Logic Tree)

```
QUERY: (single statement)
└── [MAIN] main_query  [=]  Cost: 100%  Rows: ~1K  — Compute eight independent time-slice counts under identical household/store filters and return them in one cross-joined row for side-by-side comparison.
    ├── SCAN (store_sales, household_demographics, time_dim, store)
    ├── JOIN (ss_sold_time_sk = time_dim.t_time_sk)
    ├── JOIN (ss_hdemo_sk = household_demographics.hd_demo_sk)
    ├── JOIN (+1 more)
    ├── FILTER (time_dim.t_hour = 8)
    ├── FILTER (time_dim.t_minute >= 30)
    ├── FILTER (+1 more)
    └── OUTPUT (*)
```

## Node Details

### 1. main_query
**Role**: Root / Output (Definition Order: 0)
**Intent**: Compute eight independent time-slice counts under identical household/store filters and return them in one cross-joined row for side-by-side comparison.
**Stats**: 100% Cost | ~1k rows
**Outputs**: [*]
**Dependencies**: store_sales, household_demographics, time_dim, store
**Joins**: ss_sold_time_sk = time_dim.t_time_sk | ss_hdemo_sk = household_demographics.hd_demo_sk | ss_store_sk = s_store_sk
**Filters**: time_dim.t_hour = 8 | time_dim.t_minute >= 30 | store.s_store_name = 'ese'
**Operators**: SEQ_SCAN[store_sales], SEQ_SCAN[household_demographics], SEQ_SCAN[time_dim]
**Key Logic (SQL)**:
```sql
SELECT
  *
FROM (
  SELECT
    COUNT(*) AS h8_30_to_9
  FROM store_sales, household_demographics, time_dim, store
  WHERE
    ss_sold_time_sk = time_dim.t_time_sk
    AND ss_hdemo_sk = household_demographics.hd_demo_sk
    AND ss_store_sk = s_store_sk
    AND time_dim.t_hour = 8
    AND time_dim.t_minute >= 30
    AND (
      (
        household_demographics.hd_dep_count = -1
        AND household_demographics.hd_vehicle_count <= -1 + 2
      )
      OR (
        household_demographics.hd_dep_count = 4
        AND household_demographics.hd_vehicle_count <= 4 + 2
...
```


## Aggregation Semantics Check

- **STDDEV_SAMP(x)** requires >=2 non-NULL values per group. Changing group membership changes the result.
- **AVG and STDDEV are NOT duplicate-safe**: join-introduced row duplication changes the aggregate.
- When splitting with GROUP BY + aggregate, each branch must preserve exact GROUP BY columns and filter to the same row set.

## Engine Profile

*This is field intelligence gathered from 88 TPC-DS queries at SF1-SF10. Use it to guide your analysis but apply your own judgment — every query is different. Add to this knowledge if you observe something new.*

### Optimizer Strengths (DO NOT fight these)
- **INTRA_SCAN_PREDICATE_PUSHDOWN**: Pushes WHERE filters directly into SEQ_SCAN. Single-table predicates are applied at scan time, zero overhead.
- **SAME_COLUMN_OR**: OR on the SAME column (e.g., t_hour BETWEEN 8 AND 11 OR t_hour BETWEEN 16 AND 17) is handled in a single scan with range checks.
- **HASH_JOIN_SELECTION**: Selects hash joins automatically. Join ordering is generally sound for 2-4 table joins.
- **CTE_INLINING**: CTEs referenced once are typically inlined (treated as subquery). Multi-referenced CTEs may be materialized.
- **COLUMNAR_PROJECTION**: Only reads columns actually referenced. Unused columns have zero I/O cost.
- **PARALLEL_AGGREGATION**: Scans and aggregations parallelized across threads. PERFECT_HASH_GROUP_BY is highly efficient.
- **EXISTS_SEMI_JOIN**: EXISTS/NOT EXISTS uses semi-join with early termination — stops after first match per outer row.

### Optimizer Gaps (opportunities)
- **CROSS_CTE_PREDICATE_BLINDNESS**: Cannot push predicates from the outer query backward into CTE definitions.
  Opportunity: Move selective predicates INTO the CTE definition. Pre-filter dimensions/facts before they get materialized.
- **REDUNDANT_SCAN_ELIMINATION**: Cannot detect when the same fact table is scanned N times with similar filters across subquery boundaries.
  Opportunity: Consolidate N subqueries on the same table into 1 scan with CASE WHEN / FILTER() inside aggregates.
- **CORRELATED_SUBQUERY_PARALYSIS**: Cannot automatically decorrelate correlated aggregate subqueries into GROUP BY + JOIN.
  Opportunity: Convert correlated WHERE to CTE with GROUP BY on the correlation column, then JOIN back.
- **CROSS_COLUMN_OR_DECOMPOSITION**: Cannot decompose OR conditions that span DIFFERENT columns into independent targeted scans.
  Opportunity: Split cross-column ORs into UNION ALL branches, each with a targeted single-column filter.
- **LEFT_JOIN_FILTER_ORDER_RIGIDITY**: Cannot reorder LEFT JOINs to apply selective dimension filters before expensive fact table joins, and cannot infer that a WHERE on the right table makes LEFT JOIN equivalent to INNER JOIN.
  Opportunity: Pre-filter the selective dimension into a CTE, or convert LEFT JOIN to INNER JOIN when WHERE eliminates NULL rows from right table.
- **UNION_CTE_SELF_JOIN_DECOMPOSITION**: When a UNION ALL CTE is self-joined N times with each join filtering to a different discriminator, the optimizer materializes the full UNION once and probes it N times, discarding most rows each time.
  Opportunity: Split the UNION ALL into N separate CTEs (one per discriminator value).
- **AGGREGATE_BELOW_JOIN_BLINDNESS**: Cannot push GROUP BY aggregation below joins when the aggregation keys align with join keys.
  Opportunity: Pre-aggregate the fact table by the join key before the dimension join. Rows entering the join drop from millions to the distinct key count.

## Tag-Matched Examples (14)

### channel_bitmap_aggregation (6.24x)
**Description:** Consolidate repeated scans of the same fact table (one per time/channel bucket) into a single scan with CASE WHEN labels and conditional aggregation
**When NOT to apply:** Do not use when the number of distinct buckets exceeds 8 (diminishing returns from CASE evaluation overhead). Also not applicable when each subquery has structurally different joins or table references.

### prefetch_fact_join (3.77x)
**Description:** Pre-filter dimension table into CTE, then pre-join with fact table in second CTE before joining other dimensions
**Principle:** Staged Join Pipeline: build a CTE chain that progressively reduces data — first CTE filters the dimension, second CTE pre-joins filtered dimension keys with the fact table, subsequent CTEs join remaining dimensions against the already-reduced fact set.
**When NOT to apply:** Do not use on queries with baseline runtime under 50ms — CTE materialization overhead dominates on fast queries. Do not use on window-function-dominated queries where filtering is not the bottleneck. Avoid on queries with 5+ table joins and complex inter-table predicates where forcing join order via CTEs prevents the optimizer from choosing a better plan. Caused 0.50x on Q25 (fast baseline query), 0.87x on Q51 (window-function bottleneck), and 0.77x on Q72 (complex multi-table join reordering).

### shared_dimension_multi_channel (1.30x)
**Description:** Extract shared dimension filters (date, item, promotion) into CTEs when multiple channel CTEs (store/catalog/web) apply identical filters independently
**Principle:** Shared Dimension Extraction: when multiple channel CTEs (store/catalog/web) apply identical dimension filters, extract those shared filters into one CTE and reference it from each channel. Avoids redundant dimension scans.

### composite_decorrelate_union (2.42x)
**Description:** Decorrelate multiple correlated EXISTS subqueries into pre-materialized DISTINCT customer CTEs with a shared date filter, and replace OR(EXISTS a, EXISTS b) with UNION of key sets
**Principle:** Composite Decorrelation: when multiple correlated EXISTS share common filters, extract shared dimensions into a single CTE and decorrelate the EXISTS checks into pre-materialized key sets joined via UNION.

### materialize_cte (1.37x)
**Description:** Extract repeated subquery patterns into a CTE to avoid recomputation
**Principle:** Shared Materialization: extract repeated subquery patterns into CTEs to avoid recomputation. When the same logical check appears multiple times, compute it once and reference the result.
**When NOT to apply:** NEVER convert EXISTS or NOT EXISTS subqueries into materialized CTEs when the EXISTS is used as a filter (not a data source). EXISTS uses semi-join short-circuiting — the database stops scanning as soon as one match is found. Materializing into a CTE forces a full scan of the subquery table, destroying this optimization. Caused 0.14x on Q16 (7x slowdown — EXISTS on catalog_sales materialized into full CTE scan) and 0.54x on Q95 (EXISTS on web_sales forced full materialization).

### date_cte_isolate (4.00x)
**Description:** Extract date filtering into a separate CTE to enable predicate pushdown and reduce scans
**Principle:** Dimension Isolation: extract small dimension lookups into CTEs so they materialize once and subsequent joins probe a tiny hash table instead of rescanning.
**When NOT to apply:** Do not use when the optimizer already pushes date predicates effectively (e.g., simple equality filters on date columns in self-joins). Do not decompose an already-efficient existing CTE into sub-CTEs — this adds materialization overhead without reducing scans. Caused 0.49x regression on Q31 (DuckDB already optimized the date pushdown) and 0.71x on Q1 (decomposed a well-structured CTE into slower pieces).

### multi_date_range_cte (2.35x)
**Description:** When query uses multiple date_dim aliases with different filters (d1, d2, d3), create separate CTEs for each date range and pre-join with fact tables
**Principle:** Early Selection per Alias: when a query joins the same dimension table multiple times with different filters (d1, d2, d3), create separate CTEs for each filter and pre-join with fact tables to reduce rows entering the main join.

### pushdown (2.11x)
**Description:** Push filters from outer query into CTEs/subqueries to reduce intermediate result sizes
**Principle:** Scan Consolidation: when multiple subqueries scan the same table with similar patterns, consolidate them into CTEs that compute all needed aggregates in fewer passes. Reduces N scans to fewer scans.

### self_join_decomposition (4.76x)
**Description:** Split self-joined CTE with different filter values into separate per-filter CTEs to avoid full materialization and post-filtering
**Principle:** When a CTE is self-joined with different filter values (e.g., inv1.d_moy=1 AND inv2.d_moy=2), split into separate CTEs each embedding their filter. The optimizer cannot push the outer WHERE filter into the CTE's GROUP BY, causing full materialization and post-filtering.
**When NOT to apply:** Only applies when the CTE is self-joined with different discriminator values. If the self-join uses the same filter on both aliases, decomposition provides no benefit.

### single_pass_aggregation (4.47x)
**Description:** Consolidate multiple subqueries scanning the same table into a single CTE with conditional aggregates
**Principle:** Single-Pass Aggregation: consolidate multiple scalar subqueries on the same table into one CTE using CASE expressions inside aggregate functions. Reduces N separate table scans to 1 pass.

### decorrelate (2.92x)
**Description:** Convert correlated subquery to separate CTE with GROUP BY, then JOIN
**Principle:** Decorrelation: convert correlated subqueries to standalone CTEs with GROUP BY, then JOIN. Correlated subqueries re-execute per outer row; a pre-computed CTE executes once.

### intersect_to_exists (1.83x)
**Description:** Convert INTERSECT subquery pattern to multiple EXISTS clauses for better join planning
**Principle:** Semi-Join Short-Circuit: replace INTERSECT with EXISTS to avoid full materialization and sorting. INTERSECT must compute complete result sets before intersecting; EXISTS stops at the first match per row, enabling semi-join optimizations.

### multi_intersect_exists_cte (2.39x)
**Description:** Convert cascading INTERSECT operations into correlated EXISTS subqueries with pre-materialized date and channel CTEs
**When NOT to apply:** Do not use when the INTERSECT operates on small result sets (< 1000 rows) where materialization cost is negligible. Also not applicable when the EXISTS correlation would be on non-indexed columns, as the correlated probe could be slower than the hash-based INTERSECT.

### early_filter (4.00x)
**Description:** Filter dimension tables FIRST, then join to fact tables to reduce expensive joins
**Principle:** Early Selection: filter small dimension tables first, then join to large fact tables. This reduces the fact table scan to only rows matching the filter, rather than scanning all rows and filtering after the join.

## Regression Warnings

### regression_q90_materialize_cte: materialize_cte on q90 (0.59x)
**Anti-pattern:** Never convert OR conditions on the SAME column (e.g., range conditions on t_hour) into UNION ALL. The optimizer already handles same-column ORs as a single scan. UNION ALL only helps when branches access fundamentally different tables or columns.
**Mechanism:** Split a simple OR condition (t_hour BETWEEN 10 AND 11 OR t_hour BETWEEN 16 AND 17) into UNION ALL of two separate web_sales scans. This doubles the fact table scan. DuckDB handles same-column OR ranges efficiently in a single scan — the UNION ALL adds materialization overhead with zero selectivity benefit.

### regression_q95_semantic_rewrite: semantic_rewrite on q95 (0.54x)
**Anti-pattern:** Do not decompose tightly-correlated EXISTS/NOT EXISTS clause pairs into independent CTEs. Their cardinality estimates depend on each other; materializing them independently causes severe join cost misestimates.
**Mechanism:** Materialized complex correlated EXISTS/NOT EXISTS logic into independent CTEs (multi_warehouse_orders, returned_multi_warehouse_orders). The original EXISTS clauses were tightly correlated and evaluated together. Decoupling them into CTEs severed their cardinality relationship, causing 4x+ join estimate errors.

### regression_q25_date_cte_isolate: date_cte_isolate on q25 (0.5x)
**Anti-pattern:** Do not pre-filter and materialize fact tables when the query has 3+ fact table joins. The optimizer needs freedom to reorder multi-way fact joins and push filters across them.
**Mechanism:** Pre-filtered and joined store_sales to date CTE BEFORE the 3-way fact table join (store_sales <- store_returns <- catalog_sales). By materializing the date-filtered store_sales early, the optimizer loses the ability to push filters across the multi-way fact join and reorder joins optimally.

## Correctness Constraints (4 — NEVER violate)

**[CRITICAL] COMPLETE_OUTPUT**: The rewritten query must output ALL columns from the original SELECT. Never drop, rename, or reorder output columns. Every column alias must be preserved exactly as in the original.

**[CRITICAL] CTE_COLUMN_COMPLETENESS**: CRITICAL: When creating or modifying a CTE, its SELECT list MUST include ALL columns referenced by downstream queries. Check the Node Contracts section: every column in downstream_refs MUST appear in the CTE output. Also ensure: (1) JOIN columns used by consumers are included in SELECT, (2) every table referenced in WHERE is present in FROM/JOIN, (3) no ambiguous column names between the CTE and re-joined tables. Dropping a column that a downstream node needs will cause an execution error.
  - Failure: Q21 — prefetched_inventory CTE omits i_item_id but main query references it in SELECT and GROUP BY
  - Failure: Q76 — filtered_store_dates CTE omits d_year and d_qoy but aggregation CTE uses them in GROUP BY

**[CRITICAL] LITERAL_PRESERVATION**: CRITICAL: When rewriting SQL, you MUST copy ALL literal values (strings, numbers, dates) EXACTLY from the original query. Do NOT invent, substitute, or 'improve' any filter values. If the original says d_year = 2000, your rewrite MUST say d_year = 2000. If the original says ca_state = 'GA', your rewrite MUST say ca_state = 'GA'. Changing these values will produce WRONG RESULTS and the rewrite will be REJECTED.

**[CRITICAL] SEMANTIC_EQUIVALENCE**: The rewritten query MUST return exactly the same rows, columns, and ordering as the original. This is the prime directive. Any rewrite that changes the result set — even by one row, one column, or a different sort order — is WRONG and will be REJECTED.

## Your Task

Work through these 3 steps in a `<reasoning>` block, then output the structured briefing below:

1. **DIAGNOSE**: Why did the best worker achieve 5.269233093530888x instead of the 2.0x target? Why did each other worker fail or regress? Be specific about structural mechanisms.
2. **IDENTIFY**: What optimization angles couldn't have been designed BEFORE seeing these empirical results? What did the results reveal about the query's actual execution behavior?
3. **SYNTHESIZE**: Design a strategy for the sniper that builds on the best foundation (if any) and exploits the newly-revealed angles. The sniper has full freedom — give it direction, not constraints.

### Output Format (follow EXACTLY)

```
=== SNIPE BRIEFING ===

FAILURE_SYNTHESIS:
<WHY the best worker won, WHY each other failed — structural mechanisms>

BEST_FOUNDATION:
<What to build on from the best result, or 'None — start fresh' if all regressed>

UNEXPLORED_ANGLES:
<What optimization approaches couldn't have been designed pre-empirically>

STRATEGY_GUIDANCE:
<Synthesized approach for the sniper — ADVISORY, not mandatory>

EXAMPLES: <ex1>, <ex2>, <ex3>

EXAMPLE_ADAPTATION:
<For each example: what to APPLY and what to IGNORE>

HAZARD_FLAGS:
<Risks based on observed failures — what NOT to do>

RETRY_WORTHINESS: high|low — <reason>
(Is there genuine headroom for a second sniper attempt if the first misses 2.0x?)

RETRY_DIGEST:
<5-10 line compact failure guide for sniper2 IF retry is needed.
What broke, why, what to change. The lesson, not the artifact.>
```
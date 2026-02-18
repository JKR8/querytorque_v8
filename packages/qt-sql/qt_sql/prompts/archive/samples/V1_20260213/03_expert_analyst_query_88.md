## §1. ROLE & MISSION

You are a senior query optimization architect. Your job is to deeply analyze a SQL query and produce a structured briefing for a single specialist worker who will write the best possible optimized version.

You are the ONLY call that sees all the data: EXPLAIN plans, logical-tree costs, full constraint list, global knowledge, and the complete example catalog. The worker will only see what YOU put in the briefing. Your output quality directly determines success.

## §2a. Original Query: query_88 (duckdb v1.4.3)

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

## §2b. EXPLAIN ANALYZE Plan

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

**NOTE:** EXPLAIN shows PHYSICAL execution — ground truth when it disagrees with the logical tree (optimizer may already split CTEs, push predicates, reorder joins).
DuckDB times are **operator-exclusive** (children excluded). Sum siblings for pipeline cost. Use EXPLAIN timings, not logical-tree %.

### §2b-i. Cardinality Estimation Routing (Q-Error)

Direction: OVER_EST (estimated >> actual — planner over-provisions, redundant work likely)
Locus: PROJECTION — worst mismatch at SEQ_SCAN  (est=28.8M, act=419K)

Pathology routing: P7, P0, P4
(Locus+Direction routing is 85% accurate at predicting where the winning transform operates)

Structural signals:
  - EST_ONE_NONLEAF: planner guessing → likely decorrelation needed (P2, P0)

## §2c. Query Structure (Logic Tree)

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

### Node Details

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


## §2d. Pre-Computed Semantic Intent

**Query intent:** Count store sales by consecutive half-hour windows from 8:30 to 12:30 at store "ese" for households matching specific dependent/vehicle constraints.

START from this pre-computed intent. In your SEMANTIC_CONTRACT output, ENRICH it with: intersection/union semantics from JOIN types, aggregation function traps, NULL propagation paths, and filter dependencies. Do NOT re-derive what is already stated above.

## §3a. Correctness Constraints (4 — NEVER violate)

**[CRITICAL] COMPLETE_OUTPUT**: The rewritten query must output ALL columns from the original SELECT. Never drop, rename, or reorder output columns. Every column alias must be preserved exactly as in the original.

**[CRITICAL] CTE_COLUMN_COMPLETENESS**: CRITICAL: When creating or modifying a CTE, its SELECT list MUST include ALL columns referenced by downstream queries. Check the Node Contracts section: every column in downstream_refs MUST appear in the CTE output. Also ensure: (1) JOIN columns used by consumers are included in SELECT, (2) every table referenced in WHERE is present in FROM/JOIN, (3) no ambiguous column names between the CTE and re-joined tables. Dropping a column that a downstream node needs will cause an execution error.
  - Failure: Q21 — prefetched_inventory CTE omits i_item_id but main query references it in SELECT and GROUP BY
  - Failure: Q76 — filtered_store_dates CTE omits d_year and d_qoy but aggregation CTE uses them in GROUP BY

**[CRITICAL] LITERAL_PRESERVATION**: CRITICAL: When rewriting SQL, you MUST copy ALL literal values (strings, numbers, dates) EXACTLY from the original query. Do NOT invent, substitute, or 'improve' any filter values. If the original says d_year = 2000, your rewrite MUST say d_year = 2000. If the original says ca_state = 'GA', your rewrite MUST say ca_state = 'GA'. Changing these values will produce WRONG RESULTS and the rewrite will be REJECTED.

**[CRITICAL] SEMANTIC_EQUIVALENCE**: The rewritten query MUST return exactly the same rows, columns, and ordering as the original. This is the prime directive. Any rewrite that changes the result set — even by one row, one column, or a different sort order — is WRONG and will be REJECTED.

## §3b. Aggregation Equivalence Rules

You MUST verify aggregation equivalence for any proposed restructuring:

- **STDDEV_SAMP(x)** requires >=2 non-NULL values per group. Returns NULL for 0-1 values. Changing group membership changes the result.
- `STDDEV_SAMP(x) FILTER (WHERE year=1999)` over a combined (1999,2000) group is NOT equivalent to `STDDEV_SAMP(x)` over only 1999 rows — FILTER still uses the combined group's membership for the stddev denominator.
- **AVG and STDDEV are NOT duplicate-safe**: if a join introduces row duplication, the aggregate result changes.
- When splitting a UNION ALL CTE with GROUP BY + aggregate, each split branch must preserve the exact GROUP BY columns and filter to the exact same row set as the original.
- **SAFE ALTERNATIVE**: If GROUP BY includes the discriminator column (e.g., d_year), each group is already partitioned. STDDEV_SAMP computed per-group is correct. You can then pivot using `MAX(CASE WHEN year = 1999 THEN year_total END) AS year_total_1999` because the GROUP BY guarantees exactly one row per (customer, year) — the MAX is just a row selector, not a real aggregation.

## §4. Exploit Algorithm: Evidence-Based Gap Intelligence

The following describes known optimizer gaps with detection rules, procedural exploit steps, and evidence. Use DETECT rules to match structural features of the query, then follow EXPLOIT_STEPS.

# DuckDB Rewrite Playbook
# 22 gold wins + 10 regressions | TPC-DS SF1–SF10

## HOW TO USE THIS DOCUMENT

Work in phase order. Each phase changes the plan shape — re-evaluate later phases after each.

  Phase 1: Reduce scan volume (P0) — always first. Every other optimization benefits from smaller input.
  Phase 2: Eliminate redundant work (P1, P3)
  Phase 3: Fix structural inefficiencies (P2, P4–P9)

## EXPLAIN ANALYSIS PROCEDURE

Before choosing any strategy, execute this procedure on the EXPLAIN plan:

1. IDENTIFY THE COST SPINE — which sequence of nodes accounts for >70% of total cost?
   The spine is your optimization target. Everything else is noise.
2. CLASSIFY EACH SPINE NODE:
   - SEQ_SCAN: how many rows? Is there a filter? Is the filter selective (>5:1)?
   - HASH_JOIN: what's the build side cardinality? Is it the smaller table?
   - AGGREGATE: input rows vs output rows ratio? >10:1 = pushdown candidate.
   - NESTED_LOOP: ALWAYS suspicious — check if decorrelation is possible.
   - WINDOW: is it computed before or after a join? Could it be deferred?
3. TRACE DATA FLOW: row counts should decrease monotonically through the plan.
   Where do they stay flat or increase? That transition point is the bottleneck.
4. CHECK THE SYMPTOM ROUTING TABLE: match your observations to primary hypotheses.
5. FORM BOTTLENECK HYPOTHESIS: "The optimizer is doing X, but Y would be better
   because Z." This hypothesis drives both pathology matching AND novel reasoning.

## SYMPTOM ROUTING — from EXPLAIN to hypothesis

Two routing paths exist and should agree. If they disagree, trust Q-Error (quantitative).

### Path A: Q-Error routing (quantitative — from §2b-i when available)

Q-Error = max(estimated/actual, actual/estimated) per operator.
The operator with the highest Q-Error is where the planner's worst decision lives.

| Q-Error Locus | Direction  | Primary hypothesis     | Why                                      |
|---------------|------------|------------------------|------------------------------------------|
| JOIN          | UNDER_EST  | P2 (decorrelate)       | Planner thinks join is cheap, it's not    |
| JOIN          | ZERO_EST   | P0, P2                 | Planner has no join estimate at all       |
| JOIN          | OVER_EST   | P5 (LEFT→INNER)        | Planner over-provisions for NULLs         |
| SCAN          | OVER_EST   | P1, P4                 | Redundant scans or missed pruning         |
| SCAN          | ZERO_EST   | P2                     | DELIM_SCAN = correlated subquery          |
| AGGREGATE     | OVER_EST   | P3 (agg below join)    | Fan-out before GROUP BY                   |
| CTE           | ZERO_EST   | P0, P7                 | Planner blind to CTE statistics           |
| CTE           | UNDER_EST  | P2, P0                 | CTE output larger than expected           |
| PROJECTION    | OVER_EST   | P7, P0, P4             | Redundant computation                     |
| PROJECTION    | UNDER_EST  | P6, P5, P0             | Set operation or join underestimate       |
| FILTER        | OVER_EST   | P9, P0                 | Shared expression or missed pushdown      |

Structural flags (free, no execution needed):
- DELIM_SCAN → P2 (correlated subquery the optimizer couldn't decorrelate)
- EST_ZERO → P0/P7 (planner gave up — CTE boundary blocks stats)
- EST_ONE_NONLEAF → P2/P0 (planner guessing on non-leaf node)
- REPEATED_TABLE → P1 (single-pass consolidation opportunity)

### Path B: Structural routing (qualitative — from EXPLAIN tree inspection)

| EXPLAIN symptom                          | Primary hypothesis   | Verify           |
|------------------------------------------|---------------------|------------------|
| Row counts flat through CTEs, late drop  | P0 (predicate push) | Filter ratio, chain depth |
| Same table scanned N times               | P1 (repeated scans) | Join structure identical? |
| Nested loop with inner aggregate         | P2 (correlated sub)  | Already hash join? |
| Aggregate input >> output after join     | P3 (agg below join)  | Key alignment    |
| Full scan, OR across DIFFERENT columns   | P4 (cross-col OR)    | Same column? → STOP |
| LEFT JOIN + WHERE on right column        | P5 (LEFT→INNER)      | COALESCE check   |
| INTERSECT node, large inputs             | P6 (INTERSECT)       | Row count >1K?   |
| CTE self-joined with discriminators      | P7 (self-join CTE)   | 2-4 values?      |
| Window in CTE before join                | P8 (deferred window) | LAG/LEAD check   |
| Identical subtrees in different branches | P9 (shared expr)     | EXISTS check     |
| None of the above                        | FIRST-PRINCIPLES     | See NO MATCH     |

## ENGINE STRENGTHS — do NOT rewrite

1. **Predicate pushdown**: filter inside scan node → leave it.
2. **Same-column OR**: handled natively in one scan. Splitting = lethal (0.23x Q13).
3. **Hash join selection**: sound for 2–4 tables. Reduce inputs, not order.
4. **CTE inlining**: single-ref CTEs inlined automatically (zero overhead).
5. **Columnar projection**: only referenced columns read.
6. **Parallel aggregation**: scans and aggregations parallelized across threads.
7. **EXISTS semi-join**: early termination. **Never materialize** (0.14x Q16).

## CORRECTNESS RULES

- Identical rows, columns, ordering as original.
- Copy ALL literals exactly (strings, numbers, dates).
- Every CTE must SELECT all columns referenced downstream.
- Never drop, rename, or reorder output columns.

## GLOBAL GUARDS

1. EXISTS/NOT EXISTS → never materialize (0.14x Q16, 0.54x Q95)
2. Same-column OR → never split to UNION (0.23x Q13, 0.59x Q90)
3. Baseline < 100ms → skip CTE-based rewrites (overhead exceeds savings)
4. 3+ fact table joins → do not pre-materialize facts (locks join order)
5. Every CTE MUST have a WHERE clause (0.85x Q67)
6. No orphaned CTEs — remove original after splitting (0.49x Q31, 0.68x Q74)
7. No cross-joining 3+ dimension CTEs (0.0076x Q80 — Cartesian product)
8. Max 2 cascading fact-table CTE chains (0.78x Q4)
9. Convert comma joins to explicit JOIN...ON
10. NOT EXISTS → NOT IN breaks with NULLs — preserve EXISTS form

---

## PATHOLOGIES

### P0: Predicate chain pushback [Phase 1 — always first, ~35% of wins]

  Gap: CROSS_CTE_PREDICATE_BLINDNESS — DuckDB plans each CTE independently.
  Predicates in the outer query or later CTEs cannot propagate backward into
  earlier CTE definitions. The CTE materializes blind to how its output will
  be consumed.

  This is the general case. date_cte_isolate, early_filter, prefetch_fact_join,
  multi_dimension_prefetch are all specific instances where the pushed predicate
  is a dimension filter. The principle applies to ANY selective predicate:
  dimension filters, HAVING thresholds, JOIN conditions, subquery results.
  The rule: find the most selective predicate, find the earliest CTE where it
  CAN apply, put it there.

  Signal: row counts stay flat through CTE chain stages then drop sharply at a
  late filter. Target state: monotonically decreasing rows through the chain.

  Decision gates:
  - Structural: 2+ stage CTE chain + late predicate with columns available earlier
  - Cardinality: filter ratio >5:1 strong, 2:1–5:1 moderate if baseline >200ms, <2:1 skip
  - Multi-fact: 1 fact = safe, 2 = careful, 3+ = STOP (0.50x Q25)
  - ROLLUP/WINDOW downstream: CAUTION (0.85x Q67)
  - CTE already filtered on this predicate: skip (0.71x Q1)

  Transform selection (lightest sufficient):
  - Single dim, ≤2 stages → date_cte_isolate (12 wins, 1.34x avg)
  - Single dim, ≥3 stages → prefetch_fact_join (4 wins, 1.89x avg)
  - Multiple dims → multi_dimension_prefetch (3 wins, 1.55x avg)
  - date_dim 3+ aliases → multi_date_range_cte (3 wins, 1.42x avg)
  - Multi-channel shared dims → shared_dimension_multi_channel (1 win, 1.40x)
  - CTE self-join with literal discriminators → self_join_decomposition (1 win, 4.76x)

  Ordering: push most selective predicate first. Selectivity compounds —
  once the first filter reduces 7M to 50K, everything downstream operates on 50K.
  Composition: often combines with aggregate_pushdown (P3) or decorrelation (P2).
  After applying: re-evaluate P1 (scans may now be small enough to skip),
  P2 (outer set may be small enough that nested loop is fine),
  P3 (pre-agg on smaller set may now be more valuable).

  Wins: Q6 4.00x, Q11 4.00x, Q39 4.76x, Q63 3.77x, Q93 2.97x, Q43 2.71x, Q29 2.35x, Q26 1.93x
  Regressions: Q80 0.0076x (dim cross-join), Q25 0.50x (3-fact), Q67 0.85x (ROLLUP), Q1 0.71x (over-decomposed)

### P1: Repeated scans of same table [Phase 2 — ZERO REGRESSIONS]

  Gap: REDUNDANT_SCAN_ELIMINATION — the optimizer cannot detect that N subqueries
  all scan the same table with the same joins. Each subquery is an independent plan
  unit with no Common Subexpression Elimination across boundaries.

  Signal: N separate SEQ_SCAN nodes on same table, identical joins, different bucket filters.
  Decision: consolidate to single scan with CASE WHEN / FILTER (WHERE ...).
  Gates: identical join structure across all subqueries, max 8 branches,
  COUNT/SUM/AVG/MIN/MAX only (not STDDEV/VARIANCE/PERCENTILE).

  Transforms: single_pass_aggregation (8 wins, 1.88x avg), channel_bitmap_aggregation (1 win, 6.24x)
  Wins: Q88 6.24x, Q9 4.47x, Q61 2.27x, Q32 1.61x, Q4 1.53x, Q90 1.47x

### P2: Correlated subquery nested loop [Phase 3]

  Gap: CORRELATED_SUBQUERY_PARALYSIS — the optimizer cannot decorrelate correlated
  aggregate subqueries into GROUP BY + hash join. It falls back to nested-loop
  re-execution instead of recognizing the equivalence.

  Signal: nested loop, inner re-executes aggregate per outer row.
  If EXPLAIN shows hash join on correlation key → already decorrelated → STOP.
  Decision: extract correlated aggregate into CTE with GROUP BY on correlation key, JOIN back.
  Gates: NEVER decorrelate EXISTS (0.34x Q93, 0.14x Q16), preserve ALL WHERE filters,
  check if Phase 1 reduced outer to <1000 rows (nested loop may be fast enough).

  Transforms: decorrelate (3 wins, 2.45x avg), composite_decorrelate_union (1 win, 2.42x)
  Wins: Q1 2.92x, Q35 2.42x
  Regressions: Q93 0.34x (semi-join destroyed), Q1 variant 0.71x (already decorrelated)

### P3: Aggregation after join — fan-out before GROUP BY [Phase 2 — ZERO REGRESSIONS]

  Gap: AGGREGATE_BELOW_JOIN_BLINDNESS — the optimizer cannot push GROUP BY below
  joins even when aggregation keys align with join keys. It always joins first
  (producing M rows), then aggregates (reducing to K groups, K << M).

  Signal: GROUP BY input rows >> distinct keys, aggregate node sits after join.
  Decision: pre-aggregate fact by join key BEFORE dimension join.
  Gates: GROUP BY keys ⊇ join keys (CORRECTNESS — wrong results if violated),
  reconstruct AVG from SUM/COUNT when pre-aggregating for ROLLUP.

  Transforms: aggregate_pushdown, star_join_prefetch
  Wins: Q22 42.90x (biggest win), Q65 1.80x, Q72 1.27x

### P4: Cross-column OR forcing full scan [Phase 3 — HIGHEST VARIANCE]

  Gap: CROSS_COLUMN_OR_DECOMPOSITION — the optimizer handles same-column OR
  efficiently (single scan range) but OR across different columns forces a full
  scan evaluating all conditions for every row.

  Signal: single scan, OR across DIFFERENT columns, 70%+ rows discarded.
  CRITICAL: same column in all OR arms → STOP (engine handles natively).
  Decision: split into UNION ALL branches + shared dim CTE.
  Gates: max 3 branches, cross-column only, no self-join, no nested OR (multiplicative expansion).

  Transforms: or_to_union
  Wins: Q15 3.17x, Q88 6.28x, Q10 1.49x, Q45 1.35x
  Regressions: Q13 0.23x (9 branches), Q48 0.41x (nested OR), Q90 0.59x (same-col), Q23 0.51x (self-join)

### P5: LEFT JOIN + NULL-eliminating WHERE [Phase 3 — ZERO REGRESSIONS]

  Gap: LEFT_JOIN_FILTER_ORDER_RIGIDITY — the optimizer cannot infer that WHERE on
  a right-table column makes LEFT JOIN semantically equivalent to INNER. LEFT JOIN
  also blocks join reordering (not commutative).

  Signal: LEFT JOIN + WHERE on right-table column (proves right non-null).
  Decision: convert to INNER JOIN, optionally pre-filter right table into CTE.
  Gate: no CASE WHEN IS NULL / COALESCE on right-table column.

  Transforms: inner_join_conversion
  Wins: Q93 3.44x, Q80 1.89x

### P6: INTERSECT materializing both sides [Phase 3 — ZERO REGRESSIONS]

  Gap: INTERSECT is implemented as set materialization + comparison. The optimizer
  doesn't recognize that EXISTS semi-join is algebraically equivalent and can
  short-circuit at first match per row.

  Signal: INTERSECT between 10K+ row result sets.
  Decision: replace with EXISTS semi-join.
  Gate: both sides >1K rows.
  Related: semi_join_exists — replace full JOIN with EXISTS when joined columns not in output (1.67x).

  Transforms: intersect_to_exists, multi_intersect_exists_cte
  Wins: Q14 2.72x

### P7: Self-joined CTE materialized for all values [Phase 3]

  Gap: UNION_CTE_SELF_JOIN_DECOMPOSITION + CROSS_CTE_PREDICATE_BLINDNESS — the
  optimizer materializes the CTE once for all values. Self-join discriminator
  filters cannot propagate backward into the CTE definition. Each arm post-filters
  the full materialized result instead of computing only its needed partition.

  Signal: CTE joined to itself with different WHERE per arm (e.g., period=1 vs period=2).
  Decision: split into per-partition CTEs, each embedding its discriminator.
  Gates: 2–4 discriminator values, MUST remove original combined CTE after splitting.

  Transforms: self_join_decomposition (1 win, 4.76x), union_cte_split (2 wins, 1.72x avg),
  rollup_to_union_windowing (1 win, 2.47x)
  Wins: Q39 4.76x, Q36 2.47x, Q74 1.57x
  Regressions: Q31 0.49x (orphaned CTE), Q74 0.68x (orphaned variant)

### P8: Window functions in CTEs before join [Phase 3 — ZERO REGRESSIONS]

  Gap: the optimizer cannot defer window computation past a join when
  partition/ordering is preserved. It computes the window in the CTE because
  that's where the SQL places it.

  Signal: N WINDOW nodes inside CTEs, same ORDER BY key, CTEs then joined.
  Decision: remove windows from CTEs, compute once on joined result.
  Gates: not LAG/LEAD (depends on pre-join row order), not ROWS BETWEEN with specific frame.
  Note: SUM() OVER() naturally skips NULLs — handles FULL OUTER JOIN gaps.

  Transforms: deferred_window_aggregation
  Wins: Q51 1.36x

### P9: Shared subexpression executed multiple times [Phase 3]

  Gap: the optimizer may not CSE identical subqueries across different query branches.
  When it doesn't, cost is N× what single execution would be.
  HARD STOP: EXISTS/NOT EXISTS → NEVER materialize (0.14x Q16). Semi-join
  short-circuit is destroyed by CTE materialization.

  Signal: identical subtrees with identical costs scanning same tables.
  Decision: extract shared subexpression into CTE.
  Gates: NOT EXISTS, subquery is expensive (joins/aggregates), CTE must have WHERE.

  Transforms: materialize_cte
  Wins: Q95 1.43x
  Regressions: Q16 0.14x (EXISTS materialized), Q95 0.54x (cardinality severed)

### NO MATCH — First-Principles Reasoning

If no pathology matches this query, do NOT stop.

1. **Check §2b-i Q-Error routing first.** Even when no pathology gate passes,
   the Q-Error direction+locus still points to where the planner is wrong.
   Use it as a starting hypothesis for novel intervention design.
2. Identify the single largest cost node. What operation dominates? Can it be restructured?
3. Count scans per base table. Repeated scans are always a consolidation opportunity.
4. Trace row counts through the plan. Where do they stay flat or increase?
5. Look for operations the optimizer DIDN'T optimize that it could have:
   - Subqueries not flattened
   - Predicates not pushed through CTE boundaries
   - CTEs re-executed instead of materialized
6. Use the transform catalog (§5a) as a menu. For each transform, check: does the
   EXPLAIN show the optimizer already handles this? If not → candidate.

Record: which pathologies checked, which gates failed, nearest miss, structural
features present. This data seeds pathology discovery for future updates.

---

## SAFETY RANKING

| Rank | Pathology | Regr. | Worst | Action |
|------|-----------|-------|-------|--------|
| 1 | P1: Repeated scans | 0 | — | Always fix |
| 2 | P3: Agg after join | 0 | — | Always fix (verify keys) |
| 3 | P5: LEFT→INNER | 0 | — | Always fix |
| 4 | P6: INTERSECT | 0 | — | Always fix |
| 5 | P8: Pre-join windows | 0 | — | Always fix |
| 6 | P7: Self-join CTE | 1 | 0.49x | Check orphan CTE |
| 7 | P0: Predicate pushback | 4 | 0.0076x | All gates must pass |
| 8 | P2: Correlated loop | 2 | 0.34x | Check EXPLAIN first |
| 9 | P9: Shared expr | 3 | 0.14x | Never on EXISTS |
| 10 | P4: Cross-col OR | 4 | 0.23x | Max 3, cross-column only |

## VERIFICATION CHECKLIST

Before finalizing any rewrite:
- [ ] Row counts decrease monotonically through CTE chain
- [ ] No orphaned CTEs (every CTE referenced downstream)
- [ ] No unfiltered CTEs (every CTE has WHERE)
- [ ] No cross-joined dimension CTEs (each dim joins to fact)
- [ ] EXISTS still uses EXISTS (not materialized)
- [ ] Same-column ORs still intact (not split)
- [ ] All original WHERE filters preserved in CTEs
- [ ] Max 2 cascading fact-table CTE chains
- [ ] Comma joins converted to explicit JOIN...ON
- [ ] Rewrite doesn't match any known regression pattern

## PRUNING GUIDE

Skip pathologies the plan rules out:

| Plan shows | Skip |
|---|---|
| No nested loops | P2 (decorrelation) |
| Each table appears once | P1 (repeated scans) |
| No LEFT JOIN | P5 (INNER conversion) |
| No OR predicates | P4 (OR decomposition) |
| No GROUP BY | P3 (aggregate pushdown) |
| No WINDOW/OVER | P8 (deferred window) |
| No INTERSECT/EXCEPT | P6 (set rewrite) |
| Baseline < 50ms | ALL CTE-based transforms |
| Row counts monotonically decreasing | P0 (predicate pushback) |

## REGRESSION REGISTRY

| Severity | Query | Transform | Result | Root cause |
|----------|-------|-----------|--------|------------|
| CATASTROPHIC | Q80 | dimension_cte_isolate | 0.0076x | Cross-joined 3 dim CTEs: Cartesian product |
| CATASTROPHIC | Q16 | materialize_cte | 0.14x | Materialized EXISTS → semi-join destroyed |
| SEVERE | Q13 | or_to_union | 0.23x | 9 UNION branches from nested OR |
| SEVERE | Q93 | decorrelate | 0.34x | LEFT JOIN was already semi-join |
| MAJOR | Q31 | union_cte_split | 0.49x | Original CTE kept → double materialization |
| MAJOR | Q25 | date_cte_isolate | 0.50x | 3-way fact join locked optimizer order |
| MAJOR | Q23 | or_to_union | 0.51x | Self-join re-executed per branch |
| MAJOR | Q95 | semantic_rewrite | 0.54x | Correlated EXISTS pairs broken |
| MODERATE | Q90 | or_to_union | 0.59x | Split same-column OR |
| MODERATE | Q74 | union_cte_split | 0.68x | Original CTE kept alongside split |
| MODERATE | Q1 | decorrelate | 0.71x | Pre-aggregated ALL stores when only SD needed |
| MODERATE | Q4 | prefetch_fact_join | 0.78x | 3rd cascading CTE chain |
| MINOR | Q72 | multi_dimension_prefetch | 0.77x | Forced suboptimal join order |
| MINOR | Q67 | date_cte_isolate | 0.85x | CTE blocked ROLLUP pushdown |


## §5a. Transform Catalog

Select the best transform (or compound strategy of 2-3 transforms) that maximizes expected speedup for THIS query.

### Predicate Movement
- **global_predicate_pushdown**: Trace selective predicates from late in the CTE chain back to the earliest scan via join equivalences. Biggest win when a dimension filter is applied after a large intermediate materialization.
  Maps to examples: pushdown, early_filter, date_cte_isolate
- **transitive_predicate_propagation**: Infer predicates through join equivalence chains (A.key = B.key AND B.key = 5 -> A.key = 5). Especially across CTE boundaries where optimizers stop propagating.
  Maps to examples: early_filter, dimension_cte_isolate
- **null_rejecting_join_simplification**: When downstream WHERE rejects NULLs from the outer side of a LEFT JOIN, convert to INNER. Enables reordering and predicate pushdown. CHECK: does the query actually have LEFT/OUTER joins before assigning this.
  Maps to examples: (no direct gold example — novel transform)

### Join Restructuring
- **self_join_elimination**: When a UNION ALL CTE is self-joined N times with each join filtering to a different discriminator, split into N pre-partitioned CTEs. Eliminates discriminator filtering and repeated hash probes on rows that don't match.
  Maps to examples: union_cte_split, shared_dimension_multi_channel
- **decorrelation**: Convert correlated EXISTS/IN/scalar subqueries to CTE + JOIN. CHECK: does the query actually have correlated subqueries before assigning this.
  Maps to examples: decorrelate, composite_decorrelate_union
- **aggregate_pushdown**: When GROUP BY follows a multi-table join but aggregation only uses columns from one side, push the GROUP BY below the join. CHECK: verify the join doesn't change row multiplicity for the aggregate (one-to-many breaks AVG/STDDEV).
  Maps to examples: (no direct gold example — novel transform)
- **late_attribute_binding**: When a dimension table is joined only to resolve display columns (names, descriptions) that aren't used in filters, aggregations, or join conditions, defer that join until after all filtering and aggregation is complete. Join on the surrogate key once against the final reduced result set. This eliminates N-1 dimension scans when the CTE references the dimension N times. CHECK: verify the deferred columns aren't used in WHERE, GROUP BY, or JOIN ON — only in the final SELECT.
  Maps to examples: dimension_cte_isolate (partial pattern), early_filter

### Scan Optimization
- **star_join_prefetch**: Pre-filter ALL dimension tables into CTEs, then probe fact table with the combined key intersection.
  Maps to examples: dimension_cte_isolate, multi_dimension_prefetch, prefetch_fact_join, date_cte_isolate
- **single_pass_aggregation**: Merge N subqueries on the same fact table into 1 scan with CASE/FILTER inside aggregates. CHECK: STDDEV_SAMP/VARIANCE are grouping-sensitive — FILTER over a combined group != separate per-group computation.
  Maps to examples: single_pass_aggregation, channel_bitmap_aggregation
- **scan_consolidation_pivot**: When a CTE is self-joined N times with each reference filtering to a different discriminator (e.g., year, channel), consolidate into fewer scans that GROUP BY the discriminator, then pivot rows to columns using MAX(CASE WHEN discriminator = X THEN agg_value END). This halves the fact scans and dimension joins. SAFE when GROUP BY includes the discriminator — each group is naturally partitioned, so aggregates like STDDEV_SAMP are computed correctly per-partition. The pivot MAX is just a row selector (one row per group), not a real aggregation.
  Maps to examples: single_pass_aggregation, union_cte_split

### Structural Transforms
- **union_consolidation**: Share dimension lookups across UNION ALL branches that scan different fact tables with the same dim joins.
  Maps to examples: shared_dimension_multi_channel
- **window_optimization**: Push filters before window functions when they don't affect the frame. Convert ROW_NUMBER + filter to LATERAL + LIMIT. Merge same-PARTITION windows into one sort pass.
  Maps to examples: deferred_window_aggregation
- **exists_restructuring**: Convert INTERSECT to EXISTS for semi-join short-circuit, or restructure complex EXISTS with shared CTEs. CHECK: does the query actually have INTERSECT or complex EXISTS.
  Maps to examples: intersect_to_exists, multi_intersect_exists_cte

## §6. REASONING PROCESS

First, use a `<reasoning>` block for your internal analysis. This will be stripped before parsing. Work through these steps IN ORDER:

1. **CLASSIFY**: What structural archetype is this query?
   (channel-comparison self-join / correlated-aggregate filter / star-join with late dim filter / repeated fact scan / multi-channel UNION ALL / EXISTS-set operations / other)

2. **EXPLAIN PLAN ANALYSIS**: From the EXPLAIN ANALYZE output, identify:
   - Compute wall-clock ms per EXPLAIN node. Sum repeated operations (e.g., 2x store_sales joins = total cost). The EXPLAIN is ground truth, not the logical-tree cost percentages.
   - Which nodes consume >10% of runtime and WHY
   - Where row counts drop sharply (existing selectivity)
   - Where row counts DON'T drop (missed optimization opportunity)
   - Whether the optimizer already splits CTEs, pushes predicates, or performs transforms you might otherwise assign
   - Count scans per base table. If a fact table is scanned N times, a restructuring that reduces it to 1 scan saves (N-1)/N of that table's I/O cost. Prioritize transforms that reduce scan count on the largest tables.
   - Whether the CTE is materialized once and probed multiple times, or re-executed per reference

   **Q-ERROR ROUTING** (§2b-i): The cardinality estimation routing above identifies WHERE the planner is wrong (locus) and HOW (direction). This routing is 85% accurate at predicting where the winning transform operates.
   - **Direction + Locus → Pathology routing**: This is the primary signal. Start your hypothesis from the routed pathologies.
   - **Structural flags** (DELIM_SCAN, EST_ZERO, etc.) are direct transform triggers. DELIM_SCAN = correlated subquery → P2. EST_ZERO = CTE stats blind → P0/P7.
   - **Ignore magnitude/severity** — Q-Error size does NOT predict optimization opportunity (win rate is flat across all severity levels).

3. **BOTTLENECK HYPOTHESIS**: From your EXPLAIN observations in Step 2, reason
   about WHY each bottleneck exists and what intervention could fix it.

   **Start from Q-Error routing.** The §2b-i routing identified the planner's
   worst mismatch direction+locus and mapped it to candidate pathologies.
   Use this as your primary hypothesis anchor, then verify against the plan structure:

   For the top 2-3 cost centers identified on the cost spine:

   a) DIAGNOSE: What optimizer behavior causes this cost?
      - What operation dominates? (scan, join, sort, aggregate, window)
      - Is the input to this node larger than it needs to be? Why?
      - Is the optimizer executing operations in a suboptimal order?
      - Is work being repeated that could be done once?

   b) HYPOTHESIZE: What SQL restructuring would change the physical plan?
      - Scan dominates + low pruning ratio → predicate not reaching scan layer
      - Same table scanned N times → consolidate into single scan + conditional agg
      - Large intermediate + selective late filter → push predicate earlier in chain
      - Nested loop on large table → decorrelate to CTE + hash join
      - Aggregate input >> output after join → pre-aggregate before join
      - CTE materialized but referenced once → inline as subquery
      - Window computed in CTE before join → defer window to post-join
      - OR across different columns + full scan → decompose into UNION ALL branches

   c) CALIBRATE against engine knowledge (§4):
      - If a documented gap matches your diagnosis: USE its evidence
        (what_worked, what_didnt_work, field_notes, decision gates)
        to sharpen your intervention. Follow its gates — they encode failures.
      - If a strength matches what you'd rewrite: STOP — the optimizer already
        handles it. Your rewrite adds overhead or destroys an optimization.
      - If no gap matches: your hypothesis is novel — tag as UNVERIFIED_HYPOTHESIS
        and proceed with structural reasoning only. Design a control variant
        that tests the opposite assumption.

4. **AGGREGATION TRAP CHECK**: For every aggregate function in the query, verify: does my proposed restructuring change which rows participate in each group? STDDEV_SAMP, VARIANCE, PERCENTILE_CONT, CORR are grouping-sensitive. SUM, COUNT, MIN, MAX are grouping-insensitive (modulo duplicates). If the query uses FILTER clauses or conditional aggregation, verify equivalence explicitly.

5. **INTERVENTION DESIGN**: For each hypothesized bottleneck from Step 3,
   design a transform:

   a) Match the diagnosed optimizer behavior to a transform category in §5a
   b) If engine evidence exists: prefer the proven approach and follow gates
   c) If no evidence exists (UNVERIFIED_HYPOTHESIS):
      - RANK by estimated impact: (scan reduction × table size) > (join reordering)
        > (aggregation restructuring) > (window deferral)
      - Include a rollback path — explain what makes this rewrite reversible
   Select the single best transform (or compound strategy of 2-3 transforms)
   that maximizes expected speedup for THIS query.

6. **LOGICAL TREE DESIGN**: Define the target logical tree topology for your chosen strategy. Verify that every node contract has exhaustive output columns by checking downstream references.
   CTE materialization matters: a CTE referenced by 2+ consumers will likely be materialized. A CTE referenced once may be inlined.

### Strategy Selection Rules

1. **CHECK APPLICABILITY**: Each transform has a structural prerequisite (correlated subquery, UNION ALL CTE, LEFT JOIN, etc.). Verify the query actually has the prerequisite before assigning a transform. DO NOT assign decorrelation if there are no correlated subqueries.
2. **CHECK OPTIMIZER OVERLAP**: Read the EXPLAIN plan. If the optimizer already performs a transform (e.g., already splits a UNION CTE, already pushes a predicate), that transform will have marginal benefit. Note this in your reasoning and prefer transforms the optimizer is NOT already doing.
3. **MAXIMIZE EXPECTED VALUE**: Select the single strategy with the highest expected speedup, considering both the magnitude of the bottleneck it addresses and the historical success rate.
4. **ASSESS RISK PER-QUERY**: Risk is a function of (transform x query complexity), not an inherent property of the transform. Decorrelation is low-risk on a simple EXISTS and high-risk on nested correlation inside a CTE. Assess per-assignment.
5. **COMPOSITION IS ALLOWED AND ENCOURAGED**: A strategy can combine 2-3 transforms from different categories (e.g., star_join_prefetch + scan_consolidation_pivot, or date_cte_isolate + early_filter + decorrelate). The TARGET_LOGICAL_TREE should reflect the combined structure. Compound strategies are often the source of the biggest wins.

Select 1-3 examples from the 'Maps to examples' notes that match the strategy. The system auto-loads full before/after SQL.

For TARGET_LOGICAL_TREE: Define the CTE structure you want produced. For NODE_CONTRACTS: Be exhaustive with OUTPUT columns — missing columns cause semantic breaks.

## §7a. Output Format

Then produce the structured briefing in EXACTLY this format:

```
=== SHARED BRIEFING ===

SEMANTIC_CONTRACT: (80-150 tokens, cover ONLY:)
(a) One sentence of business intent (start from pre-computed intent if available).
(b) JOIN type semantics that constrain rewrites (INNER = intersection = all sides must match).
(c) Any aggregation function traps specific to THIS query.
(d) Any filter dependencies that a rewrite could break.
Do NOT repeat information already in ACTIVE_CONSTRAINTS or REGRESSION_WARNINGS.

BOTTLENECK_DIAGNOSIS:
[Which operation dominates cost and WHY (not just '50% cost').
Scan-bound vs join-bound vs aggregation-bound.
Cardinality flow (how many rows at each stage).
What the optimizer already handles well (don't re-optimize).
Whether logical-tree cost percentages are misleading.]

ACTIVE_CONSTRAINTS:
- [CORRECTNESS_CONSTRAINT_ID]: [Why it applies to this query, 1 line]
- [ENGINE_GAP_ID]: [Evidence from EXPLAIN that this gap is active]
(List all 4 correctness constraints + the 1-3 engine gaps that
are active for THIS query based on your EXPLAIN analysis.)

REGRESSION_WARNINGS:
1. [Pattern name] ([observed regression]):
   CAUSE: [What happened mechanistically]
   RULE: [Actionable avoidance rule for THIS query]
(If no regression warnings are relevant, write 'None applicable.')

NODE_CONTRACTS: Write all fields as SQL fragments, not natural language. Example: `WHERE: d_year IN (1999, 2000)` not `WHERE: filter to target years`. Workers use these as specifications to code against.

=== WORKER 1 BRIEFING ===

STRATEGY: [strategy_name]
TARGET_LOGICAL_TREE:
  [node] -> [node] -> [node]
NODE_CONTRACTS:
  [node_name]:
    FROM: [tables/CTEs]
    JOIN: [join conditions]
    WHERE: [filters]
    GROUP BY: [columns] (if applicable)
    AGGREGATE: [functions] (if applicable)
    OUTPUT: [exhaustive column list]
    EXPECTED_ROWS: [approximate row count from EXPLAIN analysis]
    CONSUMERS: [downstream nodes]
EXAMPLES: [ex1], [ex2], [ex3]
EXAMPLE_ADAPTATION:
  [For each: what to apply, what to IGNORE for this strategy.]
HAZARD_FLAGS:
- [Specific risk for this approach on this query]
```

## Section Validation Checklist (MUST pass before final output)

### SHARED BRIEFING
- `SEMANTIC_CONTRACT`: 30-250 tokens covering business intent, JOIN semantics, aggregation trap, filter dependency.
- `BOTTLENECK_DIAGNOSIS`: dominant mechanism, bound type (`scan-bound`/`join-bound`/`aggregation-bound`), what optimizer already handles.
- `ACTIVE_CONSTRAINTS`: all 4 correctness IDs + 0-3 engine gap or hypothesis IDs with EXPLAIN evidence.
- `REGRESSION_WARNINGS`: `None applicable.` or entries with `CAUSE:` and `RULE:`.

### WORKER 1 BRIEFING
- `STRATEGY`: non-empty, describes the best single strategy.
- `TARGET_LOGICAL_TREE`: explicit node chain. `NODE_CONTRACTS`: every node has FROM, OUTPUT, CONSUMERS.
- `EXAMPLES`: 1-3 IDs. `EXAMPLE_ADAPTATION`: what to adapt/ignore per example.
- `HAZARD_FLAGS`: query-specific risks, not generic cautions.

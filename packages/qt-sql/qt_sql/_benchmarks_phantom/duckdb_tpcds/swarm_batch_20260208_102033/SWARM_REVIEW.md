# Swarm Pipeline Review: DuckDB TPC-DS SF10

**Batch**: `swarm_batch_20260208_102033`
**Date**: 2026-02-08
**Database**: TPC-DS SF10 on DuckDB 1.4.3
**LLM**: DeepSeek Reasoner (deepseek-reasoner)
**Pipeline**: 3-iteration swarm (4 workers + snipe + reanalyze/final)

---

## Executive Summary

| Metric | Value |
|--------|-------|
| Total queries | 101 |
| Total LLM API calls | 788 |
| Total runtime | ~65 min |
| Exit gate | 2.0x |
| Estimated cost | ~$3.46 |

### Outcome Breakdown

| Status | Threshold | Count | % |
|--------|-----------|------:|---:|
| WIN | >=2.0x | 11 | 10.9% |
| IMPROVED | 1.1x-2.0x | 55 | 54.5% |
| NEUTRAL | 0.95x-1.1x | 28 | 27.7% |
| REGRESSION | <0.95x | 3 | 3.0% |
| ERROR | all fail/error | 4 | 4.0% |

**Net positive rate**: 66/101 (65.3%) queries improved by >= 1.1x

**Average best speedup** (passing queries): 1.45x
**Median best speedup** (passing queries): 1.21x

### Top 10 Winners

| Rank | Query | Speedup | Worker | Iter | Baseline (ms) | Strategy |
|-----:|-------|--------:|-------:|-----:|--------------:|----------|
| 1 | query_88 | **6.24x** | W2 | iter0 | 1416 | moderate_dimension_isolation |
| 2 | query_40 | **5.23x** | W3 | iter0 | 79 | Aggressive Multi-CTE Prefetch |
| 3 | query_95 | **4.69x** | W3 | iter0 | 2784 | aggressive_prefetch_restructure |
| 4 | query_70 | **2.52x** | W1 | iter0 | 740 | safe_pushdown_and_materialization |
| 5 | query_36 | **2.47x** | W6 | iter2 | 879 | final_worker (reanalyze) |
| 6 | query_14 | **2.39x** | W6 | iter2 | 5290 | final_worker (reanalyze) |
| 7 | query_35 | **2.36x** | W3 | iter0 | 735 | aggressive_decorrelate_prefetch |
| 8 | query_99 | **2.33x** | W6 | iter2 | 314 | final_worker (reanalyze) |
| 9 | query_59 | **2.23x** | W3 | iter0 | 1794 | aggressive_single_pass_prefetch |
| 10 | query_15 | **2.13x** | W5 | iter1 | 92 | snipe_worker |

### Queries Exiting at 2x Gate (iter0)

6 queries achieved >= 2.0x in iter0 and skipped subsequent iterations:

- **query_88**: 6.24x
- **query_40**: 5.23x
- **query_95**: 4.69x
- **query_70**: 2.52x
- **query_35**: 2.36x
- **query_59**: 2.23x

---

## Pipeline Performance

### Phase Timing

| Phase | Description | Calls | Completed At | Duration |
|-------|-------------|------:|-------------|----------|
| phase2 | Analyst Fan-Out | 101 | 11:13:21 | 2.4 min |
| phase2_5 | Parse Assignments | --- | 11:13:45 | 0.4 min |
| phase3 | Worker Generation | 404 | 11:22:22 | 8.6 min |
| phase4 | Benchmark iter0 | 101 | 11:42:25 | 20.1 min |
| phase5 | Snipe Workers (LLM) | 95 | 11:45:31 | 3.1 min |
| phase5_bench | Benchmark iter1 | 95 | 11:53:58 | 8.4 min |
| phase6 | Re-Analyze (LLM) | 94 | 11:59:23 | 5.4 min |
| phase7 | Final Workers (LLM) | 94 | 12:04:12 | 4.8 min |
| phase7_bench | Benchmark iter2 | 93 | 10:58:17 |  |

**Total wall time**: ~65 min (11:11 to 12:15)

### API Call Success Rates

| Phase | Total | Success | Rate |
|-------|------:|--------:|-----:|
| Analyst Fan-Out (P2) | 101 | 101 | 100% |
| Worker Generation (P3) | 404 | 404 | 100% |
| Snipe Workers (P5) | 95 | 95 | 100% |
| Re-Analyze (P6) | 94 | 94 | 100% |
| Final Workers (P7) | 94 | 94 | 100% |
| **Total** | **788** | **788** | **100%** |

All 788 LLM API calls succeeded with zero transport/timeout errors.

### SQL Extraction Success

| Iteration | Extracted | Total | Rate |
|-----------|----------:|------:|-----:|
| iter0 (4 workers) | 404 | 404 | 100.0% |
| iter1 (snipe) | 95 | 95 | 100.0% |
| iter2 (final) | 94 | 94 | 100.0% |

---

## Iteration Effectiveness Analysis

The swarm pipeline runs up to 3 iterations per query:

1. **iter0**: 4 parallel workers with diverse strategies (W1-W4)
2. **iter1 (snipe)**: 1 worker (W5) sees iter0 results, targets best approach
3. **iter2 (reanalyze + final)**: Analyst re-examines all results, 1 final worker (W6) gets refined strategy

### Where Did the Best Result Come From?

| Source | Queries | % |
|--------|--------:|---:|
| iter0 | 46 | 45.5% |
| iter1 | 32 | 31.7% |
| iter2 | 19 | 18.8% |
| none | 4 | 4.0% |

- **iter1 (snipe) beat iter0** on 36 queries (35.6%)
- **iter2 (final) beat iter0+iter1** on 19 queries (18.8%)
- **Multi-iteration produced the overall best** on 51 queries (50.5%)

### Verdict: Did Multi-Iteration Add Value?

**Yes.** 51 of 101 queries (50.5%) got their best result from iter1 or iter2, not iter0.

Notable queries where iter2 (reanalyze+final) produced the best result:

- **query_36**: 2.47x
- **query_14**: 2.39x
- **query_99**: 2.33x
- **query_93**: 2.02x
- **query_87**: 1.91x
- **query_32**: 1.82x
- **query_92**: 1.81x
- **query_64**: 1.65x
- **query_90**: 1.64x
- **query_62**: 1.62x

Notable queries where iter1 (snipe) produced the best result:

- **query_15**: 2.13x
- **query_94**: 1.74x
- **query_75**: 1.58x
- **query_57**: 1.49x
- **query_56**: 1.35x
- **query_51**: 1.28x
- **query_81**: 1.27x
- **query_16**: 1.25x
- **query_23a**: 1.24x
- **query_30**: 1.24x

### iter0 Worker Performance (W1-W4)

| Worker | Pass | Fail (rows) | Error (SQL) | Win Rate (>=1.1x) |
|-------:|-----:|------------:|------------:|-------------------:|
| W1 | 88 | 5 | 8 | 17/101 (17%) |
| W2 | 87 | 6 | 8 | 22/101 (22%) |
| W3 | 85 | 6 | 10 | 23/101 (23%) |
| W4 | 83 | 5 | 13 | 19/101 (19%) |

---

## Transform / Strategy Analysis

### Strategy Frequency (assigned by Analyst)

| Strategy | Assigned | Passing | Avg Speedup | Win Rate (>=1.1x) |
|----------|--------:|-------:|-----------:|---------:|
| moderate_dimension_isolation | 65 | 56 | 1.14x | 13/56 (23%) |
| novel_structural_transform | 59 | 50 | 1.01x | 11/50 (22%) |
| aggressive_prefetch_restructure | 30 | 25 | 1.15x | 5/25 (20%) |
| conservative_filter_pushdown | 17 | 16 | 0.99x | 3/16 (19%) |
| aggressive_prefetch_restructuring | 16 | 14 | 0.90x | 1/14 (7%) |
| conservative_pushdown_filter | 15 | 12 | 1.01x | 1/12 (8%) |
| novel_structural_transforms | 14 | 10 | 0.83x | 2/10 (20%) |
| moderate_date_dimension_isolation | 13 | 9 | 1.17x | 4/9 (44%) |
| conservative_early_filtering | 12 | 10 | 1.02x | 1/10 (10%) |
| conservative_predicate_pushdown | 10 | 10 | 0.98x | 1/10 (10%) |
| aggressive_multi_cte_prefetch | 7 | 6 | 1.14x | 3/6 (50%) |
| novel_structural_transformation | 7 | 6 | 1.08x | 2/6 (33%) |
| conservative_early_filter_pushdown | 7 | 5 | 1.00x | 0/5 (0%) |
| aggressive_prefetch_consolidation | 7 | 6 | 1.05x | 1/6 (17%) |
| aggressive_fact_prefetch | 6 | 5 | 0.94x | 2/5 (40%) |
| aggressive_multi_cte_restructure | 6 | 5 | 1.19x | 2/5 (40%) |
| conservative_early_pushdown | 6 | 6 | 1.11x | 2/6 (33%) |
| conservative_early_reduction | 5 | 4 | 2.08x | 1/4 (25%) |
| conservative_pushdown_earlyfilter | 5 | 5 | 1.01x | 1/5 (20%) |
| aggressive_multi_cte_restructuring | 4 | 4 | 0.98x | 1/4 (25%) |

### Example Pattern Usage (from Gold Examples)

| Example | Times Used | Passing | Avg Speedup | Win Rate (>=1.1x) |
|---------|----------:|-------:|-----------:|---------:|
| pushdown | 101 | 88 | 1.08x | 17/88 (19%) |
| early_filter | 101 | 88 | 1.08x | 17/88 (19%) |
| dimension_cte_isolate | 101 | 87 | 1.12x | 24/87 (28%) |
| prefetch_fact_join | 100 | 84 | 1.19x | 22/84 (26%) |
| or_to_union | 97 | 80 | 0.98x | 19/80 (24%) |
| date_cte_isolate | 96 | 82 | 1.13x | 21/82 (26%) |
| materialize_cte | 95 | 84 | 1.08x | 14/84 (17%) |
| intersect_to_exists | 93 | 77 | 0.91x | 17/77 (22%) |
| multi_dimension_prefetch | 92 | 78 | 1.14x | 20/78 (26%) |
| shared_dimension_multi_channel | 84 | 70 | 1.10x | 19/70 (27%) |
| single_pass_aggregation | 84 | 71 | 1.22x | 17/71 (24%) |
| composite_decorrelate_union | 65 | 58 | 1.01x | 12/58 (21%) |
| decorrelate | 37 | 27 | 1.00x | 7/27 (26%) |
| multi_date_range_cte | 31 | 26 | 1.16x | 7/26 (27%) |
| deferred_window_aggregation | 20 | 17 | 1.07x | 5/17 (29%) |
| union_cte_split | 15 | 12 | 1.36x | 5/12 (42%) |

---

## Error Analysis

| Metric | Count | % |
|--------|------:|---:|
| Total worker evaluations | 592 | 100% |
| Clean passes | 493 | 83.3% |
| Row mismatches (FAIL) | 34 | 5.7% |
| SQL errors (ERROR) | 65 | 11.0% |

### Error Categories

| Category | Count | % of Errors |
|----------|------:|------------:|
| Binder Error | 59 | 90.8% |
| Catalog Error | 3 | 4.6% |
| Parser/Syntax Error | 2 | 3.1% |
| Not Implemented | 1 | 1.5% |

### All SQL Errors (detailed)

| Query | Worker | Iter | Category | Error Message (truncated) |
|-------|-------:|-----:|----------|---------------------------|
| query_10 | W3 | 0 | Binder Error | Binder Error: Table "cd" does not have a column named "cd_employed_count"  Candidate bindings: : "cd |
| query_13 | W1 | 0 | Binder Error | Binder Error: Referenced column "cd_marital_status" not found in FROM clause! Candidate bindings: "d |
| query_13 | W5 | 1 | Binder Error | Binder Error: Referenced column "ss_net_profit" not found in FROM clause! Candidate bindings: "ca_gm |
| query_13 | W6 | 2 | Parser/Syntax Error | Parser Error: syntax error at or near "WHERE"  LINE 22:     WHERE cd_marital_status IN ('D', 'S', 'M |
| query_14 | W4 | 0 | Binder Error | Binder Error: Ambiguous reference to column name "ss_item_sk" (use: "store_sales.ss_item_sk" or "ci. |
| query_14 | W5 | 1 | Binder Error | Binder Error: Ambiguous reference to column name "ss_item_sk" (use: "store_sales.ss_item_sk" or "cro |
| query_15 | W3 | 0 | Binder Error | Binder Error: Referenced column "cs_bill_customer_sk" not found in FROM clause! Candidate bindings:  |
| query_16 | W1 | 0 | Catalog Error | Catalog Error: Table with name filtered_call_enter does not exist! Did you mean "call_center"?  LINE |
| query_17 | W4 | 0 | Binder Error | Binder Error: Ambiguous reference to column name "d_date_sk" (use: "filtered_dates.d_date_sk" or "fi |
| query_17 | W6 | 2 | Binder Error | Binder Error: Values list "sr" does not have a column named "i_item_desc"  LINE 96:     AND ss.i_ite |
| query_18 | W3 | 0 | Binder Error | Binder Error: Ambiguous reference to column name "cd_dep_count" (use: "prejoined_fact.cd_dep_count"  |
| query_22 | W6 | 2 | Parser/Syntax Error | Parser Error: syntax error at or near "UNION"  LINE 60:   UNION ALL            ^ |
| query_23 | W6 | 2 | Binder Error | Binder Error: Referenced column "d_date" not found in FROM clause! Candidate bindings: "i_category", |
| query_23a | W2 | 0 | Binder Error | Binder Error: Referenced column "d_date" not found in FROM clause! Candidate bindings: "d_date_sk",  |
| query_23a | W3 | 0 | Binder Error | Binder Error: Referenced column "d_date" not found in FROM clause! Candidate bindings: "d_date_sk",  |
| query_23b | W2 | 0 | Binder Error | Binder Error: Referenced column "d_date" not found in FROM clause! Candidate bindings: "d_date_sk",  |
| query_23b | W4 | 0 | Binder Error | Binder Error: Ambiguous reference to column name "c_customer_sk" (use: "customer.c_customer_sk" or " |
| query_23b | W6 | 2 | Binder Error | Binder Error: Ambiguous reference to column name "c_customer_sk" (use: "best_ss_customer.c_customer_ |
| query_24 | W1 | 0 | Binder Error | Binder Error: Referenced table "s" not found! Candidate tables: "ss", "sr"  LINE 71:     s.s_state,  |
| query_24 | W4 | 0 | Binder Error | Binder Error: column threshold must appear in the GROUP BY clause or be used in an aggregate functio |
| query_25 | W4 | 0 | Binder Error | Binder Error: Referenced column "d_year" not found in FROM clause! Candidate bindings: "d_date_sk",  |
| query_27 | W4 | 0 | Binder Error | Binder Error: Values list "sb" does not have a column named "ss_sold_date_sk"  LINE 111: JOIN filter |
| query_27 | W5 | 1 | Binder Error | Binder Error: Values list "fs" does not have a column named "s_state"  LINE 41: GROUP BY ROLLUP(i.i_ |
| query_27 | W6 | 2 | Binder Error | Binder Error: column "agg1" must appear in the GROUP BY clause or must be part of an aggregate funct |
| query_29 | W1 | 0 | Binder Error | Binder Error: Referenced column "d_moy" not found in FROM clause! Candidate bindings: "ss_cdemo_sk", |
| query_30 | W6 | 2 | Binder Error | Binder Error: Referenced column "ca_state" not found in FROM clause! Candidate bindings: "wr_account |
| query_31 | W4 | 0 | Not Implemented | Not implemented Error: Non-inner join on correlated columns not supported |
| query_32 | W4 | 0 | Binder Error | Binder Error: Ambiguous reference to column name "cs_item_sk" (use: "catalog_sales.cs_item_sk" or "i |
| query_32 | W5 | 1 | Binder Error | Binder Error: Ambiguous reference to column name "cs_item_sk" (use: "catalog_sales.cs_item_sk" or "i |
| query_34 | W6 | 2 | Binder Error | Binder Error: Referenced column "hd_buy_ppotENTIAL" not found in FROM clause! Candidate bindings: "h |
| query_39 | W6 | 2 | Binder Error | Binder Error: Referenced table "d" not found! Candidate tables: "i"  LINE 23: ...  GROUP BY w.w_ware |
| query_4 | W4 | 0 | Binder Error | Binder Error: Referenced table "s1" not found! Candidate tables: "s2", "c1", "w1"  LINE 91:   c2.yea |
| query_44 | W2 | 0 | Binder Error | Binder Error: WHERE clause cannot contain window functions!  LINE 22:     RANK() OVER (ORDER BY rank |
| query_45 | W5 | 1 | Binder Error | Binder Error: Referenced column "ca_address_sk" not found in FROM clause! Candidate bindings: "d_dat |
| query_46 | W6 | 2 | Catalog Error | Catalog Error: unrecognized configuration parameter "force_parallelism"  Did you mean: "force_compre |
| query_48 | W2 | 0 | Binder Error | Binder Error: Referenced column "cd_marital_status" not found in FROM clause! Candidate bindings: "d |
| query_48 | W3 | 0 | Binder Error | Binder Error: Referenced column "cd_marital_status" not found in FROM clause! Candidate bindings: "c |
| query_48 | W5 | 1 | Binder Error | Binder Error: Referenced column "ss_store_sk" not found in FROM clause! Candidate bindings: "s_store |
| query_5 | W1 | 0 | Binder Error | Binder Error: Table "web_returns" does not have a column named "wr_web_site_sk"  Candidate bindings: |
| query_50 | W6 | 2 | Binder Error | Binder Error: Referenced column "sr_returned_date_sk" not found in FROM clause! Candidate bindings:  |
| query_54 | W1 | 0 | Binder Error | Binder Error: Ambiguous reference to column name "d_date_sk" (use: "date_dim.d_date_sk" or "may_date |
| query_54 | W3 | 0 | Binder Error | Binder Error: Ambiguous reference to column name "d_month_seq" (use: "date_dim.d_month_seq" or "base |
| query_56 | W6 | 2 | Binder Error | Binder Error: Referenced table "i" not found! Candidate tables: "store_sales"  LINE 25:   GROUP BY i |
| query_58 | W2 | 0 | Binder Error | Binder Error: Ambiguous reference to column name "d_week_seq" (use: "date_dim.d_week_seq" or "target |
| query_58 | W6 | 2 | Binder Error | Binder Error: Ambiguous reference to column name "d_week_seq" (use: "date_dim.d_week_seq" or "target |
| query_6 | W3 | 0 | Binder Error | Binder Error: Ambiguous reference to column name "i_category" (use: "item.i_category" or "cap.i_cate |
| query_61 | W5 | 1 | Binder Error | Binder Error: Referenced column "c_customer_sk" not found in FROM clause! Candidate bindings: "s_sto |
| query_67 | W4 | 0 | Binder Error | Binder Error: Referenced column "d_year" not found in FROM clause! Candidate bindings: "d_date_sk",  |
| query_67 | W5 | 1 | Binder Error | Binder Error: Referenced column "ss_sold_date_sk" not found in FROM clause! Candidate bindings: "ss_ |
| query_70 | W4 | 0 | Binder Error | Binder Error: HAVING clause cannot contain window functions!  LINE 14:     HAVING RANK() OVER (ORDER |
| query_72 | W2 | 0 | Binder Error | Binder Error: Values list "base" does not have a column named "cs_ship_date_sk"  LINE 82: JOIN filte |
| query_72 | W5 | 1 | Binder Error | Binder Error: Referenced table "d1" not found! Candidate tables: "d2", "d3"  LINE 52: WHERE d1.d_wee |
| query_74 | W1 | 0 | Binder Error | Binder Error: Referenced column "ss_ss_net_paid" not found in FROM clause! Candidate bindings: "ss_n |
| query_74 | W3 | 0 | Binder Error | Binder Error: Referenced column "d_year" not found in FROM clause! Candidate bindings: "c_email_addr |
| query_77 | W6 | 2 | Binder Error | Binder Error: Referenced column "sr_store_sk" not found in FROM clause! Candidate bindings: "ss_stor |
| query_8 | W6 | 2 | Binder Error | Binder Error: Referenced column "ss_ss_net_profit" not found in FROM clause! Candidate bindings: "ss |
| query_81 | W3 | 0 | Binder Error | Binder Error: Values list "ca" does not have a column named "ca_street_number"  LINE 27:     ca.ca_s |
| query_85 | W1 | 0 | Binder Error | Binder Error: Values list "ca" does not have a column named "ca_state"  LINE 64:         AND ca.ca_s |
| query_85 | W2 | 0 | Binder Error | Binder Error: Referenced column "ca_state" not found in FROM clause! Candidate bindings: "ca_address |
| query_85 | W3 | 0 | Binder Error | Binder Error: Referenced column "ca_state" not found in FROM clause! Candidate bindings: "ca_address |
| query_89 | W4 | 0 | Binder Error | Binder Error: Values list "d" does not have a column named "d_moy"  LINE 41:     d.d_moy             |
| query_9 | W6 | 2 | Catalog Error | Catalog Error: unrecognized configuration parameter "enable_verification"  Did you mean: "enable_pro |
| query_90 | W4 | 0 | Binder Error | Binder Error: Referenced column "ws_sold_time_sk" not found in FROM clause! Candidate bindings: "t_t |
| query_92 | W5 | 1 | Binder Error | Binder Error: Ambiguous reference to column name "ws_item_sk" (use: "web_sales.ws_item_sk" or "iad.w |
| query_93 | W2 | 0 | Binder Error | Binder Error: Values list "t" does not have a column named "sr_reason_sk"  LINE 26:     WHERE t.sr_r |

### Queries Where No Valid Optimization Was Found (4)

Every worker either errored or produced wrong row counts across all 3 iterations:

#### query_2 (baseline: 607ms, 2513 rows)

- iter0 W1: **FAIL** (rows mismatch, would-be speedup 2.08x)
- iter0 W2: **FAIL** (rows mismatch, would-be speedup 2.43x)
- iter0 W3: **FAIL** (rows mismatch, would-be speedup 1.51x)
- iter0 W4: **FAIL** (rows mismatch, would-be speedup 2.39x)
- iter1 W5: **FAIL** (rows mismatch, would-be speedup 1.23x)
- iter2 W6: **FAIL** (rows mismatch, would-be speedup 1.83x)

**Note**: 6 workers produced faster SQL (2.43x best) but wrong row counts -- semantically incorrect optimization.

#### query_23 (baseline: 12755ms, 5 rows)

- iter0 W1: **FAIL** (rows mismatch, would-be speedup 2.63x)
- iter0 W2: **FAIL** (rows mismatch, would-be speedup 1.94x)
- iter0 W3: **FAIL** (rows mismatch, would-be speedup 3.13x)
- iter0 W4: **FAIL** (rows mismatch, would-be speedup 2.84x)
- iter1 W5: **FAIL** (rows mismatch, would-be speedup 2.90x)
- iter2 W6: **ERROR** - `Binder Error: Referenced column "d_date" not found in FROM clause!
Candidate bindings: "i_category", "i_category_id", "i`

**Note**: 5 workers produced faster SQL (3.13x best) but wrong row counts -- semantically incorrect optimization.

#### query_24 (baseline: 913ms, 0 rows)

- iter0 W1: **ERROR** - `Binder Error: Referenced table "s" not found!
Candidate tables: "ss", "sr"

LINE 71:     s.s_state,
             ^`
- iter0 W2: **FAIL** (rows mismatch, would-be speedup 1.94x)
- iter0 W3: **FAIL** (rows mismatch, would-be speedup 0.78x)
- iter0 W4: **ERROR** - `Binder Error: column threshold must appear in the GROUP BY clause or be used in an aggregate function`
- iter1 W5: **FAIL** (rows mismatch, would-be speedup 0.76x)
- iter2 W6: **FAIL** (rows mismatch, would-be speedup 1.99x)

**Note**: 2 workers produced faster SQL (1.99x best) but wrong row counts -- semantically incorrect optimization.

#### query_39 (baseline: 3202ms, 52 rows)

- iter0 W1: **FAIL** (rows mismatch, would-be speedup 11.05x)
- iter0 W2: **FAIL** (rows mismatch, would-be speedup 8.72x)
- iter0 W3: **FAIL** (rows mismatch, would-be speedup 8.79x)
- iter0 W4: **FAIL** (rows mismatch, would-be speedup 12.85x)
- iter1 W5: **FAIL** (rows mismatch, would-be speedup 12.52x)
- iter2 W6: **ERROR** - `Binder Error: Referenced table "d" not found!
Candidate tables: "i"

LINE 23: ...  GROUP BY w.w_warehouse_sk, w.w_wareho`

**Note**: 5 workers produced faster SQL (12.85x best) but wrong row counts -- semantically incorrect optimization.

### Mixed Queries (some workers error, some pass) [42]

These queries had at least one SQL error AND at least one passing result:

- **query_10**: 1 errors, 5 passes, best 1.11x
- **query_13**: 3 errors, 3 passes, best 1.21x
- **query_14**: 2 errors, 4 passes, best 2.39x
- **query_15**: 1 errors, 4 passes, best 2.13x
- **query_16**: 1 errors, 5 passes, best 1.25x
- **query_17**: 2 errors, 4 passes, best 1.04x
- **query_18**: 1 errors, 5 passes, best 1.57x
- **query_22**: 1 errors, 5 passes, best 1.04x
- **query_23a**: 2 errors, 4 passes, best 1.24x
- **query_23b**: 3 errors, 3 passes, best 1.18x
- **query_25**: 1 errors, 4 passes, best 0.95x
- **query_27**: 3 errors, 3 passes, best 1.58x
- **query_29**: 1 errors, 5 passes, best 1.03x
- **query_30**: 1 errors, 3 passes, best 1.24x
- **query_31**: 1 errors, 5 passes, best 0.81x
- **query_32**: 2 errors, 4 passes, best 1.82x
- **query_34**: 1 errors, 5 passes, best 1.03x
- **query_4**: 1 errors, 5 passes, best 1.61x
- **query_44**: 1 errors, 5 passes, best 1.22x
- **query_45**: 1 errors, 5 passes, best 1.34x
- **query_46**: 1 errors, 5 passes, best 1.02x
- **query_48**: 3 errors, 3 passes, best 1.11x
- **query_5**: 1 errors, 5 passes, best 1.36x
- **query_50**: 1 errors, 5 passes, best 1.10x
- **query_54**: 2 errors, 4 passes, best 1.05x
- **query_56**: 1 errors, 5 passes, best 1.35x
- **query_58**: 2 errors, 4 passes, best 1.19x
- **query_6**: 1 errors, 4 passes, best 1.23x
- **query_61**: 1 errors, 5 passes, best 1.34x
- **query_67**: 2 errors, 4 passes, best 1.50x
- **query_70**: 1 errors, 3 passes, best 2.52x
- **query_72**: 2 errors, 3 passes, best 1.13x
- **query_74**: 2 errors, 4 passes, best 1.23x
- **query_77**: 1 errors, 5 passes, best 1.07x
- **query_8**: 1 errors, 5 passes, best 1.07x
- **query_81**: 1 errors, 4 passes, best 1.27x
- **query_85**: 3 errors, 2 passes, best 1.01x
- **query_89**: 1 errors, 5 passes, best 1.06x
- **query_9**: 1 errors, 5 passes, best 0.42x
- **query_90**: 1 errors, 5 passes, best 1.64x
- **query_92**: 1 errors, 5 passes, best 1.81x
- **query_93**: 1 errors, 5 passes, best 2.02x

---

## Regression Deep Dive

**3 queries** where the best passing result was still a regression (<0.95x):

| Query | Best Speedup | Baseline (ms) | Best Worker | Iter | Notes |
|-------|------------:|--------------:|------------:|-----:|-------|
| query_9 | **0.424x** | 1366 | W1 | iter0 | conservative_single_pass |
| query_31 | **0.806x** | 442 | W5 | iter1 | snipe |
| query_91 | **0.832x** | 34 | W1 | iter0 | conservative_early_filtering |

### Are Fast Queries More Prone to Regression?

- Average baseline of regressed queries: 614ms
- Average baseline of non-regressed queries: 965ms
- **Yes** - regressed queries have lower baselines (614ms vs 965ms), suggesting fast queries leave less room for optimization.

### Regression Details

#### query_9 (0.424x, baseline 1366ms)

| Worker | Iter | Speedup | Status |
|-------:|-----:|--------:|--------|
| W1 | 0 | 0.42x | REGRESSION |
| W2 | 0 | 0.39x | REGRESSION |
| W3 | 0 | 0.27x | REGRESSION |
| W4 | 0 | 0.39x | REGRESSION |
| W5 | 1 | 0.41x | REGRESSION |
| W6 | 2 | --- | ERROR |

**Re-analyze failure assessment**: All attempts fell short because they failed to address the fundamental bottleneck: DuckDB's columnar execution engine struggles with computing 15 different conditional aggregates (5 counts + 10 averages) in a single scan when they involve overlapping CASE conditions. The original query's independent subqueries allow DuckDB to apply specialized optimizations for each quantity range (potentially usi

#### query_31 (0.806x, baseline 442ms)

| Worker | Iter | Speedup | Status |
|-------:|-----:|--------:|--------|
| W1 | 0 | 0.76x | REGRESSION |
| W2 | 0 | 0.77x | REGRESSION |
| W3 | 0 | 0.77x | REGRESSION |
| W4 | 0 | --- | ERROR |
| W5 | 1 | 0.81x | REGRESSION |
| W6 | 2 | 0.67x | REGRESSION |

**Re-analyze failure assessment**: All attempts fell short because they only optimized the dimension filtering phase while leaving the core bottleneck untouched: the six-way self-join (three instances each of ss and ws) with complex ratio comparisons. The attempts merely pushed date filters earlier (year=2000, quarters 1-3) into CTEs, but this filtering happens naturally during grouping anyway. The real overhead comes from:
1. **Ma

#### query_91 (0.832x, baseline 34ms)

| Worker | Iter | Speedup | Status |
|-------:|-----:|--------:|--------|
| W1 | 0 | 0.83x | REGRESSION |
| W2 | 0 | 0.82x | REGRESSION |
| W3 | 0 | 0.02x | REGRESSION |
| W4 | 0 | 0.70x | FAIL |
| W5 | 1 | 0.62x | REGRESSION |
| W6 | 2 | 0.77x | REGRESSION |

**Re-analyze failure assessment**: All workers fell short due to improper join ordering and missed opportunities to reduce the largest fact table early. Worker 1-2 isolated dimension filtering but still joined all filtered dimensions to customer before joining to catalog_returns, forcing unnecessary work on the customer table. Worker 3's cross join between customer_demographics and household_demographics was catastrophic. Worker 4'

---

## Row Mismatch (FAIL) Analysis

**16 queries** had at least one worker produce wrong row counts.

**4 queries** had ALL workers produce wrong rows or errors:

- **query_2** (baseline 607ms) - best would-have-been: 2.43x
- **query_23** (baseline 12755ms) - best would-have-been: 3.13x
- **query_24** (baseline 913ms) - best would-have-been: 1.99x
- **query_39** (baseline 3202ms) - best would-have-been: 12.85x

**Row mismatch rate**: 34/527 non-error evaluations (6.5%)

### Queries with Most Row Mismatches

| Query | Fail Count | Total Workers | Baseline (ms) |
|-------|----------:|--------------:|--------------:|
| query_2 | 6 | 6 | 607 |
| query_23 | 5 | 6 | 12755 |
| query_39 | 5 | 6 | 3202 |
| query_24 | 4 | 6 | 913 |
| query_30 | 2 | 6 | 113 |
| query_49 | 2 | 6 | 320 |
| query_6 | 1 | 6 | 198 |
| query_25 | 1 | 6 | 89 |
| query_37 | 1 | 6 | 83 |
| query_41 | 1 | 6 | 16 |
| query_64 | 1 | 6 | 1420 |
| query_81 | 1 | 6 | 133 |
| query_84 | 1 | 6 | 52 |
| query_85 | 1 | 6 | 234 |
| query_91 | 1 | 6 | 34 |

---

## Per-Query Details

Sorted by best speedup (descending). Each query shows all worker results across all iterations.

### query_88 **[WIN]**

- **Baseline**: 1415.6 ms (1 rows)
- **Best speedup**: 6.24x (W2, iter0)
- **Exited at 2x gate** after iter0

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 5.27x | WIN |  |
| W2 | 0 | 6.24x | WIN | BEST |
| W3 | 0 | 5.85x | WIN |  |
| W4 | 0 | 6.10x | WIN |  |

**Strategies assigned:**

- W1: `conservative_early_reduction` (early_filter, pushdown, materialize_cte)
  - Apply aggressive early filtering to dimension tables before joining, push predicates down into subqueries, and materialize repeated filter patterns into CTEs to avoid recomputation.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, date_cte_isolate, shared_dimension_multi_channel)
  - Isolate filtered dimension tables (store, household_demographics) and time ranges into separate CTEs before joining, enabling predicate pushdown and reusing shared dimension filters across time window
- W3: `aggressive_single_pass_restructure` (single_pass_aggregation, prefetch_fact_join, multi_date_range_cte)
  - Consolidate all time-window subqueries into a single CTE that scans store_sales once with conditional aggregation, prefetch filtered dimensions, and handle multiple time ranges in a unified structure.
- W4: `novel_structural_transform` (or_to_union, union_cte_split, composite_decorrelate_union)
  - Transform OR conditions on household_demographics into UNION ALL branches for better index usage, split into specialized CTEs, and apply decorrelation techniques to restructure the entire query flow.

---

### query_40 **[WIN]**

- **Baseline**: 78.6 ms (100 rows)
- **Best speedup**: 5.23x (W3, iter0)
- **Exited at 2x gate** after iter0

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.96x | NEUTRAL |  |
| W2 | 0 | 0.95x | NEUTRAL |  |
| W3 | 0 | 5.23x | WIN | BEST |
| W4 | 0 | 0.87x | REGRESSION |  |

**Strategies assigned:**

- W1: `Conservative Early Filtering` (early_filter, pushdown, materialize_cte)
  - Apply dimension filtering first to reduce join costs, push predicates down, and materialize reusable filtered dimensions as CTEs.
- W2: `Dimension Pre-isolation Restructuring` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate date and dimension filtering into separate CTEs before joining with facts, enabling predicate pushdown and dimension reuse across multiple channel joins.
- W3: `Aggressive Multi-CTE Prefetch` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-filter and pre-join dimensions with facts in staged CTEs, then consolidate multiple conditional aggregations into a single scan.
- W4: `Novel Structural Transformation` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Transform conditional logic (CASE to UNION) for better parallelism, apply subquery decorrelation techniques, and explore intersection patterns for join optimization.

---

### query_95 **[WIN]**

- **Baseline**: 2784.2 ms (1 rows)
- **Best speedup**: 4.69x (W3, iter0)
- **Exited at 2x gate** after iter0

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.15x | IMPROVED |  |
| W2 | 0 | 1.00x | NEUTRAL |  |
| W3 | 0 | 4.69x | WIN | BEST |
| W4 | 0 | 0.74x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_early_filter` (early_filter, pushdown, dimension_cte_isolate)
  - Filter dimension tables (date_dim, customer_address, web_site) first into CTEs before joining with web_sales to reduce intermediate data volume. Push predicates into CTEs where possible.
- W2: `moderate_date_dimension_isolation` (date_cte_isolate, materialize_cte, shared_dimension_multi_channel)
  - Extract date filtering into a dedicated CTE, materialize the ws_wh CTE to avoid recomputation, and share filtered dimension tables across all web_sales joins for consistency.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-filter dimensions into CTEs, then pre-join with web_sales early. Consolidate the two subquery checks on ws_wh into a single CTE with conditional logic to scan web_sales once.
- W4: `novel_structural_transform` (intersect_to_exists, composite_decorrelate_union, or_to_union)
  - Convert the dual IN subqueries to EXISTS clauses for better join planning. If any OR conditions emerge, split into UNION ALL. Consider decorrelating multiple subqueries into a pre-materialized distinc

---

### query_70 **[WIN]**

- **Baseline**: 739.9 ms (7 rows)
- **Best speedup**: 2.52x (W1, iter0)
- **Exited at 2x gate** after iter0

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 2.52x | WIN | BEST |
| W2 | 0 | 0.97x | NEUTRAL |  |
| W3 | 0 | 0.96x | NEUTRAL |  |
| W4 | 0 | --- | ERROR | Binder Error |

**Strategies assigned:**

- W1: `safe_pushdown_and_materialization` (early_filter, pushdown, materialize_cte)
  - Focus on safe predicate pushdown into CTEs and early filtering of dimension tables to reduce join sizes before expensive aggregations. Use materialized CTEs to avoid recomputation of common subqueries
- W2: `date_dimension_isolation_decorrelation` (date_cte_isolate, dimension_cte_isolate, decorrelate)
  - Isolate date and store dimension filtering into separate CTEs with predicate pushdown, then decorrelate the top-5 state subquery into a standalone CTE for efficient joining.
- W3: `aggressive_prefetch_single_pass` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-filter and pre-join date/store dimensions with fact data in chained CTEs, then consolidate aggregations into a single-pass operation before final rollup and window calculations.
- W4: `structural_transform_union_exists` (or_to_union, intersect_to_exists, deferred_window_aggregation)
  - Explore converting the IN subquery to UNION ALL branches, transform subquery patterns to EXISTS, and defer window ranking until after intermediate aggregation to reduce computation overhead.

---

### query_36 **[WIN]**

- **Baseline**: 879.2 ms (100 rows)
- **Best speedup**: 2.47x (W6, iter2)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.99x | NEUTRAL |  |
| W2 | 0 | 0.99x | NEUTRAL |  |
| W3 | 0 | 0.96x | NEUTRAL |  |
| W4 | 0 | 0.43x | REGRESSION |  |
| W5 | 1 | 1.05x | NEUTRAL |  |
| W6 | 2 | 2.47x | WIN | BEST |

**Strategies assigned:**

- W1: `conservative_predicate_pushdown` (early_filter, pushdown, date_cte_isolate)
  - Pre-filter dimension tables (date_dim, store, item) into separate CTEs before the main join to reduce fact table rows early. Push filters into CTEs and use explicit joins.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, materialize_cte, shared_dimension_multi_channel)
  - Isolate all dimension filters into individual CTEs, then join with fact table. Materialize grouped aggregations before window function to simplify the final query.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-filter and pre-join dimensions with the fact table in staged CTEs. Consider consolidating the ROLLUP aggregation into a single-pass CTE before applying the window function.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, union_cte_split)
  - Transform the IN-list filter on s_state into UNION ALL branches for parallel filtering. Explore restructuring the ROLLUP logic with set operations to optimize aggregation paths.

**Reanalyze insight**: All workers focused primarily on dimension table pre-filtering (CTE isolation) which only reduces join cardinality but ignores the fundamental bottleneck: the window function RANK() over partitions that depend on GROUPING() results forces a complete re-aggregation of all data after ROLLUP. DuckDB ca...

---

### query_14 **[WIN]**

- **Baseline**: 5289.8 ms (100 rows)
- **Best speedup**: 2.39x (W6, iter2)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.74x | IMPROVED |  |
| W2 | 0 | 1.62x | IMPROVED |  |
| W3 | 0 | 0.94x | REGRESSION |  |
| W4 | 0 | --- | ERROR | Binder Error |
| W5 | 1 | --- | ERROR | Binder Error |
| W6 | 2 | 2.39x | WIN | BEST |

**Strategies assigned:**

- W1: `conservative_pushdown_and_materialization` (pushdown, materialize_cte, early_filter)
  - Focus on pushing date/item filters deeper into CTEs and materializing repeated subqueries to avoid redundant computation. Keep structure similar but reduce intermediate result sizes through early filt
- W2: `moderate_date_dimension_isolation` (date_cte_isolate, shared_dimension_multi_channel, dimension_cte_isolate)
  - Isolate date filtering and dimension pre-filtering into reusable CTEs before joins. Share common dimension filters across sales channels to reduce redundant scanning.
- W3: `aggressive_consolidation_prefetch` (single_pass_aggregation, prefetch_fact_join, multi_dimension_prefetch)
  - Consolidate multiple sales table scans into single-pass CTEs with conditional aggregation. Pre-join filtered dimensions with fact data early to reduce join cardinality.
- W4: `novel_structural_transforms` (intersect_to_exists, decorrelate, or_to_union)
  - Transform INTERSECT operations to EXISTS/JOIN patterns for better optimizer planning. Decorrelate subqueries and convert OR conditions to UNION ALL for selective execution.

**Reanalyze insight**: The main bottleneck is the triple INTERSECT operation requiring three large fact table scans (store_sales, catalog_sales, web_sales) across overlapping 3-year windows, each with separate joins to date_dim. Worker1 and Worker2 showed moderate gains (1.74x, 1.62x) by pre-filtering date_dim but still p...

---

### query_35 **[WIN]**

- **Baseline**: 734.7 ms (100 rows)
- **Best speedup**: 2.36x (W3, iter0)
- **Exited at 2x gate** after iter0

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.07x | NEUTRAL |  |
| W2 | 0 | 1.07x | NEUTRAL |  |
| W3 | 0 | 2.36x | WIN | BEST |
| W4 | 0 | 1.14x | IMPROVED |  |

**Strategies assigned:**

- W1: `conservative_early_pushdown` (early_filter, pushdown, materialize_cte)
  - Filter dimension tables (customer_address, customer_demographics) first and push date filters into subqueries early to reduce join volumes, then materialize intermediate results.
- W2: `moderate_date_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, multi_date_range_cte)
  - Pre-filter date_dim (year=2001, qoy<4) into a shared CTE, then pre-filter customer dimensions into separate CTEs before joining with sales channels.
- W3: `aggressive_decorrelate_prefetch` (prefetch_fact_join, composite_decorrelate_union, single_pass_aggregation)
  - Decorrelate EXISTS subqueries by pre-joining each sales channel with filtered date_dim into CTEs, then union distinct customer keys before joining with main dimensions.
- W4: `novel_or_transform` (or_to_union, intersect_to_exists, shared_dimension_multi_channel)
  - Transform the OR between web_sales and catalog_sales into a UNION ALL of distinct customers, then use EXISTS with a shared date CTE; consider intersect semantics for multi-channel.

---

### query_99 **[WIN]**

- **Baseline**: 313.9 ms (100 rows)
- **Best speedup**: 2.33x (W6, iter2)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.99x | NEUTRAL |  |
| W2 | 0 | 1.04x | NEUTRAL |  |
| W3 | 0 | 1.11x | IMPROVED |  |
| W4 | 0 | 1.03x | NEUTRAL |  |
| W5 | 1 | 1.13x | IMPROVED |  |
| W6 | 2 | 2.33x | WIN | BEST |

**Strategies assigned:**

- W1: `conservative_filter_pushdown` (early_filter, pushdown, materialize_cte)
  - Apply safe, proven optimizations by pushing filters down to dimension tables early, materializing intermediate results, and reducing join sizes before aggregation.
- W2: `moderate_cte_isolation` (date_cte_isolate, dimension_cte_isolate, multi_dimension_prefetch)
  - Restructure with CTEs to isolate filtered dimension tables (especially date_dim) before joining, enabling better predicate pushdown and reducing fact table scans.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, shared_dimension_multi_channel, single_pass_aggregation)
  - Use advanced CTE restructuring to prefilter and pre-join dimensions with facts, share filtered dimensions across channels, and consolidate computations into single-pass scans.
- W4: `novel_structural_transform` (or_to_union, decorrelate, intersect_to_exists)
  - Explore structural transformations like splitting OR conditions, decorrelating subqueries, or converting set operations to exists—even if not directly present, adapt patterns to this query's logic.

**Reanalyze insight**: All workers fell short because they only applied incremental CTE-based restructurings without addressing the core bottleneck: the massive catalog_sales table scan and subsequent large join fanout. DuckDB's optimizer already pushes filters automatically, so isolated CTEs for dimension filtering (date...

---

### query_59 **[WIN]**

- **Baseline**: 1794.1 ms (100 rows)
- **Best speedup**: 2.23x (W3, iter0)
- **Exited at 2x gate** after iter0

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.11x | IMPROVED |  |
| W2 | 0 | 1.17x | IMPROVED |  |
| W3 | 0 | 2.23x | WIN | BEST |
| W4 | 0 | 1.20x | IMPROVED |  |

**Strategies assigned:**

- W1: `conservative_pushdown_filtering` (pushdown, early_filter, materialize_cte)
  - Apply safe optimizations by pushing filters into CTEs, materializing repeated patterns, and filtering dimension tables early to reduce join sizes.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Extract date and store dimension filtering into separate CTEs to enable better predicate pushdown and reduce redundant joins across channels.
- W3: `aggressive_single_pass_prefetch` (single_pass_aggregation, prefetch_fact_join, multi_dimension_prefetch)
  - Consolidate multiple scans into single-pass aggregation, pre-filter dimensions, and pre-join with facts to minimize intermediate results.
- W4: `novel_structural_transformation` (or_to_union, intersect_to_exists, deferred_window_aggregation)
  - Explore alternative join structures by converting OR conditions to UNION ALL, transforming intersect patterns to EXISTS, and deferring window computations.

---

### query_15 **[WIN]**

- **Baseline**: 92.1 ms (100 rows)
- **Best speedup**: 2.13x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.00x | NEUTRAL |  |
| W2 | 0 | 1.01x | NEUTRAL |  |
| W3 | 0 | --- | ERROR | Binder Error |
| W4 | 0 | 1.28x | IMPROVED |  |
| W5 | 1 | 2.13x | WIN | BEST |

**Strategies assigned:**

- W1: `conservative_filter_pushdown` (pushdown, early_filter, materialize_cte)
  - Apply safe filter pushdown and materialization strategies to reduce data early without restructuring joins; focus on minimizing risk while improving cardinality reduction.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, date_cte_isolate, multi_dimension_prefetch)
  - Pre-filter all dimension tables into separate CTEs before joining to the fact table; isolate date_dim first, then customer and address with their respective filters.
- W3: `aggressive_prefetch_restructuring` (prefetch_fact_join, multi_date_range_cte, single_pass_aggregation)
  - Restructure with CTE chains that pre-join filtered dimensions to the fact table in stages; consider consolidating operations into single passes where possible.
- W4: `novel_or_restructuring` (or_to_union, composite_decorrelate_union, intersect_to_exists)
  - Transform the complex OR condition using UNION ALL branches to enable better index usage; explore structural transformations of the predicate logic.

---

### query_93 **[WIN]**

- **Baseline**: 1051.2 ms (100 rows)
- **Best speedup**: 2.02x (W6, iter2)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.81x | REGRESSION |  |
| W2 | 0 | --- | ERROR | Binder Error |
| W3 | 0 | 0.77x | REGRESSION |  |
| W4 | 0 | 0.80x | REGRESSION |  |
| W5 | 1 | 0.80x | REGRESSION |  |
| W6 | 2 | 2.02x | WIN | BEST |

**Strategies assigned:**

- W1: `conservative_early_filter_restructure` (early_filter, pushdown, materialize_cte)
  - Filter the `reason` table first to reduce join volume, push filters into CTE, and materialize filtered dimension data early to optimize the left join pattern.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, date_cte_isolate, shared_dimension_multi_channel)
  - Isolate dimension table filtering (reason) into a dedicated CTE, then restructure the query to join this pre-filtered dimension with fact tables using explicit joins instead of cross joins.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-filter reason table, then pre-join with store_returns to create a filtered returns CTE before joining to store_sales in a single aggregation pass, eliminating cross join inefficiencies.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Transform the CASE logic and cross join into a UNION ALL structure separating returned vs non-returned sales, or explore decorrelation techniques to restructure the dimension join pattern.

**Reanalyze insight**: All workers fell short because they fundamentally misdiagnosed the query's bottleneck. The original query contains an implicit cross join between store_sales and reason before filtering via sr_reason_sk = r_reason_sk, creating an explosion of intermediate rows. Workers 1, 3, 4, and 5 incorrectly tra...

---

### query_87 [IMPROVED]

- **Baseline**: 928.4 ms (1 rows)
- **Best speedup**: 1.91x (W6, iter2)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.96x | NEUTRAL |  |
| W2 | 0 | 0.84x | REGRESSION |  |
| W3 | 0 | 0.93x | REGRESSION |  |
| W4 | 0 | 0.79x | REGRESSION |  |
| W5 | 1 | 0.89x | REGRESSION |  |
| W6 | 2 | 1.91x | IMPROVED | BEST |

**Strategies assigned:**

- W1: `conservative_pushdown_prefilter` (pushdown, early_filter, dimension_cte_isolate)
  - Push filters into subqueries and pre-filter dimension tables before joining to reduce data movement and avoid redundant scans.
- W2: `moderate_date_shared_dimension_cte` (date_cte_isolate, shared_dimension_multi_channel, materialize_cte)
  - Isolate date filtering into a CTE, reuse shared dimension filters across channels, and materialize repeated join patterns to minimize recomputation.
- W3: `aggressive_prefetch_single_pass` (single_pass_aggregation, prefetch_fact_join, multi_dimension_prefetch)
  - Prefetch filtered fact-dimension joins into CTEs, then consolidate the three sales-channel queries into a single CTE using conditional aggregation to scan fact tables once.
- W4: `novel_set_transform` (intersect_to_exists, or_to_union, composite_decorrelate_union)
  - Transform set operations (EXCEPT) into EXISTS/NOT EXISTS or UNION-based patterns, and decorrelate subqueries to enable more efficient join plans.

**Reanalyze insight**: All attempts failed because they continued to compute three separate channel-specific result sets then perform set operations, which is inherently expensive. Worker 1-3 and 5 used the same basic pattern of CTEs with DISTINCT followed by EXCEPT, failing to fundamentally change the computation pattern...

---

### query_80 [IMPROVED]

- **Baseline**: 683.8 ms (100 rows)
- **Best speedup**: 1.91x (W2, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.07x | NEUTRAL |  |
| W2 | 0 | 1.91x | IMPROVED | BEST |
| W3 | 0 | 1.86x | IMPROVED |  |
| W4 | 0 | 1.82x | IMPROVED |  |
| W5 | 1 | 1.01x | NEUTRAL |  |
| W6 | 2 | 1.38x | IMPROVED |  |

**Strategies assigned:**

- W1: `conservative_pushdown_earlyfilter` (pushdown, early_filter, materialize_cte)
  - Focus on pushing filters into CTEs early, materializing repeated dimension lookups, and reducing intermediate result sizes through standard predicate pushdown.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Extract date and dimension filters into separate CTEs before joining with fact tables, enabling predicate pushdown and reducing redundant joins across channels.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-filter and pre-join dimensions with fact tables in separate CTEs, then consider consolidating multi-channel aggregations into a single scan for radical join reduction.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Explore structural query transformations like converting OR conditions to UNION ALL, rewriting subquery patterns, and decorrelating nested aggregations for alternative execution plans.

**Reanalyze insight**: All attempts relied on standard dimension pre-filtering (date, item, promotion) into CTEs—which helps but isn't enough. The fundamental bottleneck is the massive LEFT OUTER JOIN between each large fact table (store_sales, catalog_sales, web_sales) and its corresponding returns table, performed BEFOR...

---

### query_65 [IMPROVED]

- **Baseline**: 1402.5 ms (100 rows)
- **Best speedup**: 1.84x (W3, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.79x | IMPROVED |  |
| W2 | 0 | 1.80x | IMPROVED |  |
| W3 | 0 | 1.84x | IMPROVED | BEST |
| W4 | 0 | 1.79x | IMPROVED |  |
| W5 | 1 | 1.73x | IMPROVED |  |
| W6 | 2 | 1.74x | IMPROVED |  |

**Strategies assigned:**

- W1: `conservative_pushdown_materialization` (pushdown, early_filter, materialize_cte)
  - Push filters early into CTEs, materialize repeated subquery patterns once, and reduce intermediate dataset sizes through predicate pushdown.
- W2: `moderate_dimension_date_isolation` (date_cte_isolate, dimension_cte_isolate, multi_date_range_cte)
  - Extract date and dimension filtering into isolated CTEs before joining with fact tables to enable better filter propagation and reduce join cardinality.
- W3: `aggressive_prefetch_restructuring` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-join filtered dimensions with fact tables in CTEs, consolidate multiple aggregations into single-pass scans, and prefetch multiple dimension filters to minimize repeated work.
- W4: `novel_structural_transformation` (or_to_union, intersect_to_exists, decorrelate)
  - Transform query structure by splitting OR conditions into UNION ALL, converting intersect patterns to EXISTS, and decorrelating subqueries into join-friendly CTEs.

**Reanalyze insight**: All attempts focused on basic CTE restructuring, filter pushdown, and window function consolidation but missed deeper optimizations. The bottleneck remains the double aggregation pattern (store-item revenue + store average) requiring two full passes over the filtered sales data. Worker 5's window fu...

---

### query_32 [IMPROVED]

- **Baseline**: 13.6 ms (1 rows)
- **Best speedup**: 1.82x (W6, iter2)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.67x | REGRESSION |  |
| W2 | 0 | 0.61x | REGRESSION |  |
| W3 | 0 | 0.44x | REGRESSION |  |
| W4 | 0 | --- | ERROR | Binder Error |
| W5 | 1 | --- | ERROR | Binder Error |
| W6 | 2 | 1.82x | IMPROVED | BEST |

**Strategies assigned:**

- W1: `conservative_predicate_pushdown` (early_filter, pushdown, materialize_cte)
  - Apply early filtering to dimension tables (item, date_dim) before joining with catalog_sales, push predicates into subqueries, and materialize the repeated subquery pattern to avoid redundant computat
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, date_cte_isolate, shared_dimension_multi_channel)
  - Isolate filtered dimension tables into separate CTEs to reduce join cardinality early, especially for date ranges, and share pre-filtered dimensions across main and subquery.
- W3: `aggressive_prefetch_restructuring` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-filter and pre-join dimensions with fact tables in staged CTEs, then restructure the correlated subquery into a single-pass aggregation to eliminate per-row execution.
- W4: `novel_correlation_transform` (decorrelate, composite_decorrelate_union, intersect_to_exists)
  - Decouple the correlated subquery via GROUP BY CTE and JOIN, transform conditional logic using set operations, and explore EXISTS semantics for alternative execution plans.

**Reanalyze insight**: All attempts fell short because they focused on CTE-based decorrelation without addressing the fundamental bottleneck: the query performs a self-join on catalog_sales with different filtering conditions. Worker 1-3 achieved only 0.44-0.67x because they still compute item averages for ALL items, then...

---

### query_92 [IMPROVED]

- **Baseline**: 70.7 ms (1 rows)
- **Best speedup**: 1.81x (W6, iter2)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.94x | REGRESSION |  |
| W2 | 0 | 0.91x | REGRESSION |  |
| W3 | 0 | 0.87x | REGRESSION |  |
| W4 | 0 | 0.81x | REGRESSION |  |
| W5 | 1 | --- | ERROR | Binder Error |
| W6 | 2 | 1.81x | IMPROVED | BEST |

**Strategies assigned:**

- W1: `conservative_early_pushdown` (early_filter, pushdown, materialize_cte)
  - Filter dimension tables first, push date predicates into subqueries, and materialize the repeated correlated subquery logic into a CTE to avoid redundant computation.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, decorrelate)
  - Create separate CTEs for pre-filtered date and item dimensions, then decorrelate the subquery by precomputing per-item averages for the date range before joining.
- W3: `aggressive_prefetch_restructure` (multi_date_range_cte, prefetch_fact_join, shared_dimension_multi_channel)
  - Precompute the date range as a CTE, prefetch filtered fact-dimension joins, and restructure using shared dimension CTEs to streamline the multi-step filtering and aggregation.
- W4: `novel_structural_transform` (single_pass_aggregation, composite_decorrelate_union, or_to_union)
  - Consolidate the main query and subquery into a single-pass aggregation, transform correlation via union-based decorrelation, and explore splitting any implicit OR conditions for optimal indexing.

**Reanalyze insight**: All previous attempts fell short because they over-relied on CTE materialization without addressing the core bottleneck: the correlated subquery's per-item aggregation forces repeated scans of the same date-filtered web_sales data. While decorrelation via CTEs eliminates repeated execution, it still...

---

### query_94 [IMPROVED]

- **Baseline**: 90.4 ms (1 rows)
- **Best speedup**: 1.74x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.04x | NEUTRAL |  |
| W2 | 0 | 0.92x | REGRESSION |  |
| W3 | 0 | 1.00x | NEUTRAL |  |
| W4 | 0 | 0.37x | REGRESSION |  |
| W5 | 1 | 1.74x | IMPROVED | BEST |
| W6 | 2 | 0.70x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_early_filtering` (early_filter, pushdown, materialize_cte)
  - Apply simple, safe optimizations by filtering dimension tables first, pushing predicates into joins, and materializing reusable subquery results.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate dimension table filtering into separate CTEs before joining with the fact table, enabling better predicate pushdown and reuse.
- W3: `aggressive_multi_cte_restructuring` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Restructure the query using multiple CTEs to pre-filter and pre-join dimensions with facts, then consolidate aggregations into a single pass.
- W4: `novel_structural_transforms` (decorrelate, intersect_to_exists, composite_decorrelate_union)
  - Transform correlated subqueries into independent CTEs with distinct keys, convert INTERSECT patterns to EXISTS, and decorrelate multiple subqueries simultaneously.

**Reanalyze insight**: All attempts failed to address the core bottleneck: correlated subqueries scanning the massive web_sales table multiple times. Worker 1-3 preserved the original EXISTS/NOT EXISTS patterns, suffering repeated full scans. Worker 4 attempted decorrelation but incorrectly applied dimension filters to th...

---

### query_64 [IMPROVED]

- **Baseline**: 1419.9 ms (3 rows)
- **Best speedup**: 1.65x (W6, iter2)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.99x | NEUTRAL |  |
| W2 | 0 | 0.97x | FAIL | row mismatch |
| W3 | 0 | 1.24x | IMPROVED |  |
| W4 | 0 | 0.37x | REGRESSION |  |
| W5 | 1 | 1.11x | IMPROVED |  |
| W6 | 2 | 1.65x | IMPROVED | BEST |

**Strategies assigned:**

- W1: `conservative_filter_pushdown` (early_filter, pushdown, materialize_cte)
  - Apply predicate pushdown to reduce intermediate result sizes, filter dimension tables early, and reuse computed CTEs to avoid redundant calculations.
- W2: `moderate_date_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate date and dimension filters into separate CTEs to enable better predicate pushdown and reduce fact table scans, especially for shared dimensions.
- W3: `aggressive_multi_cte_prefetch` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Restructure joins by prefetching filtered dimensions and pre-joining with facts in separate CTEs, and consolidate aggregations where possible.
- W4: `novel_structural_transforms` (intersect_to_exists, or_to_union, decorrelate)
  - Transform query structure by converting patterns (like INTERSECT) to EXISTS, splitting OR conditions into UNION ALL, and decorrelating subqueries for better join planning.

**Reanalyze insight**: The attempts fell short due to superficial application of optimization patterns without addressing the core computational bottlenecks. Worker 1's conservative pushdown missed crucial early filtering of large fact tables. Worker 2 incorrectly filtered date dimensions for d2/d3 when they shouldn't be ...

---

### query_90 [IMPROVED]

- **Baseline**: 75.2 ms (1 rows)
- **Best speedup**: 1.64x (W6, iter2)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.98x | NEUTRAL |  |
| W2 | 0 | 1.02x | NEUTRAL |  |
| W3 | 0 | 0.07x | REGRESSION |  |
| W4 | 0 | --- | ERROR | Binder Error |
| W5 | 1 | 0.11x | REGRESSION |  |
| W6 | 2 | 1.64x | IMPROVED | BEST |

**Strategies assigned:**

- W1: `conservative_filter_pushdown` (early_filter, pushdown, materialize_cte)
  - First filter dimension tables aggressively before joining to fact tables, then push predicates into subqueries, and use CTE to avoid repeating the same filter logic.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, date_cte_isolate, shared_dimension_multi_channel)
  - Create separate CTEs for pre-filtered dimension tables (household_demographics, time_dim, web_page) to enable better predicate pushdown and reuse them across both subqueries.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, single_pass_aggregation, multi_date_range_cte)
  - Pre-join filtered dimensions with fact table in CTEs, then restructure to compute both hour ranges in a single scan using conditional aggregation, eliminating duplicate subquery pattern.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Transform the implicit cross join between subqueries using UNION ALL with case statements, or restructure as EXISTS patterns to enable different join strategies and reduce intermediate results.

**Reanalyze insight**: All attempts fell short due to fundamental execution plan miscalculations. Worker 1-2 maintained the original two-pass structure, missing opportunities for single-scan optimization. Worker 3-5 attempted single-pass approaches but introduced catastrophic Cartesian products via CROSS JOINs between dim...

---

### query_1 [IMPROVED]

- **Baseline**: 107.3 ms (100 rows)
- **Best speedup**: 1.62x (W3, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.01x | NEUTRAL |  |
| W2 | 0 | 1.50x | IMPROVED |  |
| W3 | 0 | 1.62x | IMPROVED | BEST |
| W4 | 0 | 1.01x | NEUTRAL |  |
| W5 | 1 | 1.05x | NEUTRAL |  |
| W6 | 2 | 0.58x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_predicate_pushdown` (pushdown, early_filter, materialize_cte)
  - Push filters early into CTEs and materialize reusable aggregates to reduce intermediate data volume before joins.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Pre-filter date_dim and store dimension into separate CTEs before the main aggregation to enable better predicate pushdown.
- W3: `aggressive_multi_cte_prefetch` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-join filtered dimensions with fact tables early, then compute both base aggregates and store-level averages in a single pass.
- W4: `novel_correlation_elimination` (decorrelate, or_to_union, intersect_to_exists)
  - Eliminate the correlated subquery by precomputing store averages, and explore alternative joins using UNION branches for state filtering.

**Reanalyze insight**: The primary bottleneck is that all attempts failed to fundamentally change the join pattern between store_returns and date_dim. The store_returns table likely has billions of rows, and the current strategies still require scanning the entire fact table. Worker 3 achieved the best speedup (1.62x) by ...

---

### query_62 [IMPROVED]

- **Baseline**: 199.2 ms (100 rows)
- **Best speedup**: 1.62x (W6, iter2)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.04x | NEUTRAL |  |
| W2 | 0 | 1.02x | NEUTRAL |  |
| W3 | 0 | 1.07x | NEUTRAL |  |
| W4 | 0 | 0.98x | NEUTRAL |  |
| W5 | 1 | 1.03x | NEUTRAL |  |
| W6 | 2 | 1.62x | IMPROVED | BEST |

**Strategies assigned:**

- W1: `conservative_early_filtering` (early_filter, pushdown, materialize_cte)
  - Apply dimension table filtering first, then join to fact tables to reduce data volume early. Use CTEs to materialize filtered dimensions and push filters into subqueries.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate date and dimension filtering into separate CTEs before joining. This enables better predicate pushdown and reduces intermediate result sizes during joins.
- W3: `aggressive_multi_cte_restructure` (multi_dimension_prefetch, prefetch_fact_join, deferred_window_aggregation)
  - Pre-filter ALL dimension tables into CTEs, then pre-join with the fact table in stages. Use multi-step CTE chains to control join order and materialization.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Explore radical query restructuring by converting conditional logic to UNION ALL branches, transforming set operations, and decorrelating complex patterns even if not directly present.

**Reanalyze insight**: All workers fell short because they focused on dimension table pre-filtering using CTEs, which DuckDB's optimizer already handles automatically through predicate pushdown and join reordering. The core bottleneck remains unchanged: scanning and joining the massive web_sales fact table with date_dim. ...

---

### query_4 [IMPROVED]

- **Baseline**: 4964.5 ms (100 rows)
- **Best speedup**: 1.61x (W6, iter2)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.54x | IMPROVED |  |
| W2 | 0 | 0.97x | NEUTRAL |  |
| W3 | 0 | 1.24x | IMPROVED |  |
| W4 | 0 | --- | ERROR | Binder Error |
| W5 | 1 | 1.24x | IMPROVED |  |
| W6 | 2 | 1.61x | IMPROVED | BEST |

**Strategies assigned:**

- W1: `conservative_predicate_pushdown` (pushdown, early_filter, dimension_cte_isolate)
  - Push filters into CTEs early, filter dimension tables before joins, and isolate customer dimension prefetching to reduce row counts in all union branches.
- W2: `moderate_date_restructuring` (date_cte_isolate, union_cte_split, shared_dimension_multi_channel)
  - Isolate date filtering into a dedicated CTE, split the UNION ALL by sale_type, and share filtered date_dim across all three sales channels to eliminate redundant filtering.
- W3: `aggressive_multi_cte_prefetch` (multi_date_range_cte, multi_dimension_prefetch, prefetch_fact_join)
  - Create separate prefiltered CTEs for each date range (1999, 2000) and each dimension, then pre-join dimensions with facts before the main union to reduce join cardinality.
- W4: `novel_structural_transformation` (intersect_to_exists, or_to_union, single_pass_aggregation)
  - Transform the self-join pattern to use EXISTS, restructure OR-like conditions in final filters to UNION ALL branches, and attempt to consolidate the three sales channel aggregations into a single pass

**Reanalyze insight**: All workers attempted predicate pushdown and CTE isolation but missed the core bottleneck: the query requires 6 self-joins on year_total (3 channels × 2 years) with complex ratio comparisons. Worker 1's modest gains (1.54x) came from reducing initial row counts but still performed massive self-joins...

---

### query_27 [IMPROVED]

- **Baseline**: 701.0 ms (100 rows)
- **Best speedup**: 1.58x (W2, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.51x | IMPROVED |  |
| W2 | 0 | 1.58x | IMPROVED | BEST |
| W3 | 0 | 1.56x | IMPROVED |  |
| W4 | 0 | --- | ERROR | Binder Error |
| W5 | 1 | --- | ERROR | Binder Error |
| W6 | 2 | --- | ERROR | Binder Error |

**Strategies assigned:**

- W1: `conservative_early_pushdown` (early_filter, pushdown, dimension_cte_isolate)
  - Apply safe, proven optimizations: pre-filter all dimension tables into CTEs before joining to dramatically reduce fact table joins, and push all eligible predicates down into these CTEs.
- W2: `moderate_date_dimension_isolation` (date_cte_isolate, multi_dimension_prefetch, shared_dimension_multi_channel)
  - Isolate date and other dimension filters into separate CTEs to maximize predicate pushdown, then strategically prefilter the fact table by joining with the most selective dimension CTEs first.
- W3: `aggressive_multi_cte_restructure` (prefetch_fact_join, single_pass_aggregation, materialize_cte)
  - Restructure with aggressive CTE chaining: pre-join filtered dimensions with the fact table in stages, and consolidate operations to minimize full table scans while materializing intermediate results.
- W4: `novel_structural_transforms` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Apply advanced structural transformations: convert the IN-list filter to UNION ALL branches for potential index benefits, and explore EXISTS transformations even if not present, using these patterns a

**Reanalyze insight**: All previous attempts fell short of 2.0x because they failed to address the fundamental bottleneck: the massive store_sales fact table join and aggregation. While CTE-based dimension filtering (1.51-1.58x) reduced join cardinality, they still required a full scan and expensive hash aggregation on th...

---

### query_75 [IMPROVED]

- **Baseline**: 1263.2 ms (100 rows)
- **Best speedup**: 1.58x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.12x | IMPROVED |  |
| W2 | 0 | 1.49x | IMPROVED |  |
| W3 | 0 | 1.40x | IMPROVED |  |
| W4 | 0 | 1.40x | IMPROVED |  |
| W5 | 1 | 1.58x | IMPROVED | BEST |
| W6 | 2 | 0.93x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_pushdown_early_filter` (early_filter, pushdown, materialize_cte)
  - Push filters into the UNION subqueries early, materialize the CTE to avoid re-scanning, and apply predicate pushdown to reduce intermediate data volume.
- W2: `moderate_date_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate date and item dimension filtering into separate CTEs before joining with fact tables, enabling predicate pushdown and shared dimension reuse across all sales channels.
- W3: `aggressive_union_restructure` (union_cte_split, single_pass_aggregation, prefetch_fact_join)
  - Split the UNION into separate CTEs per sales channel, pre-join filtered dimensions with fact tables, and consider consolidating aggregations into a single pass where possible.
- W4: `novel_structural_transform` (intersect_to_exists, decorrelate, or_to_union)
  - Transform the self-join pattern using EXISTS logic, decorrelate the year comparison, and restructure ratio conditions using UNION ALL branches for better join planning.

**Reanalyze insight**: All attempts focused on dimension filtering and UNION restructuring but missed the core bottleneck: excessive LEFT JOIN operations on large returns tables across all channels. Worker 3's pre-aggregation approach showed promise (1.49x) but still performed three separate joins to returns tables. The r...

---

### query_18 [IMPROVED]

- **Baseline**: 302.5 ms (100 rows)
- **Best speedup**: 1.57x (W2, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.06x | NEUTRAL |  |
| W2 | 0 | 1.57x | IMPROVED | BEST |
| W3 | 0 | --- | ERROR | Binder Error |
| W4 | 0 | 0.82x | REGRESSION |  |
| W5 | 1 | 1.00x | NEUTRAL |  |
| W6 | 2 | 1.04x | NEUTRAL |  |

**Strategies assigned:**

- W1: `conservative_early_filtering` (early_filter, pushdown, materialize_cte)
  - Apply aggressive early filtering on dimension tables before joins, push predicates into base tables, and materialize reusable filtered subsets to reduce join cardinality.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, date_cte_isolate, shared_dimension_multi_channel)
  - Pre-filter all dimension tables into isolated CTEs before joining with the fact table, ensuring predicate pushdown and reuse of filtered dimension sets.
- W3: `aggressive_prefetch_restructuring` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-join filtered dimensions with the fact table in CTEs, prefetch multiple dimension subsets, and consolidate aggregations into single-pass computations.
- W4: `novel_structural_transforms` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Transform OR/IN conditions into UNION ALL branches, rewrite intersection patterns as EXISTS, and decorrelate nested patterns through distinct pre-materialization.

**Reanalyze insight**: All workers fell short because they focused primarily on dimension table filtering (early_filter, dimension_cte_isolate) but didn't adequately reduce the massive fact table (catalog_sales) early enough. Worker 2 achieved the best speedup (1.57x) by isolating dimension filters, but still required joi...

---

### query_67 [IMPROVED]

- **Baseline**: 10269.0 ms (100 rows)
- **Best speedup**: 1.50x (W6, iter2)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.03x | NEUTRAL |  |
| W2 | 0 | 1.03x | NEUTRAL |  |
| W3 | 0 | 0.99x | NEUTRAL |  |
| W4 | 0 | --- | ERROR | Binder Error |
| W5 | 1 | --- | ERROR | Binder Error |
| W6 | 2 | 1.50x | IMPROVED | BEST |

**Strategies assigned:**

- W1: `conservative_filter_pushdown` (early_filter, pushdown, materialize_cte)
  - Apply early filtering to dimension tables (date_dim, item, store) before joining, push predicates into subqueries, and materialize repeated expressions.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, prefetch_fact_join)
  - Isolate filtered dimensions into separate CTEs (date range, item categories) before joining with fact table, enabling better predicate pushdown and join planning.
- W3: `aggressive_cte_restructuring` (multi_date_range_cte, multi_dimension_prefetch, deferred_window_aggregation)
  - Restructure with multiple CTEs to pre-filter and pre-join dimensions, defer window function computation until after aggregation to reduce data volume.
- W4: `novel_structural_transforms` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Explore radical transformations: split rollup into UNION ALL branches, convert implicit groupings to explicit structures, and decorrelate nested aggregations.

**Reanalyze insight**: All attempts fell short due to three core issues: (1) Insufficient reduction of the massive fact table early enough (all workers kept the entire store_sales join), (2) Preserving the expensive ROLLUP operation that generates 2^8=256 grouping combinations (including null placeholders), and (3) Attemp...

---

### query_57 [IMPROVED]

- **Baseline**: 663.2 ms (100 rows)
- **Best speedup**: 1.49x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.16x | IMPROVED |  |
| W2 | 0 | 1.10x | IMPROVED |  |
| W3 | 0 | 1.04x | NEUTRAL |  |
| W4 | 0 | 0.72x | REGRESSION |  |
| W5 | 1 | 1.49x | IMPROVED | BEST |
| W6 | 2 | 1.26x | IMPROVED |  |

**Strategies assigned:**

- W1: `conservative_filter_pushdown` (early_filter, pushdown, materialize_cte)
  - Apply early filtering to dimension tables and push predicates into CTEs to reduce intermediate data volume before expensive joins and window functions.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Pre-filter each dimension table (date_dim, item, call_center) into separate CTEs before joining with the fact table, enabling better predicate pushdown and join optimization.
- W3: `aggressive_prefetch_restructuring` (prefetch_fact_join, multi_dimension_prefetch, deferred_window_aggregation)
  - Restructure the query to pre-join filtered dimensions with the fact table in a single pass, then compute window functions on the reduced dataset, avoiding repeated self-joins.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, single_pass_aggregation)
  - Transform the OR date filter into UNION ALL branches for better partition pruning, and explore consolidating the self-join logic into a single-pass LAG/LEAD window function.

**Reanalyze insight**: All attempts fell short because they addressed peripheral optimizations without tackling the core bottleneck: the triangular self-join (v1 × v1_lag × v1_lead) that creates O(n²) complexity. Worker 1-3 merely shuffled filter pushdown strategies but kept the expensive self-join. Worker 4's UNION ALL i...

---

### query_38 [IMPROVED]

- **Baseline**: 918.3 ms (1 rows)
- **Best speedup**: 1.43x (W6, iter2)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.02x | NEUTRAL |  |
| W2 | 0 | 1.04x | NEUTRAL |  |
| W3 | 0 | 0.98x | NEUTRAL |  |
| W4 | 0 | 0.91x | REGRESSION |  |
| W5 | 1 | 0.97x | NEUTRAL |  |
| W6 | 2 | 1.43x | IMPROVED | BEST |

**Strategies assigned:**

- W1: `conservative_pushdown_filter` (pushdown, early_filter, materialize_cte)
  - Apply safe, proven optimizations: push filters into subqueries, filter dimension tables before joining, and materialize repeated date_dim logic into a CTE.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate filtered dimension tables (date_dim, customer) into CTEs to enable reuse across sales channels and reduce redundant scans.
- W3: `aggressive_prefetch_restructure` (multi_dimension_prefetch, prefetch_fact_join, single_pass_aggregation)
  - Restructure the query into a multi‑CTE pipeline that prefilters dimensions, pre‑joins with fact tables, and consolidates the three sales channels into a single aggregation pass.
- W4: `novel_intersect_transform` (intersect_to_exists, or_to_union, composite_decorrelate_union)
  - Transform the INTERSECT set operation into EXISTS/join logic, explore splitting OR conditions on sales channels, and decorrelate the union of distinct customer‑date tuples.

**Reanalyze insight**: All attempts failed because they merely restructured the query into CTEs without addressing the fundamental performance bottlenecks. The conservative approaches (Workers 1-3,5) only isolated dimension filters but preserved the expensive three-way INTERSECT of large DISTINCT results, which requires c...

---

### query_41 [IMPROVED]

- **Baseline**: 15.8 ms (16 rows)
- **Best speedup**: 1.43x (W3, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.09x | NEUTRAL |  |
| W2 | 0 | 1.28x | IMPROVED |  |
| W3 | 0 | 1.43x | IMPROVED | BEST |
| W4 | 0 | 0.35x | REGRESSION |  |
| W5 | 1 | 1.22x | IMPROVED |  |
| W6 | 2 | 1.62x | FAIL | row mismatch |

**Strategies assigned:**

- W1: `conservative_early_filtering` (early_filter, pushdown, materialize_cte)
  - Apply basic filtering optimization by pushing conditions into CTEs and materializing reusable subqueries to reduce row processing early.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, shared_dimension_multi_channel, multi_dimension_prefetch)
  - Isolate and pre-filter the dimension table (item) into CTEs before the main correlation, enabling better predicate pushdown and reducing correlated subquery complexity.
- W3: `aggressive_correlation_elimination` (decorrelate, single_pass_aggregation, composite_decorrelate_union)
  - Eliminate the correlated subquery entirely by converting it to joins with pre-aggregated CTEs, enabling single-pass processing of the item table.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, union_cte_split)
  - Transform the complex OR conditions into UNION ALL branches for better index usage and restructure the existence check using alternative patterns.

**Reanalyze insight**: All workers attempted decorrelation but fell short due to three key issues:
1. **Inefficient manufacturer filtering**: Workers 1-3 and 5 still scan the entire item table for qualifying manufacturers, missing the opportunity to leverage the i_manufact_id range constraint to drastically reduce the sea...

---

### query_86 [IMPROVED]

- **Baseline**: 131.7 ms (100 rows)
- **Best speedup**: 1.39x (W6, iter2)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.00x | NEUTRAL |  |
| W2 | 0 | 0.95x | NEUTRAL |  |
| W3 | 0 | 1.08x | NEUTRAL |  |
| W4 | 0 | 1.03x | NEUTRAL |  |
| W5 | 1 | 1.28x | IMPROVED |  |
| W6 | 2 | 1.39x | IMPROVED | BEST |

**Strategies assigned:**

- W1: `conservative_pushdown_filter` (early_filter, pushdown, materialize_cte)
  - Apply safe, proven optimizations by isolating filters early, pushing predicates down, and materializing reusable results to reduce join sizes and avoid recomputation.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, multi_date_range_cte)
  - Pre-filter dimension tables into separate CTEs to enable predicate pushdown and reduce fact table joins, focusing on date and item dimension isolation.
- W3: `aggressive_prefetch_restructure` (multi_dimension_prefetch, prefetch_fact_join, single_pass_aggregation)
  - Restructure joins using multi-CTE prefetching, pre-joining filtered dimensions with facts, and consolidating aggregations into a single scan to minimize intermediate data.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Explore structural query transformations by converting OR conditions to UNION ALL, rewriting INTERSECT patterns, and decorrelating complex subqueries for alternative join plans.

**Reanalyze insight**: All attempts fell short because they focused on predicate pushdown and join restructuring—optimizations DuckDB's query planner already handles automatically. The attempts failed to address the true bottlenecks: 1) The expensive ROLLUP operation computing hierarchical aggregations, 2) The window func...

---

### query_5 [IMPROVED]

- **Baseline**: 486.5 ms (100 rows)
- **Best speedup**: 1.36x (W2, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | --- | ERROR | Binder Error |
| W2 | 0 | 1.36x | IMPROVED | BEST |
| W3 | 0 | 1.07x | NEUTRAL |  |
| W4 | 0 | 1.02x | NEUTRAL |  |
| W5 | 1 | 1.15x | IMPROVED |  |
| W6 | 2 | 0.90x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_early_filter_pushdown` (early_filter, pushdown, materialize_cte)
  - Apply early filtering and predicate pushdown aggressively in CTEs to reduce data volume, and materialize reusable date filters.
- W2: `moderate_date_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate date and dimension filtering into separate CTEs to enable reuse across channels and improve predicate pushdown.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, single_pass_aggregation, multi_dimension_prefetch)
  - Pre-filter and pre-join dimension tables with fact tables, then restructure the union to a single-pass aggregation where possible.
- W4: `novel_union_transform` (union_cte_split, or_to_union, composite_decorrelate_union)
  - Split the UNION ALL into specialized CTEs, transform implicit OR conditions to UNION ALL, and decorrelate any nested patterns.

**Reanalyze insight**: All attempts fell short because they focused exclusively on dimension table pre-filtering (date_dim) but missed the true bottleneck: massive data shuffling in UNION ALL operations with zero-column filling patterns. Worker 1 had a syntax error, but Workers 2-5 showed diminishing returns (1.36x→1.07x→...

---

### query_56 [IMPROVED]

- **Baseline**: 252.1 ms (100 rows)
- **Best speedup**: 1.35x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.19x | IMPROVED |  |
| W2 | 0 | 1.11x | IMPROVED |  |
| W3 | 0 | 1.18x | IMPROVED |  |
| W4 | 0 | 1.11x | IMPROVED |  |
| W5 | 1 | 1.35x | IMPROVED | BEST |
| W6 | 2 | --- | ERROR | Binder Error |

**Strategies assigned:**

- W1: `conservative_pushdown` (pushdown, early_filter, materialize_cte)
  - Apply safe, proven optimizations by pushing filters into CTEs, filtering dimensions before joins, and materializing the repeated item subquery to avoid recomputation.
- W2: `dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate filtered dimension tables (date, address, item) into reusable CTEs before joining with each fact table, enabling better predicate pushdown.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Restructure by pre-joining filtered dimensions with facts in dedicated CTEs, then attempt to consolidate the three channel aggregations into a single pass where possible.
- W4: `structural_transform` (or_to_union, intersect_to_exists, decorrelate)
  - Transform query structure by splitting OR conditions (color filter) into UNION branches, converting subquery patterns, and decorrelating where applicable for alternative execution paths.

**Reanalyze insight**: All previous attempts fell short because they focused solely on dimension isolation through CTEs, which DuckDB's optimizer already handles automatically via predicate pushdown. The real bottleneck is the massive fact table scans (store_sales, catalog_sales, web_sales) and the subsequent hash joins w...

---

### query_61 [IMPROVED]

- **Baseline**: 10.8 ms (1 rows)
- **Best speedup**: 1.34x (W3, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.54x | REGRESSION |  |
| W2 | 0 | 0.02x | REGRESSION |  |
| W3 | 0 | 1.34x | IMPROVED | BEST |
| W4 | 0 | 0.59x | REGRESSION |  |
| W5 | 1 | --- | ERROR | Binder Error |
| W6 | 2 | 0.94x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_pushdown_filtering` (pushdown, early_filter, materialize_cte)
  - Focus on basic filter pushdown into subqueries, early dimension table filtering, and simple CTE materialization to reduce intermediate data volumes safely.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, date_cte_isolate, shared_dimension_multi_channel)
  - Pre-filter all dimension tables into separate CTEs before joining with fact tables, isolating date filtering and sharing dimension filters between the two subqueries.
- W3: `aggressive_prefetch_consolidation` (prefetch_fact_join, single_pass_aggregation, multi_dimension_prefetch)
  - Pre-filter dimensions and pre-join with facts in CTEs, then consolidate both promotional and total calculations into a single CTE with conditional aggregation to eliminate duplicate scans.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Transform OR conditions on promotion channels into UNION ALL branches, restructure subqueries as EXISTS patterns, and decorrelate any implicit dependencies between promotional and total logic.

**Reanalyze insight**: Worker 1-4 achieved only modest speedups (0.02x-1.34x) because they focused on dimension pre-filtering but missed the critical bottleneck: dual full scans of the massive store_sales fact table. Worker 3's "single-pass aggregation" (1.34x) showed promise but failed to reach 2.0x due to suboptimal joi...

---

### query_45 [IMPROVED]

- **Baseline**: 105.3 ms (36 rows)
- **Best speedup**: 1.34x (W6, iter2)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.97x | NEUTRAL |  |
| W2 | 0 | 1.22x | IMPROVED |  |
| W3 | 0 | 0.76x | REGRESSION |  |
| W4 | 0 | 0.84x | REGRESSION |  |
| W5 | 1 | --- | ERROR | Binder Error |
| W6 | 2 | 1.34x | IMPROVED | BEST |

**Strategies assigned:**

- W1: `conservative_pushdown_earlyfilter` (early_filter, pushdown, materialize_cte)
  - Apply early filtering to dimension tables before joining, push predicates into CTEs, and materialize the item subquery to avoid repeated execution.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate filtered dimension tables (date_dim, customer_address, item) into separate CTEs before joining with web_sales, enabling better predicate pushdown.
- W3: `aggressive_prefetch_restructuring` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-filter dimensions into CTEs, then pre-join with the fact table in stages; consolidate filtering logic to minimize intermediate result sizes.
- W4: `novel_or_transform_decorrelate` (or_to_union, decorrelate, intersect_to_exists)
  - Split the OR condition into UNION ALL branches for better index usage, decorrelate the item subquery, and explore EXISTS transformations for the IN clause.

**Reanalyze insight**: All attempts failed to achieve 2.0x because they focused on minor optimizations while missing the fundamental bottleneck: the OR condition combines two weakly-correlated filters (zip codes vs item IDs) forcing DuckDB to process massive intermediate results. Worker 1's CTE materialization didn't redu...

---

### query_43 [IMPROVED]

- **Baseline**: 356.5 ms (18 rows)
- **Best speedup**: 1.32x (W6, iter2)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.05x | NEUTRAL |  |
| W2 | 0 | 1.04x | NEUTRAL |  |
| W3 | 0 | 0.97x | NEUTRAL |  |
| W4 | 0 | 1.02x | NEUTRAL |  |
| W5 | 1 | 1.02x | NEUTRAL |  |
| W6 | 2 | 1.32x | IMPROVED | BEST |

**Strategies assigned:**

- W1: `conservative_early_pushdown` (early_filter, pushdown, materialize_cte)
  - Focus on pushing filters into the base tables/CTEs early to reduce join sizes, and consider materializing filtered dimension data to avoid repeated computation.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate filtered dimension tables into separate CTEs to enable better predicate pushdown and reuse across the query, improving join efficiency.
- W3: `aggressive_multi_cte_prefetch` (multi_dimension_prefetch, prefetch_fact_join, single_pass_aggregation)
  - Restructure with multiple CTEs to pre-filter and pre-join dimensions with facts, then perform aggregation in a single pass to minimize intermediate data.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Explore structural transformations such as splitting OR conditions, converting INTERSECT patterns, or decorrelating subqueries to unlock alternative execution plans.

**Reanalyze insight**: All previous attempts failed to achieve significant speedup because they only applied superficial CTE restructuring without addressing the core performance bottlenecks. The original query already has efficient filter pushdown in DuckDB's optimizer, so wrapping filters in CTEs provides minimal benefi...

---

### query_51 [IMPROVED]

- **Baseline**: 4867.9 ms (100 rows)
- **Best speedup**: 1.28x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.99x | NEUTRAL |  |
| W2 | 0 | 1.25x | IMPROVED |  |
| W3 | 0 | 1.00x | NEUTRAL |  |
| W4 | 0 | 0.98x | NEUTRAL |  |
| W5 | 1 | 1.28x | IMPROVED | BEST |
| W6 | 2 | 0.77x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_early_filters` (early_filter, pushdown, materialize_cte)
  - Apply early filtering and predicate pushdown to reduce data volume before joins; materialize repeated patterns to avoid recomputation.
- W2: `moderate_date_dimension_restructure` (date_cte_isolate, dimension_cte_isolate, deferred_window_aggregation)
  - Isolate date and dimension filtering into separate CTEs to enable better predicate pushdown; defer window calculations to reduce intermediate data.
- W3: `aggressive_multi_channel_prefetch` (shared_dimension_multi_channel, multi_dimension_prefetch, prefetch_fact_join)
  - Pre-filter and pre-join shared dimensions across channels in dedicated CTEs; consolidate fact-table scans to minimize redundant processing.
- W4: `novel_structural_transforms` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Transform full outer joins and complex conditions into UNION/EXISTS patterns; decorrelate and flatten query structure for better join planning.

**Reanalyze insight**: All workers failed to reach 2.0x because they focused on incremental improvements (early filtering, CTE materialization) without addressing the fundamental scalability bottleneck: the query performs TWO separate cumulative window aggregations over the same date range (web_v1 and store_v1), each with...

---

### query_11 [IMPROVED]

- **Baseline**: 3538.7 ms (100 rows)
- **Best speedup**: 1.27x (W3, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.09x | NEUTRAL |  |
| W2 | 0 | 1.05x | NEUTRAL |  |
| W3 | 0 | 1.27x | IMPROVED | BEST |
| W4 | 0 | 0.45x | REGRESSION |  |
| W5 | 1 | 1.24x | IMPROVED |  |
| W6 | 2 | 0.09x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_pushdown_filter` (pushdown, early_filter, materialize_cte)
  - Focus on pushing year and sale_type filters deeper into CTEs, pre-filtering customer/date dimensions before joins, and materializing intermediate results to reduce repetitive computation.
- W2: `moderate_date_dimension_isolate` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Extract date filtering (dyear=2001/2002) and customer dimension into separate CTEs before joining with sales tables, enabling better predicate pushdown and shared dimension reuse across store/web chan
- W3: `aggressive_multi_cte_restructure` (multi_date_range_cte, multi_dimension_prefetch, prefetch_fact_join)
  - Create separate CTEs for each date range (2001, 2002) and pre-join filtered dimensions with sales facts before aggregation, restructuring the multi-alias self-join into staged pre-aggregated results.
- W4: `novel_structural_transform` (union_cte_split, intersect_to_exists, or_to_union)
  - Split the UNION ALL CTE into separate store/web CTEs, transform the four-way intersection pattern into EXISTS subqueries, and explore converting conditional ratios to UNION ALL branches for better joi

**Reanalyze insight**: All attempts fell short because they addressed surface-level optimizations without tackling the fundamental bottleneck: the four-way self-join of the UNION ALL CTE creates O(n²) complexity with large intermediate results. Worker 3's aggressive restructuring (1.27x) showed promise by splitting into s...

---

### query_81 [IMPROVED]

- **Baseline**: 133.1 ms (100 rows)
- **Best speedup**: 1.27x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.94x | REGRESSION |  |
| W2 | 0 | 2.28x | FAIL | row mismatch |
| W3 | 0 | --- | ERROR | Binder Error |
| W4 | 0 | 0.97x | NEUTRAL |  |
| W5 | 1 | 1.27x | IMPROVED | BEST |
| W6 | 2 | 0.97x | NEUTRAL |  |

**Strategies assigned:**

- W1: `conservative_predicate_pushdown` (pushdown, early_filter, materialize_cte)
  - Focus on pushing filters into CTEs early and materializing repeated subqueries. Keep the original structure but improve filter application order.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate dimension table filtering (date_dim, customer_address) into separate CTEs before joining to reduce fact table scans and enable better predicate pushdown.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-filter dimensions and pre-join with fact tables in CTEs. Consolidate multiple scans into single-pass aggregations where possible.
- W4: `novel_decorrelation_transform` (decorrelate, composite_decorrelate_union, intersect_to_exists)
  - Transform the correlated subquery using decorrelation techniques. Explore converting the state-based comparison to a JOIN with pre-aggregated averages, potentially using UNION patterns.

**Reanalyze insight**: Worker 1's conservative approach failed because it didn't address the fundamental bottleneck: the correlated subquery computing state averages requires repeated aggregation. Worker 2 incorrectly filtered returning addresses to 'CA' in the CTE, changing semantics. Worker 3's error stemmed from incorr...

---

### query_84 [IMPROVED]

- **Baseline**: 51.7 ms (100 rows)
- **Best speedup**: 1.26x (W4, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.03x | NEUTRAL |  |
| W2 | 0 | 1.00x | NEUTRAL |  |
| W3 | 0 | 1.02x | NEUTRAL |  |
| W4 | 0 | 1.26x | IMPROVED | BEST |
| W5 | 1 | 1.25x | IMPROVED |  |
| W6 | 2 | 0.26x | FAIL | row mismatch |

**Strategies assigned:**

- W1: `conservative_early_reduction` (pushdown, early_filter, materialize_cte)
  - Apply filters to dimension tables first and push predicates down early; use CTE materialization for filtered dimension sets to reduce join sizes.
- W2: `dimension_isolation_restructure` (dimension_cte_isolate, date_cte_isolate, multi_dimension_prefetch)
  - Pre-filter and isolate all dimension tables into separate CTEs before joining with fact tables, enabling better predicate pushdown and join ordering.
- W3: `aggressive_fact_prefetch` (prefetch_fact_join, single_pass_aggregation, deferred_window_aggregation)
  - Pre-join filtered dimensions with fact tables early; consolidate multiple filtering passes into single CTEs with combined logic.
- W4: `structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Transform query structure using set operation rewrites and decorrelation patterns, even if not directly present, to inspire alternative join approaches.

**Reanalyze insight**: All attempts fell short because they focused on dimension table filtering while missing the core bottleneck: the store_returns join creates a many-to-many explosion that dominates execution. The strategies prematurely joined filtered dimensions but failed to reduce the fact table early. Worker 4's 1...

---

### query_16 [IMPROVED]

- **Baseline**: 25.1 ms (1 rows)
- **Best speedup**: 1.25x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | --- | ERROR | Catalog Error |
| W2 | 0 | 1.07x | NEUTRAL |  |
| W3 | 0 | 1.09x | NEUTRAL |  |
| W4 | 0 | 0.02x | REGRESSION |  |
| W5 | 1 | 1.25x | IMPROVED | BEST |
| W6 | 2 | 0.49x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_pushdown_filter` (early_filter, pushdown, materialize_cte)
  - Apply basic dimension table filtering first, push predicates into subqueries, and materialize repeated subquery patterns.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, date_cte_isolate, shared_dimension_multi_channel)
  - Pre-filter all dimension tables into separate CTEs before joining with fact tables to enable better predicate pushdown.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Use multi-step CTEs to pre-filter and pre-join dimensions with facts, then consolidate correlated subqueries into single-pass aggregations.
- W4: `novel_structural_transform` (composite_decorrelate_union, or_to_union, intersect_to_exists)
  - Decorrelate EXISTS subqueries using UNION-based patterns, transform OR conditions, and explore alternative set operation semantics.

**Reanalyze insight**: All attempts fell short of 2.0x because they optimized the wrong bottleneck. The query has two correlated subqueries (EXISTS and NOT EXISTS) that dominate execution time, but workers focused on dimension table filtering which was already efficient. Worker 1 failed due to syntax error (typo). Worker ...

---

### query_23a [IMPROVED]

- **Baseline**: 6400.4 ms (1 rows)
- **Best speedup**: 1.24x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.14x | IMPROVED |  |
| W2 | 0 | --- | ERROR | Binder Error |
| W3 | 0 | --- | ERROR | Binder Error |
| W4 | 0 | 1.21x | IMPROVED |  |
| W5 | 1 | 1.24x | IMPROVED | BEST |
| W6 | 2 | 1.15x | IMPROVED |  |

**Strategies assigned:**

- W1: `conservative_filter_pushdown` (pushdown, early_filter, materialize_cte)
  - Apply safe, low-risk optimizations: push filters into CTEs early, materialize reusable subqueries, and filter dimension tables before joining to reduce data volumes.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate date and dimension filtering into dedicated CTEs to enable predicate pushdown across multiple channel fact tables and reduce repeated scans.
- W3: `aggressive_multi_cte_restructure` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Restructure CTEs by prefetching dimension-filtered fact joins, isolating multiple dimension filters, and consolidating similar subqueries into single-pass aggregations.
- W4: `novel_structural_transform` (intersect_to_exists, or_to_union, union_cte_split)
  - Transform query structure: convert subquery patterns to EXISTS, split OR conditions into UNION ALL branches, and specialize UNION ALL CTEs for better filter pushdown.

**Reanalyze insight**: All attempts fell short due to two primary mechanisms:
1. **Column projection errors**: Workers 2 and 3 attempted dimension isolation but incorrectly projected only date_sk columns while subsequent CTEs needed d_date for grouping/output. DuckDB's column binding occurs during parsing, causing fatal e...

---

### query_30 [IMPROVED]

- **Baseline**: 112.6 ms (100 rows)
- **Best speedup**: 1.24x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.93x | FAIL | row mismatch |
| W2 | 0 | 1.17x | IMPROVED |  |
| W3 | 0 | 1.98x | FAIL | row mismatch |
| W4 | 0 | 1.15x | IMPROVED |  |
| W5 | 1 | 1.24x | IMPROVED | BEST |
| W6 | 2 | --- | ERROR | Binder Error |

**Strategies assigned:**

- W1: `conservative_pushdown_filtering` (pushdown, early_filter, materialize_cte)
  - Push filters into CTEs early, pre-filter dimension tables before joins, and materialize repeated patterns to avoid redundant computation.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, multi_date_range_cte)
  - Isolate filtered dimension tables into separate CTEs before fact joins, focusing on date and address dimension isolation to reduce join cardinality.
- W3: `aggressive_correlated_prefetch` (decorrelate, prefetch_fact_join, multi_dimension_prefetch)
  - Eliminate correlated subquery via decorrelation, pre-join filtered dimensions with facts in CTEs, and prefetch multiple dimension filters before main aggregation.
- W4: `novel_structural_transform` (intersect_to_exists, or_to_union, composite_decorrelate_union)
  - Apply unconventional structural transformations - convert subquery patterns to EXISTS/UNION forms, and explore composite decorrelation for complex patterns.

**Reanalyze insight**: All attempts fell short due to missing two critical optimizations: (1) insufficient reduction of the web_returns scan before aggregation, and (2) repeated expensive joins with customer_address. Worker 1 filtered state='IN' early but still joined web_returns with all three tables before aggregation, ...

---

### query_6 [IMPROVED]

- **Baseline**: 198.4 ms (51 rows)
- **Best speedup**: 1.23x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.84x | FAIL | row mismatch |
| W2 | 0 | 0.97x | NEUTRAL |  |
| W3 | 0 | --- | ERROR | Binder Error |
| W4 | 0 | 1.22x | IMPROVED |  |
| W5 | 1 | 1.23x | IMPROVED | BEST |
| W6 | 2 | 1.19x | IMPROVED |  |

**Strategies assigned:**

- W1: `conservative_early_filtering` (early_filter, pushdown, materialize_cte)
  - Apply safe, proven optimizations by filtering dimension tables first, pushing predicates down, and materializing the repeated date subquery into a CTE to avoid recomputation.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, decorrelate)
  - Isolate filtered dimensions into separate CTEs before joining with the fact table, and decorrelate the item price subquery by pre-computing category averages.
- W3: `aggressive_multi_cte_prefetch` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Restructure with multi-step CTEs that pre-filter dimensions and pre-join with the fact table, and consolidate the item category average calculation into a single pass.
- W4: `novel_structural_transform` (intersect_to_exists, or_to_union, composite_decorrelate_union)
  - Explore non-conventional transformations such as rewriting subqueries with EXISTS patterns, splitting potential OR conditions on item categories, and applying advanced decorrelation techniques.

**Reanalyze insight**: All attempts fell short of 2.0x because they focused on dimension table optimizations without addressing the fundamental bottleneck: the massive store_sales table. Worker 1's CTE approach actually added overhead (0.84x). Worker 2's dimension isolation helped but still processed the full fact table j...

---

### query_74 [IMPROVED]

- **Baseline**: 2270.8 ms (100 rows)
- **Best speedup**: 1.23x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | --- | ERROR | Binder Error |
| W2 | 0 | 1.21x | IMPROVED |  |
| W3 | 0 | --- | ERROR | Binder Error |
| W4 | 0 | 1.22x | IMPROVED |  |
| W5 | 1 | 1.23x | IMPROVED | BEST |
| W6 | 2 | 0.78x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_pushdown_materialization` (pushdown, early_filter, materialize_cte)
  - Push filters into CTEs to reduce data early, pre-filter dimension tables, and materialize the CTE to avoid repeated computation.
- W2: `moderate_dimension_date_isolation` (date_cte_isolate, dimension_cte_isolate, union_cte_split)
  - Isolate date and dimension filtering into separate CTEs, then split the UNION ALL CTE by channel to enable targeted optimizations.
- W3: `aggressive_multi_channel_prefetch` (shared_dimension_multi_channel, multi_date_range_cte, prefetch_fact_join)
  - Pre-filter shared dimensions for multiple sales channels, create separate CTEs for each date range, and prefetch fact joins to minimize intermediate data.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, decorrelate)
  - Transform OR conditions to UNION ALL, convert intersect patterns to EXISTS, and decorrelate subqueries to improve join planning and reduce dependencies.

**Reanalyze insight**: All optimization attempts fell short because they failed to address the core performance bottlenecks: (1) redundant full scans of customer and date_dim tables across channels, (2) expensive four-way self-join on aggregated CTE results, and (3) missed opportunities for early aggregation pruning. Work...

---

### query_83 [IMPROVED]

- **Baseline**: 47.6 ms (100 rows)
- **Best speedup**: 1.22x (W3, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.08x | NEUTRAL |  |
| W2 | 0 | 1.07x | NEUTRAL |  |
| W3 | 0 | 1.22x | IMPROVED | BEST |
| W4 | 0 | 1.19x | IMPROVED |  |
| W5 | 1 | 1.14x | IMPROVED |  |
| W6 | 2 | 1.01x | NEUTRAL |  |

**Strategies assigned:**

- W1: `conservative_early_reduction` (pushdown, early_filter, materialize_cte)
  - Apply safe, proven optimizations: push date filters into CTEs, pre-filter dimension tables before joining, and materialize the repeated date subquery to avoid redundant computation.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate shared dimension filters (date, item) into reusable CTEs before joining with fact tables, enabling better predicate pushdown and reducing dimension table scans across all three channels.
- W3: `aggressive_join_restructuring` (prefetch_fact_join, multi_dimension_prefetch, decorrelate)
  - Restructure joins aggressively: pre-filter dimensions and pre-join with facts in CTEs, isolate multiple dimension filters early, and decorrelate the date subquery by pre-materializing week sequences.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, union_cte_split)
  - Transform query structure using advanced patterns: convert date IN conditions to UNION ALL branches, rewrite subqueries as EXISTS clauses, and consider splitting the three channel CTEs into a unified 

**Reanalyze insight**: All previous attempts fell short because they only optimized dimension filtering while ignoring the fundamental structural inefficiency: three separate full scans of massive fact tables (store_returns, catalog_returns, web_returns) with identical date filtering. Each CTE repeats the same expensive j...

---

### query_97 [IMPROVED]

- **Baseline**: 1180.8 ms (1 rows)
- **Best speedup**: 1.22x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.05x | NEUTRAL |  |
| W2 | 0 | 1.06x | NEUTRAL |  |
| W3 | 0 | 0.99x | NEUTRAL |  |
| W4 | 0 | 0.57x | REGRESSION |  |
| W5 | 1 | 1.22x | IMPROVED | BEST |
| W6 | 2 | 0.60x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_filter_pushdown` (early_filter, pushdown, materialize_cte)
  - Apply safe, proven optimizations: push date filters early into CTEs, ensure predicate pushdown to reduce intermediate rows, and consider materializing CTEs for stability.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate shared date filtering into a reusable CTE to eliminate duplicate scans and enable better dimension pushdown across both sales channels.
- W3: `aggressive_multi_cte_restructure` (deferred_window_aggregation, prefetch_fact_join, single_pass_aggregation)
  - Restructure the multi-CTE pattern by prefetching dimension-filtered fact data, then attempt to consolidate the two channel aggregations into a single pass where possible.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Explore radical transforms: convert the FULL OUTER JOIN logic to UNION/EXISTS patterns, or decompose the correlation between channels using decorrelation techniques.

**Reanalyze insight**: All attempts fell short because they only addressed superficial filter pushdown while missing the core bottleneck: expensive full outer join of two large aggregated CTEs. Worker 1-3 and 5 merely restructured CTE organization but kept the same join pattern. Worker 4 attempted a radical rewrite using ...

---

### query_44 [IMPROVED]

- **Baseline**: 3.0 ms (0 rows)
- **Best speedup**: 1.22x (W4, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.98x | NEUTRAL |  |
| W2 | 0 | --- | ERROR | Binder Error |
| W3 | 0 | 1.20x | IMPROVED |  |
| W4 | 0 | 1.22x | IMPROVED | BEST |
| W5 | 1 | 1.17x | IMPROVED |  |
| W6 | 2 | 0.68x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_pushdown_filtering` (pushdown, early_filter, materialize_cte)
  - Apply safe, proven optimizations: push filters into subqueries, filter dimension tables first, and materialize repeated subqueries to avoid recomputation.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, decorrelate, shared_dimension_multi_channel)
  - Isolate dimension filtering and decorrelate subqueries using CTEs, while reusing shared dimension logic across query parts to reduce repeated work.
- W3: `aggressive_single_pass_restructure` (single_pass_aggregation, prefetch_fact_join, multi_dimension_prefetch)
  - Aggressively consolidate multiple scans into single-pass aggregations, prefetch filtered dimension-fact joins, and pre-isolate multiple dimensions to minimize table scans.
- W4: `novel_structural_transform` (intersect_to_exists, or_to_union, composite_decorrelate_union)
  - Apply novel structural transformations: convert patterns to EXISTS, split OR conditions into UNION ALL, and decorrelate complex subqueries using UNION-based approaches.

**Reanalyze insight**: All attempts fell short because they focused on CTE restructuring without addressing the fundamental bottleneck: the store_sales table scan with ss_store_sk=146 filter. Worker 3's 1.20x gain came from eliminating duplicate scans via single-pass aggregation, but all workers missed three critical oppo...

---

### query_13 [IMPROVED]

- **Baseline**: 817.7 ms (1 rows)
- **Best speedup**: 1.21x (W2, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | --- | ERROR | Binder Error |
| W2 | 0 | 1.21x | IMPROVED | BEST |
| W3 | 0 | 0.85x | REGRESSION |  |
| W4 | 0 | 0.83x | REGRESSION |  |
| W5 | 1 | --- | ERROR | Binder Error |
| W6 | 2 | --- | ERROR | Parser/Syntax Error |

**Strategies assigned:**

- W1: `conservative_pushdown_early_filter` (pushdown, early_filter, date_cte_isolate)
  - Apply safe, proven optimizations by pushing filters into dimension CTEs first, then joining with the fact table. Reduce join costs by filtering early and isolating date dimension.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, multi_dimension_prefetch, shared_dimension_multi_channel)
  - Pre-filter all dimension tables into separate CTEs before joining with the fact table. Isolate shared dimension patterns to reduce redundant filtering.
- W3: `aggressive_multi_cte_restructuring` (prefetch_fact_join, multi_date_range_cte, composite_decorrelate_union)
  - Restructure joins using multiple CTE stages—pre-join filtered dimensions with fact data early. Decorate complex conditions by materializing distinct subsets.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, single_pass_aggregation)
  - Transform OR conditions into UNION ALL branches for better indexing. Consolidate multiple filtering passes into single aggregates and rethink set operations.

**Reanalyze insight**: Worker 1 failed due to incorrect column isolation - they created filtered CTEs that selected only primary keys, losing necessary filter columns (cd_marital_status, cd_education_status, ca_state) that were referenced later in OR conditions. Worker 5 made the opposite mistake - pushing fact table cond...

---

### query_82 [IMPROVED]

- **Baseline**: 136.3 ms (9 rows)
- **Best speedup**: 1.21x (W3, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.95x | REGRESSION |  |
| W2 | 0 | 0.95x | REGRESSION |  |
| W3 | 0 | 1.21x | IMPROVED | BEST |
| W4 | 0 | 0.45x | REGRESSION |  |
| W5 | 1 | 1.14x | IMPROVED |  |
| W6 | 2 | 1.18x | IMPROVED |  |

**Strategies assigned:**

- W1: `conservative_predicate_pushdown` (early_filter, pushdown, materialize_cte)
  - Focus on moving filters as early as possible in the query plan. Filter dimension tables first, then push predicates into joins. Use CTE materialization for clarity but keep transformations minimal.
- W2: `dimension_prefilter_isolation` (dimension_cte_isolate, date_cte_isolate, shared_dimension_multi_channel)
  - Create separate CTEs for each pre-filtered dimension table (item, date_dim) before joining with fact tables. Isolate shared dimension filters to avoid repeated computation.
- W3: `aggressive_fact_prefetch` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-join filtered dimensions with fact tables in CTEs to reduce intermediate result sizes. Consolidate multiple fact table accesses and push aggregation earlier in the pipeline.
- W4: `structural_query_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Transform query structure fundamentally: convert IN clause to UNION ALL, explore EXISTS transformations, and decorrelate any implicit dependencies for better join planning.

**Reanalyze insight**: All attempts fell short because they focused on CTE-based pre-filtering without addressing the fundamental join explosion problem. The query joins three fact-like tables (inventory, store_sales) through item with no selective constraints on store_sales. This creates a multiplicative explosion: each ...

---

### query_53 [IMPROVED]

- **Baseline**: 236.2 ms (100 rows)
- **Best speedup**: 1.21x (W6, iter2)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.91x | REGRESSION |  |
| W2 | 0 | 1.06x | NEUTRAL |  |
| W3 | 0 | 1.18x | IMPROVED |  |
| W4 | 0 | 0.68x | REGRESSION |  |
| W5 | 1 | 1.04x | NEUTRAL |  |
| W6 | 2 | 1.21x | IMPROVED | BEST |

**Strategies assigned:**

- W1: `conservative_predicate_pushdown` (early_filter, pushdown, materialize_cte)
  - Focus on early filtering of dimension tables and pushing predicates into CTEs to reduce join sizes before any expensive operations.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate all dimension filtering into dedicated CTEs, then join with fact table to enable predicate pushdown and reuse filtered dimensions.
- W3: `aggressive_fact_prefetch` (prefetch_fact_join, multi_dimension_prefetch, deferred_window_aggregation)
  - Pre-join filtered dimensions with fact table in CTEs, then compute aggregations and window functions in separate stages to optimize data flow.
- W4: `novel_structural_transform` (or_to_union, composite_decorrelate_union, intersect_to_exists)
  - Transform complex OR conditions into UNION ALL branches and restructure subquery patterns to eliminate correlation and improve join planning.

**Reanalyze insight**: All attempts fell short because they focused solely on predicate pushdown through CTE isolation, which DuckDB's optimizer already handles well in the original query. Worker 4's UNION ALL approach (0.68x) failed catastrophically because it doubled the fact table joins and eliminated join-order flexib...

---

### query_58 [IMPROVED]

- **Baseline**: 160.1 ms (5 rows)
- **Best speedup**: 1.19x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.94x | REGRESSION |  |
| W2 | 0 | --- | ERROR | Binder Error |
| W3 | 0 | 0.93x | REGRESSION |  |
| W4 | 0 | 1.03x | NEUTRAL |  |
| W5 | 1 | 1.19x | IMPROVED | BEST |
| W6 | 2 | --- | ERROR | Binder Error |

**Strategies assigned:**

- W1: `conservative_pushdown_filter` (pushdown, early_filter, materialize_cte)
  - Push filters into CTEs early, materialize repeated date subqueries, and reduce intermediate result sizes through dimension-first filtering.
- W2: `moderate_date_dimension_isolation` (shared_dimension_multi_channel, date_cte_isolate, dimension_cte_isolate)
  - Extract shared date-week logic into reusable CTEs, pre-filter all dimension tables, and isolate date filtering to enable predicate pushdown across channel CTEs.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-join filtered dimensions with fact tables in CTEs, consider merging the three channel CTEs into one conditional-aggregation pass over a union of sales tables.
- W4: `novel_structural_transform` (intersect_to_exists, decorrelate, composite_decorrelate_union)
  - Transform the subquery structure—decorrelate date subqueries, convert implicit joins to explicit exists patterns, and restructure for better join planning.

**Reanalyze insight**: All attempts fell short because they only addressed surface-level optimizations (predicate pushdown, CTE materialization) without tackling the core bottleneck: the three separate CTEs perform identical joins with `item` and `date_dim` for each sales channel, tripling dimension table processing. Work...

---

### query_79 [IMPROVED]

- **Baseline**: 589.5 ms (100 rows)
- **Best speedup**: 1.18x (W6, iter2)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.12x | IMPROVED |  |
| W2 | 0 | 1.11x | IMPROVED |  |
| W3 | 0 | 1.09x | NEUTRAL |  |
| W4 | 0 | 1.13x | IMPROVED |  |
| W5 | 1 | 1.07x | NEUTRAL |  |
| W6 | 2 | 1.18x | IMPROVED | BEST |

**Strategies assigned:**

- W1: `conservative_early_pushdown` (early_filter, pushdown, materialize_cte)
  - Focus on pushing filters down to dimension tables early and materializing intermediate results to reduce join costs and avoid recomputation.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, multi_date_range_cte)
  - Isolate filtered dimension tables (date, store, household_demographics) into CTEs to enable better predicate pushdown and reduce join sizes before the main aggregation.
- W3: `aggressive_prefetch_restructuring` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-filter dimensions and pre-join with fact table in a staged CTE approach, then consolidate aggregations in a single pass to minimize intermediate data movement.
- W4: `novel_structural_transforms` (or_to_union, decorrelate, composite_decorrelate_union)
  - Transform OR conditions into UNION ALL for better index usage, and flatten any correlated subqueries (if present) to improve parallelization and join planning.

**Reanalyze insight**: All attempts failed to reach 2.0x because they only addressed dimension table filtering—which DuckDB's optimizer already handles efficiently. The true bottleneck is the massive fact-table aggregation (GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, s_city) on store_sales after joining with th...

---

### query_23b [IMPROVED]

- **Baseline**: 6726.5 ms (5 rows)
- **Best speedup**: 1.18x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.94x | REGRESSION |  |
| W2 | 0 | --- | ERROR | Binder Error |
| W3 | 0 | 0.89x | REGRESSION |  |
| W4 | 0 | --- | ERROR | Binder Error |
| W5 | 1 | 1.18x | IMPROVED | BEST |
| W6 | 2 | --- | ERROR | Binder Error |

**Strategies assigned:**

- W1: `conservative_early_reduction` (early_filter, pushdown, materialize_cte)
  - Apply early filtering on dimension tables and push predicates into CTEs, and materialize repeated subqueries to avoid recomputation.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Extract date and dimension filtering into separate CTEs to reduce scans and enable predicate pushdown across multiple channel queries.
- W3: `aggressive_prefetch_consolidation` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-join filtered dimensions with facts in CTEs and consolidate multiple aggregations into a single pass to reduce table scans.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, decorrelate)
  - Transform query structure by converting OR conditions to UNION ALL, replacing INTERSECT with EXISTS, and decorrelating subqueries for better join planning.

**Reanalyze insight**: All attempts fell short because they didn't address the core computational bottlenecks. Worker 5's 1.18x gain came from column pruning, but deeper issues remain: (1) The frequent_ss_items CTE performs fine-grained grouping by date (d_date) which is unnecessary for the final output and expensive. (2)...

---

### query_68 [IMPROVED]

- **Baseline**: 516.6 ms (100 rows)
- **Best speedup**: 1.17x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.10x | IMPROVED |  |
| W2 | 0 | 1.11x | IMPROVED |  |
| W3 | 0 | 1.10x | NEUTRAL |  |
| W4 | 0 | 0.67x | REGRESSION |  |
| W5 | 1 | 1.17x | IMPROVED | BEST |
| W6 | 2 | 1.02x | NEUTRAL |  |

**Strategies assigned:**

- W1: `conservative_filter_pushdown` (early_filter, pushdown, materialize_cte)
  - Apply early filtering on dimension tables before joining to fact tables, push predicates into subqueries, and materialize repeated subquery patterns to reduce redundant computation.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, multi_dimension_prefetch)
  - Isolate date and other dimension filters into separate CTEs to enable predicate pushdown, then pre-join filtered dimensions with the fact table in a controlled sequence.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, single_pass_aggregation, deferred_window_aggregation)
  - Pre-filter dimensions and pre-join them with the fact table in CTEs, consolidate multiple aggregations into a single pass, and defer window computations to minimize intermediate data.
- W4: `novel_structural_transform` (or_to_union, composite_decorrelate_union, decorrelate)
  - Transform OR conditions on different columns into UNION ALL branches, decorrelate multiple subqueries into pre-materialized CTEs, and convert correlated subqueries to joins for better parallelism.

**Reanalyze insight**: All attempts fell short because they optimized the wrong bottleneck. The query has three critical issues none properly addressed: 1) Expensive GROUP BY on store_sales (ticket-level aggregation) before joining with customer/customer_address, 2) Non-equijoin condition (current_addr.ca_city <> bought_c...

---

### query_63 [IMPROVED]

- **Baseline**: 274.7 ms (100 rows)
- **Best speedup**: 1.16x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.09x | NEUTRAL |  |
| W2 | 0 | 1.08x | NEUTRAL |  |
| W3 | 0 | 1.10x | NEUTRAL |  |
| W4 | 0 | 0.51x | REGRESSION |  |
| W5 | 1 | 1.16x | IMPROVED | BEST |
| W6 | 2 | 0.06x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_pushdown_and_simplify` (pushdown, early_filter, materialize_cte)
  - Apply safe, proven optimizations: push filters into subqueries, filter dimension tables before joining, and extract repeated calculations to CTEs for clarity and potential reuse.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, multi_dimension_prefetch)
  - Isolate and pre-filter each dimension table into separate CTEs before joining with the fact table, enabling better predicate pushdown and reducing intermediate join sizes.
- W3: `aggressive_fact_prefetch_restructure` (prefetch_fact_join, single_pass_aggregation, shared_dimension_multi_channel)
  - Radically restructure by prefetching filtered fact-dimension combinations into CTEs, then perform aggregation and window calculations in a consolidated single pass to minimize repeated scans.
- W4: `novel_structural_transformation` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Transform query structure: split OR conditions into UNION ALL branches, convert INTERSECT logic to EXISTS, and decorrelate complex conditions to enable better join planning and parallelization.

**Reanalyze insight**: All attempts fell short because they only applied surface-level optimizations (predicate pushdown, CTE isolation) that DuckDB's optimizer already performs automatically. The query's core bottleneck—the expensive windowed aggregation over a large fact table with complex multi-column OR filters—was ne...

---

### query_60 [IMPROVED]

- **Baseline**: 286.4 ms (100 rows)
- **Best speedup**: 1.15x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.02x | NEUTRAL |  |
| W2 | 0 | 0.99x | NEUTRAL |  |
| W3 | 0 | 1.00x | NEUTRAL |  |
| W4 | 0 | 1.10x | NEUTRAL |  |
| W5 | 1 | 1.15x | IMPROVED | BEST |
| W6 | 2 | 1.13x | IMPROVED |  |

**Strategies assigned:**

- W1: `conservative_pushdown_materialization` (pushdown, early_filter, materialize_cte)
  - Apply safe, proven optimizations: push filters into CTEs, filter dimensions early, and materialize repeated subqueries to avoid redundant computation.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate filtered dimension tables (date, address) into separate CTEs before joining with fact tables, and consolidate shared dimension logic across sales channels.
- W3: `aggressive_prefetch_consolidation` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-join filtered dimensions with fact tables in CTEs, prefetch multiple dimensions simultaneously, and consolidate multiple channel scans into a single aggregation pass.
- W4: `novel_structural_transformation` (or_to_union, intersect_to_exists, decorrelate)
  - Transform query structure using advanced patterns: convert OR conditions to UNION ALL, rewrite subqueries as EXISTS, and decorrelate dependent subqueries into independent CTEs.

**Reanalyze insight**: All previous attempts failed to achieve significant speedup because they focused only on dimension pre-filtering and CTE materialization, which DuckDB's optimizer already handles automatically. The original query's bottleneck is the triple scan of large fact tables (store_sales, catalog_sales, web_s...

---

### query_33 [IMPROVED]

- **Baseline**: 233.3 ms (100 rows)
- **Best speedup**: 1.15x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.98x | NEUTRAL |  |
| W2 | 0 | 0.92x | REGRESSION |  |
| W3 | 0 | 1.04x | NEUTRAL |  |
| W4 | 0 | 0.93x | REGRESSION |  |
| W5 | 1 | 1.15x | IMPROVED | BEST |
| W6 | 2 | 1.05x | NEUTRAL |  |

**Strategies assigned:**

- W1: `conservative_filter_pushdown` (early_filter, pushdown, materialize_cte)
  - Apply aggressive predicate pushdown into CTEs, pre-filter dimension tables first, and materialize the repeated item subquery into a shared CTE to avoid redundant computation across all three sales cha
- W2: `dimension_isolation_multi_channel` (dimension_cte_isolate, date_cte_isolate, shared_dimension_multi_channel)
  - Pre-filter and isolate each dimension table (date, item, customer_address) into separate CTEs before joining with the three sales fact tables, enabling reuse of filtered dimensions across all sales ch
- W3: `aggressive_prefetch_consolidation` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-join filtered dimensions with each fact table in dedicated CTEs, then consolidate the three sales streams into a single aggregation pass. Restructure to scan each fact table only once with all nec
- W4: `structural_transform_decorrelate` (decorrelate, or_to_union, intersect_to_exists)
  - Transform the repeated IN subquery using decorrelation techniques, restructure with UNION branches for different conditions, and explore alternative subquery formulations to improve join planning and 

**Reanalyze insight**: All workers fell short because they focused on dimension pre-filtering without addressing the fundamental bottleneck: three separate fact table scans with expensive hash joins. DuckDB's optimizer already handles predicate pushdown effectively, so dimension CTEs provide minimal benefit. The real issu...

---

### query_7 [IMPROVED]

- **Baseline**: 476.9 ms (100 rows)
- **Best speedup**: 1.15x (W3, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.01x | NEUTRAL |  |
| W2 | 0 | 1.06x | NEUTRAL |  |
| W3 | 0 | 1.15x | IMPROVED | BEST |
| W4 | 0 | 1.12x | IMPROVED |  |
| W5 | 1 | 1.02x | NEUTRAL |  |
| W6 | 2 | 0.35x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_pushdown_filter` (early_filter, pushdown, materialize_cte)
  - Apply safe, proven optimizations by pushing filters down early, materializing filtered dimension tables first, and reducing join sizes before aggregation.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, date_cte_isolate, multi_dimension_prefetch)
  - Isolate each filtered dimension into separate CTEs before joining with the fact table, enabling independent predicate pushdown and better join ordering.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, composite_decorrelate_union, deferred_window_aggregation)
  - Restructure joins via prefetching fact tables, decorrelate complex patterns, and defer expensive operations to minimize intermediate data.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, shared_dimension_multi_channel)
  - Transform OR conditions into UNION ALL, rewrite subqueries with EXISTS, and leverage shared dimension filtering to exploit DuckDB's parallelization.

**Reanalyze insight**: All attempts fell short because they focused exclusively on predicate pushdown via CTEs, which DuckDB's optimizer already handles automatically. The CTE-based rewrites provided minimal benefit (1.01x-1.15x) because they didn't address the fundamental bottlenecks: 1) Large fact-table scan with no pre...

---

### query_49 [IMPROVED]

- **Baseline**: 319.9 ms (41 rows)
- **Best speedup**: 1.14x (W2, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.09x | NEUTRAL |  |
| W2 | 0 | 1.14x | IMPROVED | BEST |
| W3 | 0 | 0.89x | FAIL | row mismatch |
| W4 | 0 | 1.01x | NEUTRAL |  |
| W5 | 1 | 1.01x | NEUTRAL |  |
| W6 | 2 | 0.96x | FAIL | row mismatch |

**Strategies assigned:**

- W1: `conservative_filter_pushdown` (early_filter, pushdown, materialize_cte)
  - Apply safe, proven optimizations by filtering dimension tables first, pushing filters into subqueries, and materializing repeated computations via CTEs.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate shared dimension filters (especially date_dim) into reusable CTEs to enable predicate pushdown and reduce redundant joins across all three channels.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, multi_dimension_prefetch, deferred_window_aggregation)
  - Pre-filter and pre-join dimension tables with fact tables in CTEs, then restructure window computations to run after union or in a single pass to minimize intermediate data.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, single_pass_aggregation)
  - Transform OR conditions into UNION ALL branches, explore alternative set-operation patterns, and consolidate multiple channel subqueries into a single aggregation with conditional logic.

**Reanalyze insight**: All attempts fell short because they focused on incremental improvements to the same underlying execution pattern without addressing the fundamental bottlenecks. The query structure forces three independent expensive operations: (1) large fact-table scans with LEFT JOINs that become INNER JOINs due ...

---

### query_37 [IMPROVED]

- **Baseline**: 83.2 ms (3 rows)
- **Best speedup**: 1.13x (W1, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.13x | IMPROVED | BEST |
| W2 | 0 | 0.91x | REGRESSION |  |
| W3 | 0 | 1.04x | NEUTRAL |  |
| W4 | 0 | 1.10x | IMPROVED |  |
| W5 | 1 | 10.64x | FAIL | row mismatch |
| W6 | 2 | 0.02x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_pushdown_earlyfilter` (early_filter, pushdown, date_cte_isolate)
  - Apply early filtering on dimension tables and push down predicates to reduce data early in the plan, minimizing join costs.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, multi_dimension_prefetch, shared_dimension_multi_channel)
  - Isolate dimension table filters into separate CTEs to enable efficient join ordering and reduce fact table scans.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, single_pass_aggregation, materialize_cte)
  - Pre-join filtered dimensions with facts in CTEs, consolidate operations, and materialize intermediate results for faster aggregation.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, multi_date_range_cte)
  - Transform query structure by splitting conditions, converting set operations, and isolating date ranges for index optimization.

**Reanalyze insight**: All workers fell short due to misapplied optimization patterns that increased overhead without addressing the true bottleneck: the large-scale join between inventory and catalog_sales with different cardinalities. Worker 1-4 used CTE isolation correctly but missed the fundamental issue that the join...

---

### query_72 [IMPROVED]

- **Baseline**: 613.6 ms (100 rows)
- **Best speedup**: 1.13x (W1, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.13x | IMPROVED | BEST |
| W2 | 0 | --- | ERROR | Binder Error |
| W3 | 0 | 0.41x | REGRESSION |  |
| W4 | 0 | 1.09x | NEUTRAL |  |
| W5 | 1 | --- | ERROR | Binder Error |

**Strategies assigned:**

- W1: `conservative_pushdown_filter` (early_filter, pushdown, materialize_cte)
  - Apply predicate pushdown to dimension tables before joining, use CTEs to materialize filtered dimensions, and leverage DuckDB's ability to push filters through joins for minimal intermediate data.
- W2: `moderate_date_dimension_isolation` (multi_date_range_cte, dimension_cte_isolate, date_cte_isolate)
  - Isolate each date_dim alias (d1,d2,d3) into separate filtered CTEs, pre-filter all dimension tables, then join with fact tables to reduce join cardinality early.
- W3: `aggressive_prefetch_restructuring` (prefetch_fact_join, multi_dimension_prefetch, shared_dimension_multi_channel)
  - Create layered CTEs—first filter dimensions, then pre-join them with catalog_sales before other joins, and share filtered dimensions across date aliases to avoid repeated filtering.
- W4: `novel_structural_transforms` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Explore converting implicit OR conditions in CASE statements to UNION ALL branches, transform join patterns to EXISTS subqueries, and decorrelate complex join logic for alternative execution plans.

**Reanalyze insight**: All attempts fell short due to fundamental structural flaws. Worker 1 only achieved 1.13x because it performed basic predicate pushdown but left the massive join between catalog_sales and inventory untouched - this remains the bottleneck. Worker 3's aggressive restructuring (0.41x) created overly co...

---

### query_21 [IMPROVED]

- **Baseline**: 60.4 ms (100 rows)
- **Best speedup**: 1.13x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.02x | NEUTRAL |  |
| W2 | 0 | 1.05x | NEUTRAL |  |
| W3 | 0 | 0.95x | NEUTRAL |  |
| W4 | 0 | 0.95x | NEUTRAL |  |
| W5 | 1 | 1.13x | IMPROVED | BEST |
| W6 | 2 | 1.03x | NEUTRAL |  |

**Strategies assigned:**

- W1: `conservative_early_filter_pushdown` (early_filter, pushdown, materialize_cte)
  - Apply early filtering on dimension tables (date_dim, item) before joining with the large inventory fact table to reduce intermediate result sizes.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate filtered dimension tables into separate CTEs to enable predicate pushdown and reuse filtered dimensions across the query.
- W3: `aggressive_multi_cte_prefetch` (multi_dimension_prefetch, prefetch_fact_join, single_pass_aggregation)
  - Pre-filter multiple dimension tables into CTEs, then pre-join them with the fact table in a second CTE before final aggregation to minimize large table scans.
- W4: `novel_structural_transformation` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Explore structural transformations like converting conditional logic to UNION ALL branches or decorrelating complex subqueries to enable more efficient join plans.

**Reanalyze insight**: All attempts focused on dimension table filtering and CTE isolation, which are sensible optimizations but insufficient for 2.0x speedup. The fundamental bottleneck is the massive inventory table (likely billions of rows) and the expensive 3-way join + aggregation. Key issues: (1) All workers used th...

---

### query_73 [IMPROVED]

- **Baseline**: 317.9 ms (15 rows)
- **Best speedup**: 1.12x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.02x | NEUTRAL |  |
| W2 | 0 | 1.06x | NEUTRAL |  |
| W3 | 0 | 1.06x | NEUTRAL |  |
| W4 | 0 | 1.07x | NEUTRAL |  |
| W5 | 1 | 1.12x | IMPROVED | BEST |
| W6 | 2 | 0.93x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_predicate_pushdown` (early_filter, pushdown, materialize_cte)
  - Focus on pushing filters earliest possible, materializing filtered dimension tables first, and ensuring predicates propagate into subqueries before joins.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate all dimension table filtering into separate CTEs before joining to fact tables, enabling parallel filter optimization and reducing join cardinality early.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-filter and pre-join dimensions with fact table in staged CTEs, then perform final aggregation; consolidate multiple dimension filters into single-pass CTEs.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Transform OR conditions on hd_buy_potential using UNION ALL, convert case-expression to filterable computed column, and consider decorrelation techniques for the grouping subquery.

**Reanalyze insight**: All workers relied heavily on dimension-table pre-filtering CTEs, which DuckDB's optimizer already implements automatically through predicate pushdown and join reordering. The minimal speedups (1.02x-1.12x) indicate:
1. **Overhead of forced CTE materialization**: DuckDB's optimizer is already pushin...

---

### query_48 [IMPROVED]

- **Baseline**: 656.3 ms (1 rows)
- **Best speedup**: 1.11x (W1, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.11x | IMPROVED | BEST |
| W2 | 0 | --- | ERROR | Binder Error |
| W3 | 0 | --- | ERROR | Binder Error |
| W4 | 0 | 0.26x | REGRESSION |  |
| W5 | 1 | --- | ERROR | Binder Error |
| W6 | 2 | 0.54x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_early_filtering` (early_filter, pushdown, materialize_cte)
  - Apply safe, proven optimizations by filtering dimension tables first, pushing predicates down, and materializing reusable filtered dimension sets.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, multi_dimension_prefetch)
  - Isolate dimension table filtering into separate CTEs before joining with fact tables, enabling predicate pushdown and reducing join cardinality early.
- W3: `aggressive_fact_prefetch` (prefetch_fact_join, single_pass_aggregation, shared_dimension_multi_channel)
  - Restructure joins to pre-filter and pre-join dimension tables with fact data in CTEs, then perform final aggregation in a single pass.
- W4: `novel_or_restructuring` (or_to_union, composite_decorrelate_union, intersect_to_exists)
  - Transform OR conditions into UNION ALL branches and apply advanced structural transformations to enable better index usage and join planning.

**Reanalyze insight**: Worker 2, 3, and 5 failed due to binder errors from incorrect column references when joining CTEs. They created dimension-filtering CTEs but then referenced dimension columns in the WHERE clause that weren't projected from those CTEs. Worker 1 succeeded but only achieved 1.11x because it performed r...

---

### query_10 [IMPROVED]

- **Baseline**: 220.7 ms (75 rows)
- **Best speedup**: 1.11x (W2, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.06x | NEUTRAL |  |
| W2 | 0 | 1.11x | IMPROVED | BEST |
| W3 | 0 | --- | ERROR | Binder Error |
| W4 | 0 | 1.07x | NEUTRAL |  |
| W5 | 1 | 0.97x | NEUTRAL |  |
| W6 | 2 | 0.87x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_early_filtering` (early_filter, pushdown, materialize_cte)
  - Apply safe, incremental optimizations by pushing filters early into CTEs, reducing join cardinality before complex operations.
- W2: `moderate_dimension_prefiltering` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate and pre-filter all dimension tables (date_dim, customer_address) into separate CTEs before joining with fact tables.
- W3: `aggressive_fact_prefetch_restructure` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-join filtered dimensions with each fact table in separate CTEs, then consolidate with customer for single-pass aggregation.
- W4: `novel_structural_transform` (or_to_union, composite_decorrelate_union, decorrelate)
  - Transform OR EXISTS logic into UNION ALL, decorrelate subqueries into pre-aggregated CTEs, and restructure joins.

**Reanalyze insight**: All attempts failed to achieve meaningful speedup because they didn't address the core bottleneck: massive intermediate result explosion during the three-fact-table correlation. Worker 1-2 only pushed down dimension filters (1.06-1.11x). Worker 3 had a syntax error. Worker 4-5 decorrelated with UNIO...

---

### query_76 [IMPROVED]

- **Baseline**: 316.6 ms (100 rows)
- **Best speedup**: 1.11x (W4, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.05x | NEUTRAL |  |
| W2 | 0 | 1.07x | NEUTRAL |  |
| W3 | 0 | 1.10x | IMPROVED |  |
| W4 | 0 | 1.11x | IMPROVED | BEST |
| W5 | 1 | 1.07x | NEUTRAL |  |
| W6 | 2 | 0.73x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_join_pushdown` (pushdown, early_filter, materialize_cte)
  - Apply safe, proven optimizations like pushing filters into subqueries, filtering dimensions before joins, and materializing repetitive patterns with CTEs.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate dimension filtering (date, item) into reusable CTEs before joining with fact tables to reduce redundant computation across all three sales channels.
- W3: `aggressive_multi_cte_restructuring` (multi_dimension_prefetch, prefetch_fact_join, deferred_window_aggregation)
  - Pre-filter and pre-join dimension tables into separate CTEs, then restructure the UNION branches to scan pre-joined intermediate results.
- W4: `novel_structural_transforms` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Explore structural changes like converting the UNION pattern to use EXISTS decorrelation or transforming the query to use OR conditions with subsequent split.

**Reanalyze insight**: All workers fell short because they only restructured the query surface without addressing the core bottleneck: massive data movement from three separate large fact table scans (store_sales, web_sales, catalog_sales) with identical dimension joins. DuckDB's optimizer already pushes down filters and ...

---

### query_98 [NEUTRAL]

- **Baseline**: 233.0 ms (15226 rows)
- **Best speedup**: 1.10x (W3, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.02x | NEUTRAL |  |
| W2 | 0 | 0.96x | NEUTRAL |  |
| W3 | 0 | 1.10x | NEUTRAL | BEST |
| W4 | 0 | 1.08x | NEUTRAL |  |
| W5 | 1 | 1.04x | NEUTRAL |  |
| W6 | 2 | 1.09x | FAIL | row mismatch |

**Strategies assigned:**

- W1: `conservative_filter_pushdown` (early_filter, pushdown, materialize_cte)
  - Apply safe, proven optimizations by pushing filters into CTEs early, ensuring predicate pushdown to reduce join sizes, and materializing reusable subresults.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, multi_dimension_prefetch)
  - Isolate filtered dimension tables (date, item) into separate CTEs before joining with the fact table to improve plan clarity and enable parallel filtering.
- W3: `aggressive_join_restructuring` (prefetch_fact_join, single_pass_aggregation, deferred_window_aggregation)
  - Restructure joins by prefetching filtered dimension-fact combinations, consolidate aggregations into single passes, and defer window computations to minimize intermediate data.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Apply structural query transformations by splitting OR conditions into UNION ALL, converting INTERSECT patterns, and decorrelating complex subqueries for better join planning.

**Reanalyze insight**: All attempts fell short because they only applied structural reorganization (CTE isolation, filter pushdown) without addressing the core computational bottlenecks. DuckDB's optimizer already performs automatic predicate pushdown and join reordering, making these surface-level transformations redunda...

---

### query_50 [NEUTRAL]

- **Baseline**: 687.2 ms (51 rows)
- **Best speedup**: 1.10x (W2, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.97x | NEUTRAL |  |
| W2 | 0 | 1.10x | NEUTRAL | BEST |
| W3 | 0 | 1.08x | NEUTRAL |  |
| W4 | 0 | 1.03x | NEUTRAL |  |
| W5 | 1 | 1.02x | NEUTRAL |  |
| W6 | 2 | --- | ERROR | Binder Error |

**Strategies assigned:**

- W1: `conservative_pushdown_filter` (pushdown, early_filter, materialize_cte)
  - Apply filter pushdown and early dimension filtering to reduce join sizes, and materialize repeated subqueries to avoid recomputation.
- W2: `moderate_date_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, multi_date_range_cte)
  - Isolate date and dimension filters into separate CTEs to enable predicate pushdown and reduce the data before joining.
- W3: `aggressive_multi_cte_restructure` (multi_dimension_prefetch, prefetch_fact_join, single_pass_aggregation)
  - Pre-filter and pre-join multiple dimension tables with fact tables in CTEs, and consolidate multiple aggregations into a single pass.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Transform the query structure by splitting OR conditions, converting INTERSECT to EXISTS, and decorrelating multiple subqueries.

**Reanalyze insight**: All workers fell short because they focused on rearranging joins into CTEs without addressing the core performance bottlenecks. The original query has three critical issues: 1) Large Cartesian product risk from joining store_sales with store_returns before filtering (both are massive fact tables), 2...

---

### query_66 [NEUTRAL]

- **Baseline**: 196.8 ms (10 rows)
- **Best speedup**: 1.08x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.98x | NEUTRAL |  |
| W2 | 0 | 0.96x | NEUTRAL |  |
| W3 | 0 | 1.06x | NEUTRAL |  |
| W4 | 0 | 1.03x | NEUTRAL |  |
| W5 | 1 | 1.08x | NEUTRAL | BEST |
| W6 | 2 | 1.00x | NEUTRAL |  |

**Strategies assigned:**

- W1: `conservative_pushdown_and_filter` (pushdown, early_filter, materialize_cte)
  - Apply safe, proven optimizations: push filters into CTEs, filter dimension tables early, and materialize repeated patterns to reduce intermediate data size.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate filtered dimension tables (date, ship_mode, time) into reusable CTEs before joining with fact tables, enabling better predicate pushdown and shared scans.
- W3: `aggressive_prefetch_restructure` (multi_dimension_prefetch, prefetch_fact_join, single_pass_aggregation)
  - Restructure with multiple CTEs to prefetch and pre-join filtered dimensions, and consolidate monthly aggregates into a single scan per fact table for efficiency.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Explore structural changes: transform OR conditions to UNION ALL, apply EXISTS-based rewrites, and decorrelate patterns to improve join planning and parallelization.

**Reanalyze insight**: All attempts failed because they only applied surface-level optimizations without addressing the core bottleneck: massive intermediate data volume from Cartesian-like joins between large fact tables and pre-filtered dimension CTEs. Worker 1-5 all used the same pattern of pre-filtering dimension tabl...

---

### query_12 [NEUTRAL]

- **Baseline**: 63.5 ms (100 rows)
- **Best speedup**: 1.08x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.00x | NEUTRAL |  |
| W2 | 0 | 0.99x | NEUTRAL |  |
| W3 | 0 | 1.04x | NEUTRAL |  |
| W4 | 0 | 0.37x | REGRESSION |  |
| W5 | 1 | 1.08x | NEUTRAL | BEST |
| W6 | 2 | 0.55x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_predicate_pushdown` (early_filter, pushdown, materialize_cte)
  - Apply early filtering on dimension tables (item, date_dim) to reduce fact table joins, push down all predicates before aggregation, and materialize filtered dimensions as CTEs.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, date_cte_isolate, shared_dimension_multi_channel)
  - Isolate filtered dimension tables into separate CTEs before joining with web_sales, enabling independent optimization and predicate pushdown on each dimension.
- W3: `aggressive_prefetch_restructuring` (prefetch_fact_join, multi_dimension_prefetch, deferred_window_aggregation)
  - Pre-join filtered dimensions with fact table in a staged CTE, then compute window aggregation separately to reduce intermediate data volume.
- W4: `novel_structural_transforms` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Transform the IN clause on i_category into UNION ALL branches, restructure subqueries using EXISTS patterns, and decorrelate hypothetical complex filters.

**Reanalyze insight**: All attempts failed because they only restructured the query without addressing the fundamental bottleneck: the window function's PARTITION BY i_class requires a full sort/partition of the aggregated data, which becomes expensive even after filtering. Worker 3's "deferred_window_aggregation" (1.04x)...

---

### query_55 [NEUTRAL]

- **Baseline**: 146.4 ms (100 rows)
- **Best speedup**: 1.08x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.99x | NEUTRAL |  |
| W2 | 0 | 0.93x | REGRESSION |  |
| W3 | 0 | 0.95x | NEUTRAL |  |
| W4 | 0 | 0.96x | NEUTRAL |  |
| W5 | 1 | 1.08x | NEUTRAL | BEST |
| W6 | 2 | 0.93x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_early_filter_pushdown` (early_filter, pushdown, date_cte_isolate)
  - Apply early filtering on dimension tables and push predicates down to reduce the size of intermediate results before joining.
- W2: `dimension_isolation_prefetch` (dimension_cte_isolate, multi_dimension_prefetch, prefetch_fact_join)
  - Pre-filter all dimension tables into separate CTEs and then join with the fact table to minimize the fact table scan cost.
- W3: `aggressive_multi_cte_restructuring` (multi_date_range_cte, single_pass_aggregation, deferred_window_aggregation)
  - Restructure using multiple CTEs to isolate date ranges and consolidate operations into single-pass aggregations for complex scenarios.
- W4: `novel_structural_transformation` (or_to_union, intersect_to_exists, decorrelate)
  - Transform query structure by splitting OR conditions, converting INTERSECT to EXISTS, and decorrelating subqueries for better join planning.

**Reanalyze insight**: All attempts essentially implemented the same optimization: pre-filtering dimension tables into CTEs before joining with the fact table. This approach failed because:
1. **DuckDB's optimizer already performs predicate pushdown automatically** - The CTEs don't fundamentally change the execution plan
...

---

### query_19 [NEUTRAL]

- **Baseline**: 284.2 ms (100 rows)
- **Best speedup**: 1.08x (W1, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.08x | NEUTRAL | BEST |
| W2 | 0 | 1.04x | NEUTRAL |  |
| W3 | 0 | 1.06x | NEUTRAL |  |
| W4 | 0 | 0.57x | REGRESSION |  |
| W5 | 1 | 0.97x | NEUTRAL |  |
| W6 | 2 | 0.96x | NEUTRAL |  |

**Strategies assigned:**

- W1: `conservative_filter_pushdown` (early_filter, pushdown, materialize_cte)
  - Start by filtering dimension tables individually before joining to reduce fact table rows; push all possible filters into CTEs; materialize expensive subpatterns.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Create separate optimized CTEs for each filtered dimension (date, item, customer/address, store) before joining with fact table; isolate shared dimension logic.
- W3: `aggressive_prefetch_restructuring` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-join filtered dimensions with fact table in staged CTEs to maximize early reduction; restructure to compute aggregates in single passes where possible.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Transform the zip code inequality into UNION ALL branches; explore EXISTS alternatives for the cross-table condition; decorrelate any implicit subqueries.

**Reanalyze insight**: All attempts fell short because they didn't address the core bottleneck: the expensive inequality join condition `substr(ca_zip,1,5) <> substr(s_zip,1,5)` that requires comparing every customer-address combination with every store. The CTE-based approaches only pushed down simple filters (date, item...

---

### query_47 [NEUTRAL]

- **Baseline**: 1270.3 ms (100 rows)
- **Best speedup**: 1.07x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.00x | NEUTRAL |  |
| W2 | 0 | 1.04x | NEUTRAL |  |
| W3 | 0 | 0.89x | REGRESSION |  |
| W4 | 0 | 0.75x | REGRESSION |  |
| W5 | 1 | 1.07x | NEUTRAL | BEST |
| W6 | 2 | 0.18x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_early_pushdown` (early_filter, pushdown, date_cte_isolate)
  - Apply early filtering to dimension tables and push filters into CTEs to minimize data movement and reduce join sizes before aggregation.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, multi_date_range_cte, shared_dimension_multi_channel)
  - Isolate filtered dimension tables into separate CTEs, then join with facts to enable better predicate pushdown and reduce window partition sizes.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, multi_dimension_prefetch, deferred_window_aggregation)
  - Pre-join filtered dimensions with fact data before full aggregation, then restructure window computations to avoid repeated scanning.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Convert OR conditions into UNION ALL branches, transform self-join patterns to EXISTS-like logic, and decorrelate through pre-materialized CTEs.

**Reanalyze insight**: All attempts failed because they only addressed surface-level optimizations without targeting the core bottlenecks. Worker 1-3 focused on dimension isolation/pre-filtering, but DuckDB's optimizer already pushes these filters efficiently. Worker 4's OR-to-UNION actually harmed performance by tripling...

---

### query_52 [NEUTRAL]

- **Baseline**: 153.5 ms (100 rows)
- **Best speedup**: 1.07x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.00x | NEUTRAL |  |
| W2 | 0 | 1.03x | NEUTRAL |  |
| W3 | 0 | 1.01x | NEUTRAL |  |
| W4 | 0 | 0.92x | REGRESSION |  |
| W5 | 1 | 1.07x | NEUTRAL | BEST |
| W6 | 2 | 0.94x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_early_filtering` (early_filter, pushdown, materialize_cte)
  - Apply standard optimizations - filter dimension tables first to reduce join sizes, push predicates down, and use CTEs for readability without risking plan changes.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, date_cte_isolate, shared_dimension_multi_channel)
  - Isolate filtered dimension tables (date_dim, item) into separate CTEs before joining to store_sales to enable predicate pushdown and reduce intermediate results.
- W3: `aggressive_prefetch_restructuring` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-filter dimensions into CTEs, then pre-join with fact table early, and consider consolidating operations to minimize passes over large fact tables.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Explore structural transformations even without matching patterns - consider converting implied operations, testing EXISTS alternatives, or splitting/combining query parts differently.

**Reanalyze insight**: All attempts fell short because they merely restructured the query with CTEs without changing the fundamental execution plan. DuckDB's optimizer already pushes predicates down and chooses efficient join orders for star schema queries. The CTE-based isolation strategies (attempts 1-5) simply rephrase...

---

### query_20 [NEUTRAL]

- **Baseline**: 36.9 ms (100 rows)
- **Best speedup**: 1.07x (W4, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.00x | NEUTRAL |  |
| W2 | 0 | 1.05x | NEUTRAL |  |
| W3 | 0 | 0.99x | NEUTRAL |  |
| W4 | 0 | 1.07x | NEUTRAL | BEST |
| W5 | 1 | 1.01x | NEUTRAL |  |
| W6 | 2 | 0.71x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_early_filter_pushdown` (early_filter, pushdown, materialize_cte)
  - Filter dimension tables first, push predicates into joins, and materialize repeated computations to reduce intermediate data size before aggregation.
- W2: `moderate_dimension_date_isolation` (dimension_cte_isolate, date_cte_isolate, multi_dimension_prefetch)
  - Pre-filter item and date dimensions into separate CTEs to enable predicate pushdown and reduce fact table join cardinality before aggregation.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, single_pass_aggregation, deferred_window_aggregation)
  - Pre-join filtered dimensions with fact table in staged CTEs, consolidate aggregations into single pass, and defer window computation to minimize scanning.
- W4: `novel_structural_transforms` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Transform IN-list (OR) conditions into UNION ALL branches, rewrite potential subquery patterns as EXISTS, and decorrelate complex joins to improve join planning.

**Reanalyze insight**: All previous attempts fell short (1.00-1.07x) because they focused only on dimension filtering, which provides minimal benefit in this query's bottleneck structure. The core issue is the massive catalog_sales table scan with 3.8B rows. Dimension filtering (item, date_dim) reduces rows by ~1000x and ...

---

### query_77 [NEUTRAL]

- **Baseline**: 284.0 ms (100 rows)
- **Best speedup**: 1.07x (W4, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.92x | REGRESSION |  |
| W2 | 0 | 1.01x | NEUTRAL |  |
| W3 | 0 | 1.02x | NEUTRAL |  |
| W4 | 0 | 1.07x | NEUTRAL | BEST |
| W5 | 1 | 1.06x | NEUTRAL |  |
| W6 | 2 | --- | ERROR | Binder Error |

**Strategies assigned:**

- W1: `conservative_pushdown_earlyfilter` (pushdown, early_filter, materialize_cte)
  - Push date filters into CTEs early, pre-filter dimension tables before joining, and materialize reusable date-range CTEs to avoid recomputation.
- W2: `moderate_date_dimension_isolate` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Create isolated CTEs for filtered date and dimension tables (store, web_page), then share them across channel-specific CTEs to reduce redundant joins.
- W3: `aggressive_prefetch_consolidation` (prefetch_fact_join, single_pass_aggregation, multi_dimension_prefetch)
  - Pre-join filtered dimensions with facts in each channel CTE, attempt consolidated aggregations per table, and prefetch multiple dimension filters in parallel.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Restructure query using UNION ALL for separate channels, transform potential set operations, and decorrelate multiple subqueries into pre-materialized aggregates.

**Reanalyze insight**: All previous attempts fell short because they focused primarily on dimension table filtering and CTE isolation, which provided minimal gains (0.92x-1.07x) for three key reasons:
1. **Missing fact table consolidation**: Each channel processes sales and returns as separate CTEs, causing duplicate scan...

---

### query_8 [NEUTRAL]

- **Baseline**: 700.1 ms (6 rows)
- **Best speedup**: 1.07x (W2, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.01x | NEUTRAL |  |
| W2 | 0 | 1.07x | NEUTRAL | BEST |
| W3 | 0 | 0.80x | REGRESSION |  |
| W4 | 0 | 0.87x | REGRESSION |  |
| W5 | 1 | 1.03x | NEUTRAL |  |
| W6 | 2 | --- | ERROR | Binder Error |

**Strategies assigned:**

- W1: `conservative_early_filter_pushdown` (early_filter, pushdown, materialize_cte)
  - Apply early filtering to dimension tables (date_dim, store) before joining with fact table; push static zip list filters into subqueries; materialize the complex zip derivation once.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Extract date and store filters into separate CTEs to isolate dimension pruning; pre-filter all dimension tables before joining with fact table to reduce intermediate rows.
- W3: `aggressive_multi_cte_prefetch` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-filter dimensions and pre-join with fact table in CTEs; consolidate the two zip subqueries into a single CTE with conditional aggregation to avoid INTERSECT.
- W4: `novel_structural_transform` (intersect_to_exists, or_to_union, composite_decorrelate_union)
  - Transform the INTERSECT into EXISTS semi-joins; restructure the static zip list as a UNION of small batches for better plan selection; decorrelate the customer subquery using distinct pre-aggregation.

**Reanalyze insight**: All workers fell short because they failed to address the fundamental bottleneck: the cross-join explosion caused by joining store_sales with eligible_zips on state-level zip prefixes. This creates massive intermediate cardinality (store_sales rows duplicated for each matching zip in the same state)...

---

### query_89 [NEUTRAL]

- **Baseline**: 337.6 ms (100 rows)
- **Best speedup**: 1.06x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.99x | NEUTRAL |  |
| W2 | 0 | 1.05x | NEUTRAL |  |
| W3 | 0 | 1.04x | NEUTRAL |  |
| W4 | 0 | --- | ERROR | Binder Error |
| W5 | 1 | 1.06x | NEUTRAL | BEST |
| W6 | 2 | 0.92x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_filter_pushdown` (early_filter, pushdown, materialize_cte)
  - Apply safe predicate pushdown by filtering dimension tables first into CTEs, then join with fact tables to reduce data volume early in the pipeline.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, multi_dimension_prefetch)
  - Isolate all dimension table filtering (date, item, store) into separate CTEs before joining with fact table, enabling independent optimization of each dimension.
- W3: `aggressive_prefetch_restructuring` (prefetch_fact_join, single_pass_aggregation, deferred_window_aggregation)
  - Pre-filter and pre-join dimensions with fact data in staged CTEs, then consolidate aggregations and defer window computations to minimize intermediate result sizes.
- W4: `novel_structural_transform` (or_to_union, union_cte_split, shared_dimension_multi_channel)
  - Transform the complex OR condition on item categories/classes into UNION ALL branches with specialized CTEs, enabling better predicate pushdown and index usage.

**Reanalyze insight**: All workers fell short because they focused exclusively on dimension pre-filtering while missing the core computational bottleneck: the expensive window function computing `avg_monthly_sales`. The attempts achieved minimal speedup (≤1.06x) because they reduced initial data volume but still required ...

---

### query_26 [NEUTRAL]

- **Baseline**: 211.7 ms (100 rows)
- **Best speedup**: 1.05x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.03x | NEUTRAL |  |
| W2 | 0 | 1.04x | NEUTRAL |  |
| W3 | 0 | 0.94x | REGRESSION |  |
| W4 | 0 | 0.51x | REGRESSION |  |
| W5 | 1 | 1.05x | NEUTRAL | BEST |
| W6 | 2 | 0.41x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_pushdown_filter` (pushdown, early_filter, materialize_cte)
  - Apply safe optimizations by pushing filters into CTEs early, materializing dimension filters first, and reducing intermediate results through predicate pushdown before joining.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, date_cte_isolate, shared_dimension_multi_channel)
  - Isolate filtered dimension tables (customer_demographics, date_dim, promotion) into separate CTEs before joining with catalog_sales to enable better join planning and reduce fact table scans.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Restructure joins by prefiltering dimensions and pre-joining them with the fact table in staged CTEs, then consolidate aggregations in a single pass to minimize intermediate data movement.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Transform the query structure by splitting the OR condition on promotion into UNION ALL branches, converting potential subquery patterns, and decorrelating joins for better parallelism and index usage

**Reanalyze insight**: All attempts failed because they only addressed dimension table filtering without tackling the core bottleneck: scanning and joining the massive catalog_sales fact table (likely the dominant cost). The CTE-based pre-filtering approaches (used by all workers) are redundant because DuckDB's optimizer ...

---

### query_28 [NEUTRAL]

- **Baseline**: 1676.8 ms (1 rows)
- **Best speedup**: 1.05x (W1, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.05x | NEUTRAL | BEST |
| W2 | 0 | 1.02x | NEUTRAL |  |
| W3 | 0 | 0.57x | REGRESSION |  |
| W4 | 0 | 0.78x | REGRESSION |  |
| W5 | 1 | 0.66x | REGRESSION |  |
| W6 | 2 | 0.60x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_safe_optimizations` (pushdown, materialize_cte, early_filter)
  - Apply filter pushdown to ensure each subquery filters rows early, use CTE materialization to avoid redundant computation, and filter the fact table before aggregation for minimal risk.
- W2: `moderate_cte_restructuring` (dimension_cte_isolate, multi_date_range_cte, union_cte_split)
  - Restructure using CTEs to isolate each quantity range as a separate filtered dataset, enabling better predicate pushdown and potential plan optimization for each range.
- W3: `aggressive_single_pass` (single_pass_aggregation, prefetch_fact_join, shared_dimension_multi_channel)
  - Consolidate all six subqueries into a single table scan with conditional aggregation, prefetch filtered data into CTEs, and share common filter logic across ranges.
- W4: `novel_or_to_union_transform` (or_to_union, composite_decorrelate_union, intersect_to_exists)
  - Transform OR conditions into UNION ALL branches for better index usage, decorrelate multiple disjunctive filters, and apply intersect/exists conversions for structural novelty.

**Reanalyze insight**: All attempts fell short because they failed to address the fundamental performance bottleneck: repeated full-table scans and inefficient OR predicate evaluation. Worker 1's CTE materialization (1.05x) added overhead without reducing scan count. Worker 3's single-pass aggregation (0.57x) overloaded C...

---

### query_54 [NEUTRAL]

- **Baseline**: 252.8 ms (0 rows)
- **Best speedup**: 1.05x (W2, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | --- | ERROR | Binder Error |
| W2 | 0 | 1.05x | NEUTRAL | BEST |
| W3 | 0 | --- | ERROR | Binder Error |
| W4 | 0 | 0.98x | NEUTRAL |  |
| W5 | 1 | 0.93x | REGRESSION |  |
| W6 | 2 | 1.01x | NEUTRAL |  |

**Strategies assigned:**

- W1: `conservative_early_reduction` (early_filter, pushdown, materialize_cte)
  - Push filters into CTEs early to reduce fact table joins, materialize repeated subquery patterns, and apply predicate pushdown to minimize intermediate data.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate date and dimension filtering into dedicated CTEs before joining with fact tables, enabling reuse across multiple sales channels and reducing redundant scans.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, multi_dimension_prefetch, union_cte_split)
  - Pre-filter and pre-join dimension tables with fact tables in separate CTEs, then restructure the UNION into specialized branches for better join optimization and partition pruning.
- W4: `novel_structural_transform` (decorrelate, intersect_to_exists, or_to_union)
  - Transform correlated subqueries into join-efficient CTEs, convert hypothetical INTERSECT patterns to EXISTS, and explore splitting OR conditions into UNION ALL for index-friendly access.

**Reanalyze insight**: All attempts failed to reach 2.0x due to three core issues: 1) Early CTE materialization of large fact tables (catalog_sales/web_sales) before applying customer joins created massive intermediate results (billions of rows), 2) Repeated scanning of date_dim for different purposes without leveraging p...

---

### query_3 [NEUTRAL]

- **Baseline**: 195.7 ms (67 rows)
- **Best speedup**: 1.04x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.94x | REGRESSION |  |
| W2 | 0 | 0.98x | NEUTRAL |  |
| W3 | 0 | 1.04x | NEUTRAL |  |
| W4 | 0 | 0.97x | NEUTRAL |  |
| W5 | 1 | 1.04x | NEUTRAL | BEST |
| W6 | 2 | 0.05x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_early_filtering` (early_filter, pushdown, date_cte_isolate)
  - Apply aggressive predicate pushdown by pre‑filtering dimension tables in CTEs before joining, minimizing fact‑table rows early.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, multi_dimension_prefetch, multi_date_range_cte)
  - Isolate each filtered dimension (date_dim, item) into separate CTEs, then join with the fact table, enabling better predicate pushdown and join ordering.
- W3: `aggressive_cte_restructuring` (prefetch_fact_join, shared_dimension_multi_channel, single_pass_aggregation)
  - Restructure into a multi‑step CTE pipeline: pre‑filter dimensions, pre‑join with facts, then aggregate in a single pass, sharing filtered dimensions across channels.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, decorrelate)
  - Apply structural transformations such as splitting OR conditions into UNION ALL, converting INTERSECT to EXISTS, and decorrelating subqueries to unlock alternative join plans.

**Reanalyze insight**: All previous attempts (0.94x-1.04x) fell short because they only applied structural reorganization without addressing the fundamental execution bottlenecks. The CTE-based approaches merely restructured syntax without changing the join algorithms or data access patterns. DuckDB's optimizer already pu...

---

### query_69 [NEUTRAL]

- **Baseline**: 230.5 ms (100 rows)
- **Best speedup**: 1.04x (W3, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.98x | NEUTRAL |  |
| W2 | 0 | 0.99x | NEUTRAL |  |
| W3 | 0 | 1.04x | NEUTRAL | BEST |
| W4 | 0 | 1.02x | NEUTRAL |  |
| W5 | 1 | 0.93x | REGRESSION |  |
| W6 | 2 | 0.71x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_pushdown_earlyfilter` (early_filter, pushdown, materialize_cte)
  - Push filters down to dimension tables early and materialize repeated date filter to reduce row counts before joins.
- W2: `moderate_dimension_date_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate date and dimension filters into CTEs to enable reuse and reduce redundant joins across multiple sales channels.
- W3: `aggressive_decorrelate_multi_cte` (multi_date_range_cte, composite_decorrelate_union, prefetch_fact_join)
  - Decorrelate subqueries by precomputing customer sets for each sales channel in separate CTEs, using a shared date CTE, and prefetch fact joins.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, single_pass_aggregation)
  - Explore transforming NOT EXISTS to set operations (INTERSECT/EXCEPT) and consolidating multiple fact table scans into a single pass.

**Reanalyze insight**: All previous attempts fell short because they failed to address the fundamental bottleneck: repeated full scans of massive fact tables (store_sales, web_sales, catalog_sales) for each customer evaluation. While they attempted decorrelation via precomputed CTEs (Workers 3,5), these CTEs still perform...

---

### query_71 [NEUTRAL]

- **Baseline**: 281.4 ms (10293 rows)
- **Best speedup**: 1.04x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.90x | REGRESSION |  |
| W2 | 0 | 0.97x | NEUTRAL |  |
| W3 | 0 | 0.94x | REGRESSION |  |
| W4 | 0 | 0.98x | NEUTRAL |  |
| W5 | 1 | 1.04x | NEUTRAL | BEST |
| W6 | 2 | 0.90x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_pushdown_filter` (pushdown, early_filter, materialize_cte)
  - Apply basic filter pushdown to reduce data early in each subquery, pre-filter dimension tables before joins, and materialize repeated date filtering patterns into a shared CTE.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate date and item dimension filters into separate CTEs before joining with fact tables, enabling predicate pushdown and reuse across all sales channels.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-join filtered dimensions with each fact table in separate CTEs, then restructure the UNION ALL to consolidate aggregation logic and reduce passes over large tables.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, union_cte_split)
  - Transform OR conditions on t_meal_time into UNION ALL branches, restructure the existing UNION ALL into specialized CTEs by channel, and explore EXISTS-based rewrites for potential join elimination.

**Reanalyze insight**: All attempts fell short because they focused solely on dimension filtering pushdown while missing the fundamental bottleneck: the massive UNION ALL of three large fact tables before any aggregation. DuckDB's optimizer can't push the item and time filters through the UNION ALL, forcing a full scan of...

---

### query_17 [NEUTRAL]

- **Baseline**: 405.6 ms (0 rows)
- **Best speedup**: 1.04x (W2, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.98x | NEUTRAL |  |
| W2 | 0 | 1.04x | NEUTRAL | BEST |
| W3 | 0 | 0.97x | NEUTRAL |  |
| W4 | 0 | --- | ERROR | Binder Error |
| W5 | 1 | 1.02x | NEUTRAL |  |
| W6 | 2 | --- | ERROR | Binder Error |

**Strategies assigned:**

- W1: `conservative_filter_pushdown` (early_filter, pushdown, materialize_cte)
  - Apply safe, incremental improvements by aggressively filtering dimension tables first, pushing filters into CTEs, and materializing repeated patterns to reduce intermediate data volume early.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, multi_date_range_cte)
  - Restructure by isolating each filtered dimension (date, store, item) into separate CTEs before joining to facts, enabling better predicate pushdown and join planning for multi-date scenarios.
- W3: `aggressive_prefetch_restructuring` (prefetch_fact_join, multi_dimension_prefetch, shared_dimension_multi_channel)
  - Pre-filter and pre-join dimensions with their respective fact tables in staged CTEs, then combine channel results, optimizing for shared dimension filters across sales channels.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, decorrelate)
  - Apply non-traditional structural transformations: convert implicit OR logic in joins to UNION ALL branches, transform intersection patterns to EXISTS, and decorrelate subquery-like joins.

**Reanalyze insight**: All attempts fell short because they failed to address the core bottleneck: the three-way join between large fact tables (store_sales, store_returns, catalog_sales) with multi-key equality conditions (customer_sk, item_sk, ticket_number). The CTE-based approaches merely reshuffled the same joins wit...

---

### query_22 [NEUTRAL]

- **Baseline**: 5724.7 ms (100 rows)
- **Best speedup**: 1.04x (W3, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.98x | NEUTRAL |  |
| W2 | 0 | 1.02x | NEUTRAL |  |
| W3 | 0 | 1.04x | NEUTRAL | BEST |
| W4 | 0 | 1.01x | NEUTRAL |  |
| W5 | 1 | 1.01x | NEUTRAL |  |
| W6 | 2 | --- | ERROR | Parser/Syntax Error |

**Strategies assigned:**

- W1: `conservative_early_filter_pushdown` (early_filter, pushdown, date_cte_isolate)
  - Apply safe, proven optimizations by filtering dimension tables first, pushing predicates down, and isolating date ranges early to reduce fact table joins.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, materialize_cte, multi_date_range_cte)
  - Restructure with CTEs to isolate filtered dimension tables, materialize intermediate results, and handle date ranges separately before joining with fact tables.
- W3: `aggressive_prefetch_restructuring` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Use aggressive prefetching by pre-joining filtered dimensions with facts in CTEs, and consolidate operations into single-pass aggregation to minimize scans.
- W4: `novel_structural_transforms` (or_to_union, composite_decorrelate_union, intersect_to_exists)
  - Apply structural query transformations by converting OR conditions to UNION ALL, decorrelating subqueries, and rethinking intersection patterns for better join planning.

**Reanalyze insight**: All workers essentially applied the same optimization: isolating date_dim filtering into a CTE. While this reduces the dimension table early, it doesn't address the core bottleneck. The real issue is that inventory is the massive fact table, and the query must:
1. Scan/join the entire date-filtered ...

---

### query_34 [NEUTRAL]

- **Baseline**: 356.4 ms (4543 rows)
- **Best speedup**: 1.03x (W2, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.01x | NEUTRAL |  |
| W2 | 0 | 1.03x | NEUTRAL | BEST |
| W3 | 0 | 1.01x | NEUTRAL |  |
| W4 | 0 | 1.00x | NEUTRAL |  |
| W5 | 1 | 0.90x | REGRESSION |  |
| W6 | 2 | --- | ERROR | Binder Error |

**Strategies assigned:**

- W1: `conservative_pushdown_filter` (early_filter, pushdown, materialize_cte)
  - Apply predicate pushdown and early filtering to reduce join sizes, using materialized CTEs for safe, incremental improvements.
- W2: `moderate_dimension_prefilter` (date_cte_isolate, dimension_cte_isolate, multi_dimension_prefetch)
  - Pre-filter all dimension tables into separate CTEs before joining with fact table, isolating date ranges and dimension constraints early.
- W3: `aggressive_fact_prefetch` (prefetch_fact_join, single_pass_aggregation, multi_date_range_cte)
  - Pre-join filtered dimensions with fact table in CTEs, then aggregate in a single pass, optimizing for reduced intermediate data movement.
- W4: `novel_or_transformation` (or_to_union, intersect_to_exists, decorrelate)
  - Transform OR conditions into UNION ALL branches, restructure complex filtering patterns, and eliminate implicit correlations for novel execution paths.

**Reanalyze insight**: All previous attempts (1.01x-1.03x) failed because they only rearranged existing filters into CTEs without fundamentally changing the execution pattern. DuckDB's optimizer already pushes down these simple filters, making CTE materialization redundant overhead. The attempts overlooked: 1) The core bo...

---

### query_29 [NEUTRAL]

- **Baseline**: 451.6 ms (4 rows)
- **Best speedup**: 1.03x (W2, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | --- | ERROR | Binder Error |
| W2 | 0 | 1.03x | NEUTRAL | BEST |
| W3 | 0 | 0.87x | REGRESSION |  |
| W4 | 0 | 1.01x | NEUTRAL |  |
| W5 | 1 | 1.00x | NEUTRAL |  |
| W6 | 2 | 0.30x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_pushdown_filter` (early_filter, pushdown, materialize_cte)
  - Apply early filtering on dimension tables (date_dim, store, item), push predicates into CTEs, and materialize repeated subquery patterns to reduce join sizes.
- W2: `moderate_date_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, multi_date_range_cte)
  - Pre-filter each date_dim alias (d1, d2, d3) and other dimension tables into separate CTEs before joining with fact tables to leverage predicate pushdown.
- W3: `aggressive_multi_stage_prefetch` (prefetch_fact_join, multi_dimension_prefetch, shared_dimension_multi_channel)
  - Pre-join filtered dimensions (date/store/item) with their respective fact tables in staged CTEs, then combine pre-aggregated results to minimize large intermediate joins.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Explore converting the d3.d_year IN clause to UNION ALL branches, apply EXISTS-style join transformations, and decorrelate multi-fact joins via distinct customer/item CTEs.

**Reanalyze insight**: All attempts fell short because they failed to fundamentally restructure the star-join pattern. Worker 1's error stemmed from attempting to push filters into subqueries without maintaining column references. Workers 2, 4, and 5 achieved only ~1.0x because they merely prefetched dimension filters wit...

---

### query_42 [NEUTRAL]

- **Baseline**: 159.8 ms (11 rows)
- **Best speedup**: 1.03x (W3, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.02x | NEUTRAL |  |
| W2 | 0 | 1.00x | NEUTRAL |  |
| W3 | 0 | 1.03x | NEUTRAL | BEST |
| W4 | 0 | 1.01x | NEUTRAL |  |
| W5 | 1 | 0.97x | NEUTRAL |  |
| W6 | 2 | 0.95x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_early_filtering` (early_filter, pushdown, date_cte_isolate)
  - Apply safe predicate pushdown to filter dimension tables first, then join to reduce fact table processing with minimal structural changes.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, multi_dimension_prefetch, materialize_cte)
  - Pre-filter both date and item dimensions into separate CTEs before joining with fact table, materializing filtered dimensions to enable better join planning.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, multi_date_range_cte, shared_dimension_multi_channel)
  - Restructure with multi-stage CTEs—first filter dimensions, then pre-join with fact table in separate CTEs—optimizing for DuckDB's join ordering and parallelism.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Explore radical query restructuring using UNION ALL splits and EXISTS transformations even where not immediately obvious, testing DuckDB's optimizer boundaries.

**Reanalyze insight**: All previous attempts failed because they only applied superficial query restructuring without addressing the fundamental bottlenecks. The original query already has efficient predicate pushdown (dt.d_moy=11, dt.d_year=2002, item.i_manager_id=1), so simply moving filters into CTEs provides no benefi...

---

### query_46 [NEUTRAL]

- **Baseline**: 582.3 ms (100 rows)
- **Best speedup**: 1.02x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.00x | NEUTRAL |  |
| W2 | 0 | 1.01x | NEUTRAL |  |
| W3 | 0 | 0.94x | REGRESSION |  |
| W4 | 0 | 0.98x | NEUTRAL |  |
| W5 | 1 | 1.02x | NEUTRAL | BEST |
| W6 | 2 | --- | ERROR | Catalog Error |

**Strategies assigned:**

- W1: `conservative_dimension_pushdown` (early_filter, pushdown, materialize_cte)
  - Apply safe dimension filtering first to reduce join volume, push filters into subqueries, and materialize repetitive patterns for clarity and potential reuse.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, date_cte_isolate, shared_dimension_multi_channel)
  - Isolate all filtered dimension tables (date, store, household_demographics, customer_address) into separate CTEs before joining with fact table to enable parallel filtering and predicate pushdown.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-filter and pre-join dimensions in a coordinated pipeline, then join with fact table in a single optimized pass to minimize intermediate result sizes.
- W4: `novel_structural_transform` (or_to_union, decorrelate, intersect_to_exists)
  - Transform OR conditions on different columns into UNION ALL, restructure the self-join pattern using decorrelation techniques, and consider alternative join formulations for better plan selection.

**Reanalyze insight**: All attempts failed because they merely rewrote the query using CTEs for dimension filtering without addressing fundamental bottlenecks. DuckDB's optimizer already pushes down filters automatically, making these CTE rewrites syntactic sugar with zero performance benefit (1.00-1.02x). The real bottle...

---

### query_78 [NEUTRAL]

- **Baseline**: 2778.2 ms (100 rows)
- **Best speedup**: 1.01x (W3, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 1.00x | NEUTRAL |  |
| W2 | 0 | 1.00x | NEUTRAL |  |
| W3 | 0 | 1.01x | NEUTRAL | BEST |
| W4 | 0 | 0.47x | REGRESSION |  |
| W5 | 1 | 0.66x | REGRESSION |  |
| W6 | 2 | 0.39x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_pushdown_filter` (pushdown, early_filter, materialize_cte)
  - Push filters into CTEs early, pre-filter dimension tables to reduce fact table joins, and ensure repeated subquery patterns are materialized for reuse.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate date filtering into a reusable CTE, pre-filter all dimension tables separately, and extract shared dimension logic across web/store/catalog CTEs.
- W3: `aggressive_prefetch_consolidation` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-join filtered dimensions with facts in dedicated CTEs, prefetch multiple dimension filters simultaneously, and consolidate the three channel CTEs into a single aggregation pass.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, decorrelate)
  - Transform the OR condition in the final WHERE into UNION ALL branches, convert implicit set logic to EXISTS for better join planning, and decorrelate any hidden subquery dependencies.

**Reanalyze insight**: All attempts fell short because they failed to address the fundamental bottleneck: massive redundant scanning of three large fact tables (store_sales, web_sales, catalog_sales) with anti-joins against their respective returns tables. Workers 1-3 only applied superficial filter pushdowns but kept the...

---

### query_85 [NEUTRAL]

- **Baseline**: 233.7 ms (13 rows)
- **Best speedup**: 1.01x (W6, iter2)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | --- | ERROR | Binder Error |
| W2 | 0 | --- | ERROR | Binder Error |
| W3 | 0 | --- | ERROR | Binder Error |
| W4 | 0 | 0.78x | FAIL | row mismatch |
| W5 | 1 | 0.28x | REGRESSION |  |
| W6 | 2 | 1.01x | NEUTRAL | BEST |

**Strategies assigned:**

- W1: `conservative_early_filter_pushdown` (early_filter, pushdown, date_cte_isolate)
  - Filter dimension tables first to reduce row counts, push predicates into CTEs, and isolate date filtering early to minimize fact table joins.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, multi_date_range_cte, multi_dimension_prefetch)
  - Pre-filter all dimension tables into separate CTEs before joining, handle date ranges separately, and prefetch multiple filtered dimensions to reduce join complexity.
- W3: `aggressive_prefetch_consolidation` (prefetch_fact_join, shared_dimension_multi_channel, single_pass_aggregation)
  - Pre-join filtered dimensions with the fact table early, extract shared dimension filters, and consolidate aggregations into single-pass CTEs to minimize repeated scans.
- W4: `novel_or_union_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Split OR conditions into UNION ALL branches for better predicate pushdown, transform set operations to EXISTS, and decorrelate multiple conditions using distinct CTEs.

**Reanalyze insight**: All failed attempts share a critical misunderstanding: they attempted to push down OR conditions by pre-filtering dimension tables in CTEs, but this breaks the query logic because the original query contains complex correlated OR conditions that require checking dimension values against specific fac...

---

### query_96 [NEUTRAL]

- **Baseline**: 185.8 ms (1 rows)
- **Best speedup**: 1.00x (W4, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.97x | NEUTRAL |  |
| W2 | 0 | 0.91x | REGRESSION |  |
| W3 | 0 | 0.95x | REGRESSION |  |
| W4 | 0 | 1.00x | NEUTRAL | BEST |
| W5 | 1 | 0.99x | NEUTRAL |  |
| W6 | 2 | 0.18x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_pushdown_filter` (early_filter, pushdown, materialize_cte)
  - Apply early filtering and predicate pushdown to dimension tables before joining with the fact table, and materialize intermediate results to reduce redundant scans.
- W2: `dimension_isolation` (date_cte_isolate, dimension_cte_isolate, multi_dimension_prefetch)
  - Isolate dimension table filters (time, household, store) into separate CTEs to enable independent optimization and reduce fact table join cardinality.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, single_pass_aggregation, shared_dimension_multi_channel)
  - Pre-join filtered dimensions with fact tables in staged CTEs, then consolidate aggregations into a single pass while sharing filtered dimension sets.
- W4: `structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Transform query structure by converting implicit OR conditions to UNION ALL, rewriting INTERSECT patterns as EXISTS, and decorrelating subqueries for better join planning.

**Reanalyze insight**: All workers fell short because they implemented essentially the same optimization: early filtering of dimension tables via CTEs followed by fact table joins. This approach fails because:
1. DuckDB's optimizer already performs predicate pushdown automatically in the original star-join pattern.
2. CTE...

---

### query_25 [NEUTRAL]

- **Baseline**: 89.4 ms (0 rows)
- **Best speedup**: 0.95x (W3, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.27x | REGRESSION |  |
| W2 | 0 | 0.28x | REGRESSION |  |
| W3 | 0 | 0.95x | NEUTRAL | BEST |
| W4 | 0 | --- | ERROR | Binder Error |
| W5 | 1 | 0.32x | REGRESSION |  |
| W6 | 2 | 0.06x | FAIL | row mismatch |

**Strategies assigned:**

- W1: `conservative_filter_pushdown` (pushdown, early_filter, materialize_cte)
  - Apply standard filter pushdown and early reduction techniques—pre-filter dimension tables first, then join to facts, and materialize repeating patterns to avoid recomputation.
- W2: `moderate_date_dimension_isolation` (multi_date_range_cte, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate each date_dim alias into separate CTEs with their respective filters, then pre-filter store and item dimensions before joining with the fact CTEs.
- W3: `aggressive_multi_fact_prefetch` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-join filtered dimensions with each fact table in separate CTEs, then combine results; consider consolidating multiple fact scans into single-pass aggregations where possible.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, decorrelate)
  - Explore radical restructuring—convert implicit joins to explicit UNION branches, transform intersection logic to EXISTS, and decorrelate subquery patterns.

**Reanalyze insight**: All attempts suffered from two fundamental issues: 1) Excessive CTE fragmentation that created optimization barriers, preventing DuckDB's optimizer from pushing filters and reordering joins effectively. Each CTE became an optimization fence, blocking join reordering and predicate pushdown that the o...

---

### query_91 [REGRESSION]

- **Baseline**: 33.9 ms (21 rows)
- **Best speedup**: 0.83x (W1, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.83x | REGRESSION | BEST |
| W2 | 0 | 0.82x | REGRESSION |  |
| W3 | 0 | 0.02x | REGRESSION |  |
| W4 | 0 | 0.70x | FAIL | row mismatch |
| W5 | 1 | 0.62x | REGRESSION |  |
| W6 | 2 | 0.77x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_early_filtering` (early_filter, pushdown, materialize_cte)
  - Apply dimension table filtering first to reduce rows early, push predicates down, and materialize filtered subsets to avoid repeated computation during joins.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, date_cte_isolate, shared_dimension_multi_channel)
  - Isolate all filtered dimension tables (including date_dim) into separate CTEs before joining, enabling parallel filtering and reuse of dimension subsets.
- W3: `aggressive_prefetch_restructuring` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-join filtered dimension CTEs with the fact table in stages, and consolidate operations to minimize passes over large tables while maintaining filter pushdown.
- W4: `novel_structure_transformation` (or_to_union, composite_decorrelate_union, intersect_to_exists)
  - Transform OR conditions on different columns into UNION ALL branches, apply decorrelation techniques, and experiment with EXISTS transformations for better join planning.

**Reanalyze insight**: All workers fell short due to improper join ordering and missed opportunities to reduce the largest fact table early. Worker 1-2 isolated dimension filtering but still joined all filtered dimensions to customer before joining to catalog_returns, forcing unnecessary work on the customer table. Worker...

---

### query_31 [REGRESSION]

- **Baseline**: 442.3 ms (307 rows)
- **Best speedup**: 0.81x (W5, iter1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.76x | REGRESSION |  |
| W2 | 0 | 0.77x | REGRESSION |  |
| W3 | 0 | 0.77x | REGRESSION |  |
| W4 | 0 | --- | ERROR | Not Implemented |
| W5 | 1 | 0.81x | REGRESSION | BEST |
| W6 | 2 | 0.67x | REGRESSION |  |

**Strategies assigned:**

- W1: `conservative_predicate_pushdown` (early_filter, pushdown, materialize_cte)
  - Push filters into CTEs early to reduce intermediate data volume, apply predicate pushdown to dimension joins, and consider materializing CTEs for reuse in the multi-instance self-join.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Pre-filter date_dim and customer_address into isolated CTEs before joining with fact tables, then share these filtered dimensions across both store and web sales CTEs to eliminate redundant filtering.
- W3: `aggressive_prefetch_restructuring` (multi_dimension_prefetch, prefetch_fact_join, single_pass_aggregation)
  - Pre-join filtered dimensions with fact tables in separate CTEs, then restructure the quarter comparisons using conditional aggregation in a single CTE pass instead of six self-joins.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, decorrelate)
  - Transform the complex self-join pattern using UNION-based rewrites, explore EXISTS for quarter progression logic, and decorrelate any implied dependencies between store and web sales comparisons.

**Reanalyze insight**: All attempts fell short because they only optimized the dimension filtering phase while leaving the core bottleneck untouched: the six-way self-join (three instances each of ss and ws) with complex ratio comparisons. The attempts merely pushed date filters earlier (year=2000, quarters 1-3) into CTEs...

---

### query_9 [REGRESSION]

- **Baseline**: 1366.1 ms (1 rows)
- **Best speedup**: 0.42x (W1, iter0)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 0.42x | REGRESSION | BEST |
| W2 | 0 | 0.39x | REGRESSION |  |
| W3 | 0 | 0.27x | REGRESSION |  |
| W4 | 0 | 0.39x | REGRESSION |  |
| W5 | 1 | 0.41x | REGRESSION |  |
| W6 | 2 | --- | ERROR | Catalog Error |

**Strategies assigned:**

- W1: `conservative_single_pass` (pushdown, single_pass_aggregation, materialize_cte)
  - Consolidate the five independent store_sales scans into a single pass with conditional aggregates using CASE statements, then push filters and materialize the results to avoid redundant computation.
- W2: `moderate_dimension_prefilter` (dimension_cte_isolate, early_filter, shared_dimension_multi_channel)
  - Pre-filter the reason table first into a CTE, then compute all bucket aggregates from store_sales in a separate CTE, ensuring dimension filtering happens early and shared across all calculations.
- W3: `aggressive_fact_prefetch` (prefetch_fact_join, multi_dimension_prefetch, multi_date_range_cte)
  - Create a pre-filtered fact CTE first, then compute all conditional aggregates from it, optimizing for fact table scan efficiency even though no join exists in the original query.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Transform the CASE/WHEN logic into UNION ALL branches for each quantity range threshold condition, then use EXISTS-style logic to simulate the conditional aggregation behavior.

**Reanalyze insight**: All attempts fell short because they failed to address the fundamental bottleneck: DuckDB's columnar execution engine struggles with computing 15 different conditional aggregates (5 counts + 10 averages) in a single scan when they involve overlapping CASE conditions. The original query's independent...

---

### query_2 [ALL FAIL]

- **Baseline**: 606.6 ms (2513 rows)
- **Best speedup**: --- (W-1, iter-1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 2.08x | FAIL | row mismatch |
| W2 | 0 | 2.43x | FAIL | row mismatch |
| W3 | 0 | 1.51x | FAIL | row mismatch |
| W4 | 0 | 2.39x | FAIL | row mismatch |
| W5 | 1 | 1.23x | FAIL | row mismatch |
| W6 | 2 | 1.83x | FAIL | row mismatch |

**Strategies assigned:**

- W1: `conservative_early_filtering` (early_filter, pushdown, date_cte_isolate)
  - Push year filters into the earliest possible CTEs and isolate date_dim filtering to reduce data volume before expensive joins and aggregations.
- W2: `moderate_cte_restructure` (materialize_cte, dimension_cte_isolate, shared_dimension_multi_channel)
  - Restructure CTEs to avoid recomputation, pre-filter dimension tables, and consolidate shared dimension logic across both sales channels.
- W3: `aggressive_prefetch_split` (union_cte_split, multi_dimension_prefetch, prefetch_fact_join)
  - Split the UNION ALL into channel-specific CTEs, prefilter multiple dimensions, and pre-join filtered dimensions with fact tables to minimize intermediate results.
- W4: `novel_structural_transform` (single_pass_aggregation, deferred_window_aggregation, or_to_union)
  - Transform the two-year comparison into a single-pass aggregation using conditional logic, explore window-function alternatives, and consider UNION restructuring for branching logic.

**Reanalyze insight**: All workers attempted CTE restructuring and early filtering but missed the fundamental bottleneck: the UNION ALL of two massive fact tables without any date filtering creates an enormous intermediate result (wscs) that dominates query cost. Worker 3 attempted prefetch joins but incorrectly split the...

---

### query_23 [ALL FAIL]

- **Baseline**: 12754.6 ms (5 rows)
- **Best speedup**: --- (W-1, iter-1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 2.63x | FAIL | row mismatch |
| W2 | 0 | 1.94x | FAIL | row mismatch |
| W3 | 0 | 3.13x | FAIL | row mismatch |
| W4 | 0 | 2.84x | FAIL | row mismatch |
| W5 | 1 | 2.90x | FAIL | row mismatch |
| W6 | 2 | --- | ERROR | Binder Error |

**Strategies assigned:**

- W1: `conservative_pushdown_filter` (early_filter, pushdown, materialize_cte)
  - Apply safe, proven optimizations: filter dimension tables early, push predicates into CTEs, and materialize repeated subqueries to avoid recomputation.
- W2: `moderate_date_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, shared_dimension_multi_channel)
  - Isolate date and dimension filtering into dedicated CTEs to enable predicate pushdown across all channel queries (store/catalog/web) and reduce redundant scans.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-filter and pre-join dimensions with facts in CTEs, then consolidate multiple subquery scans into single-pass aggregations to minimize I/O and intermediate results.
- W4: `novel_structural_transform` (intersect_to_exists, or_to_union, decorrelate)
  - Transform subquery patterns (IN → EXISTS/joins), convert OR conditions to UNION ALL branches, and decorrelate dependent subqueries to enable better join planning and parallelism.

**Reanalyze insight**: All previous attempts failed to consistently reach 2.0x speedup because they didn't address the root cause: multiple full scans of massive fact tables (store_sales, catalog_sales, web_sales) due to correlated subqueries and disconnected optimizations. Worker 2's date isolation (1.94x) helped but sti...

---

### query_24 [ALL FAIL]

- **Baseline**: 913.1 ms (0 rows)
- **Best speedup**: --- (W-1, iter-1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | --- | ERROR | Binder Error |
| W2 | 0 | 1.94x | FAIL | row mismatch |
| W3 | 0 | 0.78x | FAIL | row mismatch |
| W4 | 0 | --- | ERROR | Binder Error |
| W5 | 1 | 0.76x | FAIL | row mismatch |
| W6 | 2 | 1.99x | FAIL | row mismatch |

**Strategies assigned:**

- W1: `conservative_basic_optimizations` (early_filter, pushdown, materialize_cte)
  - Apply early filtering on dimension tables before joining with facts, push predicates into CTE definitions, and materialize repeated CTEs to avoid recomputation across queries.
- W2: `moderate_dimension_isolation` (dimension_cte_isolate, shared_dimension_multi_channel, date_cte_isolate)
  - Pre-filter all dimension tables into separate CTEs before joining, extract shared dimension constraints (like market_id), and isolate date-like filters even though none exist—apply similarly to custom
- W3: `aggressive_restructure_with_prefetch` (prefetch_fact_join, multi_dimension_prefetch, single_pass_aggregation)
  - Pre-join filtered dimensions with fact tables in CTEs, prefetch multiple dimension filters into a single CTE, and consolidate the two color queries into one pass with conditional aggregation.
- W4: `novel_structural_transforms` (or_to_union, intersect_to_exists, decorrelate)
  - Convert OR conditions (like birth_country <> country) into UNION ALL branches, transform HAVING subqueries to decorrelated joins, and re-express set operations as EXISTS patterns if they appear.

**Reanalyze insight**: Worker1 failed due to aliasing errors when restructuring joins. Worker2 achieved 1.94x but fell just short by not eliminating redundant work: it still computes the expensive base CTE twice (once per color query). Worker3's consolidation attempt (0.78x) performed worse because the "single-pass aggreg...

---

### query_39 [ALL FAIL]

- **Baseline**: 3201.9 ms (52 rows)
- **Best speedup**: --- (W-1, iter-1)

| Worker | Iter | Speedup | Status | Notes |
|-------:|-----:|--------:|--------|-------|
| W1 | 0 | 11.05x | FAIL | row mismatch |
| W2 | 0 | 8.72x | FAIL | row mismatch |
| W3 | 0 | 8.79x | FAIL | row mismatch |
| W4 | 0 | 12.85x | FAIL | row mismatch |
| W5 | 1 | 12.52x | FAIL | row mismatch |
| W6 | 2 | --- | ERROR | Binder Error |

**Strategies assigned:**

- W1: `conservative_filter_pushdown` (pushdown, early_filter, materialize_cte)
  - Push filters early into CTEs, isolate dimension table filtering, and materialize the repeated inv CTE to avoid redundant computation across both query blocks.
- W2: `moderate_dimension_isolation` (date_cte_isolate, dimension_cte_isolate, multi_dimension_prefetch)
  - Pre-filter date_dim and other dimension tables into dedicated CTEs before joining with the large fact table, reducing join cardinality early.
- W3: `aggressive_prefetch_restructure` (prefetch_fact_join, single_pass_aggregation, multi_date_range_cte)
  - Restructure the query to prefetch filtered dimension-fact joins, then compute both month aggregates in a single pass, potentially pivoting d_moy values.
- W4: `novel_structural_transform` (or_to_union, intersect_to_exists, composite_decorrelate_union)
  - Explore transforming the self-join into UNION/EXISTS patterns or decorrelating subquery structures, even if not directly present, to unlock alternative join plans.

**Reanalyze insight**: All attempts fell short because they didn't address the fundamental bottleneck: the query requires calculating expensive statistical functions (STDDEV_SAMP and AVG) over the entire inventory table for two specific months, then filtering by coefficient of variation. Workers 1-3 merely rearranged join...

---

## Appendix: Full Leaderboard

| # | Query | Best Speedup | Worker | Iter | Baseline (ms) | Rows | Status |
|--:|-------|------------:|-------:|-----:|--------------:|-----:|--------|
| 1 | query_88 | 6.24x | W2 | 0 | 1416 | 1 | WIN |
| 2 | query_40 | 5.23x | W3 | 0 | 79 | 100 | WIN |
| 3 | query_95 | 4.69x | W3 | 0 | 2784 | 1 | WIN |
| 4 | query_70 | 2.52x | W1 | 0 | 740 | 7 | WIN |
| 5 | query_36 | 2.47x | W6 | 2 | 879 | 100 | WIN |
| 6 | query_14 | 2.39x | W6 | 2 | 5290 | 100 | WIN |
| 7 | query_35 | 2.36x | W3 | 0 | 735 | 100 | WIN |
| 8 | query_99 | 2.33x | W6 | 2 | 314 | 100 | WIN |
| 9 | query_59 | 2.23x | W3 | 0 | 1794 | 100 | WIN |
| 10 | query_15 | 2.13x | W5 | 1 | 92 | 100 | WIN |
| 11 | query_93 | 2.02x | W6 | 2 | 1051 | 100 | WIN |
| 12 | query_87 | 1.91x | W6 | 2 | 928 | 1 | IMPROVED |
| 13 | query_80 | 1.91x | W2 | 0 | 684 | 100 | IMPROVED |
| 14 | query_65 | 1.84x | W3 | 0 | 1402 | 100 | IMPROVED |
| 15 | query_32 | 1.82x | W6 | 2 | 14 | 1 | IMPROVED |
| 16 | query_92 | 1.81x | W6 | 2 | 71 | 1 | IMPROVED |
| 17 | query_94 | 1.74x | W5 | 1 | 90 | 1 | IMPROVED |
| 18 | query_64 | 1.65x | W6 | 2 | 1420 | 3 | IMPROVED |
| 19 | query_90 | 1.64x | W6 | 2 | 75 | 1 | IMPROVED |
| 20 | query_1 | 1.62x | W3 | 0 | 107 | 100 | IMPROVED |
| 21 | query_62 | 1.62x | W6 | 2 | 199 | 100 | IMPROVED |
| 22 | query_4 | 1.61x | W6 | 2 | 4964 | 100 | IMPROVED |
| 23 | query_27 | 1.58x | W2 | 0 | 701 | 100 | IMPROVED |
| 24 | query_75 | 1.58x | W5 | 1 | 1263 | 100 | IMPROVED |
| 25 | query_18 | 1.57x | W2 | 0 | 302 | 100 | IMPROVED |
| 26 | query_67 | 1.50x | W6 | 2 | 10269 | 100 | IMPROVED |
| 27 | query_57 | 1.49x | W5 | 1 | 663 | 100 | IMPROVED |
| 28 | query_38 | 1.43x | W6 | 2 | 918 | 1 | IMPROVED |
| 29 | query_41 | 1.43x | W3 | 0 | 16 | 16 | IMPROVED |
| 30 | query_86 | 1.39x | W6 | 2 | 132 | 100 | IMPROVED |
| 31 | query_5 | 1.36x | W2 | 0 | 487 | 100 | IMPROVED |
| 32 | query_56 | 1.35x | W5 | 1 | 252 | 100 | IMPROVED |
| 33 | query_61 | 1.34x | W3 | 0 | 11 | 1 | IMPROVED |
| 34 | query_45 | 1.34x | W6 | 2 | 105 | 36 | IMPROVED |
| 35 | query_43 | 1.32x | W6 | 2 | 356 | 18 | IMPROVED |
| 36 | query_51 | 1.28x | W5 | 1 | 4868 | 100 | IMPROVED |
| 37 | query_11 | 1.27x | W3 | 0 | 3539 | 100 | IMPROVED |
| 38 | query_81 | 1.27x | W5 | 1 | 133 | 100 | IMPROVED |
| 39 | query_84 | 1.26x | W4 | 0 | 52 | 100 | IMPROVED |
| 40 | query_16 | 1.25x | W5 | 1 | 25 | 1 | IMPROVED |
| 41 | query_23a | 1.24x | W5 | 1 | 6400 | 1 | IMPROVED |
| 42 | query_30 | 1.24x | W5 | 1 | 113 | 100 | IMPROVED |
| 43 | query_6 | 1.23x | W5 | 1 | 198 | 51 | IMPROVED |
| 44 | query_74 | 1.23x | W5 | 1 | 2271 | 100 | IMPROVED |
| 45 | query_83 | 1.22x | W3 | 0 | 48 | 100 | IMPROVED |
| 46 | query_97 | 1.22x | W5 | 1 | 1181 | 1 | IMPROVED |
| 47 | query_44 | 1.22x | W4 | 0 | 3 | 0 | IMPROVED |
| 48 | query_13 | 1.21x | W2 | 0 | 818 | 1 | IMPROVED |
| 49 | query_82 | 1.21x | W3 | 0 | 136 | 9 | IMPROVED |
| 50 | query_53 | 1.21x | W6 | 2 | 236 | 100 | IMPROVED |
| 51 | query_58 | 1.19x | W5 | 1 | 160 | 5 | IMPROVED |
| 52 | query_79 | 1.18x | W6 | 2 | 589 | 100 | IMPROVED |
| 53 | query_23b | 1.18x | W5 | 1 | 6726 | 5 | IMPROVED |
| 54 | query_68 | 1.17x | W5 | 1 | 517 | 100 | IMPROVED |
| 55 | query_63 | 1.16x | W5 | 1 | 275 | 100 | IMPROVED |
| 56 | query_60 | 1.15x | W5 | 1 | 286 | 100 | IMPROVED |
| 57 | query_33 | 1.15x | W5 | 1 | 233 | 100 | IMPROVED |
| 58 | query_7 | 1.15x | W3 | 0 | 477 | 100 | IMPROVED |
| 59 | query_49 | 1.14x | W2 | 0 | 320 | 41 | IMPROVED |
| 60 | query_37 | 1.13x | W1 | 0 | 83 | 3 | IMPROVED |
| 61 | query_72 | 1.13x | W1 | 0 | 614 | 100 | IMPROVED |
| 62 | query_21 | 1.13x | W5 | 1 | 60 | 100 | IMPROVED |
| 63 | query_73 | 1.12x | W5 | 1 | 318 | 15 | IMPROVED |
| 64 | query_48 | 1.11x | W1 | 0 | 656 | 1 | IMPROVED |
| 65 | query_10 | 1.11x | W2 | 0 | 221 | 75 | IMPROVED |
| 66 | query_76 | 1.11x | W4 | 0 | 317 | 100 | IMPROVED |
| 67 | query_98 | 1.10x | W3 | 0 | 233 | 15226 | NEUTRAL |
| 68 | query_50 | 1.10x | W2 | 0 | 687 | 51 | NEUTRAL |
| 69 | query_66 | 1.08x | W5 | 1 | 197 | 10 | NEUTRAL |
| 70 | query_12 | 1.08x | W5 | 1 | 64 | 100 | NEUTRAL |
| 71 | query_55 | 1.08x | W5 | 1 | 146 | 100 | NEUTRAL |
| 72 | query_19 | 1.08x | W1 | 0 | 284 | 100 | NEUTRAL |
| 73 | query_47 | 1.07x | W5 | 1 | 1270 | 100 | NEUTRAL |
| 74 | query_52 | 1.07x | W5 | 1 | 153 | 100 | NEUTRAL |
| 75 | query_20 | 1.07x | W4 | 0 | 37 | 100 | NEUTRAL |
| 76 | query_77 | 1.07x | W4 | 0 | 284 | 100 | NEUTRAL |
| 77 | query_8 | 1.07x | W2 | 0 | 700 | 6 | NEUTRAL |
| 78 | query_89 | 1.06x | W5 | 1 | 338 | 100 | NEUTRAL |
| 79 | query_26 | 1.05x | W5 | 1 | 212 | 100 | NEUTRAL |
| 80 | query_28 | 1.05x | W1 | 0 | 1677 | 1 | NEUTRAL |
| 81 | query_54 | 1.05x | W2 | 0 | 253 | 0 | NEUTRAL |
| 82 | query_3 | 1.04x | W5 | 1 | 196 | 67 | NEUTRAL |
| 83 | query_69 | 1.04x | W3 | 0 | 230 | 100 | NEUTRAL |
| 84 | query_71 | 1.04x | W5 | 1 | 281 | 10293 | NEUTRAL |
| 85 | query_17 | 1.04x | W2 | 0 | 406 | 0 | NEUTRAL |
| 86 | query_22 | 1.04x | W3 | 0 | 5725 | 100 | NEUTRAL |
| 87 | query_34 | 1.03x | W2 | 0 | 356 | 4543 | NEUTRAL |
| 88 | query_29 | 1.03x | W2 | 0 | 452 | 4 | NEUTRAL |
| 89 | query_42 | 1.03x | W3 | 0 | 160 | 11 | NEUTRAL |
| 90 | query_46 | 1.02x | W5 | 1 | 582 | 100 | NEUTRAL |
| 91 | query_78 | 1.01x | W3 | 0 | 2778 | 100 | NEUTRAL |
| 92 | query_85 | 1.01x | W6 | 2 | 234 | 13 | NEUTRAL |
| 93 | query_96 | 1.00x | W4 | 0 | 186 | 1 | NEUTRAL |
| 94 | query_25 | 0.95x | W3 | 0 | 89 | 0 | NEUTRAL |
| 95 | query_91 | 0.83x | W1 | 0 | 34 | 21 | REGRESSION |
| 96 | query_31 | 0.81x | W5 | 1 | 442 | 307 | REGRESSION |
| 97 | query_9 | 0.42x | W1 | 0 | 1366 | 1 | REGRESSION |
| 98 | query_2 | --- | W-1 | -1 | 607 | 2513 | ERROR |
| 99 | query_23 | --- | W-1 | -1 | 12755 | 5 | ERROR |
| 100 | query_24 | --- | W-1 | -1 | 913 | 0 | ERROR |
| 101 | query_39 | --- | W-1 | -1 | 3202 | 52 | ERROR |

---

*Generated by `generate_review.py` on 2026-02-08*
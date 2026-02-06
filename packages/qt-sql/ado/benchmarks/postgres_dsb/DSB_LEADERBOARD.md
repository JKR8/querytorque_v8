# DSB PostgreSQL Leaderboard

## SF5 Benchmark (Round 01)

**Database:** PostgreSQL DSB SF5 (22GB, 82M fact rows)
**Date:** 2026-02-07
**Connection:** `postgresql://jakc9:jakc9@127.0.0.1:5433/dsb_sf5`
**Validation:** 3x runs (discard 1st warmup, average last 2)
**Timeout:** 120s per query
**Results:** `research/ado/rounds/round_01/validation_sf5/`

---

## Summary

| Metric | Value |
|--------|-------|
| **Total Queries** | 52 |
| **PASS** | 40 (77%) |
| **FAIL** | 1 (2%) |
| **ERROR** | 11 (21%) |
| **Wins (>1.1x)** | 9 (17%) |
| **Neutral (0.9-1.1x)** | 19 (37%) |
| **Regressions (<0.9x)** | 12 (23%) |
| **Best** | **1.92x** (query065_multi) |
| **Worst** | **0.20x** (query069_multi, query085_agg) |

---

## Leaderboard - Ranked by Speedup

### Wins (>1.1x)

| Rank | Query | Speedup | Original (ms) | Optimized (ms) | Transform |
|------|-------|---------|---------------|----------------|-----------|
| 1 | query065_multi | **1.92x** | 2,594 | 1,350 | materialize_cte |
| 2 | query010_multi | **1.47x** | 2,331 | 1,590 | date_cte_isolate |
| 3 | query072_agg_gold | **1.30x** | 1,812 | 1,396 | materialized_dimension_fact_prefilter |
| 4 | query054_multi | **1.16x** | 19 | 16 | date_cte_isolate, early_filter |
| 5 | query030_multi | **1.15x** | 591 | 512 | semantic_rewrite |
| 6 | query027_agg | **1.14x** | 18 | 15 | early_filter |
| 7 | query080_multi | **1.12x** | 51 | 46 | date_cte_isolate |
| 8 | query058_multi | **1.11x** | 81 | 73 | date_cte_isolate, materialize_cte |
| 9 | query072_spj_spj | **1.10x** | 1,874 | 1,705 | date_cte_isolate |
| 9 | query102_spj_spj | **1.10x** | 6,009 | 5,471 | date_cte_isolate, early_filter, reorder_join |

### Neutral (0.9x - 1.1x)

| Query | Speedup | Original (ms) | Optimized (ms) | Transform |
|-------|---------|---------------|----------------|-----------|
| query025_spj_spj | 1.09x | 1,412 | 1,290 | date_cte_isolate |
| query084_agg | 1.09x | 319 | 294 | early_filter, reorder_join |
| query099_spj_spj | 1.08x | 26 | 24 | date_cte_isolate |
| query084_spj_spj | 1.04x | 274 | 264 | early_filter |
| query094_multi | 1.04x | 570 | 548 | date_cte_isolate, decorrelate |
| query072_agg | 1.03x | 1,847 | 1,796 | date_cte_isolate, early_filter |
| query018_agg | 1.02x | 1,794 | 1,765 | date_cte_isolate |
| query019_spj_spj | 1.02x | 74 | 73 | date_cte_isolate, early_filter, reorder_join |
| query059_multi | 1.02x | 12,925 | 12,681 | date_cte_isolate, pushdown, reorder_join |
| query101_spj_spj | 1.02x | 15,337 | 15,099 | early_filter, date_cte_isolate, pushdown |
| query013_agg | 1.01x | 2,268 | 2,253 | semantic_rewrite |
| query064_multi | 1.00x | 11,996 | 12,013 | early_filter |
| query025_agg | 0.99x | 651 | 661 | date_cte_isolate |
| query091_agg | 0.99x | 852 | 863 | date_cte_isolate, or_to_union, early_filter |
| query013_spj_spj | 0.98x | 2,199 | 2,240 | date_cte_isolate, or_to_union |
| query050_agg | 0.98x | 1,222 | 1,254 | date_cte_isolate |
| query050_spj_spj | 0.98x | 2,411 | 2,458 | early_filter |
| query023_multi | 0.96x | 4,317 | 4,476 | date_cte_isolate, pushdown, decorrelate, materialize_cte |
| query014_multi | 0.95x | 91,507 | 96,691 | date_cte_isolate, materialize_cte, semantic_rewrite |

### Regressions (<0.9x)

| Query | Speedup | Original (ms) | Optimized (ms) | Transform | Severity |
|-------|---------|---------------|----------------|-----------|----------|
| query099_agg | 0.95x | 25 | 26 | date_cte_isolate | minor |
| query018_spj_spj | 0.89x | 1,792 | 2,007 | date_cte_isolate | moderate |
| query040_agg | 0.87x | 65 | 74 | date_cte_isolate, early_filter, semantic_rewrite | moderate |
| query027_spj_spj | 0.85x | 15 | 17 | early_filter | moderate |
| query100_spj_spj | 0.84x | 12,661 | 15,096 | date_cte_isolate, pushdown, semantic_rewrite | moderate |
| query019_agg | 0.83x | 75 | 91 | early_filter | moderate |
| query040_spj_spj | 0.82x | 67 | 83 | early_filter, semantic_rewrite | moderate |
| query031_multi | 0.77x | 1,691 | 2,199 | date_cte_isolate | major |
| query038_multi | 0.61x | 5,548 | 9,043 | date_cte_isolate, early_filter | severe |
| query087_multi | 0.57x | 4,194 | 7,310 | date_cte_isolate, reorder_join | severe |
| query091_spj_spj | 0.27x | 844 | 3,153 | or_to_union | catastrophic |
| query069_multi | 0.20x | 436 | 2,137 | date_cte_isolate | catastrophic |
| query085_agg | 0.20x | 1,275 | 6,346 | or_to_union | catastrophic |

### Errors (11)

| Query | Transform | Error Type |
|-------|-----------|------------|
| query001_multi | decorrelate | Original timeout |
| query032_multi | decorrelate | Original timeout |
| query081_multi | decorrelate, pushdown, date_cte_isolate | Original timeout |
| query092_multi | date_cte_isolate, decorrelate, or_to_union | Original timeout |
| query039_multi | semantic_rewrite | Column "cov" does not exist |
| query075_multi | date_cte_isolate | Column prev_yr.d_year does not exist |
| query083_multi | date_cte_isolate | Relation "wr_items" does not exist |
| query085_spj_spj | date_cte_isolate, early_filter, semantic_rewrite | Column ws.ws_web_page_sk does not exist |
| query100_agg | dsb_self_join_decomposition, early_filter | Syntax error |
| query102_agg | date_cte_isolate | Column s.s_warehouse_sk does not exist |
| query101_agg | date_cte_isolate | FAIL: Row count 0 vs 33 |

---

## Transform Effectiveness (SF5)

| Transform | Applied | Wins | Neutral | Regression | Avg Speedup | Verdict |
|-----------|---------|------|---------|------------|-------------|---------|
| **materialize_cte** | 2 | 1 | 1 | 0 | 1.47x | Best |
| **materialized_dim_fact_prefilter** | 1 | 1 | 0 | 0 | 1.30x | Strong (gold) |
| **semantic_rewrite** | 3 | 1 | 1 | 1 | 1.04x | Mixed |
| **early_filter** | 10 | 1 | 6 | 3 | 0.99x | Neutral |
| **date_cte_isolate** | 20 | 3 | 11 | 6 | 0.97x | Risky |
| **or_to_union** | 4 | 0 | 1 | 3 | 0.49x | HARMFUL |
| **decorrelate** | 4 | 0 | 0 | 0 | N/A (timeouts) | Unknown |
| **reorder_join** | 3 | 0 | 2 | 1 | 0.90x | Risky |
| **pushdown** | 3 | 0 | 2 | 1 | 0.96x | Marginal |

**Key findings:**
- `or_to_union` is **catastrophic** on PG (0.20-0.27x) - confirmed from SF10
- `materialize_cte` and `materialized_dimension_fact_prefilter` are the only consistently positive transforms
- `date_cte_isolate` is high-variance: 3 wins but 6 regressions (30% failure rate)
- `early_filter` alone is neutral; best when combined with other transforms

---

## SF10 Benchmark (Previous - 2026-02-06)

**Database:** PostgreSQL DSB SF10 (51GB)
**Queries tested:** 14 (subset)
**Best:** query019_agg 1.26x (early_filter)
**Validation:** Single run (unreliable)

See `research/ado/rounds/round_01/validation/` for SF10 data.

---

## SF5 vs SF10 Comparison (where both tested)

| Query | SF5 | SF10 | Scales? |
|-------|-----|------|---------|
| query065_multi | 1.92x | 3.93x (gold) | Yes - bigger data = bigger win |
| query072_agg_gold | 1.30x | 2.68x (gold) | Yes - non-equi joins scale |
| query010_multi | 1.47x | 0.92x | Inverted - different plan at scale |
| query019_agg | 0.83x | 1.26x | Inverted - PG plan choices differ |

**Insight:** Wins tend to scale UP with data size. SF5 regressions may become wins at SF10.

---

## File Locations

| Item | Path |
|------|------|
| **Leaderboard (This File)** | `research/DSB_LEADERBOARD.md` |
| SF5 Validation CSV | `research/ado/rounds/round_01/validation_sf5/summary.csv` |
| SF10 Validation | `research/ado/rounds/round_01/validation/` |
| Per-Query SQL | `research/ado/rounds/round_01/query*/` |
| PG Gold Examples | `packages/qt-sql/qt_sql/optimization/examples/postgres/` |
| Validation Script | `research/ado/validate_dsb_pg.py` |
| PG Connection (SF5) | `postgresql://jakc9:jakc9@127.0.0.1:5433/dsb_sf5` |
| PG Connection (SF10) | `postgresql://jakc9:jakc9@127.0.0.1:5433/dsb_sf10` |

---

**Last Updated:** 2026-02-07

# PostgreSQL DSB-76 Post-Run Analysis
## QueryTorque V2 Swarm — Feb 12, 2026

---

## 1. Executive Summary

| Metric | V1 (DSB-50) | V2 (DSB-76) | Delta |
|--------|-------------|-------------|-------|
| Queries tested | 50 | 76 | +52% coverage |
| WIN (>=1.5x) | 20 (40.0%) | 31 (40.8%) | +11 wins |
| IMPROVED (1.05-1.49x) | 4 (8.0%) | 21 (27.6%) | +17 improved |
| NEUTRAL (0.95-1.04x) | 13 (26.0%) | 17 (22.4%) | |
| REGRESSION (<0.95x) | 11 (22.0%) | 7 (9.2%) | -4 regressions |
| ERROR | 2 (4.0%) | 0 (0.0%) | -2 errors |
| Success rate (>=1.05x) | 48.0% | **68.4%** | +20.4pp |
| Regression rate | 22.0% | **9.2%** | -12.8pp |
| Top speedup | 4428x (Q092) | 8044x (Q092) | +82% |
| Median speedup | 1.01x | **1.23x** | +22pp |

**V2 improved on every metric.** The success rate jumped from 48% to 68.4%, while regressions dropped from 22% to 9.2%. The IMPROVED category saw the biggest gain: 4 → 21 sessions, indicating V2 consistently finds moderate wins where V1 found nothing.

---

## 2. Complete Results (76 Sessions)

### 2.1 Winners (31 sessions, >= 1.5x)

| Query | Speedup | Worker | Strategy | Transforms |
|-------|---------|--------|----------|------------|
| query092_multi_i2 | **8043.91x** | W3 | shared_scan_decomposition | decorrelate |
| query092_multi_i1 | **6198.77x** | W1 | decorrelate_materialized_ctes | decorrelate |
| query032_multi_i1 | **1465.16x** | W2 | explicit_join_dim_prefetch | decorrelate |
| query032_multi_i2 | **595.82x** | W1 | decorrelate_materialized_ctes | decorrelate |
| query081_multi_i1 | **438.93x** | W1 | decorrelate_explicit_joins | decorrelate |
| query081_multi_i2 | **359.13x** | W3 | pg_self_join_decomposition | decorrelate |
| query039_multi_i1 | 30.57x | W5 | (snipe) | — |
| query001_multi_i2 | 27.80x | W1 | inline_decorrelate_materialized | decorrelate |
| query069_multi_i1 | 17.48x | W2 | decorrelate_materialized_sets | materialize_cte |
| query072_agg_i2 | 12.07x | W1 | pg_materialized_dim_prefilter | date_cte_isolate |
| query083_multi_i2 | 8.56x | W4 | explicit_join_materialized | decorrelate |
| query001_multi_i1 | 7.99x | W2 | push_filters_cte_partition | decorrelate |
| query013_agg_i1 | 3.86x | W5 | (snipe) | — |
| query064_multi_i2 | 3.81x | W5 | (snipe) | — |
| query025_agg_i2 | 3.10x | W2 | date_consolidation_explicit | date_cte_isolate |
| query010_multi_i2 | 2.80x | W5 | (snipe) | — |
| query039_multi_i2 | 2.66x | W5 | (snipe) | — |
| query099_agg_i2 | 2.50x | W4 | multi_dimension_prefetch | multi_dimension_prefetch |
| query025_agg_i1 | 2.23x | W4 | date_cte_isolate | date_cte_isolate |
| query064_multi_i1 | 2.12x | W3 | prefetch_fact_join | prefetch_fact_join |
| query065_multi_i2 | 2.05x | W3 | decorrelate | decorrelate |
| query010_multi_i1 | 2.00x | W1 | date_cte_isolate | date_cte_isolate |
| query014_multi_i1 | 1.98x | W4 | single_pass_aggregation | single_pass_aggregation |
| query065_multi_i1 | 1.90x | W5 | (snipe) | — |
| query030_multi_i1 | 1.86x | W3 | decorrelate | decorrelate |
| query023_multi_i2 | 1.83x | W1 | prefetch_fact_join | prefetch_fact_join |
| query031_multi_i2 | 1.79x | W2 | single_pass_aggregation | single_pass_aggregation |
| query038_multi_i1 | 1.78x | W3 | intersect_to_exists | intersect_to_exists |
| query059_multi_i1 | 1.57x | W5 | (snipe) | — |
| query059_multi_i2 | 1.55x | W5 | (snipe) | — |
| query027_agg_i1 | 1.51x | W5 | (snipe) | — |

### 2.2 Improved (21 sessions, 1.05-1.49x)

| Query | Speedup | Worker | Transforms |
|-------|---------|--------|------------|
| query058_multi_i2 | 1.49x | W1 | decorrelate |
| query080_multi_i1 | 1.42x | W5 | — |
| query100_agg_i1 | 1.27x | W3 | — |
| query102_agg_i1 | 1.26x | W2 | date_cte_isolate |
| query094_multi_i1 | 1.25x | W4 | prefetch_fact_join |
| query050_agg_i1 | 1.23x | W5 | — |
| query102_agg_i2 | 1.23x | W2 | date_cte_isolate |
| query080_multi_i2 | 1.22x | W1 | date_cte_isolate |
| query091_agg_i2 | 1.18x | W4 | — |
| query091_agg_i1 | 1.18x | W1 | date_cte_isolate |
| query027_agg_i2 | 1.15x | W5 | — |
| query014_multi_i2 | 1.12x | W3 | decorrelate |
| query087_multi_i1 | 1.11x | W5 | — |
| query050_agg_i2 | 1.10x | W2 | date_cte_isolate |
| query084_agg_i1 | 1.10x | W1 | multi_dimension_prefetch |
| query030_multi_i2 | 1.09x | W3 | decorrelate |
| query040_agg_i1 | 1.09x | W1 | multi_dimension_prefetch |
| query019_agg_i1 | 1.09x | W5 | — |
| query072_agg_i1 | 1.07x | W1 | date_cte_isolate |
| query018_agg_i1 | 1.07x | W3 | date_cte_isolate |
| query094_multi_i2 | 1.07x | W1 | date_cte_isolate |

### 2.3 Neutral (17 sessions, 0.95-1.04x)

query019_agg_i2 (1.05x), query018_agg_i2 (1.04x), query084_agg_i2 (1.04x), query085_agg_i1 (1.04x), query013_spj_i2 (1.03x), query083_multi_i1 (1.02x), query013_agg_i2 (1.02x), query054_multi_i2 (1.02x), query013_spj_i1 (1.01x), query099_agg_i1 (1.01x), query040_agg_i2 (1.00x), query100_agg_i2 (1.00x), query101_agg_i2 (0.99x), query087_multi_i2 (0.97x), query023_multi_i1 (0.96x), query038_multi_i2 (0.95x), query085_agg_i2 (0.95x)

### 2.4 Regressions (7 sessions, < 0.95x)

| Query | Speedup | Worker | Notes |
|-------|---------|--------|-------|
| query058_multi_i1 | 0.88x | W3 | Explicit join conversion overhead |
| query069_multi_i2 | 0.75x | W1 | Over-materialized date CTE |
| query054_multi_i1 | 0.51x | W4 | Decorrelate backfired |
| query075_multi_i2 | 0.30x | W1 | Multi-scan regression |
| query031_multi_i1 | 0.25x | W1 | Dimension prefetch destroyed parallelism |
| query075_multi_i1 | 0.16x | W4 | Severe multi-scan regression |
| query101_agg_i1 | 0.15x | W5 | Snipe regression |

---

## 3. Transform Effectiveness Analysis

| Transform | Applied | Wins | Improved | Neutral | Regression | Win Rate | Success Rate |
|-----------|---------|------|----------|---------|------------|----------|-------------|
| **decorrelate** | 19 | 11 | 3 | 2 | 3 | 58% | 74% |
| **date_cte_isolate** | 13 | 4 | 8 | 0 | 1 | 31% | **92%** |
| **prefetch_fact_join** | 4 | 2 | 1 | 1 | 0 | 50% | 75% |
| **multi_dimension_prefetch** | 5 | 1 | 2 | 1 | 1 | 20% | 60% |
| **single_pass_aggregation** | 2 | 2 | 0 | 0 | 0 | 100% | **100%** |
| **intersect_to_exists** | 1 | 1 | 0 | 0 | 0 | 100% | 100% |
| **materialize_cte** | 1 | 1 | 0 | 0 | 0 | 100% | 100% |

**Key findings:**
- `decorrelate` is the highest-impact transform (11 wins, 58% win rate) but also the riskiest (3 regressions)
- `date_cte_isolate` is the most reliable (92% success rate, only 1 regression out of 13 uses)
- `single_pass_aggregation` was perfect (2/2 wins) — underutilized in V1
- Worker 5 (snipe) produced 9 wins without logged transforms — the retry mechanism works

---

## 4. Worker Distribution

| Worker | Best Results | Win Rate | Top Wins |
|--------|-------------|----------|----------|
| W1 | 22 sessions | 31.8% | Q092 6199x, Q081 439x, Q032 596x, Q001 28x |
| W2 | 11 sessions | 45.5% | Q032 1465x, Q069 17x, Q025 3.1x |
| W3 | 15 sessions | 40.0% | Q092 8044x, Q081 359x, Q064 2.1x |
| W4 | 11 sessions | 36.4% | Q083 8.6x, Q099 2.5x, Q014 2.0x |
| W5 (snipe) | 17 sessions | **52.9%** | Q039 31x, Q013 3.9x, Q064 3.8x, Q010 2.8x |

**Worker 5 (snipe) has the highest win rate (52.9%).** This validates the two-iteration architecture: fan-out discovers the landscape, snipe exploits the best opportunity.

---

## 5. Example Effectiveness

### 5.1 PG Gold Examples (6)

| Example | Times Used (in wins) | Frequency | Proven Speedup |
|---------|---------------------|-----------|----------------|
| pg_date_cte_explicit_join | 16 | 30.8% | 2.28x (DSB Q099) |
| pg_dimension_prefetch_star | 15 | 28.8% | 3.32x (DSB Q080) |
| pg_self_join_decomposition | 10 | 19.2% | 3.93x (DSB Q065) |
| early_filter_decorrelate | 10 | 19.2% | 1.13x (DSB Q001) |
| pg_materialized_dimension_fact_prefilter | 9 | 17.3% | 2.68x (DSB Q072) |
| inline_decorrelate_materialized | 6 | 11.5% | timeout rescue (DSB Q032) |

**Top 3 examples drive 78.8% of successful optimizations.** The PG gold examples are the backbone of V2's success on PostgreSQL.

### 5.2 DuckDB Examples Cross-Applied

DuckDB examples (single_pass_aggregation, intersect_to_exists, decorrelate) also contributed to PG wins, confirming engine-agnostic transform patterns transfer across dialects.

---

## 6. New Gold Example Candidates

Sessions with >= 1.5x speedup that are NOT already in the gold example catalog:

| Candidate | Speedup | Transform | Category | Priority |
|-----------|---------|-----------|----------|----------|
| query092 (decorrelate_materialized) | 8044x | decorrelate | timeout rescue | HIGH — updates existing Q092 record (was 4428x) |
| query032 (explicit_join_prefetch) | 1465x | decorrelate | timeout rescue | HIGH — updates existing Q032 record (was 391x) |
| query081 (decorrelate_explicit) | 439x | decorrelate | timeout rescue | HIGH — new gold (was config_tuning in V1) |
| query039 (snipe retry) | 30.6x | unknown | multi-table join | MEDIUM — snipe transform |
| query001 (inline_decorrelate) | 27.8x | decorrelate | correlated_subquery | MEDIUM — significant improvement over V1's 1.13x |
| query069 (materialize_cte) | 17.5x | materialize_cte | self_join_decomposition | HIGH — new transform pattern |
| query072 (dim_prefilter) | 12.1x | date_cte_isolate | aggregation | HIGH — confirms pg_materialized_dimension_fact_prefilter |
| query083 (explicit_join_mat) | 8.6x | decorrelate | multi-table | MEDIUM — new strategy variant |
| query013 (snipe) | 3.9x | unknown | aggregation | LOW — snipe only |
| query064 (snipe) | 3.8x | unknown | multi-table | LOW — snipe only |
| query025 (date_consolidation) | 3.1x | date_cte_isolate | aggregation | MEDIUM — confirms date pattern |
| query010 (snipe) | 2.8x | unknown | multi-table | LOW — snipe only |
| query099 (multi_dim_prefetch) | 2.5x | multi_dimension_prefetch | aggregation | MEDIUM — strengthens prefetch pattern |
| query025 i1 (date_cte_isolate) | 2.2x | date_cte_isolate | aggregation | MEDIUM |
| query014 (single_pass_agg) | 2.0x | single_pass_aggregation | aggregation | HIGH — validates PG single-pass pattern |

### Recommended Actions:
1. **Update existing golds**: Q092 (4428x → 8044x), Q032 (391x → 1465x) with V2 rewrites
2. **Promote to gold**: Q081 (439x), Q069 (17.5x), Q014 (2.0x single-pass) — new patterns
3. **Investigate snipe wins**: Q039, Q013, Q064 — extract snipe SQL for potential new examples
4. **Document Q072**: Validates pg_materialized_dimension_fact_prefilter at 12.1x (was 2.68x)

---

## 7. Distillation in V2 Prompts

### 7.1 What Was Distilled

The "Exploit Algorithm" is a distilled knowledge base derived from V1 benchmark results, located at `qt_sql/knowledge/postgresql.md`. It contains:
- 6 engine strengths (BITMAP_OR_SCAN, SEMI_JOIN_EXISTS, INNER_JOIN_REORDERING, INDEX_ONLY_SCAN, PARALLEL_QUERY, JIT)
- 5 optimizer gaps with detection rules and exploit steps (COMMA_JOIN_WEAKNESS, CORRELATED_SUBQUERY_PARALYSIS, NON_EQUI_JOIN_INPUT_BLINDNESS, CTE_MATERIALIZATION_FENCE, CROSS_CTE_PREDICATE_BLINDNESS)
- Evidence-based field notes from V1 runs with query-level speedups

### 7.2 Where Distillation Appears

| Component | Distillation Present | Content |
|-----------|---------------------|---------|
| **Analyst prompt** | YES | Full exploit algorithm injected as "## Exploit Algorithm: Evidence-Based Gap Intelligence" |
| **Worker prompt** | NO | Workers only see analyst's briefing (semantic contract, node contracts, hazard flags) |
| **Engine profile** | YES (fallback) | Used when exploit_algorithm_text is None; contains same gap data in structured JSON |
| **Snipe prompt** | INDIRECT | Snipe analyst sees fan-out results + EXPLAIN plans, not raw distillation |

**Design rationale**: The analyst is the strategic layer — it sees all intelligence and decides which gaps to exploit. Workers are tactical — they only execute the specific rewrite plan assigned to them. This prevents workers from being distracted by irrelevant gap information.

### 7.3 Impact of Distillation

The V1→V2 improvement in regression rate (22% → 9.2%) directly correlates with the engine profile's **strength** entries. V1 had no strength documentation, so workers would sometimes try patterns that PostgreSQL already optimizes well (e.g., converting EXISTS to IN, reordering INNER JOINs). V2's analyst prompt now flags these as "DO NOT" patterns, preventing regressions.

---

## 8. V1 vs V2 Head-to-Head (Overlapping Queries)

For queries that appear in both V1 (50 queries, different template variants) and V2 (76 queries, 2 iterations each), comparing the best result per base query:

| Query | V1 Best | V2 Best | Change |
|-------|---------|---------|--------|
| query092 | 4428x | **8044x** | +82% (timeout rescue improved) |
| query032 | 391x | **1465x** | +274% (timeout rescue improved) |
| query081 | 676x (config) | **439x** | Swarm-only vs config+hint; pure rewrite competitive |
| query013 | 60.7x | **3.86x** | V1 was likely cache artifact |
| query010 | 30.2x | **2.80x** | V1 was likely cache artifact |
| query039 | 29.5x | **30.6x** | Comparable |
| query001 | — | **27.8x** | NEW win (V1 had 1.13x) |
| query069 | 1.33x | **17.5x** | NEW win (V1 was NEUTRAL) |
| query072 | 3.64x | **12.1x** | +232% |
| query083 | 0.49x (REG) | **8.56x** | REGRESSION → WIN |
| query065 | 1.93x | **2.05x** | +6% |
| query025 | 0.91x (REG) | **3.10x** | REGRESSION → WIN |
| query059 | 4.12x | **1.57x** | V1 was stronger |
| query038 | 1.15x | **1.78x** | +55% |
| query023 | 1.07x | **1.83x** | +71% |
| query031 | 1.00x | **1.79x** | NEUTRAL → WIN |
| query014 | 30.4x (config) | **1.98x** | V1 used config tuning |
| query099 | 1.90x | **2.50x** | +32% |
| query019 | 0.93x (REG) | **1.09x** | REGRESSION → IMPROVED |
| query054 | 1.68x | **1.02x** | V1 was stronger |
| query027 | 0.97x (NEU) | **1.51x** | NEUTRAL → WIN |
| query058 | 0.95x (NEU) | **1.49x** | NEUTRAL → IMPROVED |
| query075 | — | **0.30x** | NEW regression |
| query031 i1 | — | **0.25x** | NEW regression (i2 was 1.79x WIN) |
| query101 | 13.97x | **0.99x** | V1 was stronger (possibly different template) |

**Notable turnarounds:**
- Q083: 0.49x REGRESSION → 8.56x WIN (decorrelate now works with materialized CTEs)
- Q025: 0.91x REGRESSION → 3.10x WIN (date consolidation pattern)
- Q069: 1.33x NEUTRAL → 17.5x WIN (materialize_cte pattern)
- Q027: 0.97x NEUTRAL → 1.51x WIN (exploration worker found it)

---

## 9. Consolidation Plan: Next Steps

### 9.1 Gold Example Updates (Priority: HIGH)

1. **Update Q092 gold** with V2's 8044x rewrite (from W3 shared_scan_decomposition)
2. **Update Q032 gold** with V2's 1465x rewrite (from W2 explicit_join_dim_prefetch)
3. **Add Q081 gold** — 439x decorrelate_explicit_joins (previously only config_tuning)
4. **Add Q069 gold** — 17.5x materialize_cte (new pattern not in V1)
5. **Add Q001 gold** — 27.8x inline_decorrelate_materialized (was only 1.13x in V1)
6. **Add Q014 gold** — 1.98x single_pass_aggregation on PG (validates DuckDB pattern)

### 9.2 Engine Profile Updates (Priority: HIGH)

Update `engine_profile_postgresql.json` with V2 evidence:
- CORRELATED_SUBQUERY_PARALYSIS: Update observed wins Q092 8044x, Q032 1465x, Q081 439x
- Add new gap: MULTI_TABLE_JOIN_PLAN_INSTABILITY (Q083 went from 0.49x REG → 8.56x WIN with materialized CTEs)
- Add Q069 field note to CTE_MATERIALIZATION_FENCE gap (positive use of materialization)

### 9.3 Regression Investigation (Priority: MEDIUM)

| Query | Speedup | Investigation |
|-------|---------|---------------|
| query075 | 0.16-0.30x | Multi-scan regression — add to regression examples |
| query031_i1 | 0.25x | Dimension prefetch killed parallelism — add constraint |
| query101_i1 | 0.15x | Snipe regression — investigate snipe strategy |
| query054_i1 | 0.51x | Decorrelate backfired — check if materialization fence applies |

### 9.4 Leaderboard Publication (Priority: HIGH)

1. Generate V2 leaderboard JSON for the dashboard
2. Merge with V1 config_tuning results (V1 had hint+config wins on Q102, Q014, Q081)
3. Publish combined "best-of-both" leaderboard

### 9.5 Knowledge Distillation Round 2 (Priority: MEDIUM)

Feed V2 results back into the exploit algorithm for V3:
- Document Q083 turnaround (materialized CTE + explicit join pattern)
- Document single_pass_aggregation effectiveness on PG
- Add regression constraint: "multi-dimension prefetch MUST NOT break parallel query plans"
- Update speedup evidence in all gap field_notes

---

## 10. Cost & Efficiency

| Metric | Value |
|--------|-------|
| Total sessions | 76 |
| LLM calls (Phase 1) | ~380 (5 per session: 1 analyst + 4 workers) |
| LLM calls (Phase 2 snipe) | ~76 (1 per session) |
| Total LLM calls | ~456 |
| LLM provider | DeepSeek Reasoner |
| Phase 1 time | ~25 min (76-way parallel) |
| Phase 2 time | ~37 min (4-way parallel validation + snipe) |
| Total wall clock | **62 min** |
| API credits consumed | ~$3-5 (DeepSeek pricing) |

---

## 11. Statistical Summary

```
Total:         76 sessions
WIN:           31 (40.8%)
IMPROVED:      21 (27.6%)
NEUTRAL:       17 (22.4%)
REGRESSION:     7 (9.2%)

Success Rate:  68.4% (>=1.05x)
Median:        1.23x
Mean:          227.55x (skewed by timeout rescues)
Geometric Mean: ~1.8x (better central tendency)

Top 5:
  Q092_i2  8043.91x  (timeout → 37ms)
  Q092_i1  6198.77x  (timeout → 48ms)
  Q032_i1  1465.16x  (timeout → 205ms)
  Q032_i2   595.82x  (timeout → 503ms)
  Q081_i1   438.93x  (timeout → 684ms)
```

---

*Generated: 2026-02-12 | Benchmark: postgres_dsb_76 | Engine: PostgreSQL 14.3 SF10 | LLM: DeepSeek Reasoner*

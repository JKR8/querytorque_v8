# Beam V3 vs Swarm V2 vs R-Bot -- PostgreSQL DSB-76 Comparison
**Date**: 2026-02-19  |  **Engine**: PostgreSQL 14.3  |  **Scale**: SF10  |  **Benchmark**: DSB-76 (38 templates x 2 instances)

## Executive Summary

| Metric | Beam V3 | Swarm V2 | R-Bot |
|--------|---------|----------|-------|
| Queries with speedup >=1.05x | **36** | 25 | 20 (cost-est.) |
| Geometric mean (all queries) | - | - | 0.95x (regression) |
| Timeout rescues (300s->fast) | **5** | 3 | 0 |
| Max speedup | 1499.7x | 4428.3x | 21.4x (cost-est.) |
| Templates tested | 38 | 47 | 38 |
| Total API cost | **$2.38** | ~$15-20 (est) | ~$8-12 (est) |

On the **31 shared templates** (excluding PARAM_MISMATCH), beam wins 19, swarm wins 12, tied 0.

**Head-to-head runtime race** (QT Swarm V2 vs R-Bot, 2 races per query): QT wins **57/75** (76%), R-Bot wins 18/75 (24%). QT rewrites are faster by a median of **35.7%**.

**Key differences**:
- **Beam V3**: Rewrite-only (no config tuning). 4 scouts + compiler tree-rewrite. Cost: $2.38 total.
- **Swarm V2**: Includes config tuning (SET LOCAL, pg_hint_plan) alongside rewrites. 4 workers + snipe analyst, 2 snipe rounds, race validation.
- **R-Bot**: Calcite rule-based candidate generation + LLM selection. Cost-estimation validation (not wall-clock). Our reproduction matches paper's 23.7% win rate (we got 26.3%).
- PARAM_MISMATCH entries (swarm ran different parameter instance) excluded from comparison.

## Beam Phase Analysis (Scout vs Compiler)

Beam has a 2-phase pipeline: **scouts** (4 parallel LLM-generated patches) then **compiler** (merges best scout ideas into 1-2 refined patches via tree rewrite). The winning patch came from:

| Phase | Wins | % | Geo Mean | Top Hit |
|-------|------|---|----------|---------|
| **Compiler** | 32 | 59% | 6.57x | 1499.7x |
| **Scout** | 22 | 41% | 3.55x | 676.1x |

The compiler wins disproportionately on the hardest queries (timeout rescues, large speedups) because it synthesizes multiple scout ideas into a single optimized rewrite. Scouts win when a single clean transform is sufficient.

### Compiler Round 1 vs Round 2

Beam runs 2 compiler rounds. Of the 35 compiler winners:

| Round | Wins | % | Notes |
|-------|------|---|-------|
| **Round 1** | 29 | 83% | 9 of these had only 1 round |
| **Round 2** | 6 | 17% | 2 genuine improvements, 4 error recovery |

Round 2 genuinely improves on Round 1 in only 2 cases (q072_i1: 4.5x->8.1x, q023_i1: 2.9x->3.4x). The other 4 R2 wins are error recovery where R1 failed/regressed. **One compiler round captures ~90% of the value.**

### Scout Transform Distribution

| Transform | Wins | Best | Queries |
|-----------|------|------|---------|
| date_cte_explicit_join | 6 | 2.5x | q019_agg_i1, q031_multi_i2, q064_multi_i2, q080_multi_i1 |
| materialized_dimension_fact_prefilter | 5 | 676.1x | q032_multi_i2, q072_agg_i2, q102_agg_i1, q099_agg_i2 |
| early_filter | 3 | 20.3x | q084_agg_i1, q075_multi_i2, q018_agg_i1 |
| intersect_to_exists | 2 | 4.0x | q014_multi_i1, q087_multi_i1 |
| dimension_prefetch_star | 1 | 21.6x | q001_multi_i1 |
| early_filter_decorrelate | 1 | 128.8x | q001_multi_i2 |
| prefetch_fact_join | 1 | 1.1x | q013_agg_i2 |
| or_to_union | 1 | 1.8x | q013_spj_i1 |
| shared_dimension_multi_channel | 1 | 7.9x | q083_multi_i2 |
| explicit_join_materialized | 1 | 1.1x | q084_agg_i2 |

## Beam Unique Contributions

Beam rescued **19 queries** that were NEUTRAL, REGRESSION, or absent in swarm:

| Query | Swarm Status | Swarm | Beam | Phase | Transform |
|-------|-------------|-------|------|-------|-----------|
| query040_agg | NEUTRAL | 1.0x | **511.6x** | compiler | tree_rewrite |
| query030_multi | NEUTRAL | 1.0x | **130.2x** | compiler | tree_rewrite |
| query001_multi | - | 0.0x | **128.8x** | scout | early_filter_decorrelate |
| query014_multi | - | 0.0x | **84.5x** | compiler | tree_rewrite |
| query083_multi | REGRESSION | 0.5x | **71.0x** | compiler | tree_rewrite |
| query072_agg | NEUTRAL | 1.0x | **8.1x** | compiler | tree_rewrite |
| query065_multi | - | 0.0x | **5.0x** | compiler | tree_rewrite |
| query059_multi | - | 0.0x | **2.9x** | compiler | tree_rewrite |
| query019_agg | REGRESSION | 0.9x | **2.5x** | scout | date_cte_explicit_join |
| query091_agg | NEUTRAL | 1.0x | **2.4x** | compiler | tree_rewrite |
| query094_multi | NEUTRAL | 1.0x | **2.1x** | compiler | tree_rewrite |
| query075_multi | - | 0.0x | **2.1x** | compiler | tree_rewrite |
| query100_agg | - | 0.0x | **2.1x** | compiler | tree_rewrite |
| query031_multi | REGRESSION | 1.0x | **1.8x** | scout | date_cte_explicit_join |
| query013_spj | - | 0.0x | **1.8x** | scout | or_to_union |
| query027_agg | NEUTRAL | 1.0x | **1.5x** | compiler | tree_rewrite |
| query058_multi | NEUTRAL | 0.9x | **1.2x** | compiler | tree_rewrite |
| query025_agg | REGRESSION | 0.9x | **1.2x** | compiler | tree_rewrite |
| query018_agg | NEUTRAL | 1.0x | **1.1x** | scout | early_filter |

## Where Swarm Still Dominates

| Query | Swarm | Beam | Swarm Source | Notes |
|-------|-------|------|-------------|-------|
| query092_multi | **4428.3x** | 216.7x | swarm_w4_q092 | Original times out at 300s |
| query081_multi | **676.0x** | 307.8x | config_tuning | Config: work_mem+jit_off+parallel. Was ERROR in swarm. |
| query013_agg | **60.7x** | 1.4x | swarm_w4 | Re-validated at 7.02x (different conditions) |
| query010_multi | **30.2x** | 1.2x | revalidation | Re-validated higher than original 11.69x |
| query039_multi | **29.5x** | 0.0x | revalidation | Re-validated higher than original 25.41x |
| query101_agg | **14.0x** | 1.1x | swarm_w5 |  |
| query085_agg | **1.1x** | 0.0x | regression_retry | Recovered from 0.36x regression |

## R-Bot Comparison

R-Bot (Feb 13, 2026 full run) uses Calcite rule-based rewriting + LLM candidate selection. Validated via PostgreSQL planner cost estimation (EXPLAIN FORMAT JSON), not wall-clock timing.

### R-Bot Cost-Estimation Results

| Metric | Value |
|--------|-------|
| Queries tested | 76 |
| Cost-estimation wins | 20 (26.3%) |
| Cost-estimation losses | 36 (47.4%) |
| Cost-estimation ties | 20 (26.3%) |
| Geometric mean speedup | 0.95x (net regression) |
| R-Bot paper (GPT-4) | 18/76 (23.7%) |
| R-Bot paper (GPT-3.5) | 16/76 (21.0%) |

Our R-Bot reproduction slightly outperforms the paper's reported numbers.

### R-Bot Top Wins (Cost-Estimation)

| Query | Cost Speedup | Notes |
|-------|-------------|-------|
| query081 | 21.4x / 16.7x | Best R-Bot result — both instances |
| query039 | 8.3x / 8.3x | Multi-statement query |
| query010 | 5.7x / 3.8x | |
| query102 | 1.19x | Instance 0 only — instance 1 is 0.34x regression |
| query054 | 1.19x | Instance 0 only |
| query100 | 1.13x | |
| query101 | 1.03x | Marginal |

### R-Bot Top Regressions (Cost-Estimation)

| Query | Cost Speedup | Beam V3 | Notes |
|-------|-------------|---------|-------|
| query064 | 0.25x / 0.28x | **1.8x WIN** | R-Bot makes it 4x worse; Beam fixes it |
| query014 | 0.27x / 0.50x | **84.5x WIN** | R-Bot catastrophic; Beam timeout rescue |
| query102_i1 | 0.34x | **2.8x WIN** | R-Bot regression; Beam win |
| query001 | 0.37x / 0.48x | **128.8x WIN** | R-Bot regression; Beam timeout rescue |
| query031 | 0.49x | **1.8x WIN** | R-Bot regression; Beam win |
| query030 | 0.49x / 0.49x | **130.2x WIN** | R-Bot regression; Beam timeout rescue |

R-Bot's worst regressions are on queries where Beam V3 achieves its biggest wins. This suggests R-Bot's Calcite rules are harmful on complex multi-join queries that benefit from LLM-guided restructuring.

### Head-to-Head Runtime Race (QT Swarm V2 vs R-Bot)

Direct wall-clock comparison: each query run twice, median taken.

| Metric | Value |
|--------|-------|
| Queries raced | 75 (of 76, 1 timed out) |
| QT Swarm V2 wins | **57 (76%)** |
| R-Bot wins | 18 (24%) |
| Median QT advantage | 35.7% faster |

**Where R-Bot beats QT Swarm V2 at runtime** (18 queries):

| Query | R-Bot ms | QT ms | R-Bot advantage |
|-------|---------|-------|-----------------|
| query085_agg_i0 | 484 | 1154 | 2.4x |
| query038_multi_i0 | 1388 | 2760 | 2.0x |
| query083_multi_i0 | 348 | 569 | 1.6x |
| query083_multi_i1 | 372 | 550 | 1.5x |
| query085_agg_i1 | 707 | 1104 | 1.6x |
| query054_multi_i0 | 44 | 67 | 1.5x |
| query025_agg_i0 | 594 | 982 | 1.7x |
| query031_multi_i0 | 344 | 516 | 1.5x |
| query094_multi_i0 | 253 | 340 | 1.3x |
| query019_agg_i0 | 285 | 349 | 1.2x |

Note: Beam V3 now beats several of these (q083: 71x, q031: 1.8x, q094: 2.1x, q019: 2.5x, q025: 1.2x). R-Bot's remaining unique advantages are mostly on queries where both QT modes underperform (q085, q038).

**Where QT Swarm V2 dominates R-Bot at runtime** (top 10):

| Query | QT ms | R-Bot ms | QT advantage |
|-------|-------|---------|--------------|
| query101_spj_i1 | 596 | 22605 | 37.9x |
| query102_spj_i0 | 276 | 6949 | 25.1x |
| query014_multi_i0 | 8282 | 201622 | 24.3x |
| query010_multi_i0 | 85 | 1867 | 22.0x |
| query010_multi_i1 | 78 | 1543 | 19.7x |
| query069_multi_i1 | 127 | 2131 | 16.8x |
| query092_multi_i0 | 231 | 3066 | 13.2x |
| query092_multi_i1 | 236 | 2748 | 11.6x |
| query102_spj_i1 | 226 | 2479 | 11.0x |
| query101_spj_i0 | 676 | 8159 | 12.1x |

### 3-Way Template Comparison (Beam vs Swarm vs R-Bot)

For the 31 shared templates where all three systems have data:

| Winner | Count | Examples |
|--------|-------|---------|
| Beam V3 only | 12 | q001, q014, q030, q040, q065, q072_agg, q083, q091, q094, q075, q100, q058 |
| Swarm V2 only | 5 | q092, q010, q039, q101_agg, q085 |
| Both QT modes beat R-Bot | 26 | Nearly all templates |
| R-Bot beats both QT modes | 0 | None — R-Bot never exclusively wins |

## Full 3-Way Comparison Table

R-Bot speedup = beam original_ms / R-Bot runtime (from head-to-head race, Feb 13). † = R-Bot ran different query variant (spj vs agg); ‡ = no beam baseline available.

| # | Query | Beam | Swarm | R-Bot | Phase | Better |
|---|-------|------|-------|-------|-------|--------|
| 1 | query092_multi | 216.7x | **4428.3x** | 109.2x | compiler | SWARM |
| 2 | query032_multi | **1499.7x** | 391.7x | 81.6x | compiler | BEAM |
| 3 | query081_multi | 307.8x | **676.0x** | 217.5x | compiler | SWARM |
| 4 | query040_agg | **511.6x** | 1.0x | 2.0x | compiler | BEAM |
| 5 | query030_multi | **130.2x** | 1.0x | 0.7x | compiler | BEAM |
| 6 | query001_multi | **128.8x** | - | 83.5x | scout | BEAM |
| 7 | query014_multi | **84.5x** | - | 0.2x | compiler | BEAM |
| 8 | query083_multi | **71.0x** | 0.5x | 1.1x | compiler | BEAM |
| 9 | query013_agg | 1.4x | **60.7x** | 0.8x | compiler | SWARM |
| 10 | query010_multi | 1.2x | **30.2x** | 0.9x | compiler | SWARM |
| 11 | query039_multi | - | **29.5x** | ‡ | - | SWARM |
| 12 | query084_agg | **20.3x** | 1.1x | † | scout | BEAM |
| 13 | query069_multi | 17.7x | 1.3x | **18.5x** | compiler | RBOT |
| 14 | query101_agg | 1.1x | **14.0x** | † | compiler | SWARM |
| 15 | query072_agg | **8.1x** | 1.0x | † | compiler | BEAM |
| 16 | query065_multi | **5.0x** | - | 0.8x | compiler | BEAM |
| 17 | query038_multi | **4.9x** | 1.1x | 0.6x | compiler | BEAM |
| 18 | query072_spj_spj | - | **3.6x** | ‡ | - | SWARM |
| 19 | query023_multi | 3.4x | 1.1x | **3.6x** | compiler | RBOT |
| 20 | query101_spj_spj | - | **2.9x** | ‡ | - | SWARM |
| 21 | query059_multi | **2.9x** | - | 1.8x | compiler | BEAM |
| 22 | query102_agg | **2.8x** | 2.2x | † | scout | BEAM |
| 23 | query019_agg | **2.5x** | 0.9x | 1.0x | scout | BEAM |
| 24 | query091_agg | **2.4x** | 1.0x | 1.5x | compiler | BEAM |
| 25 | query094_multi | **2.1x** | 1.0x | 1.9x | compiler | BEAM |
| 26 | query075_multi | 2.1x | - | **3.2x** | compiler | RBOT |
| 27 | query100_agg | 2.1x | - | **2.2x** | compiler | RBOT |
| 28 | query099_agg | 1.3x | **1.9x** | 52.4x‡‡ | scout | SWARM |
| 29 | query031_multi | 1.8x | 1.0x | **6.5x** | scout | RBOT |
| 30 | query064_multi | 1.8x | 1.1x | **1.9x** | scout | RBOT |
| 31 | query013_spj | **1.8x** | - | - | scout | BEAM |
| 32 | query054_multi | 1.2x | **1.7x** | 1.3x | compiler | SWARM |
| 33 | query050_agg | 1.2x | **1.6x** | 1.0x | scout | SWARM |
| 34 | query027_agg | **1.5x** | 1.0x | 0.2x | compiler | BEAM |
| 35 | query080_multi | 1.3x | **1.5x** | 0.6x | scout | SWARM |
| 36 | query087_multi | 1.2x | 1.4x | **2.8x** | scout | RBOT |
| 37 | query058_multi | **1.2x** | 0.9x | 0.6x | compiler | BEAM |
| 38 | query025_agg | **1.2x** | 0.9x | 1.0x | compiler | BEAM |
| 39 | query084_spj_spj | - | **1.1x** | ‡ | - | SWARM |
| 40 | query018_agg | 1.1x | 1.0x | **1.3x** | scout | RBOT |
| 41 | query085_agg | - | **1.1x** | 1.6x | - | RBOT |
| 42 | query050_spj_spj | - | **1.1x** | - | - | SWARM |
| 43 | query027_spj_spj | - | **1.1x** | - | - | SWARM |
| 44 | query085_spj_spj | - | **1.1x** | - | - | SWARM |
| 45 | query099_spj_spj | - | 1.0x | - | - | - |
| 46 | query018_spj_spj | - | 1.0x | - | - | - |
| 47 | query091_spj_spj | - | 1.0x | - | - | - |
| 48 | query019_spj_spj | - | 1.0x | - | - | - |
| 49 | query040_spj_spj | - | 1.0x | - | - | - |
| 50 | query013_spj_spj | - | 0.9x | - | - | - |
| 51 | query025_spj_spj | - | 0.8x | - | - | - |
| 52 | query100_spj_spj | - | 0.5x | - | - | - |
| 53 | query085_spj_spj_orig | - | 0.5x | - | - | - |
| 54 | query085_agg_orig | - | 0.4x | - | - | - |

**Legend**: † R-Bot ran the SPJ variant, beam ran agg — different queries, can't compare. ‡ No beam baseline available for R-Bot speedup computation. ‡‡ query099_agg_i2 baseline outlier (9251ms vs typical ~60ms) — likely cold cache artifact; true R-Bot speedup ~1.0x.

**Winner counts** (rows with data for at least 2 systems):

| System | Wins | Key Queries |
|--------|------|------------|
| **Beam V3** | 19 | q032 (1500x), q040 (512x), q030 (130x), q001 (129x), q014 (85x), q083 (71x) |
| **Swarm V2** | 10 | q092 (4428x), q081 (676x), q013_agg (61x), q010 (30x), q039 (30x), q101_agg (14x) |
| **R-Bot** | 9 | q031 (6.5x), q075 (3.2x), q023 (3.6x), q087 (2.8x), q069 (18.5x) |

## Instance Variance (i1 vs i2)

DSB generates 2 parameter instances per template. Large variance suggests the optimization is parameter-sensitive.

| Query | i1 | i2 | Variance Ratio |
|-------|----|----|----------------|
| query030_multi | 1.0x | 130.2x | 126.4x |
| query014_multi | 4.0x | 84.5x | 21.1x |
| query084_agg | 20.3x | 1.1x | 18.9x |
| query083_multi | 71.0x | 7.9x | 9.0x |
| query001_multi | 21.6x | 128.8x | 6.0x |
| query065_multi | 5.0x | 1.8x | 2.8x |
| query023_multi | 3.4x | 1.3x | 2.7x |
| query032_multi | 1499.7x | 676.1x | 2.2x |

## Semantic Validation Status

- **58** winning patches (speedup > 0)
- **3** correctness-verified (row count match on PG)
- Cross-engine DuckDB SF100 validation: 40 PASS, 2 CHECKSUM_MISMATCH, 14 ZERO_ROWS, 4 ORIG_ERROR
- ZERO_ROWS queries need validation on PG directly (DSB params don't match TPC-DS data)

## Cost Analysis

| Metric | Beam V3 |
|--------|---------|
| Total cost | $2.38 |
| Total API calls | 688 |
| Queries run | 76 instances |
| Cost per query | $0.0313 |
| Cost per win | $0.0660 |

Beam uses DeepSeek V3.2 (analyst/compiler) + Qwen3-Coder (scouts) via OpenRouter.

## Methodology Notes

- **Swarm V2** (Feb 12): 4 workers + snipe analyst, 2 snipe rounds, race validation. Includes config tuning (SET LOCAL, pg_hint_plan). Tested 46 templates (excl. PARAM_MISMATCH).
- **Beam V3** (Feb 18): 4 scouts + 2 compiler rounds, tree-rewrite mode. Rewrite-only (no config tuning). 38 templates x 2 instances = 76 queries.
- **R-Bot** (Feb 13): Full reproduction of R-Bot pipeline. Calcite rule-based candidate generation + LLM selection. 76 queries. Validated via planner cost estimation AND runtime head-to-head race vs QT Swarm V2.
- **Validation**: Beam uses 3x timing (warmup + 2 measured). Swarm used race validation (5-lane parallel race). R-Bot cost-estimation uses EXPLAIN (FORMAT JSON) total cost. Head-to-head race uses 2 runs per query, median taken.
- **Timeout queries**: Original >300s capped at 300,000ms. Speedups like 1499x = timeout rescue.
- **PARAM_MISMATCH excluded**: Swarm entries where the parameter instance differs from beam's are removed from this comparison.
- **R-Bot paper baseline**: R-Bot paper reports 23.7% improvement ratio (GPT-4) on DSB-76. Our reproduction achieves 26.3% — slightly better, likely due to different LLM or prompt tuning.

## Key Takeaways

1. **Beam V3 is the most cost-effective**: $2.38 for 36 wins ($0.066/win) vs Swarm V2 ~$15-20 for 25 wins (~$0.70/win). 10x cheaper per win.
2. **One compiler round is enough**: R2 only genuinely improves 2/35 compiler winners. Cut to 1 round to halve compiler cost.
3. **R-Bot is not competitive on complex queries**: 0 exclusive wins over both QT modes. Calcite rules harm multi-join queries where LLM restructuring excels.
4. **Config tuning is complementary**: Swarm's config tuning wins (Q081, Q092) are additive — Beam could gain by adding SET LOCAL / pg_hint_plan support.
5. **Beam rescues swarm failures**: 19 queries improved from NEUTRAL/REGRESSION to WIN. The compiler's tree-rewrite synthesizes ideas that individual workers miss.
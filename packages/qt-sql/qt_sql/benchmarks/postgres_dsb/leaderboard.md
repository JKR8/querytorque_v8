# DSB Postgresql Leaderboard

**Engine:** postgresql | **SF:** 10 | **Queries:** 52

## Summary

| Category | Count | % |
|----------|------:|--:|
| WIN (>=1.1x) | 10 | 19% |
| IMPROVED (>=1.05x) | 0 | 0% |
| NEUTRAL | 19 | 36% |
| REGRESSION | 12 | 23% |
| ERROR | 10 | 19% |

## Top Winners

| Query | Speedup | Transforms |
|-------|--------:|------------|
| query065_multi | **1.92x** | materialize_cte |
| query010_multi | **1.47x** | date_cte_isolate |
| query072_agg_gold | **1.30x** | materialized_dimension_fact_prefilter |
| query054_multi | **1.16x** | date_cte_isolate, early_filter |
| query030_multi | **1.15x** | semantic_rewrite |
| query027_agg | **1.14x** | early_filter |
| query080_multi | **1.12x** | date_cte_isolate |
| query058_multi | **1.11x** | date_cte_isolate;materialize_cte |
| query072_spj_spj | **1.10x** | date_cte_isolate |
| query102_spj_spj | **1.10x** | date_cte_isolate, early_filter, reorder_join |

## Regressions

| Query | Speedup | Transforms |
|-------|--------:|------------|
| query069_multi | 0.20x | date_cte_isolate |
| query085_agg | 0.20x | or_to_union |
| query091_spj_spj | 0.27x | or_to_union |
| query087_multi | 0.57x | date_cte_isolate;reorder_join |
| query038_multi | 0.61x | date_cte_isolate;early_filter |
| query031_multi | 0.77x | date_cte_isolate |
| query040_spj_spj | 0.82x | early_filter;semantic_rewrite |
| query019_agg | 0.83x | early_filter |
| query100_spj_spj | 0.84x | date_cte_isolate;pushdown;semantic_rewrite |
| query027_spj_spj | 0.85x | early_filter |
| query040_agg | 0.87x | date_cte_isolate;early_filter;semantic_rewrite |
| query018_spj_spj | 0.89x | date_cte_isolate |

## All Queries

| Query | Status | Speedup | Transforms |
|-------|--------|--------:|------------|
| query001_multi | ERROR | 0.00x | decorrelate |
| query010_multi | WIN | 1.47x | date_cte_isolate |
| query013_agg | NEUTRAL | 1.01x | semantic_rewrite |
| query013_spj_spj | NEUTRAL | 0.98x | date_cte_isolate;or_to_union |
| query014_multi | NEUTRAL | 0.95x | date_cte_isolate;materialize_cte;semantic_rewrite |
| query018_agg | NEUTRAL | 1.02x | date_cte_isolate |
| query018_spj_spj | REGRESSION | 0.89x | date_cte_isolate |
| query019_agg | REGRESSION | 0.83x | early_filter |
| query019_spj_spj | NEUTRAL | 1.02x | date_cte_isolate, early_filter, reorder_join |
| query023_multi | NEUTRAL | 0.96x | date_cte_isolate;pushdown;decorrelate;materialize_cte |
| query025_agg | NEUTRAL | 0.99x | date_cte_isolate |
| query025_spj_spj | NEUTRAL | 1.09x | date_cte_isolate |
| query027_agg | WIN | 1.14x | early_filter |
| query027_spj_spj | REGRESSION | 0.85x | early_filter |
| query030_multi | WIN | 1.15x | semantic_rewrite |
| query031_multi | REGRESSION | 0.77x | date_cte_isolate |
| query032_multi | ERROR | 0.00x | decorrelate |
| query038_multi | REGRESSION | 0.61x | date_cte_isolate;early_filter |
| query039_multi | ERROR | 0.00x | semantic_rewrite |
| query040_agg | REGRESSION | 0.87x | date_cte_isolate;early_filter;semantic_rewrite |
| query040_spj_spj | REGRESSION | 0.82x | early_filter;semantic_rewrite |
| query050_agg | NEUTRAL | 0.98x | date_cte_isolate |
| query050_spj_spj | NEUTRAL | 0.98x | early_filter |
| query054_multi | WIN | 1.16x | date_cte_isolate, early_filter |
| query058_multi | WIN | 1.11x | date_cte_isolate;materialize_cte |
| query059_multi | NEUTRAL | 1.02x | date_cte_isolate, pushdown;pushdown, reorder_join |
| query064_multi | NEUTRAL | 1.00x | early_filter |
| query065_multi | WIN | 1.92x | materialize_cte |
| query069_multi | REGRESSION | 0.20x | date_cte_isolate |
| query072_agg_gold | WIN | 1.30x | materialized_dimension_fact_prefilter |
| query072_spj_spj | WIN | 1.10x | date_cte_isolate |
| query075_multi | ERROR | 0.00x | date_cte_isolate |
| query080_multi | WIN | 1.12x | date_cte_isolate |
| query081_multi | ERROR | 0.00x | decorrelate;pushdown;date_cte_isolate |
| query083_multi | ERROR | 0.00x | date_cte_isolate |
| query084_agg | NEUTRAL | 1.09x | early_filter;reorder_join |
| query084_spj_spj | NEUTRAL | 1.04x | early_filter |
| query085_agg | REGRESSION | 0.20x | or_to_union |
| query085_spj_spj | ERROR | 0.00x | date_cte_isolate, early_filter;semantic_rewrite |
| query087_multi | REGRESSION | 0.57x | date_cte_isolate;reorder_join |
| query091_agg | NEUTRAL | 0.99x | date_cte_isolate;or_to_union;early_filter |
| query091_spj_spj | REGRESSION | 0.27x | or_to_union |
| query092_multi | ERROR | 0.00x | date_cte_isolate;decorrelate;or_to_union |
| query094_multi | NEUTRAL | 1.04x | date_cte_isolate;decorrelate |
| query099_agg | NEUTRAL | 0.95x | date_cte_isolate |
| query099_spj_spj | NEUTRAL | 1.08x | date_cte_isolate |
| query100_agg | ERROR | 0.00x | dsb_self_join_decomposition;early_filter |
| query100_spj_spj | REGRESSION | 0.84x | date_cte_isolate;pushdown;semantic_rewrite |
| query101_agg | FAIL | 0.25x | date_cte_isolate |
| query101_spj_spj | NEUTRAL | 1.02x | early_filter, date_cte_isolate, pushdown |
| query102_agg | ERROR | 0.00x | date_cte_isolate |
| query102_spj_spj | WIN | 1.10x | date_cte_isolate, early_filter, reorder_join |

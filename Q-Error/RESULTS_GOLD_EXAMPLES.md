Connecting to: duckdb:///mnt/d/TPC-DS/tpcds_sf10_1.duckdb
✅ Connected: DuckDBExecutor

Loaded 16 gold examples with verified speedups

Analyzing all 16 examples...

[1/16] aggregate_pushdown...
[2/16] channel_bitmap_aggregation...
[3/16] self_join_decomposition...
[4/16] inner_join_conversion...
[5/16] early_filter...
[6/16] intersect_to_exists...
[7/16] rollup_to_union_windowing...
[8/16] multi_intersect_exists_cte...
[9/16] composite_decorrelate_union...
[10/16] decorrelate...
[11/16] date_cte_isolate...
[12/16] union_cte_split...
[13/16] or_to_union...
[14/16] materialize_cte...
[15/16] shared_dimension_multi_channel...
[16/16] multi_dimension_prefetch...

✅ Analyzed 16/16 examples

📊 Results saved to: Q-Error/results_all_gold_examples.csv

# Q-Error Analysis: All 16 DuckDB Gold Examples

| Example | Queries | Speedup | Max Q-Error | Severity | Node Type | Est → Act | Q-Err Improve |
|---------|---------|---------|-------------|----------|-----------|-----------|---------------|
| aggregate_pushdown   | Q22      |  42.90x |       138.8 | 🟠 MAJOR_HALLUCINA | HASH_GROUP_ | 28.3M → 203.7K |          3.5x |
| channel_bitmap_aggre | Q88      |   6.24x |        68.8 | 🟡 MODERATE_GUESS  | TABLE_SCAN  | 28.8M → 418.5K |         11.5x |
| self_join_decomposit | Q39      |   4.76x |      101.4M | 🚨 CATASTROPHIC_BL | CTE         | 101.4M → 0    |      30566.1x |
| inner_join_conversio | Q93      |   3.44x |       831.2 | 🟠 MAJOR_HALLUCINA | PROJECTION  | 74 → 61.5K    |         17.8x |
| early_filter         | Q93, Q11 |   2.97x |       831.2 | 🟠 MAJOR_HALLUCINA | PROJECTION  | 74 → 61.5K    |         17.8x |
| intersect_to_exists  | Q14      |   2.72x |        2.6K | 🚨 CATASTROPHIC_BL | PROJECTION  | 5 → 13.2K     |          0.0x |
| rollup_to_union_wind | Q36      |   2.47x |       297.3 | 🟠 MAJOR_HALLUCINA | PROJECTION  | 48.8K → 164   |          1.1x |
| multi_intersect_exis | Q14      |   2.39x |        2.6K | 🚨 CATASTROPHIC_BL | PROJECTION  | 5 → 13.2K     |          0.0x |
| composite_decorrelat | Q35      |   2.01x |      280.1K | 🚨 CATASTROPHIC_BL | DELIM_SCAN  | 280.1K → 0    |      20106.1x |
| decorrelate          | Q1       |   1.87x |      157.8K | 🚨 CATASTROPHIC_BL | HASH_JOIN   | 1 → 157.8K    |         63.7x |
| date_cte_isolate     | Q6, Q11  |   1.86x |      253.8K | 🚨 CATASTROPHIC_BL | HASH_JOIN   | 0 → 253.8K    |        107.7x |
| union_cte_split      | Q74      |   1.57x |   127905.4M | 🚨 CATASTROPHIC_BL | PROJECTION  | 561120916.8M → 4.4K |          0.0x |
| or_to_union          | Q15      |   1.52x |        71.0 | 🟡 MODERATE_GUESS  | PROJECTION  | 2.4M → 33.3K  |          0.0x |
| materialize_cte      | Q95      |   1.43x |       239.5 | 🟠 MAJOR_HALLUCINA | FILTER      | 14.6K → 61    |          0.0x |
| shared_dimension_mul | Q80      |   1.40x |        4.0K | 🚨 CATASTROPHIC_BL | PROJECTION  | 201.7K → 51   |          8.4x |
| multi_dimension_pref | Q43      |   1.07x |       735.1 | 🟠 MAJOR_HALLUCINA | PROJECTION  | 13.2K → 18    |          0.1x |

## Summary Statistics

- **Total Examples Analyzed**: 16
- **High Q-Error (>100)**: 14/16 (88%)
- **High Speedup (>1.5x)**: 13/16 (81%)
- **Overlap (both conditions)**: 11/16 (69%)

### Correlation Strength: ✅ **STRONG** (69%)

## Q-Error Severity Distribution

- 🚨 **CATASTROPHIC_BLINDNESS**: 8/16 (50%)
- 🟠 **MAJOR_HALLUCINATION**: 6/16 (38%)
- 🟡 **MODERATE_GUESS**: 2/16 (12%)

---
*Generated from 16 gold examples with verified speedups*

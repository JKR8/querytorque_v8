# Pattern Analysis - DuckDB TPC-DS

## Transform Type Effectiveness

Analysis of optimization transforms and their success rates:

| Transform | Count | Wins | Win% | Avg Speedup |
|-----------|-------|------|------|-------------|
| date_cte_isolate | 40 | 5 | 12.5% | 0.99x |
| decorrelate | 7 | 3 | 42.9% | 1.75x |
| early_filter | 2 | 0 | 0.0% | 0.99x |
| materialize_cte | 7 | 2 | 28.6% | 0.90x |
| multi_push_predicate | 1 | 0 | 0.0% | 0.96x |
| or_to_union | 17 | 0 | 0.0% | 0.88x |
| pushdown | 9 | 3 | 33.3% | 1.25x |
| reorder_join | 1 | 1 | 100.0% | 1.22x |
| semantic_rewrite | 4 | 2 | 50.0% | 0.92x |


## Gold Pattern Correlations

From ML pattern weights model:

- **GLD-001**: decorrelate (confidence 1.0, 2.92x on Q1)
- **GLD-002**: or_to_union (confidence 1.0, 2.78x on Q15)
- **GLD-003**: early_filter, decorrelate (mixed confidence)
- **GLD-004**: decorrelate, union_cte_split (0.5-0.5 confidence)
- **GLD-005**: decorrelate (1.0 confidence, 2.92x)
- **GLD-006**: union_cte_split (1.0 confidence, 1.36x)

## Risk Distribution

- **LOW**: 78 queries
- **MEDIUM**: 10 queries


## Speedup Distribution

Distribution of measured speedups across all 99 queries:

| Speedup Range | Count | Queries |
|---|---|---|
| ≤0.5x | 9 | [9, 16, 30, 32, 34]... |
| 0.5-0.9x | 8 | [7, 24, 26, 53, 70]... |
| 0.9-1.0x | 25 | [3, 11, 14, 21, 22]... |
| 1.0-1.2x | 39 | [4, 5, 8, 10, 12]... |
| 1.2-1.5x | 8 | [6, 28, 62, 66, 74]... |
| 1.5-2.0x | 5 | [35, 51, 59, 65, 90] |
| >2.0x | 5 | [1, 2, 15, 81, 93] |

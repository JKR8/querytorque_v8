# Discovery Prompts

Prompts designed to push Claude toward discovering NEW optimization patterns by explicitly forbidding known patterns.

## Files

### PROMPT_DISCOVER_NEW_PATTERNS.txt
- **Purpose**: Find completely new optimization techniques we haven't codified yet
- **Usage**: Replace the standard optimization prompt when:
  - Investigating regressions (Q25, Q31, Q49, Q54, Q58)
  - Testing remaining neutral queries
  - Trying to break out of local optima
- **Key Feature**: Lists all 17 known patterns explicitly and forbids their use
- **Output**: Requires model to explain new pattern discovered (NEW PATTERN name + description)

## Known Patterns (Forbidden in Discovery)

1. date_cte_isolate
2. dimension_cte_isolate
3. multi_dimension_prefetch
4. early_filter
5. decorrelate
6. pushdown
7. or_to_union
8. materialize_cte
9. intersect_to_exists
10. union_cte_split
11. single_pass_aggregation
12. time_bucket_aggregation
13. multi_cte_chain
14. triple_dimension_isolate
15. dual_dimension_isolate
16. channel_split_union
17. prefetch_fact_join

## Expected New Patterns

Suggestions for areas to explore:
- Aggregation strategies (aggregate_before_join, partial aggregation)
- Join ordering (reverse_join_order, diamond_join_optimization)
- Subquery placement (bottom_up_aggregation)
- Window functions (custom_window_partitioning)
- Set operations (creative INTERSECT/EXCEPT usage)
- Conditional logic (boolean_simplification)
- Data flow optimization (distributed_groupby, materialization_strategy)
- Hybrid approaches (combined techniques)

## How to Use

1. Load the query to optimize
2. Substitute `[QUERY_WILL_BE_INSERTED_HERE]` with actual SQL
3. Send to Claude with this prompt
4. Examine response for `[OPTIMIZATION ANALYSIS]` section
5. Extract NEW PATTERN name and description
6. Add to gold examples if speedup is significant

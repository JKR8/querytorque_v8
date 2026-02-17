# DuckDB Dialect Knowledge

## Metadata
- dialect: `duckdb`
- version: `2026-02-17-format-v1`
- source_of_truth:
  - engine_profile: `constraints/engine_profile_duckdb.json`
  - transforms: `knowledge/transforms.json`
  - examples: `examples/duckdb/*.json`
- generated_from: `hybrid`
- last_updated: `2026-02-17`

## Engine Strengths (Do Not Fight)
| Strength ID | Summary | Implication | Evidence |
|---|---|---|---|
| `INTRA_SCAN_PREDICATE_PUSHDOWN` | Pushes WHERE filters into scan nodes. | If the selective filter is already at scan, do not add rewrite-only CTE layers. | `engine_profile_duckdb.json` |
| `SAME_COLUMN_OR` | OR on same column is handled in one scan. | Do not split same-column OR into UNION branches. | `0.23x`, `0.59x` regressions |
| `HASH_JOIN_SELECTION` | Hash join choice is generally reliable. | Reduce input cardinality first; do not force manual join reordering by default. | `engine_profile_duckdb.json` |
| `CTE_INLINING` | Single-reference CTEs are inlined. | Single-use CTEs are often free; multi-ref CTEs still need care. | `engine_profile_duckdb.json` |
| `COLUMNAR_PROJECTION` | Only referenced columns are read. | Prefer narrow projections; avoid `SELECT *` in staging CTEs. | `engine_profile_duckdb.json` |
| `PARALLEL_AGGREGATION` | Scans and aggregates parallelize well. | Keep large aggregate work in engine-native grouped form. | `engine_profile_duckdb.json` |
| `EXISTS_SEMI_JOIN` | EXISTS/NOT EXISTS uses semi-join early-stop. | Never materialize EXISTS paths into wide CTEs. | `0.14x`, `0.54x` regressions |

## Global Guards
| Guard ID | Rule | Severity | Fail Action | Source |
|---|---|---|---|---|
| `G_EXISTS_NO_MATERIALIZE` | Never materialize `EXISTS/NOT EXISTS` into standalone CTE scans. | `BLOCKER` | `SKIP_TRANSFORM` | `EXISTS_SEMI_JOIN`, regressions |
| `G_OR_SAME_COLUMN_NO_UNION` | Do not rewrite same-column OR into UNION ALL. | `BLOCKER` | `SKIP_TRANSFORM` | `SAME_COLUMN_OR`, regressions |
| `G_LOW_BASELINE_SKIP_HEAVY` | If baseline is low (`<100ms`), skip structural CTE-heavy rewrites. | `MEDIUM` | `DOWNRANK_TO_EXPLORATION` | legacy playbook |
| `G_MAX_FACT_CHAIN` | Avoid 3+ cascading fact-table CTE chains. | `HIGH` | `SKIP_PATHOLOGY` | `0.78x`, `0.50x` regressions |
| `G_NO_ORPHANED_CTE` | Remove original CTE bodies when split variants replace them. | `HIGH` | `REQUIRE_MANUAL_REVIEW` | `0.49x`, `0.68x` regressions |
| `G_DIM_CROSSJOIN_HARD_STOP` | Never cross-join 3+ filtered dimension CTEs. | `BLOCKER` | `SKIP_TRANSFORM` | `0.0076x` regression |
| `G_EXPLICIT_JOIN_STYLE` | Convert comma joins to explicit `JOIN ... ON`. | `MEDIUM` | `DOWNRANK_TO_EXPLORATION` | legacy playbook |

## Decision Gates (Normative Contract)
| Gate ID | Scope | Type | Severity | Check | Pass Criteria | Fail Action | Evidence Required |
|---|---|---|---|---|---|---|---|
| `DG_TYPE_ENUM` | global | `SEMANTIC_RISK` | `BLOCKER` | Gate type validity | One of `SQL_PATTERN`, `PLAN_SIGNAL`, `RUNTIME_CONTEXT`, `SEMANTIC_RISK` | `REQUIRE_MANUAL_REVIEW` | gate row schema |
| `DG_SEVERITY_ENUM` | global | `SEMANTIC_RISK` | `BLOCKER` | Severity validity | One of `BLOCKER`, `HIGH`, `MEDIUM` | `REQUIRE_MANUAL_REVIEW` | gate row schema |
| `DG_FAIL_ACTION_ENUM` | global | `SEMANTIC_RISK` | `BLOCKER` | Fail action validity | One of `SKIP_PATHOLOGY`, `SKIP_TRANSFORM`, `DOWNRANK_TO_EXPLORATION`, `REQUIRE_MANUAL_REVIEW` | `REQUIRE_MANUAL_REVIEW` | gate row schema |
| `DG_BLOCKER_POLICY` | global | `RUNTIME_CONTEXT` | `BLOCKER` | Any blocker failed | Failed blocker always blocks that pattern/transform path | `SKIP_PATHOLOGY` | failed gate log |
| `DG_MIN_PATTERN_GATES` | pattern | `RUNTIME_CONTEXT` | `HIGH` | Gate coverage | Each pattern has at least 1 `SEMANTIC_RISK`, 1 `PLAN_SIGNAL`, 1 `RUNTIME_CONTEXT` gate | `REQUIRE_MANUAL_REVIEW` | pattern gate table |
| `DG_EVIDENCE_BINDING` | global | `RUNTIME_CONTEXT` | `HIGH` | Claim traceability | Quantitative claims map to example IDs or benchmark artifacts | `REQUIRE_MANUAL_REVIEW` | evidence table row |

## Gap-Driven Optimization Patterns

### Pattern ID: `CROSS_CTE_PREDICATE_BLINDNESS` (`HIGH`)
- Goal: `SMALLEST_SET_FIRST`
- Detect: row counts are flat through CTE chain then drop late.
- Preferred transforms: `date_cte_isolate`, `multi_dimension_prefetch`, `self_join_decomposition`, `prefetch_fact_join`.

#### Decision Gates for `CROSS_CTE_PREDICATE_BLINDNESS`
| Gate ID | Type | Severity | Check | Pass Criteria | Fail Action | Evidence |
|---|---|---|---|---|---|---|
| `G_CROSS_CTE_FILTER_RATIO` | `PLAN_SIGNAL` | `HIGH` | Late filter selectivity ratio | Strong if `>5:1`; moderate if `2:1-5:1` and baseline high | `DOWNRANK_TO_EXPLORATION` | plan row-flow |
| `G_CROSS_CTE_FACT_COUNT` | `RUNTIME_CONTEXT` | `BLOCKER` | Fact-table fanout risk | 1 fact safe, 2 careful, 3+ blocked | `SKIP_PATHOLOGY` | join graph |
| `G_CROSS_CTE_SEMANTIC` | `SEMANTIC_RISK` | `HIGH` | Existing CTE already filtered | Skip when target predicate already pushed | `SKIP_TRANSFORM` | SQL predicate map |

#### Evidence Table
| Example ID | Query | Warehouse | Validation | Orig ms | Opt ms | Speedup | Outcome |
|---|---|---|---|---:|---:|---:|---|
| `date_cte_isolate` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `4.00x` | `WIN` |
| `multi_dimension_prefetch` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `2.71x` | `WIN` |
| `self_join_decomposition` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `4.76x` | `WIN` |

#### Failure Modes
| Pattern | Impact | Triggered Gate | Mitigation |
|---|---|---|---|
| 3-dimension cross join | `0.0076x` | `G_CROSS_CTE_FACT_COUNT` | Join each filtered dimension directly to fact path |
| 3-way fact chain lock | `0.50x` | `G_CROSS_CTE_FACT_COUNT` | Limit to <=2 fact-chain layers |
| Over-decomposition of already filtered CTE | `0.71x` | `G_CROSS_CTE_SEMANTIC` | Skip if predicate already present upstream |

### Pattern ID: `REDUNDANT_SCAN_ELIMINATION` (`HIGH`)
- Goal: `DONT_REPEAT_WORK`
- Detect: repeated scans of same table with identical joins but bucketed predicates.
- Preferred transforms: `single_pass_aggregation`, `channel_bitmap_aggregation`.

#### Decision Gates for `REDUNDANT_SCAN_ELIMINATION`
| Gate ID | Type | Severity | Check | Pass Criteria | Fail Action | Evidence |
|---|---|---|---|---|---|---|
| `G_REPEAT_SCAN_STRUCTURAL_MATCH` | `SQL_PATTERN` | `HIGH` | Join skeleton equivalence across branches | Same join keys and table path | `SKIP_TRANSFORM` | AST compare |
| `G_REPEAT_SCAN_BRANCH_COUNT` | `RUNTIME_CONTEXT` | `MEDIUM` | Branch count | <=8 branches | `DOWNRANK_TO_EXPLORATION` | branch count |
| `G_REPEAT_SCAN_AGG_SAFE` | `SEMANTIC_RISK` | `BLOCKER` | Aggregate compatibility | Safe with `COUNT/SUM/AVG/MIN/MAX`; avoid variance-style aggregates | `SKIP_PATHOLOGY` | select list audit |

#### Evidence Table
| Example ID | Query | Warehouse | Validation | Orig ms | Opt ms | Speedup | Outcome |
|---|---|---|---|---:|---:|---:|---|
| `single_pass_aggregation` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `4.47x` | `WIN` |
| `channel_bitmap_aggregation` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `6.24x` | `WIN` |

#### Failure Modes
| Pattern | Impact | Triggered Gate | Mitigation |
|---|---|---|---|
| none observed in curated examples | `n/a` | `n/a` | Keep branch and aggregate gates enforced |

### Pattern ID: `CORRELATED_SUBQUERY_PARALYSIS` (`LOW`)
- Goal: `SETS_OVER_LOOPS`
- Detect: nested loop style repeated aggregate evaluation by outer-row correlation.
- Preferred transforms: `decorrelate`, `composite_decorrelate_union`.

#### Decision Gates for `CORRELATED_SUBQUERY_PARALYSIS`
| Gate ID | Type | Severity | Check | Pass Criteria | Fail Action | Evidence |
|---|---|---|---|---|---|---|
| `G_CORR_EXISTS_PROTECTED` | `SEMANTIC_RISK` | `BLOCKER` | EXISTS-path decorrelation | EXISTS/NOT EXISTS stays protected | `SKIP_PATHOLOGY` | SQL form + plan |
| `G_CORR_ALREADY_DECORRELATED` | `PLAN_SIGNAL` | `HIGH` | Correlation already flattened | Skip if hash join already on correlation key | `SKIP_TRANSFORM` | EXPLAIN nodes |
| `G_CORR_OUTER_SIZE` | `RUNTIME_CONTEXT` | `MEDIUM` | Outer cardinality after filters | If outer set small, decorrelation optional | `DOWNRANK_TO_EXPLORATION` | row count estimate |

#### Evidence Table
| Example ID | Query | Warehouse | Validation | Orig ms | Opt ms | Speedup | Outcome |
|---|---|---|---|---:|---:|---:|---|
| `decorrelate` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `2.92x` | `WIN` |
| `composite_decorrelate_union` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `2.42x` | `WIN` |

#### Failure Modes
| Pattern | Impact | Triggered Gate | Mitigation |
|---|---|---|---|
| EXISTS path rewritten as materialized branch | `0.34x`, `0.14x` | `G_CORR_EXISTS_PROTECTED` | Preserve semi-join style EXISTS |
| Already decorrelated shape rewritten again | `0.71x` | `G_CORR_ALREADY_DECORRELATED` | Skip if plan already uses join-style decorrelation |

### Pattern ID: `AGGREGATE_BELOW_JOIN_BLINDNESS` (`HIGH`)
- Goal: `MINIMIZE_ROWS_TOUCHED`
- Detect: aggregate appears after high-cardinality join fanout.
- Preferred transforms: `aggregate_pushdown`, star-join prefilter variants.

#### Decision Gates for `AGGREGATE_BELOW_JOIN_BLINDNESS`
| Gate ID | Type | Severity | Check | Pass Criteria | Fail Action | Evidence |
|---|---|---|---|---|---|---|
| `G_AGG_KEY_COMPAT` | `SEMANTIC_RISK` | `BLOCKER` | Grouping-key compatibility | Group keys remain compatible with join keys | `SKIP_PATHOLOGY` | grouping/join key map |
| `G_AGG_FANOUT` | `PLAN_SIGNAL` | `HIGH` | Join-to-aggregate compression opportunity | Large compression opportunity exists | `SKIP_TRANSFORM` | row-flow stats |
| `G_AGG_BASELINE` | `RUNTIME_CONTEXT` | `MEDIUM` | Baseline runtime significance | Prefer when baseline is materially high | `DOWNRANK_TO_EXPLORATION` | baseline ms |

#### Evidence Table
| Example ID | Query | Warehouse | Validation | Orig ms | Opt ms | Speedup | Outcome |
|---|---|---|---|---:|---:|---:|---|
| `aggregate_pushdown` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `42.90x` | `WIN` |

#### Failure Modes
| Pattern | Impact | Triggered Gate | Mitigation |
|---|---|---|---|
| none observed in curated examples | `n/a` | `n/a` | Keep key-compatibility blocker mandatory |

### Pattern ID: `LEFT_JOIN_FILTER_ORDER_RIGIDITY` (`HIGH`)
- Goal: `ARM_THE_OPTIMIZER`
- Detect: LEFT JOIN plus right-table filter that proves non-null right side.
- Preferred transforms: `inner_join_conversion`.

#### Decision Gates for `LEFT_JOIN_FILTER_ORDER_RIGIDITY`
| Gate ID | Type | Severity | Check | Pass Criteria | Fail Action | Evidence |
|---|---|---|---|---|---|---|
| `G_LEFTJOIN_NULL_SEMANTICS` | `SEMANTIC_RISK` | `BLOCKER` | Null-sensitive expression usage | No null-preserving CASE/COALESCE dependency on right side | `SKIP_PATHOLOGY` | expression audit |
| `G_LEFTJOIN_PROOF` | `PLAN_SIGNAL` | `HIGH` | Null-eliminating right-side predicate | Proof present in WHERE predicates | `SKIP_TRANSFORM` | predicate map |
| `G_LEFTJOIN_RUNTIME` | `RUNTIME_CONTEXT` | `MEDIUM` | Runtime payoff | Prioritize when join dominates runtime | `DOWNRANK_TO_EXPLORATION` | operator costs |

#### Evidence Table
| Example ID | Query | Warehouse | Validation | Orig ms | Opt ms | Speedup | Outcome |
|---|---|---|---|---:|---:|---:|---|
| `inner_join_conversion` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `3.44x` | `WIN` |

#### Failure Modes
| Pattern | Impact | Triggered Gate | Mitigation |
|---|---|---|---|
| none observed in curated examples | `n/a` | `n/a` | keep null-semantics blocker enforced |

### Pattern ID: `CROSS_COLUMN_OR_DECOMPOSITION` (`MEDIUM`)
- Goal: `MINIMIZE_ROWS_TOUCHED`
- Detect: OR across different columns with very high filter discard.
- Preferred transforms: `or_to_union`.

#### Decision Gates for `CROSS_COLUMN_OR_DECOMPOSITION`
| Gate ID | Type | Severity | Check | Pass Criteria | Fail Action | Evidence |
|---|---|---|---|---|---|---|
| `G_OR_CROSS_COLUMN_ONLY` | `SQL_PATTERN` | `BLOCKER` | OR column shape | Cross-column OR only; same-column OR blocked | `SKIP_PATHOLOGY` | predicate parse |
| `G_OR_BRANCH_LIMIT` | `RUNTIME_CONTEXT` | `HIGH` | Branch explosion risk | <=3 union branches | `SKIP_TRANSFORM` | branch count |
| `G_OR_NO_SELF_JOIN_MULTIPLY` | `SEMANTIC_RISK` | `HIGH` | Self-join plus union branch multiplier | No multiplicative re-execution pattern | `REQUIRE_MANUAL_REVIEW` | join graph |

#### Evidence Table
| Example ID | Query | Warehouse | Validation | Orig ms | Opt ms | Speedup | Outcome |
|---|---|---|---|---:|---:|---:|---|
| `or_to_union` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `3.17x` | `WIN` |

#### Failure Modes
| Pattern | Impact | Triggered Gate | Mitigation |
|---|---|---|---|
| Nested OR expansion to 9 branches | `0.23x` | `G_OR_BRANCH_LIMIT` | hard cap branch count |
| Same-column OR split | `0.59x` | `G_OR_CROSS_COLUMN_ONLY` | keep as native OR |
| Self-join repeated per branch | `0.51x` | `G_OR_NO_SELF_JOIN_MULTIPLY` | avoid branch split or require manual review |

## Pruning Guide
| Plan shows | Skip |
|---|---|
| No nested loops | `CORRELATED_SUBQUERY_PARALYSIS` |
| Each table appears once | `REDUNDANT_SCAN_ELIMINATION` |
| No LEFT JOIN | `LEFT_JOIN_FILTER_ORDER_RIGIDITY` |
| No OR predicates | `CROSS_COLUMN_OR_DECOMPOSITION` |
| No GROUP BY | `AGGREGATE_BELOW_JOIN_BLINDNESS` |
| Baseline < 50ms | all CTE-heavy structural rewrites |
| Row counts already decrease early | `CROSS_CTE_PREDICATE_BLINDNESS` |

## Regression Registry
| Severity | Transform | Speedup | Query | Root Cause |
|---|---|---:|---|---|
| `CATASTROPHIC` | `dimension_cte_isolate` | `0.0076x` | `n/a` | 3-dimension Cartesian cross-join |
| `CATASTROPHIC` | `materialize_cte` | `0.14x` | `n/a` | semi-join short-circuit destroyed |
| `SEVERE` | `or_to_union` | `0.23x` | `n/a` | nested OR branch explosion |
| `SEVERE` | `decorrelate` | `0.34x` | `n/a` | protected EXISTS/semi-join path rewritten |
| `MAJOR` | `union_cte_split` | `0.49x` | `n/a` | orphaned original CTE retained |
| `MAJOR` | `date_cte_isolate` | `0.50x` | `n/a` | fact join-order lock |
| `MODERATE` | `or_to_union` | `0.59x` | `n/a` | same-column OR split |
| `MODERATE` | `decorrelate` | `0.71x` | `n/a` | already decorrelated plan rewritten |
| `MINOR` | `multi_dimension_prefetch` | `0.77x` | `n/a` | forced suboptimal join order |

## Notes
- Additional low-priority patterns also tracked in profile: `INTERSECT_MATERIALIZATION`, `WINDOW_BEFORE_JOIN`, `UNION_CTE_SELF_JOIN_DECOMPOSITION`, `SHARED_SUBEXPRESSION`.
- Config tuning is separate from SQL rewrite guidance.

# Snowflake Dialect Knowledge

## Metadata
- dialect: `snowflake`
- version: `2026-02-17-format-v1`
- source_of_truth:
  - engine_profile: `constraints/engine_profile_snowflake.json`
  - transforms: `knowledge/transforms.json`
  - examples: `examples/snowflake/*.json`
- generated_from: `hybrid`
- last_updated: `2026-02-17`

## Engine Strengths (Do Not Fight)
| Strength ID | Summary | Implication | Evidence |
|---|---|---|---|
| `MICRO_PARTITION_PRUNING` | Clustered filter predicates prune partitions early. | Avoid wrapping filter columns in functions when pruning is critical. | `engine_profile_snowflake.json` |
| `COLUMN_PRUNING` | Only referenced columns are read through query graph. | Keep projections narrow; avoid unnecessary wide intermediate selects. | `engine_profile_snowflake.json` |
| `PREDICATE_PUSHDOWN` | Filters push into storage and single-ref CTE paths. | Do not duplicate already-effective filters blindly. | `engine_profile_snowflake.json` |
| `CORRELATED_DECORRELATION` | Simple EXISTS/IN correlation often decorrelates to joins. | Reserve manual decorrelation for scalar aggregate correlation cases. | `engine_profile_snowflake.json` |
| `SEMI_JOIN` | EXISTS patterns get early-stop semi-join behavior. | Protect EXISTS from materialization rewrites. | `engine_profile_snowflake.json` |
| `JOIN_FILTER` | Join-filter pushdown commonly appears on star-schema joins. | Avoid plan-shape rewrites that remove effective join filters without reason. | `engine_profile_snowflake.json`, `benchmarks/snowflake_tpcds/explains/*.json` |
| `COST_BASED_JOIN_ORDER` | Join ordering is generally cost-driven and robust. | Prefer cardinality reduction over forced join-order plans. | `engine_profile_snowflake.json` |
| `QUALIFY_OPTIMIZATION` | QUALIFY is native and efficient for window filtering. | Prefer QUALIFY-form filter placement where semantics permit. | `engine_profile_snowflake.json` |

## Global Guards
| Guard ID | Rule | Severity | Fail Action | Source |
|---|---|---|---|---|
| `G_SF_EXISTS_PROTECTED` | Never materialize `EXISTS/NOT EXISTS` into broad CTE branches. | `BLOCKER` | `SKIP_TRANSFORM` | `SEMI_JOIN` strength |
| `G_SF_FILTER_FUNCTION_WRAP` | Do not wrap partition/filter keys in functions when pruning matters. | `HIGH` | `SKIP_TRANSFORM` | `MICRO_PARTITION_PRUNING` strength |
| `G_SF_JOINFILTER_PRESERVE` | Avoid destructive shape rewrites when join-filter behavior is already strong. | `MEDIUM` | `REQUIRE_MANUAL_REVIEW` | `JOIN_FILTER` strength |
| `G_SF_UNION_BRANCH_LIMIT` | Keep UNION ALL branch count modest for branch-level scan costs. | `MEDIUM` | `DOWNRANK_TO_EXPLORATION` | legacy playbook |
| `G_SF_CTE_REUSE_RULE` | Single-ref CTEs tend to inline; multi-ref CTEs need explicit reason. | `MEDIUM` | `DOWNRANK_TO_EXPLORATION` | legacy playbook |
| `G_SF_NOTIN_NULL_SAFETY` | Use NULL-safe anti-join semantics (prefer NOT EXISTS to unsafe NOT IN patterns). | `HIGH` | `REQUIRE_MANUAL_REVIEW` | legacy playbook |
| `G_SF_LOW_BASELINE_SKIP_HEAVY` | If baseline is low (`<100ms`), skip structural rewrite churn. | `MEDIUM` | `DOWNRANK_TO_EXPLORATION` | legacy playbook |

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

### Pattern ID: `CORRELATED_SUBQUERY_PARALYSIS` (`HIGH`)
- Goal: `DECORRELATE`
- Detect: correlated scalar aggregate subquery re-scans fact table per outer row.
- Preferred transforms: `sf_inline_decorrelate`, `sf_shared_scan_decorrelate`.

#### Decision Gates for `CORRELATED_SUBQUERY_PARALYSIS`
| Gate ID | Type | Severity | Check | Pass Criteria | Fail Action | Evidence |
|---|---|---|---|---|---|---|
| `G_SF_CORR_SCALAR_REQUIRED` | `SQL_PATTERN` | `BLOCKER` | Correlated scalar aggregate exists | AVG/SUM/COUNT scalar correlation present | `SKIP_PATHOLOGY` | SQL + parse |
| `G_SF_CORR_SIMPLE_EXISTS_SKIP` | `PLAN_SIGNAL` | `HIGH` | Already simple decorrelation class | Skip manual rewrite when simple EXISTS/IN already optimized | `SKIP_TRANSFORM` | EXPLAIN shape |
| `G_SF_CORR_FACT_CONTEXT` | `RUNTIME_CONTEXT` | `MEDIUM` | Fact-table involvement | Inner query actually touches fact-table path | `DOWNRANK_TO_EXPLORATION` | SQL relation map |
| `G_SF_CORR_SEMANTIC_KEYS` | `SEMANTIC_RISK` | `HIGH` | Correlation key and aggregate semantics preserved | Correlation predicates and aggregate semantics unchanged | `REQUIRE_MANUAL_REVIEW` | rewrite diff |

#### Evidence Table
| Example ID | Query | Warehouse | Validation | Orig ms | Opt ms | Speedup | Outcome |
|---|---|---|---|---:|---:|---:|---|
| `sf_inline_decorrelate` | `n/a` | `MEDIUM` | `3x3 (discard warmup, average last 2)` | `69414.7` | `2995.5` | `23.17x` | `WIN` |
| `sf_shared_scan_decorrelate` | `n/a` | `MEDIUM` | `3x3 (discard warmup, average last 2)` | `8024.6` | `1026.1` | `7.82x` | `WIN` |

#### Failure Modes
| Pattern | Impact | Triggered Gate | Mitigation |
|---|---|---|---|
| none observed in curated examples | `n/a` | `n/a` | keep blocker gates enforced |

### Pattern ID: `PREDICATE_TRANSITIVITY_FAILURE` (`n/a in engine_profile`)
- Goal: `SK_PUSHDOWN`
- Detect: date_dim filter exists but sold_date_sk range is not pushed into fact scans, often across UNION ALL or multi-fact comma-join shapes.
- Preferred transforms: `sf_sk_pushdown_union_all`, `sf_sk_pushdown_multi_fact`.

#### Decision Gates for `PREDICATE_TRANSITIVITY_FAILURE`
| Gate ID | Type | Severity | Check | Pass Criteria | Fail Action | Evidence |
|---|---|---|---|---|---|---|
| `G_SF_SK_DATE_FILTER_REQUIRED` | `SQL_PATTERN` | `BLOCKER` | Date filter on date_dim exists | Date filter plus sold_date_sk join path present | `SKIP_PATHOLOGY` | SQL parse |
| `G_SF_SK_SCAN_PRESSURE` | `PLAN_SIGNAL` | `HIGH` | Fact scan pressure | Fact scan appears broad enough to justify pushdown | `DOWNRANK_TO_EXPLORATION` | EXPLAIN table scan stats |
| `G_SF_SK_COMPUTE_BOUND_SKIP` | `RUNTIME_CONTEXT` | `HIGH` | Compute-bound workload | Skip when dominant cost is compute-heavy aggregate/rollup path | `SKIP_TRANSFORM` | operator profile |
| `G_SF_SK_RANGE_SEMANTICS` | `SEMANTIC_RISK` | `HIGH` | Date key range correctness | Date_sk range derived from same predicate domain as original query | `REQUIRE_MANUAL_REVIEW` | range derivation audit |

#### Evidence Table
| Example ID | Query | Warehouse | Validation | Orig ms | Opt ms | Speedup | Outcome |
|---|---|---|---|---:|---:|---:|---|
| `sf_sk_pushdown_union_all` | `Q2` | `X-Small` | `5x trimmed mean (discard min/max, average middle 3)` | `229847.3` | `107982.0` | `2.13x` | `WIN` |
| `sf_sk_pushdown_3fact` | `Q56` | `X-Small` | `5x trimmed mean (discard min/max, average middle 3)` | `10233.6` | `8729.9` | `1.17x` | `WIN` |

#### Failure Modes
| Pattern | Impact | Triggered Gate | Mitigation |
|---|---|---|---|
| Wide-range pushdown gave neutral result | `0.97x` (legacy note) | `G_SF_SK_SCAN_PRESSURE` | require strong scan-pressure evidence |
| Compute-bound rollup path timed out | timeout (legacy note) | `G_SF_SK_COMPUTE_BOUND_SKIP` | skip pushdown-only strategy on compute-bound plans |

## Pruning Guide
| Plan shows | Skip |
|---|---|
| No correlated scalar aggregate pattern | `CORRELATED_SUBQUERY_PARALYSIS` |
| Correlation is simple EXISTS/IN already optimized | `CORRELATED_SUBQUERY_PARALYSIS` |
| No date_dim filter or no sold_date_sk join linkage | `PREDICATE_TRANSITIVITY_FAILURE` |
| Low scan pressure on fact tables | `PREDICATE_TRANSITIVITY_FAILURE` |
| Dominant compute-bound aggregate/rollup path | `PREDICATE_TRANSITIVITY_FAILURE` |
| Baseline < 100ms | most structural rewrite paths |

## Regression Registry
| Severity | Transform | Speedup | Query | Root Cause |
|---|---|---:|---|---|
| `INFO` | `sf_sk_pushdown_union_all` | `0.97x` | `Q17` | wide date range reduced pruning benefit (legacy playbook note) |
| `INFO` | `sf_sk_pushdown_union_all` | `timeout` | `Q67` | compute-bound rollup path, not scan-bound (legacy playbook note) |

## Notes
- `PREDICATE_TRANSITIVITY_FAILURE` is represented in transforms and examples, but is not yet listed in `engine_profile_snowflake.json` gaps.
- Consider promoting this pattern into the Snowflake engine profile to keep profile and playbook fully aligned.

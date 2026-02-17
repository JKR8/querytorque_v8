# PostgreSQL Dialect Knowledge

## Metadata
- dialect: `postgresql`
- version: `2026-02-17-format-v1`
- source_of_truth:
  - engine_profile: `constraints/engine_profile_postgresql.json`
  - transforms: `knowledge/transforms.json`
  - examples: `examples/postgres/*.json`
- generated_from: `hybrid`
- last_updated: `2026-02-17`

## Engine Strengths (Do Not Fight)
| Strength ID | Summary | Implication | Evidence |
|---|---|---|---|
| `BITMAP_OR_SCAN` | Indexed OR predicates use BitmapOr. | Avoid default OR-to-UNION rewrites when indexes are already used. | `engine_profile_postgresql.json` |
| `SEMI_JOIN_EXISTS` | EXISTS/NOT EXISTS use semi-join early-stop. | Protect EXISTS paths from materialization rewrites. | `engine_profile_postgresql.json` |
| `INNER_JOIN_REORDERING` | Inner joins reorder well via cost model. | Prefer cardinality reduction over manual join-order forcing. | `engine_profile_postgresql.json` |
| `INDEX_ONLY_SCAN` | Covering indexes can avoid heap reads. | Small dimensions may not benefit from heavy CTE staging. | `engine_profile_postgresql.json` |
| `PARALLEL_QUERY_EXECUTION` | Large scans/aggregates parallelize. | Extra CTE fences can reduce useful parallelism. | `engine_profile_postgresql.json` |
| `JIT_COMPILATION` | Complex expressions are JIT-compiled on longer runs. | Expression complexity alone is not always the runtime bottleneck. | `engine_profile_postgresql.json` |

## Global Guards
| Guard ID | Rule | Severity | Fail Action | Source |
|---|---|---|---|---|
| `G_PG_OR_INDEX_PROTECTED` | Do not split same-column indexed OR predicates into UNION ALL. | `BLOCKER` | `SKIP_TRANSFORM` | `BITMAP_OR_SCAN`, `0.21x`, `0.26x` regressions |
| `G_PG_EXISTS_PROTECTED` | Keep simple EXISTS/NOT EXISTS in native semi-join form. | `BLOCKER` | `SKIP_TRANSFORM` | `SEMI_JOIN_EXISTS`, `0.50x`, `0.75x` regressions |
| `G_PG_CTE_DUPLICATION_STOP` | Never duplicate a 5+ table CTE body to push filters inward. | `BLOCKER` | `SKIP_TRANSFORM` | `CTE_MATERIALIZATION_FENCE` field notes |
| `G_PG_SCALE_VALIDATION` | Validate at target scale before promoting rewrite. | `HIGH` | `REQUIRE_MANUAL_REVIEW` | SF5->SF10 drift note in profile |
| `G_PG_LOW_BASELINE_SKIP_HEAVY` | If baseline is low (`<100ms`), avoid structural rewrite churn. | `MEDIUM` | `DOWNRANK_TO_EXPLORATION` | existing knowledge guidance |
| `G_PG_EXPLICIT_JOIN_STYLE` | Normalize comma joins to explicit `JOIN ... ON`. | `MEDIUM` | `DOWNRANK_TO_EXPLORATION` | `COMMA_JOIN_WEAKNESS` |

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

### Pattern ID: `COMMA_JOIN_WEAKNESS` (`HIGH`)
- Goal: `ARM_THE_OPTIMIZER`
- Detect: comma-separated FROM with join predicates in WHERE and poor row estimates.
- Preferred transforms: `date_cte_explicit_join`, `dimension_prefetch_star`, `explicit_join_materialized`.

#### Decision Gates for `COMMA_JOIN_WEAKNESS`
| Gate ID | Type | Severity | Check | Pass Criteria | Fail Action | Evidence |
|---|---|---|---|---|---|---|
| `G_PG_COMMA_JOIN_PRESENT` | `SQL_PATTERN` | `HIGH` | Comma-join pattern exists | Multiple comma-joined relations with equi-join predicates | `SKIP_TRANSFORM` | SQL parse |
| `G_PG_COMMA_FACT_FANOUT` | `RUNTIME_CONTEXT` | `HIGH` | Fact-table fanout | 1-2 fact tables; avoid broad multi-fact lockups | `DOWNRANK_TO_EXPLORATION` | join graph |
| `G_PG_COMMA_SEMANTIC` | `SEMANTIC_RISK` | `MEDIUM` | Explicit-join conversion safety | Join predicates preserved exactly | `REQUIRE_MANUAL_REVIEW` | predicate diff |

#### Evidence Table
| Example ID | Query | Warehouse | Validation | Orig ms | Opt ms | Speedup | Outcome |
|---|---|---|---|---:|---:|---:|---|
| `pg_explicit_join_materialized` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `8.56x` | `WIN` |
| `pg_dimension_prefetch_star` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `3.32x` | `WIN` |
| `pg_date_cte_explicit_join` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `2.28x` | `WIN` |

#### Failure Modes
| Pattern | Impact | Triggered Gate | Mitigation |
|---|---|---|---|
| Existing explicit join order rewritten unnecessarily | qualitative risk | `G_PG_COMMA_JOIN_PRESENT` | skip when joins already explicit |

### Pattern ID: `CORRELATED_SUBQUERY_PARALYSIS` (`HIGH`)
- Goal: `SETS_OVER_LOOPS`
- Detect: correlated scalar aggregate subquery re-executes per outer row.
- Preferred transforms: `inline_decorrelate_materialized`, `pg_shared_scan_decorrelate`, `pg_state_avg_decorrelate`, `early_filter_decorrelate`.

#### Decision Gates for `CORRELATED_SUBQUERY_PARALYSIS`
| Gate ID | Type | Severity | Check | Pass Criteria | Fail Action | Evidence |
|---|---|---|---|---|---|---|
| `G_PG_CORR_SCALAR_REQUIRED` | `SQL_PATTERN` | `BLOCKER` | Correlated scalar aggregate exists | Pattern present and correlation key identified | `SKIP_PATHOLOGY` | SQL + AST |
| `G_PG_CORR_ALREADY_DECORRELATED` | `PLAN_SIGNAL` | `HIGH` | Already hash-decorrelated | Skip if plan already flattened on correlation key | `SKIP_TRANSFORM` | EXPLAIN |
| `G_PG_CORR_EXISTS_PROTECTION` | `SEMANTIC_RISK` | `BLOCKER` | EXISTS/NOT EXISTS transform request | Keep EXISTS protected | `SKIP_PATHOLOGY` | SQL + plan |
| `G_PG_CORR_FACT_COUNT` | `RUNTIME_CONTEXT` | `HIGH` | Fact-join complexity | 1-2 fact tables preferred; 3+ needs manual review | `REQUIRE_MANUAL_REVIEW` | join graph |

#### Evidence Table
| Example ID | Query | Warehouse | Validation | Orig ms | Opt ms | Speedup | Outcome |
|---|---|---|---|---:|---:|---:|---|
| `pg_shared_scan_decorrelate` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `8043.91x (timeout rescue)` | `WIN` |
| `inline_decorrelate_materialized` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `1465x (timeout rescue)` | `WIN` |
| `pg_state_avg_decorrelate` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `438.93x (timeout rescue)` | `WIN` |
| `early_filter_decorrelate` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `27.80x` | `WIN` |

#### Failure Modes
| Pattern | Impact | Triggered Gate | Mitigation |
|---|---|---|---|
| EXISTS path materialized | `0.75x` | `G_PG_CORR_EXISTS_PROTECTION` | preserve semi-join EXISTS shape |
| Multi-fact lock during decorrelation | `0.51x` | `G_PG_CORR_FACT_COUNT` | avoid aggressive decorrelation on broad multi-fact joins |

### Pattern ID: `NON_EQUI_JOIN_INPUT_BLINDNESS` (`HIGH`)
- Goal: `MINIMIZE_ROWS_TOUCHED`
- Detect: non-equi join with high input cardinality and late selectivity.
- Preferred transforms: `materialized_dimension_fact_prefilter`.

#### Decision Gates for `NON_EQUI_JOIN_INPUT_BLINDNESS`
| Gate ID | Type | Severity | Check | Pass Criteria | Fail Action | Evidence |
|---|---|---|---|---|---|---|
| `G_PG_NONEQUI_PRESENT` | `SQL_PATTERN` | `HIGH` | Non-equi predicate exists | BETWEEN/< /> on join path | `SKIP_TRANSFORM` | SQL parse |
| `G_PG_NONEQUI_CARDINALITY` | `PLAN_SIGNAL` | `HIGH` | Input size pressure | Large inputs on both sides with late drop | `DOWNRANK_TO_EXPLORATION` | plan rows |
| `G_PG_NONEQUI_FILTER_QUALITY` | `SEMANTIC_RISK` | `HIGH` | Prefilter selectivity realism | Tight, not loose superset prefilter | `SKIP_TRANSFORM` | predicate audit |

#### Evidence Table
| Example ID | Query | Warehouse | Validation | Orig ms | Opt ms | Speedup | Outcome |
|---|---|---|---|---:|---:|---:|---|
| `pg_materialized_dimension_fact_prefilter` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `12.07x` | `WIN` |

#### Failure Modes
| Pattern | Impact | Triggered Gate | Mitigation |
|---|---|---|---|
| Loose OR/UNION superset filter | `0.79x` | `G_PG_NONEQUI_FILTER_QUALITY` | require tight prefilter predicates only |

### Pattern ID: `CTE_MATERIALIZATION_FENCE` (`MEDIUM`)
- Goal: `ARM_THE_OPTIMIZER`
- Detect: large CTE fences block predicate pushdown or parallel flow.
- Preferred transforms: strategic materialization only when reuse is real.

#### Decision Gates for `CTE_MATERIALIZATION_FENCE`
| Gate ID | Type | Severity | Check | Pass Criteria | Fail Action | Evidence |
|---|---|---|---|---|---|---|
| `G_PG_CTE_DUPLICATION_BLOCK` | `SEMANTIC_RISK` | `BLOCKER` | CTE body duplication | No duplication of heavy CTE bodies | `SKIP_PATHOLOGY` | rewrite diff |
| `G_PG_CTE_REUSE_REQUIRED` | `RUNTIME_CONTEXT` | `HIGH` | Reuse benefit | CTE has meaningful multi-consumer reuse | `DOWNRANK_TO_EXPLORATION` | reference count |
| `G_PG_CTE_EXISTS_INTERSECT_RISK` | `PLAN_SIGNAL` | `HIGH` | High-risk contexts | avoid problematic EXISTS or INTERSECT fences | `SKIP_TRANSFORM` | SQL + EXPLAIN |

#### Evidence Table
| Example ID | Query | Warehouse | Validation | Orig ms | Opt ms | Speedup | Outcome |
|---|---|---|---|---:|---:|---:|---|
| `profile_note_strategic_materialization` | `n/a` | `n/a` | `engine profile` | `n/a` | `n/a` | `1.95x` | `WIN` |

#### Failure Modes
| Pattern | Impact | Triggered Gate | Mitigation |
|---|---|---|---|
| Fence blocked pushdown | `0.74x` | `G_PG_CTE_REUSE_REQUIRED` | avoid forced fence without reuse |
| Date CTE fence blocked INTERSECT optimization | `0.77x` | `G_PG_CTE_EXISTS_INTERSECT_RISK` | avoid fence in set-operation paths |
| Duplicated 18-table CTE body | `0.65x` | `G_PG_CTE_DUPLICATION_BLOCK` | filter materialized output, not CTE body copy |

### Pattern ID: `CROSS_CTE_PREDICATE_BLINDNESS` (`MEDIUM`)
- Goal: `SMALLEST_SET_FIRST`
- Detect: selective predicates applied too late after CTE boundaries.
- Preferred transforms: `date_cte_explicit_join`, `early_filter_decorrelate`, explicit join cleanup.

#### Decision Gates for `CROSS_CTE_PREDICATE_BLINDNESS`
| Gate ID | Type | Severity | Check | Pass Criteria | Fail Action | Evidence |
|---|---|---|---|---|---|---|
| `G_PG_CROSS_CTE_COMMA_JOIN_PAIRING` | `SQL_PATTERN` | `HIGH` | Comma-join pairing with filter push | perform explicit join cleanup with push | `DOWNRANK_TO_EXPLORATION` | SQL rewrite plan |
| `G_PG_CROSS_CTE_SCALE_GUARD` | `RUNTIME_CONTEXT` | `HIGH` | Target-scale confidence | evidence validated near target scale | `REQUIRE_MANUAL_REVIEW` | benchmark scope |
| `G_PG_CROSS_CTE_SETOP_RISK` | `SEMANTIC_RISK` | `MEDIUM` | Set-op sensitivity | avoid blind pushdown through INTERSECT/EXCEPT-heavy shapes | `SKIP_TRANSFORM` | SQL structure |

#### Evidence Table
| Example ID | Query | Warehouse | Validation | Orig ms | Opt ms | Speedup | Outcome |
|---|---|---|---|---:|---:|---:|---|
| `pg_date_cte_explicit_join` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `2.28x` | `WIN` |
| `pg_dimension_prefetch_star` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `3.32x` | `WIN` |
| `early_filter_decorrelate` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `27.80x` | `WIN` |

#### Failure Modes
| Pattern | Impact | Triggered Gate | Mitigation |
|---|---|---|---|
| SF5 win did not hold at SF10 | `0.97x` at target | `G_PG_CROSS_CTE_SCALE_GUARD` | require target-scale validation |
| Over-decomposition of efficient query | `0.55x` | `G_PG_CROSS_CTE_SETOP_RISK` | avoid excessive decomposition |

## Pruning Guide
| Plan shows | Skip |
|---|---|
| All joins already explicit and estimates stable | `COMMA_JOIN_WEAKNESS` |
| No correlated scalar aggregate in SQL/plan | `CORRELATED_SUBQUERY_PARALYSIS` |
| No non-equi join predicate | `NON_EQUI_JOIN_INPUT_BLINDNESS` |
| CTE is single-use and not expensive | `CTE_MATERIALIZATION_FENCE` |
| Predicate already applied at the earliest valid node | `CROSS_CTE_PREDICATE_BLINDNESS` |
| Baseline < 100ms | most structural rewrite paths |

## Regression Registry
| Severity | Transform | Speedup | Query | Root Cause |
|---|---|---:|---|---|
| `SEVERE` | `or_to_union` style split on indexed OR | `0.21x` | `n/a` | fought BitmapOr strength |
| `SEVERE` | `or_to_union` style split on indexed OR | `0.26x` | `n/a` | fought BitmapOr strength |
| `MAJOR` | decorrelation on protected EXISTS path | `0.50x` | `n/a` | semi-join path broken |
| `MODERATE` | broad decorrelation on multi-fact shape | `0.51x` | `n/a` | join-order lock |
| `MODERATE` | duplicated deep CTE body | `0.65x` | `n/a` | forced materialization cost |
| `MODERATE` | CTE fence blocked pushdown | `0.74x` | `n/a` | optimization fence |
| `MODERATE` | CTE fence harmed set-op path | `0.77x` | `n/a` | blocked INTERSECT optimization |
| `MINOR` | loose prefilter before non-equi join | `0.79x` | `n/a` | low-selectivity staging |

## Notes
- Config tuning is separate from rewrite logic. Use `knowledge/config/postgresql.json` after semantic-safe SQL is established.
- `set_local_config_intel` in engine profile is authoritative for runtime knobs, not this rewrite playbook.

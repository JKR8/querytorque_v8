# Databricks (Spark SQL) Dialect Knowledge

## Metadata
- dialect: `databricks`
- version: `2026-02-19-format-v1`
- source_of_truth:
  - engine_profile: `constraints/engine_profile_databricks.json` *(not yet created)*
  - transforms: `knowledge/transforms.json`
  - examples: `examples/databricks/*.json` *(not yet created)*
- generated_from: `stub`
- last_updated: `2026-02-19`

## Engine Overview
Databricks SQL runs Spark SQL on the Photon execution engine (C++ vectorized).
Warehouses are serverless or classic; queries execute on auto-scaling clusters.
TPC-H SF5 baseline: 22/22 queries pass on warehouse `8696c5b909737cb9`.

## Engine Strengths (Do Not Fight)
| Strength ID | Summary | Implication |
|---|---|---|
| `AQE` | Adaptive Query Execution dynamically adjusts shuffle partitions, join strategies, and skew handling at runtime. | Do not hard-code partition counts or broadcast thresholds — AQE will optimize. |
| `BROADCAST_JOIN` | Small tables auto-broadcast when under threshold (~10 MB default). | Avoid manual broadcast hints for small dimension tables. |
| `PHOTON_VECTORIZED` | Photon engine vectorizes scans, filters, aggregations, and joins in C++. | Prefer simple expressions that Photon can vectorize; avoid complex UDFs. |
| `PREDICATE_PUSHDOWN` | Filters push through joins, projections, and into Delta scan predicates. | Do not duplicate filters that already push down effectively. |
| `COLUMN_PRUNING` | Only referenced columns are read from columnar Delta files. | Keep projections narrow. |
| `CBO` | Cost-based optimizer uses table/column statistics for join ordering. | Ensure ANALYZE TABLE has been run for optimal plans. |
| `DELTA_DATA_SKIPPING` | Min/max statistics on Delta files enable data skipping for filter predicates. | Avoid wrapping filter columns in functions that prevent skipping. |

## Global Guards
| Guard ID | Rule | Severity |
|---|---|---|
| `G_DBX_AQE_RESPECT` | Do not force shuffle partition counts or join strategies that conflict with AQE. | `BLOCKER` |
| `G_DBX_NO_UDF_HOTPATH` | Avoid Python/Scala UDFs on hot-path columns — Photon cannot vectorize them. | `HIGH` |
| `G_DBX_DELTA_SKIP` | Do not wrap filter-key columns in functions that prevent Delta data skipping. | `HIGH` |

## Pathologies
*(No pathologies documented yet — pending first benchmark results.)*

## Regression Registry
*(Empty — no regressions recorded yet.)*

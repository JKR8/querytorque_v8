# Analyst Probe Checklist

## Dispatch
- objective: ``
- n_probes: `5`

## Probes
| probe_id | family | transform_id | confidence | target |
|---|---|---|---:|---|
| `p01` | `A` | `sf_sk_pushdown_union_all` | 0.85 | Push date_sk BETWEEN predicate into both catalog_sales and web_sales branches of the UNION ALL. |
| `p02` | `C` | `aggregate_pushdown` | 0.75 | Pre-aggregate catalog_sales and web_sales by customer_sk before joining with other tables. |
| `p03` | `A` | `date_cte_isolate` | 0.65 | Extract date_dim filtering into a separate CTE to materialize date_sks once. |
| `p04` | `E` | `materialize_cte` | 0.60 | Materialize the filtered item table once. |
| `p05` | `A` | `shared_dimension_multi_channel` | 0.55 | Extract shared dimension filters into a single CTE. |

## Dropped
- count: `2`

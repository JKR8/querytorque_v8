# Query Optimization Benchmark Report Template

**Report Date**: {{YYYY-MM-DD}}  
**Report Author**: {{Name/Team}}  
**Tool Version**: {{Version}}

---

## 1. Setup

**Engines tested**: 
- {{e.g., Postgres 16, DuckDB 1.1.0, "Rbot + Postgres 16"}}

**Benchmark**: 
- {{TPC-DS / DSB / TPC-H / Custom}}

**Scale factor**: 
- {{e.g., 100 GB, 1 TB, SF10, SF100}}

**Hardware / cloud**: 
- vCPUs: {{N}}
- RAM: {{X}} GB
- Instance type: {{type}}
- Storage: {{type}} ({{size}})
- Region: {{location}}

**Concurrency model**: 
- {{single-stream / N concurrent streams}}

**Dataset layout**: 
- Format: {{row/columnar}}
- Indexes: {{Y/N, which ones}}
- Partitioning: {{scheme}}
- Statistics: {{fresh/stale/none}}

**LLM**: 
- Model: {{name, provider}}
- Temperature: {{value}}
- Max tokens: {{N}}
- Context window: {{N}}

**Rewrite policy**: 
- {{e.g., offline training, heuristic + cost-based gate, verified rewrites only}}

---

## 2. Workload Coverage

| Metric | Value |
|--------|-------|
| Total queries in suite | {{N}} |
| Queries considered 'slow' (baseline > X s) | {{N_slow}} |
| Queries eligible for rewrite | {{N_eligible}} |
| Queries actually rewritten | {{N_rewritten}} |
| Queries skipped by policy | {{N_skipped}} |
| Rewrite success rate | {{percent}}% |

---

## 3. Correctness & Safety

| Metric | Value |
|--------|-------|
| Semantic equivalence method | {{e.g., result diffing with tolerances, formal checker, bounded sampling}} |
| Rewrites passing equivalence checks | {{N_eq_pass}} / {{N_rewritten}} |
| Rewrites rejected for semantic mismatch | {{N_eq_fail}} |
| Queries with >20% regression (filtered) | {{N_regress_filtered}} |

---

## 4. Per-Query Speedups (Slow Subset)

**Slow subset definition**: {{definition, e.g., "baseline > 5 s"}}

**Engine baseline**: {{which engine, e.g., DuckDB native / Postgres native}}

| Metric | Value |
|--------|-------|
| Number of slow queries | {{N_slow}} |
| Slow queries with accepted rewrites | {{N_slow_rewritten}} |
| Geometric mean speedup (slow subset) | {{GM_speedup_slow}}× |
| Median (p50) speedup (slow subset) | {{p50_speedup_slow}}× |
| p90 speedup (slow subset) | {{p90_speedup_slow}}× |
| Max speedup observed | {{max_speedup}}× |

---

## 5. Distribution of Effects (All Queries)

**Relative to baseline engine**: {{baseline engine}}

| Performance Band | Count | Percent |
|---|---|---|
| >2× speedup | {{count}} | {{percent}}% |
| 1.5–2× speedup | {{count}} | {{percent}}% |
| 1.1–1.5× speedup | {{count}} | {{percent}}% |
| Within ±10% (no material change) | {{count}} | {{percent}}% |
| 10–20% slowdown | {{count}} | {{percent}}% |
| >20% slowdown (after policy) | {{count}} | {{percent}}% |

---

## 6. End-to-End Workload Impact

| Metric | Baseline | With Rewrite | Change |
|---|---|---|---|
| Total runtime | {{T_baseline}} | {{T_rewrite}} | {{ΔT_absolute}} ({{ΔT_percent}}%) |
| Total cost (if cloud) | {{C_baseline}} | {{C_rewrite}} | {{ΔC_absolute}} ({{ΔC_percent}}%) |
| Queries improved | — | {{N_improved}} | — |
| Queries unchanged | — | {{N_unchanged}} | — |
| Queries rejected (safety) | — | {{N_rejected}} | — |

---

## 7. DSB-Specific Metrics (If Applicable)

Using DSB {{scale factor}} with dynamic workload generator:

| Metric | Value |
|--------|-------|
| Total query instances executed | {{N_instances}} |
| Unique templates touched | {{N_templates}} / {{total_templates}} |
| Instances with accepted rewrites | {{N_instances_rewritten}} |
| Success rate (faster + equivalent) | {{success_percent}}% |
| GM speedup (slow instances, baseline > X s) | {{GM_speedup_DSB_slow}}× |
| End-to-end schedule runtime change | {{ΔT_DSB_percent}}% |

---

## 8. Key Findings

### Wins
- {{Finding 1}}
- {{Finding 2}}
- {{Finding 3}}

### Limitations
- {{Limitation 1}}
- {{Limitation 2}}
- {{Limitation 3}}

### Recommendations
- {{Recommendation 1}}
- {{Recommendation 2}}
- {{Recommendation 3}}

---

## 9. Headline Summary

**On {{benchmark}} at SF{{X}}, our LLM-powered rewrite layer on top of {{engine}} accelerates the slowest {{Y}}% of queries by a geometric mean of {{A}}× (up to {{B}}×), while reducing total workload time by {{C}}% and avoiding >{{D}}% regressions on any accepted rewrite.**

---

## 10. Appendix: Query-Level Details

| Query | Baseline (ms) | Rewritten (ms) | Speedup | Status | Transform |
|-------|---|---|---|---|---|
| {{Q#}} | {{T_base}} | {{T_opt}} | {{speedup}}× | {{pass/fail}} | {{transform}} |
| ... | ... | ... | ... | ... | ... |

---

**Template Version**: 1.0  
**Last Updated**: 2026-02-05


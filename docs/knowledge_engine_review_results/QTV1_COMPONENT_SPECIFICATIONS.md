# QTV1 Knowledge Engine — Component Specifications

> **Purpose**: Exact schemas, populated examples, and injection templates for every data structure in the Knowledge Engine.
> Companion to: QTV1_KNOWLEDGE_ENGINE_ENGINEERING_NOTES_CANON.md

---

## Table of Contents

1. [Layer 1: Blackboard Entry (Outcome)](#1-layer-1-blackboard-entry)
2. [Layer 2: Finding](#2-layer-2-finding)
3. [Layer 3: Pattern](#3-layer-3-pattern)
4. [Layer 4a: Engine Profile](#4-layer-4a-engine-profile)
5. [Layer 4b: Gold Example](#5-layer-4b-gold-example)
6. [KnowledgeResponse: What Phase 2 Receives](#6-knowledgeresponse)
7. [Prompt Injection: How Knowledge Enters the LLM](#7-prompt-injection)
8. [Compression Examples: L1 → L2 → L3 → L4](#8-compression-walkthrough)

---

## 1. Layer 1: Blackboard Entry

**What it is**: One row of JSONL. Every optimization attempt, scanner observation, or config experiment produces exactly one blackboard entry.

**File**: `data/layer1/{engine}_{benchmark}/{date}/outcomes.jsonl`

**One line = one entry. Each entry is self-contained.**

### 1.1 Example: 4W Worker Win (DuckDB)

```json
{
  "id": "q88",
  "source": {
    "type": "4w_worker",
    "scanner_config": null
  },
  "base": {
    "query_id": "q88",
    "engine": "duckdb",
    "benchmark": "tpcds",
    "original_sql": "SELECT s_store_name, s_store_id, SUM(CASE WHEN (d_day_name='Sunday') THEN ss_sales_price ELSE null END) sun_sales, SUM(CASE WHEN (d_day_name='Monday') THEN ss_sales_price ELSE null END) mon_sales, ... FROM store_sales, date_dim, store WHERE d_date_sk = ss_sold_date_sk AND s_store_sk = ss_store_sk AND s_gmt_offset = -5.00 AND d_year = 2001 GROUP BY s_store_name, s_store_id ORDER BY s_store_name, s_store_id, sun_sales, mon_sales LIMIT 100",
    "fingerprint": "correlated_subquery_time_buckets_star_schema",
    "timestamp": "2026-02-11T10:05:22Z",
    "run_id": "swarm_batch_20260211_100500"
  },
  "opt": {
    "approach": "4w_worker",
    "worker_id": 1,
    "strategy": "aggressive_single_pass_restructure",
    "iteration": 0,
    "optimized_sql": "WITH date_filter AS (SELECT d_date_sk, d_day_name FROM date_dim WHERE d_year = 2001), store_filter AS (SELECT s_store_sk, s_store_name, s_store_id FROM store WHERE s_gmt_offset = -5.00) SELECT sf.s_store_name, sf.s_store_id, SUM(CASE WHEN df.d_day_name='Sunday' THEN ss.ss_sales_price END) sun_sales, ... FROM store_sales ss JOIN date_filter df ON df.d_date_sk = ss.ss_sold_date_sk JOIN store_filter sf ON sf.s_store_sk = ss.ss_store_sk GROUP BY sf.s_store_name, sf.s_store_id ORDER BY 1, 2 LIMIT 100",
    "examples_used": ["q6_date_cte", "q9_single_pass"],
    "engine_profile_version": "2026.02.11-v3"
  },
  "principle": {
    "what": "Isolated dimension filters into CTEs, converted implicit joins to explicit joins",
    "why": "DuckDB cannot push predicates through implicit comma-join syntax as effectively as through explicit JOIN ON; CTEs allow the optimizer to filter dimensions before the fact table scan",
    "mechanism": "dimension_cte_isolate + explicit_join",
    "transform_type": "date_cte_isolate",
    "gap_exploited": "IMPLICIT_JOIN_PUSHDOWN",
    "supporting_evidence": "EXPLAIN: Seq Scan on store_sales reduced from 28.7M to 4.2M rows after dimension pre-filter",
    "confidence": "high"
  },
  "semantics": {
    "business_intent": "Weekly sales breakdown by store for fiscal year 2001",
    "tables_accessed": ["store_sales", "date_dim", "store"],
    "join_pattern": "star_schema",
    "aggregation_type": "conditional_sum",
    "filter_selectivity": 0.15,
    "query_archetype": "star_schema_conditional_aggregation"
  },
  "config": {
    "settings": {},
    "reasoning": null,
    "impact_additive": null,
    "impact_combined": null
  },
  "scanner_finding": null,
  "outcome": {
    "status": "WIN",
    "speedup": 3.41,
    "speedup_type": "measured",
    "timing": {
      "original_ms": 2456.0,
      "optimized_ms": 720.0
    },
    "validation": {
      "confidence": "high",
      "rows_match": true,
      "checksum_match": true
    },
    "error": null
  },
  "transforms": {
    "primary": "date_cte_isolate",
    "all": ["date_cte_isolate", "dimension_cte_isolate", "pushdown"]
  },
  "principles": {
    "what_worked": "Dimension CTE isolation + explicit joins",
    "why_it_worked": "Allowed DuckDB to pre-filter dimensions before scanning fact table",
    "principle_id": "IMPLICIT_JOIN_PUSHDOWN"
  },
  "error": null,
  "reasons": {
    "reasoning_chain": "The query uses implicit comma-join syntax which limits pushdown opportunity. By isolating date_dim and store into CTEs with their filters, and converting to explicit JOIN ON syntax, DuckDB can apply hash join with pre-filtered build sides.",
    "evidence": "EXPLAIN ANALYZE shows 28.7M → 4.2M rows scanned on store_sales"
  },
  "tags": [
    "date_cte_isolate",
    "star_schema",
    "conditional_aggregation",
    "high_impact",
    "dimension_prefetch"
  ],
  "provenance": {
    "model": "deepseek-reasoner",
    "provider": "deepseek",
    "git_sha": "a1b2c3d",
    "reviewed": false,
    "knowledge_version_used": "2026.02.11-v3"
  },
  "version": {
    "schema_version": "2.0",
    "entry_version": 1,
    "superseded_by": null,
    "status": "active"
  }
}
```

### 1.2 Example: Scanner Observation (PostgreSQL)

```json
{
  "id": "q85_scanner",
  "source": {
    "type": "plan_scanner",
    "scanner_config": {
      "enable_nestloop": "off"
    }
  },
  "base": {
    "query_id": "q85",
    "engine": "postgresql",
    "benchmark": "tpcds",
    "original_sql": "SELECT ... FROM web_sales ws JOIN web_returns wr ON ws.ws_order_number = wr.wr_order_number ...",
    "fingerprint": "star_schema_web_returns_dimension_heavy",
    "timestamp": "2026-02-10T08:30:00Z",
    "run_id": "scanner_batch_20260210_083000"
  },
  "opt": {
    "approach": "plan_scanner",
    "worker_id": null,
    "strategy": "config_exploration",
    "iteration": 0,
    "optimized_sql": null,
    "examples_used": [],
    "engine_profile_version": "2026.02.10-v2"
  },
  "principle": {
    "what": "Disabling nested loops caused >4x regression on dimension-heavy star query",
    "why": "PostgreSQL's nested loop index lookups on dimension PKs are critical for this join topology; hash join on small dimension tables is catastrophically slower",
    "mechanism": "join_method_sensitivity",
    "transform_type": "config_experiment",
    "gap_exploited": null,
    "supporting_evidence": "SET LOCAL enable_nestloop = off → plan cost 4523 → 18920, time 1240ms → 5210ms",
    "confidence": "high"
  },
  "semantics": {
    "business_intent": "Web returns analysis with customer demographics",
    "tables_accessed": ["web_sales", "web_returns", "date_dim", "customer_demographics", "customer_address"],
    "join_pattern": "star_schema",
    "aggregation_type": "count_groupby",
    "filter_selectivity": 0.08,
    "query_archetype": "star_schema_web_returns"
  },
  "config": {
    "settings": {
      "enable_nestloop": "off"
    },
    "reasoning": {
      "trigger": "Testing optimizer behavior with nested loops disabled",
      "rationale": "Exploring whether hash join performs better for dimension-heavy joins",
      "expected_benefit": "Potentially better for large dimension joins",
      "risk_assessment": "Medium - may regress on PK lookups"
    },
    "impact_additive": 0.24,
    "impact_combined": null
  },
  "scanner_finding": {
    "claim": "Disabling nested loops causes >4x regression on dim-heavy star queries",
    "category": "join_sensitivity",
    "setting_tested": {"enable_nestloop": "off"},
    "baseline_plan_cost": 4523.0,
    "modified_plan_cost": 18920.0,
    "baseline_time_ms": 1240.0,
    "modified_time_ms": 5210.0,
    "implication": "Do NOT restructure joins that eliminate nested loop index lookups on dimension PKs",
    "boundaries": [
      "Applies when baseline uses nested loops for dimension PK lookups",
      "Query has 3+ dimension tables with PK filters"
    ],
    "applicable_queries": ["q085", "q091", "q065"]
  },
  "outcome": {
    "status": "REGRESSION",
    "speedup": 0.24,
    "speedup_type": "measured",
    "timing": {
      "original_ms": 1240.0,
      "optimized_ms": 5210.0
    },
    "validation": {
      "confidence": "row_count_only",
      "rows_match": true,
      "checksum_match": null
    },
    "error": null
  },
  "transforms": {
    "primary": "config_experiment",
    "all": []
  },
  "principles": {
    "what_worked": null,
    "why_it_worked": null,
    "principle_id": null
  },
  "error": null,
  "reasons": {
    "reasoning_chain": "Scanner disabled enable_nestloop to test hash join performance on star schema with 5 dimension tables",
    "evidence": "EXPLAIN ANALYZE: Nested Loop (baseline) 1240ms vs Hash Join (modified) 5210ms on dimension lookups"
  },
  "tags": [
    "join_sensitivity",
    "nested_loop",
    "star_schema",
    "config_experiment",
    "regression",
    "scanner"
  ],
  "provenance": {
    "model": "plan_scanner",
    "provider": "system",
    "git_sha": "a1b2c3d",
    "reviewed": false,
    "knowledge_version_used": "2026.02.10-v2"
  },
  "version": {
    "schema_version": "2.0",
    "entry_version": 1,
    "superseded_by": null,
    "status": "active"
  }
}
```

### 1.3 Example: Config-Only Speedup (PostgreSQL SET LOCAL)

```json
{
  "id": "q93_config",
  "source": {
    "type": "plan_scanner",
    "scanner_config": {
      "work_mem": "1GB"
    }
  },
  "base": {
    "query_id": "q93",
    "engine": "postgresql",
    "benchmark": "tpcds",
    "original_sql": "SELECT ss_customer_sk, SUM(act_sales) ... FROM (SELECT ... ) t GROUP BY ss_customer_sk ORDER BY ... LIMIT 100",
    "fingerprint": "nested_aggregation_with_sort",
    "timestamp": "2026-02-10T09:15:00Z",
    "run_id": "scanner_batch_20260210_083000"
  },
  "opt": {
    "approach": "plan_scanner",
    "worker_id": null,
    "strategy": "config_tuning",
    "iteration": 0,
    "optimized_sql": null,
    "examples_used": [],
    "engine_profile_version": "2026.02.10-v2"
  },
  "principle": {
    "what": "Increasing work_mem to 1GB eliminated sort spill to disk",
    "why": "Default work_mem (4MB) forced external merge sort; 1GB keeps sort in RAM for this aggregation size",
    "mechanism": "memory_tuning",
    "transform_type": "config_tuning",
    "gap_exploited": "SORT_SPILL_TO_DISK",
    "supporting_evidence": "EXPLAIN: Sort Method changed from 'external merge Disk' to 'quicksort Memory'",
    "confidence": "high"
  },
  "semantics": {
    "business_intent": "Top customers by returns-adjusted sales",
    "tables_accessed": ["store_sales", "store_returns"],
    "join_pattern": "self_join",
    "aggregation_type": "nested_sum_topn",
    "filter_selectivity": 0.45,
    "query_archetype": "nested_aggregation_topn"
  },
  "config": {
    "settings": {
      "work_mem": "1GB"
    },
    "reasoning": {
      "trigger": "EXPLAIN showed Sort Method: external merge Disk: 892MB",
      "rationale": "Sort spilling to temp files; increase work_mem to keep entire sort in RAM",
      "expected_benefit": "Eliminate temp file I/O for sort operation",
      "risk_assessment": "Low - work_mem is per-operation, only applies to this query via SET LOCAL"
    },
    "impact_additive": 6.82,
    "impact_combined": null
  },
  "scanner_finding": null,
  "outcome": {
    "status": "WIN",
    "speedup": 6.82,
    "speedup_type": "measured",
    "timing": {
      "original_ms": 8940.0,
      "optimized_ms": 1311.0
    },
    "validation": {
      "confidence": "row_count_only",
      "rows_match": true,
      "checksum_match": null
    },
    "error": null
  },
  "transforms": {
    "primary": "config_tuning",
    "all": []
  },
  "principles": {
    "what_worked": "SET LOCAL work_mem = '1GB'",
    "why_it_worked": "Eliminated external merge sort disk spill",
    "principle_id": "SORT_SPILL_TO_DISK"
  },
  "error": null,
  "reasons": {
    "reasoning_chain": "Scanner detected Sort Method: external merge Disk in EXPLAIN output. work_mem increase to 1GB resolved spill.",
    "evidence": "Sort Method: external merge Disk → quicksort Memory. 8940ms → 1311ms."
  },
  "tags": [
    "config_tuning",
    "work_mem",
    "sort_spill",
    "high_impact",
    "set_local"
  ],
  "provenance": {
    "model": "plan_scanner",
    "provider": "system",
    "git_sha": "a1b2c3d",
    "reviewed": false,
    "knowledge_version_used": "2026.02.10-v2"
  },
  "version": {
    "schema_version": "2.0",
    "entry_version": 1,
    "superseded_by": null,
    "status": "active"
  }
}
```

### 1.4 Schema Rules

- **One entry per optimization attempt** — 5 workers × 99 queries = 495 entries per benchmark run
- **Scanner entries** have `source.type = "plan_scanner"`, `opt.worker_id = null`, `opt.optimized_sql = null` (they test config, not SQL rewrites)
- **Config-only speedups** have `source.type = "plan_scanner"`, `outcome.status = "WIN"`, and `config.settings` populated
- **Failures must be recorded** — `outcome.status = "REGRESSION" | "ERROR" | "FAIL"` entries are critical for learning what_didnt_work
- **`provenance.knowledge_version_used`** — records exactly which L4 knowledge version was active when this outcome was produced

---

## 2. Layer 2: Finding

**What it is**: A claim about engine behavior extracted by DeepSeek R1 from a batch of ~400 Layer 1 entries. Findings are observations with boundary conditions — they are NOT recommendations.

**File**: `data/layer2/findings/{engine}/{category}/{finding_id}.json`

**Key distinction**: A finding says "we observed X under conditions Y." It does not say "you should do X." The engine profile reasoning step makes that judgment.

### 2.1 Example: 4W-Derived Finding (DuckDB)

```json
{
  "schema_version": "2.0",
  "id": "F-DUCK-042",
  "category": "scan_method",
  "claim": "DuckDB fails to push predicates through implicit comma-join syntax, resulting in full fact table scans that explicit JOIN ON syntax avoids",
  "evidence": {
    "summary": "Across 12 star-schema queries using comma-join syntax, converting to explicit JOIN ON with dimension CTE isolation reduced fact table scan rows by 70-85%. Average speedup 2.8x. No regressions observed.",
    "count": 12,
    "contradicting": 0,
    "supporting_queries": ["q6", "q15", "q27", "q39", "q88", "q92", "q45", "q73", "q79", "q85", "q90", "q93"]
  },
  "mechanism": "DuckDB's optimizer handles predicate pushdown through explicit JOIN ON clauses but does not propagate filter conditions across implicit comma-separated FROM clauses. Isolating dimension filters into CTEs and using explicit JOIN ON syntax allows the optimizer to build hash tables on pre-filtered dimensions before scanning the fact table.",
  "boundaries": {
    "applies_when": "Query uses implicit comma-join syntax with dimension table filters in WHERE clause; star schema topology with 2+ dimension tables",
    "does_not_apply_when": "Query already uses explicit JOIN ON syntax; single-table query; dimension tables have no selective filters",
    "boundary_conditions": [
      "Star schema with fact + dimension topology",
      "Implicit comma-join in FROM clause",
      "Dimension filters present in WHERE (not just join conditions)",
      "Dimension selectivity < 0.3 (filters meaningful, not selecting most rows)"
    ]
  },
  "confidence": "high",
  "confidence_rationale": "12/12 queries showed improvement, 0 contradictions, consistent mechanism across all observations",
  "implication": "When observing implicit comma-join syntax on star schemas with dimension filters, isolating dimensions into CTEs with explicit JOIN ON is a reliable optimization path for DuckDB",
  "engine_specific": {
    "engine": "duckdb",
    "version_tested": "0.10.x",
    "set_local_relevant": false,
    "relevant_configs": []
  },
  "metadata": {
    "extracted_at": "2026-02-11T12:00:00Z",
    "source_batch": "layer1/duckdb_tpcds/2026-02-11/outcomes.jsonl",
    "source_entry_count": 412,
    "blackboard_hash": "sha256:abc123...",
    "extraction_model": "deepseek-r1",
    "reviewed": false,
    "reviewed_at": null,
    "reviewed_by": null
  }
}
```

### 2.2 Example: Scanner-Derived Finding (PostgreSQL)

```json
{
  "schema_version": "2.0",
  "id": "SF-017",
  "category": "join_sensitivity",
  "claim": "PostgreSQL nested loop index lookups on dimension PKs are 3-5x faster than hash join for star schemas with 3+ dimension tables and selective PK filters; disabling nested loops causes severe regression",
  "evidence": {
    "summary": "Across 8 star-schema queries, SET LOCAL enable_nestloop=off caused 3.2-5.1x regression. All queries had 3+ dimension tables with PK equality filters. Hash join builds full hash tables on dimension tables unnecessarily when index lookup returns 1-10 rows per probe.",
    "count": 8,
    "contradicting": 1,
    "supporting_queries": ["q85", "q91", "q65", "q72", "q80", "q82", "q94", "q95"]
  },
  "mechanism": "For dimension PK lookups (WHERE dim.pk = fact.fk), PostgreSQL uses nested loop + index scan which probes the dimension index once per fact row. Hash join must first build a full hash table of the dimension, then probe — this is wasteful when the dimension filter is highly selective (returning <100 rows). The nested loop + index path handles selectivity naturally.",
  "boundaries": {
    "applies_when": "Star schema with 3+ dimension tables; dimension join is on PK/indexed column; dimension filter selectivity < 0.1",
    "does_not_apply_when": "Dimension tables are large (>1M rows) and join is not on indexed PK; fact table is small (<10K rows); only 1-2 dimension joins",
    "boundary_conditions": [
      "Star schema topology",
      "Dimension join on PK or indexed column",
      "3+ dimension tables in query",
      "Dimension filter reduces to < 100 rows",
      "Fact table is >100K rows"
    ]
  },
  "confidence": "high",
  "confidence_rationale": "8/9 observations consistent (1 contradicting: q48 with 2 dimensions showed no regression, consistent with '3+ dimensions' boundary)",
  "implication": "Do NOT restructure joins that eliminate nested loop index lookups on dimension PKs. Workers should preserve the join topology that allows nested loop access on small dimension tables.",
  "engine_specific": {
    "engine": "postgresql",
    "version_tested": "16.x",
    "set_local_relevant": true,
    "relevant_configs": ["enable_nestloop"]
  },
  "metadata": {
    "extracted_at": "2026-02-10T14:00:00Z",
    "source_batch": "layer1/postgresql_tpcds/2026-02-10/outcomes.jsonl",
    "source_entry_count": 396,
    "blackboard_hash": "sha256:def456...",
    "extraction_model": "deepseek-r1",
    "reviewed": true,
    "reviewed_at": "2026-02-10T16:30:00Z",
    "reviewed_by": "human"
  }
}
```

### 2.3 What Makes a Good Finding

A finding is only useful if:
1. **The claim is falsifiable** — "X happens under conditions Y" can be tested
2. **Boundary conditions are specific** — not "sometimes helps" but "helps when star schema with 3+ dim tables and selectivity < 0.1"
3. **Evidence count > 1** — a single observation is noise, not a finding
4. **Contradictions are explained** — the 1 contradiction in SF-017 is explained by the boundary condition (2 dims vs 3+)
5. **The mechanism explains WHY** — not just "it was faster" but "nested loop avoids building unnecessary hash tables"

### 2.4 DeepSeek R1 Extraction Prompt Template

```
You are analyzing {entry_count} optimization outcomes for {engine} on {benchmark}.

Each entry contains:
- The original SQL and what optimization was attempted
- Whether it succeeded or failed, and by how much
- What transform/config was applied and why
- The query archetype and conditions (table sizes, join patterns, selectivity)

Your task: Extract FINDINGS — observations about {engine}'s optimizer behavior.

A finding must have:
1. A falsifiable claim about engine behavior
2. Specific boundary conditions (when it applies, when it doesn't)
3. A mechanism explaining WHY
4. Evidence count (how many entries support this claim)
5. Any contradictions and why they occurred (different conditions)

Do NOT make recommendations. State what you observed.
Do NOT generalize beyond the evidence. If you saw it on star schemas, say "star schemas", not "all queries."

Output as JSON array of findings matching this schema: {scanner_finding_schema}

Entries:
{jsonl_batch}
```

---

## 3. Layer 3: Pattern

**What it is**: An aggregated, cross-query optimization technique distilled from multiple findings. Patterns have statistics, applicability rules, and counter-indications.

**File**: `data/layer3/patterns/{engine}/{mechanism}/{pattern_id}.json`

**Key distinction from findings**: A finding says "we observed X." A pattern says "technique X works Y% of the time across N queries, under conditions Z, but NOT when W."

### 3.1 Example: Promoted Pattern (DuckDB)

```json
{
  "schema_version": "2.0",
  "id": "PATTERN-DIM-CTE-001",
  "name": "Dimension CTE Isolation for Star Schema",
  "classification": {
    "mechanism": "predicate_pushdown",
    "impact_tier": "high",
    "pattern": "dimension_isolation",
    "risk": "safe",
    "exploit_type": "gap_exploit"
  },
  "technique": {
    "description": "Extract dimension table filters from WHERE clause into CTEs, convert implicit comma-joins to explicit JOIN ON. This gives DuckDB's optimizer the structure it needs to pre-filter dimensions before scanning the fact table.",
    "sql_template": "-- BEFORE:\nSELECT ... FROM fact_table, dim1, dim2\nWHERE dim1.pk = fact_table.fk1 AND dim2.pk = fact_table.fk2\n  AND dim1.filter_col = 'value' AND dim2.year = 2001\n\n-- AFTER:\nWITH filtered_dim1 AS (\n  SELECT pk, ... FROM dim1 WHERE filter_col = 'value'\n),\nfiltered_dim2 AS (\n  SELECT pk, ... FROM dim2 WHERE year = 2001\n)\nSELECT ... FROM fact_table\n  JOIN filtered_dim1 ON filtered_dim1.pk = fact_table.fk1\n  JOIN filtered_dim2 ON filtered_dim2.pk = fact_table.fk2",
    "before_example": "SELECT ... FROM store_sales, date_dim, store WHERE d_date_sk = ss_sold_date_sk AND s_store_sk = ss_store_sk AND s_gmt_offset = -5.00 AND d_year = 2001 ...",
    "after_example": "WITH date_filter AS (SELECT d_date_sk, d_day_name FROM date_dim WHERE d_year = 2001), store_filter AS (SELECT s_store_sk, s_store_name FROM store WHERE s_gmt_offset = -5.00) SELECT ... FROM store_sales ss JOIN date_filter df ON df.d_date_sk = ss.ss_sold_date_sk JOIN store_filter sf ON sf.s_store_sk = ss.ss_store_sk ..."
  },
  "stats": {
    "n_observations": 24,
    "n_wins": 21,
    "success_rate": 0.875,
    "avg_speedup": 2.67,
    "speedup_range": [1.45, 6.28],
    "n_queries": 12
  },
  "applicability": {
    "query_archetypes": [
      "star_schema_conditional_aggregation",
      "star_schema_groupby",
      "star_schema_topn",
      "star_schema_web_returns"
    ],
    "required_features": [
      "Implicit comma-join syntax (FROM a, b, c)",
      "Dimension table filters in WHERE clause",
      "Star schema topology (fact + 2 or more dimensions)"
    ],
    "sql_patterns": [
      "FROM\\s+\\w+\\s*,\\s*\\w+.*WHERE.*=.*_sk",
      "FROM\\s+\\w+_sales.*,.*_dim"
    ],
    "engines": ["duckdb"]
  },
  "counter_indications": [
    {
      "pattern": "Query already uses explicit JOIN ON syntax",
      "reason": "No benefit — optimizer already has the structure it needs",
      "observed_regression": null,
      "example_queries": []
    },
    {
      "pattern": "Dimension filter selectivity > 0.5 (most rows pass)",
      "reason": "CTE overhead exceeds benefit when dimension filter isn't selective",
      "observed_regression": 0.92,
      "example_queries": ["q48"]
    },
    {
      "pattern": "Single dimension table only",
      "reason": "Marginal benefit doesn't justify restructure; optimizer handles single-dim pushdown adequately",
      "observed_regression": null,
      "example_queries": ["q96"]
    }
  ],
  "related_patterns": ["PATTERN-DATE-CTE-001", "PATTERN-PUSH-001"],
  "contradictory_patterns": [],
  "source_findings": ["F-DUCK-042", "F-DUCK-038", "F-DUCK-051"],
  "example_queries": {
    "positive": ["q6", "q15", "q27", "q39", "q88", "q92", "q45", "q73", "q79", "q85", "q90", "q93"],
    "negative": ["q48", "q96"]
  },
  "mechanism": "DuckDB's optimizer handles predicate pushdown through explicit JOIN ON but not through implicit comma-joins. CTE isolation gives it the structure needed to build hash tables on pre-filtered dimensions.",
  "last_validated": "2026-02-11",
  "status": "promoted",
  "version": 1
}
```

### 3.2 Promotion Criteria

A pattern moves from `candidate` → `promoted` when:
- `n_wins >= 5`
- `success_rate >= 0.70`
- `n_queries >= 3` (works across multiple queries, not a one-off)
- Counter-indications documented (failures explained)

A pattern moves to `deprecated` when:
- Success rate drops below 0.50 after new observations
- DB engine version update makes it obsolete
- Superseded by a more general pattern

---

## 4. Layer 4a: Engine Profile

**What it is**: The single document that tells the LLM everything it needs to know about a database engine's optimizer — what it does well (don't fight it), what it does poorly (exploit it), and how to tune it (config knobs).

**File**: `data/layer4/engine_profiles/{engine}.json`

**Design principle**: The engine profile must be ACTIONABLE. Every entry answers the question "what should the worker DO differently because of this knowledge?" An entry that is merely informational with no action is wasted tokens.

### 4.1 Full Example: DuckDB Engine Profile

```json
{
  "schema_version": "2.0",
  "engine": "duckdb",
  "version_tested": "0.10.x, 1.0.x",
  "profile_type": "engine_profile",
  "last_updated": "2026-02-11T12:00:00Z",

  "briefing_note": "DuckDB is a columnar analytical engine with strong automatic optimization for explicit joins, parallel execution, and window functions. Its main exploitable gaps are around implicit join syntax, correlated subquery handling, and multi-scan patterns on large fact tables. Do NOT fight its automatic filter pushdown or parallel scan — restructure SQL to give the optimizer what it needs.",

  "strengths": [
    {
      "id": "AUTO_FILTER_PUSHDOWN",
      "summary": "Automatically pushes filters through explicit JOINs — do not manually restructure filter placement on explicit join queries",
      "field_note": "If the query already uses JOIN ON syntax, DuckDB handles filter pushdown well. Adding CTEs to 'help' pushdown on explicit joins adds overhead with no benefit. Only intervene on implicit comma-joins.",
      "source_patterns": ["PATTERN-DIM-CTE-001"],
      "source_findings": ["F-DUCK-042"]
    },
    {
      "id": "PARALLEL_HASH_JOIN",
      "summary": "Parallelizes hash joins automatically across all cores — do not split large joins into smaller pieces",
      "field_note": "DuckDB scales hash join linearly with cores. Splitting a large join into UNION ALL of smaller filtered joins is always worse. Let the engine parallelize.",
      "source_patterns": [],
      "source_findings": ["F-DUCK-055"]
    },
    {
      "id": "WINDOW_FUNCTION_OPTIMIZATION",
      "summary": "Efficient window function execution with automatic partition pruning",
      "field_note": "DuckDB handles ROW_NUMBER, RANK, SUM OVER well. Do not rewrite window functions to self-joins or correlated subqueries — this is always a regression.",
      "source_patterns": [],
      "source_findings": ["F-DUCK-061"]
    }
  ],

  "gaps": [
    {
      "id": "IMPLICIT_JOIN_PUSHDOWN",
      "priority": "CRITICAL",
      "what": "DuckDB cannot push predicates through implicit comma-join syntax",
      "why": "The optimizer's predicate pushdown logic operates on the explicit JOIN tree, not on comma-separated FROM with WHERE conditions. Comma-joins produce a cross-product node that blocks pushdown.",
      "opportunity": "Convert implicit comma-joins to explicit JOIN ON with dimension CTEs. Average 2.67x speedup on star schemas.",
      "what_worked": [
        "q88: 8 correlated subqueries → single pass with CASE = 6.28x",
        "q6: date CTE isolation = 2.67x",
        "q15: date + store CTE isolation = 2.1x",
        "q27: dimension prefetch = 3.41x"
      ],
      "what_didnt_work": [
        "q48: CTE isolation on 2-dim query with low selectivity = 0.92x (slight regression)",
        "q96: single dimension, optimizer handled it fine without intervention"
      ],
      "field_notes": [
        "Only apply when: implicit comma-join + 2 or more dimension tables + selective filters",
        "Do NOT apply when: query already uses JOIN ON, or dimension selectivity > 0.5",
        "The key diagnostic: EXPLAIN shows full fact table scan despite dimension filters in WHERE"
      ],
      "source_patterns": ["PATTERN-DIM-CTE-001"],
      "source_findings": ["F-DUCK-042", "F-DUCK-038"]
    },
    {
      "id": "CORRELATED_SUBQUERY_DECORRELATION",
      "priority": "HIGH",
      "what": "DuckDB does not automatically decorrelate subqueries that reference outer columns",
      "why": "The optimizer lacks a general decorrelation pass. Correlated subqueries execute once per outer row (nested loop), which is catastrophic on large tables.",
      "opportunity": "Rewrite correlated subqueries as pre-computed CTEs with JOINs or window functions. 2.81x average on applicable queries.",
      "what_worked": [
        "q1: correlated AVG subquery → CTE with JOIN = 2.81x",
        "q23: correlated EXISTS → semi-join CTE = 1.95x"
      ],
      "what_didnt_work": [
        "q4: decorrelation on small subquery (<1000 rows) added JOIN overhead with no benefit"
      ],
      "field_notes": [
        "Check correlated column cardinality: only decorrelate when outer table > 10K rows",
        "For small correlated subqueries (<1000 row estimate), the nested loop is fine — leave it",
        "Window function rewrite (ROW_NUMBER/SUM OVER) often cleaner than CTE + JOIN"
      ],
      "source_patterns": ["PATTERN-DECORR-001"],
      "source_findings": ["F-DUCK-029", "F-DUCK-033"]
    },
    {
      "id": "REDUNDANT_SCAN_ELIMINATION",
      "priority": "HIGH",
      "what": "DuckDB does not automatically detect multiple scans of the same fact table with different filters",
      "why": "Each subquery or UNION branch that references the same table triggers a separate scan. DuckDB does not merge them.",
      "opportunity": "Consolidate multiple scans into single-pass aggregation with CASE expressions. Up to 6.28x on queries with 4+ scans of the same table.",
      "what_worked": [
        "q88: 8 scans → 1 scan with CASE = 6.28x",
        "q90: 3 scans → 1 scan = 1.84x",
        "q45: 4 scans → 1 scan with OR decomposition = 2.98x"
      ],
      "what_didnt_work": [
        "q95: attempted scan consolidation but different filter predicates required different join topologies, CTE approach was cleaner"
      ],
      "field_notes": [
        "Count the fact table scans in EXPLAIN — if 3+, consolidation is almost always a win",
        "CASE-based consolidation works for conditional aggregation patterns",
        "For UNION-based patterns, OR decomposition may be more appropriate than CASE"
      ],
      "source_patterns": ["PATTERN-SCAN-001"],
      "source_findings": ["F-DUCK-044", "F-DUCK-047"]
    }
  ],

  "tuning_intel": {
    "available": false,
    "mechanism": null,
    "briefing_note": "DuckDB does not support SET LOCAL or runtime configuration knobs that affect query planning. All optimization must be through SQL rewriting.",
    "rules": [],
    "key_findings": [
      "No equivalent to PostgreSQL's SET LOCAL for per-query tuning",
      "PRAGMA settings are session-level, not query-level",
      "All optimization value comes from SQL restructuring, not configuration"
    ]
  },

  "scale_sensitivity_warning": "DuckDB performance characteristics are validated at SF10 and SF100. At SF1, many queries are too fast for optimization to matter. At SF1000+, memory pressure may change which optimizations are beneficial.",

  "metadata": {
    "version": "2026.02.11-v3",
    "source_runs": [
      "swarm_batch_20260208_102033",
      "swarm_batch_20260209_143000",
      "swarm_batch_20260211_100500"
    ],
    "source_finding_count": 18,
    "source_pattern_count": 6,
    "auto_generated": true,
    "human_reviewed": false
  }
}
```

### 4.2 Full Example: PostgreSQL Engine Profile (Partial — Tuning Intel Focus)

```json
{
  "schema_version": "2.0",
  "engine": "postgresql",
  "version_tested": "16.x",
  "profile_type": "engine_profile",
  "last_updated": "2026-02-10T16:30:00Z",

  "briefing_note": "PostgreSQL has a mature cost-based optimizer with strong join algorithm selection. Its main gaps are around CTE optimization fences, correlated subquery handling, and cost estimation for complex CTEs. It supports per-query tuning via SET LOCAL which can produce dramatic speedups (6-8x) on memory and parallelism-sensitive queries. Workers must consider both SQL rewriting AND config tuning as complementary strategies.",

  "strengths": [
    {
      "id": "NESTED_LOOP_DIM_LOOKUP",
      "summary": "Nested loop index lookups on dimension PKs are 3-5x faster than hash join for selective star schemas — do NOT restructure joins that would eliminate this access path",
      "field_note": "When EXPLAIN shows Nested Loop + Index Scan on a dimension PK, this is optimal. Do not convert to hash join or restructure the join order. This strength applies when query has 3+ dimension tables with PK filters.",
      "source_patterns": [],
      "source_findings": ["SF-017"]
    },
    {
      "id": "COST_BASED_JOIN_ORDER",
      "summary": "PostgreSQL's cost-based optimizer selects good join orders for equi-joins — do not manually reorder unless EXPLAIN shows clearly suboptimal choice",
      "field_note": "Manual join reordering is rarely beneficial. Only intervene when EXPLAIN shows the optimizer chose a clearly wrong order (e.g., large table as inner side of nested loop).",
      "source_patterns": [],
      "source_findings": ["SF-022"]
    }
  ],

  "gaps": [
    {
      "id": "CTE_OPTIMIZATION_FENCE",
      "priority": "CRITICAL",
      "what": "CTEs are optimization fences in PostgreSQL — the optimizer cannot push predicates into CTEs or pull them up",
      "why": "PostgreSQL materializes CTEs by default (pre-v12 always, v12+ only for multi-referenced CTEs). This blocks predicate pushdown.",
      "opportunity": "For single-use CTEs: inline as subquery. For multi-use: accept the fence or rewrite as temp table. For DuckDB ports: be aware this is a critical difference.",
      "what_worked": [
        "q9: inlined single-use CTE → subquery = 1.8x",
        "q73: restructured multi-use CTE to avoid materialization = 1.45x"
      ],
      "what_didnt_work": [
        "q23: inlining multi-use CTE duplicated computation = 0.65x regression"
      ],
      "field_notes": [
        "Check CTE usage count: single-use = safe to inline, multi-use = risky",
        "PostgreSQL 12+ only materializes multi-referenced CTEs, but the optimizer still cannot push predicates into them",
        "This is the #1 gotcha when porting DuckDB optimizations to PG — DuckDB inlines CTEs automatically"
      ],
      "source_patterns": ["PATTERN-CTE-FENCE-001"],
      "source_findings": ["F-PG-008", "F-PG-012"]
    }
  ],

  "tuning_intel": {
    "available": true,
    "mechanism": "set_local",
    "briefing_note": "PostgreSQL supports SET LOCAL for per-query configuration within a transaction. These settings revert at transaction end. Key tunable areas: memory allocation (work_mem), parallelism (max_parallel_workers_per_gather), JIT compilation (jit), and planner method selection (enable_nestloop, enable_hashjoin). Config-only speedups can achieve 2-8x independently of SQL rewriting, and are additive with SQL rewrites.",
    "rules": [
      {
        "id": "TUNE-PG-001",
        "trigger": "EXPLAIN shows 'Sort Method: external merge Disk' on any sort operation",
        "config": "SET LOCAL work_mem = '256MB' (start conservative, increase to 1GB if sort > 500MB)",
        "evidence": "q93: 8940ms → 1311ms (6.82x) with work_mem = 1GB. Sort changed from external merge to quicksort.",
        "risk": "LOW"
      },
      {
        "id": "TUNE-PG-002",
        "trigger": "Query has 2+ sequential scans on tables > 1M rows AND server has 4+ cores",
        "config": "SET LOCAL max_parallel_workers_per_gather = 4",
        "evidence": "q72: 12400ms → 4100ms (3.0x) with parallel workers. Sequential scan parallelized across 4 workers.",
        "risk": "LOW"
      },
      {
        "id": "TUNE-PG-003",
        "trigger": "EXPLAIN shows 'JIT: true' AND query execution < 100ms baseline",
        "config": "SET LOCAL jit = off",
        "evidence": "Short queries spend more time in JIT compilation than execution. q96: 145ms → 52ms (2.8x) with jit=off. JIT overhead exceeded query execution time.",
        "risk": "LOW"
      },
      {
        "id": "TUNE-PG-004",
        "trigger": "EXPLAIN shows Hash Join on dimension table with < 1000 rows in build side",
        "config": "Do NOT apply — nested loop is correct here. Only intervene if hash join is on a LARGE build side (> 100K rows) and probe side is small",
        "evidence": "SF-017: Disabling nested loops caused 3-5x regression on dim-heavy star queries",
        "risk": "HIGH"
      }
    ],
    "key_findings": [
      "SET LOCAL is transaction-scoped and safe for per-query tuning",
      "work_mem is the highest-impact single knob (up to 6.8x on sort-heavy queries)",
      "Parallelism helps on sequential scan-heavy queries but has diminishing returns beyond 4 workers",
      "JIT hurts short queries (< 100ms) — always disable for fast queries",
      "Config speedups are ADDITIVE with SQL rewrites — apply both when applicable"
    ]
  },

  "metadata": {
    "version": "2026.02.10-v2",
    "source_runs": [
      "scanner_batch_20260210_083000",
      "swarm_batch_20260210_120000"
    ],
    "source_finding_count": 24,
    "source_pattern_count": 4,
    "auto_generated": true,
    "human_reviewed": true
  }
}
```

### 4.3 Engine Profile Design Principles

1. **Every entry is actionable.** Each strength says "do NOT do X." Each gap says "DO do Y, here's how, here's when, here's when not." Each tuning rule says "when you see TRIGGER, apply CONFIG."

2. **Evidence is mandatory.** No gap entry exists without `what_worked` examples. No tuning rule exists without measured `evidence`. This is the quality gate — an engine profile entry without gold example backing is not valid.

3. **Failures are as valuable as wins.** `what_didnt_work` prevents workers from repeating known regressions. Counter-conditions prevent overgeneralization.

4. **Field notes are operator-level.** They tell the worker exactly what to look for and what to do, in plain language. Not academic descriptions — field-tested tactical guidance.

5. **Strengths prevent wasted effort.** If the optimizer already does something well, workers should not waste tokens trying to help. "Don't fight the optimizer" is itself optimization knowledge.

6. **Human review is protected.** `human_reviewed: true` prevents auto-generation from overwriting human-curated content. Auto-generated entries can only ADD to the profile, not modify human-reviewed entries.

---

## 5. Layer 4b: Gold Example

**What it is**: A proven, validated optimization example used as few-shot context in worker prompts. The four-part explanation (what/why/when/when_not) teaches the LLM the technique, not just the SQL.

**File**: `data/layer4/gold_examples/{engine}/{example_id}.json`

### 5.1 Full Example

```json
{
  "schema_version": "2.0",
  "id": "q6_date_cte",
  "query_id": "q6",
  "dialect": "duckdb",

  "classification": {
    "tags": ["date_cte_isolate", "star_schema", "predicate_pushdown", "dimension_prefetch"],
    "archetype": "star_schema_groupby_topn",
    "transforms": ["date_cte_isolate", "pushdown"],
    "complexity": "moderate"
  },

  "original_sql": "SELECT a.ca_state state, COUNT(*) cnt FROM customer_address a, customer c, store_sales s, date_dim d, item i WHERE a.ca_address_sk = c.c_current_addr_sk AND c.c_customer_sk = s.ss_customer_sk AND s.ss_sold_date_sk = d.d_date_sk AND s.ss_item_sk = i.i_item_sk AND d.d_month_seq IN (SELECT DISTINCT d_month_seq FROM date_dim WHERE d_year = 2001 AND d_moy = 1) AND i.i_current_price > 1.2 * (SELECT AVG(j.i_current_price) FROM item j WHERE j.i_category = i.i_category) GROUP BY a.ca_state HAVING COUNT(*) >= 10 ORDER BY cnt, a.ca_state LIMIT 100",

  "optimized_sql": "WITH target_months AS (SELECT DISTINCT d_month_seq FROM date_dim WHERE d_year = 2001 AND d_moy = 1), category_avg AS (SELECT i_category, AVG(i_current_price) AS avg_price FROM item GROUP BY i_category), filtered_items AS (SELECT i.i_item_sk FROM item i JOIN category_avg ca ON i.i_category = ca.i_category WHERE i.i_current_price > 1.2 * ca.avg_price), filtered_dates AS (SELECT d_date_sk FROM date_dim d JOIN target_months tm ON d.d_month_seq = tm.d_month_seq) SELECT a.ca_state AS state, COUNT(*) AS cnt FROM store_sales s JOIN filtered_dates fd ON s.ss_sold_date_sk = fd.d_date_sk JOIN filtered_items fi ON s.ss_item_sk = fi.i_item_sk JOIN customer c ON s.ss_customer_sk = c.c_customer_sk JOIN customer_address a ON c.c_current_addr_sk = a.ca_address_sk GROUP BY a.ca_state HAVING COUNT(*) >= 10 ORDER BY cnt, a.ca_state LIMIT 100",

  "explanation": {
    "what": "Isolated the correlated subquery on item price into a pre-computed CTE (category_avg), isolated the date filter into a CTE (filtered_dates), and converted implicit comma-joins to explicit JOIN ON.",
    "why": "The original query has a correlated subquery (i_current_price > 1.2 * AVG(j.i_current_price) WHERE j.i_category = i.i_category) that executes once per item row. Pre-computing category averages eliminates the correlated scan. Isolating date_dim and item into CTEs allows DuckDB to build small hash tables before scanning the large store_sales fact table.",
    "when": "Apply when: (1) query has correlated subquery with aggregate on same table, (2) implicit comma-joins with dimension filters, (3) star schema with 3+ dimension tables. The key diagnostic is EXPLAIN showing repeated scans or nested loops on dimension lookups.",
    "when_not": "Do NOT apply when: (1) query already uses explicit JOIN ON syntax, (2) dimension tables have low selectivity (most rows pass filters), (3) correlated subquery operates on < 1000 rows (overhead exceeds benefit)."
  },

  "outcome": {
    "speedup": 2.67,
    "original_ms": 1890.0,
    "optimized_ms": 708.0,
    "validated_at_sf": 10,
    "validation_confidence": "high",
    "rows_match": true,
    "checksum_match": true
  },

  "provenance": {
    "source_run": "swarm_batch_20260208_102033",
    "worker_id": 2,
    "model": "deepseek-reasoner",
    "promoted_at": "2026-02-09T10:00:00Z",
    "promoted_by": "auto",
    "reviewed_by": "human"
  },

  "status": "active",
  "superseded_by": null,
  "usage_count": 47,
  "last_used": "2026-02-11T10:05:22Z"
}
```

### 5.2 The Four-Part Explanation

This is the most important field in the gold example. The SQL before/after teaches by example. The explanation teaches the TECHNIQUE:

| Field | Purpose | Failure Mode if Missing |
|-------|---------|------------------------|
| `what` | Names the transforms applied | Worker can copy SQL but can't generalize to new queries |
| `why` | Explains the performance mechanism | Worker applies transform blindly without understanding cost model |
| `when` | Specifies conditions for application | Worker overgeneralizes, applies to wrong query shapes |
| `when_not` | Specifies counter-indications | Worker causes regressions on edge cases |

All four parts are required for a gold example to be useful. An example with only `what` teaches nothing — the LLM needs `why` to reason about new queries, `when` to match applicability, and `when_not` to avoid regressions.

---

## 6. KnowledgeResponse

**What it is**: The exact payload that Interface A returns to Pipeline Phase 2. This is the sole input to prompt generation from the knowledge system.

### 6.1 Full Example Response

```json
{
  "matched_examples": [
    {
      "id": "q6_date_cte",
      "query_id": "q6",
      "relevance_score": 0.92,
      "match_reason": "star_schema + implicit_comma_join + date_dimension_filter",
      "original_sql": "SELECT a.ca_state ... FROM customer_address a, customer c, store_sales s, date_dim d ...",
      "optimized_sql": "WITH target_months AS (...), filtered_dates AS (...) SELECT ... JOIN ...",
      "explanation": {
        "what": "Isolated correlated subquery + dimension CTE isolation + explicit joins",
        "why": "Pre-compute category averages, pre-filter dimensions, give optimizer explicit join structure",
        "when": "Correlated subquery + implicit comma-joins + 3+ dimension tables",
        "when_not": "Already explicit joins, low dimension selectivity, small correlated subquery"
      },
      "speedup": 2.67,
      "transforms": ["date_cte_isolate", "decorrelate", "pushdown"]
    },
    {
      "id": "q9_single_pass",
      "query_id": "q9",
      "relevance_score": 0.78,
      "match_reason": "multiple_fact_scans + conditional_aggregation",
      "original_sql": "...",
      "optimized_sql": "...",
      "explanation": { "what": "...", "why": "...", "when": "...", "when_not": "..." },
      "speedup": 1.84,
      "transforms": ["single_pass_aggregation"]
    }
  ],

  "engine_profile": {
    "engine": "duckdb",
    "briefing_note": "DuckDB is a columnar analytical engine with strong automatic optimization for explicit joins...",
    "relevant_strengths": [
      {
        "id": "AUTO_FILTER_PUSHDOWN",
        "summary": "Automatically pushes filters through explicit JOINs — do not manually restructure filter placement on explicit join queries",
        "field_note": "If the query already uses JOIN ON syntax, DuckDB handles filter pushdown well."
      }
    ],
    "relevant_gaps": [
      {
        "id": "IMPLICIT_JOIN_PUSHDOWN",
        "priority": "CRITICAL",
        "what": "DuckDB cannot push predicates through implicit comma-join syntax",
        "opportunity": "Convert implicit comma-joins to explicit JOIN ON with dimension CTEs. Average 2.67x speedup.",
        "field_notes": ["Only apply when: implicit comma-join + 2 or more dimension tables + selective filters"],
        "what_worked": ["q88: 6.28x", "q6: 2.67x", "q15: 2.1x"],
        "what_didnt_work": ["q48: 0.92x (low selectivity)"]
      },
      {
        "id": "CORRELATED_SUBQUERY_DECORRELATION",
        "priority": "HIGH",
        "what": "DuckDB does not automatically decorrelate subqueries that reference outer columns",
        "opportunity": "Rewrite as pre-computed CTEs with JOINs or window functions. 2.81x average.",
        "field_notes": ["Only decorrelate when outer table > 10K rows"],
        "what_worked": ["q1: 2.81x"],
        "what_didnt_work": ["q4: small subquery, no benefit"]
      }
    ],
    "tuning_intel": {
      "available": false,
      "briefing_note": "DuckDB does not support per-query tuning. All optimization through SQL rewriting."
    }
  },

  "constraints": [
    {
      "type": "semantic",
      "rule": "Preserve HAVING COUNT(*) >= 10 filter — this is business logic, not an optimization target"
    },
    {
      "type": "correctness",
      "rule": "LIMIT must remain after ORDER BY — removing LIMIT changes result cardinality"
    }
  ],

  "scanner_findings": null,

  "knowledge_version": "2026.02.11-v3"
}
```

### 6.2 Relevance Filtering

The full engine profile may have 10+ gaps and 5+ strengths. NOT all of them are relevant to every query. Interface A performs relevance filtering:

1. **Match examples by**: SQL fingerprint similarity, tag overlap, archetype match, transform applicability
2. **Filter profile gaps by**: Does this query exhibit the conditions described in `field_notes`? Is the gap relevant to this query's structure?
3. **Filter profile strengths by**: Does this query use patterns the optimizer handles well? (If so, include the strength to prevent workers from "helping" the optimizer unnecessarily)
4. **Include tuning intel ONLY for PG** (DuckDB has none)
5. **Include scanner findings ONLY for PG** (scanner is PG-only)

The goal: send workers only the knowledge that is relevant to THIS query. An irrelevant gap wastes tokens and confuses the model. A missing relevant gap misses an optimization opportunity.

---

## 7. Prompt Injection: How Knowledge Enters the LLM

### 7.1 Analyst Briefing (Phase 3)

The analyst receives the full KnowledgeResponse and uses it to assign strategies and examples to workers. The analyst prompt includes:

```
=== INTELLIGENCE BRIEFING ===

ENGINE PROFILE: {engine} ({version_tested})
{briefing_note}

STRENGTHS (do NOT fight these):
{for each relevant_strength:}
- {id}: {summary}
  NOTE: {field_note}

EXPLOITABLE GAPS (target these):
{for each relevant_gap:}
- [{priority}] {id}: {what}
  OPPORTUNITY: {opportunity}
  WHAT WORKED: {what_worked joined}
  WHAT DIDN'T WORK: {what_didnt_work joined}
  GUIDANCE: {field_notes joined}

{if tuning_intel.available:}
TUNING OPPORTUNITIES (SET LOCAL):
{briefing_note}
{for each applicable rule:}
- TRIGGER: {trigger}
  CONFIG: {config}
  EVIDENCE: {evidence}
  RISK: {risk}

{if scanner_findings:}
SCANNER INTELLIGENCE:
{scanner findings formatted}

CONSTRAINTS:
{for each constraint:}
- [{type}] {rule}

=== MATCHED EXAMPLES ===
{for each matched_example:}
--- Example: {id} ({speedup}x speedup) ---
Match reason: {match_reason}
WHAT: {explanation.what}
WHY: {explanation.why}
WHEN TO APPLY: {explanation.when}
WHEN NOT TO APPLY: {explanation.when_not}
Transforms: {transforms joined}
```

### 7.2 Worker Prompt (Phase 3)

Each worker receives a SUBSET of knowledge based on their assigned strategy:

```
=== YOUR ASSIGNMENT ===
Strategy: {strategy}
Examples to learn from: {assigned_example_ids}

=== RELEVANT ENGINE KNOWLEDGE ===
{only the gaps/strengths relevant to this worker's strategy}

=== EXAMPLE: {example_id} ===
BEFORE:
{original_sql}

AFTER:
{optimized_sql}

WHAT was done: {explanation.what}
WHY it works: {explanation.why}
WHEN to apply: {explanation.when}
WHEN NOT to apply: {explanation.when_not}

{if PG and tuning applicable:}
=== CONFIG TUNING ===
After optimizing the SQL, also consider:
{applicable tuning rules}
```

### 7.3 Token Budget Interaction

Knowledge injection must fit within the token budget:

| Section | Budget Allocation | Priority |
|---------|-------------------|----------|
| Engine profile briefing | ~200 tokens | HIGH — always included |
| Relevant gaps (2-3) | ~300 tokens | HIGH — primary optimization guidance |
| Relevant strengths (1-2) | ~100 tokens | MEDIUM — prevents wasted effort |
| Matched examples (2-3) | ~800 tokens | HIGH — few-shot context is critical |
| Tuning intel (PG only) | ~150 tokens | MEDIUM — additive value |
| Scanner findings (PG only) | ~200 tokens | MEDIUM — PG-specific intelligence |
| Constraints | ~100 tokens | HIGH — never truncated |

If token budget is tight, truncation order: scanner findings → tuning intel → 3rd example → 3rd gap → strengths → 2nd example → 2nd gap. Never truncate: constraints, 1st example, 1st gap, briefing note.

---

## 8. Compression Walkthrough: L1 → L2 → L3 → L4

### Step 1: 400 Layer 1 Entries → 8 Layer 2 Findings

**Input**: 400 JSONL entries from `duckdb_tpcds` across 4 runs, covering 99 queries × ~4 workers each.

**DeepSeek R1 examines the batch and extracts**:

| Finding ID | Category | Claim | Evidence | Confidence |
|-----------|----------|-------|----------|------------|
| F-DUCK-042 | scan_method | Implicit comma-joins block pushdown | 12 queries, 0 contradictions | high |
| F-DUCK-029 | scan_method | Correlated subqueries not auto-decorrelated | 6 queries, 1 contradiction | high |
| F-DUCK-044 | scan_method | Multiple fact scans not merged | 8 queries, 0 contradictions | high |
| F-DUCK-055 | scan_method | Hash join parallelizes well, don't split | 5 queries, 0 contradictions | medium |
| F-DUCK-058 | scan_method | OR-to-UNION decomposition helps selective OR | 4 queries, 1 contradiction | medium |
| F-DUCK-061 | scan_method | Window functions efficient, don't rewrite | 3 queries, 0 contradictions | medium |
| F-DUCK-063 | scan_method | CTE inlining is automatic, don't force | 4 queries, 0 contradictions | medium |
| F-DUCK-067 | scan_method | GROUP BY ALL handles grouping well | 2 queries, 0 contradictions | low |

**Compression**: 400 entries → 8 findings (50:1)

### Step 2: 8 Findings → 3 Layer 3 Patterns

**DeepSeek R1 aggregates findings into patterns**, requiring 3+ query span:

| Pattern ID | Source Findings | Stats | Status |
|-----------|----------------|-------|--------|
| PATTERN-DIM-CTE-001 | F-042, F-055, F-063 | 24 obs, 21 wins, 87.5%, 12 queries | promoted |
| PATTERN-DECORR-001 | F-029 | 9 obs, 7 wins, 77.8%, 6 queries | promoted |
| PATTERN-SCAN-001 | F-044, F-058 | 18 obs, 14 wins, 77.8%, 8 queries | promoted |

F-061, F-067 don't qualify for patterns yet (< 3 queries or need more observations). They remain as findings only.

**Compression**: 8 findings → 3 patterns (~3:1)

### Step 3: 3 Patterns → Engine Profile Updates

**DeepSeek R1 reasoning step**:

The model receives:
1. Current DuckDB engine profile
2. 3 new promoted patterns with full stats and counter-indications
3. 5 remaining findings that didn't promote to patterns

It reasons:

- PATTERN-DIM-CTE-001 maps to existing gap `IMPLICIT_JOIN_PUSHDOWN` → UPDATE gap with new stats, add new `what_worked` entries from latest runs, update field notes
- PATTERN-DECORR-001 maps to existing gap `CORRELATED_SUBQUERY_DECORRELATION` → UPDATE with new evidence
- PATTERN-SCAN-001 maps to existing gap `REDUNDANT_SCAN_ELIMINATION` → UPDATE with new examples
- F-055 (hash join parallelization) → ADD new strength `PARALLEL_HASH_JOIN`
- F-061 (window function efficiency) → ADD new strength `WINDOW_FUNCTION_OPTIMIZATION`

The profile grows with evidence. Nothing is added without supporting data.

### Step 4: Gold Example Promotion

From the 21 wins in PATTERN-DIM-CTE-001, the promotion pipeline selects the best candidates:

**Selection criteria**:
- Highest speedup with highest validation confidence
- Diverse query archetypes (don't promote 3 examples that all teach the same thing)
- Clear, generalizable SQL (not query-specific hacks)

**Result**: `q88_dimension_cte` promoted as new gold example alongside existing `q6_date_cte`. They teach complementary aspects — q6 shows date isolation, q88 shows multi-dimension isolation with conditional aggregation.

---

*End of component specifications.*

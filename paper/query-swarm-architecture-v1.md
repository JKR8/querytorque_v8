# Query Optimization Swarm — Architecture Specification v1

> **Audience:** Engineering team (implementation spec)
> **Status:** Architecture-final, ready for technical design
> **Scope:** Per-query pipeline, Workload mode, engine pack system, prompt composition

---

## 1. System Overview

The system optimizes SQL queries to run faster on smaller infrastructure. It operates in two modes:

- **Single-Query Mode** — full diagnostic and rewrite pipeline for one query
- **Workload Mode** — fleet-level optimization of 100+ queries to fit a smaller warehouse/instance

Both modes share the same core pipeline. Workload mode wraps it with triage, fleet-level actions, and iterative downsizing.

### 1.1 Core Value Proposition

```
Client runs workload on Medium warehouse → $X/month
System optimizes queries to fit on Small warehouse → $Y/month
Savings = $X - $Y, validated by benchmark
```

The system gets paid by saving money. Every architectural decision optimizes for: smaller box, same SLA.

### 1.2 Composition Model

Every pipeline run is assembled from four modular components:

```
SYSTEM = universal_doctrine
       + engine_pack(target_engine)
       + scenario_card(resource_envelope)
       + output_contract
```

These are composed by the Orchestrator at runtime. No component knows about the others at authoring time.

---

## 2. Architecture Diagrams

### 2.1 Single-Query Pipeline

```
┌─────────────────────────────────────────────────────────────────────┐
│                        PRE-COMPUTE (deterministic, no LLM)          │
│                                                                     │
│   Input SQL ──→ Parse Profile ──→ Cost Spine + Runtime Profile      │
│                                   (§II.B structured evidence)       │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATOR                                  │
│                                                                     │
│   Assembles: universal_doctrine                                     │
│            + engine_pack(postgres_17)                                │
│            + scenario_card(small_instance)                           │
│            + output_contract                                        │
│                                                                     │
│   Compiles: evidence bundle (SQL + cost spine + runtime profile)    │
│   Briefs:   analyst with full context                               │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        ANALYST (single LLM call)                     │
│                                                                     │
│   Reads: §I (gold examples), §II (evidence), §III (engine pack)    │
│   Produces: diagnosis + 4 diversified worker strategies             │
│                                                                     │
│   Step 1: Map query to optimal abstract plan                        │
│   Step 2: Map actual plan from evidence                             │
│   Step 3: Identify divergence (optimizer blind spots + runtime)     │
│   Step 4: Design 4 strategies across rewrite families               │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
              ▼                ▼                ▼                ▼
┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
│  W1      │ │  W2      │ │  W3      │ │  W4      │
│  Family  │ │  Family  │ │  Compound│ │  Novel / │
│  A       │ │  B       │ │  A+B+... │ │  Runtime │
│  rewrite │ │  rewrite │ │  rewrite │ │  rewrite │
└────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘
     │             │             │             │
     └─────────────┴──────┬──────┴─────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        VALIDATE (per-worker output)                  │
│                                                                     │
│   Equivalence check (parse → normalize → compare)                   │
│   Regression detection (EXPLAIN cost comparison)                    │
│   Runtime constraint check (does rewrite fit target scenario?)      │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        COMPRESS (merge 4 → ranked candidates)        │
│                                                                     │
│   Deduplicate equivalent rewrites                                   │
│   Rank by: expected impact × confidence × invasiveness              │
│   Forward top candidates to sniper                                  │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        SNIPER (single best rewrite)                  │
│                                                                     │
│   Sees: all candidates + original evidence + engine pack            │
│   Produces: one final SQL rewrite (may combine elements)            │
│   Validates: final equivalence + cost check                         │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        CONFIG BOOST (deterministic, no LLM)          │
│                                                                     │
│   Pattern-match on runtime profile + rewrite characteristics        │
│   Apply engine-specific session config / knobs                      │
│   Produce: final SQL + config recommendations                       │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        BENCHMARK                                     │
│                                                                     │
│   Run original vs optimized on target warehouse/instance            │
│   Measure: latency, spill, memory, partitions scanned              │
│   Verdict: pass/fail against scenario card thresholds               │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 Workload Mode Pipeline

```
CLIENT WORKLOAD
(100+ queries + execution logs + frequency data)
                │
                ▼
┌───────────────────────────────────────────────────────────────┐
│                   STAGE 0: WORKLOAD TRIAGE                     │
│                                                               │
│   Input: all queries + profiles + frequency + current cost    │
│                                                               │
│   Actions:                                                    │
│     1. Score every query: pain × frequency × tractability     │
│     2. Detect fleet-level patterns (shared scans, config)     │
│     3. Classify: skip / tier-1-only / tier-2 / tier-3        │
│     4. Set target warehouse size (start one step down)        │
└──────────────────────────────┬────────────────────────────────┘
                               │
                ┌──────────────┼──────────────┐
                │              │              │
                ▼              ▼              ▼
┌────────────────┐ ┌────────────────┐ ┌────────────────────────┐
│  TIER 1:       │ │  TIER 2:       │ │  TIER 3:               │
│  FLEET ACTIONS │ │  LIGHT OPT     │ │  DEEP OPT              │
│                │ │                │ │                        │
│  Config changes│ │  Single-pass   │ │  Full pipeline:        │
│  Index/cluster │ │  analyst,      │ │  analyst → 4W →        │
│  MV candidates │ │  1 rewrite,    │ │  validate → compress → │
│  Stats refresh │ │  validate,     │ │  sniper → config       │
│  Service enable│ │  done          │ │                        │
│                │ │                │ │  10-20% of queries     │
│  ~1 analysis   │ │  ~5K tok/query │ │  ~40-50K tok/query     │
│  benefits N    │ │  ~60-70% of    │ │                        │
│  queries       │ │  queries       │ │                        │
└───────┬────────┘ └───────┬────────┘ └───────┬────────────────┘
        │                  │                  │
        └──────────────────┴─────────┬────────┘
                                     │
                                     ▼
┌───────────────────────────────────────────────────────────────┐
│                   RE-BENCHMARK ON TARGET WAREHOUSE              │
│                                                               │
│   Run ALL queries on target (smaller) warehouse               │
│   Score: how many now pass SLA?                               │
│                                                               │
│   If all pass → done, report savings                          │
│   If most pass → report, flag remaining as "needs larger"     │
│   If <80% pass → try next size up, or iterate tier 2/3       │
└──────────────────────────────┬────────────────────────────────┘
                               │
                               ▼
┌───────────────────────────────────────────────────────────────┐
│                   ITERATE DOWN (optional)                       │
│                                                               │
│   All queries pass on Small?                                  │
│   → Try X-Small. Re-triage, re-optimize, re-benchmark.       │
│   → Keep pushing until failure rate exceeds threshold.        │
│   → Report: "workload fits on [smallest viable size]"         │
└──────────────────────────────┬────────────────────────────────┘
                               │
                               ▼
┌───────────────────────────────────────────────────────────────┐
│                   DELIVERABLE                                   │
│                                                               │
│   1. Fleet actions (config, indexes, services)                │
│   2. Per-query rewrites (SQL + validation evidence)           │
│   3. Scorecard (before/after per query on target warehouse)   │
│   4. Business case ($X/month savings, methodology)            │
│   5. Residuals (queries that can't fit, with explanation)     │
└───────────────────────────────────────────────────────────────┘
```

> **Note:** Tiers are sequential, not parallel. Tier 1 fleet actions run first, then re-benchmark and re-triage. Tier 2 light optimization runs next on reclassified queries. Only remaining hard queries enter Tier 3. This ordering maximizes the benefit of fleet-level actions before spending deep-pipeline tokens.

### 2.3 Composition Assembly

```
                    ┌──────────────────────┐
                    │    ORCHESTRATOR       │
                    │                      │
                    │  Reads:              │
                    │   - target engine    │
                    │   - target size      │
                    │   - query/workload   │
                    └──────────┬───────────┘
                               │
            ┌──────────────────┼──────────────────┐
            │                  │                  │
            ▼                  ▼                  ▼
   ┌─────────────────┐ ┌──────────────┐ ┌──────────────────┐
   │ ENGINE PACK     │ │ SCENARIO     │ │ PATTERN LIBRARY  │
   │ (plugin)        │ │ CARD         │ │ (cross-engine)   │
   │                 │ │ (plugin)     │ │                  │
   │ postgres_17     │ │ tiny_memory  │ │ or_to_union_all  │
   │ snowflake_2025  │ │ strict_sla   │ │ cte_isolation    │
   │ duckdb_1_2      │ │ spill_fatal  │ │ decorrelate      │
   └────────┬────────┘ └──────┬───────┘ └────────┬─────────┘
            │                  │                  │
            └──────────────────┼──────────────────┘
                               │
                               ▼
                 ┌──────────────────────────┐
                 │  ASSEMBLED SYSTEM PROMPT  │
                 │                          │
                 │  universal_doctrine      │
                 │  + engine_pack(...)      │
                 │  + scenario_card(...)    │
                 │  + output_contract       │
                 │  + evidence_bundle       │
                 └──────────────────────────┘
```

### 2.4 Design Decisions

**Why no engine-specialist workers?**
Workers are family-diversified, not engine-specialized. Each worker sees the full engine pack and scenario card. An engine-specialist worker would narrow its search space prematurely — the family diversity constraint already ensures coverage across rewrite strategies while the engine pack provides engine-specific guardrails.

**Why family-diversified workers?**
Empirical evidence from 172 validated optimizations shows that the winning rewrite comes from different families across queries. No single family dominates (A: 24%, B: 21%, C: 18%, D: 15%, E: 12%, F: 10%). Family diversification maximizes coverage of the solution space.

**Why deterministic config boost?**
Config boost rules (SET work_mem, enable/disable features) are deterministic pattern matches on EXPLAIN plans. LLM involvement adds latency and hallucination risk for decisions that can be fully codified. The LLM's role is rewrite creativity; config is mechanical.

---

## 3. Universal Doctrine

The universal doctrine is engine-agnostic and stable across all deployments. It defines the optimization philosophy, safety rules, and shared vocabulary.

### 3.1 Mission

```yaml
mission: |
  Optimize SQL queries to run faster on smaller infrastructure.
  Every recommendation must reduce latency, memory, or cost
  on the target resource envelope.
```

### 3.2 Optimization Principles (engine-agnostic)

```yaml
principles:
  minimum_work: |
    The best optimization is eliminating work the engine doesn't 
    need to do. Prefer rewrites that reduce rows processed, joins 
    evaluated, and bytes scanned.
  
  fit_the_box: |
    The query must complete within the scenario card's resource 
    envelope. A 2x faster query that spills to disk on the target 
    is worse than a 1.3x faster query that fits in memory.
  
  equivalence_first: |
    Every rewrite must produce identical results to the original.
    No optimization justifies correctness risk.
  
  evidence_required: |
    No recommendation without evidence. Cite the plan node, 
    profile counter, or cost estimate that supports the claim.
```

### 3.3 Common Failure Modes (universal taxonomy)

```yaml
bottleneck_taxonomy:
  - spill:           "Intermediate results exceed memory, written to disk/remote"
  - join_explosion:  "Join produces more rows than either input (bad cardinality)"
  - bad_join_order:  "Large table on build side, small table on probe side"
  - bad_pruning:     "Engine scans partitions/pages it doesn't need"
  - redundant_scan:  "Same table scanned multiple times unnecessarily"
  - sort_pressure:   "ORDER BY / GROUP BY on large intermediate"
  - skew:            "Data skew causes one worker to do most of the work"
  - materialization: "CTE or subquery materialized when streaming would suffice"
  - bad_estimates:   "Optimizer cardinality estimates off by >10x"
```

This taxonomy is universal. Engine packs map engine-specific signals to these labels.

### 3.4 Hallucination Prevention

```yaml
hallucination_rules:
  feature_gate: |
    Any recommendation referencing an engine feature MUST satisfy:
      (a) engine_pack.capabilities confirms feature exists: true
      (b) evidence_bundle contains supporting signal
    If either gate fails, label recommendation as:
      "CONDITIONAL: verify [feature] is available and [signal] is present"
  
  version_gate: |
    Do not assert behavior from one engine version applies to another.
    Engine packs are versioned. Use only the loaded version.
  
  no_invented_syntax: |
    Do not invent SQL hints, session parameters, or commands.
    Only use syntax documented in the engine pack.
```

### 3.5 Worker Diversity Rules

```yaml
worker_diversity:
  structure: |
    4 workers, family-diversified. Each produces a complete SQL rewrite.
    
    W1: Rewrite family A (assigned by analyst based on diagnosis)
    W2: Rewrite family B (different family from W1)
    W3: Compound strategy (combines elements from 2+ families)
    W4: Novel / runtime-motivated (technique not in gold library,
        OR engine-runtime-motivated rewrite when runtime profile 
        shows primary bottleneck is physical constraints)
  
  constraint: |
    W1 and W2 MUST use different rewrite families.
    W3 MUST combine elements from at least 2 families.
    W4 MUST differ from W1-W3 in approach.
    All workers see the engine pack and scenario card.
    All workers must produce rewrites that fit the scenario card's
    resource envelope.
```

---

## 4. Engine Pack Specification

Engine packs are declarative, versioned knowledge bases. One per engine, stored as YAML, injected into the system prompt by the orchestrator.

### 4.1 Schema

```yaml
# ENGINE PACK SCHEMA
# All fields are required unless marked (optional)

engine: string                    # e.g., "postgres", "snowflake", "duckdb"
version: string                   # e.g., "17", "2025-gen2-optima", "1.2"
label: string                     # human-readable, e.g., "PostgreSQL 17"

# --- What the engine can do ---
capabilities:
  services:                       # (optional) engine-specific services
    <service_name>:
      exists: boolean
      description: string
      triggers: [string]          # when to consider enabling
      knobs: [string]             # session params / DDL to enable
      limitations: [string]       # (optional) what it can't do
      
  hints:                          # (optional) optimizer hints
    <hint_category>: [string]     # e.g., join_order: ["FORCE_JOIN_ORDER"]
    
  stats_operations: [string]      # commands to refresh statistics
  
  materialization_controls:       # (optional) CTE/view materialization
    cte_materialization:
      behavior: string            # e.g., "physical store (WithClauseResult)"
      hints: [string]             # e.g., ["AS MATERIALIZED", "AS NOT MATERIALIZED"]
      
  parallel_execution:             # (optional)
    controls: [string]            # session params for parallelism
    
  partitioning:                   # (optional)
    type: string                  # e.g., "micro-partitions", "declarative"
    clustering: string            # e.g., "CLUSTER BY", "N/A"
    auto_maintenance: boolean

# --- What the optimizer handles well and poorly ---
optimizer_profile:
  handles_well:                   # patterns the optimizer already optimizes
    - pattern: string
      mechanism: string           # HOW the optimizer handles it
      implication: string         # "do NOT rewrite this" guidance
  blind_spots:                    # patterns the optimizer misses
    - pattern: string
      mechanism: string           # WHY the optimizer misses it
      opportunity: string         # what rewrite can exploit
      what_worked: [string]       # speedup-ranked evidence
      what_didnt_work: [string]   # regression-ranked evidence
      field_notes: [string]       # hard-learned rules

# --- What to look for in execution profiles ---
profile_signals:
  spill:
    counters: [string]            # exact counter names in profile output
    likely_causes: [string]       # common root causes
    
  pruning:
    metrics: [string]             # e.g., "Partitions Scanned vs Total"
    good_threshold: string        # e.g., "<5% of total partitions"
    
  memory:
    counters: [string]            # e.g., "Peak Memory Usage"
    
  estimates:
    accuracy_signals: [string]    # e.g., "Q-Error", "Rows Removed by Filter vs estimate"
    
  plan_nodes:                     # engine-specific plan node names
    cte_materialized: [string]    # e.g., ["WithClauseResult"], ["CTE Scan"]
    hash_join: [string]           # e.g., ["Hash Join"], ["HashJoin"]
    sort: [string]                # e.g., ["Sort"], ["Top-N Sort"]
    scan: [string]                # e.g., ["Seq Scan", "Index Scan"], ["TableScan"]

# --- Approved transformations ---
rewrite_playbook:
  - name: string                  # identifier, e.g., "or_to_union_all_for_pruning"
    detect: string                # what to look for in the query/plan
    action: string                # what to do
    why: string                   # why this helps on THIS engine
    guard: string                 # (optional) when NOT to do this
    
# --- Physical design guidance ---
physical_design:
  indexing:
    types: [string]               # e.g., ["btree", "hash", "gin", "gist"]
    auto_managed: boolean         # does the engine auto-create?
    recommendations_format: string # DDL template
    
  clustering:
    command: string               # e.g., "ALTER TABLE ... CLUSTER BY (...)"
    economics: string             # when clustering is/isn't worth it
    
  materialized_views:
    supported: boolean
    incremental: boolean
    limitations: [string]

# --- Per-query session config (applied per-query, SET LOCAL) ---
config_boost_rules:
  - condition: string             # pattern to match on profile + rewrite
    action: string                # SET LOCAL parameter change
    rationale: string             # why this helps
    scope: "session"              # always session-level

# --- Fleet-level physical design (DDL, applied once for all queries) ---
physical_design_recommendations:
  - condition: string             # pattern detected across fleet
    action: string                # DDL command (CREATE INDEX, CLUSTER, etc.)
    rationale: string             # why this helps
    scope: "fleet"                # applied once, benefits multiple queries

# --- Validation probes (what to re-check after optimization) ---
validation:
  - string                        # e.g., "Re-check spill counters after rewrite"
```

### 4.2 Example: PostgreSQL 17

```yaml
engine: postgres
version: "17"
label: "PostgreSQL 17"

capabilities:
  services: {}                    # Postgres has no QAS/SOS equivalents

  hints:
    join_order:
      - "SET join_collapse_limit = 1"
      - "SET from_collapse_limit = 1"
    scan_method:
      - "SET enable_seqscan = off"
      - "SET enable_indexscan = off"
      - "SET enable_hashjoin = off"
      - "SET enable_mergejoin = off"
      - "SET enable_nestloop = off"

  stats_operations:
    - "ANALYZE <table>"
    - "CREATE STATISTICS <name> (dependencies) ON <cols> FROM <table>"

  materialization_controls:
    cte_materialization:
      behavior: |
        PG12+: optimizer chooses. Single-reference CTEs are typically 
        inlined. Multi-reference CTEs may be materialized.
      hints:
        - "AS MATERIALIZED"
        - "AS NOT MATERIALIZED"

  parallel_execution:
    controls:
      - "SET max_parallel_workers_per_gather"
      - "SET parallel_tuple_cost"
      - "SET parallel_setup_cost"
      - "SET min_parallel_table_scan_size"

  partitioning:
    type: "declarative (range, list, hash)"
    clustering: "N/A (no auto clustering; CLUSTER command for one-time sort)"
    auto_maintenance: false

profile_signals:
  spill:
    counters:
      - "Sort Method: external merge"
      - "Batches: N (originally 1)"
      - "temp_blks_read / temp_blks_written"
    likely_causes:
      - "work_mem too small for sort"
      - "hash join build side exceeds work_mem"
      - "hash_mem_multiplier insufficient"

  pruning:
    metrics:
      - "Rows Removed by Filter (high = bad pruning or missing index)"
      - "partitions scanned vs total partitions (declarative partitioning)"
    good_threshold: "Rows Removed by Filter should be <10x rows returned"

  memory:
    counters:
      - "Peak Memory Usage (per-node, EXPLAIN ANALYZE BUFFERS)"
      - "shared_blks_hit + shared_blks_read (buffer usage)"
      - "temp_blks_written (spill indicator)"

  estimates:
    accuracy_signals:
      - "Rows (estimated) vs Rows (actual) per plan node"
      - "ratio > 10x indicates bad statistics or correlation"

  plan_nodes:
    cte_materialized: ["CTE Scan"]
    hash_join: ["Hash Join"]
    sort: ["Sort", "Incremental Sort"]
    scan: ["Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan"]

rewrite_playbook:
  - name: or_to_union_all
    detect: "OR predicates across different indexed columns"
    action: "Split into UNION ALL branches, each using its own index"
    why: "Postgres cannot use multiple indexes efficiently through OR; BitmapOr is often slower than separate index scans"
    guard: "Skip if both columns are in a composite index"

  - name: cte_inline_for_pushdown
    detect: "CTE with filter applied outside; optimizer not pushing filter in"
    action: "Use AS NOT MATERIALIZED or rewrite as subquery"
    why: "Materialized CTEs are optimization fences in older PG; even in 17, multi-ref CTEs may fence"

  - name: decorrelate_subquery
    detect: "Correlated subquery in SELECT or WHERE with high outer row count"
    action: "Rewrite as JOIN with pre-aggregation"
    why: "Nested loop over correlated subquery executes inner query once per outer row"

  - name: predicate_pullup
    detect: "Filter applied late in plan (after join) that could filter earlier"
    action: "Move restrictive predicate into subquery or CTE to reduce join input"
    why: "Reduces intermediate row count, less memory/spill pressure"

  - name: partial_aggregation
    detect: "GROUP BY on large join result; aggregation is the bottleneck"
    action: "Pre-aggregate one side before joining"
    why: "Reduces join input cardinality; hash join build side fits in work_mem"

physical_design:
  indexing:
    types: ["btree", "hash", "gin", "gist", "brin"]
    auto_managed: false
    recommendations_format: "CREATE INDEX [CONCURRENTLY] idx_name ON table (columns) [WHERE ...]"

  clustering:
    command: "CLUSTER table USING index_name"
    economics: "Only useful for range scans on the clustered column. Does not auto-maintain. BRIN indexes are the low-cost alternative for append-only tables."

  materialized_views:
    supported: true
    incremental: false
    limitations:
      - "REFRESH MATERIALIZED VIEW is full recomputation"
      - "CONCURRENTLY option requires unique index"

config_boost_rules:
  - condition: "Hash join spill detected (Batches > 1)"
    action: "SET work_mem = '{calculated}MB'"
    rationale: "Eliminates hash join spill, keeps processing in memory"

  - condition: "Sort spill detected (external merge)"
    action: "SET work_mem = '{calculated}MB'"
    rationale: "In-memory quicksort is orders of magnitude faster than external merge"

  - condition: "Parallel workers not utilized on large sequential scan"
    action: "SET max_parallel_workers_per_gather = {calculated}"
    rationale: "Distributes scan across cores; reduces wall-clock time"

  - condition: "Cardinality estimates off by >10x on multi-column predicate"
    action: "CREATE STATISTICS (dependencies) ON col1, col2 FROM table"
    rationale: "Extended statistics capture column correlations the optimizer misses"

  - condition: "Join collapse limit preventing optimal join order"
    action: "SET join_collapse_limit = {calculated}"
    rationale: "Default limit of 8 may prevent optimizer from finding optimal order for complex joins"

validation:
  - "Compare EXPLAIN ANALYZE before/after: check actual rows at each node"
  - "Check for new spill (Sort Method, Hash Batches) after rewrite"
  - "Verify temp_blks_written reduced (pg_stat_statements)"
  - "Confirm no regression in total Buffers (shared + temp)"
  - "Run on target instance size, not development instance"
```

### 4.3 Example: Snowflake 2025 Gen2/Optima

```yaml
engine: snowflake
version: "2025-gen2-optima"
label: "Snowflake 2025 (Gen2 / Optima Architecture)"

capabilities:
  services:
    query_acceleration_service:
      exists: true
      description: "Offloads scan-heavy portions to elastic compute pool"
      triggers:
        - "Large TableScan nodes with high percentage of total time"
        - "Queries with LIMIT clause (even without ORDER BY)"
        - "Filter-heavy scans on large tables"
      knobs:
        - "ALTER WAREHOUSE SET ENABLE_QUERY_ACCELERATION = TRUE"
        - "ALTER WAREHOUSE SET QUERY_ACCELERATION_MAX_SCALE_FACTOR = N"
      limitations:
        - "Not supported on Hybrid Tables"
        - "Most effective with selective predicates"

    search_optimization_service:
      exists: true
      description: "Persistent search structures for point-lookup and search patterns"
      triggers:
        - "High-cardinality equality lookups (point queries)"
        - "LIKE / ILIKE / substring predicates"
        - "Geo-spatial predicates"
        - "Scalar functions in predicates"
        - "Queries joining massive table to small subset"
      knobs:
        - "ALTER TABLE ADD SEARCH OPTIMIZATION ON EQUALITY(...)"
        - "ALTER TABLE ADD SEARCH OPTIMIZATION ON SUBSTRING(...)"
        - "ALTER TABLE ADD SEARCH OPTIMIZATION ON GEO(...)"
      limitations:
        - "Not supported on Hybrid Tables"
        - "Ongoing storage cost"

    dynamic_tables:
      exists: true
      description: "Declarative incremental materialization"
      triggers:
        - "Complex transformation pipelines (multiple CTEs, joins)"
        - "Deduplication patterns (QUALIFY RANK()=1)"
        - "ETL replacing INSERT OVERWRITE statements"
      knobs:
        - "CREATE DYNAMIC TABLE ... TARGET_LAG = '...' AS SELECT ..."
      limitations:
        - "2025: supports Left Outer Join, incremental Rank"
        - "Verify specific join/window patterns are supported"

    hybrid_tables:
      exists: true
      description: "Row-oriented storage for transactional workloads (Unistore)"
      triggers:
        - "Single-row INSERT/UPDATE patterns"
        - "Strict constraint enforcement needs"
      knobs:
        - "CREATE HYBRID TABLE ..."
      limitations:
        - "No QAS support"
        - "No SOS support"
        - "Different performance model from columnar"

    optima_indexing:
      exists: true
      description: "Automatic background index creation for recurring lookup patterns"
      triggers:
        - "Recurring point-lookup queries on non-clustered columns"
      knobs: []
      limitations:
        - "Background process; not immediate"
        - "Only recommend manual intervention if SLA < 200ms"

  hints:
    join_order:
      - "/*+ FORCE_JOIN_ORDER */"
    cte_materialization:
      - "AS MATERIALIZED"
      - "-- remove AS MATERIALIZED to force inline"

  stats_operations:
    - "ANALYZE TABLE <table>"

  materialization_controls:
    cte_materialization:
      behavior: "Physical materialization (WithClauseResult node in plan). Stores full result before downstream consumption."
      hints:
        - "AS MATERIALIZED"
        - "Remove to allow optimizer inlining"

  parallel_execution:
    controls: []                  # managed by warehouse size

  partitioning:
    type: "micro-partitions (automatic, columnar)"
    clustering: "ALTER TABLE ... CLUSTER BY (...)"
    auto_maintenance: true

profile_signals:
  spill:
    counters:
      - "Bytes Spilled to Local Storage"
      - "Bytes Spilled to Remote Storage"
    likely_causes:
      - "Hash join build side exceeds warehouse memory"
      - "Large sort exceeds warehouse memory"
      - "CTE materialization of large intermediate"

  pruning:
    metrics:
      - "Partitions Scanned vs Partitions Total (per TableScan node)"
    good_threshold: "<5% of total partitions scanned for filtered queries"

  memory:
    counters:
      - "Bytes Spilled to Remote Storage"
      - "Peak Memory Usage (if shown in profile)"

  estimates:
    accuracy_signals:
      - "Q-Error on join keys (ratio of estimated to actual NDV)"
      - "Estimated vs actual row counts per operator (if available)"

  plan_nodes:
    cte_materialized: ["WithClauseResult"]
    hash_join: ["HashJoin"]
    sort: ["Sort", "SortWithLimit"]
    scan: ["TableScan", "ExternalScan"]

rewrite_playbook:
  - name: or_to_union_all_for_pruning
    detect: "OR predicates across different columns in WHERE clause"
    action: "Rewrite as UNION ALL with one predicate per branch"
    why: "Snowflake cannot prune micro-partitions through OR across columns. UNION ALL allows independent pruning per branch."
    guard: "Skip if both predicates are on the clustering key"

  - name: cte_inline_for_memory
    detect: "Large CTE materialized (WithClauseResult) on small warehouse"
    action: "Remove AS MATERIALIZED; allow optimizer to inline and stream"
    why: "Physical materialization of large intermediate is fatal on memory-constrained warehouses."
    guard: "Only if CTE is referenced once. Multi-reference CTEs may benefit from materialization if result is small."

  - name: pre_aggregate_for_spill
    detect: "Hash join spills to remote; one input is aggregatable before join"
    action: "Pre-aggregate the large input before joining"
    why: "Reduces hash join build side below warehouse memory. Eliminates remote spill."

  - name: force_join_order_for_build_side
    detect: "Q-Error shows wrong join order; large table on build side"
    action: "Reorder joins and apply FORCE_JOIN_ORDER hint, or ANALYZE TABLE"
    why: "Stale or incorrect NDV estimates cause optimizer to pick wrong build/probe assignment"

physical_design:
  indexing:
    types: ["automatic (Optima)", "SOS"]
    auto_managed: true
    recommendations_format: "ALTER TABLE ... ADD SEARCH OPTIMIZATION ON ..."

  clustering:
    command: "ALTER TABLE ... CLUSTER BY (...)"
    economics: |
      High-churn tables (frequent UPDATE/DELETE): STOP automatic clustering.
      Use natural ingestion sort or SOS for point lookups.
      Low-churn or append-only: automatic clustering is cost-effective.

  materialized_views:
    supported: true
    incremental: true
    limitations:
      - "Limited transformation support"
      - "Consider Dynamic Tables for complex transforms"

config_boost_rules:
  - condition: "QAS eligible (large scan + selective filter or LIMIT)"
    action: "ALTER WAREHOUSE SET ENABLE_QUERY_ACCELERATION = TRUE"
    rationale: "Offloads scan to elastic compute; warehouse only processes filtered result"

  - condition: "Spill to remote detected + stale statistics (Q-Error > 5x)"
    action: "ANALYZE TABLE on tables with bad estimates"
    rationale: "Better statistics → correct join order → smaller build side → no spill"

  - condition: "Poor pruning (>5% partitions scanned) on date-filtered query"
    action: "ALTER TABLE ... CLUSTER BY (date_column)"
    rationale: "Aligns micro-partition boundaries with common filter predicates"

  - condition: "High-churn table with expensive automatic clustering"
    action: "ALTER TABLE ... SUSPEND RECLUSTER; use natural ingestion sort"
    rationale: "Automatic clustering on high-churn tables has poor cost/benefit"

  - condition: "Recurring point-lookup, SLA > 200ms"
    action: "No action (Optima will auto-index in background)"
    rationale: "Manual intervention unnecessary"

  - condition: "Recurring point-lookup, SLA < 200ms"
    action: "ALTER TABLE ... ADD SEARCH OPTIMIZATION ON EQUALITY(lookup_column)"
    rationale: "SOS provides immediate search structure; Optima too slow for tight SLA"

validation:
  - "Re-check Bytes Spilled to Remote Storage (must be 0 on target warehouse)"
  - "Check Partitions Scanned ratio (must be <5% for filtered queries)"
  - "Verify join order in profile matches intended order"
  - "Check QAS usage in profile (Bytes accelerated)"
  - "Run on target warehouse size, not larger"
```

---

## 5. Scenario Card Specification

Scenario cards define the resource envelope and failure criteria. They are engine-agnostic.

### 5.1 Schema

```yaml
# SCENARIO CARD SCHEMA

name: string
description: string

resource_envelope:
  memory: string
  compute: string
  storage_io: string

failure_definitions:
  - metric: string
    threshold: string
    severity: string              # "fatal" | "warning"

strategy_priorities: [string]     # ordered, most important first
strategy_avoid: [string]          # things that will fail on this envelope
```

### 5.2 Example: X-Small Survival

```yaml
name: xsmall_survival
description: |
  Minimal compute. Any spill to remote storage is fatal (causes timeout).
  Queries must fit entirely in memory or use offload services.

resource_envelope:
  memory: "~8GB (warehouse-managed, not configurable)"
  compute: "1 node, X-Small class"
  storage_io: "Micro-partition reads; remote spill = disk I/O death"

failure_definitions:
  - metric: query_duration
    threshold: ">300s"
    severity: fatal
  - metric: bytes_spilled_remote
    threshold: ">0"
    severity: fatal
  - metric: bytes_spilled_local
    threshold: ">1GB"
    severity: warning

strategy_priorities:
  - "Eliminate remote spill (reduce intermediate sizes below memory)"
  - "Maximize partition pruning (reduce scan volume)"
  - "Offload scan to QAS where eligible"
  - "Avoid physical materialization of large intermediates"
  - "Pre-aggregate before joins to reduce build side"

strategy_avoid:
  - "Large CTE materialization (fatal on X-Small)"
  - "Strategies that increase intermediate row count"
  - "Broad scans without pruning (10TB will not fit through X-Small)"
```

### 5.3 Example: Postgres Small Instance

```yaml
name: postgres_small_instance
description: |
  Limited memory instance. Disk spill degrades performance severely.
  Parallel workers limited. Must fit operations in work_mem budget.

resource_envelope:
  memory: "~2GB total, work_mem default 4MB per operation"
  compute: "2 vCPU, limited parallel workers"
  storage_io: "EBS gp3, 3000 IOPS baseline"

failure_definitions:
  - metric: query_duration
    threshold: ">60s"
    severity: fatal
  - metric: temp_blks_written
    threshold: ">500MB"
    severity: warning
  - metric: external_merge_sort
    threshold: "any"
    severity: warning

strategy_priorities:
  - "Eliminate hash join spill (fit build side in work_mem)"
  - "Eliminate sort spill (fit sort input in work_mem)"
  - "Reduce sequential scan volume (indexes, partition pruning)"
  - "Pre-aggregate before joins to reduce intermediate sizes"
  - "Minimize buffer cache pressure (fewer pages touched)"

strategy_avoid:
  - "Strategies that increase intermediate row count"
  - "Heavy parallel plans (limited workers available)"
  - "Plans requiring large temp file usage"
```

---

## 6. Evidence Bundle Specification

Before the analyst runs, a deterministic extractor produces structured evidence from the query's execution profile. No LLM required for extraction.

### 6.1 Schema

```yaml
# EVIDENCE BUNDLE (per query)

query_id: string
query_sql: string

cost_spine:
  total_cost: number
  top_operators:
    - node_id: string
      operator: string
      cost_pct: number
      estimated_rows: number
      actual_rows: number         # if ANALYZE available
      notes: string

runtime_profile:
  spill:
    detected: boolean
    details: string
    root_cause: string
  pruning:
    ratio: string
    status: string                # "good" | "poor" | "terrible"
    blocking_factor: string
  memory:
    peak: string
    budget: string                # (enriched by orchestrator from scenario card)
    status: string                # (enriched by orchestrator: "within_budget" | "over_budget")
  # Note: `memory.budget` and `memory.status` are NOT set by the evidence extractor.
  # The orchestrator enriches these fields by comparing `memory.peak` against the
  # scenario card's resource envelope.
  estimates:
    worst_node: string
    ratio: string
  service_eligibility:
    - service: string
      eligible: boolean
      reason: string

frequency:                        # (optional, workload mode)
  executions_per_day: number
  
current_cost:                     # (optional, workload mode)
  estimated_monthly: string
```

### 6.2 Extraction Rules

**PostgreSQL:** Parse `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`.
- Spill: `"Sort Method": "external merge"`, `"Hash Batches"` > 1
- Memory: `temp_blks_written` × 8KB
- Estimates: `"Plan Rows"` / `"Actual Rows"` ratio per node
- Pruning: `"Rows Removed by Filter"` relative to `"Actual Rows"`

**Snowflake:** Parse Query Profile JSON from `GET_QUERY_OPERATOR_STATS()`.
- Spill: `bytes_spilled_remote_storage`, `bytes_spilled_local_storage`
- Pruning: `partitions_scanned` / `partitions_total` per TableScan
- Memory: infer from spill (no direct peak memory in standard profile)
- QAS: check `query_acceleration_bytes_scanned`

These are deterministic scripts per engine. Build alongside each engine pack.

---

## 7. Output Contract

### 7.1 Per-Query Output

```yaml
query_id: string
original_sql: string
optimized_sql: string
config_changes: [string]

diagnosis:
  bottleneck: string              # from universal taxonomy
  evidence: string
  engine_feature: string          # (optional)

optimization:
  technique: string
  description: string
  
expected_impact:
  metric: string
  before: string
  after: string
  confidence: string              # "high" | "medium" | "low"

validation:
  equivalence: string             # "verified" | "needs_manual_check"
  regression_check: string
  benchmark_result:               # (optional)
    latency_before: string
    latency_after: string
    spill_before: string
    spill_after: string
    fits_scenario: boolean
```

### 7.2 Workload Scorecard

```yaml
workload_id: string
client: string
date: string

sizing:
  original_warehouse: string
  target_warehouse: string
  achieved_warehouse: string      # smallest where all queries pass

fleet_actions:
  - action: string
    queries_affected: number
    impact: string

query_results:
  total_queries: number
  skipped: number
  tier1_fixed: number
  tier2_fixed: number
  tier3_fixed: number
  residual_failures: number

  per_query: [per-query output contracts]

business_case:
  original_cost_monthly: string
  optimized_cost_monthly: string
  savings_monthly: string
  savings_annual: string
  methodology: string
```

---

## 8. Workload Mode — Detailed Specification

### 8.1 Input Requirements

```
REQUIRED:
  - All SQL queries in the workload
  - Current warehouse/instance size
  
PREFERRED (significantly improves triage):
  - Execution logs from current warehouse (profiles, stats)
  - Query frequency data (executions per day/week)
  - Cost data (credits/compute per query)
  
MINIMUM VIABLE:
  - All SQL queries
  - Target warehouse size to attempt
  (system runs queries on target and collects its own profiles)
```

### 8.2 Triage Scoring

```yaml
pain_score:
  timeout_or_crash:          10
  spills_to_disk_remote:     7
  exceeds_memory_budget:     5
  slow_but_completes:        2
  already_meets_sla:         0

frequency_score:
  1000_plus_per_day:         10
  100_to_999_per_day:        5
  10_to_99_per_day:          2
  under_10_per_day:          1
  unknown:                   3   # default

tractability_score:
  direct_gold_match:         3
  known_blind_spot:          2
  complex_novel:             1
  already_optimized:         0

priority: pain × frequency × tractability
# Range: 0 — 300
```

### 8.3 Classification Rules

```yaml
classification:
  priority_0:          "SKIP"
  priority_1_to_15:    "TIER 2 (light optimization)"
  priority_16_plus:    "TIER 3 (deep optimization)"

overrides:
  - "Timeout + frequency > 100/day → TIER 3 regardless"
  - "Query in 5+ variants → flag for TIER 1 fleet analysis"
  - "Already meets SLA on target → SKIP (include in scorecard)"
```

### 8.3.1 Quick-Win Fast Path

Before entering the standard triage → tier pipeline, identify quick-win queries:

```yaml
quick_win_detection:
  criteria:
    - pain_score >= 8  # timeout or remote spill
    - frequency in top 3 of workload
    - combined pain × frequency >= 80% of total workload pain

  action: |
    Route these queries directly to Tier 3 (deep pipeline).
    Skip Tier 2 light pass — these queries justify full investment.
    Run in parallel with fleet-level Tier 1 analysis.

  rationale: |
    In most workloads, 3-5 queries account for >80% of compute cost.
    Spending 50K tokens on each (150-250K total) is justified when
    the alternative is iterating through Tier 2 → escalate → Tier 3.
```

### 8.4 Tier 1: Fleet-Level Actions

Analyze all queries together. Detect shared patterns:

```yaml
fleet_patterns:
  shared_scans:
    detect: "N queries scan table T with filter on column C"
    action: "Single index / clustering key on C"
    value: "One change, N queries improved"

  shared_subexpressions:
    detect: "N queries compute same aggregation subquery"
    action: "Materialized view for shared computation"
    value: "Eliminates redundant computation across N queries"

  config_opportunities:
    detect: "N queries show same resource bottleneck"
    action: "Single config change (work_mem, QAS, etc.)"
    value: "One setting, N queries improved"

  statistics_staleness:
    detect: "Estimates off by >10x for table T across N queries"
    action: "ANALYZE / statistics refresh"
    value: "Corrects plans for all N queries touching T"
```

**After tier 1: re-benchmark everything, re-triage.**

### 8.5 Tier 2: Light Per-Query Optimization

Single-pass analyst. No 4-worker fan-out. ~5K tokens per query.

```yaml
tier2_decision_tree:
  a_already_passes:    "SKIP, confirm in scorecard"
  b_direct_gold_match: "Apply gold example rewrite, validate, done"
  c_single_transform:  "Produce 1 rewrite, validate, done"
  d_needs_deeper:      "ESCALATE to tier 3"
```

### 8.6 Tier 3: Deep Per-Query Optimization

Full pipeline: analyst → W1-W4 → validate → compress → sniper → config boost → benchmark.

Reserved for 10-20% hardest queries.

### 8.6.1 Tier 3 Failure Escalation

When Tier 3 deep optimization fails to produce a passing rewrite:

```yaml
escalation_levels:
  level_1_constraint_feedback:
    trigger: "All 4 workers + sniper fail validation"
    action: |
      Feed validation failure details back to analyst.
      Re-run with relaxed constraints (e.g., allow 10% more memory).
      Try alternative hypothesis from discovery mode.
    token_cost: "~50K additional"

  level_2_human_escalation:
    trigger: "Level 1 retry also fails"
    action: |
      Flag query for human review.
      Include: diagnosis, attempted rewrites, failure reasons.
      Suggest: physical design changes (index, MV, clustering).
    output: "Residual with diagnosis + recommended infra changes"

  level_3_accept_and_recommend:
    trigger: "Query is fundamentally compute-bound"
    action: |
      Accept that this query cannot fit the target envelope.
      Recommend: keep on current size OR specific infra upgrade.
      Include in scorecard as explained residual.
    output: "Residual with business justification for larger compute"
```

### 8.7 Iterative Downsizing Loop

```
1. target = one_size_down(current)
2. Run all queries on target → collect profiles
3. Triage → Tier 1 → re-benchmark → re-triage
4. Tier 2 on light queries → re-benchmark
5. Tier 3 on hard queries → re-benchmark
6. Calculate pass rate:
   - 100% → go to step 7 (try smaller)
   - 95-99% → report, flag residuals, client decides
   - <95% → stop, this is the floor
7. target = one_size_down(target)
8. Repeat from step 2

STOP when:
  - Pass rate below threshold
  - Smallest available size reached
  - Client-specified floor reached

OUTPUT: "Workload fits on [smallest_viable]. Savings: $X/month."
```

### 8.8 Token Budget (100-query workload)

```
Triage + fleet:   ~100K tokens (fleet-level pattern detection uses LLM)
Tier 1 apply:     0 (applying fleet actions is deterministic)
Re-triage:        0 (re-scoring is deterministic)
Tier 2 (~65 Qs):  ~125K tokens (25 rewrites × 5K, 40 skips)
Tier 3 (~15 Qs):  ~750K tokens (15 × 50K)
Config boost:     0 (deterministic)

TOTAL PER SIZE:   ~975K tokens
TWO ITERATIONS:   ~1.95M tokens

vs NAIVE:         ~5.0M tokens (every query × full pipeline)
SAVINGS:          60-80% with workload-aware triage
```

---

## 9. Pattern Library

Cross-engine collection of anti-patterns and canonical rewrites.

### 9.1 Pattern Schema

```yaml
name: string
family: string                    # rewrite family (A, B, C, D, etc.)
anti_pattern: string
canonical_rewrite: string
engine_tags: [string]             # "all" or specific engines
scenario_tags: [string]           # scenarios that amplify this pattern

motivation_variants:
  - engine: string
    motivation: string            # WHY this helps on this engine
    detection: string             # engine-specific signal

gold_examples:
  - engine: string
    before_sql: string
    after_sql: string
    evidence: string
    improvement: string
```

### 9.2 Example: OR to UNION ALL

```yaml
name: or_to_union_all
family: D
anti_pattern: "OR predicates across different columns prevent index/partition usage"
canonical_rewrite: "Split OR branches into UNION ALL, each with single predicate"
engine_tags: ["all"]
scenario_tags: ["tiny_memory"]

motivation_variants:
  - engine: postgres
    motivation: "BitmapOr across indexes is often slower than separate index scans"
    detection: "BitmapOr node in plan with high cost; OR across indexed columns"
    
  - engine: snowflake
    motivation: "Cannot prune micro-partitions through OR across different columns"
    detection: "Partitions Scanned > 5% of total; OR predicate in WHERE"

gold_examples:
  - engine: postgres
    before_sql: |
      SELECT * FROM orders 
      WHERE customer_id = 42 OR product_id = 99
    after_sql: |
      SELECT * FROM orders WHERE customer_id = 42
      UNION ALL
      SELECT * FROM orders WHERE product_id = 99
    evidence: "BitmapOr on idx_customer + idx_product, cost 45,000"
    improvement: "Two Index Scans, cost 1,200 total"

  - engine: snowflake
    before_sql: |
      SELECT * FROM store_sales
      WHERE ss_item_sk = 42 OR ss_customer_sk = 99
    after_sql: |
      SELECT * FROM store_sales WHERE ss_item_sk = 42
      UNION ALL
      SELECT * FROM store_sales WHERE ss_customer_sk = 99
    evidence: "47% partitions scanned (5,800 of 12,400)"
    improvement: "3% partitions scanned per branch (< 400 total)"
```

---

## 10. Orchestrator Specification

### 10.1 Single-Query Responsibilities

```
1. LOAD: universal_doctrine + engine_pack + scenario_card + pattern_library
2. PRE-COMPUTE: run extraction script → evidence bundle
3. COMPOSE: system prompt = doctrine + pack + card + contract
4. BRIEF ANALYST: system prompt + evidence + gold examples → diagnosis + 4 strategies
5. FAN OUT: 4 workers in parallel, each gets system prompt + evidence + strategy
6. VALIDATE: equivalence + regression + scenario fit per worker
7. COMPRESS: deduplicate, rank by impact × confidence × invasiveness
8. SNIPER: produce final rewrite (may combine)
9. CONFIG BOOST: deterministic engine rules
10. BENCHMARK: original vs optimized on target, verdict
11. OUTPUT: per-query contract
```

### 10.2 Workload Responsibilities

```
1. LOAD all queries + metadata
2. INITIAL BENCHMARK (if no client logs): run on target, collect profiles
3. PRE-COMPUTE: batch extract evidence for all queries
4. TRIAGE: score, classify, detect fleet patterns
5. TIER 1: fleet actions → re-benchmark → re-triage
6. TIER 2: batch light optimization
7. TIER 3: per-query full pipeline (parallelizable across queries)
8. AGGREGATE: compile scorecard + business case
9. ITERATE DOWN: try smaller size, repeat from step 2
10. REPORT: smallest viable warehouse + all rewrites + savings
```

### 10.3 Compress Scoring Rubric

Candidates are ranked by a composite score:

```yaml
compress_score: impact × confidence × invasiveness
# Range: 1-125 (5 × 5 × 5)

impact:  # 1-5, from validation results
  5: ">2x speedup validated"
  4: "1.5-2x speedup validated"
  3: "1.2-1.5x speedup validated"
  2: "1.1-1.2x speedup validated"
  1: "<1.1x or estimate-only"

confidence:  # 1-5, from validation tier
  5: "Race-validated (simultaneous execution)"
  4: "Sequential 3-run validated"
  3: "Single-run validated"
  2: "Cost-estimate only"
  1: "No validation (parse-only)"

invasiveness:  # 5=least invasive (best), 1=most invasive
  5: "Query rewrite only (no config changes)"
  4: "Query rewrite + SET LOCAL config"
  3: "Query rewrite + statistics refresh"
  2: "Query rewrite + index creation"
  1: "Query rewrite + schema changes"
```

**Deduplication:** Before scoring, candidates are AST-normalized via `sqlglot.parse_one().sql()`. Candidates that produce identical normalized SQL are deduplicated (keep highest-scoring).

**Tie-breaking:** When scores are equal, prefer the candidate with fewer transforms (simpler rewrite).

---

## 11. Analyst Prompt Structure

### 11.1 Sections Injected by Orchestrator

```
§I. GOLD EXAMPLES
   From pattern library, filtered by engine tag.
   Before/after SQL pairs with evidence.

   Selection logic:
     1. Filter patterns by engine tag
     2. Score each pattern:
        - Bottleneck match (evidence bundle → pattern.anti_pattern): +3
        - Signal match (runtime profile → pattern.detection): +2
        - Scenario match (scenario card → pattern.scenario_tags): +1
     3. Select top 5 by score
     4. Fallback: if <3 matches, include 3 most common patterns for engine

§II. THE CASE
   §II.A: Original SQL
   §II.B: Evidence Bundle
     - Cost Spine
     - Runtime Profile (spill, pruning, memory, service eligibility)

§III. THIS ENGINE
   From engine pack:
   - HANDLES WELL (table)
   - BLIND SPOTS (table)
   From scenario card:
   - RUNTIME CONSTRAINTS (table)
   From engine pack capabilities:
   - AVAILABLE ACCELERATORS (table)

§IV. OUTPUT CONTRACT
```

### 11.2 Analyst Steps

```
Step 1: Map query to OPTIMAL abstract plan
Step 2: Map ACTUAL plan from evidence
Step 3: Identify DIVERGENCE
  - Which bottleneck taxonomy label?
  - Which engine blind spot?
  - How many excess rows/bytes?
  - Runtime profile: spill? pruning? memory? accelerator eligibility?
Step 4: Design 4 DIVERSIFIED strategies
  - All must respect scenario card constraints
  - W4 targets runtime-motivated rewrite when runtime is primary bottleneck
```

---

## 12. Implementation Phases

### Phase 1: Core Pipeline (Single-Query, PostgreSQL)

```
1. Universal doctrine document
2. Evidence bundle extractor for PostgreSQL
3. Engine pack: postgres_17.yaml
4. Scenario card: postgres_small_instance.yaml
5. Analyst prompt template
6. Worker prompt template
7. Validate / Compress / Sniper stages
8. Config boost rules (PostgreSQL)
9. Benchmark harness
10. Output contract renderer
```

### Phase 2: Workload Mode

```
1. Triage scorer
2. Fleet-level pattern detector
3. Tier 2 light analyst prompt
4. Workload orchestrator (iterative downsizing)
5. Scorecard renderer
6. Business case calculator
```

### Phase 3: Multi-Engine

```
1. Engine pack: snowflake_2025.yaml
2. Evidence extractor for Snowflake
3. Config boost rules (Snowflake)
4. Scenario cards per warehouse size
5. Additional engine packs as needed
```

### Phase 4: Paper

```
1. TPC-DS on PostgreSQL, smallest passing instance
2. Run workload mode → optimize → downsize
3. Re-benchmark on smaller instance
4. Calculate savings at production scale
5. Publish: methodology, results, reproducibility
```

---

## Appendix A: Capability Matrix

```
| Capability            | PostgreSQL 17 | Snowflake 2025 | DuckDB 1.2 |
|-----------------------|---------------|----------------|-------------|
| Scan offload          | —             | QAS            | —           |
| Search optimization   | GIN/GiST     | SOS            | —           |
| Auto indexing         | —             | Optima         | ART (auto)  |
| CTE materialization   | Configurable  | Physical       | Optimizer   |
| Partition pruning     | Declarative   | Micro-partition| Hive-style  |
| Auto clustering       | —             | Yes (service)  | —           |
| Incremental MV        | —             | Yes            | —           |
| Dynamic tables        | —             | Yes (2025)     | —           |
| Parallel execution    | Configurable  | Warehouse-sized| Auto        |
| Stats refresh         | ANALYZE       | ANALYZE TABLE  | Auto        |
| Join order hints      | collapse_limit| FORCE_JOIN_ORDER| —          |
| Session config boost  | SET work_mem  | SET QAS, etc.  | SET memory  |
```

---

## Appendix B: Glossary

| Term | Definition |
|------|-----------|
| **Engine Pack** | Declarative YAML describing engine capabilities, profile signals, rewrite playbook, config rules. One per engine, versioned. |
| **Scenario Card** | Resource envelope + failure definitions + strategy priorities. Engine-agnostic, reusable. |
| **Universal Doctrine** | Shared optimization principles, bottleneck taxonomy, hallucination rules. Stable across engines. |
| **Evidence Bundle** | Pre-computed structured evidence from query profile. Cost spine + runtime profile. Per-query. |
| **Output Contract** | Required output format. Bottleneck → fix → why → validate. |
| **Pattern Library** | Cross-engine anti-patterns and rewrites with engine-specific motivations and gold examples. |
| **Fleet Actions** | Multi-query optimizations: config, indexes, clustering, MVs, services. Tier 1 in workload mode. |
| **Config Boost** | Deterministic post-sniper session configuration. No LLM. |
| **Triage Score** | `pain × frequency × tractability`. Routes queries to tier 2 (light) or tier 3 (deep). |
| **Workload Scorecard** | Final deliverable: per-query results + fleet actions + business case + smallest viable warehouse. |

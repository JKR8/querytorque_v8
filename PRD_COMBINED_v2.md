# QueryTorque for SQL — Combined PRD v2.0

## VS Code Extension + Beam Optimization Engine + Validator Architecture

### February 2026 | Confidential

---

## 1. Executive Summary

QueryTorque for SQL is a VS Code extension backed by an AI-powered SQL rewriting engine that detects anti-patterns, recommends configuration changes, and rewrites queries for measurably faster execution — validated against the user's live database.

**What makes it different from every other SQL tool:**

1. **It actually rewrites SQL and proves it's faster.** Not advice. Not "consider adding an index." A rewritten query, validated to return identical results, benchmarked to run faster, written back to your file.

2. **8 parallel AI workers attack every query from different angles.** Fan-out architecture: an analyst LLM reasons about the execution plan, dispatches 8 specialist workers that each try a different optimisation strategy, a validator LLM proves semantic equivalence via column lineage, then survivors race against the original on the live database.

3. **Free tier is genuinely useful.** Static anti-pattern detection (30+ patterns), EXPLAIN-driven PostgreSQL config recommendations you can copy-paste and test immediately (`SET LOCAL work_mem = '512MB'`), and 3 free beam optimisations per month. You don't pay until you've seen the engine work.

4. **FinOps lives in Fleet Control.** The extension is where developers optimise queries. The web dashboard is where engineering managers see aggregate cost, track savings, and approve gainshare contracts. Two products, one pipeline.

**Architecture:** TypeScript extension handles file scanning, pattern detection, and UX. Python beam engine (battle-tested on 88 TPC-DS + 76 DSB queries) handles the heavy optimisation via REST API. No sidecar binary. No Docker. Install the extension, connect your database, go.

---

## 2. The Product Loop

### 2.1 The Five Steps

```
1. DETECT    ──→  Scan .sql files, detect patterns, show Torque Score
2. DIAGNOSE  ──→  Connect to DB, run EXPLAIN, recommend SET LOCAL configs
3. REWRITE   ──→  8-beam AI fan-out generates optimised candidates
4. VALIDATE  ──→  Validator LLM proves equivalence, then benchmark on live DB
5. APPLY     ──→  Write optimised SQL back to source file
```

Steps 1–2 are **free**. Step 3 costs a beam credit (3 free/month, then paid). Steps 4–5 execute automatically after rewrite.

### 2.2 What the User Sees

**Free (no beam credit):**
```
⚠ QS-002: Correlated subquery detected (line 14)
  This pattern forces row-by-row execution. Decorrelation typically yields 2-10x speedup.

  💡 Config recommendation (test for free):
     SET LOCAL work_mem = '256MB';    -- Hash spill detected (Batches=4)
     SET LOCAL jit = off;             -- JIT overhead 120ms on 340ms query

  ⚡ Run beam optimisation (2 credits remaining this month)
```

**After beam (1 credit):**
```
✅ Beam complete: 8 candidates generated, 5 validated equivalent, 3 benchmarked
   Best: Worker 3 (decorrelate + early_filter) — 2.45x faster
   Original: 4,200ms → Optimised: 1,714ms (validated on 10,000 rows, checksums match)

   [Apply to file]  [View diff]  [View all candidates]  [Reject]
```

---

## 3. Architecture

### 3.1 System Diagram

```
┌──────────────────────────────────────────────────────────┐
│  VS Code Extension (TypeScript)                           │
│                                                           │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────┐  │
│  │ SQL Scanner   │  │ Pattern      │  │ UX Layer       │  │
│  │ (.sql, dbt,   │  │ Detector     │  │ (CodeLens,     │  │
│  │  ORM, inline) │  │ (30+ rules,  │  │  Diagnostics,  │  │
│  │              │  │  AST-based)  │  │  Panels, Diff) │  │
│  └──────┬───────┘  └──────┬───────┘  └────────────────┘  │
│         │                 │                                │
│  ┌──────┴─────────────────┴──────┐                        │
│  │  Database Connector Layer      │                        │
│  │  (pg, mysql2, snowflake-sdk,  │                        │
│  │   duckdb-node, databricks)    │                        │
│  └──────────────┬────────────────┘                        │
│                 │                                          │
│  ┌──────────────┴────────────────┐                        │
│  │  QueryTorque API Client       │                        │
│  │  (Beam requests, Fleet sync)  │                        │
│  └───────────────────────────────┘                        │
└──────────┬──────────────────────────┬─────────────────────┘
           │                          │
           ▼                          ▼
     ┌───────────┐            ┌──────────────────────┐
     │  User's   │            │  QueryTorque API     │
     │  Database │            │                      │
     │  (live)   │            │  Analyst LLM         │
     └───────────┘            │  8× Worker LLMs      │
                              │  1× Validator LLM    │
                              │  Fleet Control API   │
                              └──────────────────────┘
```

### 3.2 What Runs Where

| Component | Runs In | Language | Why |
|-----------|---------|----------|-----|
| File scanning + pattern detection | Extension | TypeScript | Must be instant, offline, no network |
| Database connection + EXPLAIN | Extension | TypeScript (native drivers) | Direct connection, no proxy |
| Config recommendations | Extension | TypeScript | Rule-based, deterministic, free |
| Beam optimisation (analyst + 8 workers) | QT API | Python | 30K+ lines of battle-tested prompts, knowledge, examples |
| Validator LLM | QT API | Python | Needs full context of all candidates |
| Result validation (checksums, benchmarks) | Extension + DB | TypeScript + SQL | Runs against user's DB directly |
| Write-back | Extension | TypeScript | Modifies user's source files |
| Fleet Control dashboard | Web app | React | Separate product for FinOps buyers |

### 3.3 Why Not Pure TypeScript?

The beam engine is 30,000+ lines of Python containing:
- 30 transform definitions with contraindications and regression registries
- 10 engine pathologies per database with field-tested detection logic
- 30 gold examples with verified speedups (up to 8,044x on PostgreSQL)
- 3-tier semantic validation with TABLESAMPLE execution
- Race validation with barrier synchronisation
- EXPLAIN-first reasoning with hypothesis-driven worker dispatch
- Per-worker SET LOCAL tuning with whitelist validation

Rewriting this in TypeScript would take 6+ months and lose battle-tested correctness. The PRD v1 already has `QueryTorque API Client` in its architecture — we're using it.

---

## 4. Free Tier: Pattern Detection + Config Recommendations

### 4.1 Philosophy

The free tier must be **genuinely useful** — not a demo, not a tease. A developer should install QueryTorque, connect their database, and get actionable advice they can test in 30 seconds without paying anything.

**Free gives you two things:**

1. **"We detected a pattern in this query that will likely make it run faster when fixed."** — Static AST analysis, no LLM needed, instant results.

2. **"Here are PostgreSQL config settings you can test right now."** — EXPLAIN-driven SET LOCAL recommendations. Copy-paste into psql, run your query, see the difference.

The beam engine (AI rewriting) is the paid product. But the free tier proves we understand your queries and your database before asking for money.

### 4.2 Anti-Pattern Library (30+ Static Rules)

Detected via SQL AST parsing. No database connection required for detection (connection enriches with EXPLAIN data).

#### Category 1: Join and Relationship Issues

| ID | Pattern | Detection | Free Advice |
|----|---------|-----------|-------------|
| JN-001 | Cartesian join (missing predicate) | EXPLAIN shows Nested Loop with no condition | "Missing join predicate — row explosion likely" |
| JN-002 | Comma join syntax | `FROM a, b WHERE` pattern in AST | "Implicit join syntax hides missing predicates" |
| JN-003 | Join on non-indexed column | EXPLAIN shows Seq Scan on join column | "Consider: `CREATE INDEX idx_{table}_{col} ON {table}({col})`" |
| JN-004 | Join type mismatch | Column types differ across join | "Implicit cast prevents index use" |
| JN-005 | DISTINCT hiding bad joins | DISTINCT on query with duplicate-producing join | "DISTINCT is masking a join problem" |
| JN-006 | Left join with right-side WHERE filter | Filter on left-joined table in WHERE vs ON | "This silently converts to inner join" |

#### Category 2: Query Structure (Rewrite Opportunities)

| ID | Pattern | Detection | What Beam Can Do |
|----|---------|-----------|-----------------|
| QS-001 | Correlated subquery | Outer reference in subquery AST | Decorrelate → 2-10x typical |
| QS-002 | Repeated table scans | Same table appears 3+ times | CTE consolidation → 1.5-6x typical |
| QS-003 | Cross-column OR branches | OR across different columns | OR-to-UNION decomposition → 1.3-5x typical |
| QS-004 | Unfiltered CTE | CTE without WHERE clause | Push predicates into CTE → 1.2-3x typical |
| QS-005 | Star join without early filter | Fact+dimension without dimension prefilter | Early dimension filter → 1.5-4x typical |
| QS-006 | SELECT * in production code | `SELECT *` in non-exploratory context | Column pruning |
| QS-007 | UNION instead of UNION ALL | UNION without dedup requirement | Remove unnecessary sort+dedup |
| QS-008 | Nested subqueries >2 levels deep | Subquery nesting depth analysis | Flatten to CTEs or joins |

#### Category 3: Cloud Cost Patterns (Snowflake / Databricks)

| ID | Pattern | Detection | Impact |
|----|---------|-----------|--------|
| CC-001 | Full scan on large warehouse | Query history shows >1TB scan | $5+ per execution |
| CC-002 | Warehouse auto-suspend too long | Idle minutes > credits consumed | Idle credit burn |
| CC-003 | Missing clustering key | Large table, frequent range filters | Excessive micro-partition scanning |
| CC-004 | ORDER BY on large result set | ORDER BY returning >100K rows | Spill-to-disk, credit burn |
| CC-005 | Repeated identical queries | Same hash >10x/hour without caching | Wasted compute |

#### Category 4: ORM-Specific Patterns

| ID | Pattern | Detection | ORM Fix |
|----|---------|-----------|---------|
| ORM-001 | N+1 query (loop-detected) | Same template executed >10x | `.select_related()` / `.includes()` |
| ORM-002 | Missing eager load | FK access triggers lazy load | `.prefetch_related()` / `.eager_load()` |
| ORM-003 | Unfiltered `.all()` on large table | ORM fetches entire table | Add `.filter()` / `.where()` |

### 4.3 Free Config Recommendations (PostgreSQL)

When the user connects their database and we run EXPLAIN ANALYZE, we can offer **6 deterministic config recommendations** with zero LLM cost:

#### Rule 1: work_mem Sizing
```
Trigger: EXPLAIN shows hash spill (Batches > 1) or peak memory >= 50% of work_mem
Advice:  SET LOCAL work_mem = '{peak × 4, capped at 2048}MB';
Why:     "Hash operation spilling to disk (4 batches). Increasing work_mem
          keeps the hash table in memory. This is per-operation, not per-query."
```

#### Rule 2: Disable Nested Loops on Large Scans
```
Trigger: EXPLAIN shows Nested Loop with actual rows > 10,000
Advice:  SET LOCAL enable_nestloop = off;
Why:     "Nested Loop scanning {rows} rows. Hash Join or Merge Join will
          likely be faster at this cardinality."
```

#### Rule 3: Enable Parallelism
```
Trigger: No parallel nodes in plan + Sequential Scan on table with > 100K rows
Advice:  SET LOCAL max_parallel_workers_per_gather = 4;
Why:     "Sequential scan on {table} ({rows} rows) with no parallelism.
          Parallel workers can divide this scan."
```

#### Rule 4: Disable JIT on Short Queries
```
Trigger: JIT active + total execution < 500ms
Advice:  SET LOCAL jit = off;
Why:     "JIT compilation adding {jit_ms}ms overhead to a {total_ms}ms query.
          JIT helps queries >1s, hurts short ones."
```

#### Rule 5: Favour Index Scans (SSD Hint)
```
Trigger: Sequential scan on large table + storage detected as SSD
Advice:  SET LOCAL random_page_cost = 1.1;
Why:     "Random I/O on SSD is nearly as fast as sequential. Lowering
          random_page_cost makes the planner prefer index scans."
```

#### Rule 6: Increase Join Collapse Limit
```
Trigger: > 6 join nodes in EXPLAIN plan
Advice:  SET LOCAL join_collapse_limit = 12;
Why:     "Query has {count} joins. Default limit (8) may prevent the planner
          from finding the optimal join order."
```

**These are safe.** All use SET LOCAL (transaction-scoped, auto-reverts). All are whitelisted against our 16-parameter safety list. The user can copy-paste into psql and test immediately.

### 4.4 Torque Score

Every detected query gets a 0-100 score:

```
Torque Score = (Pattern Severity × 0.2) + (Execution Cost × 0.4)
             + (Scan Efficiency × 0.2) + (Frequency × 0.2)
```

- **Execution Cost**: From EXPLAIN ANALYZE (time-based for PG/MySQL) or credits (Snowflake/Databricks)
- **Scan Efficiency**: Rows examined vs rows returned. Ratio >100:1 = strong signal.
- **Frequency**: From `pg_stat_statements` or query history. A 2ms waste running 10,000x/day = worth fixing.

Prioritisation:
- **Fix Now (Red):** Score > 80 or cost >$100/month
- **Fix Soon (Amber):** Score 50-80 or cost $10-100/month
- **Improve (Blue):** Score < 50, best-practice violations

---

## 5. Paid Tier: Beam Optimisation Engine

### 5.1 What a Beam Does

A "beam" is one complete optimisation session for one SQL query. It is the core paid product.

```
                    ┌─────────────┐
                    │  Your SQL   │
                    │  + EXPLAIN  │
                    │  + Schema   │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  ANALYST    │  ← 1 LLM call (DeepSeek R1)
                    │  Reasons    │    Reads EXPLAIN plan, identifies bottlenecks,
                    │  about plan │    matches against 10 known engine pathologies,
                    │             │    dispatches worker strategies
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │            │            │         (8 parallel workers)
         ┌────▼───┐  ┌────▼───┐  ┌────▼───┐
         │ Worker │  │ Worker │  │ Worker │  ...  × 8
         │  Qwen  │  │  Qwen  │  │  Qwen  │
         │   1    │  │   2    │  │   3    │
         └────┬───┘  └────┬───┘  └────┬───┘
              │            │            │
              └────────────┼────────────┘
                           │
                    ┌──────▼──────┐
                    │  GATE 1     │  ← Parse check (instant, no LLM)
                    │  SQLGlot    │    Rejects syntax errors
                    │  Parse      │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  VALIDATOR  │  ← 1 LLM call (Qwen 72B)
                    │  Qwen       │    Proves semantic equivalence via
                    │  (NEW)      │    column lineage tracing
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  BENCHMARK  │  ← Database execution (no LLM)
                    │  Race or    │    Row count + MD5 checksum + timing
                    │  3-run      │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  RESULT     │
                    │  Best SQL   │
                    │  + Speedup  │
                    └─────────────┘
```

**Total LLM calls per beam:** 10 typical (1 analyst + 8 workers + 1 validator)
**Total cost per beam:** ~$0.02-0.08 depending on query complexity and provider pricing

### 5.2 Worker Strategy Allocation

Each of the 8 workers attacks the query from a different angle, based on the analyst's bottleneck hypothesis:

| Worker | Strategy Family | What It Tries | Gold Example Speedup |
|--------|----------------|---------------|---------------------|
| W1 | Early Filtering (A) | Push dimension filters into CTEs, add prefilters | 27.80x (PG Q001) |
| W2 | Decorrelation (B) | Convert correlated subqueries to JOINs or CTEs | 1,465x (PG Q032) |
| W3 | Aggregation (C) | Consolidate repeated scans, single-pass aggregation | 5.25x (DuckDB Q88) |
| W4 | Set Operations (D) | OR→UNION decomposition, INTERSECT→EXISTS | 1.78x (DuckDB Q35) |
| W5 | Materialization (E) | CTE materialization, dimension prefetch | 3.23x (DuckDB Q46) |
| W6 | Join Transform (F) | Join reordering, predicate pushdown | 8.56x (PG explicit_join) |
| W7 | Config + Rewrite | SET LOCAL tuning combined with SQL rewrite | 2.22x (PG Q102 with pg_hint_plan) |
| W8 | Exploration | Novel strategies, compound transforms, what_didnt_work patterns | 4.47x (DuckDB Q9) |

Workers 1-6 are assigned based on detected patterns. Worker 7 always tries config tuning. Worker 8 always explores.

### 5.3 Engine Knowledge

The beam engine carries battle-tested knowledge per database:

| Database | Pathologies | Gold Examples | Transforms | Verified Speedups |
|----------|------------|---------------|------------|-------------------|
| PostgreSQL | 7 (P1-P7) | 14 | 12 | Up to 8,044x (Q092 shared_scan_decorrelate) |
| DuckDB | 10 (P0-P9) | 16 + 10 regressions | 15 | Up to 6.28x (Q88 or_to_union) |
| Snowflake | 9 hypothesised (H1-H9) | 2 verified | 8 | Up to 23.17x (inline_decorrelate) |
| MySQL | Planned | — | — | — |
| Databricks | Planned | — | — | — |

Each pathology includes: detection criteria, what works, what doesn't work (with regression data), contraindications, and field notes from real benchmark runs.

---

## 6. The Validator LLM (New — 9th Model)

### 6.1 Why We Need It

Current validation catches most errors but has blind spots:

| Error Type | Current Gate | Caught? | Example |
|-----------|-------------|---------|---------|
| Syntax error | Gate 1 (SQLGlot parse) | Yes | Missing comma |
| Wrong row count | Benchmark (execution) | Yes | Missing WHERE clause |
| Wrong values | Benchmark (MD5 checksum) | Yes | SUM vs COUNT swap |
| Column reordering | Checksum (order-independent) | **No** | SELECT a,c,b vs a,b,c |
| LEFT→INNER conversion | Checksum (if no NULLs in sample) | **Sometimes** | WHERE on right table |
| Subtle filter change | Checksum (if boundary rows absent) | **Sometimes** | > vs >= on edge values |
| NULL handling change | Checksum (if NULLs absent in data) | **Sometimes** | COALESCE vs CASE |
| Aggregate scope change | Checksum (if groups happen to match) | **Sometimes** | Different GROUP BY |

The validator catches the "sometimes" cases **before** we waste benchmark time. It's cheaper to spend $0.002 on an LLM call than $0.50 on benchmark execution that produces a false positive.

### 6.2 Validator Architecture

**Model:** Qwen-2.5-72B-Instruct (or Qwen-2.5-32B for cost reduction)

**Why Qwen, not DeepSeek R1:**
- Validator doesn't need creative reasoning. It needs systematic verification.
- Qwen is cheaper per token, faster response time.
- The task is structured analysis (column lineage), not open-ended generation.
- DeepSeek R1 stays as the analyst (where creative reasoning matters).

**Input (single prompt, all candidates):**

```
You are a SQL equivalence validator. Your job is to PROVE or DISPROVE
that each candidate query produces identical results to the original.

## Original Query
{original_sql}

## Table Schemas
{schema_info}

## Candidates
### Candidate 1 (Worker: decorrelate)
{candidate_1_sql}

### Candidate 2 (Worker: early_filter)
{candidate_2_sql}

... (up to 8 candidates)

## Validation Rubric

For EACH candidate, you MUST produce:

1. COLUMN LINEAGE PROOF
   Trace each output column from source table → transformations → final output.
   Original:  orders.total → SUM(orders.total) → grouped by region
   Candidate: orders.total → SUM(o.total)      → grouped by region
   Verdict: MATCH ✓

2. JOIN GRAPH EQUIVALENCE
   List all joins in original and candidate. Same tables, same predicates, same types.
   Original:  orders INNER JOIN users ON orders.user_id = users.id
   Candidate: orders JOIN users ON orders.user_id = users.id
   Verdict: MATCH ✓ (JOIN defaults to INNER)

3. FILTER EQUIVALENCE
   List all WHERE/HAVING predicates. Prove logical equivalence.
   Original:  WHERE o.created_at > '2025-01-01' AND u.region = 'APAC'
   Candidate: WHERE u.region = 'APAC' AND o.created_at > '2025-01-01'
   Verdict: MATCH ✓ (AND is commutative)

4. AGGREGATION CHECK
   Same GROUP BY columns? Same aggregate functions on same source columns?

5. ROW ORDERING
   If original has ORDER BY, candidate must have equivalent ORDER BY.
   If original has no ORDER BY, candidate's ORDER BY is acceptable (extra, not wrong).

6. NULL HANDLING
   Does the rewrite change behaviour for NULL values?
   LEFT JOIN → INNER JOIN changes NULL rows.
   COALESCE(x, 0) ≠ x when x is NULL.

## Output Format (JSON)
{
  "candidates": [
    {
      "candidate_id": 1,
      "verdict": "EQUIVALENT",          // EQUIVALENT | NOT_EQUIVALENT | UNCERTAIN
      "confidence": 0.95,               // 0.0-1.0
      "column_lineage": {
        "col1": {"source": "orders.total", "transform": "SUM", "match": true},
        ...
      },
      "join_graph_match": true,
      "filter_match": true,
      "aggregation_match": true,
      "null_handling_safe": true,
      "issues": [],                      // Empty if EQUIVALENT
      "reasoning": "Decorrelation replaces correlated subquery with JOIN.
                    All columns trace to same sources. Join graph adds one
                    explicit JOIN but produces identical rows."
    },
    {
      "candidate_id": 2,
      "verdict": "NOT_EQUIVALENT",
      "confidence": 0.88,
      "issues": [
        "LEFT JOIN on line 4 replaced with INNER JOIN — drops rows where
         users.id IS NULL in original"
      ],
      "reasoning": "..."
    }
  ]
}
```

### 6.3 Validator Integration Point

```
Workers return 8 candidates
    │
    ▼
Gate 1: SQLGlot Parse ──── rejects syntax errors (instant, free)
    │
    ▼ (survivors, typically 6-8)
    │
VALIDATOR LLM ──── proves/disproves equivalence (1 API call, ~$0.002)
    │
    ├── EQUIVALENT (high confidence) ──→ proceed to benchmark
    ├── NOT_EQUIVALENT ──→ reject, include reason in retry prompt
    └── UNCERTAIN ──→ proceed to benchmark (let execution decide)
    │
    ▼ (survivors, typically 3-6)
    │
Benchmark: Row count + MD5 checksum + timing
    │
    ▼
Result: Best candidate with proven speedup
```

### 6.4 What the Validator Catches (Examples)

**Example 1: LEFT → INNER conversion**
```sql
-- Original
SELECT o.id, u.name
FROM orders o
LEFT JOIN users u ON o.user_id = u.id
WHERE u.region = 'APAC'   -- ← This filters NULLs, converting to INNER

-- Candidate (worker removed LEFT, made it INNER)
SELECT o.id, u.name
FROM orders o
JOIN users u ON o.user_id = u.id
WHERE u.region = 'APAC'

-- Validator: "EQUIVALENT — the WHERE u.region = 'APAC' already eliminates
-- NULL rows from the LEFT JOIN, so the LEFT→INNER conversion is safe here."
```

**Example 2: Aggregate scope change**
```sql
-- Original
SELECT department, AVG(salary) FROM employees GROUP BY department

-- Candidate (accidentally added DISTINCT)
SELECT department, AVG(DISTINCT salary) FROM employees GROUP BY department

-- Validator: "NOT_EQUIVALENT — AVG(DISTINCT salary) excludes duplicate
-- salary values. If two employees in the same department earn $50K,
-- original counts both, candidate counts once."
```

**Example 3: Filter boundary shift**
```sql
-- Original
WHERE created_at >= '2025-01-01'

-- Candidate
WHERE created_at > '2024-12-31'

-- Validator: "NOT_EQUIVALENT — if created_at has time component, rows at
-- exactly midnight 2025-01-01 are included by original (>=) but excluded
-- by candidate (> '2024-12-31' = > '2024-12-31 00:00:00'). However, if
-- created_at is DATE type (no time), these are equivalent."
-- Verdict: UNCERTAIN (type-dependent)
```

### 6.5 Validator Cost Model

| Metric | Value |
|--------|-------|
| Input tokens (8 candidates + schema) | ~4,000-8,000 |
| Output tokens (structured JSON) | ~1,500-3,000 |
| Cost per validation (Qwen 72B) | ~$0.002-0.005 |
| Latency | 2-4 seconds |
| False positive rate (wrongly rejects good SQL) | Target: <2% |
| False negative rate (passes bad SQL) | Target: <5% |
| Errors caught before benchmark | Estimated 15-25% of candidates |

**ROI:** Each caught error saves 3-15 seconds of benchmark time and prevents false-positive wins from reaching the user. At $0.003 per validation vs $0.10+ per wasted benchmark, the validator pays for itself 30x over.

---

## 7. Validation Framework (Post-Validator)

### 7.1 Full Validation Pipeline

After the validator LLM approves candidates, execution-based validation runs:

```
EQUIVALENT candidates from validator
    │
    ▼
Step 1: Execute original query (with LIMIT safety, default 10,000 rows)
        Capture: row count, column names, MD5 checksum of normalised sorted rows
    │
    ▼
Step 2: Execute each candidate (same LIMIT)
        Fail-fast: if row count differs on first run, reject immediately
    │
    ▼
Step 3: Compare checksums
        MD5 of normalised values (NULL→"__NULL__", floats→9 decimal places)
        Match → proceed to timing
        Mismatch → compute row-level diff, show to user
    │
    ▼
Step 4: Benchmark timing
        Method A (query >= 2s): Race validation
          - Original + candidates run simultaneously on separate connections
          - threading.Barrier synchronisation for identical start
          - Race clock: original finish + 10% grace
          - Candidates still running = DID_NOT_FINISH (slower by definition)

        Method B (query < 2s): Sequential 5x trimmed mean
          - Run 5 times, discard min and max, average remaining 3
          - More reliable for short queries where variance is high
    │
    ▼
Step 5: Classify result
        >= 1.10x → WIN
        >= 1.05x → IMPROVED
        >= 0.95x → NEUTRAL
        <  0.95x → REGRESSION (rejected)
```

### 7.2 Safety Mechanisms

| Concern | Handling |
|---------|---------|
| Query modifies data | `BEGIN ... ROLLBACK` wrapping. No writes during validation. |
| Query is extremely slow (>30s) | Timeout with progress bar. User can extend or skip. |
| Query returns millions of rows | LIMIT safety cap (configurable, default 10,000). |
| Non-deterministic functions | Pin `NOW()` via SET. Flag `RANDOM()` as non-validatable. |
| Production database | Warn if connection flagged as production. Recommend dev/staging. |
| Cloud credit consumption | Estimate credit cost before execution. Require approval for >$1. |

---

## 8. VS Code Extension UX

### 8.1 Activation

Activates when workspace contains `.sql` files, `dbt_project.yml`, or `.querytorque.yml`. Also activates when any file containing SQL patterns is opened.

### 8.2 Primary Views

**View 1: Database Health Panel (Activity Bar)**

The QueryTorque icon in the activity bar shows:
- Connection status (which databases connected, latency, version)
- Workspace Torque Score (aggregate across all detected SQL)
- Issue tree grouped by priority (Fix Now / Fix Soon / Improve)
- Top Expensive Queries (ranked by $/month or execution time)
- Quick actions: "Scan Workspace", "Connect to Database"

**View 2: Issue Detail Panel**

Clicking an issue shows:
- Plain-language explanation of the anti-pattern
- Current SQL with problematic section highlighted
- EXPLAIN plan visualisation (tree view with node costs)
- Impact estimate: "This query runs 2,400x/day. Each execution: 4.2s, 847K rows scanned."
- **Config recommendations** (free, copy-paste ready)
- **"Run Beam" button** (uses 1 credit)

**View 3: Beam Results Panel (Side-by-Side)**

After beam completion:
- Left: Original SQL with problems highlighted
- Right: Optimised SQL with changes highlighted
- Below: Explanation, confidence level, speedup metrics
- EXPLAIN plan comparison (before/after, side-by-side)
- All 8 worker results (expandable, shows which strategies were tried)
- Validator reasoning (which candidates were rejected and why)
- Action buttons: "Apply to File", "Apply Without Validation", "Reject", "Edit"

**View 4: Query Cost Explorer**

Dedicated view showing most expensive queries across connected databases. Ranked by total monthly cost. Links to source file. Shows issue count and beam availability. This is the discovery view.

### 8.3 Editor Integration

- **Diagnostics:** Squiggly underlines on anti-patterns in `.sql` files and inline SQL
- **CodeLens:** Above each query: `Torque: 72 | $34/mo | 3 issues | Last run: 2.4s`
- **Code Actions:** Lightbulb → "QueryTorque: Fix this query" / "Explain this plan"
- **Status Bar:** Workspace Torque Score + connection indicator + credits remaining
- **Inline annotations:** Per-execution cost and monthly cost at current frequency

### 8.4 Command Palette

```
QueryTorque: Scan Current File
QueryTorque: Scan Workspace
QueryTorque: Connect to Database
QueryTorque: Disconnect
QueryTorque: Show Query Cost Explorer
QueryTorque: Explain Query at Cursor
QueryTorque: Run Beam on Query at Cursor
QueryTorque: Run Beam on All Issues in File
QueryTorque: Show Config Recommendations
QueryTorque: Export Report (HTML / JSON)
QueryTorque: Open Fleet Control Dashboard
```

---

## 9. Pricing and Tiers

### 9.1 Pricing Model

Two completely different models for two completely different buyers. The extension is a developer tool sold per-seat with beam credits. Fleet Control is a FinOps platform sold on value (gainshare or platform fee).

**Competitive context:** EverSQL (acquired by Aiven, 100K+ users, bootstrapped to profitability) charges $99/mo for 10 single-pass optimisations (~$10/optimisation). Our Beam runs 8 parallel workers with a validator LLM — objectively more capable. EverSQL never did gainshare. We have that as an additional lever they never had.

#### Developer Tool (Extension)

| Tier | Price | Beams | Static Analysis | Validation | Buyer |
|------|-------|-------|----------------|------------|-------|
| **Free** | $0 | **3/month** | Full (top 5 issues) | No | Individual dev trying it out |
| **Pro** | **$49/mo** | **50/month** (~$1/beam) | Full (all issues) | Yes | Individual dev who's hooked |
| **Team** | **$199/seat/mo** | **Unlimited** | Full + shared config | Yes | Team lead, 5+ devs |

#### FinOps Platform (Fleet Control)

| Model | Price | What You Get | Buyer |
|-------|-------|-------------|-------|
| **Platform fee** | **$2,500+/mo** per database | Batch beams + dashboard + savings tracking | Orgs that won't do gainshare (procurement complexity) |
| **Gainshare** | **10–15% of verified savings** | Same + aligned incentives | Orgs with large cloud spend ($100K+/mo) |
| **Hybrid** | **$1,500/mo base + 10% gainshare** | Same + floor guarantee for us | Negotiated enterprise deals |

The $2,500/mo platform fee is the **floor**, not the ceiling. That's for a small org with modest cloud spend. The real money is gainshare — 10–15% of verified savings on a $200K/month Snowflake bill is **$20–30K/month** from a single customer.

### 9.2 Feature Matrix

| Feature | Free | Pro ($49/mo) | Team ($199/seat/mo) | Fleet Control |
|---------|------|-------------|--------------------|--------------|
| SQL file scanning | All files | All files | All files | Batch upload |
| Anti-pattern detection | Top 5 issues | All 30+ patterns | All patterns | Org-wide |
| Database connections | 1 | 3 | Unlimited | Unlimited |
| EXPLAIN plan analysis | Yes | Yes | Yes | Yes |
| Config recommendations (SET LOCAL) | Yes | Yes | Yes | Yes |
| Cost estimation | Basic (time only) | Full ($/month + frequency) | Full | Full + historical |
| Torque Score | Yes | Yes | Yes | Org-wide trending |
| **Beam optimisations** | **3/month** | **50/month** | **Unlimited** | **Batch** |
| Automated validation | Yes (on beams) | Yes | Yes | Yes |
| Write-back to source files | Yes (on beams) | Yes | Yes | N/A (report) |
| Credit rollover | No | **Yes (1 month, max 100 banked)** | N/A (unlimited) | N/A |
| ORM fix suggestions | Detection only | Detection + fix | Full | Full |
| dbt integration | File scanning | Full (ref resolution) | Full | Full |
| Query Cost Explorer | Top 5 queries | Full | Full + trending | Org-wide leaderboard |
| Export report | Watermarked | Full HTML/JSON | Full + branding | Full + PDF |
| Fleet Control sync | No | No | Yes | Core product |
| CI/CD (GitHub Action) | Score gate only | Score + issue blocking | Full config | Full + compliance |
| Gainshare tracking | No | No | No | Yes |
| Savings verification | No | No | No | Yes (checksummed) |

### 9.3 Why Each Price Point Works

**$49/mo Pro (~$1/beam) is a steal by comparison.** EverSQL proved $99 for 10 optimisations works — that's ~$10/optimisation. We give 50 beams for $49 — roughly $1/beam. It feels like a steal, but margins are 95%+ because a beam costs ~$0.05 in LLM calls. The lower per-unit cost drives higher usage, which drives more data for Fleet Control. Volume over margin on the developer tool.

**$199/seat/mo Team is justified because unlimited beams changes behaviour.** At Pro, developers think about whether a query is "worth" a beam credit. At Team, they beam everything. More fixes deployed → more savings proven → more data flowing to Fleet Control. The unlimited model creates the usage pattern that makes Fleet Control valuable. Team is the bridge to the enterprise sale.

**Fleet Control at $2,500+/mo is the floor, not the ceiling.** That's for a small org. The gainshare model scales with the customer's database spend:
- $50K/mo Snowflake bill × 20% savings × 15% gainshare = **$1,500/mo**
- $200K/mo Snowflake bill × 20% savings × 15% gainshare = **$6,000/mo**
- $500K/mo Snowflake bill × 15% savings × 12% gainshare = **$9,000/mo**

Give customers the choice: flat platform fee or gainshare. Orgs with budget structure constraints choose the flat fee. Orgs that want zero risk choose gainshare (they pay nothing if we deliver nothing).

### 9.4 Why 3 Free Beams (Not 0, Not 1)

Three beams per month lets a developer experience the full loop:

1. **Beam 1:** See the fan-out in action — 8 candidates generated, validator reasoning, a validated speedup written back to their file. The "holy shit" moment.
2. **Beam 2:** Try it on a different query. Confirm it's not a fluke. See a different set of transforms.
3. **Beam 3:** The query they actually care about. The one from their production dashboard that's been slow for months. This is where the ROI calculation happens: "If this beam saves $340/month on one query, $49/month for Pro is obvious."

Zero free beams means the user never sees what they're paying for. One beam might be a fluke. Three is a pattern. That's enough to hook, not enough to satisfy.

### 9.5 Credit Rollover (Pro Only)

Unused Pro beam credits carry forward **1 month**, with a **max of 100 banked credits**. This addresses the "I'm paying but not using it this month" churn trigger that kills subscription products.

- Month 1: Use 30 of 50 → 20 roll over → Month 2 starts with 70
- Month 2: Use 10 of 70 → 50 roll over (capped from 60) → Month 3 starts with 100
- Month 3: Use 0 of 100 → 50 roll over (cap applies) → Month 4 starts with 100

Team tier doesn't need rollover because it's unlimited. Free tier doesn't need it because 3 is already generous for a trial.

### 9.6 Conversion Triggers

Natural upgrade moments where free hits a wall:

- **4th beam in a month:** "You've used your 3 free beams. This query costs ~$340/month. Upgrade to Pro for 50 beams/month at ~$1 each."
- **6th issue hidden:** "Your workspace has 14 issues. Upgrade to Pro to see all and auto-fix them."
- **2nd database connection:** "Pro supports up to 3 database connections."
- **Export without watermark:** "Upgrade to remove the watermark."
- **Team upsell from Pro:** "Your team has 4 Pro seats. Upgrade to Team for unlimited beams and shared config — saves you $196/mo vs 4× Pro."
- **Fleet Control from Team:** "Your team has deployed 47 fixes saving an estimated $8,200/month. See the full picture in Fleet Control."

### 9.7 Distribution Channels

| Channel | Audience | Fee | Notes |
|---------|----------|-----|-------|
| VS Code Marketplace | Primary | 0% | Free extension; billing via Stripe |
| Open VSX | Cursor, Gitpod, Windsurf users | 0% | Same extension package |
| GitHub Marketplace | CI/CD users | ~3% | For the GitHub Action |
| AWS Marketplace | Enterprise procurement | ~15-20% | Metered billing against committed spend |
| Snowflake Marketplace | Snowflake-heavy orgs | ~15% | Native App Framework |
| Direct (querytorque.com) | Content-led visitors | 0% | Lowest fee path |

AWS and Snowflake Marketplace take a cut, but unlock procurement budgets that are otherwise inaccessible. A VP of Engineering who has $200K/year in AWS committed spend can buy QueryTorque without a new PO — it comes out of existing cloud budget.

---

## 10. Fleet Control (Web Dashboard — FinOps)

### 10.1 Scope

Fleet Control is a **separate web product** for the Team/Enterprise buyer who never opens VS Code. It consumes telemetry from the extension and presents aggregate cost data.

The Fleet Control buyer is a FinOps Manager, VP of Engineering, or Platform Engineering lead who cares about: total query cost trending, cost per team/service, top 50 most expensive queries, and total savings delivered. This persona justifies the enterprise contract and gainshare model.

### 10.2 Gainshare Mechanics

Gainshare is the core Fleet Control revenue model. It works because we can **prove** savings:

1. **Baseline:** Before beam, capture original query execution time (5x trimmed mean) and frequency (from pg_stat_statements / query history).
2. **Optimisation:** Beam generates validated rewrite. Checksummed result-set comparison proves equivalence.
3. **Deployment:** Developer applies fix. Extension tracks the deployed rewrite.
4. **Verification:** Fleet Control runs periodic re-benchmarks (weekly) to confirm the speedup holds in production.
5. **Savings calculation:**
   - `saved_time_per_exec = original_ms - optimised_ms`
   - `monthly_executions = frequency × 30`
   - `monthly_savings_seconds = saved_time_per_exec × monthly_executions / 1000`
   - For Snowflake/Databricks: convert seconds to credits, credits to dollars at customer's rate
   - For Postgres/MySQL: convert seconds to $/hour at customer's configured compute rate
6. **Invoice:** `total_verified_savings × gainshare_rate` (10-15%, negotiated per contract)

**Trust mechanism:** All savings calculations are based on checksummed benchmarks stored in Fleet Control. Customer can audit every number. We never claim savings we can't prove.

### 10.3 Dashboard Features

- **Org-wide Torque Score** trending over time
- **Cost leaderboard:** Top 50 queries by monthly cost across all developers
- **Savings waterfall:** Total $ saved per week, cumulative, by query and by team
- **Team health:** Per-team scores, issue trends, fix adoption rates
- **Gainshare tracker:** Verified savings × rate = current invoice amount (live)
- **Compliance:** % of PRs passing Torque Score gates, deployment block rate
- **Batch processor:** Upload SQL files or pg_stat_statements export for bulk beam optimisation

### 10.4 What the Extension Reports (Opt-In, Team Tier Only)

| Data Point | Sent? | Detail |
|-----------|-------|--------|
| Torque Scores | Yes | Aggregate per workspace, daily |
| Issue counts by severity | Yes | Per scan |
| Cost estimates per query | Yes | Per scan (anonymised query hash, not SQL) |
| Fix acceptance rate | Yes | Per beam |
| Validation pass rate | Yes | Per beam |
| Deployed fix tracking | Yes | Rewrite hash + speedup (not SQL) |
| **Actual SQL queries** | **Never** | Privacy guarantee |
| **Query results/data** | **Never** | Privacy guarantee |
| **Connection credentials** | **Never** | Privacy guarantee |
| **Source code** | **Never** | Privacy guarantee |

Enterprise customers can route Fleet Control telemetry through their own proxy for additional filtering before it reaches our servers.

### 10.5 Batch Processing (Fleet Workflow)

Engineering managers can upload `.sql` files or point Fleet Control at a `pg_stat_statements` export. Fleet Control queues beam optimisations for all queries, produces a report:

```
Batch: 156 queries from pg_stat_statements
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Results:
  31 WIN       (avg 3.2x speedup)
  21 IMPROVED  (avg 1.3x speedup)
  17 NEUTRAL
   7 REGRESSION (rejected — not deployed)

Estimated monthly savings: $4,200/month across all WINs
  Top 3:
    Q092 — shared_scan_decorrelate — saves $1,200/mo (was $1,240/mo, now $40/mo)
    Q032 — inline_decorrelate     — saves $890/mo
    Q001 — early_filter           — saves $640/mo

Gainshare (15%): $630/month
```

This is the enterprise sale. The extension is the foot in the door. Fleet Control is the wedge into procurement.

---

## 11. Database Connector Layer

### 11.1 Common Interface

```typescript
interface DatabaseConnector {
  connect(config: ConnectionConfig): Promise<Connection>;
  explain(query: string, analyze?: boolean): Promise<QueryPlan>;
  execute(query: string, limit?: number): Promise<ResultSet>;
  getTableSchema(table: string): Promise<TableSchema>;
  getIndexes(table: string): Promise<Index[]>;
  getStatistics(table: string): Promise<TableStats>;
  estimateCost(plan: QueryPlan): CostEstimate;
  disconnect(): Promise<void>;
}
```

### 11.2 Connector Matrix

| Database | Driver | Plan Command | Stats Source | Status |
|----------|--------|-------------|--------------|--------|
| PostgreSQL | `pg` (node-postgres) | `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` | `pg_stat_statements`, `pg_stat_user_tables` | **Ready** (Python executor exists, TS driver mature) |
| DuckDB | `duckdb-node` | `EXPLAIN ANALYZE` | Built-in profiling | **Ready** (Python executor exists) |
| Snowflake | `snowflake-sdk` | `GET_QUERY_PLAN` + query history | `QUERY_HISTORY`, `WAREHOUSE_METERING_HISTORY` | **Ready** (Python executor exists) |
| MySQL | `mysql2` | `EXPLAIN ANALYZE FORMAT=JSON` (8.0+) | `performance_schema`, `sys` schema | **Phase 2** |
| Databricks | `@databricks/sql` | `EXPLAIN EXTENDED` | Query history API, `system.billing` | **Phase 3** |

### 11.3 Connection Configuration

```yaml
# .querytorque.yml
connections:
  - name: main-postgres
    type: postgresql
    host: localhost
    port: 5432
    database: myapp_dev
    username_env: PGUSER      # Always use env vars
    password_env: PGPASSWORD

  - name: analytics-snowflake
    type: snowflake
    account_env: SNOWFLAKE_ACCOUNT
    warehouse: COMPUTE_WH
    database: ANALYTICS
    auth: externalbrowser     # SSO support

  - name: local-duckdb
    type: duckdb
    path: ./data/analytics.duckdb
```

Credentials: **Never** stored in config files. Always environment variables, OS keychain (`VS Code SecretStorage`), or SSO.

---

## 12. dbt Integration

### 12.1 Detection

When `dbt_project.yml` is present:
1. Identify models in `models/`, `analyses/`, `macros/`
2. Resolve `{{ ref('model') }}` and `{{ source('src', 'table') }}` via `manifest.json` or schema.yml parsing
3. Compile Jinja to raw SQL (calls `dbt compile` if CLI available)

### 12.2 dbt-Specific Analysis

| Feature | Free Tier | Pro Tier |
|---------|-----------|----------|
| Model file scanning | Yes | Yes |
| Anti-pattern detection in compiled SQL | Yes | Yes |
| Materialisation analysis (table vs view vs incremental) | No | Yes |
| Ref resolution (compiled SQL) | Basic | Full (via dbt compile) |
| Incremental model validation | No | Yes |
| Source freshness correlation | No | Yes |

---

## 13. CLI and CI/CD

### 13.1 CLI

```bash
# Scan SQL files (static analysis, free)
npx @querytorque/cli scan ./src/queries/ --db postgresql

# Scan with live connection (adds EXPLAIN + config recommendations)
npx @querytorque/cli scan ./src/ --connection "$DATABASE_URL"

# Run beam on a specific query
npx @querytorque/cli beam ./src/queries/slow_query.sql --connection "$DATABASE_URL"

# Scan dbt project
npx @querytorque/cli scan ./dbt_project/ --project-type dbt

# Output formats
npx @querytorque/cli scan ./src/ --format json --output report.json
npx @querytorque/cli scan ./src/ --format sarif --output results.sarif
```

### 13.2 GitHub Action

```yaml
name: QueryTorque SQL Check
on: [pull_request]

jobs:
  torque-scan:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: querytorque/scan-action@v1
        with:
          scan_path: ./src/
          database_type: postgresql
          min_score: 60
          block_on:
            - severity: critical
            - pattern: JN-001
          sarif_output: results.sarif
      - uses: github/codeql-action/upload-sarif@v3
        with:
          sarif_file: results.sarif
```

---

## 14. Development Phases

### Phase 1: The Weapon (Weeks 1-4)

**Goal:** Extension installs, connects to database, runs beam on a query, writes result back. Ship the magic.

| ID | Task | Effort | Notes |
|----|------|--------|-------|
| P1-01 | VS Code extension scaffold (TS, webpack, CI) | S | Standard extension boilerplate |
| P1-02 | PostgreSQL connector (node-postgres, EXPLAIN ANALYZE) | M | TypeScript, direct connection |
| P1-03 | Connection management UX (settings, test, SecretStorage) | M | First-run experience |
| P1-04 | QT API client (beam request/response) | M | REST client calling Python backend |
| P1-05 | Python API: `/api/v1/beam` endpoint wrapping Pipeline.from_dsn() | M | Thin wrapper around existing engine |
| P1-06 | Validator LLM integration in beam pipeline | L | New: Qwen validation between Gate 1 and benchmark |
| P1-07 | Beam Results panel (diff view, speedup, worker details) | L | Core UX |
| P1-08 | Write-back to .sql files | S | Apply optimised SQL to source |
| P1-09 | Validation execution in extension (checksums, timing) | L | Row count + MD5 + benchmark |
| P1-10 | VS Code Marketplace listing + publish CI | S | Ship it |

**Deliverable:** Working extension. Connect PG, select query, run beam, see 8 candidates, see validator reasoning, see validated speedup, apply to file. This is the demo that sells.

### Phase 2: The Hook (Weeks 5-8)

**Goal:** Free tier is genuinely useful. Static analysis + config recommendations. DuckDB + Snowflake support.

| ID | Task | Effort | Notes |
|----|------|--------|-------|
| P2-01 | SQL parser integration (multi-dialect AST) | L | node-sql-parser or sql-parser-cst |
| P2-02 | Anti-pattern library (YAML schema, 30 initial patterns) | M | Static rules, no LLM |
| P2-03 | Static analysis engine: detect patterns against AST | L | Match rules to parsed SQL |
| P2-04 | Config recommendation engine (6 EXPLAIN-based rules) | M | Port from Python config_boost.py |
| P2-05 | Torque Score calculation | S | Weighted severity + cost + frequency |
| P2-06 | Database Health panel (tree view, score gauge, issue list) | M | Main activity bar panel |
| P2-07 | Editor diagnostics (squiggly lines, CodeLens, hover) | M | In-editor integration |
| P2-08 | DuckDB connector (duckdb-node) | M | Second database |
| P2-09 | Snowflake connector (snowflake-sdk, credit-based cost) | L | Cloud database |
| P2-10 | Query Cost Explorer view | M | Discovery view |
| P2-11 | Free beam credit system (3/month, Stripe for Pro) | M | Billing integration |

**Deliverable:** Extension that's useful without paying. Install → connect → see issues + config advice → run 3 free beams → convert to Pro.

### Phase 3: The Scale (Weeks 9-14)

**Goal:** CI/CD, dbt, ORM detection, Fleet Control MVP, MySQL.

| ID | Task | Effort | Notes |
|----|------|--------|-------|
| P3-01 | CLI package (@querytorque/cli) | M | Shared with extension analysis engine |
| P3-02 | GitHub Action (PR comments, SARIF, status checks) | L | CI/CD integration |
| P3-03 | dbt full integration (ref resolution, compile) | L | Jinja parsing + model analysis |
| P3-04 | ORM detection (Django, Rails, SQLAlchemy patterns) | L | AST analysis of Python/Ruby/Java |
| P3-05 | MySQL connector (mysql2, EXPLAIN ANALYZE) | M | Fourth database |
| P3-06 | Fleet Control MVP (web dashboard, telemetry pipeline) | XL | Separate React app |
| P3-07 | Batch beam processing (upload SQL, bulk optimise) | L | Fleet Control feature |
| P3-08 | Team tier (seat management, shared config) | M | Stripe + licence management |
| P3-09 | Gainshare tracking (verified savings calculation) | L | Fleet Control feature |
| P3-10 | Expand anti-pattern library to 40+ patterns | M | Ongoing |

**Deliverable:** Full product suite for enterprise sale.

### Phase 4: The Moat (Weeks 15-20)

| ID | Task | Effort | Notes |
|----|------|--------|-------|
| P4-01 | Databricks connector | L | Fifth database |
| P4-02 | AWS Marketplace submission | L | Enterprise procurement |
| P4-03 | Snowflake Marketplace / Native App | L | Cloud-native distribution |
| P4-04 | Cross-database comparison (same query on PG vs Snowflake) | M | Migration planning tool |
| P4-05 | Performance benchmarking on large workspaces (1000+ files) | M | Scale testing |
| P4-06 | Editor page (optional web-based SQL editor) | M | For users without VS Code |

---

## 15. Technical Requirements

| Requirement | Minimum | Notes |
|-------------|---------|-------|
| VS Code | 1.85+ | Or Cursor, VS Code Insiders, Windsurf |
| Node.js | 18+ | For extension host and database drivers |
| OS | Windows 10+, macOS 12+, Linux | Cross-platform from day one |
| Python | Not required on client | Beam engine runs server-side |
| Network | Required for beams and Fleet Control only | Static analysis works fully offline |
| Extension size | <15MB | No sidecar binary |

---

## 16. Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Validator LLM false negatives (passes bad SQL) | Medium | Execution-based validation is the final gate. Validator is additive, not the only check. |
| Validator LLM false positives (rejects good SQL) | Low | UNCERTAIN verdict passes to benchmark. Only NOT_EQUIVALENT blocks. Tune threshold on TPC-DS corpus. |
| Python API latency for beams | Medium | 8 workers run in parallel. Total beam time ~15-30s. Show progress spinner with per-worker status. |
| Database driver compatibility | Medium | Pin versions. Integration test matrix. Community bug reports. |
| SQL parser dialect variations | High | Start with PostgreSQL (best tooling). Add dialects incrementally. Prefer false negatives over false positives. |
| Copilot competition | High | Copilot generates SQL but doesn't validate against live DB, track cost, or prove speedup. Our moat is the closed loop. |
| Low adoption due to credential sensitivity | Medium | Extension-local credentials (never sent to API). Read-only mode. Full query logging. |
| Qwen model availability / pricing changes | Low | Validator prompt is model-agnostic. Can swap to any instruction-following model. |

---

## 17. Success Metrics

### Phase 1 (Ship the Weapon)

| Metric | Target |
|--------|--------|
| VS Code Marketplace installs | >500 |
| Beams executed (including free) | >200 |
| Validation pass rate (beam produces valid speedup) | >70% |
| Median beam latency | <30 seconds |

### Phase 2 (The Hook)

| Metric | Target |
|--------|--------|
| Weekly active users | >500 |
| Files scanned (static analysis) | >10,000 |
| Config recommendations generated | >2,000 |
| Free-to-Pro conversion rate | >5% |

### Phase 3 (The Scale)

| Metric | Target |
|--------|--------|
| Marketplace installs | >5,000 |
| Pro + Team subscribers | >100 |
| Fleet Control active orgs | >10 |
| Total MRR | >$15K |
| Gainshare revenue | >$5K/month |

---

## 18. Open Decisions

| Decision | Options | Decide By |
|----------|---------|-----------|
| SQL parser: `node-sql-parser` vs `sql-parser-cst` vs `libpg-query`? | Multi-dialect breadth vs PG depth | Week 1 |
| Bundle all 5 DB drivers or lazy-load? | Install size vs first-connection latency | Week 1 |
| Validator model: Qwen-72B vs Qwen-32B vs DeepSeek-V3? | Accuracy vs cost vs latency | Week 2 (benchmark on TPC-DS corpus) |
| Beam API: hosted (querytorque.com) vs self-hosted option? | Simplicity vs enterprise security | Week 4 |
| Credential storage: VS Code SecretStorage vs OS keychain? | Simplicity vs security depth | Week 2 |
| Validation: run on connected DB or require dev/staging? | Safety vs friction | Week 4 |
| Free beams: 3/month vs 1/month vs 5/month? | Conversion rate vs cost exposure | Week 8 (A/B test) |

---

## Appendix A: Existing Engine Inventory (What We Ship With)

### Code We Keep (Core Engine — ~15,000 lines)

| Component | Lines | Purpose |
|-----------|-------|---------|
| `sessions/beam_session.py` | 3,145 | Beam orchestrator (analyst → workers → validate → snipe) |
| `pipeline.py` | 660 | Entry point (`Pipeline.from_dsn()`) |
| `validation/benchmark.py` | 550 | Fail-fast benchmarking with checksum verification |
| `validation/equivalence_checker.py` | 320 | MD5 checksums with float tolerance |
| `validation/cross_engine_checker.py` | 500 | Cross-engine equivalence (Gate 1.5) |
| `prompts/` | ~3,000 | Analyst, worker, snipe prompt builders |
| `knowledge/` | ~1,200 | Pathologies, transforms, gold examples, engine profiles |
| `execution/` | ~1,500 | PG, DuckDB, Snowflake executors |
| `pg_tuning.py` | 360 | SET LOCAL whitelist, resource envelope, validation |
| `config_boost.py` | 290 | 6 rule-based EXPLAIN→config recommendations |
| `tag_index.py` | 884 | AST feature extraction (patterns, OR analysis, subquery detection) |
| `schemas.py` | 500 | Core data models |
| `ir/` | ~2,000 | Patch engine, AST tools |

### Code We Strip (SaaS Bloat — ~5,000 lines)

| Component | Lines | Why |
|-----------|-------|-----|
| `api/routes/billing.py` | 149 | Stripe webhooks — rebuild for Fleet Control |
| `api/routes/auth.py` | 114 | Auth0 — not needed for extension |
| `api/routes/credentials.py` | 182 | Encrypted vault — extension uses SecretStorage |
| `api/routes/fleet.py` | 166 | Survey endpoints — rebuild for Fleet Control |
| `api/routes/github.py` | 103 | PR bot — move to GitHub Action |
| `celery_app.py` + `tasks.py` | 400 | Redis job queue — use direct execution |
| `billing/` | 250 | Stripe service — rebuild for Fleet Control |
| `auth/` | 500 | JWT/Auth0 — extension doesn't need this |
| `vault/` | 72 | Fernet encryption — use SecretStorage |
| `llm/metered_client.py` | 102 | Per-org metering — rebuild for Fleet Control |
| `database/models.py` | 545 | Multi-tenant ORM — rebuild lighter for Fleet |
| `docker-compose.yml` | 149 | SaaS deployment — not needed |
| `fleet/ws_server.py` | 718 | WebSocket dashboard — rebuild as Fleet Control |
| `fleet/orchestrator.py` | 1,033 | Survey logic — move to Fleet Control |
| `github/` | 426 | PR bot — move to GitHub Action |

### Code We Defer (Fleet Control — Future)

| Component | Lines | When |
|-----------|-------|------|
| `fleet/event_bus.py` | 274 | Phase 3 |
| `fleet/orchestrator.py` | 1,033 | Phase 3 (refactored) |
| `fleet/dashboard.py` | 145 | Phase 3 (replaced by React app) |

---

## Appendix B: Verified Speedups (Marketing Ammunition)

These are real, validated speedups from our benchmark corpus. Every number is from 5x trimmed mean or 3x3 validation.

### PostgreSQL (DSB Benchmark — 76 queries)

| Query | Transform | Speedup | Technique |
|-------|-----------|---------|-----------|
| Q092 | shared_scan_decorrelate | **8,044x** | Consolidated 847K correlated subquery executions into single scan |
| Q032 | inline_decorrelate | **1,465x** | Replaced correlated subquery with JOIN |
| Q081 | state_avg_decorrelate | **439x** | Decorrelated state-level average computation |
| Q001 | early_filter_decorrelate | **27.80x** | Pushed dimension filters + decorrelated |
| Q069 | set_operation_materialization | **17.48x** | Materialized set operation intermediate |
| Q102 | config + pg_hint_plan | **2.22x** | HashJoin hints + work_mem + JIT off |

### DuckDB (TPC-DS — 88 queries)

| Query | Transform | Speedup | Technique |
|-------|-----------|---------|-----------|
| Q88 | or_to_union + time_bucket | **5.25x** | Decomposed 9-branch OR into UNION |
| Q9 | single_pass_aggregation | **4.47x** | Consolidated repeated scans into one pass |
| Q40 | multi_cte_chain | **3.35x** | Materialized CTE chain |
| Q46 | triple_dimension_isolate | **3.23x** | Isolated 3 dimension filters into CTEs |
| Q35 | intersect_to_exists | **1.78x** | Replaced INTERSECT with EXISTS |

### Snowflake (TPC-DS SF10)

| Query | Transform | Speedup | Technique |
|-------|-----------|---------|-----------|
| — | inline_decorrelate | **23.17x** | Decorrelated subquery (verified 3x3) |
| — | shared_scan_decorrelate | **7.82x** | Consolidated scans (verified 3x3) |

---

*End of document. QueryTorque for SQL — Combined PRD v2.0*

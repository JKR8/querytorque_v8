# QueryTorque for SQL

## Product Requirements Document

### February 2026 | Confidential

---

## 1. What This Is

QueryTorque for SQL is a VS Code extension that finds performance problems in SQL, explains them to the developer, and fixes them using an 8-worker AI rewriting engine — with a validation layer that proves the fix returns identical results before writing anything back.

The extension is the product surface. The Beam is the engine underneath. What makes it different from every other SQL tool:

1. **It actually rewrites SQL and proves it's faster.** Not advice. Not "consider adding an index." A rewritten query, validated to return identical results, benchmarked to run faster, written back to your file.

2. **8 parallel AI workers attack every query from different angles.** An analyst LLM reasons about the execution plan, dispatches 8 specialist workers that each try a different optimisation strategy, a validator LLM proves semantic equivalence via column lineage, then survivors race against the original on the live database.

3. **Free tier is genuinely useful.** Static anti-pattern detection (30+ patterns), EXPLAIN-driven PostgreSQL config recommendations you can copy-paste and test immediately (`SET LOCAL work_mem = '512MB'`), and 3 free beam optimisations per month.

4. **FinOps lives in Fleet Control.** The extension is where developers optimise queries. The web dashboard is where engineering managers see aggregate cost, track savings, and approve gainshare contracts. Two products, one pipeline.

---

## 2. Architecture: What Exists vs What We Build

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                  │
│   WHAT WE BUILD (this PRD)                                      │
│                                                                  │
│   ┌─────────────┐  ┌──────────────┐  ┌───────────────────────┐  │
│   │ Static      │  │ Validation   │  │ VS Code Extension     │  │
│   │ Analysis    │  │ Layer        │  │                       │  │
│   │ Engine      │  │              │  │ - Health Panel        │  │
│   │             │  │ - Execute    │  │ - Issue Tree          │  │
│   │ - Anti-     │  │   original   │  │ - Fix Review (diff)   │  │
│   │   pattern   │  │ - Apply fix  │  │ - CodeLens / Diag     │  │
│   │   detection │  │ - Execute    │  │ - Query Cost Explorer │  │
│   │ - Torque    │  │   again      │  │ - Write-back          │  │
│   │   Score     │  │ - Compare    │  │                       │  │
│   │ - Config    │  │   results    │  │                       │  │
│   │   recs      │  │ - Rollback   │  │                       │  │
│   └──────┬──────┘  └──────┬───────┘  └───────────┬───────────┘  │
│          │                │                      │              │
│   ┌──────┴────────────────┴──────────────────────┴───────────┐  │
│   │  Database Connector Layer                                 │  │
│   │  pg - mysql2 - snowflake-sdk - @databricks/sql - duckdb  │  │
│   └──────────────────────┬───────────────────────────────────┘  │
│                          │                                      │
└──────────────────────────┼──────────────────────────────────────┘
                           │
              ┌────────────┼────────────────┐
              │            │                │
              ▼            ▼                ▼
        ┌──────────┐ ┌──────────┐   ┌─────────────┐
        │ User's   │ │ BEAM     │   │ Fleet       │
        │ Database │ │ ENGINE   │   │ Control     │
        │ (live)   │ │ (exists) │   │ (Phase 4)   │
        └──────────┘ └──────────┘   └─────────────┘

        WHAT EXISTS    WHAT EXISTS    FUTURE
```

### 2.1 What Runs Where

| Component | Runs In | Language | Why |
|-----------|---------|----------|-----|
| File scanning + pattern detection | Extension | TypeScript | Must be instant, offline, no network |
| Database connection + EXPLAIN | Extension | TypeScript (native drivers) | Direct connection, no proxy |
| Config recommendations (SET LOCAL) | Extension | TypeScript | Rule-based, deterministic, free |
| Beam optimisation (analyst + 8 workers + validator) | QT API | Python | 30K+ lines of battle-tested engine |
| Result validation (checksums, benchmarks) | Extension + DB | TypeScript + SQL | Runs against user's DB directly |
| Write-back | Extension | TypeScript | Modifies user's source files |
| Fleet Control dashboard | Web app | React | Separate product for FinOps buyers |

### 2.2 Why Not Pure TypeScript?

The Beam engine is 30,000+ lines of Python containing 30 transform definitions with regression registries, 10 engine pathologies per database, 30 gold examples with verified speedups (up to 8,044x on PostgreSQL), 3-tier semantic validation, race validation with barrier synchronisation, EXPLAIN-first reasoning with hypothesis-driven dispatch, and per-worker SET LOCAL tuning. Rewriting this in TypeScript would take 6+ months and lose battle-tested correctness.

**The extension calls the Beam via API.** The Beam does not run locally. This keeps the Beam proprietary and the extension lightweight (<15MB, no sidecar, no Docker).

---

## 3. The Product Loop

### 3.1 Five Steps

```
1. DETECT    ──→  Scan .sql files, detect patterns, show Torque Score
2. DIAGNOSE  ──→  Connect to DB, run EXPLAIN, recommend SET LOCAL configs
3. REWRITE   ──→  8-beam AI fan-out generates optimised candidates
4. VALIDATE  ──→  Validator LLM proves equivalence, then benchmark on live DB
5. APPLY     ──→  Write optimised SQL back to source file
```

Steps 1-2 are **free**. Step 3 costs a beam credit (3 free/month, then paid). Steps 4-5 execute automatically.

### 3.2 What the User Sees

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

## 4. Free Tier: Pattern Detection + Config Recommendations

### 4.1 Philosophy

The free tier must be **genuinely useful**. A developer should install QueryTorque, connect their database, and get actionable advice they can test in 30 seconds without paying.

Free gives you two things:

1. **"We detected a pattern in this query that will likely make it run faster when fixed."** Static AST analysis, no LLM, instant.

2. **"Here are PostgreSQL config settings you can test right now."** EXPLAIN-driven SET LOCAL recommendations. Copy-paste into psql, see the difference.

### 4.2 Anti-Pattern Library (30+ Static Rules)

#### Category 1: Join and Relationship Issues

| ID | Pattern | Detection | Impact |
|----|---------|-----------|--------|
| JN-001 | Cartesian join (missing predicate) | EXPLAIN: Nested Loop, no condition | 10x-1000x row explosion |
| JN-002 | Comma-join syntax | Parser: `FROM a, b WHERE` | Hides missing predicates |
| JN-003 | Join on non-indexed column | EXPLAIN: Seq Scan on join column | Full table scan per join |
| JN-004 | Type mismatch across join | Column types differ (varchar vs int) | Implicit cast prevents index use |
| JN-005 | DISTINCT masking bad join | DISTINCT on duplicate-producing join | Sort overhead hiding root cause |
| JN-006 | Left join with WHERE on right side | Filter on outer-joined table in WHERE | Silently converts to inner join |

#### Category 2: Index and Scan Issues

| ID | Pattern | Detection | Impact |
|----|---------|-----------|--------|
| IX-001 | Sequential scan on large table | EXPLAIN: Seq Scan, >10K rows, filter present | Full table read |
| IX-002 | Unused index | `pg_stat_user_indexes`: idx_scan = 0 | Wasted storage, slower writes |
| IX-003 | Function on indexed column | `WHERE LOWER(col)` or `WHERE col + 1 > x` | Index bypass |
| IX-004 | Leading wildcard LIKE | `WHERE col LIKE '%term'` | Full scan |
| IX-005 | Missing composite index | Multiple equality filters, no multi-column index | Multiple scans vs one |
| IX-006 | Over-indexing | >10 indexes or index size > table size | Write amplification |

#### Category 3: Query Structure (Rewrite Opportunities)

| ID | Pattern | Detection | What Beam Can Do |
|----|---------|-----------|-----------------|
| QS-001 | Correlated subquery | Outer reference in subquery | Decorrelate → 2-10x typical |
| QS-002 | Repeated table scans | Same table 3+ times in AST | CTE consolidation → 1.5-6x |
| QS-003 | Cross-column OR branches | OR across different columns | OR-to-UNION → 1.3-5x |
| QS-004 | Unfiltered CTE | CTE without WHERE clause | Predicate pushdown → 1.2-3x |
| QS-005 | Star join without early filter | Fact + dimensions, no prefilter | Early filter → 1.5-4x |
| QS-006 | SELECT * in production code | Wildcard select | Column pruning |
| QS-007 | UNION instead of UNION ALL | UNION without dedup need | Remove sort+dedup |
| QS-008 | Deep subquery nesting (>2 levels) | AST depth analysis | Flatten to CTEs or joins |

#### Category 4: Data Type Issues

| ID | Pattern | Detection | Impact |
|----|---------|-----------|--------|
| DT-001 | Implicit type cast in WHERE | Compare varchar to int | Index bypass |
| DT-002 | Timestamp vs date without cast | `WHERE ts_col = '2024-01-01'` | Misses rows after midnight |
| DT-003 | Float for currency | FLOAT/DOUBLE for monetary values | Rounding errors |

#### Category 5: Cloud Cost Patterns (Snowflake / Databricks)

| ID | Pattern | Detection | Impact |
|----|---------|-----------|--------|
| CC-001 | Full scan on large warehouse | Query history: >1TB scan | $5+ per execution |
| CC-002 | Warehouse auto-suspend too long | Idle minutes > credits consumed | Idle credit burn |
| CC-003 | Missing clustering key | >1B rows, no clustering, range filters | Excessive partition scanning |
| CC-004 | ORDER BY on huge result | >100K rows with ORDER BY | Spill-to-disk, credit burn |
| CC-005 | Repeated identical queries | Same hash >10x/hour, no caching | Wasted compute |

#### Category 6: ORM-Specific Patterns

| ID | Pattern | Detection | Impact |
|----|---------|-----------|--------|
| ORM-001 | N+1 loop pattern | Same template >10x, varying param | 100x+ round-trips |
| ORM-002 | Missing eager load | FK access triggers lazy load | N+1 variant |
| ORM-003 | Unfiltered .all() on large table | ORM fetches entire table | Memory explosion |
| ORM-004 | Count then fetch | `.count()` then `.all()` | Redundant round-trip |

### 4.3 Free Config Recommendations (PostgreSQL)

When the user connects their database and we run EXPLAIN ANALYZE, we offer **6 deterministic config recommendations** at zero LLM cost:

| Rule | Trigger | Advice |
|------|---------|--------|
| **work_mem sizing** | Hash spill (Batches > 1) or peak memory >= 50% of work_mem | `SET LOCAL work_mem = '{peak × 4}MB';` |
| **Disable nested loops** | Nested Loop with actual rows > 10,000 | `SET LOCAL enable_nestloop = off;` |
| **Enable parallelism** | No parallel nodes + Seq Scan on > 100K rows | `SET LOCAL max_parallel_workers_per_gather = 4;` |
| **Disable JIT** | JIT active + total execution < 500ms | `SET LOCAL jit = off;` |
| **Favour index scans** | Seq Scan on large table + SSD detected | `SET LOCAL random_page_cost = 1.1;` |
| **Join collapse limit** | > 6 join nodes in plan | `SET LOCAL join_collapse_limit = 12;` |

All use SET LOCAL (transaction-scoped, auto-reverts). All whitelisted against a 16-parameter safety list. Copy-paste into psql and test immediately.

### 4.4 Torque Score

0-100 per query:

```
Score = (Pattern Severity × 0.2) + (Execution Cost × 0.4)
      + (Scan Efficiency × 0.2) + (Frequency × 0.2)
```

Prioritisation:
- **Fix Now (Red):** Score > 80 or cost > $100/month
- **Fix Soon (Amber):** Score 50-80 or cost $10-100/month
- **Improve (Blue):** Score < 50, best-practice violations

---

## 5. Paid Tier: Beam Optimisation Engine

### 5.1 What a Beam Does

A "beam" is one complete optimisation session for one SQL query.

```
                    ┌─────────────┐
                    │  Your SQL   │
                    │  + EXPLAIN  │
                    │  + Schema   │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  ANALYST    │  ← 1 LLM call (DeepSeek R1)
                    │  Reasons    │    Reads plan, identifies bottlenecks,
                    │  about plan │    dispatches worker strategies
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │            │            │         (8 parallel workers)
         ┌────▼───┐  ┌────▼───┐  ┌────▼───┐
         │ Worker │  │ Worker │  │ Worker │  ...  × 8
         │  Qwen  │  │  Qwen  │  │  Qwen  │
         └────┬───┘  └────┬───┘  └────┬───┘
              │            │            │
              └────────────┼────────────┘
                           │
                    ┌──────▼──────┐
                    │  GATE 1     │  ← Parse check (instant, no LLM)
                    │  SQLGlot    │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  VALIDATOR  │  ← 1 LLM call (Qwen 72B)
                    │  LLM        │    Proves equivalence via
                    │  (Section 6)│    column lineage tracing
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  BENCHMARK  │  ← Database execution (no LLM)
                    │  Race or    │    Row count + MD5 checksum + timing
                    │  5x mean    │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  RESULT     │
                    │  Best SQL   │
                    │  + Speedup  │
                    └─────────────┘
```

**LLM calls per beam:** 10 typical (1 analyst + 8 workers + 1 validator)
**Cost per beam:** ~$0.02-0.08

### 5.2 Worker Strategy Allocation

| Worker | Strategy Family | What It Tries | Best Verified Speedup |
|--------|----------------|---------------|----------------------|
| W1 | Early Filtering (A) | Push dimension filters into CTEs | 27.80x (PG Q001) |
| W2 | Decorrelation (B) | Convert correlated subqueries to JOINs | 1,465x (PG Q032) |
| W3 | Aggregation (C) | Consolidate repeated scans, single-pass | 5.25x (DuckDB Q88) |
| W4 | Set Operations (D) | OR→UNION, INTERSECT→EXISTS | 1.78x (DuckDB Q35) |
| W5 | Materialization (E) | CTE materialization, dimension prefetch | 3.23x (DuckDB Q46) |
| W6 | Join Transform (F) | Join reordering, predicate pushdown | 8.56x (PG explicit_join) |
| W7 | Config + Rewrite | SET LOCAL tuning combined with SQL rewrite | 2.22x (PG Q102) |
| W8 | Exploration | Novel strategies, compound transforms | 4.47x (DuckDB Q9) |

### 5.3 Engine Knowledge

| Database | Pathologies | Gold Examples | Verified Speedups |
|----------|------------|---------------|-------------------|
| PostgreSQL | 7 (P1-P7) | 14 | Up to 8,044x |
| DuckDB | 10 (P0-P9) | 16 + 10 regressions | Up to 6.28x |
| Snowflake | 9 hypothesised | 2 verified | Up to 23.17x |
| MySQL | Planned | - | - |
| Databricks | Planned | - | - |

---

## 6. The Validator LLM

### 6.1 Why We Need It

Current validation catches most errors but has blind spots:

| Error Type | Current Gate Catches? |
|-----------|----------------------|
| Syntax error | Yes (SQLGlot parse) |
| Wrong row count | Yes (execution) |
| Wrong values | Yes (MD5 checksum) |
| Column reordering | **No** (order-independent checksum) |
| LEFT→INNER conversion | **Sometimes** (only if NULLs in sample) |
| Subtle filter change (> vs >=) | **Sometimes** (only if boundary rows present) |
| NULL handling change | **Sometimes** (only if NULLs in data) |
| Aggregate scope change | **Sometimes** (if groups happen to match) |

The validator catches the "sometimes" cases **before** benchmark execution. $0.003 per validation vs $0.10+ per wasted benchmark — 30x ROI.

### 6.2 Architecture

**Model:** Qwen-2.5-72B-Instruct (systematic verification, not creative reasoning — cheaper and faster than DeepSeek R1).

**Input:** Single prompt containing original SQL, all 8 candidate rewrites, and table schemas.

**For each candidate, the validator must produce:**

1. **Column lineage proof** — Trace each output column from source → transforms → output
2. **Join graph equivalence** — Same tables, same predicates, same join types
3. **Filter equivalence** — Prove WHERE/HAVING predicates are logically equivalent
4. **Aggregation check** — Same GROUP BY, same aggregate functions on same source columns
5. **NULL handling** — Does the rewrite change behaviour for NULL values?

**Output per candidate:**
```json
{
  "candidate_id": 1,
  "verdict": "EQUIVALENT",
  "confidence": 0.95,
  "column_lineage": {"col1": {"source": "orders.total", "transform": "SUM", "match": true}},
  "issues": [],
  "reasoning": "Decorrelation replaces correlated subquery with JOIN. All columns trace to same sources."
}
```

### 6.3 Integration Point

```
Workers return 8 candidates
    │
    ▼
Gate 1: SQLGlot Parse ──── rejects syntax errors (instant)
    │
    ▼ (survivors, typically 6-8)
    │
VALIDATOR LLM ──── proves/disproves equivalence (1 call, ~$0.003)
    │
    ├── EQUIVALENT ──→ proceed to benchmark
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

**LEFT → INNER conversion:**
```sql
-- Original: LEFT JOIN users ... WHERE u.region = 'APAC'
-- Candidate: removes LEFT, makes it INNER JOIN
-- Validator: "EQUIVALENT — WHERE u.region = 'APAC' already eliminates NULLs"
```

**Aggregate scope change:**
```sql
-- Original: AVG(salary)
-- Candidate: AVG(DISTINCT salary)
-- Validator: "NOT_EQUIVALENT — AVG(DISTINCT) excludes duplicate salary values"
```

**Filter boundary shift:**
```sql
-- Original: WHERE created_at >= '2025-01-01'
-- Candidate: WHERE created_at > '2024-12-31'
-- Validator: "UNCERTAIN — equivalent for DATE type, not for TIMESTAMP"
```

---

## 7. Validation Framework (Post-Validator)

### 7.1 Execution-Based Validation

After the validator LLM approves candidates:

1. **Execute original** (with LIMIT safety, default 10,000 rows). Capture row count, column names, MD5 checksum.
2. **Execute each candidate** (same LIMIT). Fail-fast: row count mismatch on first run = reject immediately.
3. **Compare checksums.** MD5 of normalised, sorted rows. Match → proceed. Mismatch → row-level diff for user.
4. **Benchmark timing.**
   - Query >= 2s: Race validation (original + candidates simultaneously, barrier-synchronised)
   - Query < 2s: 5x trimmed mean (discard min/max, average remaining 3)
5. **Classify:** >= 1.10x WIN, >= 1.05x IMPROVED, >= 0.95x NEUTRAL, < 0.95x REGRESSION (rejected)

### 7.2 Safety Mechanisms

| Concern | Handling |
|---------|---------|
| Query modifies data | `BEGIN ... ROLLBACK` wrapping |
| Extremely slow (>30s) | Timeout with progress bar |
| Huge result set | LIMIT cap (configurable, default 10K) |
| Non-deterministic functions | Pin `NOW()` via SET, flag `RANDOM()` |
| Production database | Warn, recommend dev/staging |
| Cloud credit cost | Estimate before execution, approve >$1 |

### 7.3 Fix Categories

| Category | What Changes | Validation | Auto-Apply |
|----------|-------------|------------|------------|
| **A: Equivalent rewrites** | SQL logic | Full execute-compare loop | Yes |
| **B: Index recommendations** | Schema (CREATE INDEX) | EXPLAIN before/after | No — DDL approval required |
| **C: ORM-level fixes** | Application code | Cannot validate via SQL | No — suggestion only |
| **D: Cloud configuration** | Infrastructure | Cost projection only | No — Fleet Control |

**Only Category A goes through the automated loop. The Beam generates Category A fixes. Everything else is advisory.**

---

## 8. VS Code Extension UX

### 8.1 Activation

Activates when workspace contains `.sql` files, `dbt_project.yml`, or `.querytorque.yml`. Immediate workspace scan on activation.

### 8.2 Primary Views

**Database Health Panel (Activity Bar)**
- Connection status, latency, database version
- Workspace Torque Score (0-100 gauge)
- Issue tree: Fix Now / Fix Soon / Improve → by file/query
- Top Expensive Queries (by $/month or execution time)
- Quick actions: Scan Workspace, Connect to Database

**Issue Detail Panel**
- Plain-language description of what's wrong
- Current SQL with problematic section highlighted
- EXPLAIN plan visualisation (tree view with node costs)
- Impact: "Runs 2,400x/day, scans 847K rows, takes 4.2s. Monthly cost: ~$340."
- Config recommendations (free, copy-paste ready)
- "Run Beam" button (uses 1 credit)

**Fix Review Panel (Side-by-Side)**
- Left: Original SQL, problems in red. Right: Optimised, changes in green.
- Explanation, confidence, speedup metrics
- EXPLAIN plan comparison (before/after)
- All 8 worker results (expandable)
- Validator reasoning per candidate
- Buttons: **[Apply to File]**, View Full Diff, Reject, Edit

**Query Cost Explorer**
- Most expensive queries across connected databases
- Links to source file, shows issue count and beam availability
- Discovery view: find problems you didn't know you had

### 8.3 Editor Integration

- **Diagnostics:** Squiggly underlines on anti-patterns in .sql and inline SQL. Hover shows issue + cost.
- **Code Actions:** Lightbulb → "Fix this query" / "Explain this plan" / "Show config recs"
- **CodeLens:** Above each query: `Torque: 72 | $34/mo | 3 issues | Last run: 2.4s`
- **Status bar:** Workspace score, connection indicator, credits remaining

### 8.4 Command Palette

```
QueryTorque: Scan Current File
QueryTorque: Scan Workspace
QueryTorque: Connect to Database
QueryTorque: Disconnect
QueryTorque: Show Query Cost Explorer
QueryTorque: Explain Query at Cursor
QueryTorque: Run Beam on Query at Cursor
QueryTorque: Show Config Recommendations
QueryTorque: Fix All Safe Issues in File
QueryTorque: Export Report (HTML / JSON)
QueryTorque: Open Fleet Control Dashboard
```

---

## 9. SQL File Detection

| Source | Detection | Parsing |
|--------|-----------|---------|
| `.sql` files | File extension | Direct SQL parsing, multiple statements, CTEs |
| dbt models | `dbt_project.yml` present | Parse Jinja+SQL; resolve refs via manifest.json or dbt compile |
| Django ORM | `.py` with `django.db` imports | Detect querysets; map fixes to ORM code |
| Rails Active Record | `.rb` with ActiveRecord patterns | Detect query chains |
| SQLAlchemy | `.py` with `sqlalchemy` imports | Detect query builder patterns |
| Inline SQL strings | Python, TypeScript, Go, Java, Ruby | Regex + AST detection |
| Stored procedures | `CREATE FUNCTION/PROCEDURE` | Parse body SQL |

### 9.1 ORM Fix Mapping

When ORM-generated SQL is detected, show two fixes:

```
Issue: N+1 query pattern detected
SQL:   SELECT * FROM orders WHERE user_id = 1  (executed 847 times)

Beam Fix (SQL):    SELECT * FROM orders WHERE user_id IN (1, 2, 3, ...)
ORM Fix (Django):  Order.objects.filter(user__in=users).select_related('user')
ORM Fix (Rails):   User.includes(:orders).where(id: user_ids)
```

The SQL fix goes through the Beam and validation. The ORM fix is a suggestion shown alongside.

### 9.2 dbt Integration

First-class, not an afterthought:
- Resolve `{{ ref() }}` and `{{ source() }}` to real table names
- Analyse materialisation choices (table vs view vs incremental)
- Flag unused models (not referenced downstream)
- Validate incremental model merge keys
- GitHub Action understands dbt project structure

---

## 10. Database Connector Layer

### 10.1 Common Interface

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

### 10.2 Connector Matrix

| Database | Driver | Plan Command | Stats Source | Status |
|----------|--------|-------------|--------------|--------|
| PostgreSQL | `pg` | `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` | `pg_stat_statements` | **Ready** |
| DuckDB | `duckdb-node` | `EXPLAIN ANALYZE` | Built-in profiling | **Ready** |
| Snowflake | `snowflake-sdk` | `GET_QUERY_PLAN` + query history | `QUERY_HISTORY` | **Ready** |
| MySQL | `mysql2` | `EXPLAIN ANALYZE FORMAT=JSON` (8.0+) | `performance_schema` | Phase 3 |
| Databricks | `@databricks/sql` | `EXPLAIN EXTENDED` | Query history API | Phase 4 |

### 10.3 Connection Configuration

```yaml
# .querytorque.yml
connections:
  - name: main-postgres
    type: postgresql
    host: localhost
    port: 5432
    database: myapp_dev
    username_env: PGUSER
    password_env: PGPASSWORD

  - name: analytics-snowflake
    type: snowflake
    account_env: SNOWFLAKE_ACCOUNT
    warehouse: COMPUTE_WH
    auth: externalbrowser

  - name: local-duckdb
    type: duckdb
    path: ./data/analytics.duckdb
```

**Security — non-negotiable:**
- Never store credentials in config files. Environment variables, VS Code SecretStorage, or SSO only.
- All queries the extension runs are logged locally (`.querytorque/query-log.jsonl`).
- Read-only connection mode available.
- Connection credentials never leave the local machine. Never sent to Beam API or Fleet Control.

---

## 11. Pricing and Tiers

### 11.1 Pricing Model

Two different models for two different buyers. The extension is a developer tool sold per-seat. Fleet Control is a FinOps platform sold on value.

**Competitive context:** EverSQL (acquired by Aiven, 100K+ users, bootstrapped to profitability) charges $99/mo for 10 single-pass optimisations (~$10/optimisation). Our Beam runs 8 parallel workers with a validator LLM. EverSQL never did gainshare.

#### Developer Tool (Extension)

| Tier | Price | Beams | Static Analysis | Buyer |
|------|-------|-------|----------------|-------|
| **Free** | $0 | **3/month** | Full (top 5 issues) | Individual dev trying it out |
| **Pro** | **$49/mo** | **50/month** (~$1/beam) | Full (all issues) | Individual dev who's hooked |
| **Team** | **$199/seat/mo** | **Unlimited** | Full + shared config | Team lead, 5+ devs |

#### FinOps Platform (Fleet Control)

| Model | Price | Buyer |
|-------|-------|-------|
| **Platform fee** | **$2,500+/mo** per database | Orgs that won't do gainshare |
| **Gainshare** | **10-15% of verified savings** | Orgs with large cloud spend |
| **Hybrid** | **$1,500/mo base + 10% gainshare** | Negotiated enterprise deals |

### 11.2 Feature Matrix

| Feature | Free | Pro ($49/mo) | Team ($199/seat) | Fleet Control |
|---------|------|-------------|-----------------|--------------|
| Anti-pattern detection | Top 5 issues | All patterns | All patterns | Org-wide |
| Database connections | 1 | 3 | Unlimited | Unlimited |
| EXPLAIN + config recs | Yes | Yes | Yes | Yes |
| Cost estimation | Time only | Full ($/month) | Full | Full + historical |
| **Beam optimisations** | **3/month** | **50/month** | **Unlimited** | **Batch** |
| Credit rollover | No | **Yes (1 month, max 100)** | N/A | N/A |
| Validation + write-back | Yes (on beams) | Yes | Yes | Report |
| ORM fix suggestions | Detection only | Detection + fix | Full | Full |
| dbt integration | File scanning | Full | Full | Full |
| Query Cost Explorer | Top 5 | Full | Full + trending | Org-wide |
| Export report | Watermarked | Full | Custom branding | Full + PDF |
| Fleet Control sync | No | No | Yes | Core |
| CI/CD (GitHub Action) | Score gate | Score + blocking | Full | Full + compliance |
| Gainshare tracking | No | No | No | Yes |

### 11.3 Why Each Price Point Works

**$49/mo Pro (~$1/beam).** EverSQL proved $99 for 10 optimisations works (~$10 each). We give 50 for $49 — ~$1/beam. Feels like a steal, but margins are 95%+ because a beam costs ~$0.05. Lower per-unit cost drives higher usage, which drives more data for Fleet Control.

**$199/seat/mo Team.** Unlimited beams changes behaviour. At Pro, developers think about whether a query is "worth" a credit. At Team, they beam everything. More fixes deployed → more savings proven → more data for Fleet Control. The unlimited model creates the usage that makes Fleet Control valuable.

**Fleet Control at $2,500+/mo is the floor.** The real money is gainshare:
- $50K/mo cloud bill × 20% savings × 15% gainshare = **$1,500/mo**
- $200K/mo cloud bill × 20% savings × 15% gainshare = **$6,000/mo**
- $500K/mo cloud bill × 15% savings × 12% gainshare = **$9,000/mo**

### 11.4 Why 3 Free Beams

Three beams lets a developer experience the full loop:
1. **Beam 1:** See fan-out, validator reasoning, validated speedup. The "holy shit" moment.
2. **Beam 2:** Confirm it's not a fluke.
3. **Beam 3:** The query they actually care about. Where ROI calculation happens.

Zero beams = never see what you're paying for. One = might be a fluke. Three = a pattern. Enough to hook, not enough to satisfy.

### 11.5 Credit Rollover (Pro Only)

Unused Pro credits carry forward 1 month, max 100 banked. Kills the "paying but not using" churn trigger.

### 11.6 Conversion Triggers

- **4th beam:** "3 free beams used. Upgrade to Pro for 50/month at ~$1 each."
- **6th issue hidden:** "14 issues found. Upgrade to see all."
- **2nd connection:** "Pro supports 3 connections."
- **Team upsell:** "4 Pro seats? Team is unlimited beams + shared config — saves $196/mo."
- **Fleet Control:** "47 fixes deployed saving $8,200/month. See the full picture."

---

## 12. Fleet Control (Web Dashboard — FinOps)

### 12.1 Scope

Separate web product for the buyer who never opens VS Code. Consumes extension telemetry, presents aggregate cost data, tracks gainshare.

### 12.2 Gainshare Mechanics

1. **Baseline:** Capture original timing (5x trimmed mean) + frequency from pg_stat_statements / query history.
2. **Optimisation:** Beam generates validated rewrite. Checksummed equivalence.
3. **Deployment:** Developer applies fix. Extension tracks deployed rewrite.
4. **Verification:** Fleet Control re-benchmarks weekly. Confirm speedup holds.
5. **Calculation:** `saved_time × monthly_frequency × $/compute_second`
6. **Invoice:** `total_verified_savings × gainshare_rate`

All savings based on checksummed benchmarks. Customer can audit every number.

### 12.3 Dashboard Features

- Org-wide Torque Score trending
- Cost leaderboard: top 50 queries by monthly cost
- Savings waterfall: $ saved per week, cumulative, by team
- Gainshare tracker: verified savings × rate = current invoice (live)
- Compliance: % of PRs passing Torque Score gates
- Batch processor: upload SQL or pg_stat_statements export for bulk beam

### 12.4 Privacy

| Data | Sent? |
|------|-------|
| Torque Scores (aggregate) | Yes |
| Issue counts by severity | Yes |
| Cost estimates (anonymised hash) | Yes |
| Fix acceptance rate | Yes |
| **Actual SQL queries** | **Never** |
| **Query results/data** | **Never** |
| **Credentials** | **Never** |
| **Source code** | **Never** |

---

## 13. Distribution and Marketplace Strategy

### 13.1 Developer Acquisition (Free to List)

| Channel | What | Buyer |
|---------|------|-------|
| **VS Code Marketplace** | Free extension, Pro/Team via Stripe | Individual developers |
| **Open VSX** | Same extension | Cursor, Gitpod, Windsurf users |
| **GitHub Marketplace** | GitHub Action | CI/CD teams |

### 13.2 Enterprise Revenue (Marketplace Fees)

| Channel | What | Fee | Why |
|---------|------|-----|-----|
| **AWS Marketplace** | Fleet Control (metered) | 3-5% | Purchases count against committed AWS spend |
| **Snowflake Marketplace** | Fleet Control (Native App) | Rev share | Embedded in Snowflake console, 330K+ customers |
| **Databricks Marketplace** | Fleet Control integration | Rev share | Partner ecosystem |

**The extension is not sold through enterprise marketplaces.** A $49/mo extension doesn't belong on AWS Marketplace where minimum viable listing is $50K+ ACV. Fleet Control at $50-150K ACV with gainshare is what enterprise marketplaces are for.

The extension is the acquisition engine. Developers adopt bottom-up. When 8 developers on a team use it, the manager asks "can I see this across everyone?" That's Fleet Control. That's the $75K contract.

---

## 14. CLI and CI/CD

### 14.1 CLI

```bash
npx @querytorque/cli scan ./src/queries/ --db postgresql
npx @querytorque/cli scan ./src/ --connection "$DATABASE_URL"
npx @querytorque/cli beam ./src/queries/slow.sql --connection "$DATABASE_URL"
npx @querytorque/cli scan ./dbt_project/ --project-type dbt
npx @querytorque/cli scan ./src/ --format sarif --output results.sarif
```

### 14.2 GitHub Action

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
```

---

## 15. Technical Requirements

| Requirement | Minimum |
|-------------|---------|
| VS Code | 1.85+ (or Cursor, Windsurf) |
| Node.js | 18+ |
| OS | Windows 10+, macOS 12+, Linux |
| Python | Not required on client (Beam runs server-side) |
| Network | Required for beams and Fleet Control only. Static analysis works offline. |
| Extension size | <15MB. No sidecar. |

---

## 16. Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Validator LLM false negatives | Medium | Execution-based validation is the final gate. Validator is additive. |
| SQL parser dialect variations | High | Start with PostgreSQL (best tooling). Add incrementally. False negatives > false positives. |
| Copilot competition | High | Copilot generates SQL but doesn't validate against live DB, track cost, or prove speedup. Closed loop is the moat. |
| Credential sensitivity | Medium | Extension-local only. Never sent to API. Read-only mode. Query logging for audit. |
| Beam API latency | Medium | 8 workers parallel. ~15-30s total. Progress spinner with per-worker status. |
| Enterprise security blocks telemetry | Low | Fleet Control opt-in, Team tier only. Core extension fully offline. |
| Qwen model availability | Low | Validator prompt is model-agnostic. Can swap to any instruction model. |

---

## 17. Success Metrics

| Phase | Metric | Target |
|-------|--------|--------|
| 1 (Extension MVP) | Marketplace installs | >500 |
| 1 | Beams executed | >200 |
| 1 | Validation pass rate | >70% |
| 2 (Free Tier) | Weekly active users | >500 |
| 2 | Config recommendations generated | >2,000 |
| 2 | Free→Pro conversion | >5% |
| 3 (Growth) | Pro + Team subscribers | >100 |
| 3 | Total MRR | >$15K |
| 4 (Enterprise) | Fleet Control active orgs | >10 |
| 4 | Gainshare revenue | >$5K/month |

---

## 18. Open Decisions

| Decision | Options | Decide By |
|----------|---------|-----------|
| SQL parser | `node-sql-parser` (broad) vs `libpg-query` WASM (PG-first) | Week 1 |
| Bundle all DB drivers or lazy-load? | ~8MB bundle vs ~2MB + on-demand | Week 1 |
| Validator model | Qwen-72B vs Qwen-32B vs DeepSeek-V3 | Week 2 |
| Beam API | Local Python server vs hosted api.querytorque.com vs both | Week 4 |
| Credential storage | VS Code SecretStorage vs env-only | Week 2 |
| Free beams | 3/month vs 1/month vs 5/month | Week 8 (A/B test) |

---

## Appendix: Verified Speedups

Real, validated speedups from benchmark corpus (5x trimmed mean or 3x3 validation).

### PostgreSQL (DSB — 76 queries)

| Query | Transform | Speedup |
|-------|-----------|---------|
| Q092 | shared_scan_decorrelate | **8,044x** |
| Q032 | inline_decorrelate | **1,465x** |
| Q081 | state_avg_decorrelate | **439x** |
| Q001 | early_filter_decorrelate | **27.80x** |
| Q069 | set_operation_materialization | **17.48x** |
| Q102 | config + pg_hint_plan | **2.22x** |

### DuckDB (TPC-DS — 88 queries)

| Query | Transform | Speedup |
|-------|-----------|---------|
| Q88 | or_to_union + time_bucket | **5.25x** |
| Q9 | single_pass_aggregation | **4.47x** |
| Q40 | multi_cte_chain | **3.35x** |
| Q46 | triple_dimension_isolate | **3.23x** |
| Q35 | intersect_to_exists | **1.78x** |

### Snowflake (TPC-DS SF10)

| Query | Transform | Speedup |
|-------|-----------|---------|
| - | inline_decorrelate | **23.17x** |
| - | shared_scan_decorrelate | **7.82x** |

---

*The Beam is the engine. The extension is the car. The static analysis tells you where to drive. The validation proves you arrived.*

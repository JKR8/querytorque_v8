# QueryTorque for SQL — VS Code Extension
## Product Requirements Document v1.0
### February 2026 | Confidential

---

## 1. Executive Summary

QueryTorque for SQL is a VS Code extension that detects, rewrites, validates, and deploys optimised SQL queries across PostgreSQL, MySQL, DuckDB, Snowflake, and Databricks. It connects directly to the user's database, analyses query plans and execution statistics, generates optimised rewrites, validates them by comparing result sets, and writes the fix back into the source file — all without leaving the editor.

The extension addresses the same closed-loop gap as the DAX edition: existing SQL tools either advise without acting (pgMustard, EverSQL), operate outside the editor (cloud console advisors), or optimise infrastructure rather than query logic (Snowflake Optima, Databricks advisor). No tool reads the SQL where it lives, connects to the live database, fixes the query, proves the fix returns identical results, and writes it back.

> **Architecture shared with DAX edition:** Both extensions share the analysis framework, UX patterns (Model Health panel → Issue Detail → Fix Review → Validation → Write-back), Torque Score system, and Fleet Control data pipeline. The SQL edition replaces PBIP parsing with SQL file detection and XMLA/VertiPaq with database-native query plans and statistics.

---

## 2. The Five-Step Loop (SQL Edition)

Every interaction follows the same closed-loop as the DAX edition, adapted for SQL:

1. **Read SQL on disk** — Detect SQL files in the workspace. Parse queries from `.sql` files, ORM-generated SQL (Django querysets, Rails Active Record, Hibernate HQL), dbt models, stored procedures, and inline SQL in application code (Python, TypeScript, Java, Go, Ruby).

2. **Analyse against live database** — Connect to the database via the user's connection string. Run `EXPLAIN ANALYZE` (Postgres/MySQL), `GET_QUERY_PLAN` (Snowflake), or equivalent. Extract actual execution time, rows scanned, memory used, spill-to-disk events, and cost estimates. Cross-reference with static anti-pattern detection.

3. **Generate fix** — LLM-powered SQL rewriting with full context: the query, table schemas, indexes, statistics, the execution plan, and the detected anti-pattern. Returns rewritten SQL with explanation and expected improvement.

4. **Validate** — Execute original query (with `LIMIT` safety), capture result set hash. Apply rewritten query, capture result set hash. Compare. Same results = green light. Different = show diff, let user decide.

5. **Write back** — Update the SQL in the source file. For ORM-generated SQL, suggest the ORM-level fix (e.g., the Django queryset change that produces the better SQL).

---

## 3. Target Users

### 3.1 Primary: Backend Developer / Data Engineer

Writes SQL directly or via ORMs. Works in VS Code. Has access to a development database. Knows their queries are slow but diagnosing which ones and why requires switching to pgAdmin, DataGrip, or the cloud console, running EXPLAIN manually, reading the plan, figuring out the fix, and testing it. QueryTorque collapses this into a single flow in their editor.

### 3.2 Secondary: dbt Developer

Writes SQL models in dbt. The extension detects dbt project structure and analyses models as SQL. The CI/CD story is particularly strong here — the GitHub Action blocks PRs that introduce slow queries, and the dbt integration means the extension understands model dependencies and materialisation strategies.

### 3.3 Tertiary: Accidental DBA

The developer who got handed database responsibility because the actual DBA left (41% attrition rate in the role). Doesn't deeply understand query plans, index strategies, or join algorithms. Needs the tool to explain what's wrong in plain language and fix it. The approval-based workflow is essential — they need to see, understand, and approve before anything changes.

### 3.4 Fleet Control Buyer: FinOps Manager / Engineering Manager

Never opens VS Code. Sees aggregate cost data across all developers and databases in Fleet Control. Cares about: total query cost trending, cost per team/service, top 10 most expensive queries, savings delivered. This persona justifies the enterprise contract and gainshare model.

---

## 4. Technical Architecture

### 4.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────┐
│  VS Code Extension Host (TypeScript)                    │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │  SQL Parser   │  │  Analysis    │  │  UX Layer    │  │
│  │  & Workspace  │  │  Engine      │  │  (Panels,    │  │
│  │  Scanner      │  │  (shared w/  │  │   CodeLens,  │  │
│  │              │  │   CLI)       │  │   Diagnostics)│ │
│  └──────┬───────┘  └──────┬───────┘  └──────────────┘  │
│         │                 │                             │
│  ┌──────┴─────────────────┴──────┐                     │
│  │  Database Connector Layer      │                     │
│  │  (pg, mysql2, snowflake-sdk,  │                     │
│  │   databricks-sql, duckdb)     │                     │
│  └──────────────┬────────────────┘                     │
│                 │                                       │
│  ┌──────────────┴────────────────┐                     │
│  │  QueryTorque API Client       │                     │
│  │  (Fix generation, Fleet sync) │                     │
│  └───────────────────────────────┘                     │
└─────────────────────────────────────────────────────────┘
         │                              │
         ▼                              ▼
   ┌───────────┐                 ┌──────────────┐
   │  User's   │                 │  QT API      │
   │  Database │                 │  (LLM fixes, │
   │  (live)   │                 │   Fleet data) │
   └───────────┘                 └──────────────┘
```

### 4.2 Key Architecture Difference from DAX Edition

The DAX edition uses a .NET sidecar for XMLA connectivity. The SQL edition does **not** need a sidecar — database drivers for PostgreSQL, MySQL, DuckDB, Snowflake, and Databricks all have mature Node.js/TypeScript implementations that run directly in the extension host process. This makes installation simpler and the extension lighter.

| Component | DAX Edition | SQL Edition |
|-----------|------------|-------------|
| File parsing | PBIP (.dax, model.bim) | .sql files, ORM code, dbt models |
| Live connection | XMLA via .NET sidecar | Native DB drivers (node-postgres, etc.) |
| Performance data | VertiPaq DMVs | EXPLAIN ANALYZE, pg_stat_statements, query history |
| Cost estimation | Memory footprint (MB) | Execution cost ($/query), time (ms), rows scanned |
| Validation | DAX EVALUATE comparison | SELECT comparison with LIMIT safety |
| Write-back target | .dax files on disk | .sql files, ORM code suggestions |

### 4.3 Database Connector Layer

Each supported database has a connector module implementing a common interface:

```typescript
interface DatabaseConnector {
  connect(config: ConnectionConfig): Promise<Connection>;
  explain(query: string): Promise<QueryPlan>;
  execute(query: string, limit?: number): Promise<ResultSet>;
  getTableSchema(table: string): Promise<TableSchema>;
  getIndexes(table: string): Promise<Index[]>;
  getStatistics(table: string): Promise<TableStats>;
  estimateCost(plan: QueryPlan): CostEstimate;
  disconnect(): Promise<void>;
}
```

#### Connector Matrix

| Database | Driver | Plan Command | Stats Source | Cost Model |
|----------|--------|-------------|--------------|------------|
| PostgreSQL | `pg` (node-postgres) | `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` | `pg_stat_statements`, `pg_stat_user_tables` | CPU time + shared buffers × I/O cost |
| MySQL | `mysql2` | `EXPLAIN ANALYZE FORMAT=JSON` (8.0+) | `performance_schema`, `sys` schema | Rows examined × cost per row |
| DuckDB | `duckdb-node` | `EXPLAIN ANALYZE` | Built-in profiling | In-memory; time-based |
| Snowflake | `snowflake-sdk` | `GET_QUERY_PLAN` + query history | `QUERY_HISTORY`, `WAREHOUSE_METERING_HISTORY` | Credits consumed × $/credit |
| Databricks | `@databricks/sql` | `EXPLAIN EXTENDED` | Query history API, `system.billing` | DBU consumed × $/DBU |

### 4.4 Connection Management

Database connections are configured via VS Code settings, `.querytorque.yml` project config, or environment variables. The extension supports multiple simultaneous connections (common in microservices — one service hits Postgres, another hits Snowflake).

```yaml
# .querytorque.yml
connections:
  - name: main-postgres
    type: postgresql
    host: localhost
    port: 5432
    database: myapp_dev
    # Credentials: use env vars, not config file
    username_env: PGUSER
    password_env: PGPASSWORD
    
  - name: analytics-snowflake
    type: snowflake
    account_env: SNOWFLAKE_ACCOUNT
    warehouse: COMPUTE_WH
    database: ANALYTICS
    schema: PUBLIC
    # Auth: key-pair, SSO, or env-based password
    auth: externalbrowser

  - name: local-duckdb
    type: duckdb
    path: ./data/analytics.duckdb
```

**Security requirements:**
- Never store credentials in config files. Always use environment variables, OS keychain, or SSO.
- Extension settings UI provides a "Test Connection" button with no credential echo.
- Connections are scoped to the workspace. Switching workspaces switches connections.
- All queries executed by the extension are logged locally (`.querytorque/query-log.jsonl`) for auditability.
- Read-only connection option: the extension can operate in read-only mode where it only runs EXPLAIN and SELECT, never writes to the database.

### 4.5 SQL File Detection and Parsing

The extension scans the workspace for SQL in multiple contexts:

| Source | Detection | Parsing Approach |
|--------|-----------|-----------------|
| `.sql` files | File extension | Direct SQL parsing (handles multiple statements, CTEs, subqueries) |
| dbt models | `dbt_project.yml` presence | Parse Jinja+SQL; extract compiled SQL via `dbt compile` or by resolving refs |
| Django ORM | `.py` files with `django.db` imports | Detect queryset chains; generate equivalent SQL via static analysis or runtime capture |
| Rails Active Record | `.rb` files with ActiveRecord patterns | Detect query chains; suggest `.to_sql` instrumentation |
| SQLAlchemy | `.py` files with `sqlalchemy` imports | Detect query builder patterns; extract SQL via `str(query)` |
| Hibernate/JPA | `.java` files with `@Query`, `createQuery` | Extract JPQL/HQL; translate to SQL |
| Inline SQL strings | Multi-language | Regex + AST detection of SQL string literals in Python, TypeScript, Go, Java, Ruby |
| Stored procedures | `.sql` files with `CREATE FUNCTION/PROCEDURE` | Parse body SQL within procedure definition |

#### ORM-Specific Handling

When the extension detects ORM-generated SQL, it does two things:

1. **Analyses the SQL** — Same anti-pattern detection and plan analysis as raw SQL.
2. **Maps the fix back to ORM code** — Instead of just showing the rewritten SQL, it shows the ORM-level change. For example:

```
Issue: N+1 query pattern detected
SQL:   SELECT * FROM orders WHERE user_id = 1  (executed 847 times in loop)

Fix (SQL):     SELECT * FROM orders WHERE user_id IN (1, 2, 3, ...)
Fix (Django):  Change  Order.objects.filter(user=user)  (in loop)
         To    Order.objects.filter(user__in=users).select_related('user')
Fix (Rails):   Change  user.orders  (in loop)
         To    User.includes(:orders).where(id: user_ids)
```

This ORM-level mapping is a major differentiator. No other tool does this.

---

## 5. Analysis Engine: Detection and Prioritisation

### 5.1 Anti-Pattern Library

Same YAML-based library architecture as DAX edition. Each pattern defines detection logic, severity, database applicability, and fix template. Initial release targets 30+ patterns across six categories.

### Category 1: Join and Relationship Issues

| ID | Pattern | Detection | Databases | Typical Impact |
|----|---------|-----------|-----------|---------------|
| JN-001 | Missing join predicate (cartesian join) | `EXPLAIN` shows Nested Loop with no condition | All | 10x-1000x row explosion |
| JN-002 | Implicit join syntax (comma joins) | SQL parser detects `FROM a, b WHERE` syntax | All | Readability; often hides missing predicates |
| JN-003 | Join on non-indexed column | `EXPLAIN` shows Seq Scan on join column; index catalog check | PG, MySQL | Full table scan per join |
| JN-004 | Join type mismatch | Column types differ across join (e.g., varchar vs int) | All | Implicit cast prevents index use |
| JN-005 | Unnecessary DISTINCT hiding bad joins | DISTINCT on query with duplicate-producing join | All | Memory/sort overhead masking root cause |
| JN-006 | Left join with right-side filter in WHERE | Filter on left-joined table in WHERE instead of ON | All | Converts to inner join silently |

### Category 2: Index and Scan Issues

| ID | Pattern | Detection | Databases | Typical Impact |
|----|---------|-----------|-----------|---------------|
| IX-001 | Sequential scan on large table | `EXPLAIN` shows Seq Scan, table >10K rows, filter present | PG, MySQL | Full table read |
| IX-002 | Unused index | `pg_stat_user_indexes` shows idx_scan = 0 | PG | Wasted storage, slower writes |
| IX-003 | Function on indexed column | `WHERE LOWER(col)` or `WHERE col + 1 > x` | All | Index bypass |
| IX-004 | Leading wildcard LIKE | `WHERE col LIKE '%term'` | All | Index bypass, full scan |
| IX-005 | Missing composite index | Multiple equality filters; no matching multi-column index | PG, MySQL | Multiple index scans vs one |
| IX-006 | Over-indexing | Table with >10 indexes or index size > table size | PG, MySQL | Write amplification |

### Category 3: Query Structure

| ID | Pattern | Detection | Databases | Typical Impact |
|----|---------|-----------|-----------|---------------|
| QS-001 | SELECT * | Parser detects `SELECT *` in production code | All | Unnecessary I/O, network transfer |
| QS-002 | Correlated subquery replaceable by JOIN | Subquery references outer table | All | N+1 execution at database level |
| QS-003 | N+1 query pattern | Same query template executed >10 times with different parameter | All (via ORM detection) | Network round-trips, connection overhead |
| QS-004 | Unnecessary ORDER BY in subquery | ORDER BY in non-LIMIT subquery/CTE | All | Wasted sort operation |
| QS-005 | UNION instead of UNION ALL | UNION without dedup requirement | All | Unnecessary sort + dedup |
| QS-006 | Nested subqueries replaceable by CTE | >2 levels of subquery nesting | All | Readability, sometimes performance |
| QS-007 | Missing LIMIT on exploratory queries | SELECT without LIMIT in interactive context | All | Full result set transfer |
| QS-008 | Redundant conditions | `WHERE x > 5 AND x > 3` | All | Confusion, no perf impact |

### Category 4: Data Type and Cast Issues

| ID | Pattern | Detection | Databases | Typical Impact |
|----|---------|-----------|-----------|---------------|
| DT-001 | Implicit type cast in comparison | Compare varchar to int | All | Index bypass, full scan |
| DT-002 | Timestamp compared to date without cast | `WHERE timestamp_col = '2024-01-01'` | PG, MySQL | Misses rows after midnight |
| DT-003 | Text column storing numbers | Schema shows varchar with >95% numeric values | All | Cannot aggregate, poor storage |
| DT-004 | Floating point for currency | FLOAT/DOUBLE for monetary values | All | Rounding errors |

### Category 5: Cloud Cost Patterns (Snowflake / Databricks)

| ID | Pattern | Detection | Databases | Typical Impact |
|----|---------|-----------|-----------|---------------|
| CC-001 | Full table scan on large warehouse | Query history shows full scan >1TB | Snowflake | $5+ per query execution |
| CC-002 | Warehouse auto-suspend too long | Warehouse idle minutes > credits consumed | Snowflake | Idle credit burn |
| CC-003 | Missing clustering key | Large table (>1B rows) without clustering, frequent range filters | Snowflake | Excessive micro-partition scanning |
| CC-004 | Unnecessary ORDER BY on large result | ORDER BY on final SELECT returning >100K rows | Snowflake, Databricks | Spill-to-disk, credit burn |
| CC-005 | Repeated identical queries | Same query hash appearing >10x/hour without caching | Snowflake, Databricks | Wasted compute credits |
| CC-006 | Cross-cluster scan | Query spans multiple clusters/zones | Databricks | Network transfer costs |

### Category 6: ORM-Specific Patterns

| ID | Pattern | Detection | Databases | Typical Impact |
|----|---------|-----------|-----------|---------------|
| ORM-001 | N+1 query (loop-detected) | Same query template in loop with varying parameter | All | 100x+ round-trips |
| ORM-002 | Missing select_related / eager load | Foreign key access triggering lazy load | All | N+1 variant |
| ORM-003 | Unfiltered .all() on large table | ORM fetches entire table into application memory | All | Memory explosion, slow response |
| ORM-004 | Count query followed by data query | `.count()` then `.all()` (two queries for one operation) | All | Redundant database round-trip |
| ORM-005 | Raw SQL inside ORM bypassing optimiser | `raw()` or `execute()` with hand-written SQL | All | Skips ORM query optimisation, injection risk |

### 5.2 Torque Score (SQL Edition)

Same 0–100 scoring system as DAX edition. The weighting shifts from memory impact to cost impact:

```
Issue Score = (Rule Severity × 0.2) + (Execution Cost × 0.4) + (Rows Scanned × 0.2) + (Frequency × 0.2)
```

- **Execution Cost**: Derived from EXPLAIN ANALYZE (time-based for Postgres/MySQL) or credit-based (Snowflake/Databricks).
- **Rows Scanned**: Actual rows examined vs rows returned. A ratio >100:1 is a strong signal.
- **Frequency**: How often this query runs (from `pg_stat_statements`, Snowflake query history, or ORM instrumentation). A query running 10,000x/day with 2ms waste = 20 seconds/day = worth fixing.

### 5.3 Prioritisation Output

Identical three-tier system:

- **Fix Now (Red):** Issues costing >$100/month or >10 seconds cumulative daily execution time.
- **Fix Soon (Amber):** Issues costing $10–100/month or introducing unnecessary load.
- **Improve (Blue):** Best-practice violations, readability improvements, minor inefficiencies.

For cloud databases (Snowflake, Databricks), cost estimates are in actual dollars using the customer's credit rate. For Postgres/MySQL, cost is expressed as execution time and I/O, with a configurable $/hour rate for the underlying compute.

---

## 6. Fix Generation Engine

### 6.1 Context Payload

When the user requests a fix, the engine builds a rich context for the LLM:

| Context Element | Source | Purpose |
|----------------|--------|---------|
| Original SQL | Source file | The query to rewrite |
| Execution plan | EXPLAIN ANALYZE | What the database is actually doing |
| Table schemas | information_schema / catalog queries | Column names, types, constraints |
| Indexes | pg_indexes / SHOW INDEX / equivalent | Available index paths |
| Table statistics | pg_stat_user_tables / equivalent | Row counts, dead tuples, last vacuum |
| Detected anti-pattern | Analysis engine | What rule triggered and why |
| Query frequency | pg_stat_statements / query history | How often this runs (priority context) |
| ORM context (if applicable) | Source file AST | The ORM code generating this SQL |
| Database version | Connection metadata | Version-specific syntax and features |

### 6.2 Fix Response Structure

```json
{
  "rewritten_sql": "SELECT o.id, o.total FROM orders o JOIN users u ON u.id = o.user_id WHERE u.region = 'APAC' AND o.created_at > '2025-01-01'",
  "explanation": "Replaced correlated subquery with JOIN. The original executed the subquery once per row in the outer table (847K executions). The JOIN allows the planner to use a Hash Join with a single pass over both tables.",
  "expected_improvement": {
    "execution_time": "from ~4.2s to ~120ms (estimated)",
    "rows_scanned": "from 847K × 12K to 847K + 12K",
    "cost_monthly": "from ~$340/mo to ~$8/mo (at current frequency)"
  },
  "confidence": "high",
  "orm_fix": {
    "framework": "django",
    "original": "Order.objects.filter(user__region='APAC', created_at__gt='2025-01-01')",
    "rewritten": "Order.objects.select_related('user').filter(user__region='APAC', created_at__gt='2025-01-01')",
    "explanation": "Added select_related to perform JOIN at database level instead of lazy-loading user objects."
  },
  "validation_query": "SELECT * FROM ({original}) a FULL OUTER JOIN ({rewritten}) b ON a.id = b.id WHERE a.id IS NULL OR b.id IS NULL LIMIT 100",
  "index_suggestions": [
    "CREATE INDEX idx_orders_user_created ON orders (user_id, created_at)"
  ]
}
```

### 6.3 Fix Categories

**Category A: Equivalent Rewrites (Result-Preserving)**

Rewritten SQL must produce identical results. Examples: replacing correlated subqueries with JOINs, converting UNION to UNION ALL where no duplicates exist, rewriting function-on-column to sargable form. Full validation loop applies.

**Category B: Index Recommendations (Schema-Altering)**

Suggested indexes that would improve the query. These don't change the SQL — they change the schema. The extension generates the `CREATE INDEX` statement but requires explicit approval. Validates by running EXPLAIN on the original query before and after index creation (on dev database only).

**Category C: ORM-Level Fixes (Application Code)**

Suggested changes to ORM code that produce better SQL. These are presented as code suggestions in the application source file (Python, Ruby, Java) rather than the SQL file. Cannot be auto-validated through SQL comparison — the extension suggests the change and explains the impact, but the developer applies it manually.

**Category D: Cloud Configuration (Infrastructure)**

Snowflake warehouse sizing, clustering keys, materialised view suggestions. Presented as recommendations with cost projections. Not auto-applied — these require admin privileges and go through Fleet Control for team-level decisions.

---

## 7. Validation Framework

### 7.1 Validation Process for Category A Fixes

Same philosophy as DAX edition — execute original, apply fix, execute again, compare — adapted for SQL:

1. **Build validation query:** Wrap both original and rewritten SQL in a comparison query. For simple cases, use `EXCEPT` (set difference). For ordered results, use row-number comparison. Apply `LIMIT` safety (default: 10,000 rows) to prevent accidental full-table-result comparison on production.

2. **Execute original:** Run the original query with `LIMIT`. Capture result set, compute row-level hash.

3. **Execute rewrite:** Run the rewritten query with same `LIMIT` and parameters. Capture result set, compute hash.

4. **Compare:** If hashes match, validation passes. If they differ, compute row-level diff showing which rows are missing, added, or changed. Present the diff to the user.

5. **Plan comparison:** Additionally compare EXPLAIN plans for original vs rewrite. Show side-by-side with highlighted improvements (Seq Scan → Index Scan, Nested Loop → Hash Join, etc.).

### 7.2 Safety Mechanisms

| Safety Concern | Handling |
|---------------|---------|
| Query modifies data (INSERT/UPDATE/DELETE) | Validation runs inside a `BEGIN ... ROLLBACK` transaction. No data is ever written during validation. |
| Query is extremely slow (>30s) | Timeout with progress bar. User can extend timeout or skip validation. Original query is killed via `pg_cancel_backend` or equivalent. |
| Query returns millions of rows | `LIMIT` safety cap (configurable, default 10,000). Warn user that validation covers a subset. |
| Query uses non-deterministic functions (RANDOM, NOW) | Detect in AST. Pin `NOW()` via `SET` statement. Flag `RANDOM()` as non-validatable. |
| Multiple queries in a transaction | Validate each independently. Transaction-level validation is out of scope for v1. |
| Query runs against production | Extension warns if connection is flagged as production. Recommend running validation against dev/staging only. |
| Snowflake/Databricks credit consumption | Estimate credit cost of validation queries before execution. Show estimate, require approval for >$1. |

### 7.3 Validation UX

Identical to DAX edition. Progress panel showing each step with spinners/checkmarks. On success: "Validated: 10,000 rows compared, all identical. Execution time improved from 4.2s → 120ms. Estimated monthly savings: $332." On failure: row-level diff viewer.

---

## 8. VS Code Extension User Experience

### 8.1 Extension Activation

Activates when VS Code opens a workspace containing `.sql` files, a `dbt_project.yml`, or a `.querytorque.yml` configuration. Also activates when the user opens any file containing detected SQL patterns.

### 8.2 Primary Views

**View 1: Database Health Panel (Activity Bar)**

The QueryTorque icon in the activity bar opens the main panel. It shows:

- **Connection status:** Which databases are connected, latency indicator, database version.
- **Workspace Torque Score:** Aggregate score across all detected SQL in the workspace.
- **Issue tree:** Grouped by priority (Fix Now / Fix Soon / Improve), then by file/query. Each node shows the pattern name, affected query (truncated), estimated cost impact.
- **Top Expensive Queries:** Ranked by cost ($/month) or execution time, pulled from `pg_stat_statements` or cloud query history.
- **Quick actions:** "Scan Workspace", "Connect to Database", "Fix All Safe Issues".

**View 2: Issue Detail Panel**

Clicking an issue opens the detail view:

- **Issue description:** Plain-language explanation of the anti-pattern.
- **Current SQL:** Syntax-highlighted with the problematic section underlined.
- **Execution plan:** Visual representation of the query plan (tree view with node costs).
- **Impact estimate:** "This query runs 2,400 times/day. Each execution scans 847K rows and takes 4.2s. Monthly cost: ~$340 on your current Snowflake warehouse."
- **"Generate Fix" button** (paid tier) or manual guidance (free tier).

**View 3: Fix Review Panel (Side-by-Side)**

After fix generation:

- **Left:** Original SQL with problems highlighted in red.
- **Right:** Rewritten SQL with changes highlighted in green.
- **Below:** Explanation text, confidence level, expected improvement metrics.
- **Plan comparison:** Side-by-side execution plans showing before/after.
- **ORM fix panel** (if applicable): Shows the application-code-level change alongside the SQL change.
- **Action buttons:** "Validate and Apply", "Apply Without Validation", "Reject", "Edit Fix".

**View 4: Query Cost Explorer**

A dedicated view showing the most expensive queries across the connected database(s), ranked by total monthly cost. Each query links to its source file (if found in the workspace) and shows issue count and fix availability. This is the "discovery" view — users find problems they didn't know they had.

### 8.3 Editor Integration

- **Diagnostics:** Squiggly underlines on problematic SQL in `.sql` files and inline SQL strings. Hover shows issue description and cost impact.
- **Code Actions:** Lightbulb menu offers "QueryTorque: Fix this query" and "QueryTorque: Explain this query plan".
- **CodeLens:** Above each SQL query (in `.sql` files and dbt models): "Torque: 72 | Cost: $34/mo | 3 issues | Last run: 2.4s"
- **Status bar:** Workspace Torque Score, active connection indicator, total monthly cost of detected issues.
- **Inline cost annotations:** After running a scan, each query gets an inline annotation showing its per-execution cost and monthly cost at current frequency.

### 8.4 Command Palette

```
QueryTorque: Scan Current File
QueryTorque: Scan Workspace
QueryTorque: Connect to Database
QueryTorque: Disconnect
QueryTorque: Show Query Cost Explorer
QueryTorque: Explain Query at Cursor
QueryTorque: Fix Query at Cursor
QueryTorque: Fix All Safe Issues in File
QueryTorque: Export Report (HTML)
QueryTorque: Export Report (JSON)
QueryTorque: Show Torque Score History
QueryTorque: Open Fleet Control Dashboard
```

---

## 9. dbt Integration

dbt is a first-class citizen, not an afterthought. Many SQL-focused developers work exclusively in dbt.

### 9.1 dbt Project Detection

When `dbt_project.yml` is present in the workspace, the extension:

1. Identifies model files in `models/`, `analyses/`, and `macros/` directories.
2. Resolves `{{ ref('model_name') }}` and `{{ source('source', 'table') }}` to actual table names using `manifest.json` (if available from a prior `dbt compile`) or by parsing `schema.yml` files.
3. Compiles Jinja templates to raw SQL for analysis (calls `dbt compile` if the dbt CLI is available, otherwise uses built-in ref resolution).

### 9.2 dbt-Specific Analysis

| Feature | Description |
|---------|-------------|
| Materialisation analysis | Flag models materialised as `table` that could be `view` (low row count, infrequent access) or vice versa |
| Unused model detection | Models not referenced by any downstream model or exposure |
| Incremental model validation | Check that `is_incremental()` blocks have correct merge keys and aren't doing full-table scans |
| Source freshness vs query cost | Correlate source freshness with query frequency — stale sources with expensive downstream models |

### 9.3 dbt CI/CD

The GitHub Action understands dbt project structure:

```yaml
# In GitHub Action
- name: QueryTorque Scan
  uses: querytorque/scan-action@v1
  with:
    project_type: dbt
    dbt_target: dev
    min_score: 70
    block_on_new_issues: true
```

---

## 10. Fleet Control Data Pipeline

### 10.1 What the Extension Reports to Fleet Control

When opted in (team/enterprise tier), the extension sends anonymised telemetry to the Fleet Control API:

| Data Point | Granularity | Purpose |
|-----------|-------------|---------|
| Torque Score per workspace | Daily snapshot | Trend tracking per project |
| Issue count by severity | Per scan | Aggregate health across org |
| Cost estimates per query | Per scan | Total org waste calculation |
| Fix acceptance rate | Per fix | Fix engine quality metrics |
| Validation pass rate | Per validation | Confidence in automation |
| Database types in use | Per connection | Platform mix visibility |

### 10.2 What Fleet Control Does NOT Receive

- Actual SQL queries (never sent)
- Query results or data (never sent)
- Connection credentials (never sent)
- Source code (never sent)
- Individual developer identifiers without consent

The extension sends only aggregate metrics and anonymised pattern IDs. Enterprise customers can route Fleet Control data through their own proxy for additional filtering.

### 10.3 Fleet Control Features (Web Dashboard)

- **Org-wide Torque Score** trending over time
- **Cost leaderboard:** Top queries by monthly cost across all developers
- **Team health:** Per-team scores, issue trends, fix adoption rates
- **Gainshare tracking:** Total savings delivered vs baseline (for gainshare contracts)
- **Compliance:** % of PRs passing Torque Score gates, deployment block rate

Fleet Control is out of scope for this PRD but is referenced here to show how extension data feeds the enterprise product.

---

## 11. Free and Paid Tier Structure

### 11.1 Tier Matrix

| Feature | Free | Pro ($29/mo) | Team ($99/mo/seat) |
|---------|------|-------------|-------------------|
| SQL file scanning | Yes | Yes | Yes |
| Anti-pattern detection (all patterns) | Top 5 issues only | All issues | All issues |
| Database connection | 1 connection | 3 connections | Unlimited |
| EXPLAIN plan analysis | Yes | Yes | Yes |
| Cost estimation | Basic (time only) | Full ($/month with frequency) | Full |
| ORM detection | Detection only | Detection + ORM fix suggestions | Full |
| dbt integration | Basic (file scanning) | Full (ref resolution, materialisation analysis) | Full |
| LLM fix generation | No | 50 fixes/month | Unlimited |
| Automated validation | No | Yes | Yes |
| Write-back to source files | No | Yes | Yes |
| Index recommendations | Show recommendation | Generate DDL + validate | Full + Fleet Control |
| Query Cost Explorer | Top 5 queries | Full | Full + historical trending |
| CI/CD (GitHub Action) | Score gate only | Score + issue blocking | Full config + Fleet Control |
| Export report | Watermarked HTML | Full HTML/JSON | Full + custom branding |
| Fleet Control sync | No | No | Yes |

### 11.2 Conversion Triggers

Same natural upgrade moments as DAX edition:

- **"Generate Fix" on a high-cost query:** "This query costs ~$340/month. Upgrade to Pro to auto-generate and validate the fix."
- **6th issue hidden:** "Your workspace has 14 issues. Upgrade to see all and auto-fix them."
- **2nd database connection:** "Pro supports up to 3 database connections."
- **Export:** Free tier watermarked, Pro removes it.

### 11.3 Distribution Channels

| Channel | Audience | Notes |
|---------|----------|-------|
| VS Code Marketplace | Primary distribution | Free extension; billing via Stripe |
| Open VSX | Cursor, Gitpod users | Same extension |
| GitHub Marketplace | CI/CD users | For the GitHub Action |
| AWS Marketplace | Enterprise procurement | Metered billing against committed spend (GTM Move 3) |
| Snowflake Marketplace | Snowflake-heavy orgs | Native App Framework integration |
| Direct (querytorque.com) | Content-led visitors | Lowest fee path |

---

## 12. CLI and CI/CD Integration

### 12.1 CLI

Shared npm package with the DAX edition CLI:

```bash
# Scan SQL files
npx @querytorque/cli scan ./src/queries/ --db postgresql

# Scan with live database connection (for plan analysis)
npx @querytorque/cli scan ./src/ --connection "postgresql://user:pass@localhost/mydb"

# Scan dbt project
npx @querytorque/cli scan ./dbt_project/ --project-type dbt --target dev

# Output formats
npx @querytorque/cli scan ./src/ --format json --output report.json
npx @querytorque/cli scan ./src/ --format sarif --output results.sarif

# Just the score
npx @querytorque/cli score ./src/
```

### 12.2 GitHub Action

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
            - pattern: JN-001  # Never allow cartesian joins
          sarif_output: results.sarif
      - uses: github/codeql-action/upload-sarif@v3
        with:
          sarif_file: results.sarif
```

### 12.3 Configuration (.querytorque.yml)

```yaml
version: 1

scan:
  paths:
    - ./src/queries/
    - ./dbt_project/models/
  exclude:
    - ./src/queries/legacy/    # Don't scan legacy SQL
    - "**/test_*.sql"          # Don't scan test fixtures
  
  min_score: 60
  block_on:
    - severity: critical
    - pattern: JN-001
    - pattern: CC-001
  ignore:
    - pattern: QS-001          # Allow SELECT * in this project
    - file: "migrations/*.sql" # Don't flag migration files

connections:
  - name: dev-postgres
    type: postgresql
    host_env: PGHOST
    database_env: PGDATABASE
    username_env: PGUSER
    password_env: PGPASSWORD

reporting:
  format: html
  output: ./reports/torque-report.html
  fleet_control: true          # Sync results to Fleet Control (team tier)
```

---

## 13. Technical Requirements

### 13.1 Runtime Requirements

| Requirement | Minimum | Recommended | Notes |
|-------------|---------|-------------|-------|
| VS Code | 1.85+ | Latest | Or Cursor, VS Code Insiders |
| Node.js | 18+ | 20 LTS | For extension host and database drivers |
| OS | Windows 10+, macOS 12+, Linux (Ubuntu 20.04+) | Latest | Cross-platform from day one (no .NET sidecar needed) |
| Memory | 2GB available | 4GB+ | Depends on workspace size and query plan complexity |
| Network | Required for LLM fixes and Fleet Control only | — | Static analysis works fully offline |

### 13.2 Technology Stack

| Component | Technology | Rationale |
|-----------|-----------|-----------|
| Extension framework | VS Code Extension API (TypeScript) | Standard extension model |
| SQL parser | `sql-parser-cst` or `node-sql-parser` | Multi-dialect SQL AST for static analysis |
| PostgreSQL driver | `pg` (node-postgres) | Mature, well-maintained, async |
| MySQL driver | `mysql2` | Promise-based, prepared statements |
| Snowflake driver | `snowflake-sdk` | Official Snowflake connector |
| Databricks driver | `@databricks/sql` | Official Databricks connector |
| DuckDB driver | `duckdb-node` or `@duckdb/node-api` | Native bindings |
| Analysis engine | TypeScript (shared with CLI and DAX edition) | Single codebase for all platforms |
| Anti-pattern library | YAML definitions | Shared with DAX edition pattern format |
| Fix engine (LLM) | QueryTorque API (REST) | Server-side, proprietary prompt engineering |
| Billing | Stripe | Standard SaaS billing; licence key in extension |
| Telemetry | PostHog (optional, off by default) | Product analytics |

### 13.3 Extension Bundle Size

Target: <15MB installed. No sidecar binary needed (unlike DAX edition). All database drivers are npm packages bundled via webpack. Tree-shaking removes unused database drivers based on configuration.

---

## 14. Development Phases

### Phase 1: Foundation (Weeks 1–4)

Goal: Extension loads, detects SQL files, shows anti-patterns with Torque Score. No live database connection yet.

| ID | Task | Effort | Dependencies |
|----|------|--------|-------------|
| S1-01 | VS Code extension scaffold (TypeScript, webpack, CI) | S | Shared with DAX edition |
| S1-02 | SQL file scanner (detect .sql, inline SQL, dbt models) | M | — |
| S1-03 | SQL parser integration (multi-dialect AST) | L | — |
| S1-04 | Anti-pattern library (YAML schema, 15 initial patterns) | M | Shared format with DAX |
| S1-05 | Static analysis engine: detect patterns against AST | L | S1-03, S1-04 |
| S1-06 | Torque Score calculation (static-only mode) | S | S1-05 |
| S1-07 | Database Health panel (tree view, score gauge) | M | S1-05 |
| S1-08 | Editor diagnostics (squiggly lines in .sql files) | M | S1-05 |
| S1-09 | HTML report export (watermarked free tier) | M | S1-05 |
| S1-10 | VS Code Marketplace listing and publish CI | S | S1-01 |

**Deliverable:** Free, cross-platform VS Code extension that scans SQL files for anti-patterns. No database connection required. This is the install-base builder.

### Phase 2: Live Connection (Weeks 5–8)

Goal: Connect to databases, enrich analysis with real execution plans and statistics.

| ID | Task | Effort | Dependencies |
|----|------|--------|-------------|
| S2-01 | Database connector interface (common abstraction) | M | — |
| S2-02 | PostgreSQL connector (pg driver, EXPLAIN ANALYZE, pg_stat) | L | S2-01 |
| S2-03 | MySQL connector (mysql2, EXPLAIN ANALYZE, perf schema) | M | S2-01 |
| S2-04 | DuckDB connector (duckdb-node, EXPLAIN ANALYZE) | M | S2-01 |
| S2-05 | Snowflake connector (SDK, GET_QUERY_PLAN, query history) | L | S2-01 |
| S2-06 | Databricks connector (SQL connector, EXPLAIN EXTENDED) | L | S2-01 |
| S2-07 | Connection management UX (settings, test, multi-connection) | M | S2-01 |
| S2-08 | Cross-reference static analysis with live plan data | L | S1-05, S2-02+ |
| S2-09 | Cost estimation engine (time-based and credit-based) | M | S2-08 |
| S2-10 | Query Cost Explorer view | M | S2-09 |
| S2-11 | CodeLens: per-query cost and score | S | S2-09 |
| S2-12 | Security: credential handling, read-only mode, query logging | M | S2-01 |

**Deliverable:** Extension with live database connection. Shows real execution plans, actual costs, and query frequency data. The Torque Score is now backed by real numbers.

### Phase 3: Fix and Validate (Weeks 9–14)

Goal: LLM-powered SQL rewriting with automated validation. Paid tier goes live.

| ID | Task | Effort | Dependencies |
|----|------|--------|-------------|
| S3-01 | QueryTorque API: SQL fix generation endpoint | L | Server-side |
| S3-02 | Context payload builder (SQL + schema + plan + stats) | M | S2-08 |
| S3-03 | Fix Review panel (split diff, explanation, plan comparison) | L | S3-01 |
| S3-04 | Validation framework: execute, compare, diff | XL | S2-02+, S3-03 |
| S3-05 | Transaction-safe validation (BEGIN/ROLLBACK wrapping) | M | S3-04 |
| S3-06 | Write-back to .sql files | M | S3-04 |
| S3-07 | ORM fix mapping (Django, Rails, SQLAlchemy) | L | S3-01 |
| S3-08 | Index recommendation engine | M | S2-02+ |
| S3-09 | Stripe billing and licence key validation | M | Shared with DAX |
| S3-10 | Upgrade prompts at conversion trigger points | S | S3-09 |
| S3-11 | dbt full integration (ref resolution, materialisation analysis) | L | S1-02, S2-08 |

**Deliverable:** Complete read–analyse–fix–validate–write loop for SQL. ORM-level fix suggestions. Pro tier live. Ship it.

### Phase 4: CI/CD, Fleet, and Scale (Weeks 15–20)

Goal: CLI, GitHub Action, team tier, marketplace listings, Fleet Control integration.

| ID | Task | Effort | Dependencies |
|----|------|--------|-------------|
| S4-01 | CLI package (@querytorque/cli SQL mode) | M | S1-05, shared with DAX |
| S4-02 | GitHub Action: PR comments, SARIF, status checks | L | S4-01 |
| S4-03 | .querytorque.yml full schema and parser | M | S4-01 |
| S4-04 | Team tier: seat management, shared config | M | S3-09, shared with DAX |
| S4-05 | Fleet Control data pipeline (extension → API) | L | S4-04 |
| S4-06 | AWS Marketplace submission | L | S3-all |
| S4-07 | Snowflake Marketplace / Native App submission | L | S2-05 |
| S4-08 | Expand anti-pattern library to 40+ patterns | M | Ongoing |
| S4-09 | Performance benchmarking on large workspaces (1000+ .sql files) | M | S1-05 |
| S4-10 | Cross-database comparison: same query on PG vs Snowflake | M | S2-02, S2-05 |

**Deliverable:** Full product suite. VS Code extension, CLI, GitHub Action, marketplace listings, team tier with Fleet Control sync.

---

## 15. Shared Infrastructure with DAX Edition

The two extensions share significant infrastructure. Engineering should build these as shared packages from day one:

| Shared Component | Package | Used By |
|-----------------|---------|---------|
| Anti-pattern library format (YAML schema, loader, validator) | `@querytorque/patterns` | Both editions, CLI |
| Torque Score calculator | `@querytorque/scoring` | Both editions, CLI, Fleet Control |
| VS Code UX components (Health panel, Fix Review, Validation progress) | `@querytorque/vscode-ui` | Both editions |
| Report generator (HTML, JSON, SARIF) | `@querytorque/reports` | Both editions, CLI, GitHub Action |
| Stripe billing client and licence validation | `@querytorque/billing` | Both editions |
| Fleet Control API client | `@querytorque/fleet-client` | Both editions |
| CLI framework and command structure | `@querytorque/cli` | Both editions |
| GitHub Action wrapper | `querytorque/scan-action` | Both editions |

Monorepo structure recommended:

```
querytorque/
├── packages/
│   ├── patterns/           # Shared anti-pattern library
│   ├── scoring/            # Torque Score engine
│   ├── vscode-ui/          # Shared VS Code components
│   ├── reports/            # Report generation
│   ├── billing/            # Stripe integration
│   ├── fleet-client/       # Fleet Control API client
│   ├── cli/                # CLI framework
│   ├── dax-extension/      # DAX Edition VS Code extension
│   ├── sql-extension/      # SQL Edition VS Code extension
│   └── ssas-sidecar/       # .NET sidecar (DAX only)
├── patterns/
│   ├── dax/                # DAX anti-pattern YAML files
│   └── sql/                # SQL anti-pattern YAML files
└── actions/
    └── scan-action/        # GitHub Action
```

---

## 16. Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Database driver compatibility across versions | Medium | High | Pin driver versions. Extensive integration testing matrix. Community bug reports via GitHub issues. |
| LLM generates syntactically invalid SQL | Medium | Medium | Post-fix SQL parsing validation. Run EXPLAIN (without ANALYZE) as cheap syntax check before full validation. |
| Snowflake/Databricks credit cost of validation queries | Medium | Medium | Estimate cost before execution. Require approval for >$1. Default to EXPLAIN-only (no ANALYZE) on cloud warehouses. |
| Enterprise security blocks extension telemetry | Low | Low | Fleet Control is opt-in. Extension works fully offline except for LLM fixes. On-prem API option for enterprises. |
| SQL parser doesn't handle all dialect variations | High | Medium | Start with PostgreSQL parser (best tooling). Add dialect-specific parsing incrementally. Accept false negatives over false positives — better to miss an issue than to report a wrong one. |
| Competition from GitHub Copilot SQL features | Medium | High | Copilot generates SQL but doesn't analyse cost, validate against live databases, or track organisational waste. QueryTorque's moat is the closed-loop validation and Fleet Control cost attribution. |
| dbt integration fragility (Jinja parsing, manifest dependency) | Medium | Medium | Graceful degradation: if `manifest.json` is missing, analyse SQL without ref resolution. Offer `dbt compile` integration for full fidelity. |
| Low adoption due to credential sensitivity | Medium | High | Extension-local credentials (never sent to API). Read-only mode. Full query logging. SOC 2 compliance for Fleet Control. Open-source the connector layer for auditing. |

---

## 17. Success Metrics

### Phase 1 (Weeks 1–4)

| Metric | Target | Measurement |
|--------|--------|-------------|
| VS Code Marketplace installs | >1,000 | Marketplace analytics |
| Weekly active users | >200 | Extension telemetry (opt-in) |
| Files scanned | >5,000 | Local event count |
| Cross-platform adoption (Win/Mac/Linux split) | >20% non-Windows | Install analytics |

### Phase 3 (Weeks 9–14)

| Metric | Target | Measurement |
|--------|--------|-------------|
| Free-to-Pro conversion | >5% | Stripe + install base |
| Fixes generated per Pro user/month | >15 | API logs |
| Validation pass rate (Category A) | >85% | Validation telemetry |
| Database types connected (% PostgreSQL) | >60% PG | Connection analytics |
| Pro MRR | >$8K | Stripe |

### Phase 4 (Weeks 15–20)

| Metric | Target | Measurement |
|--------|--------|-------------|
| Marketplace installs | >5,000 | Marketplace analytics |
| Pro + Team subscribers | >100 | Stripe |
| GitHub Action installs | >200 repos | GitHub Marketplace |
| Fleet Control active orgs | >10 | Fleet Control analytics |
| Total MRR (SQL + DAX combined) | >$25K | Stripe |

---

## 18. Open Questions for Engineering

| Question | Options | Decision Impact | Decide By |
|----------|---------|----------------|-----------|
| SQL parser: `node-sql-parser` vs `sql-parser-cst` vs `libpg-query`? | node-sql-parser has broadest dialect support; libpg-query has highest PG fidelity | Multi-dialect accuracy vs Postgres depth | Week 1 |
| Should the extension bundle all 5 database drivers or lazy-load? | Bundle all (~8MB) vs download on first connection (~2MB base) | Install size vs first-connection latency | Week 1 |
| ORM detection: static AST analysis vs runtime instrumentation? | Static is zero-config; runtime captures actual SQL | Accuracy vs setup friction | Week 8 |
| dbt integration: depend on dbt CLI or build independent parser? | CLI dependency gives full Jinja resolution; independent parser reduces setup | Accuracy vs portability | Week 5 |
| Credential storage: VS Code SecretStorage vs OS keychain vs env only? | SecretStorage is simplest; OS keychain is most secure; env-only is most transparent | Security posture vs UX | Week 2 |
| Should validation run on the connected DB or spin up a local copy? | Connected DB is simpler; local copy (via Docker) is safer for production connections | Safety vs complexity | Week 9 |
| How to handle stored procedures and functions? | Analyse body SQL only vs full control flow analysis | Depth vs engineering cost | Phase 2 |
| Multi-statement SQL files: analyse each statement independently or as a batch? | Independent is simpler; batch captures transaction context | Accuracy for migration files | Week 3 |

---

*End of document. QueryTorque for SQL — VS Code Extension PRD v1.0*

# QueryTorque V8 Prompt Specification

Master reference for all prompt builders — inputs, outputs, constraints, and token budgets.

## 1. Script Oneshot — `build_script_oneshot_prompt()`

| Field | Value |
|-------|-------|
| **File** | `prompts/analyst_briefing.py:1605` |
| **Purpose** | Optimize a full multi-statement SQL pipeline end-to-end |
| **Dialect** | DuckDB / PG / both |

### Parameters

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `sql_script` | `str` | Yes | Full SQL script text (all statements) |
| `script_dag` | `ScriptDAG` | Yes | Dependency graph from `ScriptParser.parse()` |
| `dialect` | `str` | No | SQL dialect (default: `"duckdb"`) |
| `explain_plans` | `Dict[str, str]` | No | Map of object_name → EXPLAIN ANALYZE text |
| `engine_profile` | `Dict[str, Any]` | No | Engine optimizer strengths/gaps JSON |
| `matched_examples` | `List[Dict]` | No | Gold examples for pattern matching |
| `constraints` | `List[Dict]` | No | Correctness constraints |
| `regression_warnings` | `List[Dict]` | No | Known regression patterns |

### Output Sections

1. Role framing (pipeline optimization architect)
2. Pipeline Dependency Graph (from `script_dag.summary()`)
3. Key Optimization Chains (lineage for optimization targets)
4. EXPLAIN ANALYZE Plans (per target, if provided)
5. Cross-Statement Optimization Opportunities (5 patterns)
6. Engine Profile (compact, if provided)
7. Optimization Examples (up to 5, if provided)
8. Complete SQL Pipeline (full script with line numbers)
9. Analysis Steps (5-step chain)
10. Output Format (`rewrite_sets` JSON with `target` field + `cross_statement_reasoning`)
11. Pipeline Validation Checklist

### Constraints Applied
- No formal correctness constraints — pipeline-level semantic equivalence enforced via checklist

### Token Budget
- ~2000–6000 tokens depending on script length and number of EXPLAIN plans

---

## 2. Oneshot Query — `build_analyst_briefing_prompt(mode="oneshot")`

| Field | Value |
|-------|-------|
| **File** | `prompts/analyst_briefing.py:653` |
| **Purpose** | Analyze single query + produce optimized SQL directly |
| **Dialect** | DuckDB / PG / both |

### Parameters

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query_id` | `str` | Yes | Query identifier (e.g., `"query_88"`) |
| `sql` | `str` | Yes | Original SQL query |
| `explain_plan_text` | `str` | No | EXPLAIN ANALYZE tree text |
| `dag` | `Any` | Yes | Parsed logical tree from Phase 1 |
| `costs` | `Dict[str, Any]` | Yes | Per-node cost analysis |
| `semantic_intents` | `Dict` | No | Pre-computed per-query intents |
| `global_knowledge` | `Dict` | No | GlobalKnowledge (principles + anti-patterns) |
| `matched_examples` | `List[Dict]` | Yes | Top tag-matched examples (full metadata) |
| `all_available_examples` | `List[Dict]` | Yes | Full catalog (id + speedup + description) |
| `constraints` | `List[Dict]` | Yes | All engine-filtered constraints |
| `regression_warnings` | `List[Dict]` | No | Tag-matched regression examples |
| `dialect` | `str` | No | SQL dialect (default: `"duckdb"`) |
| `dialect_version` | `str` | No | Engine version string |
| `strategy_leaderboard` | `Dict` | No | Pre-built leaderboard JSON |
| `query_archetype` | `str` | No | Archetype classification |
| `engine_profile` | `Dict` | No | Engine profile JSON |
| `resource_envelope` | `str` | No | PG system resource envelope text |
| `exploit_algorithm_text` | `str` | No | Evidence-based exploit algorithm YAML |
| `plan_scanner_text` | `str` | No | Pre-computed plan-space scanner results (PG) |
| `iteration_history` | `Dict` | No | Prior optimization attempts |
| `mode` | `str` | Yes | Must be `"oneshot"` |

### Output Sections

1. Role (analyze + produce SQL directly)
2. Query SQL with line numbers
3. EXPLAIN plan (DuckDB tree or PG tree, with timing notes)
4. Plan-Space Scanner Intelligence (PG only, if provided)
5. Query Structure (Logical Tree) node cards
6. Pre-Computed Semantic Intent (if available)
7. Aggregation Semantics Check
8. Tag-Matched Examples (full metadata)
9. Additional Examples (compact, not tag-matched)
10. Optimization Principles (from benchmark history)
11. Regression Examples
12. Iteration History (if iterative mode)
13. Engine Profile or Exploit Algorithm
14. Resource Envelope (PG only)
15. Correctness Constraints (4 gates)
16. Reasoning chain (7 steps: CLASSIFY → EXPLAIN → GAP MATCHING → AGG TRAP → TRANSFORM → LOGICAL TREE DESIGN → WRITE REWRITE)
17. Output format: `=== SHARED BRIEFING ===` + `=== REWRITE ===` (JSON rewrite_set)
18. Section validation checklist (oneshot)
19. Transform Catalog (full)
20. Strategy Leaderboard (if available)
21. Strategy Selection Rules

### Constraints Applied
- 4 correctness gates: LITERAL_PRESERVATION, SEMANTIC_EQUIVALENCE, COMPLETE_OUTPUT, CTE_COLUMN_COMPLETENESS
- Aggregation semantics check (STDDEV_SAMP, AVG duplicate-safety)

### Token Budget
- ~3500–6000 tokens input

---

## 3. Expert Analyst — `build_analyst_briefing_prompt(mode="expert")`

| Field | Value |
|-------|-------|
| **File** | `prompts/analyst_briefing.py:653` |
| **Purpose** | Produce briefing for a single specialist worker |
| **Dialect** | DuckDB / PG / both |

### Parameters
Same as Oneshot Query above, with `mode="expert"`.

### Output Sections
Same as Oneshot, except:
- Step 5: Single best transform selection (not 4)
- Step 6: Single Logical tree design
- No Step 7 (WRITE REWRITE) — worker writes the SQL
- Output format: `=== SHARED BRIEFING ===` + `=== WORKER 1 BRIEFING ===`
- Section validation checklist (expert)
- No Exploration Budget section

### Token Budget
- ~3000–5500 tokens input

---

## 4. Expert Worker — `build_worker_prompt()`

| Field | Value |
|-------|-------|
| **File** | `prompts/worker.py:25` |
| **Purpose** | Write optimized SQL from analyst briefing |
| **Dialect** | DuckDB / PG / both |

### Parameters

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `worker_briefing` | `BriefingWorker` | Yes | This worker's assignment |
| `shared_briefing` | `BriefingShared` | Yes | Shared analysis |
| `examples` | `List[Dict]` | Yes | Loaded gold examples (full before/after SQL) |
| `original_sql` | `str` | Yes | The original SQL query |
| `output_columns` | `List[str]` | Yes | Expected output columns |
| `dialect` | `str` | No | SQL dialect (default: `"duckdb"`) |
| `engine_version` | `str` | No | Engine version string |
| `resource_envelope` | `str` | No | PG system resource envelope text |

### Output Sections

1. Role + dialect + output format
2. DuckDB specifics (if DuckDB)
3. Semantic Contract (from shared briefing)
4. Target Logical Tree + Node Contracts (from worker briefing)
5. Hazard Flags (from worker briefing)
6. Regression Warnings (from shared briefing)
7. Constraints (from shared briefing)
8. Example Adaptation Notes (from worker briefing)
9. Reference Examples (full before/after SQL)
10. Original SQL
11. Per-Rewrite Configuration / SET LOCAL (PG only)
12. Rewrite Checklist
13. Column Completeness Contract + Output Format (JSON spec)

### Constraints Applied
- Column Completeness Contract (exact output columns)
- Analyst-filtered constraints passed through shared briefing

### Token Budget
- ~2000–4000 tokens input

---

## 5. Swarm Analyst — `build_analyst_briefing_prompt(mode="swarm")`

| Field | Value |
|-------|-------|
| **File** | `prompts/analyst_briefing.py:653` |
| **Purpose** | Produce structured briefing for 4 specialist workers |
| **Dialect** | DuckDB / PG / both |

### Parameters
Same as Oneshot Query above, with `mode="swarm"`.

### Output Sections
Same as Oneshot, except:
- Step 5: Select 4 structurally diverse transforms
- Step 6: Logical tree design for each worker
- No Step 7 (workers write SQL)
- Output format: `=== SHARED BRIEFING ===` + 4 × `=== WORKER N BRIEFING ===`
- Worker 4 has EXPLORATION fields (CONSTRAINT_OVERRIDE, EXPLORATION_TYPE)
- Section validation checklist (analyst/swarm)
- Exploration Budget section (Worker 4)
- Output Consumption Spec section

### Token Budget
- ~4000–6000 tokens input

---

## 6. Fan-Out — `build_fan_out_prompt()`

| Field | Value |
|-------|-------|
| **File** | `prompts/swarm_fan_out.py:14` |
| **Purpose** | Distribute examples across 4 workers with diverse strategies |
| **Dialect** | DuckDB / PG / both |

### Parameters

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query_id` | `str` | Yes | Query identifier |
| `sql` | `str` | Yes | The SQL query |
| `dag` | `Any` | Yes | Parsed logical tree |
| `costs` | `Dict` | Yes | Per-node cost analysis |
| `matched_examples` | `List[Dict]` | Yes | Top tag-matched examples |
| `all_available_examples` | `List[Dict]` | Yes | Full catalog |
| `regression_warnings` | `List[Dict]` | No | Regression examples |
| `dialect` | `str` | No | SQL dialect (default: `"duckdb"`) |

### Output Sections

1. Role (swarm coordinator)
2. Query SQL
3. Logical Tree Structure & Bottlenecks (via `append_dag_summary`)
4. Top N Matched Examples (by structural similarity)
5. All Available Examples (full catalog)
6. Regression Warnings (if any)
7. Task instructions + diversity guidelines
8. Output Format (4 × WORKER_N: STRATEGY + EXAMPLES + HINT)

### Constraints Applied
- Each worker gets exactly 3 examples
- No duplicate examples across workers (12 total)
- Diversity guidelines (conservative → novel)

### Token Budget
- ~1500–3000 tokens input

---

## 7. Swarm Worker — `build_worker_prompt()`

Same as Expert Worker (#4 above). The worker prompt is identical regardless of swarm vs expert mode — only the briefing content differs.

---

## 8. Snipe Analyst — `build_snipe_analyst_prompt()`

| Field | Value |
|-------|-------|
| **File** | `prompts/swarm_snipe.py:111` |
| **Purpose** | Diagnose worker results + synthesize strategy for sniper |
| **Dialect** | DuckDB / PG / both |

### Parameters

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query_id` | `str` | Yes | Query identifier |
| `original_sql` | `str` | Yes | Original SQL query |
| `worker_results` | `List[WorkerResult]` | Yes | ALL results from previous iterations |
| `target_speedup` | `float` | Yes | Target speedup ratio |
| `dag` | `Any` | Yes | Parsed logical tree |
| `costs` | `Dict` | Yes | Per-node cost analysis |
| `explain_plan_text` | `str` | No | EXPLAIN ANALYZE plan text |
| `engine_profile` | `Dict` | No | Engine profile JSON |
| `constraints` | `List[Dict]` | No | Correctness constraints |
| `matched_examples` | `List[Dict]` | No | Tag-matched examples |
| `all_available_examples` | `List[Dict]` | No | Full catalog (compact) |
| `semantic_intents` | `Dict` | No | Pre-computed semantic intents |
| `regression_warnings` | `List[Dict]` | No | Regression examples |
| `resource_envelope` | `str` | No | PG system resource envelope text |
| `dialect` | `str` | No | SQL dialect (default: `"duckdb"`) |
| `dialect_version` | `str` | No | Engine version string |

### Output Sections

1. Role (diagnostic analyst)
2. Target (speedup bar)
3. Previous Optimization Attempts (full SQL for each worker, sorted by speedup)
4. Original SQL (with line numbers)
5. EXPLAIN Plan (if available)
6. Query Structure (Logical Tree)
7. Aggregation Semantics Check
8. Engine Profile (strengths + gaps)
9. Tag-Matched Examples
10. Regression Warnings
11. Correctness Constraints (4 gates)
12. Task (3-step chain: DIAGNOSE → IDENTIFY → SYNTHESIZE)
13. Output Format: `=== SNIPE BRIEFING ===` with FAILURE_SYNTHESIS, BEST_FOUNDATION, UNEXPLORED_ANGLES, STRATEGY_GUIDANCE, EXAMPLES, EXAMPLE_ADAPTATION, HAZARD_FLAGS, RETRY_WORTHINESS, RETRY_DIGEST

### Constraints Applied
- 4 correctness constraints
- Aggregation semantics check

### Token Budget
- ~3000–8000 tokens input (depends on worker count and SQL sizes)

---

## 9. Sniper (iter 1) — `build_sniper_prompt(previous_sniper_result=None)`

| Field | Value |
|-------|-------|
| **File** | `prompts/swarm_snipe.py:368` |
| **Purpose** | High-level reasoner with full freedom to design optimal SQL |
| **Dialect** | DuckDB / PG / both |

### Parameters

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `snipe_analysis` | `SnipeAnalysis` | Yes | Parsed analyst diagnosis |
| `original_sql` | `str` | Yes | Original SQL query |
| `worker_results` | `List[WorkerResult]` | Yes | ALL previous worker results |
| `best_worker_sql` | `str` | No | Best result SQL (if any > 1.0x) |
| `examples` | `List[Dict]` | Yes | Loaded gold examples |
| `output_columns` | `List[str]` | Yes | Expected output columns |
| `dag` | `Any` | Yes | Parsed logical tree |
| `costs` | `Dict` | Yes | Per-node cost analysis |
| `engine_profile` | `Dict` | No | Engine profile JSON |
| `constraints` | `List[Dict]` | No | Correctness constraints |
| `semantic_intents` | `Dict` | No | Pre-computed semantic intents |
| `regression_warnings` | `List[Dict]` | No | Regression examples |
| `dialect` | `str` | No | SQL dialect (default: `"duckdb"`) |
| `engine_version` | `str` | No | Engine version string |
| `resource_envelope` | `str` | No | PG system resource envelope text |
| `target_speedup` | `float` | No | Target speedup ratio (default: 2.0) |
| `previous_sniper_result` | `WorkerResult` | No | Previous sniper result (None for iter 1) |

### Output Sections

1. Role (full freedom SQL optimization architect)
2. Target (speedup bar)
3. Previous Attempts Summary (compact table)
4. Best Foundation SQL (if any)
5. Failure Synthesis (from analyst)
6. Unexplored Angles (from analyst)
7. Strategy Guidance (ADVISORY)
8. Example Adaptation Notes (from analyst)
9. Reference Examples (full before/after SQL)
10. Hazard Flags (from analyst)
11. Engine Profile (strengths + gaps + what_worked)
12. Correctness Invariants (4 HARD STOPS)
13. Aggregation Semantics Check (HARD STOP)
14. Regression Warnings
15. Original SQL (clean, no line numbers)
16. SET LOCAL config (PG only)
17. Rewrite Checklist (sniper-specific)
18. Column Completeness Contract + Output Format

### Constraints Applied
- 4 correctness invariants (HARD STOPS)
- Aggregation semantics (HARD STOP)
- Column completeness contract

### Token Budget
- ~3000–6000 tokens input

---

## 10. Sniper (iter 2) — `build_sniper_prompt(previous_sniper_result=...)`

Same as Sniper iter 1, with added:
- **PREVIOUS SNIPER ATTEMPT** section prepended (speedup achieved, error if any, retry digest from analyst)
- `worker_results` includes the previous sniper result

### Token Budget
- ~3500–7000 tokens input (iter 1 prompt + retry prepend)

---

## 11. PG Tuner — `build_pg_tuner_prompt()`

| Field | Value |
|-------|-------|
| **File** | `prompts/pg_tuner.py:74` |
| **Purpose** | LLM-driven per-query SET LOCAL tuning for PostgreSQL |
| **Dialect** | PostgreSQL only |

### Parameters

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query_sql` | `str` | Yes | The SQL query being optimized |
| `explain_plan` | `str` | No | EXPLAIN ANALYZE output (text format) |
| `current_settings` | `Dict[str, str]` | No | Current PG settings |
| `engine_profile` | `Dict` | No | PG engine profile JSON |
| `baseline_ms` | `float` | No | Baseline execution time in ms |
| `plan_json` | `Any` | No | PG EXPLAIN (FORMAT JSON) — auto-rendered if no text plan |

### Output Sections

1. Role (PostgreSQL performance tuning expert)
2. SET LOCAL scope explanation
3. SQL Query
4. Current baseline (if provided)
5. EXPLAIN ANALYZE Plan (up to 200 lines)
6. Current PostgreSQL Settings
7. System Constraints (max_parallel_workers, shared_buffers, etc.)
8. Engine Profile (compact)
9. Tunable Parameters (whitelist with ranges)
10. Analysis Instructions (8 patterns: sort spills, parallel workers, random_page_cost, JIT, etc.)
11. Critical Rules (evidence-based only, empty is valid, count before sizing)
12. Output Format: JSON `{"params": {...}, "reasoning": "..."}`

### Constraints Applied
- Whitelist-only parameters (from `PG_TUNABLE_PARAMS`)
- Evidence-based: every param must cite EXPLAIN node
- Empty is valid (no speculative tuning)

### Token Budget
- ~1500–3000 tokens input

# Prompt Review Package — Feb 9, 2026

All artifacts for reviewing the V2 analyst + worker prompt system.
Sample query: **TPC-DS Q74** (DuckDB, SF10).

---

## Folder Structure

```
prompt_review/
├── INDEX.md                          ← this file
├── raw_inputs/
│   ├── engine_profile_duckdb.json    ← DuckDB optimizer intel (7 strengths, 6 gaps)
│   ├── engine_profile_postgresql.json← PG optimizer intel (6 strengths, 5 gaps + SET LOCAL)
│   ├── matched_examples/             ← 19 gold example JSONs (full catalog, DuckDB)
│   └── constraints/                  ← 25 constraint JSONs + 2 engine profiles
└── rendered_prompts/
    ├── analyst_v2_query_74.md        ← Full analyst prompt (~55K chars, ~13.7K tokens)
    └── worker_v2_query_74.md         ← Full worker prompt (~16.5K chars, ~4.1K tokens)
```

---

## How Examples Work

### Tag-Based Matching (replaced FAISS Feb 8)
1. `extract_tags(sql)` extracts: table names, SQL keywords, structural patterns
2. Each gold example has pre-computed tags in `ado/models/similarity_tags.json`
3. Scoring: rank by `|query_tags ∩ example_tags|` (overlap count), with category bonus
4. Engine filter: only returns examples matching target engine

**Known limitation**: Tag overlap is bag-of-words — shared TPC-DS vocabulary (date_dim, store_sales, etc.) can cause structural mismatches to rank high (e.g., `intersect_to_exists` matching Q74 which has no INTERSECT). Two defenses:
- Analyst Step 5 (TRANSFORM SELECTION) explicitly instructs: "REJECT tag-matched examples whose primary technique requires a structural feature this query lacks"
- 9/19 examples include `when_not_to_use` fields rendered in the prompt

### What the Analyst Sees
Two example sections:

**Section 6 — "Top N Tag-Matched Examples" (full format):**
- id + verified_speedup, description + principle
- when NOT to apply (present in 9/19 examples)

**Section 7 — "Additional Examples" (compact, de-duplicated):**
Only examples NOT already in Section 6.

### What the Analyst Produces for Workers
Selects 1-3 examples per worker. Sharing across workers allowed if adaptation differs. For each example, EXAMPLE_ADAPTATION states:
- **APPLY**: What aspect to use for THIS worker's strategy
- **IGNORE**: What to skip (prevents over-analogizing from structural mismatches)

### What Workers See
Workers get adaptation notes + full example JSON (before/after SQL, principle, speedup).

---

## Prompt Section Map

### Analyst V2 Prompt

| # | Section | Purpose |
|---|---------|---------|
| 1 | Role | Senior query optimization architect |
| 2 | Query SQL | Line-numbered original SQL + dialect/version |
| 3 | EXPLAIN Plan | Formatted plan tree |
| 4 | DAG Structure | Node cards with cost attribution from EXPLAIN |
| 5 | Semantic Intent | Pre-computed query intent |
| 5b | Aggregation Semantics Check | STDDEV_SAMP traps, FILTER semantics, duplicate safety |
| 6 | **Tag-Matched Examples** | Top N examples (full: id, speedup, principle, when_not) |
| 7 | **Additional Examples** | Non-matched only (compact, de-duplicated from Section 6) |
| 8 | Optimization Principles | Global knowledge from benchmark history |
| 9 | Regression Examples | Anti-patterns with causal mechanisms |
| 10 | **Engine Profile** | Strengths (don't fight) + Gaps (hunt these) + field notes |
| 10a | Resource Envelope | PG only: memory, parallelism, storage for SET LOCAL |
| 10b | Correctness Constraints | 4 gates: LITERAL, SEMANTIC, COMPLETE_OUTPUT, CTE_COLUMNS |
| 11 | Task + Reasoning | 6-step chain (see below) |
| 12 | Output Format | Structured briefing spec (SHARED + 4 WORKER sections) |
| 12a | Section Validation Checklist | 39-line quality checklist |
| 13 | Transform Catalog | 13 transforms with CHECK guards |
| 13b | Strategy Leaderboard | Per-archetype empirical win rates |
| 14 | Strategy Selection Rules | 6 rules for diverse strategies |
| 14b | Exploration Budget | W4: prefer novel (c) > compound (b) > retry (a) |
| 15 | Output Consumption Spec | What workers receive, presentation order |

**Reasoning chain (Step 11):**
1. CLASSIFY — structural archetype
2. EXPLAIN PLAN ANALYSIS — compute ms per node, count scans, check materialization
3. GAP MATCHING — compare EXPLAIN to engine profile gaps, check opportunity/disqualifiers/strengths
4. AGGREGATION TRAP CHECK — verify grouping-sensitive aggregates
5. TRANSFORM SELECTION — rank by expected value; **reject structurally mismatched examples**
6. DAG DESIGN — define CTE topology; **consider CTE materialization behavior** (single-ref inlined, multi-ref materialized)

### Worker V2 Prompt

| # | Section | Purpose |
|---|---------|---------|
| 1 | Role + Dialect | SQL rewrite engine + CASE WHEN guard policy + comment stripping |
| 2 | Semantic Contract | Business intent, join semantics, aggregation traps |
| 3 | Target DAG + Node Contracts | CTE blueprint (FROM/JOIN/WHERE/GROUP BY/OUTPUT/CONSUMERS) |
| 4 | Hazard Flags | Strategy-specific risks for THIS query |
| 4b | Regression Warnings | Observed failures on similar queries |
| 5 | Active Constraints | Analyst-filtered constraints |
| 6 | **Example Adaptation Notes** | APPLY + IGNORE per example |
| 6b | Reference Examples | Before/after SQL pairs |
| 7 | Original SQL | Full source query |
| 7b | SET LOCAL Config (PG only) | Resource envelope, whitelist, tuning rules |
| 7c | Rewrite Checklist | 4-item validation checklist |
| 8 | Output + Column Contract | Expected columns + mechanism-level Changes format |

---

## Engine Profile Structure

```json
{
  "engine": "duckdb" | "postgresql",
  "version_tested": "1.1+" | "14.3+",
  "briefing_note": "...",
  "strengths": [{ "id", "summary", "field_note" }],
  "gaps": [{
    "id", "priority", "what", "why", "opportunity",
    "what_worked": [], "what_didnt_work": [], "field_notes": []
  }],
  "set_local_config_intel": {...}   // PG only
}
```

---

## Runtime vs Sample Path

### Analyst prompt: same renderer, minor input difference
- Both use `build_analyst_briefing_prompt()` with same engine profile JSON
- Sample retrieves k=16 tag-matched examples; runtime uses k=20

### Worker prompt: same renderer, different content source
- **Runtime**: analyst LLM generates briefing → parsed into dataclasses
- **Sample**: `build_mock_worker_briefing()` provides hand-written mock for Q74 W2
- Same `build_worker_v2_prompt()` renderer. Prompt **structure** identical; **content** hand-crafted in sample.

---

## Changes Log

| Fix | What changed |
|-----|-------------|
| Double-x speedups | `.rstrip("x")` on all `verified_speedup` formatting |
| Constraint failures | Falls through to `error` field (was "regressed to ?") |
| Example `when_not` | Reads `example.when_not_to_use` (was missing from 9 examples) |
| Catalog de-duplication | Section 7 shows only examples NOT in Section 6 |
| EXAMPLE_ADAPTATION | Renamed from EXAMPLE_REASONING; APPLY + IGNORE format |
| Example sharing | Workers may share examples if adaptation differs |
| Structural rejection | Step 5: "REJECT examples whose technique requires absent structure" |
| CTE materialization in DAG DESIGN | Step 6: design shared CTEs only when multi-consumer |
| CASE WHEN guard | Worker role: preserve defensive division guards |
| Comment stripping | Worker role: strip benchmark comments |
| W4 exploration bias | Priority: novel > compound > retry |
| Mechanism-level Changes | Worker output: structural change + expected mechanism |
| Mock DAG fix | Explicit JOIN...ON (was chained a=b=c=d), FK key tracing |
| Mock adaptation notes | APPLY/IGNORE format (was apply-only) |
| Engine profile facts | Fixed CTE materialization claim, DuckDB "index" framing |
| PG version | "14.3+" (was "16+") |
| GAP MATCHING fields | References actual field names from profile JSON |

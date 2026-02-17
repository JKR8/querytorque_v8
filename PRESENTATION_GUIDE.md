# QueryTorque V8 - Presentation Guide

## Executive Summary

**QueryTorque V8** is an AI-powered SQL optimization system that models database optimizer gaps and generates high-impact rewrites (2x-42.90x speedup). Unlike generic tools, it uses **pathology-based reasoning** (understanding WHY optimizers fail) and validates rewrites with **semantic checks + synthetic validation** before expensive benchmarking.

**Core Innovation**: Shift from constraint-based rules to **engine-aware pathology modeling** backed by empirical evidence from 200+ benchmark runs.

---

## The BEAM Architecture (Cheaper, Faster, More Coverage)

### Three Stages:

**Stage 1: BEAM Analyst** (Reasoning)
- Reads EXPLAIN ANALYZE plan
- Identifies cost spine (dominant operators)
- Routes by symptom (flat→early filter, nested→decorrelate, etc.)
- Selects **adaptive probe count**: ★★★=12-16, ★★=8-12, ★=4-8

**Stage 2: BEAM Workers** (4-16 Parallel Execution)

Two worker types:

**Non-Reasoning Workers** (beam_worker_v3.txt)
- Each worker executes ONE probe (single-transform DAG)
- Straightforward implementation from analyst dispatch
- Fast, deterministic

**Reasoning Workers** (beam_reasoning_worker_v1.txt)
- More sophisticated probe exploration
- Can adapt probe within reasoning space
- Better for complex/ambiguous cases
- Slower but higher quality on sticky queries

Both worker types:
- **Synthetic validation**: Quick semantic check on temp DuckDB
  - AST walk generates test data (≥1 row per table)
  - Compare original vs rewritten results (milliseconds)
  - Catches obvious errors without full validation cost
- Returns modified DAG structure + BDA (Battle Damage Assessment)

**Stage 3: BEAM Compiler** (Synthesis)
- Merges BDA from all workers
- Evidence-first approach (pick strongest winners)
- Applies regression registry checks
- Outputs 1-2 final DAG attempts

**Why Better Than Fixed 4-Worker?**
- Cheaper: Single-transform probes (not full rewrites)
- Faster: Parallel + compiler synthesis (vs sequential benchmarking)
- More Coverage: 12-16 probes on hard queries vs fixed 4

---

## System Overview

```
User Query
    ↓
Parse + EXPLAIN ANALYZE
    ↓
BEAM ANALYST (diagnose + select probes)
    ↓
BEAM WORKERS (4-16 parallel)
├─ Non-Reasoning (V3): straightforward execution
├─ Reasoning (V1): adaptive exploration
└─ All get: synthetic validation on temp DuckDB
    ↓
BDA Collection (Battle Damage Assessment)
    ↓
BEAM COMPILER (merge winners)
    ↓
Final DAG(s) → Validation → Benchmarking
    ↓
RECOMMENDATION
```

---

## Pathology-Based Reasoning (The Core Innovation)

**Traditional**: "Use transform X when you see pattern Y" (generic, no understanding)

**QueryTorque**: Exploit specific **engine gaps** with evidence-backed rewrites
- Model WHERE optimizer fails (pathology)
- Understand the MECHANISM
- Back with 50+ real benchmark runs per transform

### 7 Transform Families (Organized by Gap, Not Syntax)

| Family | Gap | Example | Speedup | Risk |
|---|---|---|---|---|
| Early Filtering (A) | CTE predicate blindness | `date_cte_isolate` | 4.00x | Low |
| Decorrelation (B) | Correlated subqueries | `decorrelate` | 8044x ⚡ | Med |
| Aggregation (C) | Aggregate→Join | `aggregate_pushdown` | 42.90x 🏆 | None |
| Set Ops (D) | INTERSECT materialization | `intersect_to_exists` | 1.83x | Low |
| Materialization (E) | Redundant scans | `single_pass_aggregation` | 4.47x | None |
| Join Topology (F) | Comma joins | `pg_explicit_join` | 8.56x | Low |

---

## Validation Strategy

### Layer 1: Synthetic Validation (In-Worker)
- Temp DuckDB with minimal test data
- AST walk generates table data (≥1 row each)
- Quick semantic check (milliseconds)
- All 4-16 workers validated immediately
- Not 100% accurate but catches obvious errors

### Layer 2: Semantic Validation (Post-Compiler, if needed)
- Tier 1: AST structural checks (instant)
- Tier 2: TABLESAMPLE on 2% dataset (10-100ms)
- Detailed error diffs for analyst

### Layer 3: Benchmarking
- Parallel race testing (all variants run simultaneously)
- Win criteria: ≥5% speedup over original

---

## Benchmark Results (You Add These)

**DuckDB** (TPC-DS SF10, 88 queries)
- 67% pass rate (59 PASS, 13 FAIL, 16 ERROR)
- Best: aggregate_pushdown **42.90x**, Q88 **6.28x**
- Zero regressions on: single_pass_aggregation, aggregate_pushdown, inner_join_conversion

**PostgreSQL** (DSB 76 queries, V2)
- 68.4% success rate (31 WIN, 21 IMPROVED, 17 NEUTRAL, 7 REGRESSION)
- Best: Q092 **8044x** timeout rescue, Q032 **1465x**, Q001 **27.80x**
- Most reliable: decorrelate + AS MATERIALIZED + explicit JOINs

**Snowflake** (TPC-DS SF10, 103 queries)
- Status: Discovery mode (hypothesis-driven)
- Verified: inline_decorrelate **23.17x**, shared_scan_decorrelate **7.82x**

---

## Presentation Structure (15 slides)

1. **Title** — QueryTorque: Engine-Aware SQL Optimization
2. **Problem** — Optimizer gaps cause 100x-8000x slowdowns
3. **Solution** — Pathology-based reasoning + BEAM architecture
4. **The BEAM Pipeline** — Analyst → Probes → Compiler
5. **7 Transform Families** — Organized by gap type
6. **Validation Layers** — Synthetic → Semantic → Benchmarking
7. **Synthetic Validation** — Quick semantic gate on temp DuckDB
8. **DuckDB Results** [YOUR DATA]
9. **PostgreSQL Results** [YOUR DATA]
10. **Snowflake Status** [YOUR DATA]
11. **Why BEAM Wins** — Cheaper, faster, more coverage
12. **Multi-Engine Support** — DuckDB, PostgreSQL, Snowflake
13. **Knowledge System** — Gold examples → Playbooks → Workers
14. **Roadmap** — Current status + next steps
15. **Impact** — Real speedup, timeout rescues, ROI

---

## Key Talking Points

### Opening Hook (30 sec)
*"Database optimizers are powerful but predictable. They have systematic blindspots where they consistently fail. QueryTorque finds those gaps and generates rewrites exploiting them for 2x to 42.90x speedup. Unlike generic optimization tools, we don't use hardcoded rules—we model the optimizer."*

### BEAM vs Fixed 4-Worker
- **Cheaper**: Single-transform probes (not full rewrites)
- **Faster**: Parallel execution + evidence-first compiler synthesis
- **More coverage**: 12-16 probes on hard queries vs fixed 4
- **Better signal**: Synthetic validation filters obvious failures early

### Biggest Wins to Mention
- DuckDB aggregate_pushdown: **42.90x** ← biggest single win
- PostgreSQL Q092 decorrelate: **8044x** timeout rescue ← timeout rescues are huge
- Snowflake inline_decorrelate: **23.17x** ← discovery mode success

### Safety Message
*"Every rewrite passes semantic validation on temp data (milliseconds) before expensive benchmarking. Regression hard bans prevent known failure modes. Only recommend rewrites that beat original by ≥5%."*

---

## Demo Script (If Showing Live Example)

**Query**: PostgreSQL Q092 (timeout, >300s)

**Problem**:
```sql
WHERE price > (
  SELECT AVG(price) FROM sales
  WHERE item_sk = outer.item_sk  -- correlated, re-executes per row
)
```

**Analyst Decision**:
- Symptom: Nested loop with repeated correlated work
- Probe count: ★★★ = 14 probes
- Family B (Decorrelation) highlighted

**Worker Results**:
- Probe P1: FAIL (semantic) — missing filters
- Probe P2: FAIL — still too slow
- Probe P3: PASS — **8044x** ✅

**Compiler Output**:
- Clear winner: Probe P3 (confidence 0.95)
- Single DAG attempt

**Result**: 320 seconds → <1 second ✅

---

## Competitive Advantages

1. **Engine-Aware** — Different gaps per engine (DuckDB/PG/Snowflake each optimized differently)
2. **Evidence-Based** — Every transform backed by 50+ real benchmark runs
3. **Safe** — Synthetic validation + semantic checks + regression bans
4. **Fast** — BEAM architecture (cheaper probes + parallel execution)
5. **Scalable** — Handles 4-16 probes based on query importance
6. **Interpretable** — Every rewrite traceable to specific optimizer gap
7. **Continuous Learning** — Gold examples grow from real optimization sessions

---

## Knowledge System

**4 Stages**:
1. **Trial JSON** (100s experiments) → Raw data
2. **Gold Examples** (30-40 per dialect) → Curated wins
3. **Playbooks** (knowledge/{dialect}.md) → Pathologies + gates
4. **Worker Application** → Examples loaded into probes

**Files**:
- `knowledge/duckdb.md` (10 pathologies, 299 lines)
- `knowledge/postgresql.md` (7 pathologies, 356 lines)
- `knowledge/snowflake.md` (10 hypotheses, 236 lines)
- `examples/{dialect}/` (26 DuckDB + 14 PG + 2 Snowflake)
- `transforms.json` (30 transforms with empirical data)

---

## Multi-Engine Status

**DuckDB** ✅ Mature
- 10 pathologies documented
- 26 gold examples
- 200+ benchmark runs
- Best for: Development, testing, rapid iteration

**PostgreSQL** ✅ Mature
- 7 pathologies documented
- 14 gold examples
- Consistent wins including timeout rescues
- Best for: Production, DSB benchmark validation

**Snowflake** 🔄 Discovery Mode
- 9 hypotheses (unverified)
- 2 verified gold examples (P3)
- Hypothesis-driven optimization
- Best for: Cloud warehouse patterns

---

## Roadmap

**Completed** (Feb 2026):
- ✅ BEAM architecture (analyst → probes → compiler)
- ✅ Synthetic validation (temp DuckDB semantic checks)
- ✅ Semantic validation 3-tier system
- ✅ Multi-engine playbooks
- ✅ Per-worker tuning (PostgreSQL)
- ✅ Automatic transform detection (AST-based)

**In Progress**:
- 🔄 Finalize Snowflake pathologies (hypothesis → evidence)
- 🔄 Complete 2-mode architecture (oneshot vs beam only)

**Planned**:
- Expand to 40+ gold examples per dialect
- Databricks engine support
- Multi-query cross-pattern optimization
- Cost model learning (predict speedup pre-benchmark)

---

## Numbers to Memorize

**Biggest Wins**:
- DuckDB: 42.90x (aggregate_pushdown)
- PostgreSQL: 8044x (timeout rescue, decorrelate)
- Snowflake: 23.17x (inline_decorrelate, verified)

**Success Rates**:
- DuckDB: 67% (59/88 queries)
- PostgreSQL: 68% (31+21/76)
- Snowflake: 50-80% (on timeout queries)

**Validation Speed**:
- Synthetic validation: milliseconds per worker
- Semantic validation: <100ms overhead
- Savings vs failed benchmarks: 60-300x ROI

**Evidence Volume**:
- 200+ benchmark runs
- 50+ gold examples
- 30 transforms with empirical data
- 7-10 pathologies per engine

---

## Common Q&A

**Q: How is this different from other optimization tools?**
A: We model specific optimizer blindspots per engine, not generic rules. Every recommendation backed by real benchmarks. Synthetic validation catches errors before expensive benchmarking.

**Q: Will this break my queries?**
A: No. Synthetic validation + semantic checks + regression bans prevent semantic errors. Only recommend rewrites that pass validation and beat original by ≥5%.

**Q: How long does optimization take?**
A: 8-48 seconds per query (analyst + 4-16 workers + compiler). Much faster than hours of manual tuning.

**Q: What if a rewrite slows down my query?**
A: We catch this in benchmarking. Worst documented regression is 0.0076x (extreme edge case with root cause documented).

**Q: Does this work on my database?**
A: Mature support for DuckDB and PostgreSQL. Snowflake in discovery mode (growing). Databricks planned next.

---

## Closing Message

*"QueryTorque shifts SQL optimization from generic rules to engine-aware pathology modeling. By understanding WHERE and WHY optimizers fail, we generate targeted, evidence-backed rewrites that deliver real, measurable speedup — safely and automatically."*

---

## Files & Resources

- **This guide**: PRESENTATION_GUIDE.md
- **1-page summary**: PRESENTATION_1PAGE_SUMMARY.md
- **Code location**: `packages/qt-sql/`
- **Benchmarks**: `research/`
- **Knowledge**: `qt_sql/knowledge/` (playbooks, examples, transforms)
- **Templates**: `qt_sql/prompts/templates/V3/` (BEAM prompt specs)


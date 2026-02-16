# Gold Example Growth Guide

The ONLY knowledge artifacts that matter for QueryTorque beam are **gold examples**.
Everything else is transient research data.

## What is a Gold Example?

A **gold example** is a curated, verified optimization case with:
- **High speedup** (>= 2.0x) OR instructive regression (< 0.90x, correct SQL)
- **key_insight**: WHY it works at the engine level
- **when_not_to_use**: Known failure modes
- **Full SQL before/after** with EXPLAIN summaries

Gold examples are:
- Loaded into beam worker prompts (show patterns)
- Used to generate knowledge playbooks (group by pathology)
- The ONLY permanent knowledge base (30-40 total per dialect)

## How to Grow Gold Examples from Beam Runs

### Step 1: Run Beam Optimization

```bash
qt run duckdb_tpcds --mode beam --concurrency 8
```

Output: `beam_sessions/query_{id}_{timestamp}/` (100+ optimization attempts)

### Step 2: Analyze Sessions for Candidates

```bash
qt analyze duckdb_tpcds --export candidates.json
```

Scans all beam sessions, filters for:
- **Wins:** speedup >= 2.0x, semantic_passed = true
- **Regressions:** speedup < 0.90x, correct SQL (not errors)

Use `--dedup` to keep only best per (query, transform):
```bash
qt analyze duckdb_tpcds --export candidates.json --dedup
```

Output: `candidates.json` with promotion candidates

### Step 3: Manual Curation (CRITICAL HUMAN STEP)

```bash
qt promote duckdb_tpcds --from candidates.json --ids 1,2,3,8,15
```

For each selected candidate:
1. Review SQL diff + EXPLAIN diff
2. Write `key_insight` (WHY it's faster, engine-level)
3. Write `when_not_to_use` (failure modes)
4. Write `input_slice` (pattern description)

Output: `qt_sql/examples/duckdb/{transform}.json` (new gold examples)

**Rebuild tag index:**
```bash
qt index
```

### Step 4: Auto-Generate Playbook

```bash
qt playbook duckdb --output knowledge/duckdb_DRAFT.md
```

Reads ONLY gold examples (no beam sessions, no blackboard).
Groups by pathology, formats as markdown.

Output: `knowledge/duckdb_DRAFT.md`

### Step 5: Review & Deploy

```bash
# Compare with existing playbook
diff knowledge/duckdb.md knowledge/duckdb_DRAFT.md

# Merge updates (new pathologies, updated win counts, new regressions)
# OR: Replace entirely if starting fresh
cp knowledge/duckdb_DRAFT.md knowledge/duckdb.md
```

### Step 6: Next Beam Run (Automatic Feedback)

```bash
qt run duckdb_tpcds --mode beam --queries q1,q2,q3
```

Beam prompt builder automatically loads:
- `knowledge/duckdb.md` (updated playbook)
- `examples/duckdb/*.json` (all gold examples, including new ones)

**Feedback loop complete.**

---

## Gold Example Quality Standards

### Win Example Requirements:
1. **Speedup >= 2.0x** (verified on SF10)
2. **Correctness** (row count + checksum match)
3. **Reusable pattern** (not query-specific hack)
4. **key_insight explains engine behavior** (not just SQL diff)
5. **when_not_to_use documents failure mode** (every transform has one)

### Regression Example Requirements:
1. **Speedup < 0.90x** (significant regression)
2. **Correct SQL** (not syntax/semantic error)
3. **Reveals engine limitation** (optimizer fighting the rewrite)
4. **Reusable anti-pattern** (prevents others from repeating)

### Common Anti-Patterns (DO NOT PROMOTE):
- Syntax errors, semantic errors
- Query-specific mistakes
- Marginal wins (1.05x-1.20x) — not strong enough signal
- Unreliable wins (high variance across runs)
- One-off hacks that don't generalize

---

## File Locations

| Artifact | Path | Purpose |
|----------|------|---------|
| **Gold Examples** | `qt_sql/examples/{dialect}/*.json` | PERMANENT knowledge base |
| **Playbook** | `qt_sql/knowledge/{dialect}.md` | Generated from gold examples |
| **Engine Profile** | `qt_sql/constraints/engine_profile_{dialect}.json` | Gap/strength metadata |
| **Transform Catalog** | `qt_sql/knowledge/transforms.json` | Transform → gap mapping |
| **Beam Sessions** | `beam_sessions/query_*_{ts}/` | Archived for research |
| **Candidates** | `candidates.json` | Transient promotion list |

---

## Maintenance Guidelines

### Monthly Review:
- Scan latest beam runs for new candidates
- Promote 3-5 strong wins per month
- Add 1-2 instructive regressions when discovered
- Regenerate playbook if 5+ new examples added

### Quality Control:
- Every gold example MUST have key_insight + when_not_to_use
- Speedups MUST be verified (3x runs minimum)
- Regressions MUST be correct SQL (not errors)
- Remove gold examples if superseded by better versions

### Growth Target:
- **30-40 gold examples per dialect** (mature knowledge base)
- DuckDB: ~19 examples (gold + regressions)
- PostgreSQL: ~14 examples
- Snowflake: ~4 examples (growing)

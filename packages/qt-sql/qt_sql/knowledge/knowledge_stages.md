# Knowledge Stages

How raw experimental data becomes actionable rewrite intelligence.

> **Architecture Decision (Feb 2026):** Stages 1-3 were originally separate
> artifacts. Stage 3 (pathology cards) has been **eliminated as a separate
> layer** — its content is folded directly into the Stage 4 dialect files.
> This reduces maintenance burden and eliminates synchronization bugs between
> intermediate files. The distilled algorithm IS the pathology card now.

## CRITICAL: Beam Pipeline Uses Gold Examples Only

QueryTorque beam **does NOT use blackboards** or trial JSON for product.

**Knowledge Flow:**
1. **Beam runs** → archived sessions (research only)
2. **Manual curation** (`qt analyze` + `qt promote`) → gold examples (ONLY product artifact)
3. **Playbook generation** (`qt playbook`) → from gold examples
4. **Next beam run** → uses playbook + gold examples

**Legacy Artifacts (Archive Only):**
- Swarm blackboards (`duckdb_tpcds.json`) — historical research
- Trial JSON (`trials.jsonl`) — historical research
- `build_blackboard.py` — legacy tool (NOT used by beam)

**Active Artifacts:**
- Gold examples (`examples/{dialect}/`) — loaded into prompts
- Knowledge playbooks (`knowledge/{dialect}.md`) — guide analyst reasoning
- Engine profiles (`engine_profile_{dialect}.json`) — gap metadata

See **GOLD_EXAMPLE_GUIDE.md** for the complete growth path.

---

## Stage 1: Transform & Trial JSON

**Purpose:** Raw experimental results. Every rewrite attempt, win or lose.

**Status:** IMPLEMENTED — `knowledge/trials.jsonl` (25 gold), full corpus in benchmark learning dirs

### Template: Trial Record

```json
{
  "query_id": "Q39",
  "transform": "self_join_decomposition",
  "trial_round": "swarm_w2",
  "original_sql": "<full SQL>",
  "rewritten_sql": "<full SQL>",
  "baseline_ms": 142.3,
  "rewrite_ms": 29.9,
  "speedup": 4.76,
  "rows_match": true,
  "tags": ["self_join_cte", "discriminator_filter", "comma_join"]
}
```

**Required fields:** query_id, transform, speedup, rows_match
**One per attempt.** Hundreds of these. Most are noise — failed attempts,
marginal wins, regressions. Never curate this layer — keep everything.

---

## Stage 2: Gold Examples

**Purpose:** Promoted specimens. Each is a verified win or an instructive
failure, with the reasoning extracted. These are loaded into worker prompts
to show concrete before/after patterns.

**Status:** IMPLEMENTED — `examples/duckdb/` (22 files), `examples/postgres/` (14 files)

### Template: Gold Example

**File:** `examples/{dialect}/{transform_name}.json`

```json
{
  "id": "self_join_decomposition",
  "type": "win",
  "verified_speedup": "4.76x",
  "benchmark": "TPC-DS",
  "benchmark_queries": ["Q39"],
  "engine": "duckdb",

  "example": {
    "input_slice": "CTE self-joined with inv1.d_moy=1 AND inv2.d_moy=2",
    "output": {
      "nodes": {
        "month1_stats": "... WHERE d_moy = 1 GROUP BY ...",
        "month2_stats": "... WHERE d_moy = 2 GROUP BY ...",
        "main_query": "... FROM month1_stats JOIN month2_stats ON ..."
      }
    },
    "key_insight": "Optimizer materializes CTE for ALL months then post-filters. Splitting embeds filter into each CTE, processing 1/12th of data.",
    "when_not_to_use": "Only when CTE is self-joined with different discriminator values. Same filter on both aliases = no benefit."
  },

  "original_sql": "<full original SQL>",
  "optimized_sql": "<full optimized SQL>",
  "explain_before_summary": "Sequential scan on inventory (7.2M rows) → GROUP BY → materialize → filter d_moy=1 (keeps 600K)",
  "explain_after_summary": "Sequential scan on inventory with d_moy=1 pushed (600K rows) → GROUP BY → hash join"
}
```

**Required fields:** id, type, verified_speedup, example.input_slice, example.key_insight, example.when_not_to_use, original_sql, optimized_sql

**What makes a good gold example:**
- Verified speedup >= 1.10x on production benchmark (3-run validated)
- Clear before/after SQL showing exactly what changed
- `key_insight` explains WHY it's faster at the engine level (not just WHAT changed)
- `when_not_to_use` documents the failure mode (every transform has one)
- Regression examples are equally valuable — they prevent workers from repeating mistakes

**Promotion criteria (trial → gold):**
1. Speedup verified on SF10 (not just SF1)
2. Row count matches (correctness proven)
3. Transform is reusable (not query-specific hack)
4. Key insight generalizes to other queries

---

## ~~Stage 3: Pathology Cards~~ (ELIMINATED)

> **MERGED INTO STAGE 4.** Pathology cards (engine gaps, detection signals,
> restructuring principles, risk calibration, gold example routing) are now
> embedded directly in the per-dialect distilled algorithm files. There is
> no separate intermediate representation.
>
> **Rationale:** Maintaining pathology cards as separate YAML/JSON files
> created a synchronization problem — the cards would drift from the
> distilled algorithm, and neither the analyst nor workers ever saw the
> raw cards. Folding them into Stage 4 means one file to update, one file
> to review, one file loaded into the prompt.
>
> **Deleted files:** `decisions.md`, `decision_card_template.md`,
> `duckdb_decision_tree.md` — all superseded by `{dialect}.md`.

---

## Stage 4: Distilled Algorithm (THE ACTIVE LAYER)

**Purpose:** What the analyst and workers actually see. Contains pathology
detection, decision gates, risk calibration, gold example routing, global
guards, verification checklist, pruning guide, and regression registry —
all in a single walkable document per engine.

**Status:** IMPLEMENTED
- `knowledge/duckdb.md` — 299 lines, 10 pathologies (P0-P9)
- `knowledge/postgresql.md` — 356 lines, 7 pathologies (P1-P7)

Loaded into the analyst prompt via `prompter.py::load_exploit_algorithm()`.
This is the tip of the spear — every optimization session starts here.

### Template: Pathology (inside {dialect}.md)

A **pathology** is a recurring engine-level inefficiency that the optimizer
cannot fix on its own. It has a root cause (the engine gap), observable
symptoms (detection signals), a known fix (restructuring principle), and
calibrated risk. Each pathology is a section in the dialect file:

```markdown
### P{N}: {Human-readable name}
  Engine gap: {GAP_NAME} from engine_profile_{dialect}.json
  Explain signal: {what to look for in EXPLAIN ANALYZE output}
  SQL signal: {what to look for in the SQL text}
  → DECISION: {what restructuring to apply}
  → Gates: {conditions that MUST hold, or skip this pathology}
  → Expected: {speedup range}x | Worst: {worst regression}x ({query + root cause})
  → Gold examples: {which examples workers should see, with speedups}
```

**Required sections per pathology:**
1. **Engine gap** — links to the engine profile entry explaining WHY the optimizer fails
2. **Detection signals** — EXPLAIN-based (preferred) and SQL-based (fallback)
3. **Decision** — the specific restructuring principle to apply
4. **Gates** — hard constraints; if any gate fails, skip this pathology entirely
5. **Risk calibration** — expected speedup range AND worst known regression with root cause
6. **Gold example routing** — specific example IDs + their speedups

**What makes a pathology (vs just a transform):**
- A pathology explains the ENGINE LIMITATION, not just the rewrite trick
- It predicts NOVEL manifestations ("same gap shows up when...")
- It has calibrated risk from real benchmark data
- It routes to specific gold examples as evidence

**What is NOT a pathology:**
- A single transform with one example (that's just a gold example)
- A general SQL best practice ("use EXISTS instead of IN")
- A rewrite that only works on one specific query shape

### Supporting sections in {dialect}.md

Beyond the pathology list, each dialect file also contains:

```markdown
# HOW TO USE THIS DOCUMENT
{Instructions for the analyst on how to walk the pathology list}

# SAFETY RANKING
{Pathologies ordered by risk, safest first}

# VERIFICATION CHECKLIST
{Post-rewrite checks that must pass before declaring a win}

# PRUNING GUIDE
{When to skip entire pathologies based on query structure}

# REGRESSION REGISTRY
{Every known regression with root cause and avoidance rule}
```

---

## Stage 5: Tree Pruning by EXPLAIN Plan (FUTURE STATE)

**Purpose:** At inference time, prune branches the analyst never needs
to read. Based on what's actually in the EXPLAIN plan, not the SQL.

**Status:** PARTIALLY IMPLEMENTED — `tag_index.py::extract_explain_features()`
extracts 6 structural features. Full dynamic pruning not yet wired into
prompt assembly.

---

## The Actual Pipeline (as-built)

```
Trial JSON (hundreds)
  → promote/curate →
Gold Examples (~36)
  → extract engine mechanisms, fold into dialect doc →
Distilled Algorithm (1 doc per engine, includes pathologies)
  → analyst reads, picks pathologies, routes gold examples →
Workers produce rewrites
  → validate → new Trial JSON (loop)
```

Note: no separate "pathology card" artifact exists. The distilled
algorithm file IS the pathology card, the decision tree, the
regression registry, and the pruning guide — all in one.

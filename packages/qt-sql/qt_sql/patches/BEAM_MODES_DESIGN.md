# Beam Strike Modes: Wide vs Focused

## Fleet Terminology Mapping

| Fleet C2 | Beam Pipeline | Description |
|----------|--------------|-------------|
| **Fleet** | Batch | All queries in a benchmark run |
| **Mission** | Query session | One query's full optimization lifecycle |
| **Sortie** | Iteration | Analyst → workers → validate → benchmark cycle |
| **Strike** | Worker probe | Single transform attempt (one LLM call) |
| **Reconnaissance** | AST front gate + analyst | Identify what to try |
| **BDA** (Battle Damage Assessment) | Tier-1 + equivalence + benchmark | Assess what worked |
| **Sniper** | Snipe round | R1 synthesizes from BDA into compound rewrites |

## Workload Router

Before any missions launch:
1. Collect baselines (cached `original_ms` or quick 1-run screen)
2. Sort queries by `original_ms` descending
3. Accumulate until 80% of total runtime covered → **HEAVY**
4. Everything else → **LIGHT**

```
HEAVY (~14 queries, 80% of work)  → beam_focused  (full strike package)
LIGHT (~62 queries, 20% of work)  → beam_wide     (reconnaissance sortie)
```

Config field: `"beam_mode": "auto"` (routes by workload), `"wide"`, or `"focused"`.

---

## Mode 1: BEAM WIDE — Reconnaissance Sortie

### Theory

Light queries don't need R1 creativity — they need COVERAGE. Fire 16
cheap scout strikes, see which ones move the needle, then optionally
let R1 synthesize a compound rewrite from the BDA.

### Pipeline

```
32 transforms in catalog
       │
       ▼ STEP 0: AST FRONT GATE (deterministic, <50ms, free)
       │ detect_transforms(sql, engine) → overlap_ratio scoring
       │ Kill transforms with <50% precondition overlap
       │ Kill transforms with active contraindications
       │ "No CORRELATED_SUB → kill all B-family"
       │ "No INTERSECT → kill D1, D3"
       │
   ~10-15 surviving transforms
       │
       ▼ STEP 1: SCOUT ANALYST (R1, 1 call)
       │ Sees: query + EXPLAIN + surviving transforms with overlap scores
       │ Outputs:
       │   1. HYPOTHESIS (compressed reasoning, 2-3 sentences)
       │      "Bottleneck: Nested Loop at line 8 re-scans 4810 rows.
       │       Secondary: Hash Join at line 12 processes 33K rows
       │       after late dimension filter."
       │   2. PRUNED LIST — drops transforms EXPLAIN shows won't help
       │      "E1: CTE only referenced once. DROP."
       │   3. Per-probe TARGET — one sentence WHERE to apply
       │      "B1: decorrelate avg_discount subquery into threshold CTE"
       │ → ~8-16 probes ranked by confidence
       │
       ▼ STEP 2: STRIKES (qwen × 8-16, parallel)
       │ Each strike gets:
       │   - Original SQL
       │   - Analyst hypothesis (shared across all strikes)
       │   - ONE probe target (transform + where to apply)
       │   - ONE gold example (before/after SQL)
       │   - CRITICAL RULES (don't change literals, don't remove filters)
       │ Output: rewritten SQL (nothing else)
       │
       ▼ STEP 3: TIER-1 BACK GATE (structural, instant)
       │ Literal mismatch? Column ref? → retry 1x → dead
       │
       ▼ STEP 4: EQUIVALENCE BACK GATE (full dataset)
       │ Row count + MD5 checksum vs original
       │ Fail → retry 1x → dead
       │
   ~4-8 verified rewrites
       │
       ▼ STEP 5: BENCHMARK (3x warmup+avg2)
       │
       ▼ STEP 6: EXPLAIN COLLECTION
       │
       ▼ STEP 7: SNIPER (R1, 1 round, optional)
       │ Only if mixed results (some wins, some fails)
       │ Sees ALL probe BDA → combines best into 2-3 compound rewrites
       │ Compound rewrites go through steps 3-6 again
       │
   Final: best_speedup, best_sql
```

### Scout Analyst Prompt (R1)

```
## Role

You are a SQL optimization scout. Your mission: analyze this query
and select which transform PROBES to fire. A separate team of code
workers will execute each probe.

## Hypothesis

First, state your BOTTLENECK HYPOTHESIS in 2-3 sentences. This gets
passed to every worker as shared context. Focus on:
- Which EXPLAIN operator is the bottleneck (cite row count, cost)
- WHY it's expensive (late filter, correlated scan, repeated table access)

## Query

```sql
{original_sql}
```

## EXPLAIN Plan

```
{explain_text}
```

## Candidate Transforms (AST-filtered)

These transforms passed precondition checks against this query's structure.
Overlap score = fraction of precondition features present.

{for each surviving transform:}
### {transform.id} (Family {transform.family}, overlap: {overlap:.0%})
**Principle**: {transform.principle}
**Gap**: {transform.gap}
{if contraindications:}
**Caution**: {contraindication.instruction}
{/if}

## Your Task

1. State your BOTTLENECK HYPOTHESIS (2-3 sentences, shared with all workers)
2. For each candidate transform above, decide: FIRE or DROP
   - FIRE: include in probe list with a specific TARGET (where to apply)
   - DROP: one sentence why EXPLAIN shows it won't help
3. Rank by expected impact

```json
{
  "hypothesis": "The Nested Loop at line 8 re-executes the scalar subquery
    4,810 times. Secondary cost: Hash Join at line 12 probes 33K rows
    because the item filter is applied after the join.",
  "probes": [
    {
      "probe_id": "p01",
      "transform_id": "decorrelate",
      "family": "B",
      "target": "Convert WHERE (SELECT avg(...) FROM web_sales ...) subquery
        into a CTE with GROUP BY ws_item_sk, then join on item key",
      "confidence": 0.95
    },
    {
      "probe_id": "p02",
      "transform_id": "early_filter",
      "family": "A",
      "target": "Push i_manufact_id/i_category filter into a filtered_items
        CTE before the hash join at line 12",
      "confidence": 0.85
    }
  ],
  "dropped": [
    {"transform_id": "materialize_cte", "reason": "Only 1 CTE reference, no re-evaluation"},
    {"transform_id": "or_to_union", "reason": "OR is on same column (i_category IN list), engine handles natively"}
  ]
}
```
```

### Strike Worker Prompt (qwen)

```
## Task

Rewrite this SQL query by applying ONE specific optimization.
Output ONLY the rewritten SQL — no explanation, no markdown.

## Bottleneck Context

{analyst_hypothesis}

## Original SQL

```sql
{original_sql}
```

## Transform to Apply

**{probe.transform_id}** (Family {probe.family}): {probe.target}

## Reference Example

BEFORE:
```sql
{gold_example.original_sql (truncated to 15 lines)}
```

AFTER:
```sql
{gold_example.optimized_sql (truncated to 15 lines)}
```

## Rules (MUST follow)

1. Output ONLY the rewritten SQL, nothing else
2. Do NOT change any literal values (numbers, strings, dates)
3. Do NOT remove any WHERE/HAVING/ON conditions
4. Do NOT add new filter conditions that weren't in the original
5. Apply ONLY the specified transform — change nothing else
6. Preserve column names, ordering, and LIMIT exactly
```

### Sniper Prompt (R1, optional round 2)

```
## Role

You are a strike synthesizer. {n_probes} transform probes were fired
against this query. Your BDA shows which ones hit. Now design 2-3
compound rewrites combining the best strategies.

## Original SQL
```sql
{original_sql}
```

## Bottleneck Hypothesis (from scout)
{analyst_hypothesis}

## EXPLAIN (Original)
```
{original_explain}
```

## Strike BDA (Battle Damage Assessment)

| Probe | Transform | Family | Status | Speedup | Key Change in EXPLAIN |
|-------|-----------|--------|--------|---------|----------------------|
{for each probe:}
| {probe_id} | {transform_id} | {family} | {status} | {speedup} | {explain_delta_summary} |

## EXPLAIN Plans (verified strikes only)

{for each passing probe with speedup > 1.0:}
### {probe_id}: {transform_id} ({speedup}x)
```
{probe_explain (truncated to 40 lines)}
```
{/for}

## Your Task

The BDA tells you WHAT WORKS on this query. Now design compound strikes:

1. Which probes improved performance? Compare their EXPLAINs to original
   — where did row counts drop? Which operators changed?
2. Which probes failed? Learn what NOT to combine.
3. Design 2-3 compound rewrites that stack winning transforms.
   (e.g., if A1 pushed a filter and B1 decorrelated, combine both)

```json
[
  {
    "strike_id": "s1",
    "strategy": "B1+A2: decorrelate + push item filter",
    "confidence": 0.9,
    "sql": "WITH filtered_items AS (...) ..."
  }
]
```

Output ONLY the JSON array.
```

---

## Mode 2: BEAM FOCUSED — Full Strike Package

### Theory

Heavy queries need CREATIVITY. The fix isn't in a template — it requires
reasoning about join topology, decorrelation strategies, and compound
transforms. R1 workers with full briefings, multiple sorties to
compound what works.

### Pipeline

```
STEP 0: Same AST front gate (for analyst context, not filtering)

STEP 1: ANALYST (R1, 1 call)
  Same as current build_beam_prompt_tiered()
  + engine profile + pathology tree
  Outputs 4 deep targets with:
    - Detailed hypothesis (EXPLAIN evidence)
    - Target IR (structural shape)
    - Recommended examples

STEP 2: STRIKES (R1 × 4, parallel)
  Full swarm-style worker briefing (9 sections):
    [1] Role + strategy + approach
    [2] Semantic contract (what MUST be preserved)
    [3] Plan gap (bottleneck diagnosis from analyst)
    [4] Target query map + node contracts (structural blueprint)
    [5] Hazard flags + regression warnings
    [6] Active constraints (engine-specific rules)
    [7] Example adaptation + full before/after SQL
    [8] Original SQL + EXPLAIN
    [9] Column completeness contract + output format

STEP 3-6: Same as wide (tier-1, equivalence, benchmark, EXPLAIN)

STEP 7: SNIPER (R1, up to 4 rounds)
  V4 protocol: Compare EXPLAINs → Design Compound Targets
  Full history table across all sorties
  Each round: 2 new strikes informed by BDA
```

### Focused Worker Prompt (R1) — Swarm-Style Briefing

The focused worker gets the FULL 9-section briefing from `worker.py`.
This is what made the V2 swarm effective:

```
[1] ROLE + ASSIGNMENT
You are a SQL rewrite engine for PostgreSQL v14.3.
Follow the Target Query Map structure below.
Preserve exact semantic equivalence.
Assignment: Strategy: shared_scan_decorrelate | Approach: Convert
correlated scalar subquery to a shared-scan CTE pattern

[2] SEMANTIC CONTRACT
- All WHERE filters preserved exactly
- Same column names, types, ordering
- Row count must match original
- Literal values unchanged (35*0.01 stays as 35*0.01, NOT 0.35)

[3] PLAN GAP (bottleneck)
The Nested Loop at line 8 re-executes the subquery 4,810 times.
Each execution scans web_sales with the same date filter as the
outer query — this is a shared-scan decorrelation opportunity.

[4] TARGET QUERY MAP + NODE CONTRACTS
TARGET_QUERY_MAP:
  S0 [SELECT]
    CTE: common_scan  → FROM web_sales JOIN date_dim
                         WHERE d_date BETWEEN ... AND ws_wholesale_cost ...
                         OUTPUT: ws_item_sk, ws_ext_discount_amt, ws_sales_price, ws_list_price
    CTE: thresholds   → FROM common_scan
                         WHERE ws_sales_price/ws_list_price BETWEEN 35*0.01 AND 50*0.01
                         GROUP BY ws_item_sk
                         OUTPUT: ws_item_sk, threshold (1.3 * AVG(ws_ext_discount_amt))
    MAIN             → FROM common_scan JOIN item JOIN thresholds
                         WHERE i_manufact_id BETWEEN ... AND ws_ext_discount_amt > threshold
                         SELECT SUM(ws_ext_discount_amt) AS "Excess Discount Amount"

[5] HAZARD FLAGS
- Do NOT change 35*0.01 to 0.35 or any other literal simplification
- Do NOT use LEFT JOIN for item (all items must match)
- Keep ORDER BY + LIMIT exactly as original

[6] ACTIVE CONSTRAINTS
- PostgreSQL: CTEs are optimization fences in PG12+, but acceptable
  here because CTE reduces scan count from 2→1

[7] EXAMPLE ADAPTATION
Pattern: pg_shared_scan_decorrelate (8044x on Q92)

BEFORE:
```sql
SELECT ... FROM web_sales ws, item, date_dim
WHERE ws.ws_ext_discount_amt > (
    SELECT 1.3 * avg(ws2.ws_ext_discount_amt)
    FROM web_sales ws2 WHERE ws2.ws_item_sk = ws.ws_item_sk ...
)
```

AFTER:
```sql
WITH common_scan AS (
    SELECT ws_item_sk, ws_ext_discount_amt, ws_sales_price, ws_list_price
    FROM web_sales JOIN date_dim ON ...
    WHERE d_date BETWEEN ... AND ws_wholesale_cost BETWEEN ...
),
thresholds AS (
    SELECT ws_item_sk, 1.3 * AVG(ws_ext_discount_amt) AS threshold
    FROM common_scan
    WHERE ws_sales_price / ws_list_price BETWEEN ...
    GROUP BY ws_item_sk
)
SELECT SUM(cs.ws_ext_discount_amt) AS "Excess Discount Amount"
FROM common_scan cs
JOIN item ON i_item_sk = cs.ws_item_sk
JOIN thresholds t ON cs.ws_item_sk = t.ws_item_sk
WHERE ... AND cs.ws_ext_discount_amt > t.threshold
```

Adapt this pattern to the query below. The key structural change:
correlated subquery → shared CTE → threshold CTE → join.

[8] ORIGINAL SQL
```sql
{original_sql}
```

[9] OUTPUT FORMAT
Output a single SQL query. No explanation. No markdown fences.
Must produce exactly these columns: "Excess Discount Amount"
```

### Focused Sniper Prompt (R1, V4 Protocol, up to 4 rounds)

Same structure as current `build_beam_snipe_prompt` with V4 protocol:

```
## Sortie History

| Sortie | Strike | Family | Transform | Speedup | Status |
|--------|--------|--------|-----------|---------|--------|
| 0      | t1     | B      | decorrelate | 1.45x | WIN |
| 0      | t2     | A      | early_filter | 0.92x | REGRESSION |
| 0      | t3     | E      | materialize | 1.02x | NEUTRAL |
| 0      | t4     | F      | explicit_join | FAIL | Tier-1: literal mismatch |
| 1      | t1     | B+A    | decorrelate+filter | 1.52x | WIN |
| 1      | t2     | A+E    | filter+materialize | 1.38x | WIN |

## EXPLAIN Comparison (latest sortie)

### Original
{original_explain}

### Best Strike (t1, B+A, 1.52x)
{best_explain}

## V4 Protocol

Step 1: COMPARE EXPLAIN PLANS
- What operators changed between original and best?
- Where did row counts drop?
- What NEW bottleneck appeared after the optimization?

Step 2: DESIGN COMPOUND TARGETS
- Build on winning strikes (B decorrelation eliminated 4810 re-scans)
- Fix regressions (A filter placement caused seq scan on item table)
- Compound: best winning strategy + fix for remaining bottleneck

Output 2 new targets.
```

---

## Cost Comparison

### Per Mission (Query)

| Mode | Analyst | Strikes | Snipe Rounds | R1 Calls | Qwen Calls | Est. Cost |
|------|---------|---------|-------------|----------|------------|-----------|
| **Wide** | 1 R1 | 16 qwen | 1 R1 | 2 | 16 | ~$0.15 |
| **Focused** | 1 R1 | 4 R1 | 4 R1 (×2 strikes each) | 13 | 0 | ~$1.30 |

### Per Fleet (76 queries)

| | Heavy (Focused) | Light (Wide) | Total |
|---|---|---|---|
| Missions | 14 | 62 | 76 |
| R1 calls | 182 | 124 | 306 |
| Qwen calls | 0 | 992 | 992 |
| Est. cost | ~$18.20 | ~$9.30 | ~$27.50 |

Current beam: 76 × (1 R1 + 4 qwen + 2 R1 snipe) = 228 R1 + 304 qwen ≈ $24.

~$3.50 more but R1 effort concentrated on queries that matter. The 14 heavy
queries get 13× more R1 reasoning each.

---

## Validation Gates (both modes)

### Front Gate: AST Precondition Filter (beam_wide only)
- `detection.py::detect_transforms()` — already built
- Scores each transform by precondition overlap ratio
- Threshold: >=50% overlap to survive
- Contraindication check: active contraindications → kill
- Result: analyst sees only applicable transforms

### Back Gate: Semantic Validator (both modes)
- **Tier-1** (instant): AST structural check — literals, column refs, column count
  - Fail → retry worker 1x with specific error
  - Still fail → dead
- **Equivalence** (full dataset): row count + MD5 checksum
  - Fail → retry worker 1x with semantic error context
  - Still fail → dead
- **Benchmark** (3x): warmup + avg last 2 runs
  - Only verified rewrites get benchmarked

---

## Implementation Files

| File | Mode | What |
|------|------|------|
| `beam_wide_prompts.py` | Wide | `build_wide_scout_prompt()`, `build_wide_strike_prompt()`, `build_wide_sniper_prompt()` |
| `beam_focused_prompts.py` | Focused | `build_focused_analyst_prompt()`, `build_focused_strike_prompt()`, `build_focused_sniper_prompt()` |
| `beam_router.py` | Both | `classify_workload()` — routes queries to wide/focused |
| `beam_front_gate.py` | Wide | `filter_applicable_transforms()` — wraps `detect_transforms()` with threshold + contraindication logic |
| `beam_session.py` | Both | Updated to call correct prompt builders based on mode |
| `pipeline.py` | Both | Updated to call router, pass mode to session |
| `config.json` | Both | `"beam_mode": "auto"`, `"wide_max_probes": 16`, `"focused_max_sorties": 5` |

## Dashboard Integration (Fleet C2)

Both modes emit the same events for the C2 dashboard:
- `MISSION_START` — query_id, mode (wide/focused), baseline_ms
- `SORTIE_START` — iteration number, n_strikes
- `STRIKE_RESULT` — probe_id, transform, status, speedup
- `BDA_COMPLETE` — n_passed, n_failed, best_speedup
- `SNIPER_RESULT` — compound strike results
- `MISSION_COMPLETE` — final status, best_speedup, best_sql

The C2 panel shows wide missions as a scatter grid (16 probes),
focused missions as a timeline (5 sorties deep).

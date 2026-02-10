# DAG-Based SQL Rewrite System — Prompt Design Specification

## Overview

This document specifies the prompt architecture for an LLM-powered SQL query rewrite system. The design is informed by:

- **"Lost in the Middle"** (Liu et al., 2024 — TACL): LLMs attend most to the beginning and end of context, with significant degradation for information in the middle.
- **Serial Position Effects in LLMs** (Guo & Vosoughi, 2024): Primacy bias is widespread across model families and tasks.
- **Prompt engineering best practices** (Anthropic, OpenAI, 2025): Role framing first, output format last, constraints near output.
- **SOTA SQL rewrite research**: GenRewrite, QUITE, LITHE, R-Bot, Taboola Rapido — all converge on decomposed, per-pattern rewrite with execution plan context.
- **Agentic code editing** (Aider, Codex CLI, Agentless): Search/replace diffs and localize-then-edit outperform whole-file regeneration for large files.

The system uses a **DAG-based pipeline** where SQL structure (CTEs, subqueries, join blocks) defines the decomposition boundary — not line count or token budget.

---

## Architecture: 5-Phase Pipeline

```
┌─────────────────────────────────────────────────────────┐
│                                                         │
│   Raw SQL ──► PARSE ──► ANNOTATE ──► REWRITE ──► REASSEMBLE ──► VALIDATE
│               (det.)    (1 LLM)     (N LLM)     (det.)         (det.+LLM)
│                                                         │
│   Phase 1     Phase 2   Phase 3     Phase 4      Phase 5│
│   No LLM      ~200 tok  ~400 tok    No LLM       Optional
│               output     per node   output                     │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Phase 1: Parse + Build DAG (deterministic, no LLM)

**Input:** Raw SQL file (any size)
**Output:** DAG structure + per-node SQL + edge contracts

Actions:
- AST-parse the SQL
- Extract CTE and subquery nodes
- Resolve inter-node references → directed edges
- Attach column contracts per edge (what columns flow across each edge)
- Compute node depth for topological ordering

```
Example DAG:

  raw_events ──┐
               ├──► daily_agg ──┐
  dim_users ───┘                │
                                ├──► final_output
  dim_products ──► product_agg ─┘

  Depth 0: raw_events, dim_users, dim_products  (source tables)
  Depth 1: daily_agg, product_agg               (parallelizable)
  Depth 2: final_output                         (depends on depth 1)
```

Edge contracts capture:
- **Columns:** what the downstream node consumes
- **Grain:** expected row-level granularity (e.g., one row per user per day)
- **Filters:** any assumed predicates (e.g., date >= '2024-01-01')
- **Cardinality estimate:** approximate row count if available

### Phase 2: Annotate (single LLM call, small input/output)

**Purpose:** Identify which nodes are bottlenecks and what rewrite pattern applies to each.

**Input:** DAG topology + execution plan costs (node names and costs only, NOT full SQL)
**Output:** `{node: pattern}` mapping as JSON, ~200 tokens

This call is intentionally lightweight — the model sees the structure and cost profile, not the code. This keeps it under any token limit and focuses attention on strategic decisions, not implementation.

### Phase 3: Rewrite (per-node LLM calls, DAG-ordered)

**This is the core prompt.** Each flagged node gets its own LLM call with a tightly scoped context window. Nodes are processed in topological order (leaves first), with contract propagation between passes.

See **§ Prompt Template: Per-Node Rewrite** below for the full specification.

### Phase 4: Reassemble (deterministic, no LLM)

- Walk DAG in topological order
- Substitute each rewritten node's SQL back into the original file structure
- Preserve all un-rewritten nodes exactly as-is

### Phase 5: Validate (deterministic + optional LLM)

Validation layers:
1. **Syntax:** AST parse the reassembled SQL — must succeed
2. **Column completeness:** For every edge in the DAG, verify all contracted columns still exist in the upstream node's output
3. **Grain preservation:** Check that GROUP BY / DISTINCT clauses preserve expected grain
4. **EXPLAIN cost comparison:** Run EXPLAIN on original vs. rewritten — confirm cost reduction
5. **Semantic equivalence (optional LLM call):** "Does the rewritten query preserve the semantics of the original?" — yes/no + explanation

If validation fails for a specific node, retry **only that node** (blast radius = 1 node, not the whole file).

---

## Prompt Template: Per-Node Rewrite (Phase 3)

This is the prompt sent for each node that Phase 2 flagged for rewriting. Section ordering follows the attention-optimized structure developed through v1/v2 iteration and validated against primacy/recency research.

### Section Ordering Rationale

```
Position    Section              Attention Zone     Why Here
────────    ──────────────────   ────────────────   ─────────────────────────
1 (START)   Role + Task          PRIMACY (high)     Frames all downstream processing
2           This Node's SQL      PRIMACY (high)     The actual code to rewrite
3           Edge Contracts       PRIMACY (high)     What must be preserved
4           Performance Profile  EARLY              Cost data for this node
5           History              EARLY-MID           What was tried before
6           Pattern Hint         EARLY-MID           Primacy-boosted preview of approach
7           Full Example         MIDDLE              Reference material (1 contrastive pair)
8           Constraints          LATE-MID            Sandwich: critical rules at top+bottom
9 (END)     Output Format        RECENCY (high)     Last thing before generation
```

Key design decisions:
- **Role + actual code first** — model knows what it is and what it's looking at before anything else
- **Pattern hint at position 6** — even if the full example (position 7) suffers middle-zone attention decay, the hint survives in the primacy zone
- **Constraints use internal sandwich** — hardest correctness guards at top and bottom of the constraints block, softer rules in the middle
- **Output format absolutely last** — recency bias means this is the freshest instruction when generation begins

### Section 1: Role + Task (5 lines)

```
You are a SQL query rewrite engine.

Your goal: rewrite a single CTE/subquery node to maximize execution speed
while preserving exact semantic equivalence.

You will receive one node from a larger query DAG, its input/output contracts,
and a suggested rewrite pattern. Apply the pattern. Preserve the contracts.
```

Notes:
- Framing is per-node, not per-file — the model should think locally
- "Preserve exact semantic equivalence" is the prime directive
- Mentioning the DAG context prevents the model from assuming it sees the whole query

### Section 2: This Node's SQL (20-80 lines)

```
## Node: {node_name}

```sql
{node_sql}
```
```

Notes:
- This is the actual CTE or subquery SQL being rewritten
- Extracted by Phase 1 — only this node's code, not the whole file
- For a well-structured query this is typically 20-80 lines — well within any token budget

### Section 3: Edge Contracts (10-20 lines)

```
## Contracts

### Input Edges (what feeds into this node)
- `raw_events`: columns [event_id, user_id, event_ts, amount], grain: one row per event
- `dim_users`: columns [user_id, region, segment], grain: one row per user

### Output Edge (what downstream nodes expect from this node)
- Consumed by: `final_output`
- Required columns: [user_id, day, revenue, event_count]
- Expected grain: one row per user per day
- Assumed filters: date >= '2024-01-01'

### Critical Constraint Echo
- All output columns listed above MUST survive the rewrite
```

Notes:
- Input edges tell the model what data shapes are available
- Output edge is the contract that must not break
- "Critical Constraint Echo" is a primacy-boosted one-liner reinforcing the most violated constraint (CTE_COLUMN_COMPLETENESS). This was added based on attention research — the full constraint appears again in Section 8, but this early echo ensures it's in the high-attention zone

### Section 4: Performance Profile (10-20 lines)

```
## Performance Profile for `{node_name}`

Execution plan cost: {cost}
Percentage of total query cost: {pct}%

Bottleneck operators:
- Sequential Scan on `raw_events`: cost {cost}, rows {rows}
- Hash Join with `dim_users`: cost {cost}

Detected opportunities:
- No partition filter on `raw_events.event_date`
- Window function computed before aggregation (could defer)
- Full dim table scanned despite only 2 columns needed
```

Notes:
- Cost attribution is per-node, from Phase 1's execution plan parsing
- Detected opportunities are merged from what was previously three separate sections (Detected Opportunities, Knowledge Base Patterns, Node-Specific Opportunities) — now one clean block
- Only include opportunities relevant to THIS node

### Section 5: History (5-10 lines)

```
## History

Previous attempts on this node:
- Attempt 1: Added partition filter → 15% improvement, kept
- Attempt 2: Converted to LATERAL join → syntax error, reverted
- No prior attempts on window deferral
```

Notes:
- Prevents the model from re-trying failed approaches
- Keeps track of what's already been applied (cumulative rewrites)
- If no history, this section is omitted entirely

### Section 6: Pattern Hint (3-5 lines)

```
## Suggested Approach

Consider applying: **deferred_window**
- Compute the aggregation first, then apply window functions on the smaller result set
- This node's window runs over {n} million rows pre-aggregation; post-aggregation would be {m} thousand rows
```

Notes:
- This is the primacy-boosted preview from Phase 2's annotation
- Short, strategic, no code — just the "what" and "why"
- Even if the full example in Section 7 gets less attention (middle zone), this hint anchors the approach
- Phrased as "consider applying" — the model can reject it if the code doesn't fit

### Section 7: Example (one contrastive pair, 40-80 lines)

```
## Reference Example: deferred_window

### BEFORE (slow)
```sql
WITH daily AS (
  SELECT
    user_id,
    DATE_TRUNC('day', event_ts) AS day,
    SUM(amount) AS revenue,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY DATE_TRUNC('day', event_ts)) AS rn
  FROM raw_events
  GROUP BY user_id, DATE_TRUNC('day', event_ts)
)
SELECT * FROM daily WHERE rn = 1;
```

**Key insight:** Window function ROW_NUMBER() is computed inside the same CTE
as the aggregation, forcing the engine to materialize all rows before windowing.

### AFTER (fast)
```sql
WITH daily AS (
  SELECT
    user_id,
    DATE_TRUNC('day', event_ts) AS day,
    SUM(amount) AS revenue
  FROM raw_events
  GROUP BY user_id, DATE_TRUNC('day', event_ts)
),
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY day) AS rn
  FROM daily
)
SELECT * FROM ranked WHERE rn = 1;
```

**Key insight:** Aggregation runs first on the full dataset, producing a much smaller
intermediate. Window function then operates on the reduced set.

### When NOT to use this pattern
- When the window function references pre-aggregation columns (e.g., individual event_ts)
- When the GROUP BY doesn't reduce cardinality significantly
- When the window's PARTITION BY differs from the GROUP BY (the split would change semantics)
```

Notes:
- Exactly one example, matched to this node's assigned pattern from Phase 2
- Contrastive pair: BEFORE/AFTER with paired DO/DON'T
- "When NOT to use" is co-located with the example rather than deferred to constraints — research shows contrastive pairs are most effective when paired together
- This section sits in the middle attention zone, which is why the Pattern Hint (Section 6) exists as a primacy-boosted preview

### Section 8: Constraints (40-50 lines)

Internal ordering uses a **sandwich pattern** — critical correctness guards at the top and bottom of this block, softer performance rules in the middle. This exploits the local primacy/recency effect within the constraints section itself.

```
## Constraints

### CRITICAL — Correctness Guards (top of sandwich)

**CTE_COLUMN_COMPLETENESS**
Include ALL columns listed in the output edge contract. If the original node
outputs [user_id, day, revenue, event_count], the rewritten node must also
output exactly those columns (plus any new columns you add). Never drop a column.

**LITERAL_PRESERVATION**
Keep all literal values (dates, strings, numbers) exactly as they appear in
the original SQL. Do not round, truncate, or reformat them.

### HIGH — Performance and Style Rules (middle of sandwich)

**MIN_BASELINE_THRESHOLD**
Only propose a rewrite if the structural change is expected to reduce cost.
Do not rewrite for cosmetic or stylistic reasons alone.

**NO_UNFILTERED_DIM_CTE**
When creating a new CTE that scans a dimension table, include at least one
filter predicate. Never materialize an entire dimension without a WHERE clause.

**OR_TO_UNION_LIMIT**
When converting OR predicates to UNION ALL, limit to 4 branches maximum.
Beyond 4, the UNION overhead exceeds the OR scan cost for most planners.

### CRITICAL — Correctness Guards (bottom of sandwich)

**KEEP_EXISTS_AS_EXISTS**
Preserve EXISTS/NOT EXISTS subqueries as-is. Do not convert them to
IN/NOT IN or to JOINs. The planner handles EXISTS semi-joins efficiently;
conversion risks NULL-handling semantic changes.

**REMOVE_REPLACED_CTES**
If your rewrite splits one CTE into two, remove the original CTE definition.
Do not leave orphaned CTEs in the output.
```

Notes:
- Constraints reframed as positive instructions where possible: "Include ALL columns" instead of "Don't drop columns"; "Keep EXISTS as EXISTS" instead of "NEVER convert EXISTS"
- Positive framing activates the target behavior directly; negation requires the model to parse and invert
- The two OR-related constraints from v1 have been merged into one (OR_TO_UNION_LIMIT)
- Severity labels [CRITICAL] / [HIGH] provide explicit priority signal
- Sandwich ordering: CTE_COLUMN_COMPLETENESS and LITERAL_PRESERVATION at top (local primacy); KEEP_EXISTS_AS_EXISTS and REMOVE_REPLACED_CTES at bottom (local recency, plus proximity to output format)

### Section 9: Output Format (15 lines)

```
## Output

Return a JSON object with exactly this structure:

```json
{
  "node": "{node_name}",
  "rewritten_sql": "WITH daily AS (\n  SELECT ...\n)\nSELECT ...",
  "output_contract": {
    "columns": ["user_id", "day", "revenue", "event_count"],
    "grain": "one row per user per day",
    "filters": ["date >= '2024-01-01'"]
  },
  "changes_summary": "Split aggregation and window into two CTEs for deferred windowing",
  "expected_cost_reduction": "~60% — window operates on 50K rows instead of 12M"
}
```

The `output_contract` is critical — downstream nodes will use it as their input
contract. If you changed columns, grain, or filters, update the contract to match.

Now output your JSON:
```

Notes:
- This is the **absolute last thing** in the prompt, in the recency-advantaged position
- "Now output your JSON:" is the final line — the generation trigger
- The model must output `output_contract` alongside `rewritten_sql` — this is how contract propagation works across the DAG walk
- `changes_summary` and `expected_cost_reduction` provide traceability for human review

---

## Prompt Template: Phase 2 Annotation

This is the single LLM call that analyzes the whole query DAG and assigns patterns to nodes.

```
You are a SQL performance analyst.

## Query DAG Topology

Nodes:
- raw_events (source table, ~50M rows)
- dim_users (source table, ~2M rows)
- dim_products (source table, ~100K rows)
- daily_agg (CTE, depth 1) → depends on: raw_events, dim_users
- product_agg (CTE, depth 1) → depends on: dim_products
- final_output (CTE, depth 2) → depends on: daily_agg, product_agg

## Execution Plan Cost Attribution

Total cost: 148,203
- daily_agg: 89,412 (60.3%)
  - Sequential Scan raw_events: 52,100
  - Hash Join dim_users: 22,300
  - WindowAgg: 15,012
- product_agg: 12,891 (8.7%)
  - Index Scan dim_products: 12,891
- final_output: 45,900 (31.0%)
  - Merge Join: 45,900

## Available Patterns

- deferred_window: move window functions after aggregation
- early_filter: push predicates closer to source scans
- prefetch_fact: pre-filter fact table with selective dimension before main join
- cte_inline: inline small CTEs to enable predicate pushdown
- union_split: convert OR predicates to UNION ALL branches

## Task

For each CTE node, determine:
1. Is it a bottleneck worth rewriting? (>10% of total cost)
2. If yes, which pattern best addresses its dominant cost operator?
3. Brief rationale (one sentence)

Return JSON:

```json
{
  "rewrites": [
    {
      "node": "daily_agg",
      "pattern": "deferred_window",
      "rationale": "WindowAgg runs over full pre-agg rowset; deferring to post-agg saves 15K cost"
    }
  ],
  "skip": [
    {
      "node": "product_agg",
      "reason": "Only 8.7% of cost, index scan already efficient"
    }
  ]
}
```

Now output your JSON:
```

Notes:
- Input is topology + costs only — no SQL code — keeping this call small
- Available patterns listed explicitly so the model picks from a known set, not hallucinated approaches
- Threshold (>10% of total cost) prevents rewriting nodes that don't matter
- The skip field provides traceability for why nodes were left alone

---

## Contract Propagation Protocol

When a node's rewrite changes its output contract, all downstream nodes must see the updated contract before their own rewrite call.

```
Topological walk with contract propagation:

  Depth 0: source tables (no rewrite, contracts from schema)
      │
      ▼
  Depth 1: daily_agg rewritten
      │   → output_contract updated: added partition_key column
      │   → propagate to final_output's input edge contract
      │
  Depth 1: product_agg skipped (below threshold)
      │   → contract unchanged, propagate as-is
      │
      ▼
  Depth 2: final_output rewritten
      │   → sees updated input from daily_agg (includes partition_key)
      │   → sees unchanged input from product_agg
      │
      ▼
  Done → Phase 4 reassembly
```

Rules:
1. **Never rewrite a node before its parents are finalized** (topological guarantee)
2. **Always use the rewritten contract** as input for downstream nodes, not the original
3. **If a rewrite adds columns,** downstream nodes see them and may use them
4. **If a rewrite would drop columns** that are in the output contract, the constraint CTE_COLUMN_COMPLETENESS prevents it at generation time, and Phase 5 validates it post-hoc
5. **Parallel rewrites** are safe only for nodes at the same depth with no shared edges

---

## Token Budget Analysis

Assuming a 16K output token limit (worst case) and typical node sizes:

```
Phase 2 (Annotation):
  Input:  ~800 tokens (topology + costs + pattern list)
  Output: ~200 tokens (JSON mapping)
  Total:  ~1,000 tokens ✓

Phase 3 (Per-node rewrite):
  Section 1 - Role:            ~60 tokens
  Section 2 - Node SQL:        ~200-600 tokens (20-80 lines)
  Section 3 - Contracts:       ~150 tokens
  Section 4 - Performance:     ~120 tokens
  Section 5 - History:         ~80 tokens
  Section 6 - Pattern Hint:    ~50 tokens
  Section 7 - Example:         ~500 tokens
  Section 8 - Constraints:     ~400 tokens
  Section 9 - Output Format:   ~150 tokens
  ──────────────────────────────────────
  Input total:                 ~1,710-2,110 tokens
  Output:                      ~400-800 tokens (rewritten SQL + contract)
  Total per node:              ~2,100-2,900 tokens ✓✓

Phase 5 (Validation, optional):
  Input:  ~1,200 tokens (original + rewritten node SQL)
  Output: ~100 tokens (yes/no + explanation)
  Total:  ~1,300 tokens ✓
```

For a query with 8 CTE nodes, 5 flagged for rewrite:
- Phase 2: 1 call, ~1K tokens
- Phase 3: 5 calls (3 parallel at depth 1, 1 at depth 2, 1 at depth 3), ~2.5K tokens each
- Phase 5: 5 calls, ~1.3K tokens each
- **Total: ~20K tokens across 11 calls, no single call exceeds 3K tokens**

Compare to whole-file approach: 1 call, ~8K input + ~8K output = 16K tokens in a single call that is likely to hit rate limits, lose middle context, and accumulate errors across unrelated nodes.

---

## Attention Map Summary

```
Within each Phase 3 per-node call:

Position    Content              Attention    Purpose
────────    ───────────────────  ──────────   ──────────────────────
START       Role + Task          ██████████   Prime the rewrite mindset
            Node SQL             █████████░   The actual code (primacy)
            Edge Contracts       ████████░░   What must not break
            + Constraint Echo    ████████░░   Column completeness echo
EARLY       Performance Profile  ███████░░░   Why this node is slow
            History              ██████░░░░   What was already tried
EARLY-MID   Pattern Hint         █████░░░░░   Strategic preview (survives)
MIDDLE      Full Example         ████░░░░░░   Reference (may decay)
LATE-MID    Constraints          █████░░░░░   Sandwich: top/bottom strong
            (top: CRITICAL)      ██████░░░░   Local primacy within block
            (mid: HIGH)          ████░░░░░░   Acceptable decay zone
            (bot: CRITICAL)      ██████░░░░   Local recency within block
END         Output Format        █████████░   Recency: freshest at gen time
            "Now output:"        ██████████   Generation trigger
```

---

## Error Recovery Protocol

Because each node is rewritten independently, errors are isolated and recoverable:

```
Per-node retry logic:

  Rewrite node X
      │
      ▼
  Phase 5 validate node X
      │
      ├── Pass → continue to next node
      │
      ├── Syntax error → retry with error message appended to History
      │   (max 2 retries)
      │
      ├── Column completeness fail → retry with explicit missing column list
      │   appended: "Your rewrite dropped column {col}. Include it."
      │   (max 2 retries)
      │
      ├── Cost regression → keep original node, skip rewrite
      │   (log for human review)
      │
      └── Max retries exceeded → keep original node, flag for human review

  Blast radius: 1 node. Other nodes unaffected.
```

---

## Appendix A: Constraint Reference

Full constraint definitions with severity, positive framing, and failure examples.

| ID | Severity | Rule (positive framing) | Failure Mode |
|----|----------|-------------------------|--------------|
| CTE_COLUMN_COMPLETENESS | CRITICAL | Include ALL columns listed in the output edge contract | Downstream node fails with "column not found" |
| LITERAL_PRESERVATION | CRITICAL | Keep all literal values exactly as they appear | Date range shifts, wrong filter boundaries |
| KEEP_EXISTS_AS_EXISTS | CRITICAL | Preserve EXISTS/NOT EXISTS subqueries as-is | NULL-handling semantic change with IN/NOT IN |
| REMOVE_REPLACED_CTES | HIGH | If rewrite splits a CTE, remove the original definition | Orphaned CTE wastes compute |
| MIN_BASELINE_THRESHOLD | HIGH | Only rewrite if structural change reduces cost | Cosmetic rewrites add risk with no benefit |
| NO_UNFILTERED_DIM_CTE | HIGH | When creating a dimension CTE, include a filter predicate | Full dimension materialization wastes memory |
| OR_TO_UNION_LIMIT | HIGH | Limit OR-to-UNION conversions to 4 branches maximum | UNION overhead exceeds OR scan cost beyond 4 |

## Appendix B: Pattern Library (extend as needed)

| Pattern ID | Trigger Signal | One-line Description |
|------------|---------------|----------------------|
| deferred_window | WindowAgg on pre-aggregation rowset | Move window functions to a separate CTE after aggregation |
| early_filter | Sequential Scan with high cost, filter not pushed down | Push WHERE predicates closer to source table scans |
| prefetch_fact | Large fact-dimension join without pre-filter | Pre-filter the fact table with the most selective dimension first |
| cte_inline | Small CTE materialized despite few references | Inline the CTE to allow predicate pushdown by the optimizer |
| union_split | OR predicate preventing index usage | Convert OR branches to UNION ALL for independent index scans |

## Appendix C: Research References

- Liu, N.F. et al. (2024). "Lost in the Middle: How Language Models Use Long Contexts." TACL, 12:157-173.
- Guo, X. & Vosoughi, S. (2024). "Serial Position Effects of Large Language Models." arXiv:2406.15981.
- He, Z. et al. (2024). "Position Engineering: Boosting Large Language Models through Positional Information Manipulation."
- Xia, C. et al. (2024). "Agentless: Demystifying LLM-based Software Engineering Agents."
- Sun, Z., Zhou, X. & Li, G. (2024). "R-Bot: An LLM-based Query Rewrite System." VLDB 2025.
- QUITE (2025). "A Query Rewrite System Beyond Rules via LLM Agents." arXiv:2506.07675.
- E³-Rewrite (2025). "Learning to Rewrite SQL for Executability, Equivalence, and Efficiency."
- GenEdit (2025). "Compounding Operators and Continuous Improvement to Tackle Text-to-SQL." CIDR 2025.
- Taboola Engineering (2025). "How We Developed Our AI-Based SQL Rewrite System (Rapido)."
- Hertwig, F. (2025). "Code Surgery: How AI Assistants Make Precise Edits to Your Files."

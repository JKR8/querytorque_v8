# Dashboard V2 Specification

**Status**: Superseded by `/PLAN.md` (project root) — the master architecture plan.
**Date**: 2026-02-15
**Engines**: PostgreSQL (primary), Snowflake (primary), DuckDB (testing)

## Design Principles

1. **Every view answers one question** — if you can't state the question, the view shouldn't exist
2. **Single source of truth** — Results tab reads from `leaderboard.json`, period
3. **Degrade gracefully** — show "unavailable" when data is missing, never show 0 or hide silently
4. **Decision-grade, not informational** — every metric should lead to an action

---

## Tab 1: FORENSIC — "Where should we spend compute?"

The analyst's pre-execution intelligence briefing. Surfaces data currently trapped in
`explains/`, `qerror_analysis.json`, `engine_profile_{engine}.json`, and `transforms.json`.

### 1.1 Global Health Bar

Compact top strip, always visible.

| Field | Source | Notes |
|-------|--------|-------|
| Engine + version | `config.json` | "PostgreSQL 14.3 / DSB SF10" |
| Total queries | `queries/*.sql` count | |
| Total workload runtime | Sum of EXPLAIN timings | Show "X hrs/day" if > 3600s |
| Dominant pathology | Most frequent pathology_routing from q-error | PG/DuckDB only; hide for Snowflake |
| Estimated opportunity | Sum of baseline_ms for HIGH+MEDIUM bucket queries | "Xms addressable" |

### 1.2 Opportunity Matrix (primary visualization)

**Chart type**: Scatter plot (or bubble chart)

| Axis | Metric | Source | Why |
|------|--------|--------|-----|
| X | Runtime (log scale) | `explains/*.json` execution_time | Pain — how much time is at stake |
| Y | Structural overlap (top match %) | `detect_transforms()` top overlap_ratio | Tractability — how likely we can win |
| Dot size | Number of matching transforms (≥25%) | `detect_transforms()` count | Breadth of attack surface |
| Dot color | Bucket (HIGH=red, MEDIUM=amber, LOW=blue, SKIP=grey) | Derived from runtime | Priority tier |

Click dot → opens query deep-dive drawer (section 1.5).

**Why not q-error on Y-axis**: r=0.007 correlation with speedup on DuckDB. Structural overlap
is the better tractability predictor because it measures "does this query look like something
we've beaten before?"

### 1.3 Cost Pareto

Keep existing Pareto bar chart, enhanced:

- Annotation: "Top N queries = X% of total runtime"
- Inline tractability indicator per bar (filled dots like fleet table)
- Click bar → opens same query deep-dive drawer

### 1.4 Engine Profile Card

From `engine_profile_{engine}.json`. Two panels:

**Strengths** (don't rewrite these patterns):
- Table: Capability | Implication

**Blind Spots** (your opportunity):
- Table: Blind Spot | Consequence | Queries Matching
- "Queries Matching" = count of queries whose pathology_routing includes this gap
- Each row expandable → lists the matching query_ids

### 1.5 Q-Error Diagnostics

**Availability**: PG (full est vs actual from EXPLAIN ANALYZE), DuckDB (precomputed qerror_analysis.json),
Snowflake (operator flow only — no planner estimates, show as "Operator Flow" instead).

**Visualization**: Barbell/lollipop chart per query (sorted by severity):
- Left end: estimated rows (planner)
- Right end: actual rows
- Bar width = severity (wider = more blind)
- Label: worst_node type + locus
- Badge: severity level (CATASTROPHIC / MAJOR / MODERATE / MINOR / ACCURATE)

**PG**: True q-error from `Plan Rows` vs `Actual Rows` in EXPLAIN ANALYZE JSON.
**DuckDB**: From `qerror_analysis.json`.
**Snowflake**: Degrade to operator stats flow (input_rows per operator). Label as
"Operator Flow" not "Q-Error" to avoid false precision.

**Important guardrail**: Header must state data source:
- "From EXPLAIN ANALYZE (actual execution)" → high confidence
- "From EXPLAIN (planner estimates only)" → label as "Estimation Risk"
- "Operator flow data" (Snowflake) → not q-error, different visualization

### 1.6 Per-Query Deep-Dive (Drawer)

Opens as a right-side drawer (not inline expand — prevents scroll monster).
Lazy-loaded on click from any chart/row.

**Three stacked panes**:

1. **Summary bar** (always visible):
   - Query ID, runtime, bucket badge, priority score
   - Structural match % (top), q-error severity badge (where available)
   - Context line: "3rd costliest query, 12% of total runtime, Priority 1"

2. **Signals pane** (collapsible):
   - Matched transforms with overlap bars (like fleet detail, but with contraindication flags prominent)
   - Pathology routing recommendation (from q-error)
   - Structural flags: [CORRELATED_SUB] [CTE_CHAIN] [OR_PREDICATE] etc.
   - Engine blind spot alignment (which gaps from engine profile match)

3. **Evidence pane** (collapsible, starts collapsed):
   - EXPLAIN plan tree (monospace, syntax highlighted)
   - Key operators highlighted (worst q-error node, most expensive node)
   - "Plan not available" message when no EXPLAIN exists

### 1.7 Pattern Coverage Summary

Bottom section. Two views:

**Transform Applicability** (what COULD work):
- Table: Transform ID | Queries Matched | Avg Overlap | Target Blind Spot
- Overlap shown as bar, not just number
- Sorted by total matched queries (descending)
- Distinct naming from Results tab's "Transform Yield"

**Coverage gap**: "X queries have no transform match above 25% — these need discovery mode"

---

## Tab 2: EXECUTION — "What's running and what's next?"

Operational control centre. Stateful, not analytical.

### 2.1 Queue Summary Panel (new, top strip)

| Metric | Source | Format |
|--------|--------|--------|
| Remaining opportunity | Sum baseline_ms for non-WIN queries | "Xms addressable" |
| Breakdown | Count per bucket of non-WIN queries | "X HIGH / Y MED / Z LOW remaining" |
| Est. API calls remaining | Sum max_iterations for non-WIN queries | "~N API calls" |
| Est. token cost | From logged token usage (see 2.5) | "$X.XX estimated" |
| Compute burn rate | Total tokens / total queries completed | "~Nk tokens/query" |

### 2.2 Fleet Table (keep, enhanced)

Existing fleet table IS the control centre. Enhancements:

- **Run selector** (bug fixed — now functional, updates table + stats)
- **Context column** (new): Compact indicator replacing per-query deep dive.
  Format: `#3 | 12% cost | P1` meaning "3rd costliest, 12% of workload, Priority tier 1"
- All existing columns: Query ID, Bucket, Runtime, Tractability, Top Transform, Priority, Status, Speedup
- All existing filters: bucket, status, search
- Sortable by any column

### 2.3 Execution Stats Cards

Same stat cards as current (Queries, Wins, Improved, Neutral, Regression, Error)
but driven by selected run (not always latest).

### 2.4 Run Selector

Already fixed in this session. Dropdown switches table data between runs.

### 2.5 Token Usage Tracking (code change required)

**New**: Log token usage per LLM call in swarm_session.py.

**Data to capture per API call**:
- `prompt_tokens`: int
- `completion_tokens`: int
- `total_tokens`: int
- `model`: str

**Storage**: Add to `session.json` as `token_usage` aggregate:
```json
{
  "token_usage": {
    "prompt_tokens": 45000,
    "completion_tokens": 12000,
    "total_tokens": 57000,
    "n_api_calls": 5
  }
}
```

**Per-call logging**: Add to each worker `result.json`:
```json
{
  "token_usage": {
    "prompt_tokens": 8500,
    "completion_tokens": 3200,
    "total_tokens": 11700
  }
}
```

**Cost estimation**: Apply per-model rate card:
- DeepSeek R1: $0.55/1M input, $2.19/1M output (cache miss)
- GPT-4o: $2.50/1M input, $10.00/1M output
- Claude Sonnet: $3.00/1M input, $15.00/1M output

Rate card stored in `config.json` or settings, not hardcoded.

**Dashboard display**: Total tokens, estimated cost per run, cost per query average.

---

## Tab 3: RESULTS — "What did we achieve?"

Outcomes, learning loop, and the canonical leaderboard.

### 3.1 Hero Metrics (top strip)

| Metric | Source |
|--------|--------|
| Total Runtime Saved | `leaderboard.json` summary or computed from entries |
| Cost Reduction % | (baseline - optimized) / baseline |
| Win Rate | wins / total |
| Avg Speedup (winners only) | Mean of speedup where status=WIN |
| Total Token Cost | From token_usage aggregates across runs |

### 3.2 Leaderboard Table (new — primary view)

Single source of truth: `leaderboard.json`.

| Column | Field |
|--------|-------|
| Rank | By speedup descending |
| Query ID | `query_id` |
| Status | Badge: WIN/IMPROVED/NEUTRAL/REGRESSION/ERROR |
| Speedup | `speedup` with color (green > 1.5x, blue > 1.1x, red < 0.95x) |
| Original (ms) | `original_ms` |
| Optimized (ms) | `optimized_ms` |
| Transforms | `transforms[]` as tags |
| Source | Which mode produced the win (Swarm/Oneshot/Retry) |

Sortable, filterable by status. Search by query_id or transform.

### 3.3 Savings Waterfall (keep)

Existing waterfall chart showing per-query savings contribution.

### 3.4 Transform Yield (new — post-execution effectiveness)

**Distinct from Forensic's "Transform Applicability"**: this shows what DID work, not what COULD work.

| Column | Source | Purpose |
|--------|--------|---------|
| Transform | Aggregated from leaderboard transforms[] | |
| Wins | Count of queries where this transform contributed to WIN | Volume |
| Total ms Saved | Sum of (original_ms - optimized_ms) for winning queries | Impact |
| Median Speedup | Median speedup of queries using this transform | Typical gain |
| Regression Rate | Count where status=REGRESSION / total uses | Risk |

Sorted by Total ms Saved (descending) — not by count. Impact over volume.

### 3.5 Worker Strategy Effectiveness (new)

From swarm session worker results.

| Column | Source |
|--------|--------|
| Worker Slot | W1-W4 |
| Wins | Count where worker was best |
| Avg Speedup | Mean speedup of wins from this slot |
| Typical Strategies | Most frequent strategy names |

### 3.6 Run History (moved from Execution)

Compact cards per historical run:
- Run ID, timestamp, mode
- Status breakdown: X wins / Y improved / Z errors
- Duration, queries completed
- Sorted newest first

### 3.7 Regressions Panel (keep)

List of REGRESSION queries with details.

### 3.8 Resource Impact (keep, PG only)

SET LOCAL aggregate: work_mem total, parallel workers, conflicts, warnings.

---

## Data Contracts

### Canonical Keys

| Key | Format | Used Across |
|-----|--------|-------------|
| `query_id` | Engine-specific (e.g., `q88`, `query001`) | All files. Normalized at dashboard load time |
| `run_id` | `run_YYYYMMDD_HHMMSS` | `runs/` dir names, summary.json |
| `benchmark_id` | Dir name (e.g., `duckdb_tpcds`) | config.json, leaderboard.json |

### Query ID Normalization

Problem: DuckDB uses `q88`, PG uses `query001`, q-error uses `query_1`.
Solution: Dashboard collector normalizes all to the format found in `queries/*.sql` filenames.

### Data Sources Per Tab

```
FORENSIC reads:
  - queries/*.sql              (SQL text)
  - explains/*.json            (EXPLAIN plans, timings)
  - qerror_analysis.json       (estimation errors — DuckDB/PG only)
  - engine_profile_{engine}.json (strengths, gaps)
  - transforms.json            (via detect_transforms() at load time)
  - config.json                (engine, benchmark metadata)

EXECUTION reads:
  - runs/*/summary.json        (run status, per-query results)
  - config.json                (max_iterations, validation settings)
  - swarm_sessions/*/session.json (token_usage — after code change)
  - Everything Forensic reads  (for context column computation)

RESULTS reads:
  - leaderboard.json           (THE source of truth for outcomes)
  - runs/*/summary.json        (run history)
  - swarm_sessions/*/          (worker strategy data, token_usage)
  - Everything Forensic reads  (for resource impact PG data)
```

### Graceful Degradation Rules

| Missing Data | Behavior |
|-------------|----------|
| No `explains/` | Hide Opportunity Matrix Y-axis detail, show "EXPLAIN not available" in deep-dive |
| No `qerror_analysis.json` | Hide Q-Error Diagnostics section entirely |
| No `engine_profile_*.json` | Hide Engine Profile Card, show "Profile not configured" |
| No `leaderboard.json` | Results tab shows "No leaderboard — run `qt leaderboard --build`" |
| No `runs/` | Execution tab shows empty state with instructions |
| No `token_usage` in session | Show "Token tracking not available" instead of $0 |
| Snowflake EXPLAIN | Show "Operator Flow" instead of "Q-Error", different viz |

---

## Build Order

### Phase 1: FORENSIC tab
Highest value — currently the biggest gap. Unlocks "where to spend compute" decisions.
- Build collector pipeline for EXPLAIN, q-error, engine profile data
- Opportunity Matrix scatter plot
- Cost Pareto (enhanced)
- Engine Profile card
- Q-Error diagnostics (barbell chart)
- Per-query deep-dive drawer
- Pattern coverage summary

### Phase 2: RESULTS tab
Second highest — gives the leaderboard and learning loop.
- Leaderboard table from leaderboard.json
- Hero metrics
- Transform Yield table
- Worker strategy effectiveness
- Move run history from Execution
- Keep savings waterfall, regressions, resource impact

### Phase 3: EXECUTION tab
Lowest urgency — fleet table already works, just needs queue summary + context column.
- Queue summary panel
- Context column in fleet table
- Token cost display (depends on Phase 3b)

### Phase 3b: Token Usage Logging (code change)
- Modify `swarm_session.py` to capture token counts from LLM responses
- Add `token_usage` to session.json and worker result.json
- Rate card in config/settings
- Wire into Execution queue summary and Results hero metrics

---

## Visual Design Notes

- Theme: Match existing querytorque.com design system (dark theme, CSS vars)
- Charts: Inline SVG or lightweight charting (no heavy libraries — this is a single HTML file)
- Responsive: Desktop-first (1200px+), don't optimize for mobile
- Drawer: Right-side slide-in panel, 480px wide, overlays content
- Badges: Reuse existing status badge CSS (`.badge.win`, `.badge.regression`, etc.)
- Data freshness: Show "Data as of: {timestamp}" in Global Health Bar from explains provenance

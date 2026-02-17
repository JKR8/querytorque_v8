# QueryTorque — Architecture Plan

**Status**: Approved
**Date**: 2026-02-15
**Scope**: Final architecture — strategy layer, validation harness, fleet dashboard

---

## 0. Core Types

Every layer communicates through these contracts. Engineering builds to these interfaces.

### 0.1 Candidate (Layer 1 → Layer 2)

The unit of work flowing from search strategy into validation.

```python
@dataclass
class Candidate:
    # Identity
    candidate_id: str           # Stable hash: sha256(sql_canonical + dialect)
    sql: str                    # The rewritten SQL
    sql_canonical: str          # sqlglot-normalized form (used for dedup + hash)
    dialect: str                # Target engine dialect

    # Provenance
    source: str                 # "beam" | "strike" | "retry" | "coach"
    worker_id: Optional[int]    # Worker slot (1-4 for beam, None for strike)
    strategy_name: str          # Strategy string (e.g., "decorrelate", "date_cte_isolate")

    # Transform claims
    claimed_transforms: List[str]   # What the generator says it did
    detected_transforms: List[str]  # What detect_transforms() finds in the diff (AST-verified)

    # Metadata
    examples_used: List[str]        # Gold example IDs fed to the worker
    set_local_commands: List[str]   # PG tuning commands (SET LOCAL work_mem = '256MB')
    token_usage: Optional[TokenUsage]  # Tokens consumed generating this candidate

    # Ranking
    rank_features: Dict[str, float]  # Structural scores used by normalize_and_rank:
    #   "parse_valid": 1.0,          #   did it parse?
    #   "column_match": 1.0,         #   do output columns match?
    #   "structural_diff": 0.73,     #   how different is it from original? (0=identical)
    #   "dedup_group": 0,            #   dedup cluster ID (identical canonical SQL = same group)
```

**Candidate identity rule**: `candidate_id = sha256(sql_canonical + dialect)`.
Two candidates with identical normalized SQL for the same dialect are the same candidate,
regardless of which worker/strategy produced them. This enables dedup and verdict caching.

### 0.2 AnalystBriefing (Layer 1 internal)

What the analyst LLM produces. Consumed by beam workers. Not needed for strike.

```python
@dataclass
class AnalystBriefing:
    # Context
    query_id: str
    engine: str
    dialect: str
    baseline_ms: float              # From EXPLAIN timing

    # Key signals (analyst's diagnosis)
    bottleneck_hypothesis: str      # What the analyst thinks is slow and why
    key_signals: List[str]          # Structural flags: ["CORRELATED_SUB", "CTE_CHAIN", ...]
    blind_spot_matches: List[str]   # Engine blind spots that apply: ["CROSS_CTE_PREDICATE_BLINDNESS"]
    match_scores: Dict[str, float]  # Transform overlap scores: {"decorrelate": 0.85, ...}
    opportunity_assessment: str     # "high" | "medium" | "low" | "discovery"

    # Worker assignments
    worker_assignments: List[WorkerAssignment]

    # Evidence references (what data informed this briefing)
    evidence_refs: EvidenceRefs


@dataclass
class WorkerAssignment:
    worker_id: int              # 1-4
    strategy_name: str          # e.g., "decorrelate", "date_cte_isolate"
    role: str                   # "proven_compound" | "structural_alt" | "aggressive" | "exploration"
    primary_family: str         # A-F family code
    examples: List[str]         # Gold example IDs to provide
    hints: str                  # Free-text guidance from analyst
    constraints: str            # Any overrides or restrictions


@dataclass
class EvidenceRefs:
    """What data the analyst had access to — for auditability."""
    has_explain: bool
    has_qerror: bool
    has_engine_profile: bool
    n_matched_examples: int
    n_matched_transforms: int
    explain_source: str         # "EXPLAIN ANALYZE" | "EXPLAIN" | "unavailable"
```

### 0.3 ValidationVerdict (Layer 2 → Layer 3)

The machine-readable output of the validation harness. The single authority on outcomes.

```python
@dataclass
class ValidationVerdict:
    # Identity
    candidate_id: str           # Links back to Candidate
    query_id: str               # Canonical q{N}

    # Decision
    status: str                 # "WIN" | "IMPROVED" | "NEUTRAL" | "REGRESSION" | "FAIL"
    speedup: float              # 1.0 = same, 2.0 = 2x faster, 0.5 = 2x slower

    # Gate results (which gates passed/failed)
    static_passed: bool
    semantic_passed: bool
    semantic_confidence: str    # "HIGH" | "MEDIUM" | "LOW" | "SKIPPED"
    semantic_method: str        # "SAMPLE" | "FULL" | "INVARIANT_ONLY" | "SKIPPED"
    perf_passed: bool
    perf_method: str            # "race" | "sequential_5x" | "sequential_3x"

    # Failure details
    gate_failed: str            # Which gate rejected: "static" | "semantic" | "perf" | "" (pass)
    reason: str                 # Human-readable: "Column mismatch: missing col 'revenue'"
    policy_decision: str        # "allowed" | "blocked:<rule>" | "n/a"

    # Feedback pack (for retry/snipe — derived ONLY from validation, never from LLM)
    feedback_pack: FeedbackPack

    # Diagnostics
    baseline_ms: float          # Original query timing
    candidate_ms: float         # Candidate timing (0 if never reached perf gate)
    row_count_original: int
    row_count_candidate: int


@dataclass
class FeedbackPack:
    """Structured feedback for retry enrichment. Derived only from validation outputs."""
    sql_diff: str                   # Unified diff of original vs rewrite
    semantic_diagnostics: List[str] # Row count mismatches, value diffs
    explain_original: str           # EXPLAIN ANALYZE of original (truncated)
    explain_candidate: str          # EXPLAIN ANALYZE of candidate (truncated)
    race_timings: Optional[Dict]    # Per-lane ms if race was used
```

### 0.4 RunManifest (Layer 3 — written once per run)

Captures the complete environment for reproducibility and trust. Without this,
results differ run-to-run and leaderboards become "vibes."

```python
@dataclass
class RunManifest:
    # Identity
    run_id: str                     # "run_YYYYMMDD_HHMMSS"
    benchmark_id: str               # "duckdb_tpcds", "postgres_dsb_76"
    timestamp_start: str            # ISO 8601
    timestamp_end: str              # ISO 8601

    # Engine environment
    engine: str                     # "duckdb" | "postgresql" | "snowflake"
    engine_version: str             # "0.10.2", "14.3", etc.
    dataset_id: str                 # "tpcds_sf10", "dsb_sf10"
    scale_factor: int               # 1, 10, 100

    # Engine-specific environment fingerprint
    environment: Dict[str, Any]
    # PostgreSQL: {"shared_buffers": "2GB", "work_mem": "4MB", "max_parallel_workers": 2,
    #              "random_page_cost": 1.1, "effective_cache_size": "6GB"}
    # Snowflake:  {"warehouse": "COMPUTE_WH", "warehouse_size": "X-Small",
    #              "account": "CVRYJTF-AW47074", "auto_suspend_s": 300}
    # DuckDB:     {"threads": 8, "memory_limit": "4GB", "version": "0.10.2"}

    # Strategy policy
    strategy_policy: Dict[str, Any]
    # {"default_strategy": "beam", "n_workers": 4, "snipe_enabled": true,
    #  "max_iterations": 3, "model": "deepseek-reasoner"}

    # Validation policy
    validation_policy: Dict[str, Any]
    # {"method": "race", "promote_threshold": 1.05, "regression_threshold": 0.95,
    #  "race_grace_pct": 10, "sequential_n_runs": 5, "semantic_enabled": true,
    #  "semantic_sample_pct": 2.0, "timeout_seconds": 300}

    # Transform policy
    transform_policy: Dict[str, Any]
    # {"allowed_transforms": null (all), "blocked_transforms": ["or_to_union"],
    #  "forbidden_constructs": ["DROP", "DELETE", "INSERT", "UPDATE", "GRANT", ...]}

    # Lineage
    git_commit: str                 # Git SHA at time of run (if available)
    qt_version: str                 # QueryTorque version string
```

**Written once at run start** to `runs/{run_id}/manifest.json`. Never modified after.
The dashboard reads this to display data provenance and detect stale comparisons.

### 0.5 TokenUsage

```python
@dataclass
class TokenUsage:
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    model: str
    estimated_cost_usd: float = 0.0     # Computed from rate card
```

### 0.6 TransformHint (Strike mode)

```python
@dataclass
class TransformHint:
    """User-provided targeting for strike mode. Supports both freeform and structured."""
    text: str = ""                      # Freeform natural language hint
    structured: Optional[Dict] = None   # Optional structured targeting:
    #   {"transform_id": "decorrelate",
    #    "target_subquery": "subq_1",           # optional: which subquery to target
    #    "constraint_mode": "bias"}              # "bias" (prefer) | "constrain" (must use) | "only" (nothing else)
```

`text` is always supported. `structured` is optional — when provided, the worker
receives both. `constraint_mode` determines semantics:
- `bias`: prefer this transform but allow others
- `constrain`: must include this transform, may add others
- `only`: only this transform, nothing else

---

## 1. The Three Layers

QueryTorque has three cleanly separated layers. Each has one job.

```
┌─────────────────────────────────────────────────────┐
│  LAYER 3: FLEET DASHBOARD                           │
│  Orchestrates strategies across a workload.          │
│  Uses forensic intelligence to direct resources.     │
│  Deploys beam or strike based on policy.             │
│                                                      │
│  Tabs: FORENSIC → EXECUTION → RESULTS               │
├─────────────────────────────────────────────────────┤
│  LAYER 2: VALIDATION HARNESS                         │
│  The authority. LLM is a generator; validation       │
│  is the decider. Everything downstream derives       │
│  from validation outcomes only.                      │
│                                                      │
│  Gates: Static → Semantic → Performance → Verdict    │
├─────────────────────────────────────────────────────┤
│  LAYER 1: SEARCH STRATEGIES                          │
│  How candidates are generated. Strategy is selected  │
│  by policy, not hard-coded.                          │
│                                                      │
│  strategy = choose_strategy(briefing, policy)        │
│  candidates = strategy.generate(briefing, history)   │
│  candidates = normalize_and_rank(candidates)         │
└─────────────────────────────────────────────────────┘
```

---

## 2. Search Strategies (Layer 1)

### 2.1 Vocabulary

| Name | Old Name | Description |
|------|----------|-------------|
| **beam** | oneshot / swarm | Automated search. Analyst LLM reasons from EXPLAIN + pathology tree → assigns strategies to N workers → workers generate candidates in parallel. |
| **strike** | *(new)* | User-directed. User picks a specific strategy/transform + target query → single worker executes → validation. User IS the analyst. |

**Fleet is NOT a strategy.** Fleet is the dashboard (Layer 3) that orchestrates beam/strike
across a workload based on policy and forensic intelligence.

### 2.2 Strategy Interface

```python
class SearchStrategy:
    """Base interface for all search strategies."""

    def generate(
        self,
        original_sql: str,
        briefing: AnalystBriefing,
        history: List[IterationResult],
        policy: StrategyPolicy,
    ) -> List[Candidate]:
        """Generate candidate rewrites."""
        raise NotImplementedError


class BeamStrategy(SearchStrategy):
    """Automated: analyst reasons → N workers explore."""

    def generate(self, original_sql, briefing, history, policy):
        # 1. Analyst LLM call → parsed briefing with worker assignments
        # 2. N workers generate in parallel
        # 3. Snipe refinement on best candidate (if enabled)
        ...


class StrikeStrategy(SearchStrategy):
    """User-directed: specific strategy → single worker."""

    def __init__(self, strategy_name: str, hint: TransformHint):
        self.strategy_name = strategy_name
        self.hint = hint  # See §0.6 — supports freeform + structured

    def generate(self, original_sql, briefing, history, policy):
        # 1. No analyst call — user already decided
        # 2. Single worker with user-specified strategy + hint
        # 3. No snipe (single candidate, nothing to refine against)
        ...
```

### 2.3 Strategy Selection

```python
def choose_strategy(briefing: AnalystBriefing, policy: StrategyPolicy) -> SearchStrategy:
    """Select strategy based on policy.

    Policy can be:
    - "beam"   → always use automated search (default for fleet)
    - "strike" → user override with specific strategy
    - "auto"   → system selects based on tractability + opportunity
    """
    if policy.mode == "strike":
        return StrikeStrategy(policy.strategy_name, policy.transform_hint)
    return BeamStrategy(n_workers=policy.n_workers)
```

### 2.4 Candidate Normalization

After generation, all candidates go through the same pipeline regardless of strategy:

```python
candidates = strategy.generate(original_sql, briefing, history, policy)
candidates = normalize_and_rank(candidates)      # parse, dedup, rank by structural quality
results   = validation_harness.run(candidates)    # escalating gate pipeline
```

---

## 3. Validation Harness (Layer 2)

### 3.1 Design Principle

**Validation is the authority.** The LLM is a generator; validation is the decider.
Everything downstream — leaderboard, promotion, analytics — must be derived from
validation outcomes only.

### 3.2 Gate Pipeline

Escalating cost. Each gate is a filter: candidates that fail are removed before the
next (more expensive) gate runs.

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ STATIC GATES │ ──► │SEMANTIC GATES│ ──► │  PERF GATES  │ ──► │   VERDICT    │
│   ~0ms       │     │  10-100ms    │     │   2-30s      │     │              │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
```

#### Gate 1: Static (~0ms)

| Check | What | Current Implementation |
|-------|------|----------------------|
| Parse | SQL parses without error (sqlglot) | `mini_validator.py` Tier 1 |
| Bind | Column names exist in schema | `mini_validator.py` Tier 1 |
| Structure | ORDER BY / LIMIT preserved from original | `mini_validator.py` Tier 1 |
| Forbidden | No DROP, DELETE, INSERT, UPDATE, GRANT | *To add* |
| Policy | Transform complies with allowlist / blocklist | *To add* |

#### Gate 2: Semantic (10-100ms)

| Check | What | Current Implementation |
|-------|------|----------------------|
| Sample equiv | TABLESAMPLE on 2% dataset: row count + value comparison | `mini_validator.py` Tier 2 |
| Column match | Output columns match original (name + type) | `mini_validator.py` Tier 1 |
| Invariant | Key invariants hold (e.g., aggregation doesn't change cardinality direction) | *Future* |

#### Gate 3: Performance (2-30s)

| Check | What | Current Implementation |
|-------|------|----------------------|
| Race | Parallel 5-lane race (original + 4 candidates). Requires original ≥ 2s. | `validate.py::race_candidates()` |
| Sequential | 5x trimmed mean. Fallback when original < 2s. | `validate.py::_timed_runs_pg()` |
| Variance | Winner must beat original by ≥ 5% | Race / sequential thresholds |
| Regression | Candidate slower than 0.95x → REGRESSION | `_classify_speedup()` |

#### Gate 4: Verdict

Every candidate gets a machine-readable verdict:

```python
@dataclass
class ValidationVerdict:
    status: str          # PASS / FAIL / NEUTRAL / REGRESSION
    speedup: float       # 1.0 = same speed, 2.0 = 2x faster
    gate_failed: str     # which gate rejected (empty if PASS)
    reason: str          # human-readable reason
    feedback_pack: dict  # structured feedback for retry (SQL diff, q-error, plan diff)
```

The feedback pack is what the snipe analyst / retry worker receives. It contains:
- SQL diff (original vs rewrite)
- Semantic validation diagnostics (row count mismatch, value diffs)
- EXPLAIN ANALYZE comparison (original vs candidate operator trees)
- Race timings (if available)

### 3.3 Verdict Cache

Perf gates are expensive. Avoid re-running the same candidate across runs, retries,
or dashboard refreshes.

```
cache_key = (candidate_id, run_fingerprint, validation_settings_hash)
```

Where:
- `candidate_id` = sha256(sql_canonical + dialect) — from §0.1
- `run_fingerprint` = hash of (engine_version, dataset_id, scale_factor, environment)
- `validation_settings_hash` = hash of (validation_method, promote_threshold, timeout_seconds, semantic_sample_pct)

**Storage**: `swarm_sessions/{query_id}/.verdict_cache/{candidate_id}.json`
Contains full `ValidationVerdict` + timings. TTL: indefinite (environment changes
invalidate via run_fingerprint, not time).

**Cache hit**: Return cached verdict immediately. Skip all gates.
**Cache miss**: Run full gate pipeline. Write result to cache.

### 3.4 Validation Policy Object

Concrete policy definition. Lives in `config.json` under `validation_policy`:

```json
{
  "validation_policy": {
    "method": "race",
    "promote_threshold": 1.05,
    "regression_threshold": 0.95,
    "race_grace_pct": 10,
    "race_min_grace_ms": 500,
    "sequential_n_runs": 5,
    "max_query_timeout_s": 300,
    "warmup_runs": 1,
    "semantic_enabled": true,
    "semantic_sample_pct": 2.0,
    "semantic_timeout_ms": 30000
  },
  "transform_policy": {
    "allowed_transforms": null,
    "blocked_transforms": [],
    "forbidden_constructs": [
      "DROP", "DELETE", "INSERT", "UPDATE", "GRANT", "REVOKE",
      "COPY", "UNLOAD", "PUT", "GET",
      "CREATE STAGE", "CREATE FUNCTION", "ALTER",
      "CALL"
    ],
    "forbidden_constructs_note": "Engine-specific — Snowflake adds PUT/GET/UNLOAD/CREATE STAGE, PG adds COPY"
  }
}
```

The static gate evaluates `transform_policy.forbidden_constructs` as a blocklist.
The `ValidationVerdict.policy_decision` field records what was checked and whether it passed.

### 3.5 Performance Gate: Resource Controls

Engine-specific safeguards to prevent interference and ensure reproducibility:

| Control | PostgreSQL | Snowflake | DuckDB |
|---------|-----------|-----------|--------|
| Max query timeout | `statement_timeout` SET LOCAL | `QUERY_TAG` + monitor | Built-in timeout |
| Warmup | 1 run before race (buffer cache) | 1 run (warehouse resume) | 1 run (OS cache) |
| Concurrency during race | `benchmark_lock` mutex — one race at a time | Single warehouse, no concurrent queries | Single process |
| Cold start mitigation | N/A | Must detect warehouse resume (95s cold start = invalid) | N/A |
| Cache effects | `pg_prewarm` optional; race makes it irrelevant (all lanes same cache) | Snowflake result cache disabled via `ALTER SESSION` | OS page cache; race makes it irrelevant |

### 3.6 Validation Rules (non-negotiable)

**Two valid ways to validate speedup for reporting:**
1. **3x runs**: Run 3 times, discard 1st (warmup), average last 2
2. **5x trimmed mean**: Run 5 times, remove min/max outliers, average remaining 3

**1-2-1-2 interleaved is screening only — NEVER report as final result.**

Single-run comparisons are unreliable. Snowflake is especially bad: warehouse
suspend/resume causes 95s cold starts that look like "wins" when they hit the original.

---

## 4. Metric Definitions

Three metrics drive the entire system. Each must be reproducible and explainable.

### 4.1 Structural Overlap %

**What**: How closely a query's AST features match a known transform's preconditions.

**Computation**: Jaccard-like overlap ratio via `detect_transforms()`:
```
overlap = len(matched_features) / len(required_features)
```

Where:
- `required_features` = the transform's `precondition_features` list from `transforms.json`
- `matched_features` = features present in the query's AST (extracted by `extract_tags()`)

**Range**: 0.0 to 1.0 (displayed as 0-100%).
**Threshold**: ≥0.25 to be considered a match. ≥0.60 for "high confidence."
**Corpus**: The 32 transforms in `knowledge/transforms.json` — this IS the transform registry.

### 4.2 Tractability (0-4 dots)

**What**: How many transforms have high-confidence structural matches for this query.

**Computation**:
```
tractability = count(transforms where overlap_ratio >= 0.60)
```

Capped at 4 (display limit). A query with tractability 0 triggers discovery mode
(no known patterns match — workers must explore from first principles).

### 4.3 Priority Score

**What**: Composite score determining resource allocation order.

**Formula**:
```
runtime_weight = {SKIP: 0, LOW: 1, MEDIUM: 3, HIGH: 5}[bucket]
structural_bonus = top_match_overlap_ratio  # 0.0 to 1.0
priority = runtime_weight × (1.0 + tractability + structural_bonus)
```

**Range**: 0.0 to ~30+ (unbounded, but practically SKIP=0, LOW=1-4, MEDIUM=6-15, HIGH=10-30).
**Interpretation**:
- 0 = SKIP (runtime < 100ms, not worth optimizing)
- 1-4 = LOW priority (runtime 100ms-1s, few matches)
- 5-15 = MEDIUM priority
- 15+ = HIGH priority (slow query with multiple high-confidence matches)

**User override**: Fleet dashboard allows manual pin/unpin to force a query to top of queue
regardless of computed score. Override is stored in run config, not in the score itself.

### 4.4 Opportunity (aggregate)

**What**: Total addressable runtime — sum of `baseline_ms` for queries in HIGH + MEDIUM buckets
that have NOT yet been won (status != WIN).

Displayed in Execution tab queue summary and Forensic global health bar.

---

## 5. Fleet Dashboard (Layer 3)

### 5.1 Three-Tab Architecture

Each tab answers one question. If you can't state the question, the view shouldn't exist.

| Tab | Question | Data Sources |
|-----|----------|-------------|
| **FORENSIC** | "Where should we spend compute?" | explains/, qerror_analysis.json, engine_profile.json, transforms.json |
| **EXECUTION** | "What's running and what's next?" | runs/, config.json, session token_usage |
| **RESULTS** | "What did we achieve?" | leaderboard.json (single source of truth) |

### 5.2 Query ID Normalization

**Canonical format: `q{N}`** — e.g., `q88`, `q1`, `q102`.

All systems normalize to this format:
- `query_88` → `q88`
- `query_1` → `q1`
- `q88` → `q88` (already canonical)

Normalization happens at the data boundary (collector, loader) — never in storage.
Source files keep their original naming. The dashboard normalizes at load time.

---

## 6. Tab 1: FORENSIC — "Where should we spend compute?"

The analyst's pre-execution intelligence briefing. Surfaces data currently trapped
in disk files. This is the pre-flight gatekeeper — uses intelligence to direct
resources toward important AND winnable queries.

### 6.1 Global Health Bar

Compact top strip, always visible.

| Field | Source |
|-------|--------|
| Engine + version + benchmark | `config.json` |
| Total queries / total workload runtime | `queries/*.sql` count, sum of EXPLAIN timings |
| Dominant pathology | Most frequent pathology_routing from q-error (PG/DuckDB only) |
| Estimated opportunity | Sum of baseline_ms for HIGH + MEDIUM bucket queries |

### 6.2 Opportunity Matrix (primary visualization)

Scatter/bubble plot. Every query is a dot. Click → opens deep-dive drawer.

| Axis | Metric | Why |
|------|--------|-----|
| X | Runtime (log scale) | Pain — how much time is at stake |
| Y | Structural overlap (top match %) | Tractability — how likely we can win |
| Size | Number of matching transforms (≥25%) | Breadth of attack surface |
| Color | Bucket (HIGH=red, MEDIUM=amber, LOW=blue, SKIP=grey) | Priority tier |

**Why structural overlap on Y, not q-error**: Tested on DuckDB TPC-DS, q-error has
r=0.007 correlation with speedup. Structural overlap measures "does this query look
like something we've beaten before?" — a better tractability predictor.

### 6.3 Cost Pareto (enhanced)

Bar chart, sorted by runtime descending, with cumulative % line.

- Annotation: "Top N queries = X% of total runtime"
- Inline tractability dots per bar
- Click bar → opens deep-dive drawer

### 6.4 Engine Profile Card

From `engine_profile_{engine}.json`.

**Strengths** (don't rewrite): Table with Capability | Implication
**Blind Spots** (opportunity): Table with Blind Spot | Consequence | N Queries Matching

Each blind spot row shows count of queries whose pathology routing matches.
Expandable → lists matching query IDs.

### 6.5 Q-Error / Estimation Diagnostics

Engine-dependent visualization:

| Engine | Data Source | Visualization | Label |
|--------|-----------|---------------|-------|
| PostgreSQL | EXPLAIN ANALYZE: `Plan Rows` vs `Actual Rows` | Barbell chart (est ← → actual per operator) | "Estimation Error (EXPLAIN ANALYZE)" |
| DuckDB | `qerror_analysis.json` | Barbell chart (est vs actual) | "Estimation Error (EXPLAIN ANALYZE)" |
| Snowflake | `operator_stats`: input_rows per operator (no estimates) | Operator flow (rows in/out per stage) | "Operator Flow" (NOT "Q-Error") |

**Guardrail**: Header must state data source and confidence level.
When data is missing, show "Not available for this engine" — never show 0.

Sorted by severity. Click bar → opens deep-dive drawer on that query.

### 6.6 Per-Query Deep-Dive (Drawer)

Right-side slide-in drawer (480px). Lazy-loaded on click from any chart/table.

**Three stacked panes:**

1. **Summary bar** (always visible):
   - Query ID, runtime, bucket badge, priority score
   - Context: "3rd costliest, 12% of total runtime, Priority 1"
   - Structural match %, q-error severity badge (where available)

2. **Signals** (collapsible, default open):
   - Matched transforms with overlap bars + contraindication flags
   - Pathology routing (from q-error when available)
   - Structural flags: `[CORRELATED_SUB]` `[CTE_CHAIN]` `[OR_PREDICATE]` etc.
   - Engine blind spot alignment

3. **Evidence** (collapsible, default collapsed):
   - EXPLAIN plan tree (monospace)
   - "Plan not available" when no EXPLAIN exists
   - **Truncation rules**: Show the "hot path subtree" first — the path from root
     to the most expensive operator (by timing or q-error). If no timing data,
     show first 80 lines. Always show the operator with worst q-error and the
     operator with highest exclusive time, even if outside the 80-line window.
   - **Highlight rules**: Bold/color the worst q-error node (red), the costliest
     node by time (amber), and any DELIM_SCAN / nested loop nodes (blue).

### 6.7 Pattern Coverage

Bottom section.

**Transform Applicability** (pre-execution — what COULD work):
- Table: Transform ID | Queries Matched | Avg Overlap | Target Blind Spot
- Overlap shown as visual bar
- Sorted by query count descending

**Coverage gap**: "X queries have no match above 25% — discovery mode required"

Named "Applicability" to distinguish from Results tab's "Transform Yield" (post-execution).

---

## 7. Tab 2: EXECUTION — "What's running and what's next?"

Operational control centre. Stateful, not analytical. Boringly operational.

### 7.1 Queue Summary Panel

Top strip with forecast-like metrics:

| Metric | Source |
|--------|--------|
| Remaining opportunity | Sum baseline_ms for non-WIN queries |
| Breakdown | X HIGH / Y MEDIUM / Z LOW remaining |
| Est. API calls remaining | Sum max_iterations for non-WIN queries |
| Est. token cost (when available) | From logged token_usage × rate card |
| Compute burn rate | Total tokens / queries completed |

### 7.2 Fleet Table (the control centre)

Sortable, filterable table. One row per query.

| Column | Source |
|--------|--------|
| Query ID | Canonical `q{N}` format |
| Bucket | HIGH / MEDIUM / LOW / SKIP badge |
| Runtime | From EXPLAIN timing |
| Tractability | Filled dots (0-4) |
| Top Transform | Best structural match |
| Priority | Composite score |
| Context | Compact: "#3 · 12% cost · P1" |
| Status | WIN / IMPROVED / NEUTRAL / REGRESSION / ERROR / -- |
| Speedup | Colored value |

**Run selector** (dropdown): Switches table data between historical runs.
Change handler re-renders table + stats with selected run's per-query results.

**Filters**: Bucket, status, text search (query ID, transform name).

### 7.3 Execution Stats Cards

Compact cards: Queries, Wins, Improved, Neutral, Regression, Error.
Driven by selected run (not always latest).

---

## 8. Tab 3: RESULTS — "What did we achieve?"

Canonical outcomes + learning loop. Single source of truth: `leaderboard.json`.

### 8.1 Hero Metrics

| Metric | Source |
|--------|--------|
| Total Runtime Saved | Computed from leaderboard entries |
| Cost Reduction % | (total_baseline - total_optimized) / total_baseline |
| Win Rate | wins / total |
| Avg Speedup (winners) | Mean speedup where status=WIN |
| Total Token Cost | From token_usage aggregates (when available) |

### 8.2 Leaderboard Table (primary view)

From `leaderboard.json`. Sortable, filterable.

| Column | Field |
|--------|-------|
| Rank | By speedup descending |
| Query ID | Canonical `q{N}` |
| Status | Badge |
| Speedup | Color-coded |
| Original (ms) | `original_ms` |
| Optimized (ms) | `optimized_ms` |
| Transforms | Tags |
| Source | beam / strike / retry |

### 8.3 Savings Waterfall

Existing waterfall chart — per-query savings contribution.

### 8.4 Transform Yield (post-execution effectiveness)

**Named "Yield" to distinguish from Forensic's "Applicability".**

| Column | Meaning |
|--------|---------|
| Transform | Aggregated from leaderboard `transforms[]` |
| Wins | Count where this transform contributed to WIN |
| Total ms Saved | Sum of (original_ms - optimized_ms) for wins using this transform |
| Median Speedup | Of queries using this transform |
| Regression Rate | REGRESSION count / total uses |

Sorted by Total ms Saved (impact over volume).

### 8.5 Worker Strategy Effectiveness

From swarm session worker results.

| Column | Source |
|--------|--------|
| Worker Slot | W1-W4 |
| Wins | Count where this worker was best |
| Avg Speedup | Mean speedup of wins |
| Top Strategies | Most frequent strategy names for this slot |

### 8.6 Run History (moved from Execution)

Compact cards per historical run: timestamp, mode, status breakdown, duration.

### 8.7 Regressions Panel

List of REGRESSION queries with diagnostics.

### 8.8 Resource Impact (PG only)

SET LOCAL aggregate: work_mem total, parallel workers, conflicts, warnings.

---

## 9. Token Usage Tracking

### 9.1 What to capture per LLM call

```python
@dataclass
class TokenUsage:
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    model: str
```

### 9.2 Storage

**Per worker** (in `worker_*/result.json`):
```json
{ "token_usage": { "prompt_tokens": 8500, "completion_tokens": 3200, "total_tokens": 11700 } }
```

**Per session** (in `session.json`, aggregated):
```json
{
  "token_usage": {
    "prompt_tokens": 45000,
    "completion_tokens": 12000,
    "total_tokens": 57000,
    "n_api_calls": 5,
    "estimated_cost_usd": 0.12
  }
}
```

### 9.3 Cost Estimation

Rate card in `config.json` or settings (not hardcoded):

```json
{
  "token_rates": {
    "deepseek-reasoner": { "input_per_1m": 0.55, "output_per_1m": 2.19 },
    "gpt-4o": { "input_per_1m": 2.50, "output_per_1m": 10.00 },
    "claude-sonnet-4-5-20250929": { "input_per_1m": 3.00, "output_per_1m": 15.00 }
  }
}
```

### 9.4 Code Changes Required

1. `swarm_session.py` — capture token counts from LLM provider response after each call
2. `generate.py` / `CandidateGenerator` — return token_usage alongside candidate
3. `schemas.py` — add `TokenUsage` dataclass, add to `WorkerResult` and `SessionResult`
4. `session.json` writer — aggregate and write token_usage
5. Dashboard collector — aggregate token_usage across sessions for display

---

## 10. Data Contracts

### 10.1 Canonical Keys

| Key | Format | Rule |
|-----|--------|------|
| `query_id` | `q{N}` (e.g., `q88`) | Normalized at data boundary, never in storage |
| `run_id` | `run_YYYYMMDD_HHMMSS` | From `runs/` directory names |
| `benchmark_id` | Directory name (e.g., `duckdb_tpcds`) | From `config.json` |

### 10.2 Data Sources Per Tab

```
FORENSIC reads:
  queries/*.sql                → SQL text
  explains/*.json              → EXPLAIN plans, operator trees, timings
  qerror_analysis.json         → estimation errors (DuckDB/PG only)
  engine_profile_{engine}.json → strengths, gaps
  transforms.json              → via detect_transforms() at load time
  config.json                  → engine, benchmark metadata

EXECUTION reads:
  runs/*/summary.json          → run status, per-query results
  config.json                  → max_iterations, validation settings
  session.json token_usage     → token cost tracking
  + everything FORENSIC reads  → for context column

RESULTS reads:
  leaderboard.json             → THE source of truth for outcomes
  runs/*/summary.json          → run history
  swarm_sessions/*/            → worker strategy data, token_usage
  + FORENSIC data              → for resource impact (PG)
```

### 10.3 Graceful Degradation

| Missing Data | Dashboard Behavior |
|-------------|-------------------|
| No `explains/` | Opportunity Matrix X-axis uses placeholder, deep-dive shows "EXPLAIN not available" |
| No `qerror_analysis.json` | Q-Error section hidden entirely |
| No `engine_profile_*.json` | Engine Profile card shows "Not configured" |
| No `leaderboard.json` | Results tab shows "No leaderboard — run `qt leaderboard --build`" |
| No `runs/` | Execution tab empty state with instructions |
| No `token_usage` | Shows "Token tracking not available" — never shows $0 |
| Snowflake EXPLAIN | Shows "Operator Flow" not "Q-Error", different viz |
| PG without ANALYZE | Label as "Estimation Risk" not "Q-Error" |

### 10.4 Single Source of Truth Enforcement

| Data | SSOT File | Operational File | Rule |
|------|-----------|-------------------|------|
| Per-query verdict (for reporting) | `leaderboard.json` | `runs/*/summary.json` | Runs are operational (queue state, errors). Leaderboard is final. If a run completes, a build step materializes/updates the leaderboard. |
| Transform IDs | `knowledge/transforms.json` | Worker strategy names, detected_transforms | All transform strings must exist in the registry. Unknown strings are flagged, not silently accepted. |
| Query identity | `queries/*.sql` filenames | Various (qerror uses `query_1`, leaderboard uses `q88`) | Canonical `q{N}` in all derived JSON objects. Source filenames stay as-is. |

**Leaderboard rebuild rule**: After any run completes, `qt leaderboard --build` should
be run to update the canonical leaderboard. The Results tab reads ONLY from
`leaderboard.json` — never directly from `runs/*/summary.json` for final outcomes.

### 10.5 Transform Registry

`knowledge/transforms.json` is the single registry. 32 transforms, each with:

```
id               — stable string ID (e.g., "decorrelate", "date_cte_isolate")
principle        — what the transform does
precondition_features — AST features required for this transform to apply
contraindications — conditions that make this transform dangerous
gap              — engine blind spot this targets (e.g., "CROSS_CTE_PREDICATE_BLINDNESS")
engines          — which engines this applies to (["duckdb", "postgresql", "snowflake"])
family           — category code (A=Early Filtering, B=Decorrelation, C=Aggregation,
                   D=Set Ops, E=Materialization, F=Join Transform)
```

**Rule**: All transform strings in Candidates, Verdicts, Leaderboard, and Worker assignments
must be valid IDs from this registry. Unknown strings are logged as warnings.

---

## 11. Implementation Stages

### Stage 1: Forensic Tab + RunManifest
**Priority: Highest** — currently the biggest gap. Unlocks "where to spend compute."
RunManifest included here because Forensic is the first thing people trust — it needs
data provenance and freshness signals from day 1.

Tasks:
1. Define and implement `RunManifest` dataclass (§0.4) — written to `runs/{run_id}/manifest.json`
2. Build forensic collector: load EXPLAIN plans, q-error data, engine profiles per query
3. Query ID normalization utility (`q{N}` canonical format)
4. Opportunity Matrix (scatter/bubble chart, inline SVG)
5. Enhanced Cost Pareto (with tractability indicators, "Top N = X%" annotation)
6. Engine Profile card (strengths/gaps from engine_profile JSON)
6. Q-Error barbell chart (PG/DuckDB, degrade for Snowflake)
7. Per-query deep-dive drawer (3-pane: summary, signals, evidence)
8. Pattern Applicability table (bottom section)
9. Global Health Bar (top strip)

### Stage 2: Results Tab
**Priority: High** — canonical P&L and learning loop.

Tasks:
1. Leaderboard table from `leaderboard.json`
2. Hero metrics (savings, win rate, avg speedup)
3. Transform Yield table (post-execution effectiveness, sorted by ms saved)
4. Worker strategy effectiveness (W1-W4 analysis)
5. Move run history cards from Execution
6. Keep: savings waterfall, regressions, resource impact

### Stage 3: Execution Tab
**Priority: Medium** — fleet table already works, just needs queue summary + context.

Tasks:
1. Queue summary panel (remaining opportunity, budget forecast)
2. Context column in fleet table ("#3 · 12% cost · P1")
3. Token cost display (depends on Stage 4)

### Stage 4: Token Usage Logging
**Priority: Medium** — code change in session layer.

Tasks:
1. Add `TokenUsage` dataclass to schemas
2. Capture token counts from LLM provider responses in `generate.py`
3. Aggregate in `swarm_session.py`, write to `session.json`
4. Per-worker token_usage in `result.json`
5. Rate card in config/settings
6. Wire into dashboard (Execution queue summary + Results hero metrics)

### Stage 5: Strategy Layer Refactor
**Priority: Lower** — rename + new strike mode.

Tasks:
1. Rename: oneshot → beam throughout codebase
2. Create `StrikeStrategy` class (user-directed single worker)
3. Refactor session classes → strategy interface
4. Update CLI: `qt run --strategy beam|strike --transform decorrelate`
5. Update fleet orchestrator to deploy beam/strike based on policy
6. Update dashboard vocabulary

### Stage 6: Validation Harness Formalization
**Priority: Lower** — mostly already implemented, needs formalization.

Tasks:
1. Add forbidden constructs gate (DROP, DELETE, INSERT, UPDATE, GRANT)
2. Add transform policy compliance gate (allowlist/blocklist)
3. Formalize `ValidationVerdict` dataclass with machine-readable reason tree
4. Unify feedback pack structure for retry enrichment
5. Document gate pipeline as code (not just description)

---

## 12. Visual Design

- **Theme**: querytorque.com design system (dark theme, CSS custom properties)
- **Charts**: Inline SVG — no heavy chart libraries. Single HTML file.
- **Drawer**: Right-side slide-in, 480px wide, overlays content
- **Badges**: Reuse existing CSS (`.badge.win`, `.badge.regression`, bucket badges)
- **Responsive**: Desktop-first (1200px+), no mobile optimization
- **Data freshness**: "Data as of: {timestamp}" in Global Health Bar

---

## 13. Files Affected (Reference)

### Dashboard (Stages 1-3)
```
qt_sql/dashboard/
  index.html           → Complete rewrite (3-tab layout, charts, drawer)
  collector.py          → Major expansion (EXPLAIN, q-error, engine profile loading)
  models.py             → New dataclasses (ForensicQuery, OpportunityEntry, etc.)
  DASHBOARD_SPEC.md     → This spec (detailed, superseded by PLAN.md)
```

### Token Tracking (Stage 4)
```
qt_sql/schemas.py                → TokenUsage dataclass
qt_sql/generate.py               → Return token counts from LLM calls
qt_sql/sessions/swarm_session.py → Aggregate + persist token_usage
qt_sql/sessions/base_session.py  → TokenUsage in SessionResult
```

### Strategy Layer (Stage 5)
```
qt_sql/sessions/
  base_session.py        → Strategy interface
  beam_session.py        → Renamed from swarm_session.py
  strike_session.py      → New: user-directed single worker
  oneshot_session.py     → Deprecated (replaced by beam)
qt_sql/cli/__init__.py   → Updated command vocabulary
qt_sql/fleet/            → Updated orchestrator
```

### Validation (Stage 6)
```
qt_sql/validation/
  mini_validator.py      → Add forbidden constructs + policy gates
  harness.py             → New: unified gate pipeline entry point
qt_sql/schemas.py        → ValidationVerdict dataclass
```

# Knowledge Engine Contract

Data contract for the Knowledge Engine — the human-authored intelligence layer that feeds the `qt_sql` optimization pipeline.

**Companion to `PRODUCT_CONTRACT.md`** — that document defines pipeline *flow* (how data moves through phases). This document defines data *states* (what each artifact IS — schema, required fields, validation rules). No workflow process, no pipeline flow — just the artifacts.

**Key design decisions:**
- `dialect` not `engine` — system-wide field name for database engine (values: `duckdb`, `postgresql`)
- `OptimizationPattern` folded into EngineProfile gaps — no separate artifact
- `KnowledgePrinciple` folded into EngineProfile gaps — purge as we go, no legacy compat layer
- Nested validation object (`confidence` + `rows_match` + `checksum_match`) instead of flat fields
- Worker ID nullable — supports 4w_worker, plan_scanner, and expert_session sources
- Transform enum whitelist enforced across all artifacts
- Config object merges `set_local` + `plan_flags` + `impact_additive`

---

## Shared Vocabulary

Enums and constants referenced across multiple artifacts. All consumers must use these values exactly.

### Dialect

```
duckdb | postgresql
```

### Status

```
WIN | IMPROVED | NEUTRAL | REGRESSION | ERROR | FAIL
```

Classification thresholds (from `PRODUCT_CONTRACT.md` Phase 6):

| Label | Threshold |
|-------|-----------|
| WIN | >= 1.10x |
| IMPROVED | >= 1.05x |
| NEUTRAL | >= 0.95x |
| REGRESSION | < 0.95x |
| ERROR | Execution or validation error |
| FAIL | Structural failure (parse, column mismatch) |

### Tier

```
CRITICAL_HIT | WIN | IMPROVED | NEUTRAL | REGRESSION | ERROR
```

Tier refines Status for analysis sessions. `CRITICAL_HIT` = WIN with speedup >= 5.0x.

### Speedup Type

```
measured | vs_timeout_ceiling | both_timeout
```

| Value | Meaning |
|-------|---------|
| `measured` | Both queries ran to completion; speedup is a real ratio |
| `vs_timeout_ceiling` | Original timed out; baseline is timeout ceiling. Ratio is inflated. |
| `both_timeout` | Both timed out; speedup is meaningless (set to 1.0) |

**Rule:** `vs_timeout_ceiling` results must never be mixed with `measured` in aggregate statistics.

### Validation Confidence

```
high | row_count_only | zero_row_unverified
```

| Value | Meaning | When |
|-------|---------|------|
| `high` | Row count + MD5 checksum match | DuckDB path |
| `row_count_only` | Row counts match, no checksum | PostgreSQL path |
| `zero_row_unverified` | Both return 0 rows — trivially matching | Either dialect, 0 rows |

### Error Category

```
syntax | semantic | timeout | execution | unknown
```

### Transform Whitelist

13 known transforms. Every `transforms.all[]` value in OptimizationOutcome and every `classification.transforms[]` value in GoldExample must be one of these:

| Transform | Description |
|-----------|-------------|
| `decorrelate` | Convert correlated subquery to GROUP BY + JOIN |
| `early_filter` | Apply selective filters earlier in execution |
| `pushdown` | Push predicates through query boundaries |
| `date_cte_isolate` | Extract date filtering into a separate CTE |
| `dimension_cte_isolate` | Extract dimension lookup into a CTE |
| `prefetch_fact_join` | Pre-join fact table with filtered dimensions |
| `multi_dimension_prefetch` | Multiple dimension pre-filters in parallel CTEs |
| `multi_date_range_cte` | Multiple date range extractions into CTEs |
| `single_pass_aggregation` | Consolidate N subqueries into 1 scan with CASE/FILTER |
| `or_to_union` | Split cross-column OR into UNION ALL branches |
| `intersect_to_exists` | Convert INTERSECT to EXISTS semi-join |
| `materialize_cte` | Strategic CTE materialization for reuse |
| `union_cte_split` | Split UNION CTE self-joined N times into N CTEs |

**Extension rule:** New transforms may be added to this list. Consumers must treat unknown transforms as valid (log warning, don't reject).

### Finding Category

```
scan_method | join_optimization | aggregation | subquery | config_tuning | index_usage | parallelism | materialization
```

### Scanner Finding Category

```
join_sensitivity | memory | parallelism | jit | cost_model | join_order | scan_method | interaction | config_sensitivity
```

---

## Artifact 1: OptimizationOutcome (Auto — Pipeline Phase 7)

One JSONL entry per optimization attempt. The raw learning record from every pipeline run.

**Format:** JSONL (one JSON object per line, append-only)
**Producer:** `build_blackboard.py`, `store.py`
**Consumers:** Analysis sessions (human review), learning analytics, blackboard aggregation

### Schema

```json
{
  "schema_version": "2.0",

  "base": {
    "query_id": "q88",
    "dialect": "duckdb",
    "fingerprint": "star_5t_3cte_2or",
    "timestamp": "2026-02-11T10:30:00Z",
    "run_id": "swarm_batch_20260211_103000"
  },

  "source": {
    "type": "4w_worker"
  },

  "opt": {
    "worker_id": 4,
    "strategy": "single_pass_aggregation",
    "examples_used": ["q9_single_pass", "q88_or_to_union"],
    "iteration": 0,
    "optimized_sql": "WITH ...",
    "engine_profile_version": "2026.02.09-v1"
  },

  "outcome": {
    "status": "WIN",
    "speedup": 5.25,
    "speedup_type": "measured",
    "timing": {
      "original_ms": 892.4,
      "optimized_ms": 170.1
    },
    "validation": {
      "confidence": "high",
      "rows_match": true,
      "checksum_match": true
    }
  },

  "transforms": {
    "primary": "single_pass_aggregation",
    "all": ["single_pass_aggregation", "or_to_union"]
  },

  "config": {
    "set_local": { "work_mem": "512MB" },
    "plan_flags": { "enable_nestloop": "off" },
    "impact_additive": 1.15
  },

  "error": null,

  "reasons": {
    "reasoning_chain": "Consolidated 8 time-bucket subqueries into single scan...",
    "evidence": "EXPLAIN shows 8x SEQ_SCAN on store_sales reduced to 1"
  },

  "tags": ["star_schema", "multi_scan", "time_bucket"],

  "provenance": {
    "model": "deepseek-reasoner",
    "provider": "deepseek",
    "git_sha": "bbb5576a",
    "knowledge_version_used": "2026.02.09-v1"
  },

  "version": {
    "schema_version": "2.0",
    "entry_version": 1,
    "status": "active"
  }
}
```

### Required Fields

| Section | Required | Optional |
|---------|----------|----------|
| `base` | `query_id`, `dialect`, `timestamp`, `run_id` | `fingerprint` |
| `source` | `type` | `scanner_config` |
| `opt` | `strategy`, `iteration` | `worker_id` (nullable), `examples_used`, `optimized_sql`, `engine_profile_version` |
| `outcome` | `status`, `speedup`, `speedup_type` | `timing`, `validation` |
| `transforms` | — | `primary`, `all` |
| `config` | — | `set_local`, `plan_flags`, `impact_additive` |
| `error` | — | `category`, `messages` |
| `provenance` | `model`, `provider` | `git_sha`, `knowledge_version_used` |
| `version` | `schema_version` | `entry_version`, `status` |

### Source Types

| `source.type` | `opt.worker_id` | Meaning |
|----------------|-----------------|---------|
| `4w_worker` | 1-4 | Standard 4-worker swarm pipeline |
| `plan_scanner` | null | PostgreSQL plan-space scanner |
| `expert_session` | null | Iterative expert session |

### Validation Rules

1. `base.dialect` must be in `{duckdb, postgresql}`
2. `outcome.status` must be in the Status enum
3. `outcome.speedup_type` must be in the Speedup Type enum
4. `transforms.all[]` values must be in the Transform Whitelist
5. `error.category` (when present) must be in the Error Category enum
6. `version.schema_version` must be `"2.0"`
7. `config.set_local` keys must be in the `PG_TUNABLE_PARAMS` whitelist (see `sql_rewriter.py`)

---

## Artifact 2: ScannerObservation (Auto — Plan Scanner, PG only)

Raw plan-space exploration data from the PostgreSQL plan scanner. One JSONL line per (query, flags) combination.

**Format:** JSONL
**Producer:** `plan_scanner.py` (explore/scan/stacking layers)
**Consumer:** ScannerFinding extraction (LLM), scanner knowledge pipeline
**Ground truth:** `scanner_knowledge/schemas.py:ScannerObservation`

### Schema

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `query_id` | string | Yes | Query identifier |
| `flags` | object | Yes | Canonical merge key — `{flag_name: value}` pairs |
| `source` | string | Yes | `"explore"`, `"scan"`, `"explore+scan"`, `"stacking"` |
| `category` | string | Yes | Derived from flags: `join_method`, `scan_method`, `memory`, `parallelism`, `jit`, `cost_model`, `join_order`, `compound`, `unknown` |
| `combo_name` | string | Yes | Human-readable: sorted flag keys joined with `+` |
| `summary` | string | Yes | 2-3 sentence summary |
| `plan_changed` | bool | No | Whether EXPLAIN plan changed from baseline |
| `cost_ratio` | float | No | Estimated cost ratio vs baseline |
| `wall_speedup` | float | No | Measured wall-clock speedup |
| `baseline_ms` | float | No | Baseline execution time |
| `combo_ms` | float | No | Combo execution time |
| `rows_match` | bool | No | Whether row counts match baseline |
| `vulnerability_types` | string[] | No | Classification of the vulnerability |
| `n_plan_changers` | int | No | Query-level: how many flag combos changed the plan |
| `n_distinct_plans` | int | No | Query-level: how many distinct plans observed |

### Merge Key

`(query_id, frozenset(flags.items()))` — used for deduplication when merging explore + scan results.

---

## Artifact 3: ScannerFinding (LLM-Extracted)

Claims about engine behavior extracted from ScannerObservations by an LLM. Evidence-backed, human-reviewed before downstream use.

**Format:** JSON
**Producer:** LLM extraction from ScannerObservation batches
**Consumer:** Engine profile updates (manual), prompt injection (scanner knowledge pipeline)
**Ground truth:** `scanner_knowledge/schemas.py:ScannerFinding`

### Schema

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | `SF-NNN` (e.g., `SF-001`) |
| `claim` | string | Yes | One falsifiable sentence about engine behavior |
| `category` | string | Yes | Scanner Finding Category enum |
| `supporting_queries` | string[] | Yes | Query IDs that support this finding |
| `evidence_summary` | string | Yes | Summary of supporting evidence |
| `evidence_count` | int | Yes | Number of supporting observations |
| `contradicting_count` | int | Yes | Number of contradicting observations |
| `boundaries` | string[] | Yes | Boundary conditions |
| `mechanism` | string | Yes | WHY the optimizer behaves this way |
| `confidence` | string | Yes | `high`, `medium`, or `low` |
| `confidence_rationale` | string | Yes | Explanation for confidence level |
| `implication` | string | Yes | What workers should do based on this finding |

### Extended Fields (review schema)

These fields from the review schema are optional and track provenance:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `engine_specific.engine` | string | No | `duckdb` or `postgresql` |
| `engine_specific.version_tested` | string | No | Engine version |
| `engine_specific.set_local_relevant` | bool | No | Whether finding relates to SET LOCAL |
| `engine_specific.relevant_configs` | string[] | No | Related SET LOCAL configs |
| `metadata.extracted_at` | datetime | No | When extracted |
| `metadata.extraction_model` | string | No | LLM model used |
| `metadata.reviewed` | bool | No | Human-reviewed flag |

---

## Artifact 4: EngineProfile (Human-Authored — THE main product)

The central knowledge artifact. Curated intelligence about a specific database engine's optimizer strengths and weaknesses, used to guide LLM workers.

**Format:** JSON
**Storage:** `qt_sql/constraints/engine_profile_{dialect}.json`
**Producer:** Human analyst (informed by analysis sessions and findings)
**Consumer:** Phase 2 knowledge retrieval, Phase 3 prompt generation (analyst briefing, worker prompts)
**Ground truth:** `constraints/engine_profile_duckdb.json`, `constraints/engine_profile_postgresql.json`

### Top-Level Schema

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `dialect` | string | Yes | `duckdb` or `postgresql` |
| `version_tested` | string | Yes | Engine version(s) validated against |
| `profile_type` | string | Yes | `"engine_profile"` (discriminator) |
| `briefing_note` | string | Yes | 1-2 sentences of high-level guidance |
| `strengths` | Strength[] | Yes | Optimizer capabilities — do NOT fight these |
| `gaps` | Gap[] | Yes | Optimizer weaknesses — opportunities for exploitation |
| `set_local_config_intel` | TuningIntel | PG only | Runtime tuning intelligence |
| `scale_sensitivity_warning` | string | No | Warnings about scale-dependent behavior |

**Note:** Current files use `engine` instead of `dialect`. Migration to `dialect` will be done incrementally — readers must accept both during transition.

### Strength

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | `SCREAMING_SNAKE_CASE`, unique within profile |
| `summary` | string | Yes | 1 sentence — what the optimizer does well |
| `field_note` | string | Yes | Tactical guidance with evidence |

### Gap (absorbs OptimizationPattern + KnowledgePrinciple)

The gap structure is the heart of the Knowledge Engine. It absorbs what was previously split across three artifacts (gap, pattern, principle) into a single, self-contained evidence block.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | `SCREAMING_SNAKE_CASE`, unique within profile |
| `priority` | string | Yes | `HIGH`, `MEDIUM`, or `LOW` |
| `what` | string | Yes | 1 sentence — what the optimizer fails to do |
| `why` | string | Yes | 1 sentence — internal mechanism explanation |
| `opportunity` | string | Yes | 1 sentence — what workers should try |
| `what_worked` | string[] | Yes | Winning evidence: `"Q88: 6.28x — technique description"` |
| `what_didnt_work` | string[] | Yes | Losing evidence (may be empty `[]`) |
| `field_notes` | string[] | Yes | Tactical rules, caveats, EXPLAIN signals |

**Evidence format:** Each entry in `what_worked` / `what_didnt_work` is a free-text string with the pattern `"Q{id}: {speedup}x — {technique}"`. This is intentionally unstructured — human readability in prompts is paramount.

**Absorbed from OptimizationPattern:**
- `stats` (n_observations, n_wins, success_rate, avg_speedup) → computed dynamically from `what_worked`/`what_didnt_work` counts when needed, not stored
- `applicability` (query_archetypes, required_features) → folded into `field_notes` as prose
- `counter_indications` → folded into `what_didnt_work` + `field_notes`

**Absorbed from KnowledgePrinciple:**
- principle text → folded into `opportunity` (the "hunt" instruction)
- evidence → folded into `what_worked`/`what_didnt_work`

### Target-State Gap (future)

When evidence grows, gaps may be expanded to structured evidence. This is the target state — not required for current profiles:

```json
{
  "id": "CROSS_CTE_PREDICATE_BLINDNESS",
  "priority": "HIGH",
  "what": "Cannot push predicates from outer query into CTE definitions",
  "why": "CTEs planned as independent subplans, no data lineage tracing",
  "opportunity": "Move selective predicates INTO the CTE. Pre-filter dims/facts before materialization.",

  "evidence": {
    "won": [
      {"query": "Q6", "speedup": 4.00, "technique": "date filter moved into CTE"},
      {"query": "Q63", "speedup": 3.77, "technique": "pre-joined filtered dates"}
    ],
    "lost": [
      {"query": "Q25", "speedup": 0.50, "technique": "CTE overhead on 31ms baseline"}
    ]
  },

  "stats": {
    "n_observations": 20,
    "n_wins": 7,
    "success_rate": 0.35,
    "avg_speedup": 2.81
  },

  "applicability": {
    "when": "Star-join with late dimension filters; EXPLAIN shows filter AFTER large scan/join",
    "when_not": "Fast queries (<100ms) where CTE overhead negates savings",
    "query_archetypes": ["star_schema_groupby_topn", "multi_dimension_filter"],
    "required_features": ["cte_count >= 1", "where_filters_on_dimension_tables >= 1"]
  },

  "rules": [
    "Check EXPLAIN: filter AFTER large scan/join -> push earlier via CTE",
    "Fast queries (<100ms): CTE overhead can negate savings",
    "~35% of all wins exploit this. Most reliable on star-join + late dim filters",
    "NEVER CROSS JOIN 3+ dim CTEs",
    "Limit cascading fact CTEs to 2 levels",
    "Remove orphaned CTEs — they still materialize"
  ]
}
```

**Migration path:** Current profiles use `what_worked`/`what_didnt_work`/`field_notes`. Target-state adds `evidence`, `stats`, `applicability`, `rules` alongside them. Readers must handle both shapes.

### Tuning Intel (PG only)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `briefing_note` | string | Yes | Overview of tuning approach and results |
| `rules` | TuningRule[] | Yes | Specific tuning rules |
| `key_findings` | string[] | Yes | Summary findings from tuning experiments |

### TuningRule

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | `SCREAMING_SNAKE_CASE` |
| `trigger` | string | Yes | EXPLAIN signal that activates this rule |
| `config` | string | Yes | SET LOCAL statement (or `"Do NOT ..."` for anti-rules) |
| `evidence` | string | Yes | Query + speedup proving this rule |
| `risk` | string | Yes | `LOW`, `MEDIUM`, `MEDIUM-HIGH`, `HIGH`, `CRITICAL` |

### Profile Metadata

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `profile_version` | string | No | `"YYYY.MM.DD-vN"` format |
| `last_validated` | string | No | `"YYYY-MM-DD"` format |
| `analysis_sessions` | string[] | No | Session IDs that informed this version |

### Validation Rules

1. Every gap `id` must be unique `SCREAMING_SNAKE_CASE` within the profile
2. Every strength `id` must be unique `SCREAMING_SNAKE_CASE` within the profile
3. `field_notes` must contain at least 1 diagnostic signal (EXPLAIN) and 1 safety rule per gap
4. `what_worked` and `what_didnt_work` must both be present (empty `[]` is valid for `what_didnt_work`)
5. PG profiles must have `set_local_config_intel` section
6. `dialect` must be in `{duckdb, postgresql}`

---

## Artifact 5: GoldExample (Human-Promoted)

A proven optimization example used as few-shot context in worker prompts. Promoted from OptimizationOutcome by human analyst during an analysis session.

**Format:** JSON
**Storage:** `qt_sql/examples/{dialect}/{id}.json`
**Producer:** Human analyst (promoted from OptimizationOutcome)
**Consumer:** Phase 2 knowledge retrieval (tag matching), Phase 3 prompt generation
**Ground truth:** `examples/duckdb/date_cte_isolate.json`

### Schema

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `schema_version` | string | No | `"2.0"` when present |
| `id` | string | Yes | Example identifier (e.g., `date_cte_isolate`, `q6_date_cte`) |
| `query_id` | string | Yes | Source query (e.g., `q6`) |
| `dialect` | string | Yes | `duckdb` or `postgresql` |
| `original_sql` | string | Yes | Original query SQL |
| `optimized_sql` | string | Yes | Optimized query SQL |
| `status` | string | Yes | `active`, `deprecated`, `superseded` |

### Explanation (4-part — required for new examples)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `explanation.what` | string | Yes | What transforms were applied |
| `explanation.why` | string | Yes | Why this improves performance (mechanism) |
| `explanation.when` | string | Yes | Conditions for application + diagnostic signal |
| `explanation.when_not` | string | Yes | Counter-indications with specific query + regression evidence |

**Rule:** `when_not` must reference at least one specific query and regression factor.

### Classification

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `classification.tags` | string[] | No | Tags for filtering and matching |
| `classification.archetype` | string | No | Query archetype (e.g., `star_schema_groupby_topn`) |
| `classification.transforms` | string[] | No | Transforms demonstrated (from whitelist) |
| `classification.complexity` | string | No | `simple`, `moderate`, or `complex` |

### Gap Linkage

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `demonstrates_gaps` | string[] | No | Engine profile gap IDs this example exploits |

**Rule:** Every ID in `demonstrates_gaps[]` must reference a valid gap `id` in the corresponding engine profile.

### Outcome

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `outcome.speedup` | float | Yes | Measured speedup ratio (> 1.0) |
| `outcome.original_ms` | float | No | Original execution time |
| `outcome.optimized_ms` | float | No | Optimized execution time |
| `outcome.validated_at_sf` | int | No | Scale factor where validated |
| `outcome.validation_confidence` | string | No | Validation confidence enum |
| `outcome.rows_match` | bool | No | Whether row counts match |
| `outcome.checksum_match` | bool/null | No | Whether checksums match (null if not checked) |

### Provenance

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `provenance.source_run` | string | No | Run that produced this example |
| `provenance.worker_id` | int | No | Worker that produced this example |
| `provenance.model` | string | No | LLM model used |
| `provenance.promoted_at` | datetime | No | When promoted to gold |
| `provenance.promoted_by` | string | Yes (new) | `"human"` only — no auto-promotion |
| `provenance.analysis_session` | string | No | `AS-{ENGINE_SHORT}-{NNN}` that promoted this |

### Legacy Fields

Current examples also contain:

| Field | Description | Status |
|-------|-------------|--------|
| `name` | Human-readable name | Legacy, kept for readability |
| `description` | One-line description | Legacy, kept for readability |
| `principle` | KnowledgePrinciple text | Legacy — migrate to `explanation.why` |
| `example.opportunity` | Legacy opportunity text | Legacy — migrate to `explanation.what` |
| `example.input_slice` | Legacy input slice | Legacy — replaced by `original_sql` |
| `example.output` | Legacy output structure | Legacy — replaced by `optimized_sql` |
| `example.key_insight` | Legacy key insight | Legacy — migrate to `explanation.why` |
| `example.when_not_to_use` | Legacy when-not | Legacy — migrate to `explanation.when_not` |
| `benchmark_queries` | Queries this example applies to | Legacy, kept |
| `verified_speedup` | String speedup (e.g., `"4.00x"`) | Legacy — migrate to `outcome.speedup` (float) |

**Migration rule:** When editing a gold example for any reason, migrate legacy fields to the new schema. New examples must use the new schema only.

### Validation Rules

1. `explanation` must have all 4 fields (what, why, when, when_not) non-empty
2. `when_not` must reference at least one specific query + regression
3. `demonstrates_gaps[]` must reference valid gap IDs from the engine profile for this dialect
4. `outcome.speedup` must be > 1.0
5. `provenance.promoted_by` must be `"human"`
6. `classification.transforms[]` values must be in the Transform Whitelist
7. `dialect` must match the storage path (`examples/{dialect}/`)

---

## Artifact 6: DetectionRule (Human-Authored)

Machine-readable predicate tree that matches a query's FeatureVector against an engine profile gap. Determines which gaps fire for a given query at runtime.

**Format:** JSON
**Storage:** `constraints/detection_rules/{dialect}/{GAP_ID}.json`
**Producer:** Human analyst
**Consumer:** Phase 2 knowledge retrieval — gap-weighted example scoring
**Reference:** `docs/knowledge_engine/05_DETECTION_AND_MATCHING.md`

### Schema

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | Yes | Same as `gap_id` |
| `gap_id` | string | Yes | Engine profile gap ID this rule detects |
| `priority` | string | No | `HIGH`, `MEDIUM`, or `LOW` |
| `detect.match` | PredicateTree | Yes | ALL conditions must be true for gap to fire |
| `detect.skip` | PredicateTree | No | ANY condition true -> gap does NOT fire (overrides match) |
| `detect.confidence.high_when` | PredicateTree | No | ANY condition true -> confidence = `high` |
| `detect.confidence.low_when` | PredicateTree | No | ANY condition true -> confidence = `low` |

Default confidence is `medium` when neither `high_when` nor `low_when` fires.

### PredicateTree

A recursive structure with three node types:

```
PredicateTree =
  | { "ALL": [PredicateTree, ...] }     # AND — every child must be true
  | { "ANY": [PredicateTree, ...] }     # OR  — at least one child must be true
  | { "feature": str, "op": str, "value": any }  # Leaf — single feature check
```

### Operators

| Op | Valid Types | Example |
|----|-------------|---------|
| `==` | any | `{"feature": "is_star_schema", "op": "==", "value": true}` |
| `!=` | any | `{"feature": "join_style", "op": "!=", "value": "none"}` |
| `>=` | int, float | `{"feature": "dimension_table_count", "op": ">=", "value": 2}` |
| `<=` | int, float | `{"feature": "baseline_ms", "op": "<=", "value": 100}` |
| `>` | int, float | `{"feature": "fact_table_max_scans", "op": ">", "value": 2}` |
| `<` | int, float | `{"feature": "table_count", "op": "<", "value": 3}` |
| `in` | enum | `{"feature": "join_style", "op": "in", "value": ["implicit_comma", "mixed"]}` |

### Example

```json
{
  "id": "COMMA_JOIN_WEAKNESS",
  "gap_id": "COMMA_JOIN_WEAKNESS",
  "priority": "HIGH",
  "detect": {
    "match": {
      "ALL": [
        {"feature": "join_style", "op": "in", "value": ["implicit_comma", "mixed"]},
        {"feature": "dimension_table_count", "op": ">=", "value": 2},
        {"feature": "where_filters_on_dimension_tables", "op": ">=", "value": 1}
      ]
    },
    "skip": {
      "ANY": [
        {"feature": "join_style", "op": "==", "value": "explicit"},
        {"feature": "table_count", "op": "==", "value": 1}
      ]
    },
    "confidence": {
      "high_when": {
        "ANY": [
          {"feature": "dimension_table_count", "op": ">=", "value": 3},
          {"feature": "where_filters_on_dimension_tables", "op": ">=", "value": 2}
        ]
      }
    }
  }
}
```

### Validation Rules

1. `gap_id` must reference a valid gap in the engine profile for this dialect
2. All `feature` values must exist in the FeatureVector vocabulary (Artifact 7)
3. All `op` values must be in `{==, !=, >=, <=, >, <, in}`
4. `match` predicate is required; `skip` and `confidence` are optional
5. Operators must be type-compatible with the feature (no `>=` on bool/enum features)

---

## Artifact 7: FeatureVector (Auto — Computed at Runtime)

Structural features extracted from SQL at query time via sqlglot. Not persisted — computed fresh for each query. Used by DetectionRules to determine which gaps fire.

**Format:** Python dict (in-memory)
**Producer:** Feature extractor (sqlglot-based)
**Consumer:** DetectionRule evaluation, gap-weighted example scoring
**Reference:** `docs/knowledge_engine/05_DETECTION_AND_MATCHING.md`

### SQL-Level Features (always available, ~25 features)

| Feature | Type | Range | Description |
|---------|------|-------|-------------|
| `join_style` | enum | explicit, implicit_comma, mixed, none | How tables are joined |
| `table_count` | int | 1-50 | Total distinct tables |
| `dimension_table_count` | int | 0-20 | Tables joined on PK, small relative to fact |
| `is_star_schema` | bool | — | One large table joined to 2+ smaller tables |
| `fact_table_max_scans` | int | 1-20 | Highest scan count of any single table |
| `tables_with_multiple_scans` | int | 0-10 | Tables scanned more than once |
| `correlated_subquery_count` | int | 0-10 | Correlated subqueries (references outer scope) |
| `correlated_with_aggregate` | int | 0-10 | Correlated subqueries containing aggregate |
| `correlated_exists_count` | int | 0-10 | Correlated EXISTS / NOT EXISTS |
| `scalar_subquery_in_select` | int | 0-10 | Scalar subqueries in SELECT list |
| `where_filters_on_dimension_tables` | int | 0-10 | Filters on dimension tables in WHERE |
| `or_chain_count` | int | 0-10 | Number of OR groups in WHERE |
| `or_branches_max` | int | 0-20 | Maximum branches in any OR chain |
| `or_branches_touch_different_indexes` | bool | — | OR branches reference different tables/indexes |
| `cte_count` | int | 0-20 | CTEs defined |
| `multi_ref_cte_count` | int | 0-10 | CTEs referenced more than once |
| `cte_max_depth` | int | 0-5 | Maximum CTE nesting depth |
| `conditional_aggregate_count` | int | 0-20 | SUM(CASE WHEN ...) etc. |
| `aggregation_type` | enum | none, simple, conditional, nested, multi_stage | Aggregation pattern |
| `has_having` | bool | — | Uses HAVING clause |
| `has_window_functions` | bool | — | Uses window functions |
| `self_join_count` | int | 0-5 | Tables joined to themselves |
| `union_branch_count` | int | 0-10 | UNION / UNION ALL branches |
| `has_lateral` | bool | — | Uses LATERAL join (PG-specific) |
| `estimated_complexity` | enum | simple, moderate, complex | Heuristic classification |

### Runtime Features (require EXPLAIN, PG only, ~8 features)

| Feature | Type | Range | Description |
|---------|------|-------|-------------|
| `has_disk_sort` | bool/null | — | Sort Method: external merge Disk |
| `disk_sort_size_mb` | float/null | 0-10000 | Disk sort spill size in MB |
| `has_large_seqscan` | bool/null | — | Sequential scan on table >1M rows |
| `large_seqscan_tables` | int/null | 0-10 | Count of tables with large seq scans |
| `has_jit` | bool/null | — | JIT compilation enabled |
| `baseline_ms` | float/null | 0-300000 | Baseline execution time |
| `nested_loop_on_dimension_pk` | bool/null | — | Nested loop + index scan for dim PK |
| `parallel_workers_used` | int/null | 0-16 | Parallel workers in plan |

Runtime features are nullable — when EXPLAIN is unavailable (DuckDB, or PG EXPLAIN failure), they are `null`. DetectionRule leaf predicates with `null` actual values evaluate to `false`.

---

## Artifact 8: AnalysisSession (Human — Manual)

A structured record of a human analyst reviewing a batch of OptimizationOutcomes. Produces findings, profile updates, and gold example promotions.

**Format:** Markdown
**Storage:** `data/analysis_sessions/{dialect}/AS-{DIALECT_SHORT}-{NNN}.md`
**Template:** `docs/knowledge_engine/templates/analysis_session.md`
**Producer:** Human analyst
**Consumer:** EngineProfile updates, GoldExample promotions

### Required Sections

| Section | Required | Content |
|---------|----------|---------|
| Header | Yes | Date, analyst, dialect, benchmark, source path, session ID |
| Batch Summary | Yes | Entry counts by status, top speedup, worst regression |
| Findings | Yes (1+) | At least one finding per session |
| Actions | Yes | One action block per finding |
| Session Summary | Yes | Counts of findings/updates/promotions, profile version change |

### Header Fields

| Field | Required | Format |
|-------|----------|--------|
| Date | Yes | `YYYY-MM-DD` |
| Analyst | Yes | Name |
| Dialect | Yes | `duckdb` or `postgresql` |
| Benchmark | Yes | `tpcds` or `dsb` |
| Blackboard source | Yes | Path to outcomes JSONL |
| Entries reviewed | Yes | Integer count |
| Session ID | Yes | `AS-{DIALECT_SHORT}-{NNN}` (e.g., `AS-DUCK-003`) |

### Batch Summary Table

| Metric | Required |
|--------|----------|
| Total entries reviewed | Yes |
| WINs (2x+) | Yes |
| CRITICAL_HITs (5x+) | Yes |
| REGRESSIONs | Yes |
| ERRORs | Yes |
| Top speedup (query, worker, transform) | Yes |
| Worst regression (query, worker, transform) | Yes |

### Finding Structure (within session)

| Field | Required | Format |
|-------|----------|--------|
| ID | Yes | `F-{DIALECT_SHORT}-{NNN}` (e.g., `F-DUCK-042`) |
| Claim | Yes | One falsifiable sentence about optimizer behavior |
| Category | Yes | Finding Category enum |
| Evidence table | Yes | Columns: Query, Worker, Speedup, Transform, Supports/Contradicts, Notes |
| Mechanism | Yes | WHY the optimizer behaves this way (internal behavior, not just "it was faster") |
| Boundary conditions | Yes | Applies when (1+), Does NOT apply when (1+), Diagnostic signal (1+) |
| Confidence | Yes | `high`, `medium`, or `low` with rationale |

### Action Structure (within session)

| Field | Required | Format |
|-------|----------|--------|
| Finding reference | Yes | Which finding this addresses |
| Action type | Yes | `update_gap`, `new_gap`, `new_strength`, `promote_example`, `no_action` |
| Profile text | If update | Exact text to add/modify in engine profile |
| Example reference | If promote | Query, speedup, worker, 4-part explanation draft |

### Session Summary Table

| Item | Required |
|------|----------|
| Findings recorded | Yes |
| Profile gaps updated | Yes |
| Profile gaps proposed | Yes |
| Profile strengths added | Yes |
| Gold examples promoted | Yes |
| Rules added/modified | Yes |
| Profile version before | Yes |
| Profile version after | Yes |
| Key takeaway (1-2 sentences) | Yes |

---

## Artifact 9: Finding (Human — Standalone)

Same structure as a finding within an AnalysisSession, but stored as a standalone markdown file. Used when a finding needs to be tracked independently or across sessions.

**Format:** Markdown
**Storage:** `data/findings/{dialect}/F-{DIALECT_SHORT}-{NNN}.md`
**Template:** `docs/knowledge_engine/templates/finding.md`

### Additional Fields (beyond in-session finding)

| Field | Required | Format |
|-------|----------|--------|
| Session reference | Yes | `AS-{DIALECT_SHORT}-{NNN}` |
| Dialect | Yes | `duckdb` or `postgresql` |
| Version tested | Yes | Engine version |
| Linked profile entry | Yes | Gap ID or `"none — propose new gap"` |
| Proposed action | Yes | `update_gap`, `new_gap`, `new_strength`, `no_action` |
| Implication | Yes | What this means for future optimization attempts |

---

## Cross-Reference Table

| # | Artifact | Format | Auto/Manual | Feeds Into |
|---|----------|--------|-------------|------------|
| 1 | OptimizationOutcome | JSONL | Auto (Phase 7) | AnalysisSession review, learning analytics |
| 2 | ScannerObservation | JSONL | Auto (plan scanner) | ScannerFinding extraction |
| 3 | ScannerFinding | JSON | LLM-extracted | EngineProfile updates, prompt context |
| 4 | EngineProfile | JSON | Human-authored | Phase 2 retrieval, Phase 3 prompts, DetectionRule reference |
| 5 | GoldExample | JSON | Human-promoted | Phase 2 example matching, Phase 3 worker prompts |
| 6 | DetectionRule | JSON | Human-authored | Phase 2 gap detection, example scoring |
| 7 | FeatureVector | Dict | Auto (runtime) | DetectionRule evaluation |
| 8 | AnalysisSession | Markdown | Human | EngineProfile updates, GoldExample promotions |
| 9 | Finding | Markdown | Human | EngineProfile updates |

### Referential Integrity

These cross-references must be valid:

| From | Field | To | Field |
|------|-------|----|-------|
| GoldExample | `demonstrates_gaps[]` | EngineProfile | `gaps[].id` |
| DetectionRule | `gap_id` | EngineProfile | `gaps[].id` |
| DetectionRule | `detect.*.feature` | FeatureVector | vocabulary key |
| OptimizationOutcome | `transforms.all[]` | Transform Whitelist | — |
| GoldExample | `classification.transforms[]` | Transform Whitelist | — |
| GoldExample | `provenance.analysis_session` | AnalysisSession | session ID |
| Finding (standalone) | `session reference` | AnalysisSession | session ID |
| Finding (standalone) | `linked profile entry` | EngineProfile | `gaps[].id` |
| OptimizationOutcome | `config.set_local` keys | `PG_TUNABLE_PARAMS` whitelist | — |

---

## Backward Compatibility

1. **Additive only** — new fields may be added with defaults; existing fields must not be removed or renamed without a migration.
2. **`engine` → `dialect` migration** — readers must accept both `engine` and `dialect` during transition. Writers should emit `dialect`.
3. **Legacy GoldExample fields** — `example.*`, `principle`, `verified_speedup` remain readable but are not required. Migrate when editing.
4. **KnowledgePrinciple purge** — as principles are encountered in code, fold their content into the relevant EngineProfile gap and delete the principle. No legacy compat layer.
5. **OptimizationPattern purge** — same as principles. Fold stats/applicability/counter_indications into the relevant gap and delete the pattern.
6. **Transform whitelist extension** — new values may be added. Consumers must treat unknown transforms as valid (log, don't reject).

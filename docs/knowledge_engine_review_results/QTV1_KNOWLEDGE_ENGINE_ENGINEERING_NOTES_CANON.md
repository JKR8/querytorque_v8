# QTV1 Knowledge Engine — Engineering Notes (CANON)

> **Compiled from systematic review of 18 documents (~350KB)**
> Review Date: 2026-02-11 | Status: CANONICAL — no open questions

---

## 1. System Summary

QueryTorque V1 (QTV1) is a two-system architecture:

**Product Pipeline** — a linear, stateless, 7-phase per-query optimizer that takes SQL in and produces optimized SQL out. It exists today in working form.

**Knowledge Engine** — a circular, stateful learning system that accumulates optimization knowledge across runs and feeds it back to the pipeline. It exists only as design documents and partially-implemented batch scripts.

The two systems connect through exactly two interfaces:
- **Interface A (READ)**: Phase 2 of the pipeline pulls knowledge from Layer 4 of the KE
- **Interface B (WRITE)**: Phase 7 of the pipeline pushes outcomes into Layer 1 of the KE

Everything else — the four-layer compression, pattern mining, promotion — happens asynchronously inside the KE, after validation completes.

---

## 2. Product Pipeline: The Existing System

### 2.1 Seven Phases

| Phase | Name | Key Components | Notes |
|-------|------|----------------|-------|
| 1 | Context Gathering | dag.py, plan_analyzer.py, pg_tuning.py, plan_scanner.py | Produces context_confidence: high / degraded / heuristic |
| 2 | Knowledge Retrieval | KE Interface A (query) | Single path: all knowledge served through KE including scanner findings |
| 3 | Prompt Generation | analyst_briefing.py, worker.py | Token-budgeted; uses gather_analyst_context() handoff contract |
| 4 | LLM Inference | CandidateGenerator, 5 parallel workers | Workers 1-3 individual examples, W4 combined, W5 explore |
| 5 | Response Processing | SQLRewriter, DAP/JSON parsing, AST validation | sqlglot-based structural validation |
| 6 | Validation & Benchmarking | Per-engine equivalence, 5-run trimmed mean | PG = row_count_only; DuckDB = checksum capable |
| 7 | Outputs & Learning | Store, Learner, KE Interface B (ingest) | Fire-and-forget to KE after validation |

### 2.2 Critical Contracts

**Intelligence Gates** (Phase 2 → 3 handoff):
- Hard fail on missing: `global_knowledge`, `matched_examples`, `plan_scanner_text` (PG only)
- Context confidence signal calibrates gate strictness
- Bootstrap override: `QT_ALLOW_INTELLIGENCE_BOOTSTRAP=1` (debug only, never for SOTA claims)

**Speedup Classification** (Phase 6):
- `measured` — both original and optimized completed within timeout
- `vs_timeout_ceiling` — original timed out, optimized completed
- `both_timeout` — both timed out
- These three types must NEVER be mixed in aggregate statistics

**Validation Confidence** (Phase 6):
- `high` — full checksum match (DuckDB)
- `row_count_only` — row counts match, no checksum (PG)
- `zero_row_unverified` — zero-row result, cannot verify
- PG compensating control: DuckDB SF100 post-hoc verification

### 2.3 Known Limitations in Existing System

1. PG equivalence checking is row-count only (no formal SQL equivalence prover for CTEs + window functions)
2. Cost-rank pre-screening works on DuckDB only (EXPLAIN cost proved useless for ranking in lab testing)
3. No automated gold example promotion — manual curation
4. Engine profiles are hand-written, not auto-derived from accumulated findings

---

## 3. Knowledge Engine: The Proposed System

### 3.1 Four-Layer Architecture

```
Layer 1: OUTCOME STORE (Raw)
    Format: JSONL, append-only, partitioned by date
    Content: Every optimization outcome from Pipeline Phase 7
             + scanner observations (via adapter)
             + config-only speedups (SET LOCAL / pragma results)
    Compression: Temporal rollup (7d individual → daily → weekly → archive)
    Compression timing: AFTER validation completes, never during a run

Layer 2: FINDINGS (Extracted)
    Format: JSON
    Content: DeepSeek R1-extracted insights from L1 (50:1 compression)
    Sources: 4W worker outcomes + scanner findings (PG only, via adapter)
    Trigger: 50+ new entries OR 24 hours since last extraction
    Batch size: ~400 JSONL entries per extraction batch

Layer 3: PATTERNS (Distilled)
    Format: JSON
    Content: Cross-query aggregated patterns from L2 (7:1 compression)
    Requirement: Pattern must apply to 3+ distinct queries
    Trigger: 10+ new findings

Layer 4: KNOWLEDGE STORE (Curated)
    Format: JSON, human-editable, version-controlled
    Content: Engine Profiles, Gold Examples, Constraints, Classification Taxonomy
    Promotion: 5+ wins AND 70%+ success rate → auto-promote
    Quality gate: No engine profile entry exists without gold example evidence
    This is what Interface A serves to the pipeline
```

Total compression ratio: ~1000:1 (1000 raw outcomes → 1 curated knowledge item).

### 3.2 Interface Contracts

**Interface A: `KnowledgeEngine.query()`**

```python
# Request
KnowledgeQuery {
    query_id: str           # e.g., "q88"
    sql_fingerprint: str    # structural hash
    dialect: str            # "duckdb" | "postgresql"
    available_context: dict # logical_tree, explain_plan
    context_confidence: str # "high" | "degraded" | "heuristic"
}

# Response — single path, includes scanner findings
KnowledgeResponse {
    matched_examples: List[GoldExample]   # REQUIRED (gate fails without)
    engine_profile: EngineProfile         # REQUIRED (gate fails without)
    constraints: List[Constraint]
    scanner_findings: ScannerFindings     # PG only, null otherwise — served through KE
    knowledge_version: str                # e.g., "2026.02.11-v3"
}
```

Contract: Must respond in <500ms (all data pre-computed/cached). Missing required fields → intelligence gate failure. Scanner findings are served through the KE — there is no separate direct path from scanner to Phase 2.

**Interface B: `KnowledgeEngine.ingest()`**

```python
# Payload
OptimizationOutcome {
    query_id, run_id, timestamp, status, speedup, speedup_type,
    validation_confidence, transforms_applied, original_sql, optimized_sql,
    worker_responses, error_category, error_messages, model, provider, git_sha,
    knowledge_version_used   # Records which L4 version produced this outcome
}
```

Contract: Fire-and-forget, async processing. Pipeline MUST report ALL outcomes (wins AND failures). KE handles deduplication internally. Compression never runs during a pipeline run — log everything first, compress after validation completes. The `knowledge_version_used` field provides built-in correlation between outcomes and the knowledge that produced them.

### 3.3 Lifecycle: INGEST → EXTRACT → DISTILL → INJECT

```
Pipeline Phase 7 → Interface B → Layer 1 (append)
    [run completes, validation done]
                                     ↓ (trigger: 50+ entries or 24h)
                                  Layer 2 (DeepSeek R1 extraction, ~400 batch)
                                     ↓ (trigger: 10+ findings)
                                  Layer 3 (DeepSeek R1 pattern mining, cross-query)
                                     ↓ (trigger: 5+ wins, 70%+ success, evidence required)
                                  Layer 4 (promoted knowledge with gold example backing)
                                     ↓
Pipeline Phase 2 ← Interface A ← Layer 4 (cached read)
```

### 3.4 Scanner Integration (Single Path Through KE)

Scanner findings are a class of optimization outcome. They flow exclusively through the KE — there is no separate direct path from scanner to Phase 2 prompts.

```
Phase 1: plan_scanner.py explores SET LOCAL plan space
    ↓
scanner_blackboard.jsonl (ScannerObservation records)
    ↓
scanner_knowledge/findings.py (two-pass DeepSeek R1: reasoner + chat)
    ↓
scanner_findings.json (ScannerFinding records)
    ↓
[adapter] ScannerFinding → OptimizationOutcome (source.type = "plan_scanner")
    ↓
KE Layer 1 (merged with 4W outcomes and config-only speedups)
    ↓ (normal compression pipeline)
KE Layer 4 (engine profile, served via Interface A)
    ↓
Pipeline Phase 2 receives scanner findings through KnowledgeResponse
```

Config-only speedups (SET LOCAL / pragma results that improve performance without SQL rewriting) are a first-class speedup class. They feed into the engine profile's `tuning_intel` section through the same KE compression pipeline.

### 3.5 Findings and the Engine Profile Reasoning Step

Findings at Layer 2 are collections of observations. They can appear contradictory — for example, "decorrelation helps on star-schema queries" alongside "decorrelation hurts on small subqueries." These are not conflicts. They are observations made under different conditions.

The critical step is the DeepSeek R1 reasoning that weighs findings against their metadata when constructing or updating the engine profile. The reasoning model:

1. Knows the current engine profile
2. Receives new findings with full condition metadata (query archetypes, table sizes, join patterns, scale factors)
3. Identifies the conditions that distinguish apparently contradictory observations
4. Updates the engine profile with properly scoped rules — e.g., a gap entry that says "decorrelation opportunity on star-schema with >3 dimension joins" alongside a counter-indication that says "do not decorrelate small correlated subqueries with <1000 row estimates"

The quality of the engine profile depends entirely on the conditions metadata captured in Layer 1 outcomes and preserved through Layer 2 findings. Without conditions, findings are ambiguous. With conditions, the reasoning model can scope every rule precisely.

### 3.6 Pattern Transfer: Same Engine, Across Benchmarks

Pattern transfer is within the same database engine across benchmarks (TPC-DS → TPC-H → production), not across engines. A pattern learned on DuckDB TPC-DS may apply to DuckDB TPC-H and DuckDB production workloads. There is no cross-engine transfer (DuckDB patterns do not transfer to PostgreSQL or vice versa).

Engine profiles aggregate knowledge cross-benchmark for a single engine. The storage model reflects this:

```
Blackboards (per engine + benchmark):
    blackboard/duckdb_tpcds.jsonl
    blackboard/duckdb_tpch.jsonl
    blackboard/postgresql_tpcds.jsonl
    blackboard/postgresql_dsb.jsonl

Engine profiles (per engine, cross-benchmark):
    engine_profiles/duckdb.json      ← derived from duckdb_tpcds + duckdb_tpch
    engine_profiles/postgresql.json  ← derived from postgresql_tpcds + postgresql_dsb
```

---

## 4. Schema Inventory

Five JSON schemas define the data model (all schema_version: "2.0"):

### 4.1 optimization_outcome.schema.json (Layer 1)
- **Required**: schema_version, base (query_id, dialect, timestamp, run_id), opt (worker_id, strategy, iteration), outcome (status, speedup, validation_confidence)
- **Optional**: transforms (primary, all[]), principles (what_worked, why_it_worked, principle_id), config (set_local, plan_flags — PG only), error (category, messages), reasons (reasoning_chain, evidence), provenance (model, provider, git_sha, reviewed), knowledge_version_used
- **Source discriminator**: type = "4w_worker" | "plan_scanner" | "expert_session"
- **Fix required**: File has formatting corruption at lines 1-22 (duplicate/misplaced JSON fragments) — fix before building validation tooling

### 4.2 scanner_finding.schema.json (Layer 2 — PG only)
- **Required**: id (SF-NNN), claim, category, supporting_queries, evidence (summary, count), confidence, implication
- **Categories**: join_sensitivity, memory, parallelism, jit, cost_model, join_order, scan_method, interaction, config_sensitivity
- **Engine-specific**: set_local_relevant flag, relevant_configs array

### 4.3 optimization_pattern.schema.json (Layer 3)
- **Required**: id (PATTERN-XXX-NNN), name, classification (mechanism, impact_tier, risk, exploit_type), technique (description), stats (n_observations, n_wins, success_rate), status
- **Classification mechanisms**: predicate_pushdown, join_reorder, scan_consolidation, decorrelation, materialization, set_operation_rewrite, aggregation_rewrite, subquery_flattening
- **Status lifecycle**: candidate → promoted → deprecated
- **Counter-indications**: Array of {pattern, reason, observed_regression, example_queries}

### 4.4 engine_profile.schema.json (Layer 4)
- **Required**: engine, version_tested, strengths[], gaps[]
- **Strengths**: id, summary, field_note, source_patterns — "what NOT to fight"
- **Gaps**: id, priority (CRITICAL/HIGH/MEDIUM/LOW), what, why, opportunity, what_worked[], what_didnt_work[], field_notes[], source_patterns[], source_findings[]
- **Quality gate**: Every gap and strength must have source_patterns or source_findings linking back to evidence. No entry without gold example or finding evidence.
- **Tuning intel**: available flag, mechanism (set_local/pragma/config/null), rules[] with trigger/config/evidence/risk
- **Freshness**: Engine profile rules are periodically retested, and retested on DB version updates. This is the freshness mechanism — not a computed score.
- **Metadata**: version, source_runs, auto_generated flag, human_reviewed flag

### 4.5 gold_example.schema.json (Layer 4)
- **Required**: id (qN_technique), query_id, dialect, original_sql, optimized_sql, speedup, status
- **Classification**: tags[], archetype, transforms[], complexity (simple/moderate/complex)
- **Explanation**: what, why, when, when_not — four-part pedagogical structure
- **Outcome**: speedup, original_ms, optimized_ms, validated_at_sf, validation_confidence, rows_match, checksum_match
- **Provenance**: source_run, worker_id, model, promoted_at, promoted_by (auto/human), reviewed_by
- **Status lifecycle**: active → deprecated | superseded (with superseded_by reference)
- **Usage tracking**: usage_count, last_used

---

## 5. Gap Analysis: What Exists vs. What's Needed

### 5.1 What Exists Today (Needs Migration)

| Component | Current Location | Status |
|-----------|-----------------|--------|
| Blackboard entries | `build_blackboard.py` (batch CLI) | Manual execution, hardcoded TRANSFORM_PRINCIPLES |
| Scanner blackboard | `scanner_knowledge/blackboard.py` | Separate schema, PG only |
| Scanner findings | `scanner_knowledge/findings.py` | Two-pass LLM extraction (reasoner + chat), PG only |
| Engine profiles | `constraints/engine_profile_*.json` | Hand-written, not auto-derived |
| Gold examples | Knowledge base files | Manual curation via TagRecommender |
| Tag index | `tag_index.py` | Basic tags for matching |

### 5.2 Critical Gaps (P0-P1)

**Gap 1: No Unified Blackboard Schema**
- Two separate systems: `build_blackboard.py` (BlackboardEntry) vs `scanner_knowledge/` (ScannerObservation)
- Different schemas, different storage, no merge capability
- Fix: Unified schema (optimization_outcome.schema.json) with source discriminator
- Effort: 2 days

**Gap 2: Findings → Engine Profile Bridge Missing**
- scanner_findings.json exists but is NOT automatically fed to engine profiles
- Engine profiles are hand-written, becoming stale
- Fix: `findings_to_profile.py` bridge with DeepSeek R1 reasoning step
- Effort: 1 day

**Gap 3: No Automated Classification**
- tag_index.py has basic tags, build_blackboard.py has hardcoded TRANSFORM_PRINCIPLES
- No systematic mechanism → impact_tier → pattern → risk classification
- Fix: OptimizationClassification taxonomy + auto-classify
- Effort: 3 days

**Gap 4: DuckDB Profile Schema Mismatch**
- PG profile has set_local_config_intel section
- DuckDB has no SET LOCAL equivalent
- Fix: Unified profile schema with engine-specific tuning_intel (available: boolean, mechanism: set_local | pragma | config | null)
- Effort: 1 day

**Gap 5: Feedback Loop Not Closed**
- Current broken flow: 4W Run → Blackboard → Findings → (stuck, no bridge to profiles)
- Fix: knowledge_pipeline.py orchestrator + compression triggers, runs after validation
- Effort: 3 days

### 5.3 Implementation Priority

| Priority | Component | Effort | Dependency |
|----------|-----------|--------|------------|
| P0 | Unified blackboard schema + Layer 1 storage | 2 days | None |
| P0 | Fix optimization_outcome.schema.json formatting | 0.5 day | None |
| P0 | DuckDB profile tuning_intel section | 1 day | None |
| P1 | Interface A & B (`knowledge_engine/api.py`) | 2 days | P0 schema |
| P1 | Findings → Profile bridge (DeepSeek R1 reasoning) | 1 day | P0 schema |
| P1 | JSON Schema validation for LLM outputs | 2 days | P0 schema |
| P2 | Layer 2 extraction (DeepSeek R1, ~400 batch) | 2 days | P1 API |
| P2 | Layer 3 pattern mining (DeepSeek R1) | 3 days | P2 extraction |
| P2 | Auto-classification taxonomy | 3 days | P1 bridge |
| P2 | Engine-specific finding templates | 1 day | P1 bridge |
| P3 | Layer 4 promotion pipeline | 2 days | P2 mining |
| P3 | Example similarity matching | 2 days | P3 promotion |
| P3 | Full compression pipeline orchestrator | 2 days | P3 promotion |

---

## 6. Three Automation Loops

The KE design requires three automation loops to close the learning cycle. None currently operate automatically.

### 6.1 Loop 1: Ingest (Pipeline → KE Layer 1)

**Current state**: `build_blackboard.py` is a batch CLI tool run manually after optimization runs. Scanner observations go to a separate `scanner_blackboard.jsonl`.

**Required state**: Every Pipeline Phase 7 outcome automatically written to Layer 1 via Interface B. Scanner findings adapted and merged into the same store. Config-only speedups captured as a first-class outcome type.

**Implementation path**:
1. Add `KnowledgeEngine.ingest()` call to `store.py:save_candidate()`
2. Create scanner adapter: ScannerObservation → OptimizationOutcome (source.type = "plan_scanner")
3. Single JSONL storage partitioned by date: `layer1/{engine}_{benchmark}/{date}/outcomes.jsonl`
4. Fire-and-forget semantics — pipeline must not block on KE write
5. Record `knowledge_version_used` in every outcome for traceability

**Migration**: Side-by-side first (write to both old and new), then cutover after validation.

### 6.2 Loop 2: Derivation (KE Layer 1-3 → Layer 4)

**Current state**: Engine profiles are hand-written. Gold examples manually curated. No automated extraction, mining, or promotion.

**Required state**: Background processing that runs AFTER validation completes (never during a run):
- L1 → L2: DeepSeek R1 extraction of findings from accumulated outcomes (triggered at 50+ entries or 24h, ~400 entry batches)
- L2 → L3: Cross-query pattern aggregation (triggered at 10+ findings, must span 3+ queries)
- L3 → L4: Promotion to engine profiles and gold examples (triggered at 5+ wins, 70%+ success rate)

**Quality guarantee**: No engine profile entry exists without evidence. Every gap, strength, and tuning rule must link back to gold examples or findings that demonstrate the claim. An engine profile entry without supporting gold examples is not valid.

**Implementation path**:
1. `layer2/extraction.py`: Port existing scanner_knowledge/findings.py extraction, generalize for 4W outcomes, use DeepSeek R1
2. `layer3/miner.py`: New — aggregate findings by mechanism, validate across queries, compute stats, DeepSeek R1
3. `layer4/promotion.py`: New — DeepSeek R1 reasoning step that knows the current engine profile, weighs new findings with condition metadata, updates profile with properly scoped rules
4. `compression/pipeline.py`: Orchestrator checking triggers and running stages, gated to run only after validation
5. `compression/triggers.py`: Configurable trigger logic (entry count, time elapsed, finding count)

**Critical detail**: The findings → engine profile reasoning step is where apparent contradictions in findings are resolved. Findings are observations under specific conditions. The reasoning model examines the condition metadata (query archetypes, table sizes, join patterns, scale factors) to scope each profile rule precisely. This is the highest-value step in the entire KE.

### 6.3 Loop 3: Retrieval (KE Layer 4 → Pipeline Phase 2)

**Current state**: `_find_examples()` directly loads files via TagRecommender, hardcoded file paths for engine profiles and constraints. Scanner findings loaded separately.

**Required state**: Pipeline queries KE via Interface A, receives pre-computed KnowledgeResponse with matched examples, engine profile, constraints, and scanner findings — all through a single path. Response in <500ms from cache.

**Implementation path**:
1. `knowledge_engine/api.py`: Implement `KnowledgeEngine.query()` reading from Layer 4 store
2. `layer4/similarity.py`: Example matching using SQL fingerprints + tag overlap (port TagRecommender logic)
3. Add caching layer: TTL-based (default 5 min), refreshed on Layer 4 updates
4. Wrap `knowledge.py:TagRecommender` to delegate to KE (backward compatibility during migration)

**Rollback**: Feature flag `KNOWLEDGE_ENGINE_ENABLED` with auto-disable on exception + fallback to legacy file loading.

---

## 7. Data Flow Diagrams

### 7.1 Current State (Batch/Manual)

```
Pipeline Run
    ↓
store.save_candidate()  →  artifacts on disk
    ↓
[manual] python build_blackboard.py  →  blackboard.jsonl
    ↓
[manual] python scanner_knowledge/build_all.py  →  scanner_findings.json
    ↓
[manual] human reviews findings, edits engine_profile_*.json
    ↓
[manual] human curates gold examples
    ↓
Next Pipeline Run reads files from disk (separate paths for scanner vs knowledge)
```

Problems: Human bottleneck at every derivation step. Profiles go stale. Knowledge doesn't compound. Scanner findings bypass KE entirely.

### 7.2 Target State (Automated)

```
Pipeline Run (Phase 7)
    ↓ Interface B (fire-and-forget, records knowledge_version_used)
KE Layer 1: append ALL outcomes to JSONL (4W + scanner + config-only)
    [run completes, validation done — compression can now proceed]
                                     ↓ (trigger: 50+ entries or 24h)
KE Layer 2: DeepSeek R1 extracts findings (~400 entry batches)
                                     ↓ (trigger: 10+ findings)
KE Layer 3: DeepSeek R1 mines cross-query patterns (3+ query span)
                                     ↓ (trigger: 5+ wins, 70%+ success, evidence required)
KE Layer 4: DeepSeek R1 reasons over findings → updates engine profile + promotes gold examples
                                     ↓ cache invalidation
Pipeline Run (Phase 2)
    ↓ Interface A (<500ms cached read, single path for everything)
KnowledgeResponse with evidence-backed, auto-derived knowledge
```

---

## 8. File Structure (Target)

```
qt_sql/
├── knowledge_engine/              # NEW MODULE
│   ├── __init__.py
│   ├── api.py                     # Interface A (query) & B (ingest)
│   ├── config.py                  # Feature flags, trigger thresholds
│   │
│   ├── layer1/                    # Outcome Store (Raw)
│   │   ├── __init__.py
│   │   ├── store.py               # JSONL append, partitioned reads
│   │   ├── schema.py              # OptimizationOutcome dataclass
│   │   ├── rollup.py              # Temporal compression (7d → daily → weekly)
│   │   └── scanner_adapter.py     # ScannerObservation → OptimizationOutcome
│   │
│   ├── layer2/                    # Findings (Extracted)
│   │   ├── __init__.py
│   │   ├── extraction.py          # DeepSeek R1 extraction from L1 outcomes
│   │   ├── schema.py              # OptimizationFinding, ScannerFinding dataclasses
│   │   └── scanner/               # PG scanner findings (ported)
│   │       ├── __init__.py
│   │       ├── blackboard.py
│   │       └── findings.py
│   │
│   ├── layer3/                    # Patterns (Distilled)
│   │   ├── __init__.py
│   │   ├── miner.py               # DeepSeek R1 cross-query pattern aggregation
│   │   ├── schema.py              # OptimizationPattern dataclass
│   │   └── validation.py          # Pattern validation (3+ queries, stats)
│   │
│   ├── layer4/                    # Knowledge Store (Curated)
│   │   ├── __init__.py
│   │   ├── store.py               # Knowledge retrieval + caching
│   │   ├── schema.py              # EngineProfile, GoldExample dataclasses
│   │   ├── promotion.py           # DeepSeek R1 reasoning: findings → profile updates
│   │   └── similarity.py          # Example matching (fingerprint + tags)
│   │
│   └── compression/               # Compression pipeline
│       ├── __init__.py
│       ├── triggers.py            # Configurable trigger logic
│       └── pipeline.py            # Orchestrator (runs AFTER validation only)
│
├── specs/                         # JSON Schemas (validation)
│   ├── optimization_outcome.schema.json   # L1 (fix formatting first)
│   ├── scanner_finding.schema.json        # L2 (PG)
│   ├── optimization_pattern.schema.json   # L3
│   ├── engine_profile.schema.json         # L4
│   └── gold_example.schema.json           # L4
│
└── data/                          # Runtime data (gitignored except profiles)
    ├── layer1/{engine}_{benchmark}/{date}/outcomes.jsonl
    ├── layer2/findings/{engine}/{category}/
    ├── layer3/patterns/{engine}/{mechanism}/
    └── layer4/
        ├── engine_profiles/{engine}.json   # Version controlled
        └── gold_examples/{engine}/{id}.json # Version controlled
```

---

## 9. Migration Strategy

### Phase 1: Side-by-Side (Week 1)

- Implement `knowledge_engine/api.py` with `query()` and `ingest()`
- Layer 1 store reads/writes JSONL
- On Interface A: Try KE first, fall back to legacy on failure, log consistency delta
- On Interface B: Write to both KE and legacy systems
- Feature flag: `KNOWLEDGE_ENGINE_ENABLED=true`

### Phase 2: Cutover (Week 2)

- KE becomes primary for reads and writes
- Legacy systems kept as backup only
- Monitor: response latency (<500ms), gate pass rates, example match quality

### Phase 3: Cleanup (Week 3)

- Remove legacy `_find_examples()` file-loading code
- Remove dual-write to old blackboard
- Remove separate scanner → Phase 2 direct path
- Delete `_legacy_*` methods
- Archive old build_blackboard.py (keep for reference)

### Rollback

```python
# Auto-disable on exception
if self.use_knowledge_engine:
    try:
        return self.knowledge_engine.query(...)
    except Exception as e:
        logger.error(f"KE failed: {e}, auto-disabling")
        self.use_knowledge_engine = False
return self._legacy_find_examples(sql, dialect)
```

---

## 10. Testing Strategy

### Unit Tests

- Interface A: Query with known fingerprint → returns expected examples and profile
- Interface B: Ingest outcome → verify written to Layer 1 JSONL, verify knowledge_version_used recorded
- Trigger logic: Verify compression stages fire at correct thresholds and ONLY after validation
- Schema validation: All JSON outputs validate against their schemas
- Scanner adapter: ScannerObservation → OptimizationOutcome round-trip
- Evidence gate: Attempt to promote pattern without gold example → rejected

### Integration Tests

- Pipeline + KE mock: Verify Phase 2 calls `query()`, Phase 7 calls `ingest()`
- Compression pipeline: L1 → L2 → L3 → L4 end-to-end with synthetic data
- Cache invalidation: L4 update → next `query()` returns fresh data
- Single path: Scanner findings arrive through KnowledgeResponse, not separate path

### Regression Tests

- Side-by-side validation: KE response matches legacy response for all benchmark queries
- Intelligence gate: KE missing data → gate fails → pipeline raises IntelligenceGateError
- Speedup type preservation: Types flow correctly through L1 → L2 → L3 → L4
- Evidence integrity: Every L4 engine profile entry traces back to supporting findings/examples

### Validation Constraints

- Gold example promotion must preserve validation_confidence and speedup_type labels
- Engine profile auto-generation must not overwrite human-reviewed content
- Pattern stats must be recomputed, not accumulated (prevents drift)
- No compression runs during active pipeline execution

---

## 11. Configuration Reference

```yaml
knowledge_engine:
  enabled: true
  store_path: "data/"
  cache_ttl_seconds: 300

  models:
    extraction: "deepseek-r1"          # L1→L2 findings extraction
    mining: "deepseek-r1"              # L2→L3 pattern mining
    promotion: "deepseek-r1"           # L3→L4 engine profile reasoning
    workers: "deepseek-r1"             # Pipeline workers (experimental: deepseek-chat)

  layers:
    layer1:
      format: "jsonl"
      retention_days: 90
      temporal_rollup_days: 7
      batch_size: 400                  # Entries per extraction batch

    layer2:
      extraction_trigger_count: 50
      extraction_trigger_hours: 24

    layer3:
      mining_trigger_count: 10
      min_queries_per_pattern: 3

    layer4:
      promotion_min_wins: 5
      promotion_min_success_rate: 0.70
      auto_promote: true
      evidence_required: true          # No profile entry without gold example backing

  freshness:
    retest_periodically: true          # Retest engine profile rules on schedule
    retest_on_version_update: true     # Retest when DB version changes

  interfaces:
    query_timeout_ms: 500
    ingest_async: true

  safety:
    compress_after_validation_only: true  # NEVER compress during a run
```

---

## 12. LLM Cost Breakdown

All extraction, mining, and promotion use DeepSeek R1. Workers use DeepSeek R1 with experimental option for DeepSeek Chat.

### Per Benchmark Run (~99 TPC-DS queries)

| Phase | Component | Calls | Model | Est. Input Tokens | Est. Output Tokens |
|-------|-----------|-------|-------|-------------------|-------------------|
| 4 | 5 workers × 99 queries | ~495 | DeepSeek R1 | ~2M | ~1M |
| 4 | 5 workers × 99 queries (Chat experiment) | ~495 | DeepSeek Chat | ~2M | ~1M |

### Per KE Compression Cycle (~400 outcomes batch)

| Stage | Component | Calls | Model | Est. Input Tokens | Est. Output Tokens |
|-------|-----------|-------|-------|-------------------|-------------------|
| L1→L2 | Findings extraction | 1-2 | DeepSeek R1 | ~200K | ~20K |
| L2→L3 | Pattern mining | 1 | DeepSeek R1 | ~50K | ~10K |
| L3→L4 | Engine profile reasoning | 1 | DeepSeek R1 | ~30K | ~10K |

### Scanner (PG only, per benchmark)

| Stage | Component | Calls | Model | Est. Input Tokens | Est. Output Tokens |
|-------|-----------|-------|-------|-------------------|-------------------|
| L1 | Scanner observations | ~99 | N/A (DB execution) | — | — |
| L2 | Findings extraction (two-pass) | 2 | DeepSeek R1 | ~100K | ~20K |

### Cost Notes

- KE compression costs are minimal relative to the pipeline worker costs (~3-4 DeepSeek R1 calls per compression cycle vs ~495 per benchmark run)
- The expensive operation is always the worker inference in Phase 4
- KE compression is amortized across runs — it fires when thresholds are met, not on every run
- DeepSeek Chat for workers would reduce the dominant cost if quality holds

---

## 13. Resolved Decisions

All questions from the initial review have been resolved. This section records the decisions for traceability.

| Question | Resolution |
|----------|-----------|
| LLM cost for extraction | DeepSeek R1 for everything. ~400 JSONL batch size. Cost breakdown in Section 12. KE compression is cheap relative to worker inference. |
| Finding contradictions | Findings are observations under different conditions, not conflicts. The DeepSeek R1 reasoning step at L3→L4 examines condition metadata to scope engine profile rules precisely. |
| Cross-engine pattern transfer | Not supported. Pattern transfer is same-engine across benchmarks (TPC-DS → TPC-H → prod). |
| Freshness | Periodic retest of engine profile rules + retest on DB version update. No computed score. |
| Scanner dual-path | Single path through KE. Config-only speedups are a first-class speedup class feeding into engine profile. |
| Schema corruption | Fix task. optimization_outcome.schema.json lines 1-22 need cleanup. |
| Cold start | Not a concern. |
| Compression quality | Quality guaranteed by evidence requirement: no engine profile entry without gold example backing. |
| Race conditions | No compression during a run. Log everything, compress after validation completes. |
| Version drift | `knowledge_version_used` recorded in every outcome. Correlation is built-in. |

---

## 14. Recommended Implementation Order

**Week 1**: Foundation
- Fix optimization_outcome.schema.json formatting
- Implement Layer 1 store (JSONL read/write with partitioning)
- Implement scanner adapter
- Implement Interface B (`ingest()`) with `knowledge_version_used`
- Implement Interface A (`query()`) reading from existing L4 files, single path including scanner
- Feature flag + migration scaffolding

**Week 2**: Extraction + Bridge
- Port scanner_knowledge/ findings extraction to new module structure
- Implement L1 → L2 extraction for 4W outcomes (DeepSeek R1, ~400 batches)
- Implement findings → engine profile bridge (DeepSeek R1 reasoning with conditions)
- Implement compression trigger logic (runs after validation only)

**Week 3**: Pattern Mining
- Implement L2 → L3 pattern aggregation (DeepSeek R1)
- Implement cross-query validation (3+ query span)
- Implement pattern statistics computation
- Implement L3 schema validation

**Week 4**: Promotion + Knowledge Store
- Implement L3 → L4 promotion logic (DeepSeek R1 reasoning)
- Implement evidence gate: no profile entry without gold example backing
- Implement gold example promotion from winning outcomes
- Implement periodic engine profile retest mechanism

**Week 5**: Integration + Testing
- Connect to Pipeline Phase 2 and Phase 7
- Remove separate scanner → Phase 2 direct path
- Side-by-side validation against legacy
- End-to-end regression testing
- Cutover and cleanup

---

*End of canonical engineering notes.*

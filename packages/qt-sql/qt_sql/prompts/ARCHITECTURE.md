# Analyst Prompt Architecture

## Overview

The analyst prompt is a structured briefing that tells the LLM everything it needs
to reason about a SQL query and produce optimization instructions for workers.
Built by `analyst_briefing.py::build_analyst_briefing_prompt()`.

## 7-Section Structure

### §1. ROLE & MISSION
- Framing: analyst as architect, workers as implementers
- Information asymmetry: analyst sees everything, workers see only their briefing
- Mode variants: swarm (4 workers), oneshot (analyze + produce SQL)

### §2. INPUT PACKAGE
- **§2a**: Original SQL with line numbers
- **§2b**: EXPLAIN ANALYZE plan (formatted ASCII tree). DuckDB JSON → `format_duckdb_explain_tree()`, PG JSON → `format_pg_explain_tree()`. Truncated at 150 lines.
- **§2b-i**: Cardinality Estimation Routing (Q-Error). Direction + Locus → pathology routing + structural flags. From `qerror.py::format_qerror_for_prompt()`. Same structure for all engines (DuckDB, PostgreSQL, Snowflake). Auto-detects plan format. Snowflake: plan_json typically null (no ANALYZE), section omitted gracefully. Only predictive signals (routing 85% accurate); magnitude/severity deliberately excluded.
- **§2c**: Logic tree from `build_logic_tree()` + per-node detail cards
- **§2d**: Pre-computed semantic intent (if available)
- **§2e**: Iteration history (oneshot iterative mode only)

### §3. CONSTRAINTS (hard rules)
- **§3a**: 4 correctness constraints (LITERAL_PRESERVATION, SEMANTIC_EQUIVALENCE, COMPLETE_OUTPUT, CTE_COLUMN_COMPLETENESS)
- **§3b**: Aggregation equivalence rules (STDDEV_SAMP, AVG, FILTER traps)

### §4. ENGINE PROFILE (reference knowledge)
- **Primary**: `exploit_algorithm_text` — loaded from `knowledge/{dialect}.md` via `prompter.py::load_exploit_algorithm()`. Contains pathologies, decision gates, regression registry.
- **Fallback**: `engine_profile` JSON — loaded from `constraints/engine_profile_{dialect}.json`. Contains strengths and gaps.
- Also: resource envelope (PG only), detected transforms (feature-matched)

### §5. TRANSFORM CATALOG (action vocabulary)
- **§5a**: 13 transforms in 4 categories (Predicate Movement, Join Restructuring, Scan Optimization, Structural Transforms). Each lists mapped gold examples.
- **§5c**: Strategy leaderboard (observed success rates, when available)

### §6. REASONING PROCESS (how to think)
- 6-7 step procedure: CLASSIFY → EXPLAIN ANALYSIS (+ Q-Error routing) → BOTTLENECK HYPOTHESIS (start from Q-Error) → AGGREGATION TRAP CHECK → INTERVENTION DESIGN → LOGICAL TREE DESIGN → [WRITE REWRITE for oneshot]
- Step 2 references §2b-i Q-Error routing as primary signal (85% accurate)
- Step 3 anchors hypothesis on Q-Error routing, then verifies against plan structure
- Strategy selection rules (applicability, optimizer overlap, diversity, risk, composition)

### §7. OUTPUT SPECIFICATION
- **§7a**: Output format template (shared briefing + per-worker briefings)
- **§7b**: Validation checklist
- **§7c**: Worker exploration rules (discovery mode vs normal mode)
- **§7d**: Output consumption spec (what each worker receives)

## EXPLAIN-First Reasoning Model

The §6 reasoning process follows an EXPLAIN-first hierarchy:

1. **Step 2 (OBSERVE + Q-ERROR)**: Read the EXPLAIN plan. Identify cost spine, classify nodes, trace data flow. Then read §2b-i Q-Error routing — direction+locus tells you where the planner is wrong and which pathologies to check first. Structural flags (DELIM_SCAN, EST_ZERO) are direct transform triggers. Ignore magnitude/severity (not predictive).
2. **Step 3 (HYPOTHESIZE + CALIBRATE)**: Start from Q-Error routing as hypothesis anchor. For each cost center, diagnose the optimizer behavior, hypothesize an intervention, then calibrate against engine knowledge (§4).
   - If a documented gap matches: USE its evidence (gates, what_worked, what_didnt_work)
   - If a strength matches: STOP (optimizer already handles it)
   - If no gap matches: tag as UNVERIFIED_HYPOTHESIS, design control variant
3. **Step 5 (DESIGN INTERVENTION)**: Match hypothesis to transform category, apply evidence if available, rank by estimated impact if novel.

This model works for ALL engines — Q-Error routing provides the primary hypothesis (85% accurate on validated wins), the pathology tree provides evidence and gates, and first-principles reasoning from the EXPLAIN plan fills remaining gaps.

## Discovery Mode

### Detection
```python
has_empirical_gaps = bool(engine_profile and engine_profile.get("gaps"))
is_discovery_mode = not has_empirical_gaps
```
Pure function of engine profile data. Triggers when no empirical gaps exist for this engine (e.g., Snowflake). Example matching is independent — workers receive examples via transform IDs regardless of discovery mode.

### Behavioral Changes
- **§7a**: ALL 4 worker blocks get CONSTRAINT_OVERRIDE, EXPLORATION_TYPE, and HYPOTHESIS_TAG fields (normally only Worker 4 gets these)
- **§7c**: Discovery mode text replaces normal Worker 4 rules. All workers explore with structured roles:
  - Worker 1: Highest-confidence hypothesis
  - Worker 2: Second-highest impact
  - Worker 3: Structural inefficiency focus
  - Worker 4: Compound/speculative (high risk / high reward)
- Each worker must specify: HYPOTHESIS, EVIDENCE, EXPECTED_MECHANISM, CONTROL_SIGNAL

### Graduation Path
When a discovery-mode run produces empirical results:
1. Confirmed hypotheses → promoted to gaps in engine profile JSON
2. Rejected hypotheses → noted in playbook with evidence
3. Novel wins → become gold examples in `examples/{engine}/`
4. After enough data, `is_discovery_mode` automatically becomes False

## Knowledge Loading

| Source | Loaded by | Injected into |
|--------|-----------|---------------|
| `knowledge/{dialect}.md` | `prompter.py::load_exploit_algorithm()` | §4 (exploit_algorithm_text) |
| `constraints/engine_profile_{dialect}.json` | `swarm_session.py` | §4 (engine_profile, fallback) |
| `examples/{engine}/*.json` | tag-based matching | Worker prompts (via analyst's EXAMPLES field → `_load_examples_by_id()`) |
| `constraints/*.json` | constraint loader | §3 (constraints) |
| `explains/{query_id}.json` → plan_json | `pipeline.py::_get_qerror_analysis()` → `qerror.py` | §2b-i (Q-Error routing) |

## Worker Prompt

Workers receive a subset of analyst output. 8 sections:
1. Role framing (strategy executor)
2. Original SQL
3. EXPLAIN plan
4. Shared briefing (from analyst: SEMANTIC_CONTRACT, OPTIMAL_PATH, CURRENT_PLAN_GAP, ACTIVE_CONSTRAINTS, REGRESSION_WARNINGS)
5. Worker-specific briefing (from analyst: STRATEGY, APPROACH, TARGET_QUERY_MAP, NODE_CONTRACTS, EXAMPLES, HAZARD_FLAGS)
6. Gold example before/after SQL (system-loaded based on EXAMPLES field)
7. Output format (component payload JSON)
7b. SET LOCAL config section (PG workers only, system resource envelope)

## Hypothesis Lifecycle

```
HYPOTHESIZED (in playbook, tagged with source + confidence)
    → TESTED (worker attempts rewrite, execution measured)
        → CONFIRMED (speedup ≥ 1.10x, row counts match)
            → Promoted to gap in engine_profile_{dialect}.json
            → Gold example created in examples/{engine}/
        → REJECTED (regression or no improvement)
            → Noted in playbook regression registry
            → Decision gates updated with failure evidence
```

The HYPOTHESIS_TAG field flows through: worker briefing → worker output → execution result → finding → engine profile update.

## Token Budget

| Section | Expected Range |
|---------|---------------|
| §1 Role | 50-80 tokens |
| §2 Input Package | 800-2000 tokens (depends on query size + EXPLAIN) |
| §3 Constraints | 200-300 tokens |
| §4 Engine Profile | 300-800 tokens (larger with exploit algorithm) |
| §5 Transform Catalog | 400-500 tokens |
| §6 Reasoning Process | 400-500 tokens |
| §7 Output Spec | 300-500 tokens |
| **Total** | **2500-4700 tokens** |

## Declarative Composition Model

The system supports an optional declarative composition layer via the `Orchestrator` class.
When an `Orchestrator` is provided (e.g., via `--scenario` or `--engine-version` CLI flags),
it assembles context from four declarative sources:

### Components

| Component | File Pattern | Purpose |
|-----------|-------------|---------|
| **Universal Doctrine** | `doctrine/universal_doctrine.yaml` | Engine-agnostic optimization philosophy, bottleneck taxonomy (9 labels), hallucination rules, correctness constraints, worker diversity (families A-F) |
| **Engine Pack** | `engine_packs/{engine}_{version}.yaml` | Per-engine capabilities, optimizer profile (handles_well + blind_spots), rewrite playbook, config boost rules, validation probes |
| **Scenario Card** | `scenario_cards/{name}.yaml` | Resource envelope + failure definitions + strategy priorities (e.g., `xsmall_survival`, `postgres_small_instance`) |
| **Pattern Library** | `patterns/library.yaml` | Cross-engine index of anti-patterns with scoring rubric: bottleneck +3, signal +2, scenario +1 |

### Composition Flow

```
Orchestrator.compose_system_context()
    ├── load_doctrine()           → universal rules, bottleneck taxonomy
    ├── load_engine_pack(engine)  → capabilities, blind spots, playbook
    ├── load_scenario_card(name)  → resource constraints, failure modes
    └── load_pattern_library()    → scored example selection
```

The composed context is added to `gather_analyst_context()` output as `orchestrator_context`.
It enriches the existing prompt assembly — it does NOT replace the battle-tested prompt builders.

### Backward Compatibility

When no orchestrator is provided (the default), the system uses the existing code path:
- `engine_profile_{dialect}.json` for gap/strength data
- `knowledge/{dialect}.md` for the exploit algorithm (tip of the spear)
- `pg_tuning.py` for resource envelope
- `tag_index.py` for example matching

Both paths produce working prompts. The orchestrator path adds structured metadata
from YAML sources alongside the existing JSON/markdown knowledge.

### Evidence Bundle

The `EvidenceBundle` unifies existing extractors into a single structure:

| Field | Source | Existing Extractor |
|-------|--------|--------------------|
| `cost_spine` | DAG per-node costs | `pipeline._parse_logical_tree()` |
| `runtime_profile.estimates` | Q-Error analysis | `qerror.py` (85% accurate) |
| `runtime_profile.spill` | Vital signs | `explain_signals.extract_vital_signs()` |
| `runtime_profile.pruning` | Vital signs | `explain_signals.extract_vital_signs()` |
| `runtime_profile.memory` | Resource envelope | `pg_tuning.build_resource_envelope()` |

### Output Contract

`QueryOutputContract` wraps `SessionResult` with structured diagnostics:
- `Diagnosis`: bottleneck label, evidence, engine feature
- `ExpectedImpact`: metric, before/after, confidence
- `ValidationSummary`: equivalence, regression check, benchmark result, scenario fit

### Compress Stage

Post-validation candidate ranking via `compress.py`:
- **Dedup**: AST-normalize via sqlglot, deduplicate identical rewrites
- **Score**: Impact (1-5) x Confidence (1-5) x Invasiveness (1-5), range 1-125
- Confidence from validation tier: race-validated=5, sequential 3-run=4, estimate-only=2
- Invasiveness: query-only=5 (best), with config=4, with stats=3, with index=2, with schema=1

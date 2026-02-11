# Knowledge Engine Architecture Overview

## Summary

This document provides a high-level overview of the **Knowledge Engine** - a self-learning circular system that feeds the **Product Pipeline** with curated optimization knowledge.

---

## Two-System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           QUERYTORQUE COMPLETE ARCHITECTURE                             │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐  │
│   │                         KNOWLEDGE ENGINE (Circular)                             │  │
│   │                                                                                 │  │
│   │   Layer 4 ◄── Layer 3 ◄── Layer 2 ◄── Layer 1 ◄──┐                             │  │
│   │   (Knowledge)  (Patterns)  (Findings)  (Raw)     │                             │  │
│   │       │                                        (Ingest)                         │  │
│   │       │                                          ▲                              │  │
│   │       │ Interface A                              │ Interface B                  │  │
│   │       │ (Read)                                   │ (Write)                      │  │
│   │       ▼                                          │                              │  │
│   └───────┼──────────────────────────────────────────┼──────────────────────────────┘  │
│           │                                          │                                 │
│           ▼                                          ▼                                 │
│   ┌─────────────────────────────────────────────────────────────────────────────────┐  │
│   │                       PRODUCT PIPELINE (Linear 7-Phase)                         │  │
│   │                                                                                 │  │
│   │   Phase 1 ──► Phase 2 ──► Phase 3 ──► Phase 4 ──► Phase 5 ──► Phase 6 ──► Phase 7│ │
│   │  Context    Knowledge   Prompt      LLM       Response  Validation   Outputs    │  │
│   │  Gather     Retrieval   Generate    Inference Process   & Bench     & Learn     │  │
│   │      │          ▲                                                      │         │  │
│   │      │          │ Interface A                                  Interface B       │  │
│   │      └──────────┘ (pull knowledge)                            (push outcomes)    │  │
│   │                                                                                 │  │
│   └─────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

### Key Distinction

| Aspect | Product Pipeline | Knowledge Engine |
|--------|-----------------|------------------|
| **Flow** | Linear (1→2→3→4→5→6→7) | Circular (L1→L2→L3→L4→L1) |
| **Timing** | Synchronous per query | Asynchronous, background |
| **Scope** | Single optimization | Accumulated learning |
| **State** | Stateless (per query) | Stateful (compounds over time) |
| **Interface** | 2 well-defined points | Clean read/write API |

---

## Interface Contract

### Interface A: Knowledge Retrieval (Engine → Pipeline)

**Called by**: Phase 2 (Knowledge Retrieval)
**Purpose**: Get relevant examples and guidance for the current query
**Contract**: Must respond in <500ms (all data pre-computed)

```python
# Request from Pipeline
KnowledgeQuery {
    query_id: "q88"
    sql_fingerprint: "decorrelate_subquery_date_filter"
    dialect: "duckdb"
    available_context: {...}
    context_confidence: "high"
}

# Response from Knowledge Engine
KnowledgeResponse {
    matched_examples: [GoldExample, ...]
    engine_profile: EngineProfile
    constraints: [Constraint, ...]
    scanner_findings: ScannerFindings  # PG only
    knowledge_version: "2026.02.11-v3"
    freshness_score: 0.92  # 0-1, triggers refresh if <0.3
}
```

### Interface B: Outcome Ingestion (Pipeline → Engine)

**Called by**: Phase 7 (Outputs & Learning)
**Purpose**: Report optimization outcome for learning
**Contract**: Fire-and-forget, async processing

```python
# Report from Pipeline
OptimizationOutcome {
    query_id: "q88"
    status: "WIN"
    speedup: 4.5
    transforms_applied: ["date_cte_isolate"]
    original_sql: "SELECT ..."
    optimized_sql: "WITH ..."
    worker_responses: ["..."]
    error_category: null
    model: "deepseek-reasoner"
}
```

---

## Knowledge Engine Layers

### Layer 1: Blackboard (Raw)

**Purpose**: Capture everything, forever
**Format**: JSONL (append-only)
**Compression**: Time-based rollups (7d → daily → weekly)

```
blackboard/raw/2026-02-11/query_88/worker_01.json
```

**Schema**: [optimization_outcome.schema.json](../packages/qt-sql/qt_sql/specs/optimization_outcome.schema.json)

```yaml
BlackboardEntry:
  base: {query_id, dialect, fingerprint, timestamp, run_id}
  opt: {worker_id, strategy, examples_used, iteration}
  outcome: {status, speedup, speedup_type, validation_confidence}
  transforms: {primary, all}
  principles: {what_worked, why_it_worked, principle_id}
  config: {set_local, plan_flags}  # PG only
  error: {category, messages}
  reasons: {reasoning_chain, evidence}
  provenance: {model, provider, git_sha, reviewed}
```

### Layer 2: Findings (Extracted)

**Purpose**: Extract patterns from raw data via LLM
**Format**: JSON
**Compression**: Semantic extraction (1000 entries → ~20 findings)

```
findings/postgresql/join_sensitivity/SF-001.json
```

**Schema**: [scanner_finding.schema.json](../packages/qt-sql/qt_sql/specs/scanner_finding.schema.json)

```yaml
ScannerFinding:
  id: "SF-001"
  category: "join_sensitivity"
  claim: "Disabling nested loops causes >4x regression on dim-heavy star queries"
  evidence: {summary, count, contradicting, supporting_queries}
  mechanism: "Nested loops exploit dim PK indexes..."
  boundaries: {applies_when, does_not_apply_when}
  confidence: "high"
  implication: "Do NOT restructure joins that eliminate nested loop..."
```

### Layer 3: Patterns (Distilled)

**Purpose**: Cross-query pattern aggregation
**Format**: JSON
**Compression**: Validation across queries (~20 findings → ~3 patterns)

```
patterns/duckdb/predicate_pushdown/PATTERN-DATE-CTE-001.json
```

**Schema**: [optimization_pattern.schema.json](../packages/qt-sql/qt_sql/specs/optimization_pattern.schema.json)

```yaml
OptimizationPattern:
  id: "PATTERN-DATE-CTE-001"
  classification: {mechanism, impact_tier, pattern, risk, exploit_type}
  technique: {description, sql_template, before_example, after_example}
  stats: {n_observations, n_wins, success_rate, avg_speedup}
  applicability: {query_archetypes, required_features}
  counter_indications: [...]
  related_patterns: [...]
  status: "promoted"  # candidate | promoted | deprecated
```

### Layer 4: Knowledge Store (Curated)

**Purpose**: Injected into Product Pipeline
**Format**: JSON (human-editable)
**Compression**: Promotion criteria (~3 patterns → 1 knowledge item)

```
knowledge/duckdb/engine_profile.json
knowledge/duckdb/gold_examples/q6_date_cte.json
```

**Schemas**:
- [engine_profile.schema.json](../packages/qt-sql/qt_sql/specs/engine_profile.schema.json)
- [gold_example.schema.json](../packages/qt-sql/qt_sql/specs/gold_example.schema.json)

```yaml
EngineProfile:
  engine: "duckdb"
  strengths: [...]  # What NOT to fight
  gaps: [...]       # What to exploit
  tuning_intel: {...}  # Engine-specific

GoldExample:
  id: "q6_date_cte"
  original_sql: "..."
  optimized_sql: "..."
  speedup: 4.0
  explanation: {what, why, when, when_not}
  status: "active"
```

---

## Compression Pipeline

```
Raw Outcomes (1000x) 
    │
    ▼ Temporal Rollup
    │
Layer 1: Blackboard Entries
    │
    ▼ LLM Extraction (50x compression)
    │
Layer 2: Findings (~20x)
    │
    ▼ Pattern Mining (7x compression)
    │
Layer 3: Patterns (~3x)
    │
    ▼ Promotion (3x compression)
    │
Layer 4: Knowledge (1x)

Total: 1000:1 compression over full lifecycle
```

### Compression Triggers

| Stage | Trigger | Implementation |
|-------|---------|----------------|
| Temporal | Age > 7 days | `blackboard/rollup.py` |
| Extraction | 50+ new entries OR 24 hours | `layer2/findings.py` |
| Pattern Mining | 10+ new findings | `layer3/miner.py` |
| Promotion | >5 wins AND >70% success | `layer4/promotion.py` |

---

## Data Storage Locations

```
qt_sql/
├── knowledge_engine/              # NEW
│   ├── layer1/blackboard/         # Raw outcomes (JSONL)
│   ├── layer2/findings/           # LLM-extracted (JSON)
│   ├── layer3/patterns/           # Aggregated patterns (JSON)
│   └── layer4/store/              # Curated knowledge (JSON)
│       ├── engine_profile.json
│       └── gold_examples/
│
├── specs/                         # NEW: JSON Schemas
│   ├── blackboard_entry.schema.json
│   ├── scanner_finding.schema.json
│   ├── optimization_pattern.schema.json
│   ├── engine_profile.schema.json
│   └── gold_example.schema.json
│
└── docs/
    ├── PRODUCT_CONTRACT.md        # Existing: Pipeline phases
    ├── KNOWLEDGE_ENGINE_DESIGN.md # NEW: Full design
    └── KNOWLEDGE_ENGINE_ARCHITECTURE_OVERVIEW.md  # This file
```

---

## Implementation Status

### ✅ Already Exists (Needs Migration)

| Component | Current Location | Target Location |
|-----------|-----------------|-----------------|
| Blackboard entries | `build_blackboard.py` | `knowledge_engine/layer1/` |
| Scanner blackboard | `scanner_knowledge/blackboard.py` | `knowledge_engine/layer1/scanner/` |
| Scanner findings | `scanner_knowledge/findings.py` | `knowledge_engine/layer2/` |
| Engine profiles | `constraints/engine_profile_*.json` | `knowledge_engine/layer4/` |

### 🔴 Not Implemented (New Development)

| Component | Purpose | Effort |
|-----------|---------|--------|
| `knowledge_engine/api.py` | Interface A & B | 2 days |
| `layer3/miner.py` | Pattern mining | 3 days |
| `layer4/promotion.py` | Auto-promotion | 2 days |
| `compression/pipeline.py` | Orchestration | 2 days |
| `layer4/similarity.py` | Example matching | 2 days |

### 🟡 Partially Exists (Needs Extension)

| Component | Current State | Needed |
|-----------|--------------|--------|
| Unified schema | Two separate schemas | Merge + extend |
| Findings → Profile | Manual only | Auto-bridge |
| Classification | Hardcoded tags | Taxonomy + auto-classify |
| JSON Schemas | None | Full spec (✅ created) |

---

## Quick Reference

### Schemas

| Schema | Purpose | File |
|--------|---------|------|
| OptimizationOutcome | Raw outcome capture | `specs/optimization_outcome.schema.json` |
| ScannerFinding | PG plan-space insight | `specs/scanner_finding.schema.json` |
| OptimizationFinding | 4W outcome insight | `specs/optimization_finding.schema.json` |
| OptimizationPattern | Cross-query pattern | `specs/optimization_pattern.schema.json` |
| EngineProfile | Curated engine knowledge | `specs/engine_profile.schema.json` |
| GoldExample | Promoted optimization | `specs/gold_example.schema.json` |

### Interfaces

| Interface | Direction | Method | Response Time |
|-----------|-----------|--------|---------------|
| A | Engine → Pipeline | `query()` | <500ms |
| B | Pipeline → Engine | `ingest()` | Fire-and-forget |

### Compression Ratios

| Layer | Compression | Cumulative |
|-------|-------------|------------|
| L1 (Raw) | 1:1 | 1:1 |
| L2 (Findings) | ~50:1 | ~50:1 |
| L3 (Patterns) | ~7:1 | ~350:1 |
| L4 (Knowledge) | ~3:1 | ~1000:1 |

---

## Next Steps

1. **Review Design**: Validate `KNOWLEDGE_ENGINE_DESIGN.md`
2. **Approve Schemas**: Review JSON schemas in `specs/`
3. **Migrate Existing**: Move `build_blackboard.py` and `scanner_knowledge/` to new structure
4. **Implement API**: Build `knowledge_engine/api.py` with Interface A & B
5. **Build Pipeline**: Implement compression pipeline (L1→L2→L3→L4)
6. **Integrate**: Connect to Product Pipeline Phase 2 & 7

---

## Appendix: Document Map

```
docs/
├── PRODUCT_CONTRACT.md              # 7-phase pipeline (EXISTS)
│   └── Defines: Phases 1-7, handoff contracts, API
│
├── KNOWLEDGE_ENGINE_DESIGN.md       # Full design spec (NEW)
│   └── Defines: Circular lifecycle, interfaces, compression
│
├── KNOWLEDGE_ENGINE_ARCHITECTURE_OVERVIEW.md  # This file (NEW)
│   └── Provides: High-level summary, quick reference
│
└── KNOWLEDGE_SYSTEM_DESIGN_GAP_ANALYSIS.md    # Gap analysis (NEW)
    └── Documents: What was missing vs Product Contract

packages/qt-sql/qt_sql/
├── specs/                           # JSON Schemas (NEW)
│   ├── blackboard_entry.schema.json
│   ├── scanner_finding.schema.json
│   ├── optimization_pattern.schema.json
│   ├── engine_profile.schema.json
│   └── gold_example.schema.json
│
└── knowledge_engine/                # NEW MODULE
    ├── api.py                       # Interface A & B
    ├── layer1/                      # Blackboard (raw)
    ├── layer2/                      # Findings (extracted)
    ├── layer3/                      # Patterns (distilled)
    ├── layer4/                      # Knowledge (curated)
    └── compression/                 # Compression pipeline
```

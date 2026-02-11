# Knowledge Learning System: Gap Analysis vs Product Contract

## Executive Summary

The **Product Contract** (`packages/qt-sql/qt_sql/docs/PRODUCT_CONTRACT.md`) defines a 7-phase optimization pipeline with clear data contracts between phases. However, it does NOT define the **closed feedback loop** architecture for knowledge learning that was described in your requirements:

```
Desired Architecture (from your description):
┌─────────────────────────────────────────────────────────────────────────────┐
│                         KNOWLEDGE LEARNING FEEDBACK LOOP                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌──────────────┐    ┌──────────┐    ┌──────────────┐    ┌──────────────┐ │
│   │  Blackboard  │───▶│ Findings │───▶│Engine Profile│───▶│ Analyst Plan │ │
│   │   (Layer 1)  │    │(Layer 2) │    │  (Layer 3)   │    │  (Layer 4)   │ │
│   └──────────────┘    └──────────┘    └──────────────┘    └──────┬───────┘ │
│          ▲                                                       │         │
│          │                                                       ▼         │
│          │                                                ┌──────────────┐ │
│          │                                                │ 4W Execution │ │
│          │                                                │   (Layer 5)  │ │
│          │                                                └──────┬───────┘ │
│          │                                                       │         │
│          └───────────────────────────────────────────────────────┘         │
│                              (Feedback Loop)                                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

The Product Contract describes **Phases 1-7** of a single optimization run, but the **knowledge accumulation loop** that feeds learnings back into future runs is only partially implemented and not fully documented as a system architecture.

---

## Part 1: Product Contract Coverage Analysis

### What's IN the Product Contract

| Component | Contract Coverage | Status |
|-----------|------------------|--------|
| **Phase 1: Context Gathering** | ✅ Fully specified | `plan_scanner.py`, `scanner_knowledge/`, `dag.py` |
| **Phase 2: Knowledge Retrieval** | ✅ Fully specified | `knowledge.py`, `tag_index.py`, `prompter.py` |
| **Phase 2→3 Handoff** | ✅ Fully specified | `gather_analyst_context()` with field contract |
| **Phase 3: Prompt Generation** | ✅ Fully specified | `prompts/analyst_briefing.py`, `prompts/worker.py` |
| **Phase 4: LLM Inference** | ✅ Fully specified | `generate.py`, `CandidateGenerator` |
| **Phase 5: Response Processing** | ✅ Fully specified | `sql_rewriter.py`, validation gates |
| **Phase 6: Validation** | ✅ Fully specified | `validate.py`, per-engine equivalence rules |
| **Phase 7: Outputs** | ✅ Fully specified | `store.py`, `learn.py`, `build_blackboard.py` |
| **API Contract** | ✅ Fully specified | FastAPI endpoints, request/response schemas |
| **Scanner Intelligence Flow** | ✅ Specified | Product-Defining Workflow A (PG only) |
| **Blackboard Intelligence Flow** | ✅ Partially specified | Product-Defining Workflow B |

### What's MISSING from the Product Contract

| Missing Component | Why It Matters |
|------------------|----------------|
| **Findings → Engine Profile Bridge** | The contract mentions `engine_profile` as input to prompts, but NOT how findings auto-update profiles |
| **Classification System Spec** | No unified taxonomy for optimization types, impact tiers, risk profiles |
| **Cross-Engine Profile Schema** | PG profile has `set_local_config_intel` section; DuckDB lacks equivalent structure |
| **Automated Pipeline Orchestration** | No specification for `knowledge_pipeline.py` that runs the full feedback loop |
| **Gold Example Promotion Rules** | Contract mentions gold examples but not auto-promotion criteria |
| **Artifact Specs (JSON Schema)** | No machine-readable specs for LLM outputs, only human-readable descriptions |

---

## Part 2: Detailed Gap Analysis

### 🔴 CRITICAL GAP 1: No Unified Blackboard Schema

**Product Contract Says:**
- `BlackboardEntry` is defined in `build_blackboard.py` with fields: `query_id`, `worker_id`, `run_name`, `timestamp`, `query_intent`, `query_fingerprint`, `examples_used`, `strategy`, `status`, `speedup`, `transforms_applied`, `error_category`, `error_messages`, `what_worked`, `why_it_worked`, `what_failed`, `why_it_failed`, `principle`, `reviewed`

- `ScannerObservation` is defined in `scanner_knowledge/schemas.py` with fields: `query_id`, `flags`, `source`, `category`, `combo_name`, `summary`, `plan_changed`, `cost_ratio`, `wall_speedup`, `baseline_ms`, `combo_ms`, `rows_match`, `vulnerability_types`, `n_plan_changers`, `n_distinct_plans`

**The Problem:**
Two different blackboard systems with NO shared schema:

```python
# build_blackboard.py BlackboardEntry
@dataclass
class BlackboardEntry:
    query_id: str
    worker_id: int
    run_name: str
    # ... captures worker outcomes from optimization runs

# scanner_knowledge/schemas.py ScannerObservation  
@dataclass
class ScannerObservation:
    query_id: str
    flags: Dict[str, str]  # SET LOCAL config flags
    # ... captures PG plan scanner observations
```

**What's Missing:**
The `base:opt:principles:semantics:config:reasons` schema you specified:

```yaml
# DESIRED unified schema (NOT implemented)
blackboard_entry:
  base:
    query_id: str
    sql: str
    dialect: str
    fingerprint: str
  
  opt:
    optimization_attempted: bool
    strategy: str
    worker_id: int
    run_name: str
    
  principles:
    what_worked: str
    why_it_worked: str
    principle_id: str
    transforms_applied: List[str]
    
  semantics:
    semantic_contract: str
    transforms_applied: List[str]
    query_intent: str
    
  config:
    set_local_configs: Dict[str, str]  # PG only
    plan_flags: Dict[str, str]  # From scanner
    
  reasons:
    reasoning_chain: str
    evidence: str
    error_category: str
    
  outcome:
    speedup: float
    speedup_type: str  # measured | vs_timeout_ceiling | both_timeout
    status: str  # WIN | IMPROVED | NEUTRAL | REGRESSION | ERROR | FAIL
    validation_results: Dict
    validation_confidence: str  # high | row_count_only | zero_row_unverified
```

---

### 🔴 CRITICAL GAP 2: Findings → Engine Profile Bridge Missing

**Product Contract Says:**
- Phase 2 loads `engine_profile` from `constraints/engine_profile_*.json`
- Scanner findings go to `scanner_findings.json` via `scanner_knowledge/findings.py`

**The Problem:**
No automation to convert `ScannerFinding` → `EngineGap`:

```python
# What exists: ScannerFinding (from scanner_knowledge/schemas.py)
@dataclass
class ScannerFinding:
    id: str
    claim: str
    category: str
    supporting_queries: List[str]
    evidence_summary: str
    evidence_count: int
    contradicting_count: int
    boundaries: List[str]
    mechanism: str
    confidence: str
    confidence_rationale: str
    implication: str

# What's loaded: Engine Profile (from constraints/engine_profile_postgresql.json)
{
  "engine": "postgresql",
  "strengths": [...],
  "gaps": [
    {
      "id": "COMMA_JOIN_WEAKNESS",
      "priority": "HIGH",
      "what": "Implicit comma-separated FROM tables...",
      "why": "The planner's join search space is less constrained...",
      "opportunity": "Convert comma-joins to explicit JOIN...",
      "what_worked": [...],
      "what_didnt_work": [...],
      "field_notes": [...]
    }
  ]
}

# MISSING: Conversion bridge
class FindingsToProfileBridge:
    def convert_finding_to_gap(self, finding: ScannerFinding) -> EngineGap:
        """Convert LLM finding to engine profile gap format."""
        return EngineGap(
            id=finding.id,
            priority="HIGH" if finding.confidence == "high" else "MEDIUM",
            what=finding.claim,
            why=finding.mechanism,
            opportunity=finding.implication,
            what_worked=[f"{q}: verified" for q in finding.supporting_queries],
            field_notes=finding.boundaries,
        )
```

**Current State:**
- `scanner_findings.json` exists but is NOT automatically fed back into `engine_profile_postgresql.json`
- Engine profile is hand-written, not auto-generated from accumulated findings

---

### 🔴 CRITICAL GAP 3: No Automated Classification System

**Product Contract Says:**
- `tag_index.py` has basic category classification
- No mention of automated classification of optimization types

**What's Missing:**

```python
# DESIRED: Auto-classification schema (NOT implemented)
class OptimizationClassification:
    # By mechanism
    mechanism: Literal[
        "predicate_pushdown", "join_reorder", "scan_consolidation",
        "decorrelation", "materialization", "set_operation_rewrite"
    ]
    
    # By impact magnitude
    impact_tier: Literal["low", "medium", "high", "breakthrough"]
    
    # By structural pattern
    pattern: Literal[
        "star_schema_prefetch", "dimension_isolation",
        "fact_table_consolidation", "temporal_join_rewrite"
    ]
    
    # By risk profile
    risk: Literal["safe", "moderate", "aggressive", "exploratory"]
    
    # By engine exploit type
    exploit_type: Literal[
        "gap_exploit",      # Exploits known optimizer gap
        "strength_avoid",   # Avoids fighting optimizer strength
        "neutral_rewrite"   # Structural improvement
    ]
```

**Current State:**
- `tag_index.py` extracts tags like `set_operations`, `aggregation_rewrite`
- `build_blackboard.py` has hardcoded `TRANSFORM_PRINCIPLES` mapping
- No systematic classification linking findings to engine gaps

---

### 🔴 CRITICAL GAP 4: Missing Artifact Specs for LLM Automation

**Product Contract Says:**
- Prompts are built by `prompts/analyst_briefing.py`, `prompts/worker.py`
- Output parsing is handled by `prompts/swarm_parsers.py`

**What's Missing:**
Machine-readable JSON Schema specs for all artifacts:

```json
// DESIRED: specs/analyst_briefing.schema.json (NOT implemented)
{
  "artifact": "analyst_briefing",
  "version": "2.0",
  "sections": {
    "semantic_contract": {
      "type": "string",
      "min_tokens": 80,
      "max_tokens": 150,
      "required_elements": ["business_intent", "join_semantics", "aggregation_traps"]
    },
    "bottleneck_diagnosis": {
      "type": "object",
      "properties": {
        "dominant_mechanism": {"enum": ["scan-bound", "join-bound", "aggregation-bound"]},
        "cardinality_flow": {"type": "array", "items": {"type": "integer"}},
        "optimizer_overlap": {"type": "string"}
      }
    },
    "active_constraints": {
      "type": "array",
      "min_items": 4,
      "items": {
        "properties": {
          "constraint_id": {"type": "string"},
          "evidence": {"type": "string"}
        }
      }
    },
    "worker_briefings": {
      "type": "array",
      "min_items": 4,
      "max_items": 4,
      "items": {"$ref": "#/$defs/worker_briefing"}
    }
  }
}
```

**Current State:**
- Human-readable spec in `prompts/sql_rewrite_spec.md` (DAP v1.0)
- `PROMPT_SPEC.md` has prompt builder reference
- NO JSON Schema for automated validation

---

### 🔴 CRITICAL GAP 5: DuckDB Profile Schema Mismatch

**Product Contract Says:**
- `constraints/engine_profile_duckdb.json` exists with `strengths` and `gaps`

**The Problem:**
Schema mismatch between DuckDB and PG profiles:

```json
// engine_profile_postgresql.json has:
{
  "engine": "postgresql",
  "set_local_config_intel": {
    "briefing_note": "...",
    "rules": [...],
    "key_findings": [...]
  }
}

// engine_profile_duckdb.json LACKS equivalent section
// DuckDB has different tunables (no SET LOCAL equivalent)
```

**What's Missing:**
A unified profile schema that handles both engines:

```json
// DESIRED unified engine profile schema
{
  "engine": "duckdb|postgresql",
  "version_tested": "string",
  "profile_type": "engine_profile",
  "briefing_note": "string",
  
  "strengths": [...],
  "gaps": [...],
  
  // Engine-specific tuning section
  "tuning_intel": {
    "available": true|false,
    "mechanism": "set_local|pragma|other",
    "rules": [...]
  }
}
```

---

### 🔴 CRITICAL GAP 6: Feedback Loop Not Closed

**Product Contract Says:**
- Phase 7 outputs go to `Store.save_candidate()`, `Learner`, `build_blackboard`
- "Blackboard findings feed both local and global intelligence context"

**The Problem:**
The loop is broken at multiple points:

```
Current (Broken) Flow:
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│   4W Run    │────▶│  Blackboard  │────▶│   Findings   │
└─────────────┘     └──────────────┘     └──────────────┘
                                                │
                                                ▼
                                         ┌──────────────┐
                                         │   (stuck)    │  ← No bridge
                                         └──────────────┘

Desired (Closed) Flow:
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│   4W Run    │────▶│  Blackboard  │────▶│   Findings   │
└─────────────┘     └──────────────┘     └──────────────┘
                                                │
                                                ▼
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│   4W Run    │◀────│ Analyst Plan │◀────│Engine Profile│◀──┘
└─────────────┘     └──────────────┘     └──────────────┘
      │                                               ▲
      └───────────────────────────────────────────────┘
                    (Gold Examples Feedback)
```

**What's Missing:**

```python
# DESIRED: qt_sql/knowledge_pipeline.py (NOT implemented)

def run_knowledge_pipeline(batch_dir: Path):
    """Full automated knowledge extraction and integration."""
    
    # 1. Extract from run
    entries = extract_blackboard_entries(batch_dir)
    
    # 2. Generate findings via LLM (PG only)
    findings = generate_findings(entries)
    
    # 3. Update engine profiles
    update_engine_profile(findings, dialect="postgresql")
    
    # 4. Promote gold examples (auto if speedup > threshold)
    promoted = auto_promote_winners(entries, min_speedup=2.0)
    
    # 5. Rebuild tag index
    rebuild_tag_index()
    
    # 6. Validate analyst can see new knowledge
    validate_prompt_injection()
    
    return KnowledgePipelineResult(...)
```

**Current State:**
- `build_blackboard.py` extracts and collates
- `scanner_knowledge/build_all.py` generates findings
- NO automated bridge to engine profiles
- NO automated gold example promotion

---

### 🔴 CRITICAL GAP 7: Findings Template Not Engine-Specific

**Product Contract Says:**
- `scanner_knowledge/templates/findings_prompt.md` is mentioned in docs/README.md

**The Problem:**
The findings prompt (in `scanner_knowledge/findings.py`) is hardcoded for PostgreSQL:

```python
# From findings.py build_findings_prompt():
"# PostgreSQL Plan-Space Scanner Analysis",
"",
"Below are observations from toggling planner flags (SET LOCAL) across 76",
"DSB benchmark queries on PostgreSQL 14.3 (SF10).",
```

**What's Missing:**
Engine-specific finding categories:

```python
# schemas/findings_postgresql.py (NOT implemented)
PG_FINDING_CATEGORIES = {
    "join_sensitivity": "Nested loop vs hash vs merge join behavior",
    "memory_spill": "Work mem effects on sort/hash operations",
    "parallelism_gap": "When parallel workers don't launch",
    "jit_overhead": "JIT compilation cost vs benefit",
    "cost_model_mismatch": "Planner estimates vs actuals"
}

# schemas/findings_duckdb.py (NOT implemented)
DUCKDB_FINDING_CATEGORIES = {
    "streaming_barrier": "Operations that block streaming execution",
    "compression_tradeoff": "When compression helps vs hurts",
    "inlining_boundary": "CTE/subquery inlining decisions",
    "pipeline_parallelism": "Thread utilization across pipelines"
}
```

---

## Part 3: Recommended Design Document Structure

To address these gaps, you need a **Knowledge Learning System Design** document that complements the Product Contract:

```
docs/
├── PRODUCT_CONTRACT.md          # Current: Phase 1-7 pipeline contract
├── KNOWLEDGE_SYSTEM_DESIGN.md   # NEW: Full feedback loop architecture
│   ├── 1-ARCHITECTURE.md        # Component diagram, data flow
│   ├── 2-BLACKBOARD_SCHEMA.md   # Unified schema spec
│   ├── 3-FINDINGS_PIPELINE.md   # Scanner → Findings → Profile
│   ├── 4-CLASSIFICATION.md      # Taxonomy, impact tiers, risk
│   ├── 5-ARTIFACT_SPECS.md      # JSON Schema for all LLM outputs
│   └── 6-AUTOMATION.md          # Pipeline orchestration
└── specs/
    ├── analyst_briefing.schema.json
    ├── worker_response.schema.json
    ├── scanner_finding.schema.json
    └── engine_profile.schema.json
```

### Component Boundaries & Data Movement

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     KNOWLEDGE LEARNING SYSTEM DESIGN                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────┐          ┌─────────────────┐          ┌─────────────┐ │
│  │   Blackboard    │          │    Findings     │          │   Profile   │ │
│  │     Layer       │─────────▶│     Layer       │─────────▶│   Layer     │ │
│  │                 │          │                 │          │             │ │
│  │ • Raw entries   │  JSONL   │ • LLM-extracted │   JSON   │ • Strengths │ │
│  │ • Observations  │─────────▶│ • Categorized   │─────────▶│ • Gaps      │ │
│  │ • Collated      │          │ • Verified      │          │ • Config    │ │
│  └─────────────────┘          └─────────────────┘          └──────┬──────┘ │
│          ▲                                                        │        │
│          │                                                        ▼        │
│          │                                               ┌─────────────┐   │
│          │                                               │   Prompt    │   │
│          │                                               │   Layer     │   │
│          │                                               │             │   │
│          │                                               │ • Analyst   │   │
│          │                                               │ • Worker    │   │
│          │                                               └──────┬──────┘   │
│          │                                                        │        │
│          │                       ┌─────────────────┐              │        │
│          └───────────────────────│   Execution     │◀─────────────┘        │
│                                  │     Layer       │                       │
│                                  │                 │                       │
│                                  │ • 4W Workers    │                       │
│                                  │ • Validation    │                       │
│                                  │ • Gold examples │───────────────────────┘
│                                  └─────────────────┘
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

Data Contracts:
┌────────────────┬──────────────────┬──────────────────┬─────────────────────┐
│   Interface    │     Source       │      Target      │      Format         │
├────────────────┼──────────────────┼──────────────────┼─────────────────────┤
│ Blackboard Out │ build_blackboard │ findings.py      │ JSONL               │
│ Findings Out   │ findings.py      │ engine_profile*  │ JSON                │
│ Profile In     │ engine_profile*  │ prompter.py      │ JSON                │
│ Prompt Out     │ prompts/*.py     │ generate.py      │ String (validated)  │
│ Response In    │ generate.py      │ sql_rewriter.py  │ String (JSON/DAP)   │
│ Validation Out │ validate.py      │ build_blackboard │ ValidationResult    │
└────────────────┴──────────────────┴──────────────────┴─────────────────────┘
```

---

## Part 4: Fix Priority (Updated with Contract Context)

| Priority | Fix | Contract Impact | Effort |
|----------|-----|-----------------|--------|
| **P0** | Create `engine_profile_duckdb.json` SET LOCAL equivalent | Clarify engine-specific tuning sections | 1 day |
| **P0** | Unify blackboard schemas (merge `build_blackboard.py` + `scanner_knowledge`) | Update Phase 7 data structures | 2 days |
| **P1** | Build `findings_to_profile.py` bridge | Add new component to contract | 1 day |
| **P1** | Artifact JSON Schema specs | Add validation layer to Phase 3 | 2 days |
| **P2** | Auto-classification system | Extend Phase 2 with taxonomy | 3 days |
| **P2** | Engine-specific finding templates | Update scanner_knowledge spec | 1 day |
| **P3** | Full automated pipeline | New orchestration component | 3 days |

---

## Part 5: Immediate Action Items

1. **Create `docs/KNOWLEDGE_SYSTEM_DESIGN.md`** as the canonical architecture doc
2. **Update `PRODUCT_CONTRACT.md`** to reference the knowledge system design
3. **Add JSON Schema specs** for all LLM artifacts in `qt_sql/specs/`
4. **Implement `findings_to_profile.py`** to close the Findings → Profile gap
5. **Add `knowledge_pipeline.py`** orchestrator for automated feedback loop

---

## Appendix: Code Locations

| Component | Current Location | Lines of Code |
|-----------|-----------------|---------------|
| Blackboard Entry | `build_blackboard.py:51-128` | ~80 lines |
| Scanner Observation | `scanner_knowledge/schemas.py:62-142` | ~80 lines |
| Scanner Finding | `scanner_knowledge/schemas.py:146-204` | ~60 lines |
| Engine Profile (PG) | `constraints/engine_profile_postgresql.json` | 208 lines |
| Engine Profile (DuckDB) | `constraints/engine_profile_duckdb.json` | 166 lines |
| Findings Extraction | `scanner_knowledge/findings.py` | 453 lines |
| Knowledge Pipeline | NOT IMPLEMENTED | N/A |

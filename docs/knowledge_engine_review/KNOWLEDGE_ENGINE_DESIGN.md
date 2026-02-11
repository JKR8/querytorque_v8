# Knowledge Engine Design Specification

## 1. Executive Summary

The **Knowledge Engine** is a self-reinforcing learning system that operates independently from, but feeds into, the Product Pipeline. It maintains a circular lifecycle where optimization outcomes continuously refine future optimization strategies.

> ⚠️ **Important Naming Note**: The Knowledge Engine is **separate** from the PostgreSQL "Plan Scanner" (`plan_scanner.py`). The Scanner is a Phase 1 tool for PostgreSQL plan-space exploration. The Knowledge Engine ingests outcomes from BOTH the Scanner (PG only) AND 4W optimization runs (all engines). See [NAMING_CLARIFICATION.md](NAMING_CLARIFICATION.md).

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              KNOWLEDGE ENGINE ARCHITECTURE                          │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│   ┌───────────────────────────────────────────────────────────────────────────┐    │
│   │                         CIRCULAR LEARNING LIFECYCLE                       │    │
│   │                                                                           │    │
│   │    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐          │    │
│   │    │ INGEST   │───▶│  EXTRACT │───▶│ DISTILL  │───▶│  INJECT  │          │    │
│   │    │          │    │          │    │          │    │          │          │    │
│   │    │ Capture  │    │ Pattern  │    │Compress &│    │ Feed to  │          │    │
│   │    │ outcomes │    │  mine    │    │ validate │    │ pipeline │          │    │
│   │    └────┬─────┘    └────┬─────┘    └────┬─────┘    └────┬─────┘          │    │
│   │         ▲               │               │               │                 │    │
│   │         │               │               │               │                 │    │
│   │         └───────────────┴───────────────┴───────────────┘                 │    │
│   │                              (feedback loop)                              │    │
│   │                                                                           │    │
│   └───────────────────────────────────────────────────────────────────────────┘    │
│                                      │                                              │
│                    Interface Layer   │   ┌─────────────────┐                        │
│                    (well-defined)    └──▶│  PRODUCT PIPE   │                        │
│                                          │   (7 phases)    │                        │
│                                          └─────────────────┘                        │
│                                                                                     │
│   Inputs:                                                                           │
│   • 4W Worker Outcomes (Swarm/Expert sessions)                                      │
│   • PostgreSQL Plan Scanner outputs (via adapter)                                   │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Key Principles

1. **Independence**: Knowledge Engine runs async from Product Pipeline
2. **Accumulation**: Knowledge compounds over time, never resets
3. **Compression**: Raw data → Patterns → Principles (progressive distillation)
4. **Validation**: All injected knowledge must have provenance
5. **Versioning**: Knowledge evolves; old beliefs can be superseded

---

## 2. System Boundaries & Interface Contract

### 2.1 Interface Points

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              INTERFACE ARCHITECTURE                                 │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│   ┌─────────────────────┐                      ┌─────────────────────┐             │
│   │   PRODUCT PIPELINE  │                      │   KNOWLEDGE ENGINE  │             │
│   │                     │                      │                     │             │
│   │  ┌───────────────┐  │   Interface A      │  ┌───────────────┐  │             │
│   │  │ Phase 2:      │◀─┼────────────────────┼──┤ Knowledge     │  │             │
│   │  │ Knowledge     │  │  (Pull: Engine    │  │ Store         │  │             │
│   │  │ Retrieval     │  │   Profile,        │  │ (Layer 4)     │  │             │
│   │  └───────────────┘  │   Examples,       │  └───────────────┘  │             │
│   │         ▲           │   Constraints)    │         ▲           │             │
│   │         │           │                   │         │           │             │
│   │  ┌──────┴──────┐    │   Interface B     │  ┌──────┴──────┐    │             │
│   │  │ Phase 7:    │────┼───────────────────┼─▶│ Ingestion   │    │             │
│   │  │ Outputs &   │    │  (Push: Outcomes, │  │ (Layer 1)   │    │             │
│   │  │ Learning    │    │   Validation)     │  └─────────────┘    │             │
│   │  └─────────────┘    │                   │                     │             │
│   │                     │                      │                     │             │
│   └─────────────────────┘                      └─────────────────────┘             │
│                                                                                     │
│   Interface A: READ-ONLY  ──▶  Knowledge Engine → Product Pipeline                 │
│   Interface B: WRITE-ONLY ──▶  Product Pipeline → Knowledge Engine                 │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Interface A: Knowledge Retrieval (Engine → Pipeline)

**Trigger**: Phase 2 of Product Pipeline (`Pipeline._find_examples()`)

**Request**:
```python
class KnowledgeQuery:
    """Query from Product Pipeline to Knowledge Engine"""
    query_id: str
    sql_fingerprint: str          # Structural fingerprint
    dialect: str                  # duckdb | postgresql
    available_context: Dict       # What Phase 1 gathered
    context_confidence: str       # high | degraded | heuristic
```

**Response**:
```python
class KnowledgeResponse:
    """Response from Knowledge Engine to Product Pipeline"""
    # Required fields (hard gate if missing)
    matched_examples: List[GoldExample]
    global_knowledge: GlobalKnowledge
    
    # Engine-specific (required for PG, optional for DuckDB)
    scanner_findings: Optional[ScannerFindings]
    exploit_algorithm: Optional[ExploitAlgorithm]
    
    # Profiles
    engine_profile: EngineProfile
    constraints: List[Constraint]
    regression_warnings: List[RegressionWarning]
    
    # Metadata
    knowledge_version: str        # Version of knowledge used
    freshness_score: float        # 0-1, how recent is this knowledge
```

**Contract**:
- Knowledge Engine MUST respond within 500ms (cached)
- Missing required fields = Pipeline intelligence gate failure
- `freshness_score < 0.3` triggers background refresh

### 2.3 Interface B: Outcome Ingestion (Pipeline → Engine)

**Trigger**: Phase 7 of Product Pipeline (`Store.save_candidate()`)

**Payload**:
```python
class OptimizationOutcome:
    """Outcome reported from Product Pipeline to Knowledge Engine"""
    
    # Identity
    query_id: str
    run_id: str
    timestamp: datetime
    
    # Inputs (what was given)
    examples_used: List[str]
    strategies_attempted: List[str]
    engine_profile_version: str
    
    # Outputs (what happened)
    status: str                   # WIN | IMPROVED | NEUTRAL | REGRESSION | ERROR
    speedup: float
    speedup_type: str             # measured | vs_timeout_ceiling
    validation_confidence: str    # high | row_count_only | zero_row_unverified
    
    # Detailed artifacts
    original_sql: str
    optimized_sql: Optional[str]
    transforms_applied: List[str]
    set_local_configs: Dict[str, str]  # PG only
    
    # Reasoning (for learning)
    worker_responses: List[str]
    error_category: Optional[str]
    error_messages: List[str]
    
    # Provenance
    model: str
    provider: str
    git_sha: str
```

**Contract**:
- Pipeline MUST report ALL outcomes (wins AND failures)
- Ingestion is fire-and-forget (async processing)
- Knowledge Engine handles deduplication

---

## 3. Knowledge Engine Internal Architecture

### 3.1 Four-Layer Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           KNOWLEDGE ENGINE LAYERS                                   │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│   LAYER 4: KNOWLEDGE STORE (Curated)                                               │
│   ┌───────────────────────────────────────────────────────────────────────────┐    │
│   │ • Engine Profiles (validated gaps/strengths)                              │    │
│   │ • Gold Examples (promoted winners)                                        │    │
│   │ • Active Constraints (living document)                                    │    │
│   │ • Classification Taxonomy (auto-generated)                                │    │
│   │                                                                           │    │
│   │ Format: JSON (human-editable, version-controlled)                         │    │
│   │ Update: Continuous from Layer 3                                           │    │
│   └───────────────────────────────────────────────────────────────────────────┘    │
│                                      ▲                                              │
│                                      │ DISTILL (compression)                         │
│                                      │                                               │
│   LAYER 3: PATTERN MINE (Aggregated)                                               │
│   ┌───────────────────────────────────────────────────────────────────────────┐    │
│   │ • Optimization Patterns (what works when)                                 │    │
│   │ • Anti-Patterns (verified regressions)                                    │    │
│   │ • Query Archetypes (classification clusters)                              │    │
│   │ • Config Rules (SET LOCAL heuristics - PG only)                           │    │
│   │                                                                           │    │
│   │ Format: Structured JSON (LLM + rule-based)                                │    │
│   │ Update: Periodic batch (hourly/daily)                                     │    │
│   └───────────────────────────────────────────────────────────────────────────┘    │
│                                      ▲                                              │
│                                      │ EXTRACT (pattern mining)                      │
│                                      │                                               │
│   LAYER 2: FINDINGS (Extracted)                                                    │
│   ┌───────────────────────────────────────────────────────────────────────────┐    │
│   │ • 4W Outcome Findings (from Swarm/Expert runs)                            │    │
│   │ • Scanner Findings (PG plan-space - via adapter)                          │    │
│   │ • Error Patterns (across all engines)                                     │    │
│   │ • Counter-Examples (verified regressions)                                 │    │
│   │                                                                           │    │
│   │ Format: JSON (LLM-extracted from Layer 1)                                 │    │
│   │ Update: Per batch/run                                                     │    │
│   │ Sources: build_blackboard.py, scanner_knowledge/findings.py               │    │
│   └───────────────────────────────────────────────────────────────────────────┘    │
│                                      ▲                                              │
│                                      │ INGEST (raw capture)                          │
│                                      │                                               │
│   LAYER 1: OUTCOME STORE (Raw)                                                     │
│   ┌───────────────────────────────────────────────────────────────────────────┐    │
│   │ • 4W Worker Outcomes (Swarm/Expert sessions)                              │    │
│   │ • Scanner Observations (PG - via adapter)                                 │    │
│   │ • Validation Results (benchmark data)                                     │    │
│   │ • Worker Artifacts (prompts, responses, SQL)                              │    │
│   │                                                                           │    │
│   │ Format: JSONL (append-only, partitioned by date)                          │    │
│   │ Update: Real-time from Interface B                                        │    │
│   │ Note: Different from scanner_blackboard.jsonl (which is PG-only)          │    │
│   └───────────────────────────────────────────────────────────────────────────┘    │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Data Flow Between Layers

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           DATA TRANSFORMATIONS                                      │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│   LAYER 1 → LAYER 2: Extraction                                                     │
│   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐                │
│   │  Raw Outcomes   │───▶│  LLM Analysis   │───▶│    Findings     │                │
│   │  (1000s items)  │    │  (pattern mine) │    │  (10s items)    │                │
│   └─────────────────┘    └─────────────────┘    └─────────────────┘                │
│                                                                                     │
│   LAYER 2 → LAYER 3: Distillation                                                   │
│   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐                │
│   │    Findings     │───▶│  Aggregation &  │───▶│    Patterns     │                │
│   │   (10s items)   │    │  Validation     │    │   (10s items)   │                │
│   └─────────────────┘    │  (cross-query)  │    │   (compressed)  │                │
│                          └─────────────────┘    └─────────────────┘                │
│                                                                                     │
│   LAYER 3 → LAYER 4: Promotion                                                      │
│   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐                │
│   │    Patterns     │───▶│   Human Review  │───▶│  Knowledge      │                │
│   │  (auto-gen'd)   │    │   (optional)    │    │  Store          │                │
│   └─────────────────┘    │  + Auto-rules   │    │  (curated)      │                │
│                          └─────────────────┘    └─────────────────┘                │
│                                                                                     │
│   Compression Ratios:                                                               │
│   • Layer 1: 100% (raw data)                                                        │
│   • Layer 2: ~10% (findings extracted)                                              │
│   • Layer 3: ~1% (patterns distilled)                                               │
│   • Layer 4: ~0.1% (knowledge promoted)                                             │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Data Storage Templates

### 4.1 Layer 1: Outcome Store

> ⚠️ **Naming Note**: This is the Knowledge Engine's **Outcome Store** (Layer 1), NOT the PostgreSQL Scanner's "blackboard". 
> 
> • Scanner blackboard: `scanner_blackboard.jsonl` (PG plan-space observations)  
> • Outcome Store: `knowledge_engine/layer1/` (all optimization outcomes)

```yaml
# Storage: knowledge_engine/layer1/{date}/{query_id}/{worker_id}.json
# Format: Append-only, never delete

OptimizationOutcome:
  schema_version: "2.0"
  
  # Base (identity)
  base:
    query_id: "q88"
    dialect: "duckdb"  # or "postgresql"
    fingerprint: "decorrelate_subquery_date_filter"
    timestamp: "2026-02-11T10:05:22Z"
    run_id: "swarm_batch_20260208_102033"
    
  # Optimization context
  opt:
    worker_id: 1
    strategy: "conservative_predicate_pushdown"
    examples_used: ["q6_date_cte", "q11_prefetch"]
    iteration: 0  # 0=initial, 1=snipe, 2=final
    
  # Outcome (measured)
  outcome:
    status: "WIN"
    speedup: 4.5
    speedup_type: "measured"
    validation_confidence: "high"
    original_ms: 1250.0
    optimized_ms: 278.0
    
  # Transforms applied
  transforms:
    primary: "date_cte_isolate"
    all: ["date_cte_isolate", "pushdown"]
    
  # Knowledge extraction (populated by Layer 2)
  principles:
    what_worked: "Isolated date filter into CTE reduced hash join probe table"
    why_it_worked: "Predicate pushdown through CTE boundary blocked; pre-filtering avoids scanning 73K rows"
    principle_id: "CROSS_CTE_PREDICATE_BLINDNESS"
    
  # Config (PG only - from Scanner or SET LOCAL tuning)
  config:
    set_local: {}  # e.g., {"work_mem": "1GB"}
    plan_flags: {}  # e.g., {"enable_nestloop": "off"} - from Scanner
    
  # Error info (if failed)
  error:
    category: null  # syntax | semantic | timeout | execution
    messages: []
    
  # Reasoning trace
  reasons:
    reasoning_chain: "SELECT..."  # Worker response text
    evidence: "EXPLAIN showed..."
    
  # Provenance
  provenance:
    model: "deepseek-reasoner"
    provider: "deepseek"
    git_sha: "abc123"
    reviewed: true
    
  # Source discriminator
  source:
    type: "4w_worker"  # "4w_worker" | "plan_scanner" | "expert_session"
    scanner_config: {}  # Only if type="plan_scanner"
```

### 4.2 Layer 2: Findings Storage

```yaml
# Storage: findings/{engine}/{category}/{finding_id}.json
# Format: LLM-extracted, human-reviewable

ScannerFinding:  # For PG plan-space findings
  schema_version: "2.0"
  
  id: "SF-001"
  category: "join_sensitivity"
  
  # The claim
  claim: "Disabling nested loops causes >4x regression on dim-heavy star queries"
  
  # Evidence
  evidence:
    summary: "8/10 queries with nested loop baseline regress >4x"
    count: 8
    contradicting: 2
    supporting_queries: ["q085", "q091", "q065"]
    
  # Mechanism
  mechanism: "Nested loops exploit dim PK indexes; hash join must full-scan dimension tables"
  
  # Boundaries
  boundaries:
    applies_when: "Baseline uses nested loops for dimension PK lookups"
    does_not_apply_when: "Query has no dimension table lookups"
    
  # Confidence
  confidence: "high"
  confidence_rationale: "Consistent across 8 queries with cost + wall-clock evidence"
  
  # Actionable implication
  implication: "Do NOT restructure joins that eliminate nested loop index lookups on dimension tables"
  
  # Metadata
  extracted_at: "2026-02-11T10:05:22Z"
  blackboard_hash: "sha256:abc..."
  reviewed: false

OptimizationFinding:  # For query optimization findings
  schema_version: "2.0"
  
  id: "OF-001"
  category: "date_cte_isolate"
  
  # The pattern
  pattern: "Pre-filter date dimension into CTE before fact table join"
  
  # Effectiveness
  effectiveness:
    wins: 12
    attempts: 15
    success_rate: 0.80
    avg_speedup: 2.8
    
  # When it works
  conditions:
    query_patterns: ["star_schema", "date_filtered"]
    sql_features: ["date_dim join", "year/month filters"]
    
  # When it doesn't
  anti_conditions:
    - "Query already < 100ms (CTE overhead dominates)"
    - "No selective date filters (> 20% selectivity)"
    
  # Examples
  examples:
    positive: ["q6", "q11", "q63"]
    negative: ["q25", "q31"]
    
  # Mechanism
  mechanism: "CTE materialization creates optimization fence; pre-filtering reduces rows entering hash join"
```

### 4.3 Layer 3: Pattern Storage

```yaml
# Storage: patterns/{engine}/{pattern_type}/{pattern_id}.json
# Format: Aggregated, validated

OptimizationPattern:
  schema_version: "2.0"
  
  id: "PATTERN-DATE-CTE-001"
  name: "Date Dimension CTE Isolation"
  
  # Classification
  classification:
    mechanism: "predicate_pushdown"
    impact_tier: "high"
    pattern: "star_schema_prefetch"
    risk: "moderate"
    exploit_type: "gap_exploit"
    
  # The technique
  technique:
    description: "Pre-filter date_dim into CTE, then join with fact table"
    sql_template: |
      WITH filtered_date AS (
        SELECT d_date_sk 
        FROM date_dim 
        WHERE d_year = 2000
      )
      SELECT ...
      FROM fact_table f
      JOIN filtered_date d ON f.date_sk = d.d_date_sk
      
  # Effectiveness statistics
  stats:
    n_observations: 45
    n_wins: 36
    success_rate: 0.80
    avg_speedup: 2.8
    speedup_range: [1.2, 4.5]
    
  # Applicability
  applicability:
    query_archetypes: ["star_schema_date_filtered"]
    required_features:
      - "date_dim table"
      - "selective date filter (< 20% selectivity)"
      - "hash join on date key"
      
  # Counter-indications
  counter_indications:
    - pattern: "query_baseline_under_100ms"
      reason: "CTE materialization overhead dominates"
      observed_regression: 0.5
      
  # Related
  related_patterns: ["PATTERN-DIM-CTE-001", "PATTERN-MULTI-DATE-001"]
  contradictory_patterns: []
  
  # Source
  source_findings: ["OF-001", "OF-015", "OF-023"]
  last_validated: "2026-02-11"
  
  # Promotion status
  status: "promoted"  # candidate | promoted | deprecated
```

### 4.4 Layer 4: Knowledge Store

```yaml
# Storage: knowledge/{engine}/engine_profile.json
# Storage: knowledge/{engine}/gold_examples/{example_id}.json
# Storage: knowledge/{engine}/constraints/{constraint_id}.json
# Format: Curated, version-controlled

EngineProfile:
  schema_version: "2.0"
  engine: "duckdb"  # or "postgresql"
  version_tested: "1.1+"
  last_updated: "2026-02-11"
  
  # Strengths (what NOT to fight)
  strengths:
    - id: "INTRA_SCAN_PREDICATE_PUSHDOWN"
      summary: "Pushes WHERE filters directly into SEQ_SCAN"
      field_note: "If EXPLAIN shows filter inside scan, don't CTE it"
      source_patterns: ["PATTERN-SCAN-001"]
      
  # Gaps (what to exploit)
  gaps:
    - id: "CROSS_CTE_PREDICATE_BLINDNESS"
      priority: "HIGH"
      what: "Cannot push predicates from outer query into CTE definitions"
      why: "CTEs are planned as independent subplans"
      opportunity: "Move selective predicates INTO CTE definition"
      source_patterns: ["PATTERN-DATE-CTE-001"]
      what_worked:
        - "Q6/Q11: 4.00x — date filter moved into CTE"
        - "Q63: 3.77x — pre-joined filtered dates with fact"
      what_didnt_work:
        - "Q25: 0.50x — query was 31ms baseline, CTE overhead dominated"
      field_notes:
        - "Check EXPLAIN: filter AFTER large scan = opportunity"
        - "Fast queries (<100ms) don't benefit"
        
  # Tuning (engine-specific)
  tuning_intel:
    available: false  # DuckDB has no SET LOCAL equivalent
    mechanism: null
    rules: []
    
GoldExample:
  schema_version: "2.0"
  
  id: "q6_date_cte"
  query_id: "q6"
  dialect: "duckdb"
  
  # Classification
  tags: ["date_cte_isolate", "star_schema", "high_impact"]
  archetype: "star_schema_date_filtered"
  
  # SQL
  original_sql: "SELECT ..."
  optimized_sql: "WITH filtered_date AS ..."
  
  # Outcome
  speedup: 4.0
  validated_at_sf: 10
  validation_confidence: "high"
  
  # Explanation
  explanation:
    what: "Isolated date filter into CTE"
    why: "Reduced hash join probe table from 73K to ~365 rows"
    when: "Query joins date_dim on selective filter with fact table"
    
  # Provenance
  source_run: "swarm_batch_20260208_102033"
  promoted_at: "2026-02-09"
  
  # Status
  status: "active"  # active | deprecated
```

---

## 5. Compression Mechanisms

### 5.1 Progressive Compression Pipeline

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           COMPRESSION PIPELINE                                      │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│   STAGE 1: Temporal Compression (L1 → L1)                                          │
│   ┌───────────────────────────────────────────────────────────────────────────┐    │
│   │ Problem: Raw blackboard grows unbounded                                    │    │
│   │                                                                            │    │
│   │ Solution: Time-based partitioning + rollup                                 │    │
│   │ • Keep last 7 days: Individual entries                                     │    │
│   │ • 7-30 days: Daily rollups (aggregated stats per query/strategy)           │    │
│   │ • 30-90 days: Weekly rollups                                               │    │
│   │ • 90+ days: Archive to cold storage (S3/glacier)                           │    │
│   │                                                                            │    │
│   │ Implementation: blackboard/rollup.py                                       │    │
│   └───────────────────────────────────────────────────────────────────────────┘    │
│                                       │                                             │
│                                       ▼                                             │
│   STAGE 2: Semantic Compression (L1 → L2)                                          │
│   ┌───────────────────────────────────────────────────────────────────────────┐    │
│   │ Problem: Too many raw outcomes to review                                   │    │
│   │                                                                            │    │
│   │ Solution: LLM-extracted findings                                           │    │
│   │ • Group by: query pattern + transform + outcome                            │    │
│   │ • Extract: common success/failure modes                                    │    │
│   │ • Generate: human-readable claims with evidence                            │    │
│   │                                                                            │    │
│   │ Compression: 1000 outcomes → 20 findings (50x)                             │    │
│   │ Implementation: scanner_knowledge/findings.py                              │    │
│   └───────────────────────────────────────────────────────────────────────────┘    │
│                                       │                                             │
│                                       ▼                                             │
│   STAGE 3: Pattern Compression (L2 → L3)                                           │
│   ┌───────────────────────────────────────────────────────────────────────────┐    │
│   │ Problem: Findings are query-specific, not generalizable                    │    │
│   │                                                                            │    │
│   │ Solution: Cross-query pattern aggregation                                  │    │
│   │ • Cluster: similar findings by query archetype                             │    │
│   │ • Validate: success rate across multiple queries                           │    │
│   │ • Abstract: SQL template + conditions                                      │    │
│   │                                                                            │    │
│   │ Compression: 20 findings → 3 patterns (7x)                                 │    │
│   │ Implementation: knowledge/pattern_miner.py (NOT IMPLEMENTED)               │    │
│   └───────────────────────────────────────────────────────────────────────────┘    │
│                                       │                                             │
│                                       ▼                                             │
│   STAGE 4: Knowledge Compression (L3 → L4)                                         │
│   ┌───────────────────────────────────────────────────────────────────────────┐    │
│   │ Problem: Patterns are auto-generated, need curation                        │    │
│   │                                                                            │    │
│   │ Solution: Promotion criteria + human review                                │    │
│   │ • Auto-promote: patterns with >5 wins, >70% success rate                   │    │
│   │ • Human-review: patterns with mixed results                                │    │
│   │ • Deprecate: patterns contradicted by new evidence                         │    │
│   │                                                                            │    │
│   │ Compression: 3 patterns → 1 knowledge item (3x)                            │    │
│   │ Implementation: knowledge/promotion.py (NOT IMPLEMENTED)                   │    │
│   └───────────────────────────────────────────────────────────────────────────┘    │
│                                                                                     │
│   TOTAL COMPRESSION: 1000 raw outcomes → 1 knowledge item (1000x)                  │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Compression Triggers

```python
# knowledge/compression.py

class CompressionTriggers:
    """When to run compression at each stage."""
    
    # Stage 1: Temporal
    TEMPORAL_ROLLUP_DAYS = 7  # Roll up entries older than 7 days
    
    # Stage 2: Extraction (L1 → L2)
    EXTRACTION_MIN_ENTRIES = 50  # Extract when 50+ new entries
    EXTRACTION_MAX_AGE_HOURS = 24  # Or every 24 hours
    
    # Stage 3: Pattern Mining (L2 → L3)
    PATTERN_MIN_FINDINGS = 10  # Mine when 10+ new findings
    PATTERN_CROSS_QUERY_MIN = 3  # Pattern must apply to 3+ queries
    
    # Stage 4: Promotion (L3 → L4)
    PROMOTION_MIN_WINS = 5
    PROMOTION_MIN_SUCCESS_RATE = 0.70
    PROMOTION_AUTO_PROMOTE = True  # Auto-promote if criteria met
```

---

## 6. Interface Implementation

### 6.1 Knowledge Engine API

```python
# knowledge_engine/api.py

from typing import List, Optional, Dict
from dataclasses import dataclass
from datetime import datetime

@dataclass
class KnowledgeQuery:
    query_id: str
    sql_fingerprint: str
    dialect: str  # "duckdb" | "postgresql"
    available_context: Dict
    context_confidence: str

@dataclass  
class KnowledgeResponse:
    matched_examples: List[GoldExample]
    global_knowledge: GlobalKnowledge
    engine_profile: EngineProfile
    constraints: List[Constraint]
    regression_warnings: List[RegressionWarning]
    scanner_findings: Optional[ScannerFindings]  # PG only
    knowledge_version: str
    freshness_score: float

@dataclass
class OptimizationOutcome:
    query_id: str
    run_id: str
    timestamp: datetime
    status: str
    speedup: float
    speedup_type: str
    validation_confidence: str
    transforms_applied: List[str]
    original_sql: str
    optimized_sql: Optional[str]
    worker_responses: List[str]
    error_category: Optional[str]
    model: str
    provider: str

class KnowledgeEngine:
    """
    The Knowledge Engine is a self-learning system that maintains
    a circular lifecycle of knowledge accumulation and refinement.
    
    It operates independently from the Product Pipeline, feeding
    curated knowledge into Phase 2 and ingesting outcomes from Phase 7.
    
    Note: The PostgreSQL Plan Scanner (plan_scanner.py) feeds INTO
    this system via an adapter - it is not part of the Knowledge Engine.
    """
    
    def __init__(self, config: KnowledgeEngineConfig):
        self.config = config
        self.layer1 = OutcomeStore(config.outcomes_path)
        self.layer2 = FindingsLayer(config.findings_path)
        self.layer3 = PatternLayer(config.patterns_path)
        self.layer4 = KnowledgeStoreLayer(config.knowledge_path)
        
    # ── Interface A: Knowledge Retrieval ─────────────────────────────────────
    
    def query(self, query: KnowledgeQuery) -> KnowledgeResponse:
        """
        Interface A: Product Pipeline → Knowledge Engine (READ)
        
        Called by Phase 2: Knowledge Retrieval
        
        Returns curated knowledge for the given query context.
        Must respond within 500ms (all data is pre-computed).
        """
        # 1. Find similar examples from Layer 4
        examples = self.layer4.find_similar_examples(
            fingerprint=query.sql_fingerprint,
            dialect=query.dialect,
            n=5
        )
        
        # 2. Load engine profile from Layer 4
        profile = self.layer4.get_engine_profile(query.dialect)
        
        # 3. Load relevant constraints
        constraints = self.layer4.get_constraints(query.dialect)
        
        # 4. Load regression warnings
        warnings = self.layer4.get_regression_warnings(
            fingerprint=query.sql_fingerprint
        )
        
        # 5. Load scanner findings (PG only)
        findings = None
        if query.dialect == "postgresql":
            findings = self.layer4.get_scanner_findings(query.query_id)
        
        # 6. Calculate freshness
        freshness = self.layer4.get_freshness_score()
        
        # 7. Trigger background refresh if stale
        if freshness < 0.3:
            self._schedule_refresh()
        
        return KnowledgeResponse(
            matched_examples=examples,
            global_knowledge=self.layer4.get_global_knowledge(),
            engine_profile=profile,
            constraints=constraints,
            regression_warnings=warnings,
            scanner_findings=findings,
            knowledge_version=self.layer4.get_version(),
            freshness_score=freshness
        )
    
    # ── Interface B: Outcome Ingestion ───────────────────────────────────────
    
    def ingest(self, outcome: OptimizationOutcome) -> None:
        """
        Interface B: Product Pipeline → Knowledge Engine (WRITE)
        
        Called by Phase 7: Outputs & Learning
        
        Records optimization outcomes for async processing.
        Fire-and-forget; ingestion is fast, processing is background.
        """
        # 1. Write to Layer 1 (outcome store) - immediate
        self.layer1.append(outcome)
        
        # 2. Check if compression triggers met
        self._check_compression_triggers()
    
    # ── Internal: Circular Lifecycle ─────────────────────────────────────────
    
    def _check_compression_triggers(self) -> None:
        """Check if any compression stage should run."""
        
        # Stage 2: Extract findings (from outcomes + scanner)
        if self.layer1.should_extract():
            self._run_extraction()
        
        # Stage 3: Mine patterns
        if self.layer2.should_mine_patterns():
            self._run_pattern_mining()
        
        # Stage 4: Promote knowledge
        if self.layer3.should_promote():
            self._run_promotion()
    
    def _run_extraction(self) -> None:
        """L1 → L2: Extract findings from outcomes AND scanner outputs."""
        # Get 4W outcomes
        new_outcomes = self.layer1.get_unprocessed_entries()
        
        # Get Scanner findings (via adapter)
        scanner_findings = self._load_scanner_findings()
        
        # Extract findings from outcomes
        outcome_findings = self._extract_findings(new_outcomes)
        
        # Merge with scanner findings
        all_findings = self._merge_findings(outcome_findings, scanner_findings)
        
        # Write to Layer 2
        for finding in all_findings:
            self.layer2.store(finding)
        
        self.layer1.mark_processed(new_outcomes)
    
    def _run_pattern_mining(self) -> None:
        """L2 → L3: Mine patterns from findings."""
        recent_findings = self.layer2.get_recent_findings(days=30)
        
        # Aggregate findings by pattern
        patterns = self._mine_patterns(recent_findings)
        
        # Write to Layer 3
        for pattern in patterns:
            self.layer3.store(pattern)
    
    def _run_promotion(self) -> None:
        """L3 → L4: Promote patterns to knowledge store."""
        candidate_patterns = self.layer3.get_promotion_candidates()
        
        for pattern in candidate_patterns:
            if self._should_promote(pattern):
                # Convert pattern to knowledge items
                self._promote_to_layer4(pattern)
    
    def _should_promote(self, pattern: OptimizationPattern) -> bool:
        """Check if pattern meets promotion criteria."""
        return (
            pattern.stats.n_wins >= PROMOTION_MIN_WINS and
            pattern.stats.success_rate >= PROMOTION_MIN_SUCCESS_RATE
        )
```

### 6.2 Product Pipeline Integration

```python
# pipeline.py (existing) - Integration points

class Pipeline:
    def __init__(self, ...):
        # ... existing init ...
        self.knowledge_engine = KnowledgeEngine(config.knowledge_config)
    
    # Phase 2: Knowledge Retrieval (Interface A)
    def _find_examples(self, sql: str, dialect: str) -> KnowledgeResponse:
        """Query Knowledge Engine for relevant examples and context."""
        
        query = KnowledgeQuery(
            query_id=self.query_id,
            sql_fingerprint=compute_fingerprint(sql),
            dialect=dialect,
            available_context=self.context,
            context_confidence=self.context_confidence
        )
        
        # Interface A: READ from Knowledge Engine
        response = self.knowledge_engine.query(query)
        
        # Intelligence gate check
        if not response.matched_examples:
            raise IntelligenceGateError("No matched examples")
        if not response.global_knowledge:
            raise IntelligenceGateError("No global knowledge")
        
        return response
    
    # Phase 7: Outputs (Interface B)
    def _save_learning(self, result: ValidationResult) -> None:
        """Report outcome to Knowledge Engine."""
        
        outcome = OptimizationOutcome(
            query_id=self.query_id,
            run_id=self.run_id,
            timestamp=datetime.utcnow(),
            status=result.status,
            speedup=result.speedup,
            speedup_type=result.speedup_type,
            validation_confidence=result.validation_confidence,
            transforms_applied=result.transforms,
            original_sql=self.original_sql,
            optimized_sql=result.optimized_sql,
            worker_responses=self.worker_responses,
            error_category=result.error_category,
            model=self.config.model,
            provider=self.config.provider
        )
        
        # Interface B: WRITE to Knowledge Engine (fire-and-forget)
        self.knowledge_engine.ingest(outcome)
```

---

## 7. Implementation Roadmap

### Phase 1: Foundation (Week 1)
- [ ] Create `knowledge_engine/` module structure
- [ ] Implement Layer 1 (Blackboard) with unified schema
- [ ] Define Interface A and B contracts
- [ ] Implement `KnowledgeEngine.query()` (read path)
- [ ] Implement `KnowledgeEngine.ingest()` (write path)

### Phase 2: Extraction (Week 2)
- [ ] Implement Layer 2 (Findings) extraction
- [ ] Port existing `scanner_knowledge/` to new schema
- [ ] Create findings → pattern bridge
- [ ] Implement Stage 2 compression (LLM extraction)

### Phase 3: Pattern Mining (Week 3)
- [ ] Implement Layer 3 (Pattern Mine)
- [ ] Create pattern aggregation logic
- [ ] Implement cross-query validation
- [ ] Implement Stage 3 compression (pattern mining)

### Phase 4: Promotion (Week 4)
- [ ] Implement Layer 4 (Knowledge Store)
- [ ] Create promotion criteria and rules
- [ ] Implement auto-promotion pipeline
- [ ] Create human review workflow (optional)

### Phase 5: Integration (Week 5)
- [ ] Integrate with Product Pipeline Phase 2
- [ ] Integrate with Product Pipeline Phase 7
- [ ] Add freshness checking and background refresh
- [ ] End-to-end testing

---

## 8. File Structure

```
qt_sql/
├── knowledge_engine/              # NEW: Knowledge Engine module
│   ├── __init__.py
│   ├── api.py                     # Interface A & B
│   ├── config.py
│   │
│   ├── layer1/                    # Blackboard (Raw)
│   │   ├── __init__.py
│   │   ├── blackboard.py          # Storage & retrieval
│   │   ├── schema.py              # BlackboardEntry schema
│   │   └── rollup.py              # Temporal compression
│   │
│   ├── layer2/                    # Findings (Extracted)
│   │   ├── __init__.py
│   │   ├── findings.py            # LLM extraction
│   │   ├── schema.py              # Finding schemas
│   │   └── scanner/               # PG scanner findings
│   │       ├── __init__.py
│   │       ├── blackboard.py
│   │       └── findings.py
│   │
│   ├── layer3/                    # Patterns (Distilled)
│   │   ├── __init__.py
│   │   ├── miner.py               # Pattern mining
│   │   ├── schema.py              # Pattern schemas
│   │   └── validation.py          # Cross-query validation
│   │
│   ├── layer4/                    # Knowledge Store (Curated)
│   │   ├── __init__.py
│   │   ├── store.py               # Knowledge retrieval
│   │   ├── schema.py              # Profile/Example schemas
│   │   ├── promotion.py           # L3 → L4 promotion
│   │   └── similarity.py          # Example matching
│   │
│   └── compression/               # Compression mechanisms
│       ├── __init__.py
│       ├── triggers.py
│       └── pipeline.py
│
├── docs/
│   ├── PRODUCT_CONTRACT.md        # Existing: Pipeline phases
│   └── KNOWLEDGE_ENGINE_DESIGN.md # NEW: This document
│
└── specs/                         # NEW: JSON Schema specs
    ├── blackboard_entry.schema.json
    ├── scanner_finding.schema.json
    ├── optimization_pattern.schema.json
    ├── engine_profile.schema.json
    └── gold_example.schema.json
```

---

## 9. Summary

The Knowledge Engine is a **separate, circular learning system** that:

1. **Ingests** optimization outcomes from the Product Pipeline (Interface B)
2. **Extracts** patterns through LLM analysis (Layer 1 → 2)
3. **Distills** validated patterns across queries (Layer 2 → 3)
4. **Promotes** curated knowledge to the store (Layer 3 → 4)
5. **Feeds** knowledge back to the Product Pipeline (Interface A)

The interface between the two systems is **clean and minimal**:
- **Interface A**: `KnowledgeEngine.query()` → returns curated knowledge
- **Interface B**: `KnowledgeEngine.ingest()` ← receives outcomes

This separation allows:
- The Product Pipeline to remain linear and deterministic
- The Knowledge Engine to learn asynchronously and continuously
- Clear boundaries for testing, versioning, and evolution

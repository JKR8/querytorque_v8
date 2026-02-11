# Naming Clarification: Scanner vs Knowledge Engine

## ⚠️ Important Distinction

The term **"Scanner"** is ALREADY USED in the codebase for a PostgreSQL-specific tool. We must preserve this meaning.

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           TERMINOLOGY MAP                                           │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│   EXISTING (Keep These Names)                                                       │
│   ═══════════════════════════                                                       │
│                                                                                     │
│   "Plan Scanner"          PostgreSQL plan-space exploration tool                    │
│   ├── plan_scanner.py     Three-layer scanner (hint, explore, knowledge)            │
│   ├── plan_explore/       Output: Cost-based plan exploration                       │
│   ├── plan_scanner/       Output: Wall-clock benchmarked plans                      │
│   └── scanner_knowledge/  Knowledge extraction FROM scanner outputs                 │
│       ├── blackboard.py   Populate FROM scanner outputs                             │
│       ├── findings.py     Extract findings FROM scanner blackboard                  │
│       └── schemas.py      ScannerObservation, ScannerFinding                        │
│                                                                                     │
│   NEW (Knowledge Engine)                                                            │
│   ══════════════════════                                                            │
│                                                                                     │
│   "Knowledge Engine"      Circular learning system (DuckDB + PostgreSQL)            │
│   ├── layer1/blackboard/  ALL optimization outcomes (not just scanner!)             │
│   ├── layer2/findings/    Extracted patterns (includes scanner + 4W outcomes)       │
│   ├── layer3/patterns/    Cross-query aggregation                                   │
│   └── layer4/store/       Curated knowledge (profiles, examples)                    │
│                                                                                     │
│   "Blackboard"            Generic term for raw outcome storage                      │
│   ├── In Knowledge Engine Layer 1: All outcomes                                     │
│   └── In Scanner Knowledge: PG SET LOCAL observations only                          │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Relationship Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    PRODUCT PIPELINE (7 Phases)                                      │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│   Phase 1: Context Gathering                                                        │
│   ┌─────────────────────────────────────────────────────────────────────────┐      │
│   │                                                                         │      │
│   │   ┌──────────────┐         ┌──────────────┐                            │      │
│   │   │   DagParser  │         │ PlanAnalyzer │                            │      │
│   │   │   (always)   │         │   (always)   │                            │      │
│   │   └──────────────┘         └──────────────┘                            │      │
│   │          │                        │                                    │      │
│   │          │                        │                                    │      │
│   │   ┌──────┴──────┐                │         ┌──────────────────────┐   │      │
│   │   │  PGTuning   │                │         │   PLAN SCANNER       │   │      │
│   │   │  (PG only)  │                └────────▶│   (PostgreSQL ONLY)  │   │      │
│   │   └─────────────┘                          │                      │   │      │
│   │                                            │ • plan_scanner.py    │   │      │
│   │                                            │ • plan_explore/      │   │      │
│   │                                            │ • plan_scanner/      │   │      │
│   │                                            │ • SET LOCAL configs  │   │      │
│   │                                            └──────────────────────┘   │      │
│   │                                                                         │      │
│   └─────────────────────────────────────────────────────────────────────────┘      │
│                                    │                                                │
│                                    ▼                                                │
│   Phase 2: Knowledge Retrieval                                                    │
│   ┌─────────────────────────────────────────────────────────────────────────┐      │
│   │                                                                         │      │
│   │   ┌─────────────────┐      ┌─────────────────┐      ┌──────────────┐   │      │
│   │   │   TagRecommender│◀────▶│  Engine Profile │      │ScannerOutput │   │      │
│   │   │   (examples)    │      │   (strengths/   │      │  (PG only)   │   │      │
│   │   └─────────────────┘      │     gaps)       │      └──────────────┘   │      │
│   │                            └─────────────────┘                           │      │
│   │                                                                         │      │
│   │   ▲                                                                 ▲   │      │
│   │   │                                                                 │   │      │
│   │   │ Interface A (READ)                                      Source │   │      │
│   │   │                                                                 │   │      │
│   └───┼─────────────────────────────────────────────────────────────────┼───┘      │
│       │                                                                 │            │
│       │                   KNOWLEDGE ENGINE                              │            │
│       │           ┌───────────────────────────────┐                     │            │
│       │           │  Layer 4: Knowledge Store     │                     │            │
│       │           │  ┌─────────────────────────┐  │                     │            │
│       │           │  │ • Engine Profiles       │  │─────────────────────┘            │
│       │           │  │ • Gold Examples         │  │                                │
│       │           │  │ • Constraints           │  │                                │
│       └───────────┤  │ • Scanner Findings (PG) │  │                                │
│                   │  └─────────────────────────┘  │                                │
│                   │           ▲                   │                                │
│                   │           │ Promotion         │                                │
│                   │  Layer 3: Pattern Mine        │                                │
│                   │  ┌─────────────────────────┐  │                                │
│                   │  │ • Optimization Patterns │  │                                │
│                   │  │ • Anti-Patterns         │  │                                │
│                   │  └─────────────────────────┘  │                                │
│                   │           ▲                   │                                │
│                   │           │ Extraction        │                                │
│                   │  Layer 2: Findings            │                                │
│                   │  ┌─────────────────────────┐  │                                │
│                   │  │ • 4W Outcomes Findings  │  │                                │
│                   │  │ • Scanner Findings (PG) │──┘                                │
│                   │  │ • Error Patterns        │                                   │
│                   │  └─────────────────────────┘                                   │
│                   │           ▲                                                    │
│                   │           │ Ingestion                                           │
│                   │  Layer 1: Blackboard                                            │
│                   │  ┌─────────────────────────┐                                   │
│                   │  │ • 4W Worker Outcomes    │                                   │
│                   │  │ • Scanner Outputs (PG)  │                                   │
│                   │  │ • Validation Results    │                                   │
│                   │  └─────────────────────────┘                                   │
│                   │                                                                │
│                   └────────────────────────────────────────────────────────────────┘
│                                    ▲
│                                    │ Interface B (WRITE)
│
│   Phase 7: Outputs & Learning
│   ┌─────────────────────────────────────────────────────────────────────────┐
│   │  Outputs  ──────▶  Leaderboard, Artifacts, Learning Records             │
│   │       │                                                                 │
│   │       └──────────────────▶  Knowledge Engine (ingest outcome)           │
│   └─────────────────────────────────────────────────────────────────────────┘
│
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow: Scanner vs 4W Outcomes

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                    TWO INPUT STREAMS TO KNOWLEDGE ENGINE                            │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                     │
│   STREAM 1: PostgreSQL Plan Scanner (PG only)                                       │
│   ═══════════════════════════════════════════                                       │
│                                                                                     │
│   plan_scanner.py                                                                   │
│        │                                                                            │
│        ├──▶ plan_explore/          (cost-based plan differences)                   │
│        │                                                                            │
│        └──▶ plan_scanner/          (wall-clock validated plans)                     │
│                    │                                                                │
│                    ▼                                                                │
│            scanner_knowledge/blackboard.py                                          │
│                    │                                                                │
│                    ├──▶ scanner_blackboard.jsonl    (ScannerObservation)            │
│                    │                                                                │
│                    ▼                                                                │
│            scanner_knowledge/findings.py                                            │
│                    │                                                                │
│                    └──▶ scanner_findings.json       (ScannerFinding)                │
│                                │                                                    │
│                                ▼                                                    │
│                    KNOWLEDGE ENGINE Layer 2                                         │
│                    (findings from scanner MERGED with 4W findings)                  │
│                                                                                     │
│   STREAM 2: 4W Optimization Outcomes (All engines)                                  │
│   ═════════════════════════════════════════════════                                 │
│                                                                                     │
│   SwarmSession / ExpertSession                                                      │
│        │                                                                            │
│        └──▶ build_blackboard.py                                                     │
│                    │                                                                │
│                    ├──▶ blackboard/raw/                                             │
│                    │       ├── worker_01.json                                       │
│                    │       ├── worker_02.json                                       │
│                    │       └── ...                                                  │
│                    │                                                                │
│                    └──▶ blackboard/collated.json                                    │
│                            ├── principles                                           │
│                            └── anti_patterns                                        │
│                                │                                                    │
│                                ▼                                                    │
│                    KNOWLEDGE ENGINE Layer 2                                         │
│                    (findings from 4W outcomes)                                      │
│                                                                                     │
│   MERGE POINT (Layer 2):                                                            │
│   ┌─────────────────────────────────────────────────────────────────────────┐      │
│   │  Scanner Findings (PG only)  +  4W Outcome Findings (All engines)       │      │
│   │                          ↓                                              │      │
│   │              Layer 3: Pattern Mining (unified)                          │      │
│   └─────────────────────────────────────────────────────────────────────────┘      │
│                                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Corrected Naming for Knowledge Engine

To avoid confusion with the existing Scanner:

### Layer 1: Outcome Store (was: Blackboard)
**Recommended name change**: Use **"Outcome Store"** or **"Raw Store"** instead of just "Blackboard" to distinguish from scanner blackboard.

```python
# AVOID this naming:
class BlackboardLayer:  # Confusing - which blackboard?
    pass

# USE this naming:
class OutcomeStore:  # Clear - stores all outcomes
    pass

# Or:
class OptimizationOutcomes:  # Explicit
    pass
```

### Layer 2: Findings Store
**Keep as**: **Findings** - this is generic enough and the Scanner already uses "ScannerFinding".

```python
# Scanner uses:
class ScannerFinding:  # PG plan-space specific
    pass

# Knowledge Engine uses:
class OptimizationFinding:  # Generic optimization outcome finding
    pass
    
class ErrorPattern:  # Error learning
    pass
```

### Relationship to Scanner

```python
# In Knowledge Engine Layer 2, we MERGE from both sources:

class FindingsLayer:
    """Layer 2: Extracted findings from all sources."""
    
    def extract(self, source: str):
        if source == "scanner":
            # Read FROM scanner_knowledge/scanner_findings.json
            return self._load_scanner_findings()
        elif source == "4w_outcomes":
            # Read FROM build_blackboard/collated.json
            return self._extract_from_outcomes()
    
    def merge_findings(self):
        """Merge scanner findings with 4W outcome findings."""
        scanner = self.extract("scanner")  # PG only
        outcomes = self.extract("4w_outcomes")  # All engines
        return self._deduplicate_and_merge(scanner, outcomes)
```

---

## Updated File Structure (Clarified Names)

```
qt_sql/
│
├── plan_scanner.py                 # EXISTING: PostgreSQL plan-space scanner
├── scanner_knowledge/              # EXISTING: PG scanner knowledge extraction
│   ├── blackboard.py               # Populate FROM scanner outputs
│   ├── findings.py                 # Extract FROM scanner blackboard
│   ├── schemas.py                  # ScannerObservation, ScannerFinding
│   └── templates/
│       └── findings_prompt.md
│
├── build_blackboard.py             # EXISTING: 4W outcome collation
│                                   # (generates collated.json from swarm batches)
│
├── knowledge_engine/               # NEW: Unified learning system
│   ├── api.py                      # Interface A (query) & B (ingest)
│   │
│   ├── layer1_outcomes/            # Was: layer1/blackboard
│   │   ├── store.py                # Raw outcome storage
│   │   ├── schema.py               # OptimizationOutcome schema
│   │   └── scanner_adapter.py      # Adapter: scanner → outcome store
│   │
│   ├── layer2_findings/            # Findings extraction
│   │   ├── extractor.py            # Extract from outcomes
│   │   ├── schema.py               # Finding schemas
│   │   └── merger.py               # Merge scanner + 4W findings
│   │
│   ├── layer3_patterns/            # Pattern mining
│   │   ├── miner.py
│   │   └── schema.py
│   │
│   └── layer4_knowledge/           # Curated knowledge
│       ├── store.py
│       ├── promotion.py
│       └── schema.py
│
└── specs/
    ├── optimization_outcome.schema.json      # Was: blackboard_entry
    ├── scanner_finding.schema.json           # EXISTING concept
    ├── optimization_finding.schema.json      # NEW: for 4W outcomes
    ├── optimization_pattern.schema.json
    ├── engine_profile.schema.json
    └── gold_example.schema.json
```

---

## Key Clarifications

| Term | What It Is | Engine | Existing or New |
|------|-----------|--------|-----------------|
| **Plan Scanner** | Tool that explores SET LOCAL configs | PostgreSQL only | ✅ Existing |
| **Scanner Blackboard** | Raw SET LOCAL observations | PostgreSQL only | ✅ Existing |
| **Scanner Finding** | Extracted insight from scanner | PostgreSQL only | ✅ Existing |
| **4W Outcomes** | Worker optimization results | All engines | ✅ Existing |
| **Outcome Store** | Unified raw storage (L1) | All engines | 🆕 New (KE) |
| **Optimization Finding** | Extracted insight from 4W | All engines | 🆕 New (KE) |
| **Pattern** | Cross-query aggregation | All engines | 🆕 New (KE) |

---

## Important: Scanner Stays Independent

The **Plan Scanner** remains a **Phase 1 tool** that feeds into:

1. **Product Pipeline Phase 2** (direct scanner_findings → prompt)
2. **Knowledge Engine Layer 2** (via adapter)

```
┌─────────────────────────────────────────────────────────────────┐
│                    SCANNER DATA FLOW                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   plan_scanner.py                                               │
│        │                                                        │
│        ├── Direct Path ──▶ Phase 2 Prompt (immediate use)       │
│        │                                                        │
│        └── Learning Path ──▶ scanner_blackboard.jsonl           │
│                     │                                           │
│                     ├──▶ scanner_findings.json (extracted)      │
│                     │           │                               │
│                     │           └──▶ Phase 2 Prompt             │
│                     │                                           │
│                     └──▶ Knowledge Engine                       │
│                                 │                               │
│                                 └── Layer 2: Findings Store     │
│                                         (merged with 4W)        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Summary of Name Changes Needed

| Original Name | Problem | Better Name |
|--------------|---------|-------------|
| `BlackboardEntry` (in KE) | Confused with scanner blackboard | `OptimizationOutcome` |
| `layer1/blackboard/` | Ambiguous | `layer1_outcomes/` |
| `BlackboardLayer` | Ambiguous | `OutcomeStore` or `Layer1Store` |
| `ScannerFinding` (in KE) | Same name as scanner module | Keep distinct, use fully qualified |

**Scanner-related names should be reserved for the PostgreSQL plan scanner only.**

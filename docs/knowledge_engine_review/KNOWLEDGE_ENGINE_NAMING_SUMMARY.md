# Knowledge Engine Naming Summary

## Clarification Made

The **Knowledge Engine** is **distinct** from the **PostgreSQL Plan Scanner**.

### Existing: Plan Scanner (PostgreSQL Only)
```
plan_scanner.py
    ├── plan_explore/         (cost-based exploration)
    ├── plan_scanner/         (wall-clock benchmarks)
    └── scanner_knowledge/    (findings extraction)
        ├── blackboard.py     (scanner_blackboard.jsonl)
        ├── findings.py       (scanner_findings.json)
        └── schemas.py        (ScannerObservation, ScannerFinding)
```

### New: Knowledge Engine (All Engines)
```
knowledge_engine/
    ├── layer1_outcomes/      (was: layer1/blackboard)
    │   ├── store.py          (all outcomes: 4W + scanner adapted)
    │   └── schema.py         (OptimizationOutcome)
    ├── layer2_findings/      (merged: 4W findings + scanner findings)
    ├── layer3_patterns/      (cross-query patterns)
    └── layer4_knowledge/     (curated: profiles, examples)
```

---

## Name Changes Applied

| Old Name | New Name | Reason |
|----------|----------|--------|
| `BlackboardEntry` | `OptimizationOutcome` | Avoid confusion with scanner blackboard |
| `layer1/blackboard/` | `layer1_outcomes/` | Clear distinction |
| `blackboard_entry.schema.json` | `optimization_outcome.schema.json` | Consistent naming |
| `BlackboardLayer` | `OutcomeStore` | Clear purpose |

---

## Data Flow

```
PostgreSQL Plan Scanner         Knowledge Engine
        │                             │
        │ ScannerObservation          │
        ▼                             ▼
scanner_blackboard.jsonl  ──▶  Layer 1 Outcome Store
(scanner-specific)            (unified, all engines)
                                      │
        4W Worker Outcomes ───────────┤
        (Swarm/Expert)                │
                                      ▼
                               Layer 2 Findings
                               (merged sources)
                                      │
                                      ▼
                               Layer 3 Patterns
                                      │
                                      ▼
                               Layer 4 Knowledge
                               (injected into Pipeline)
```

---

## Key Distinction

| Aspect | Plan Scanner | Knowledge Engine |
|--------|-------------|------------------|
| **Purpose** | Explore SET LOCAL configs | Learn from optimization outcomes |
| **Engine** | PostgreSQL only | DuckDB + PostgreSQL |
| **Phase** | Phase 1 (Context) | Feeds Phase 2, fed by Phase 7 |
| **Output** | Plan observations | Curated knowledge |
| **Relationship** | Feeds INTO Knowledge Engine | Central learning system |

---

## Documents Updated

1. **KNOWLEDGE_ENGINE_DESIGN.md** - Renamed all "blackboard" to "outcome store"
2. **KNOWLEDGE_ENGINE_ARCHITECTURE_OVERVIEW.md** - Updated schema references
3. **NAMING_CLARIFICATION.md** - Created detailed naming map
4. **optimization_outcome.schema.json** - Renamed from blackboard_entry

---

## Scanner Stays Independent

The Plan Scanner (`plan_scanner.py`) remains a **Phase 1 tool** that:
1. Directly feeds Phase 2 prompts (immediate use)
2. Adapts into Knowledge Engine Layer 1 (learning)

The Scanner is **not replaced** by the Knowledge Engine - it is **one input source** to it.

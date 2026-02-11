# Knowledge Engine: Two-System Architecture

> **Status**: Target state specification
> **Scope**: Knowledge Engine only — the Product Pipeline is stable and unchanged
> **Key change from v1 spec**: Engine profile is fully human-authored. No autonomous compression pipeline.

---

## Two Systems

QueryTorque consists of two cleanly separated systems that connect at exactly two interfaces.

### System 1: Product Pipeline (Execution)

A linear, stateless, 7-phase per-query optimizer. Takes SQL in, produces optimized SQL out.

| Phase | Name | Key Files | Notes |
|-------|------|-----------|-------|
| 1 | Context Gathering | `plan_analyzer.py`, `pg_tuning.py`, `plan_scanner.py` | EXPLAIN, plan scanning, system profiling |
| 2 | Knowledge Retrieval | `pipeline.py:gather_analyst_context()` | **Interface A** — KE serves knowledge here |
| 3 | Prompt Generation | `analyst_briefing.py`, `worker.py` | Token-budgeted prompts, per-worker examples |
| 4 | LLM Inference | `generate.py:CandidateGenerator` | 4 parallel workers + analyst |
| 5 | Response Processing | `sql_rewriter.py`, DAP/JSON parsing | sqlglot AST validation |
| 6 | Validation | `validate.py`, per-engine equivalence | 5-run trimmed mean, checksum (DuckDB) |
| 7 | Outputs & Learning | `store.py`, `learn.py` | **Interface B** — KE captures outcomes here |

**This system exists, works, and is not changed by the Knowledge Engine spec.**

### System 2: Knowledge Engine (Reasoning — Human-Driven)

A human-operated learning system. The pipeline captures outcomes automatically; a human reviews them, reasons about patterns, and maintains the knowledge artifacts.

**Components:**

| Component | Purpose | How It Works |
|-----------|---------|--------------|
| **Blackboard** | Append-only outcome log | Auto-captured by pipeline. JSONL per batch. Input to human analysis. |
| **Analysis Sessions** | Structured human reasoning | You review blackboard, fill in analysis session form, produce findings. |
| **Engine Profile** | Optimizer intelligence for LLM | Markdown document you author. Injected into analyst prompt as-is. |
| **Gold Examples** | Few-shot learning material | JSON files you curate. Matched to queries via tag overlap + gap scoring. |
| **Detection Rules** | Machine-readable gap predicates | You author, code validates against feature vocabulary. |

**No LLM in the knowledge engine.** The pipeline uses LLMs for optimization. The knowledge engine is you + schemas + templates.

---

## The Two Interfaces

### Interface A (Read): Pipeline Phase 2 Pulls Knowledge

**Where**: `pipeline.py:1537` — `gather_analyst_context()`

Returns a dict with 13 keys. The engine profile is the critical one — it's injected directly into the analyst prompt at `analyst_briefing.py:996-1064`.

| Source | Call | File |
|--------|------|------|
| Gold examples | `_find_examples(sql, engine, k=20)` via `TagRecommender` | `knowledge.py:31` |
| Engine profile | `_load_engine_profile(dialect)` | `constraints/engine_profile_{dialect}.json` |
| Constraints | `_load_constraint_files(dialect)` | `prompter.py` |
| Global knowledge | `load_global_knowledge()` | `benchmarks/<name>/knowledge/*.json` |
| Regressions | `_find_regression_warnings(sql, engine, k=3)` | `knowledge.py` |

**Target state**: Same interface. Engine profile transitions from JSON → markdown (loaded and injected as text). Gold examples gain `demonstrates_gaps[]` for gap-weighted scoring. No pipeline code changes.

### Interface B (Write): Pipeline Phase 7 Pushes Outcomes

**Where**: Phase 7 stores results via `store.py:save_candidate()` and `learn.py:Learner`.

**Currently captured**: status, speedup, transforms, errors, examples_used, SET LOCAL config.
**Not captured**: EXPLAIN evidence, worker reasoning (on disk only), knowledge version used, structural SQL features.

**Target state**: Extend `BlackboardEntry` to capture the missing fields. Still fire-and-forget — the blackboard is a log, not a queue. You review it when you're ready.

---

## Artifact Map

```
Knowledge Engine
├── Blackboard (Outcome Log)              → 02_BLACKBOARD.md
│   ├── BlackboardEntry schema
│   ├── 3-phase extraction pipeline
│   └── JSONL append-only storage
│
├── Knowledge Atoms                       → 01_KNOWLEDGE_ATOM.md
│   ├── 5-component structure
│   └── What makes an observation replicable
│
├── Analysis Sessions & Findings          → 06_MANUAL_WORKFLOW.md
│   ├── Analysis session form
│   ├── Finding schema
│   └── Human reasoning workflow
│
├── Engine Profiles                       → 03_ENGINE_PROFILE.md
│   ├── Markdown-native format (what you write = what LLM reads)
│   ├── Schema validates structure
│   └── Token-efficient design
│
├── Gold Examples                         → 04_GOLD_EXAMPLES.md
│   ├── 4-part explanation (what/why/when/when_not)
│   ├── Manual promotion criteria
│   └── Gap-weighted matching
│
├── Detection & Matching                  → 05_DETECTION_AND_MATCHING.md
│   ├── Feature vocabulary (~25 features)
│   ├── Detection rule predicates
│   └── Gap-weighted example scoring
│
├── Schemas                               → 07_SCHEMAS.md
│   ├── BlackboardEntry
│   ├── AnalysisSession
│   ├── Finding
│   ├── EngineProfile (markdown structure)
│   ├── GoldExample
│   ├── DetectionRule
│   └── FeatureVector
│
└── Templates                             → templates/
    ├── analysis_session.md               ← The reasoning form
    ├── finding.md                        ← Standalone finding
    ├── gold_example_template.json        ← Promotion template
    └── engine_profile_template.md        ← Profile structure
```

---

## What Stays Unchanged

These are Pipeline components. They work. They are not part of the KE spec.

| Component | File | Why Unchanged |
|-----------|------|---------------|
| Analyst prompt builder | `prompts/analyst_briefing.py` | Prompt architecture is Pipeline |
| Worker prompt builder | `prompts/worker.py` | Worker sections [1]-[8] are Pipeline |
| Swarm orchestration | `sessions/swarm_session.py` | Fan-out/validate loop is Pipeline |
| SQL rewriter | `sql_rewriter.py` | Response parsing is Pipeline |
| Validation | `validate.py` | Timing + equivalence is Pipeline |
| Candidate generation | `generate.py` | LLM inference is Pipeline |
| Per-worker SET LOCAL | `worker.py:[7b]`, `sql_rewriter.py:_split_set_local()` | Tuning is Pipeline |

---

## Data Flow

```
Pipeline runs → BlackboardEntry (auto-captured JSONL)
                    ↓
You review blackboard batch
                    ↓
You fill in Analysis Session form (templates/analysis_session.md)
  → Findings (observations about optimizer behavior)
  → Proposed actions (profile updates, gold example promotions)
                    ↓
You edit engine profile markdown (constraints/engine_profile_{dialect}.md)
  → Validate: qt validate-profile {dialect}
                    ↓
You promote gold examples (qt_sql/examples/{dialect}/*.json)
  → Validate: qt validate-example {id}
                    ↓
Next pipeline run reads updated knowledge via Interface A
```

Every step is human-initiated. The pipeline is the only thing that runs autonomously — and even it doesn't touch the knowledge store.

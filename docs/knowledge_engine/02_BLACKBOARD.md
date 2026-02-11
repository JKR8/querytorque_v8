# Blackboard: The Outcome Log

> **Status**: Target state specification
> **Extends**: `build_blackboard.py` (`BlackboardEntry`, `GlobalBlackboardQuery`, `SourceAttempt`, 3-phase pipeline)
> **Role**: Auto-captured input to human analysis. The blackboard does NOT contain intelligence — it contains raw data.

---

## Design Principle

The blackboard is an **append-only log of what happened**. Every optimization attempt produces one entry. Everything else — findings, engine profiles, gold examples — comes from your analysis of this log.

One entry per outcome. No deduplication. No intelligence layer. The blackboard is the factual record.

---

## Current Schema: BlackboardEntry

From `build_blackboard.py:51-128`:

```python
@dataclass
class BlackboardEntry:
    query_id: str
    worker_id: int
    run_name: str
    timestamp: str
    query_intent: str = ""
    query_fingerprint: str = ""
    examples_used: List[str]
    strategy: str = ""
    status: str = ""                          # WIN | IMPROVED | NEUTRAL | REGRESSION | ERROR | FAIL
    speedup: float = 0.0
    transforms_applied: List[str]
    error_category: Optional[str] = None      # syntax | semantic | timeout | execution | unknown
    error_messages: List[str]
    what_worked: Optional[str] = None
    why_it_worked: Optional[str] = None
    what_failed: Optional[str] = None
    why_it_failed: Optional[str] = None
    principle: Optional[str] = None
    reviewed: bool = False
```

## Target Schema: Extended BlackboardEntry

Extends the existing schema with missing data points. New fields marked `[NEW]`. See `07_SCHEMAS.md` for the full JSON Schema definition.

```json
{
  "id": "q9_w2_20260211",
  "source": {
    "type": "4w_worker"
  },

  "base": {
    "query_id": "q9",
    "engine": "duckdb",
    "benchmark": "tpcds",
    "original_sql": "SELECT ... FROM store_sales, reason, time_dim ...",
    "fingerprint": "star_schema_conditional_aggregation_multi_scan",
    "timestamp": "2026-02-11T10:05:22Z",
    "run_id": "swarm_batch_20260211_100500"
  },

  "opt": {
    "approach": "4w_worker",
    "worker_id": 2,
    "strategy": "aggressive_scan_consolidation",
    "iteration": 0,
    "optimized_sql": "WITH ... SELECT SUM(CASE WHEN ...) ...",
    "examples_used": ["q88_channel_bitmap", "q9_single_pass"],
    "engine_profile_version": "2026.02.11-v3"
  },

  "outcome": {
    "status": "WIN",
    "tier": "CRITICAL_HIT",
    "speedup": 4.47,
    "speedup_type": "measured",
    "timing": {
      "original_ms": 3200.0,
      "optimized_ms": 716.0
    },
    "validation": {
      "confidence": "high",
      "rows_match": true,
      "checksum_match": true
    },
    "error": null
  },

  "transforms": {
    "primary": "single_pass_aggregation",
    "all": ["single_pass_aggregation", "dimension_cte_isolate"]
  },

  "config": {
    "settings": {},
    "impact_additive": null
  },

  "error": null,

  "reasons": {
    "reasoning_chain": "Worker identified 8 correlated subqueries each scanning store_sales...",
    "evidence": "EXPLAIN ANALYZE shows 8 x 28.7M row scans eliminated to 1 x 28.7M"
  },

  "tags": ["single_pass_aggregation", "star_schema", "scan_consolidation", "critical_hit"],

  "provenance": {
    "model": "deepseek-reasoner",
    "provider": "deepseek",
    "git_sha": "a1b2c3d",
    "knowledge_version_used": "2026.02.11-v3"
  },

  "version": {
    "schema_version": "2.0",
    "entry_version": 1,
    "status": "active"
  }
}
```

### What Changed

| Current Field | Target Field | Change |
|---------------|-------------|--------|
| `query_fingerprint` (string) | `base.fingerprint` | Renamed, same content |
| `query_intent` (usually empty) | Dropped from blackboard | You capture intent in analysis sessions, not auto-extraction |
| — | `outcome.tier` | **[NEW]** WIN / CRITICAL_HIT classification |
| — | `outcome.speedup_type` | **[NEW]** measured / vs_timeout_ceiling / both_timeout |
| — | `provenance.knowledge_version_used` | **[NEW]** Which profile version produced this outcome |
| — | `opt.engine_profile_version` | **[NEW]** Profile version used during optimization |
| — | `config` section | **[NEW]** SET LOCAL config (from `Candidate.set_local_commands`) |
| — | `reasons.reasoning_chain` | **[NEW]** Worker LLM reasoning text |
| — | `reasons.evidence` | **[NEW]** Key EXPLAIN signals |
| — | `source.type` discriminator | **[NEW]** `4w_worker` / `plan_scanner` / `expert_session` |
| `what_worked` / `why_it_worked` | Dropped | You record these in analysis sessions, not auto-populated from lookups |
| `principle` | Dropped | Principles are human-derived in analysis sessions |

**Key design change**: The blackboard captures **raw data only**. No `TRANSFORM_PRINCIPLES` lookup, no `PRINCIPLE_WHEN` lookup, no auto-derived intelligence. The hardcoded lookups (`build_blackboard.py:347-378`) stay for the legacy `GlobalKnowledge` path but are not part of the target blackboard schema.

---

## Storage

**Format**: JSONL, append-only, date-partitioned.

```
data/layer1/{engine}_{benchmark}/{date}/outcomes.jsonl
```

One JSONL line per outcome. A full benchmark run (4 workers x 99 queries) produces ~396 entries.

---

## 3-Phase Extraction Pipeline

The existing pipeline in `build_blackboard.py` works. Here's what stays and what changes.

### Phase 1: Extract — `extract_query_entries()` (`build_blackboard.py:538`)

**Stays as-is**:
- Loading `assignments.json` per query for strategy context
- 4-tier transform extraction (assignments -> response regex -> strategy map -> SQL diff)
- Error categorization via `categorize_error()`
- Status classification via `classify_status()`

**Changes**:
- Populate `reasons.reasoning_chain` from worker response text
- Populate `reasons.evidence` from EXPLAIN plan signals
- Populate `config.settings` from `Candidate.set_local_commands`
- Populate `outcome.tier` based on speedup
- Assign `source.type` discriminator
- Populate `provenance.knowledge_version_used`

### Phase 2: Collate — `phase2_collate()` (`build_blackboard.py:798`)

**Stays as-is for legacy path**: Grouping, regression cross-referencing, anti-pattern extraction, `GlobalKnowledge` output with `TRANSFORM_PRINCIPLES` fallbacks.

**Not extended**: Phase 2 stays a mechanical grouping step. All reasoning happens in your analysis sessions, not in code.

### Phase 3: Global Merge — `phase3_global()` (`build_blackboard.py:970`)

**Stays as-is**: Canonical storage, merge logic, source run tracking.

---

## Supporting Structures (Unchanged)

### GlobalBlackboardQuery (`build_blackboard.py:303`)
Best outcome per query across all runs. Stays as-is.

### SourceAttempt (`build_blackboard.py:259`)
One worker's attempt within a query's history. Stays as-is.

### KnowledgePrinciple (`build_blackboard.py:131`)
Structure stays for legacy `GlobalKnowledge` compat. The hardcoded `TRANSFORM_PRINCIPLES` and `PRINCIPLE_WHEN` lookups continue to populate this for the legacy path.

### GlobalKnowledge (`build_blackboard.py:198`)
Stays as-is. It's the legacy knowledge format. The engine profile and gold examples are the target knowledge format.

---

## Migration Path

1. **Extend `BlackboardEntry`** with new fields (all optional with defaults for backward compat)
2. **Phase 1 changes** populate new fields from existing pipeline data
3. **Legacy path unmodified**: `GlobalKnowledge` + `TRANSFORM_PRINCIPLES` continue working
4. **New runs** capture all fields; old entries keep what they have
5. **No breaking changes**: All new fields are optional

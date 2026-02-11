# Knowledge Engine Integration Guide

## Overview

This guide shows exactly how to integrate the Knowledge Engine with the existing Product Pipeline.

---

## Integration Points

There are exactly **2 integration points** between the Knowledge Engine and Product Pipeline:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           INTEGRATION ARCHITECTURE                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   KNOWLEDGE ENGINE                    PRODUCT PIPELINE                      │
│                                                                             │
│   ┌─────────────────┐                 ┌─────────────────┐                  │
│   │                 │◀── Interface A──┤ Phase 2:        │                  │
│   │   Layer 4       │    (READ)       │ Knowledge       │                  │
│   │   (Knowledge    │                 │ Retrieval       │                  │
│   │    Store)       │                 │                 │                  │
│   │                 ├─── Interface B──┤                 │                  │
│   │   Layer 1       │    (WRITE)      │ Phase 7:        │                  │
│   │   (Blackboard)  │                 │ Outputs         │                  │
│   └─────────────────┘                 └─────────────────┘                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Interface A: Knowledge Retrieval (Phase 2)

### Current State (pipeline.py)

```python
# Current: pipeline.py (simplified)
class Pipeline:
    def _find_examples(self, sql: str, dialect: str):
        # 1. Load from knowledge.py
        recommender = TagRecommender()
        matched = recommender.find_similar_examples(sql, dialect)
        
        # 2. Load engine profile
        profile = self._load_engine_profile(dialect)
        
        # 3. Load constraints
        constraints = self._load_constraint_files(dialect)
        
        # 4. Load scanner findings (PG only)
        scanner_text = None
        if dialect == "postgresql":
            scanner_text = self._load_scanner_findings()
        
        return {
            "matched_examples": matched,
            "engine_profile": profile,
            "constraints": constraints,
            "scanner_findings": scanner_text,
        }
```

### Target State (with Knowledge Engine)

```python
# Target: pipeline.py (with Knowledge Engine)
class Pipeline:
    def __init__(self, ...):
        # ... existing init ...
        self.knowledge_engine = KnowledgeEngine(config.knowledge_config)
    
    def _find_examples(self, sql: str, dialect: str) -> KnowledgeResponse:
        """Interface A: Query Knowledge Engine for relevant knowledge."""
        
        # Build query
        query = KnowledgeQuery(
            query_id=self.query_id,
            sql_fingerprint=compute_fingerprint(sql),
            dialect=dialect,
            available_context={
                "logical_tree": self.logical_tree,
                "explain_plan": self.explain_plan,
            },
            context_confidence=self.context_confidence,  # high | degraded | heuristic
        )
        
        # Interface A: READ from Knowledge Engine
        try:
            response = self.knowledge_engine.query(query)
        except KnowledgeNotFoundError as e:
            # Intelligence gate failure
            raise IntelligenceGateError(f"Knowledge Engine: {e}")
        
        # Check freshness
        if response.freshness_score < 0.3:
            logger.warning(f"Knowledge stale (freshness={response.freshness_score:.2f}), "
                          "background refresh scheduled")
        
        return response
```

### Required Changes to knowledge.py

```python
# knowledge.py - Integration layer

from knowledge_engine.api import KnowledgeEngine, KnowledgeQuery

class TagRecommender:
    """Legacy interface - delegate to Knowledge Engine."""
    
    def __init__(self):
        self.engine = KnowledgeEngine()
    
    def find_similar_examples(self, sql: str, dialect: str) -> List[Dict]:
        """Delegate to Knowledge Engine Interface A."""
        query = KnowledgeQuery(
            query_id="unknown",
            sql_fingerprint=compute_fingerprint(sql),
            dialect=dialect,
            available_context={},
            context_confidence="high",
        )
        
        response = self.engine.query(query)
        
        # Convert GoldExample objects to legacy dict format
        return [
            {
                "query_id": ex.query_id,
                "original_sql": ex.original_sql,
                "optimized_sql": ex.optimized_sql,
                "speedup": ex.speedup,
                "tags": ex.classification.tags,
            }
            for ex in response.matched_examples
        ]
```

---

## Interface B: Outcome Ingestion (Phase 7)

### Current State (pipeline.py)

```python
# Current: pipeline.py (simplified)
class Pipeline:
    def _update_benchmark_leaderboard(self, result: ValidationResult):
        # 1. Update leaderboard
        leaderboard.update(result)
        
        # 2. Save artifacts
        store.save_candidate(result)
        
        # 3. Create learning record
        learner.create_learning_record(...)
```

### Target State (with Knowledge Engine)

```python
# Target: pipeline.py (with Knowledge Engine)
class Pipeline:
    def _save_outcome(self, result: ValidationResult) -> None:
        """Interface B: Report outcome to Knowledge Engine."""
        
        # Build outcome
        outcome = OptimizationOutcome(
            query_id=self.query_id,
            run_id=self.run_id,
            timestamp=datetime.utcnow(),
            status=result.status,  # WIN | IMPROVED | NEUTRAL | REGRESSION | ERROR
            speedup=result.speedup,
            speedup_type=result.speedup_type,
            validation_confidence=result.validation_confidence,
            transforms_applied=result.transforms,
            original_sql=self.original_sql,
            optimized_sql=result.optimized_sql,
            worker_responses=self.worker_responses,
            error_category=result.error_category,
            error_messages=result.error_messages,
            model=self.config.model,
            provider=self.config.provider,
            git_sha=get_git_sha(),
        )
        
        # Interface B: WRITE to Knowledge Engine (fire-and-forget)
        self.knowledge_engine.ingest(outcome)
        
        # Also update legacy systems (during migration)
        self._legacy_update_leaderboard(result)
```

### Integration with store.py

```python
# store.py - Add Knowledge Engine reporting

class Store:
    def __init__(self, ...):
        self.knowledge_engine = KnowledgeEngine()
    
    def save_candidate(self, result: ValidationResult) -> StoredArtifact:
        """Save artifacts AND report to Knowledge Engine."""
        
        # 1. Save artifacts (existing)
        artifact = self._save_files(result)
        
        # 2. Report to Knowledge Engine (new)
        outcome = self._build_outcome(result)
        self.knowledge_engine.ingest(outcome)
        
        return artifact
```

---

## Migration Strategy

### Phase 1: Side-by-Side (Week 1)

Run Knowledge Engine alongside existing system:

```python
# pipeline.py - Phase 1 migration
class Pipeline:
    def _find_examples(self, sql: str, dialect: str):
        # Try Knowledge Engine first
        try:
            ke_response = self.knowledge_engine.query(...)
            # Validate against legacy
            legacy = self._legacy_find_examples(sql, dialect)
            self._validate_consistency(ke_response, legacy)
            return ke_response
        except Exception as e:
            logger.warning(f"Knowledge Engine failed: {e}, using legacy")
            return self._legacy_find_examples(sql, dialect)
    
    def _save_outcome(self, result):
        # Always save to both
        self._legacy_save_outcome(result)
        try:
            self.knowledge_engine.ingest(result)
        except Exception as e:
            logger.error(f"Knowledge Engine ingest failed: {e}")
```

### Phase 2: Cutover (Week 2)

Switch to Knowledge Engine as primary:

```python
# pipeline.py - Phase 2 migration
class Pipeline:
    def _find_examples(self, sql: str, dialect: str):
        # Knowledge Engine is primary
        return self.knowledge_engine.query(...)
    
    def _save_outcome(self, result):
        # Knowledge Engine is primary
        self.knowledge_engine.ingest(result)
        # Legacy for backup only
        self._legacy_save_outcome(result)
```

### Phase 3: Cleanup (Week 3)

Remove legacy code:

```python
# pipeline.py - Phase 3 migration
class Pipeline:
    def _find_examples(self, sql: str, dialect: str):
        return self.knowledge_engine.query(...)
    
    def _save_outcome(self, result):
        return self.knowledge_engine.ingest(result)
    
    # Delete _legacy_* methods
```

---

## Code Changes Required

### 1. pipeline.py

```python
# ADD: Import
from knowledge_engine.api import KnowledgeEngine, KnowledgeQuery, OptimizationOutcome

# ADD: Constructor
self.knowledge_engine = KnowledgeEngine(config.knowledge_config)

# MODIFY: _find_examples()
# Delegate to Knowledge Engine Interface A

# MODIFY: Phase 7 save
# Add call to Knowledge Engine Interface B
```

### 2. knowledge.py

```python
# ADD: Import
from knowledge_engine.api import KnowledgeEngine, KnowledgeQuery

# MODIFY: TagRecommender
# Delegate to Knowledge Engine
```

### 3. store.py

```python
# ADD: Import
from knowledge_engine.api import KnowledgeEngine, OptimizationOutcome

# ADD: Constructor
self.knowledge_engine = KnowledgeEngine()

# MODIFY: save_candidate()
# Add call to knowledge_engine.ingest()
```

### 4. learn.py

```python
# ADD: Import
from knowledge_engine.api import KnowledgeEngine, OptimizationOutcome

# MODIFY: Learner
# Delegate to Knowledge Engine or keep for compatibility
```

---

## Configuration

### .env / settings.py

```python
# Knowledge Engine Configuration
KNOWLEDGE_ENGINE_ENABLED = True
KNOWLEDGE_ENGINE_PATH = "knowledge_engine/store"
KNOWLEDGE_ENGINE_CACHE_TTL = 300  # 5 minutes

# Compression triggers
KNOWLEDGE_COMPRESSION_TEMPORAL_DAYS = 7
KNOWLEDGE_COMPRESSION_EXTRACT_MIN = 50
KNOWLEDGE_COMPRESSION_PATTERN_MIN = 10

# Promotion criteria
KNOWLEDGE_PROMOTION_MIN_WINS = 5
KNOWLEDGE_PROMOTION_MIN_SUCCESS_RATE = 0.70
KNOWLEDGE_PROMOTION_AUTO = True
```

### config.yaml

```yaml
knowledge_engine:
  enabled: true
  store_path: "knowledge_engine/store"
  
  layers:
    layer1:
      path: "layer1/blackboard"
      format: "jsonl"
      retention_days: 90
    
    layer2:
      path: "layer2/findings"
      format: "json"
      extraction_trigger: 50  # entries
    
    layer3:
      path: "layer3/patterns"
      format: "json"
      mining_trigger: 10  # findings
    
    layer4:
      path: "layer4/store"
      format: "json"
      promotion:
        min_wins: 5
        min_success_rate: 0.70
        auto_promote: true
  
  interfaces:
    query_timeout_ms: 500
    ingest_async: true
```

---

## Testing Strategy

### Unit Tests

```python
# tests/test_knowledge_engine_integration.py

def test_interface_a_query():
    """Test Knowledge Engine query interface."""
    engine = KnowledgeEngine()
    query = KnowledgeQuery(
        query_id="q88",
        sql_fingerprint="test",
        dialect="duckdb",
        available_context={},
        context_confidence="high",
    )
    
    response = engine.query(query)
    
    assert response.matched_examples is not None
    assert response.engine_profile is not None
    assert response.freshness_score > 0

def test_interface_b_ingest():
    """Test Knowledge Engine ingest interface."""
    engine = KnowledgeEngine()
    outcome = OptimizationOutcome(
        query_id="q88",
        run_id="test_run",
        timestamp=datetime.utcnow(),
        status="WIN",
        speedup=2.5,
        # ... other fields
    )
    
    # Should not raise
    engine.ingest(outcome)
    
    # Verify written to Layer 1
    assert engine.layer1.get_last_entry().query_id == "q88"
```

### Integration Tests

```python
# tests/test_pipeline_integration.py

def test_pipeline_uses_knowledge_engine():
    """Test that Pipeline correctly uses Knowledge Engine."""
    pipeline = Pipeline()
    
    # Mock Knowledge Engine
    mock_ke = Mock()
    mock_ke.query.return_value = KnowledgeResponse(...)
    pipeline.knowledge_engine = mock_ke
    
    # Run Phase 2
    pipeline._find_examples("SELECT 1", "duckdb")
    
    # Verify KE was called
    assert mock_ke.query.called

def test_pipeline_reports_to_knowledge_engine():
    """Test that Pipeline reports outcomes to Knowledge Engine."""
    pipeline = Pipeline()
    
    # Mock Knowledge Engine
    mock_ke = Mock()
    pipeline.knowledge_engine = mock_ke
    
    # Run Phase 7
    result = ValidationResult(status="WIN", speedup=2.5)
    pipeline._save_outcome(result)
    
    # Verify KE was called
    assert mock_ke.ingest.called
```

---

## Rollback Plan

If issues are discovered:

```python
# pipeline.py - Rollback capability
class Pipeline:
    def __init__(self, ...):
        self.use_knowledge_engine = config.get(
            "KNOWLEDGE_ENGINE_ENABLED", 
            False
        )
    
    def _find_examples(self, sql: str, dialect: str):
        if self.use_knowledge_engine:
            try:
                return self.knowledge_engine.query(...)
            except Exception as e:
                logger.error(f"KE failed: {e}, using legacy")
                self.use_knowledge_engine = False  # Auto-disable
        
        return self._legacy_find_examples(sql, dialect)
```

---

## Summary

### Integration Points

| Interface | Direction | Pipeline Method | KE Method |
|-----------|-----------|-----------------|-----------|
| A | Engine → Pipeline | `_find_examples()` | `query()` |
| B | Pipeline → Engine | `_save_outcome()` | `ingest()` |

### Files to Modify

1. `pipeline.py` - Add KE calls to Phase 2 & 7
2. `knowledge.py` - Delegate to KE
3. `store.py` - Add ingest call
4. `learn.py` - Optional delegation

### New Files

1. `knowledge_engine/api.py` - Interface A & B
2. `knowledge_engine/layer1/` - Blackboard
3. `knowledge_engine/layer2/` - Findings
4. `knowledge_engine/layer3/` - Patterns
5. `knowledge_engine/layer4/` - Knowledge Store

### Migration Timeline

- Week 1: Side-by-side (validation)
- Week 2: Cutover (KE primary)
- Week 3: Cleanup (remove legacy)

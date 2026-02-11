# Unified Blackboard Design

## Core Principle

**One blackboard per (engine, benchmark) pair.**

All data - worker outcomes, scanner findings, config experiments - goes into the **same blackboard**, keyed by query.

```
blackboard/
├── duckdb_tpcds.jsonl         # DuckDB + TPC-DS benchmark
├── duckdb_tpch.jsonl          # DuckDB + TPCH benchmark  
├── postgresql_dsb.jsonl       # PostgreSQL + DSB benchmark
└── postgresql_tpcds.jsonl     # PostgreSQL + TPC-DS benchmark

engine_profiles/               # Cross-benchmark, per engine
├── duckdb.json                # Aggregated from all DuckDB blackboards
└── postgresql.json            # Aggregated from all PostgreSQL blackboards
```

---

## Unified Schema (JSON)

```json
{
  "id": "q88",
  
  "base": {
    "query_id": "q88",
    "engine": "duckdb",
    "benchmark": "tpcds",
    "original_sql": "SELECT ... FROM ... WHERE ...",
    "fingerprint": "decorrelate_subquery_date_filter",
    "timestamp": "2026-02-11T10:05:22Z"
  },
  
  "opt": {
    "approach": "4w_worker",
    "worker_id": 1,
    "strategy": "aggressive_single_pass_restructure",
    "iteration": 0,
    "optimized_sql": "WITH ... SELECT ...",
    "examples_used": ["q6_date_cte", "q9_single_pass"],
    "engine_profile_version": "2026.02.11-v3"
  },
  
  "principle": {
    "what": "Consolidated 8 correlated subqueries into 1 scan with CASE",
    "why": "Eliminated 7 redundant fact table scans",
    "mechanism": "Single-pass aggregation with conditional logic",
    "transform_type": "single_pass_aggregation",
    "gap_exploited": "REDUNDANT_SCAN_ELIMINATION",
    "supporting_evidence": "EXPLAIN: 1 scan vs 8 scans",
    "confidence": "high"
  },
  
  "semantics": {
    "business_intent": "Analyze sales across 8 time buckets",
    "tables_accessed": ["store_sales", "time_dim", "item"],
    "join_pattern": "star_schema",
    "aggregation_type": "multi_stage",
    "filter_selectivity": 0.15,
    "query_archetype": "correlated_subquery_time_buckets"
  },
  
  "config": {
    "settings": {
      "work_mem": "1GB"
    },
    "reasoning": {
      "trigger": "EXPLAIN showed Sort Space Type = 'Disk'",
      "rationale": "Sort spilling to temp files; increase work_mem to keep in RAM",
      "expected_benefit": "Eliminate temp file I/O",
      "risk_assessment": "Low - work_mem is per-operation"
    },
    "impact_additive": 6.82,
    "impact_combined": null
  },
  
  "scanner_finding": {
    "claim": "Disabling nested loops causes >4x regression on dim-heavy star queries",
    "category": "join_sensitivity",
    "setting_tested": {
      "enable_nestloop": "off"
    },
    "baseline_plan_cost": 4523.0,
    "modified_plan_cost": 18920.0,
    "baseline_time_ms": 1240.0,
    "modified_time_ms": 5210.0,
    "implication": "Do NOT restructure joins that eliminate nested loop index lookups",
    "boundaries": [
      "Applies when baseline uses nested loops for dimension PK lookups",
      "Query has 3+ dimension tables with PK filters"
    ],
    "applicable_queries": ["q085", "q091", "q065"]
  },
  
  "outcome": {
    "status": "WIN",
    "speedup": 6.28,
    "speedup_type": "measured",
    "timing": {
      "original_ms": 4520.0,
      "optimized_ms": 720.0
    },
    "validation": {
      "confidence": "high",
      "rows_match": true,
      "checksum_match": true
    },
    "error": null
  },
  
  "tags": [
    "single_pass_aggregation",
    "star_schema",
    "correlated_subquery",
    "high_impact",
    "duckdb_optimized"
  ],
  
  "provenance": {
    "run_id": "swarm_batch_20260208_102033",
    "model": "deepseek-reasoner",
    "provider": "deepseek",
    "git_sha": "abc123",
    "reviewed": true
  },
  
  "version": {
    "schema_version": "2.0",
    "entry_version": 1,
    "superseded_by": null,
    "status": "active"
  }
}
```

---

## Schema Specification

### Top-Level Fields

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Query identifier (e.g., "q88", "query_23") |
| `base` | object | Original query + identification |
| `opt` | object | Optimization attempt details |
| `principle` | object | What worked and why |
| `semantics` | object | What the query actually does |
| `config` | object | SET LOCAL / pragma settings |
| `scanner_finding` | object \| null | PG plan-space insight |
| `outcome` | object | Measured results |
| `tags` | [string] | Tags from tagger |
| `provenance` | object | Where this came from |
| `version` | object | Versioning info |

### Field Details

#### `id`
```json
"id": "q88"
```
- Query number/identifier
- Used for quick lookup and reference

#### `base`
```json
"base": {
  "query_id": "q88",
  "engine": "duckdb",
  "benchmark": "tpcds",
  "original_sql": "SELECT ... FROM ... WHERE ...",
  "fingerprint": "decorrelate_subquery_date_filter",
  "timestamp": "2026-02-11T10:05:22Z"
}
```
| Sub-field | Type | Description |
|-----------|------|-------------|
| `query_id` | string | Same as top-level `id` |
| `engine` | string | "duckdb" \| "postgresql" |
| `benchmark` | string | "tpcds" \| "tpch" \| "dsb" |
| `original_sql` | string | **The actual original query SQL** |
| `fingerprint` | string | Structural pattern signature |
| `timestamp` | string | ISO 8601 timestamp |

#### `opt`
```json
"opt": {
  "approach": "4w_worker",
  "worker_id": 1,
  "strategy": "aggressive_single_pass_restructure",
  "iteration": 0,
  "optimized_sql": "WITH ... SELECT ...",
  "examples_used": ["q6_date_cte", "q9_single_pass"],
  "engine_profile_version": "2026.02.11-v3"
}
```
| Sub-field | Type | Description |
|-----------|------|-------------|
| `approach` | string | "4w_worker" \| "plan_scanner" \| "expert_manual" |
| `worker_id` | int \| null | 1-4 for 4W, null for scanner |
| `strategy` | string | Strategy name |
| `iteration` | int \| null | 0=initial, 1=snipe, 2=final |
| `optimized_sql` | string | **The actual optimized query SQL** |
| `examples_used` | [string] | Gold examples provided |
| `engine_profile_version` | string | Profile version used |

#### `principle`
```json
"principle": {
  "what": "Consolidated 8 correlated subqueries into 1 scan",
  "why": "Eliminated 7 redundant fact table scans",
  "mechanism": "Single-pass aggregation with CASE",
  "transform_type": "single_pass_aggregation",
  "gap_exploited": "REDUNDANT_SCAN_ELIMINATION",
  "supporting_evidence": "EXPLAIN: 1 scan vs 8 scans",
  "confidence": "high"
}
```

#### `semantics`
```json
"semantics": {
  "business_intent": "Analyze sales across 8 time buckets",
  "tables_accessed": ["store_sales", "time_dim", "item"],
  "join_pattern": "star_schema",
  "aggregation_type": "multi_stage",
  "filter_selectivity": 0.15,
  "query_archetype": "correlated_subquery_time_buckets"
}
```

#### `config`
```json
"config": {
  "settings": {
    "work_mem": "1GB",
    "jit": "off"
  },
  "reasoning": {
    "trigger": "EXPLAIN showed Sort Space Type = 'Disk'",
    "rationale": "Sort spilling to temp files",
    "expected_benefit": "Eliminate temp file I/O",
    "risk_assessment": "Low - work_mem is per-operation"
  },
  "impact_additive": 6.82,
  "impact_combined": null
}
```

#### `scanner_finding` (PostgreSQL only, null otherwise)
```json
"scanner_finding": {
  "claim": "Disabling nested loops causes >4x regression",
  "category": "join_sensitivity",
  "setting_tested": {
    "enable_nestloop": "off"
  },
  "baseline_plan_cost": 4523.0,
  "modified_plan_cost": 18920.0,
  "baseline_time_ms": 1240.0,
  "modified_time_ms": 5210.0,
  "implication": "Do NOT eliminate nested loop index lookups",
  "boundaries": ["Applies to dim PK lookups"],
  "applicable_queries": ["q085", "q091", "q065"]
}
```

#### `outcome`
```json
"outcome": {
  "status": "WIN",
  "speedup": 6.28,
  "speedup_type": "measured",
  "timing": {
    "original_ms": 4520.0,
    "optimized_ms": 720.0
  },
  "validation": {
    "confidence": "high",
    "rows_match": true,
    "checksum_match": true
  },
  "error": null
}
```

#### `tags`
```json
"tags": [
  "single_pass_aggregation",
  "star_schema",
  "correlated_subquery",
  "high_impact",
  "duckdb_optimized"
]
```
- Generated by tagger
- Used for matching and filtering

#### `provenance`
```json
"provenance": {
  "run_id": "swarm_batch_20260208_102033",
  "model": "deepseek-reasoner",
  "provider": "deepseek",
  "git_sha": "abc123",
  "reviewed": true
}
```

#### `version`
```json
"version": {
  "schema_version": "2.0",
  "entry_version": 1,
  "superseded_by": null,
  "status": "active"
}
```

---

## Examples by Approach

### Example 1: 4W Worker (DuckDB)

```json
{
  "id": "q88",
  "base": {
    "query_id": "q88",
    "engine": "duckdb",
    "benchmark": "tpcds",
    "original_sql": "SELECT ... 8 correlated subqueries ...",
    "fingerprint": "decorrelate_subquery_date_filter",
    "timestamp": "2026-02-11T10:05:22Z"
  },
  "opt": {
    "approach": "4w_worker",
    "worker_id": 1,
    "strategy": "aggressive_single_pass_restructure",
    "iteration": 0,
    "optimized_sql": "WITH time_buckets AS ... SELECT ... CASE WHEN ...",
    "examples_used": ["q6_date_cte", "q9_single_pass"],
    "engine_profile_version": "2026.02.11-v3"
  },
  "principle": {
    "what": "Consolidated 8 correlated subqueries into 1 scan with CASE",
    "why": "Eliminated 7 redundant fact table scans",
    "mechanism": "Single-pass aggregation with conditional logic",
    "transform_type": "single_pass_aggregation",
    "gap_exploited": "REDUNDANT_SCAN_ELIMINATION",
    "supporting_evidence": "EXPLAIN: 1 scan vs 8 scans",
    "confidence": "high"
  },
  "semantics": {
    "business_intent": "Analyze sales across 8 time buckets",
    "tables_accessed": ["store_sales", "time_dim", "item"],
    "join_pattern": "star_schema",
    "aggregation_type": "multi_stage",
    "filter_selectivity": 0.15,
    "query_archetype": "correlated_subquery_time_buckets"
  },
  "config": {
    "settings": {},
    "reasoning": {},
    "impact_additive": null,
    "impact_combined": null
  },
  "scanner_finding": null,
  "outcome": {
    "status": "WIN",
    "speedup": 6.28,
    "speedup_type": "measured",
    "timing": {
      "original_ms": 4520.0,
      "optimized_ms": 720.0
    },
    "validation": {
      "confidence": "high",
      "rows_match": true,
      "checksum_match": true
    },
    "error": null
  },
  "tags": [
    "single_pass_aggregation",
    "star_schema",
    "correlated_subquery",
    "high_impact"
  ],
  "provenance": {
    "run_id": "swarm_batch_20260208_102033",
    "model": "deepseek-reasoner",
    "provider": "deepseek",
    "git_sha": "abc123",
    "reviewed": true
  },
  "version": {
    "schema_version": "2.0",
    "entry_version": 1,
    "superseded_by": null,
    "status": "active"
  }
}
```

### Example 2: Scanner Finding (PostgreSQL)

```json
{
  "id": "q85",
  "base": {
    "query_id": "q85",
    "engine": "postgresql",
    "benchmark": "dsb",
    "original_sql": "SELECT ... FROM web_sales ... 5 table join ...",
    "fingerprint": "star_schema_multi_dim_lookup",
    "timestamp": "2026-02-11T10:05:22Z"
  },
  "opt": {
    "approach": "plan_scanner",
    "worker_id": null,
    "strategy": "config_experiment_nested_loop",
    "iteration": null,
    "optimized_sql": null,
    "examples_used": [],
    "engine_profile_version": null
  },
  "principle": {
    "what": "Nested loops with dimension PK indexes are optimal for star queries",
    "why": "Dimension tables are small; index lookups beat hash scan+probe",
    "mechanism": "BitmapOr + Index Scan on dimension PKs",
    "transform_type": "config_tuning",
    "gap_exploited": null,
    "supporting_evidence": "no_nestloop caused 4.2x regression",
    "confidence": "high"
  },
  "semantics": {
    "business_intent": "Customer purchase analysis across channels",
    "tables_accessed": ["web_sales", "customer", "date_dim", "item"],
    "join_pattern": "star_schema",
    "aggregation_type": "single_pass",
    "filter_selectivity": 0.08,
    "query_archetype": "star_dim_filtered"
  },
  "config": {
    "settings": {
      "enable_nestloop": "off"
    },
    "reasoning": {
      "trigger": "Testing optimizer behavior",
      "rationale": "Disabled nested loop to test hash join performance",
      "expected_benefit": "Potentially better for large dimension joins",
      "risk_assessment": "Medium - may regress on PK lookups"
    },
    "impact_additive": 0.24,
    "impact_combined": null
  },
  "scanner_finding": {
    "claim": "Disabling nested loops causes >4x regression on dim-heavy star queries",
    "category": "join_sensitivity",
    "setting_tested": {
      "enable_nestloop": "off"
    },
    "baseline_plan_cost": 4523.0,
    "modified_plan_cost": 18920.0,
    "baseline_time_ms": 1240.0,
    "modified_time_ms": 5210.0,
    "implication": "Do NOT restructure joins that eliminate nested loop index lookups",
    "boundaries": [
      "Applies when baseline uses nested loops for dimension PK lookups",
      "Query has 3+ dimension tables with PK filters"
    ],
    "applicable_queries": ["q085", "q091", "q065"]
  },
  "outcome": {
    "status": "REGRESSION",
    "speedup": 0.24,
    "speedup_type": "measured",
    "timing": {
      "original_ms": 1240.0,
      "optimized_ms": 5210.0
    },
    "validation": {
      "confidence": "high",
      "rows_match": true,
      "checksum_match": null
    },
    "error": null
  },
  "tags": [
    "join_sensitivity",
    "nested_loop",
    "star_schema",
    "config_experiment",
    "regression"
  ],
  "provenance": {
    "run_id": "scanner_batch_20260210_083000",
    "model": "plan_scanner",
    "provider": "system",
    "git_sha": "abc123",
    "reviewed": true
  },
  "version": {
    "schema_version": "2.0",
    "entry_version": 1,
    "superseded_by": null,
    "status": "active"
  }
}
```

---

## Storage Format

### File Format: JSON Lines (JSONL)

```
blackboard/duckdb_tpcds.jsonl
─────────────────────────────
{"id": "q6", "base": {...}, "opt": {...}, ...}
{"id": "q88", "base": {...}, "opt": {...}, ...}
{"id": "q23", "base": {...}, "opt": {...}, ...}
```

- One line per entry
- Append-only
- Easy to stream/process

### Python Interface

```python
from dataclasses import dataclass
from typing import List, Dict, Optional

@dataclass
class BlackboardEntry:
    """Single entry in the unified blackboard."""
    
    # Top-level
    id: str
    
    # Core sections
    base: 'BaseSection'
    opt: 'OptSection'
    principle: 'PrincipleSection'
    semantics: 'SemanticsSection'
    config: 'ConfigSection'
    scanner_finding: Optional['ScannerFindingSection']
    outcome: 'OutcomeSection'
    tags: List[str]
    provenance: 'ProvenanceSection'
    version: 'VersionSection'

@dataclass 
class BaseSection:
    query_id: str
    engine: str
    benchmark: str
    original_sql: str
    fingerprint: str
    timestamp: str

@dataclass
class OptSection:
    approach: str
    worker_id: Optional[int]
    strategy: str
    iteration: Optional[int]
    optimized_sql: Optional[str]
    examples_used: List[str]
    engine_profile_version: Optional[str]

@dataclass
class PrincipleSection:
    what: str
    why: str
    mechanism: str
    transform_type: str
    gap_exploited: Optional[str]
    supporting_evidence: str
    confidence: str

@dataclass
class SemanticsSection:
    business_intent: str
    tables_accessed: List[str]
    join_pattern: str
    aggregation_type: str
    filter_selectivity: float
    query_archetype: str

@dataclass
class ConfigSection:
    settings: Dict[str, str]
    reasoning: Dict[str, str]
    impact_additive: Optional[float]
    impact_combined: Optional[float]

@dataclass
class ScannerFindingSection:
    claim: str
    category: str
    setting_tested: Dict[str, str]
    baseline_plan_cost: float
    modified_plan_cost: float
    baseline_time_ms: float
    modified_time_ms: float
    implication: str
    boundaries: List[str]
    applicable_queries: List[str]

@dataclass
class OutcomeSection:
    status: str
    speedup: float
    speedup_type: str
    timing: Dict[str, float]
    validation: Dict[str, any]
    error: Optional[Dict[str, str]]

@dataclass
class ProvenanceSection:
    run_id: str
    model: str
    provider: str
    git_sha: str
    reviewed: bool

@dataclass
class VersionSection:
    schema_version: str
    entry_version: int
    superseded_by: Optional[str]
    status: str
```

---

## Derivation to Engine Profile

```python
def derive_engine_profile(engine: str) -> dict:
    """Derive cross-benchmark engine profile from blackboards."""
    
    # Load all blackboards for this engine
    all_entries = []
    for benchmark in get_benchmarks_for_engine(engine):
        entries = load_blackboard(engine, benchmark)
        all_entries.extend(entries)
    
    # Extract gaps (from principles with gap_exploited)
    gaps = {}
    for entry in all_entries:
        if entry.principle.gap_exploited:
            gap_id = entry.principle.gap_exploited
            if gap_id not in gaps:
                gaps[gap_id] = {
                    "what": entry.principle.what,
                    "why": entry.principle.why,
                    "wins": [],
                    "failures": []
                }
            if entry.outcome.status in ("WIN", "IMPROVED"):
                gaps[gap_id]["wins"].append(entry.id)
            elif entry.outcome.status == "REGRESSION":
                gaps[gap_id]["failures"].append(entry.id)
    
    # Extract strengths (from scanner findings that are regressions when disabled)
    strengths = {}
    for entry in all_entries:
        if entry.scanner_finding and entry.outcome.status == "REGRESSION":
            # This is something that hurt when we changed it = strength
            strength_id = f"STRENGTH_{entry.scanner_finding.category.upper()}"
            strengths[strength_id] = {
                "what": entry.principle.what,
                "field_note": entry.scanner_finding.implication
            }
    
    return {
        "engine": engine,
        "gaps": gaps,
        "strengths": strengths,
        "derived_from": len(all_entries)
    }
```

---

## Summary

| Field | Contains |
|-------|----------|
| `id` | Query identifier (e.g., "q88") |
| `base` | **Original SQL** + metadata |
| `opt` | **Optimized SQL** + approach details |
| `principle` | What worked and why |
| `semantics` | Business intent and query structure |
| `config` | Settings + reasoning |
| `scanner_finding` | PG plan-space insight (or null) |
| `outcome` | Measured results |
| `tags` | Tagger classifications |
| `provenance` | Source info |
| `version` | Versioning |

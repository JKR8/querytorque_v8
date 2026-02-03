# PUCT Architecture for SQL MCTS Optimizer

## Quick Reference: Two Selection Modes

| Aspect | RANDOM (Original) | PUCT (New - Default) |
|--------|-------------------|----------------------|
| **Config** | `RANDOM_CONFIG` | `PUCT_CONFIG` |
| **Selection** | `random.choice(candidates)` | PUCT score ranking |
| **Prioritization** | None | KB weights + opportunity detection |
| **Progressive Widening** | No (all candidates) | Yes (top-k by prior) |
| **Use Case** | Baseline comparison | Production use |

```python
# ORIGINAL (baseline)
optimizer = MCTSSQLOptimizer(database="db.duckdb", prior_config=RANDOM_CONFIG)

# NEW (default)
optimizer = MCTSSQLOptimizer(database="db.duckdb", prior_config=PUCT_CONFIG)
optimizer = MCTSSQLOptimizer(database="db.duckdb")  # Same as above
```

---

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           MCTS SELECTION MODES                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────┐          ┌─────────────────────────────────────┐  │
│  │  RANDOM_CONFIG      │          │  PUCT_CONFIG (default)              │  │
│  │  use_puct=False     │          │  use_puct=True                      │  │
│  │                     │          │                                     │  │
│  │  ┌───────────────┐  │          │  ┌─────────────────────────────┐   │  │
│  │  │ random.choice │  │          │  │ Knowledge Base Weights      │   │  │
│  │  │  (original)   │  │          │  │ (weight 1-10 per transform) │   │  │
│  │  └───────────────┘  │          │  └─────────────┬───────────────┘   │  │
│  │                     │          │                │                    │  │
│  └─────────────────────┘          │  ┌─────────────▼───────────────┐   │  │
│                                   │  │ Opportunity Detection       │   │  │
│                                   │  │ (regex pattern matching)    │   │  │
│                                   │  │ - OR conditions             │   │  │
│                                   │  │ - Correlated subqueries     │   │  │
│                                   │  │ - Date dimension joins      │   │  │
│                                   │  └─────────────┬───────────────┘   │  │
│                                   │                │                    │  │
│                                   │  ┌─────────────▼───────────────┐   │  │
│                                   │  │ Contextual Boosts           │   │  │
│                                   │  │ - opportunity_boost (1.5x)  │   │  │
│                                   │  │ - high_value_boost (1.2x)   │   │  │
│                                   │  │ - applied_penalty (0.5x)    │   │  │
│                                   │  └─────────────┬───────────────┘   │  │
│                                   │                │                    │  │
│                                   │  ┌─────────────▼───────────────┐   │  │
│                                   │  │ PUCT Score Computation      │   │  │
│                                   │  │                             │   │  │
│                                   │  │ Q + c·P·√N / (1+n)          │   │  │
│                                   │  │                             │   │  │
│                                   │  │ Q = avg reward              │   │  │
│                                   │  │ P = prior probability       │   │  │
│                                   │  │ N = parent visits           │   │  │
│                                   │  │ n = child visits            │   │  │
│                                   │  │ c = exploration constant    │   │  │
│                                   │  └─────────────────────────────┘   │  │
│                                   └─────────────────────────────────────┘  │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  PUCT_LLM_CONFIG (Phase 3)                                          │   │
│  │  use_puct=True, use_llm_ranking=True                                │   │
│  │                                                                     │   │
│  │  Same as PUCT_CONFIG, plus:                                         │   │
│  │  ┌───────────────────────────────────────────────────────────────┐ │   │
│  │  │ LLM Ranking (triggered when):                                 │ │   │
│  │  │ - Many candidates (>4)                                        │ │   │
│  │  │ - Node is "stuck" (high visits, low reward)                   │ │   │
│  │  │                                                               │ │   │
│  │  │ Batched call → JSON ranking → Convert to priors               │ │   │
│  │  │ Fallback to contextual priors on timeout/error                │ │   │
│  │  └───────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                         PROGRESSIVE WIDENING                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Only active when use_puct=True                                             │
│                                                                             │
│  k = ceil(1.5 × √visits)     ← Number of candidates to consider            │
│                                                                             │
│  Visit 1:  k = 2   [████████░░░░░░░░░░░░]  Consider top 2 by prior         │
│  Visit 4:  k = 3   [████████████░░░░░░░░]  Consider top 3                   │
│  Visit 9:  k = 5   [████████████████████]  Consider top 5                   │
│  Visit 16: k = 6   [all candidates]        Consider all (if ≤6)             │
│                                                                             │
│  Benefits:                                                                  │
│  - Focus early exploration on high-prior transforms                         │
│  - Allow diversity as node is visited more                                  │
│  - Avoid wasting LLM calls on low-probability transforms early              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## File Structure

```
mcts/
├── __init__.py          # Exports all public APIs
├── node.py              # MCTSNode with puct_score() method
├── tree.py              # MCTSTree with PUCT selection
├── optimizer.py         # MCTSSQLOptimizer entry point
├── priors.py            # Prior computation (NEW)
├── llm_ranker.py        # LLM ranking module (NEW)
├── transforms.py        # Transform prompts
├── reward.py            # Reward computation
└── PUCT_ARCHITECTURE.md # This document
```

## Design Decisions

### 1. Staged Implementation

The PUCT system was implemented in phases to allow incremental testing:

| Phase | Feature | Config |
|-------|---------|--------|
| 1 | UCB1 with KB weights | `use_kb_weights=True` |
| 2 | + Opportunity detection | `use_opportunity_detection=True` |
| 3 | + LLM ranking | `use_llm_ranking=True` |
| 4 | + Progressive widening | Always on with PUCT |

### 2. Toggle for A/B Comparison

The `use_puct` flag allows easy comparison between methods:

```python
# Original random selection (baseline)
optimizer = MCTSSQLOptimizer(
    database="db.duckdb",
    prior_config=RANDOM_CONFIG,  # use_puct=False
)

# New PUCT selection
optimizer = MCTSSQLOptimizer(
    database="db.duckdb",
    prior_config=PUCT_CONFIG,    # use_puct=True (default)
)
```

### 3. Prior Sources (Priority Order)

1. **LLM Ranking** (if enabled and triggered): Batched call with timeout
2. **Contextual Priors**: KB weights × opportunity boost × high-value boost ÷ applied penalty
3. **Uniform Priors**: Pure KB weights normalized to sum to 1.0

Fallback chain ensures robustness: LLM error → contextual → uniform

### 4. PUCT Formula

```
PUCT(s,a) = Q(s,a) + c · P(s,a) · √N(s) / (1 + N(s,a))

Where:
- Q(s,a) = average reward for taking action a from state s
- P(s,a) = prior probability of action a (from KB/opportunity/LLM)
- N(s)   = visit count of parent state
- N(s,a) = visit count of child (action a from state s)
- c      = exploration constant (default 2.0)
```

For unvisited nodes: `PUCT = c · P · √(N+1)` (pure exploration)

### 5. Knowledge Base Weights

From `knowledge_base.py`, transforms have weights 1-10:

| Transform | Weight | Category |
|-----------|--------|----------|
| `correlated_to_cte` | 9 | high_value |
| `push_pred` | 8 | high_value |
| `or_to_union` | 8 | high_value |
| `date_cte_isolate` | 7 | high_value |
| `consolidate_scans` | 7 | high_value |
| `multi_push_pred` | 6 | standard |
| `materialize_cte` | 5 | standard |
| `flatten_subq` | 5 | standard |
| `reorder_join` | 4 | standard |
| `inline_cte` | 3 | standard |
| `remove_redundant` | 2 | standard |

### 6. Opportunity Detection Triggers

Pattern matching in `detect_opportunities()`:

| Pattern | Transform Boosted |
|---------|-------------------|
| `OR` in WHERE clause | `or_to_union` |
| `WHERE col > (SELECT AVG...)` | `correlated_to_cte` |
| `date_dim` + `d_year` filter + fact table | `date_cte_isolate` |
| Same table scanned 2+ times | `consolidate_scans` |
| Repeated subquery pattern | `materialize_cte` |

### 7. LLM Ranking Triggers

LLM ranking is called (when enabled) if:
- Many candidates: `len(candidates) > 4`
- Node stuck: `visits >= 5 AND avg_reward < 0.3 AND failure_rate > 50%`

Timeout: 5 seconds (configurable via `llm_timeout_ms`)

## Comparison Benchmark

Run both modes on same queries to compare:

```python
from qt_sql.optimization.mcts import (
    MCTSSQLOptimizer,
    RANDOM_CONFIG,
    PUCT_CONFIG,
)

# Compare on TPC-DS Q1
query = open("D:/TPC-DS/queries_duckdb_converted/q1.sql").read()

# Baseline: random selection
opt_random = MCTSSQLOptimizer(
    database="D:/TPC-DS/tpcds_sf100.duckdb",
    prior_config=RANDOM_CONFIG,
)
result_random = opt_random.optimize(query, max_iterations=30)

# PUCT selection
opt_puct = MCTSSQLOptimizer(
    database="D:/TPC-DS/tpcds_sf100.duckdb",
    prior_config=PUCT_CONFIG,
)
result_puct = opt_puct.optimize(query, max_iterations=30)

print(f"Random: {result_random.speedup:.2f}x in {result_random.iterations} iters")
print(f"PUCT:   {result_puct.speedup:.2f}x in {result_puct.iterations} iters")
```

## Metrics to Track

For comparison benchmarks:

1. **Speedup achieved** - Primary metric
2. **Iterations to best** - How fast did we find the best result?
3. **LLM calls** - Total transformation attempts
4. **LLM ranking calls** - Phase 3 only
5. **Tree size** - Exploration breadth
6. **Success rate** - % of transforms that passed validation

## Configuration Reference

```python
@dataclass
class PriorConfig:
    use_puct: bool = True              # False = original random selection
    use_kb_weights: bool = True        # Use KB weight as baseline
    use_opportunity_detection: bool = True  # Boost for detected patterns
    use_llm_ranking: bool = False      # Phase 3: LLM ranking
    opportunity_boost: float = 1.5     # Multiplier for opportunities
    high_value_boost: float = 1.2      # Multiplier for high_value category
    diminishing_returns_penalty: float = 0.5  # Penalty for re-applying
    llm_timeout_ms: int = 5000         # LLM call timeout
    c_puct: float = 2.0                # PUCT exploration constant
    widening_factor: float = 1.5       # Progressive widening: k = factor × √visits
    min_widening: int = 2              # Minimum candidates to consider
```

Pre-defined configs:
- `RANDOM_CONFIG` - Original random selection (baseline)
- `PUCT_CONFIG` - Default PUCT (KB + opportunity, no LLM)
- `PUCT_LLM_CONFIG` - Full PUCT with LLM ranking

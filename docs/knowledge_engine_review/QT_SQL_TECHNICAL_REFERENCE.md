# Qt-SQL Technical Reference

> **Comprehensive implementation catalog for the qt-sql SQL optimization engine**

Version: 1.0 | Date: 2026-02-04 | Package: `packages/qt-sql/`

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Overview & Architecture](#2-overview--architecture)
3. [Static Analysis (AST Detector)](#3-static-analysis-ast-detector)
4. [Knowledge Base](#4-knowledge-base)
5. [DSPy Implementations](#5-dspy-implementations)
6. [DAG-Based Optimization](#6-dag-based-optimization)
7. [Adaptive Rewriter v5](#7-adaptive-rewriter-v5)
8. [MCTS Optimizer](#8-mcts-optimizer)
9. [Validation Pipeline](#9-validation-pipeline)
10. [Optimization Modes](#10-optimization-modes)
11. [Execution & Plan Analysis](#11-execution--plan-analysis)
12. [Report Generation](#12-report-generation)
13. [CLI & API](#13-cli--api)
14. [Testing](#14-testing)
15. [Performance & Benchmarks](#15-performance--benchmarks)
16. [Configuration](#16-configuration)
17. [Appendices](#appendices)

---

## 1. Executive Summary

**Qt-SQL** is an AI-powered SQL optimization engine that combines static analysis, machine learning, and automated validation to improve query performance. The system achieves 2-3x speedups on complex analytical queries through proven transformation patterns.

### Key Statistics

- **119 AST Rules**: Comprehensive static analysis across 13 categories
- **11 Optimization Transforms**: Verified patterns with TPC-DS SF100 benchmarks
- **5 Optimization Modes**: Standard, DAG, Adaptive, MCTS, DAG v2
- **47/99 Queries Optimized**: TPC-DS SF100 validation (Kimi K2.5)
- **2.81x Top Speedup**: Q1 correlated subquery decorrelation

### Architecture at a Glance

```
┌─────────────────────────────────────────────────────────────────┐
│                        INPUT: SQL QUERY                          │
└───────────────────────┬─────────────────────────────────────────┘
                        │
        ┌───────────────┴────────────────┐
        │                                │
        ▼                                ▼
┌───────────────┐              ┌──────────────────┐
│ STATIC        │              │ EXECUTION        │
│ ANALYSIS      │              │ PLAN ANALYSIS    │
│               │              │                  │
│ • 119 AST     │              │ • DuckDB         │
│   Rules       │              │   EXPLAIN        │
│ • Opportunity │              │ • Cost           │
│   Detection   │              │   Attribution    │
└───────┬───────┘              └────────┬─────────┘
        │                               │
        └───────────┬───────────────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │   OPTIMIZATION        │
        │                       │
        │ • DSPy Signatures     │
        │ • DAG v2/v3           │
        │ • Adaptive v5         │
        │ • MCTS Tree Search    │
        │ • Knowledge Base      │
        └───────────┬───────────┘
                    │
                    ▼
        ┌───────────────────────┐
        │   VALIDATION          │
        │                       │
        │ • Equivalence Check   │
        │ • Benchmarking        │
        │ • Row Comparison      │
        └───────────┬───────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────┐
│               OUTPUT: OPTIMIZED SQL + REPORT                     │
└─────────────────────────────────────────────────────────────────┘
```

### Navigation Guide

- **Developers adding new transforms**: See [Knowledge Base](#4-knowledge-base) and [MCTS Optimizer](#8-mcts-optimizer)
- **Understanding optimization strategies**: See [Optimization Modes](#10-optimization-modes)
- **Debugging validation failures**: See [Validation Pipeline](#9-validation-pipeline)
- **Extending AST rules**: See [Static Analysis](#3-static-analysis-ast-detector)
- **Adding LLM providers**: See [DSPy Implementations](#5-dspy-implementations)

---

## 2. Overview & Architecture

### Three-Layer Architecture

Qt-SQL processes queries through three distinct layers:

**Layer 1: Static Analysis**
- Parse SQL into AST using sqlglot
- Apply 119 detection rules across 13 categories
- Identify optimization opportunities
- No database execution required

**Layer 2: Optimization**
- Multiple strategies (DSPy, DAG, Adaptive, MCTS)
- LLM-powered rewrites with validation
- Knowledge base of proven patterns
- Few-shot learning with gold examples

**Layer 3: Validation**
- Semantic equivalence checking
- Performance benchmarking (3-run pattern)
- Regression detection
- Detailed error reporting

### Package Structure

```
packages/qt-sql/
├── qt_sql/
│   ├── analyzers/
│   │   └── ast_detector/           # 119 AST rules
│   │       ├── base.py             # ASTRule, ASTContext
│   │       ├── registry.py         # Rule registration
│   │       └── rules/              # Rule implementations
│   │
│   ├── optimization/
│   │   ├── knowledge_base.py       # 11 transforms (single source of truth)
│   │   ├── dspy_optimizer.py       # DSPy signatures & pipelines
│   │   ├── dag_v2.py               # DAG architecture (contracts, cost)
│   │   ├── dag_v3.py               # Gold example loading
│   │   ├── adaptive_rewriter_v5.py # Parallel fan-out
│   │   └── mcts/
│   │       ├── optimizer.py        # Main MCTS entry point
│   │       ├── tree.py             # PUCT selection
│   │       ├── policy.py           # DSPy policy network
│   │       ├── transforms.py       # 11 AST transforms
│   │       └── benchmark.py        # Trimmed-mean timing
│   │
│   ├── validation/
│   │   ├── equivalence_checker.py  # Row comparison with checksums
│   │   ├── benchmarker.py          # 3-run pattern timing
│   │   └── sql_validator.py        # Combined validation
│   │
│   ├── execution/
│   │   ├── duckdb_executor.py      # DuckDB execution
│   │   ├── postgres_executor.py    # PostgreSQL execution
│   │   └── plan_analyzer.py        # EXPLAIN plan parsing
│   │
│   └── reporting/
│       └── html_generator.py       # HTML report rendering
│
├── cli/                            # Command-line interface
├── api/                            # FastAPI backend
└── web/                            # React frontend

research/
└── knowledge_base/                 # Shared configs
    ├── model_configs/              # deepseek.yaml, groq.yaml
    └── db_configs/                 # duckdb.yaml, postgres.yaml
```

### Entry Points

**Python API:**
```python
from qt_sql.optimization.dspy_optimizer import optimize_query_with_validation

result = optimize_query_with_validation(
    original_sql="SELECT ...",
    execution_plan=plan_text,
    row_estimates=scan_info,
    db_path="/path/to/db.duckdb",
    provider="deepseek"
)
```

**CLI:**
```bash
qt-sql audit query.sql              # Static analysis only
qt-sql optimize query.sql           # Full optimization
qt-sql validate orig.sql opt.sql    # Validate rewrite
```

**API:**
```bash
POST /api/v1/optimize
{
  "sql": "SELECT ...",
  "mode": "dag",
  "provider": "deepseek"
}
```

---

## 3. Static Analysis (AST Detector)

The AST detector provides **context-aware** SQL analysis using 119 rules across 13 categories. Rules are implemented as classes inheriting from `ASTRule` and use sqlglot for parsing.

### Rule Categories & Counts

| Category | Rules | Examples |
|----------|-------|----------|
| **SELECT** | 6 | SELECT *, scalar subqueries, DISTINCT crutch |
| **WHERE** | 11 | Non-sargable predicates, OR vs IN, leading wildcards |
| **JOIN** | 11 | Cartesian joins, triangular patterns, inequality joins |
| **Subquery** | 7 | Correlated subqueries, deep nesting, repeated patterns |
| **UNION** | 3 | UNION without ALL, large chains, type mismatches |
| **ORDER BY** | 5 | ORDER BY without LIMIT, expression ordering |
| **CTE** | 6 | SELECT * in CTE, multi-ref CTEs, deep nesting |
| **Window** | 4 | ROW_NUMBER without ORDER, partition misuse |
| **Aggregation** | 9 | GROUP BY ordinals, HAVING without aggregates |
| **Cursor/Loop** | 3 | Cursor usage, WHILE loops, dynamic SQL |
| **Data Types** | 3 | String/numeric comparison, date as string |
| **PostgreSQL** | 10 | Large IN lists, LATERAL usage, JSONB indexing |
| **DuckDB** | 18 | QUALIFY, GROUP BY ALL, optimizer exploits |
| **Snowflake** | 20 | Clustering keys, micro-partition pruning |
| **Optimization POC** | 16 | Single-use CTE inline, EXISTS to semi-join |
| **Optimization Opportunity** | 11 | Patterns synced with knowledge_base |

**Total: 119 rules**

### Rule Classification

**High-Precision Rules (82 rules)** - Default for `qt-sql audit`:
- DuckDB optimizer exploits (SQL-DUCK-011 to 018)
- Optimization opportunity rules (QT-OPT-001 to 011)
- Structural optimization rules from POC rulebook
- Proven patterns from TPC-DS benchmarks

**Style Rules (37 rules)** - Optional with `--include-style`:
- SELECT * (often intentional)
- Implicit joins (style preference)
- GROUP BY ordinals (sometimes cleaner)
- ORDER BY without LIMIT (often intentional)

### ASTRule Structure

File: `packages/qt-sql/qt_sql/analyzers/ast_detector/base.py`

```python
@dataclass
class RuleMatch:
    rule_id: str
    severity: str  # error, warning, info
    message: str
    line: Optional[int]
    column: Optional[int]
    sql_snippet: Optional[str]
    fix_hint: Optional[str]

class ASTRule(ABC):
    rule_id: str        # e.g., "SQL-SEL-001"
    severity: str       # error, warning, info
    category: str       # "SELECT", "WHERE", etc.
    description: str

    @abstractmethod
    def check(self, node: exp.Expression, context: ASTContext) -> List[RuleMatch]:
        """Check a single AST node for violations."""
        pass
```

### Context-Aware Detection

The `ASTContext` class provides query-wide information:

```python
class ASTContext:
    query_sql: str
    tables: Set[str]              # All table references
    ctes: Dict[str, exp.CTE]      # CTE name -> definition
    has_aggregation: bool
    has_window: bool
    has_subquery: bool
    column_references: Dict[str, Set[str]]  # table -> columns
```

### Usage Example

```python
from qt_sql.analyzers.ast_detector import ASTDetector

detector = ASTDetector()
matches = detector.analyze(sql_query)

for match in matches:
    print(f"{match.rule_id}: {match.message}")
    if match.fix_hint:
        print(f"  Fix: {match.fix_hint}")
```

### File References

- **Registry**: `packages/qt-sql/qt_sql/analyzers/ast_detector/registry.py`
- **Base classes**: `packages/qt-sql/qt_sql/analyzers/ast_detector/base.py`
- **Rule implementations**: `packages/qt-sql/qt_sql/analyzers/ast_detector/rules/*.py`

---

## 4. Knowledge Base

The **Knowledge Base** is the single source of truth for all optimization patterns. Every service (MCTS, DSPy, Web UI, AST detection) imports from here.

File: `packages/qt-sql/qt_sql/optimization/knowledge_base.py`

### The 11 Optimization Transforms

| ID | Code | Name | Speedup | Queries | Category |
|---|---|---|---|---|---|
| `or_to_union` | QT-OPT-001 | OR to UNION ALL Decomposition | 2.98x | Q15, Q23, Q24, Q45 | high_value |
| `correlated_to_cte` | QT-OPT-002 | Correlated Subquery to Pre-computed CTE | 2.81x | Q1 | high_value |
| `date_cte_isolate` | QT-OPT-003 | Date CTE Isolation | 2.67x | Q6, Q15, Q27, Q39, Q92 | high_value |
| `push_pred` | QT-OPT-004 | Predicate Pushdown into CTE | 2.71x | Q27, Q93, Q9 | high_value |
| `consolidate_scans` | QT-OPT-005 | Scan Consolidation | 1.84x | Q90 | high_value |
| `multi_push_pred` | QT-OPT-006 | Multi-layer Predicate Pushdown | - | - | standard |
| `materialize_cte` | QT-OPT-007 | Materialize Repeated Subquery | - | Q95 | standard |
| `flatten_subq` | QT-OPT-008 | Flatten Subquery to JOIN | - | - | standard |
| `reorder_join` | QT-OPT-009 | Join Reordering | - | - | standard |
| `inline_cte` | QT-OPT-010 | Inline Single-Use CTE | - | - | standard |
| `remove_redundant` | QT-OPT-011 | Remove Redundant Operations | - | - | standard |

### TransformPattern Structure

```python
@dataclass
class TransformPattern:
    id: TransformID                      # Canonical ID for MCTS
    code: str                            # Display code for UI (QT-OPT-001)
    name: str                            # Human-readable name
    description: str                     # What this pattern does
    trigger: str                         # How to detect
    rewrite_hint: str                    # How to fix (for LLM prompts)
    benchmark_queries: list[str]         # TPC-DS queries where verified
    avg_speedup: float                   # Normalized 0-1 from benchmarks
    category: str                        # "high_value" or "standard"
    enabled: bool                        # Whether active
```

### Impact Scoring Formula

```python
# avg_speedup = (speedup - 1) * num_queries / 10
# Measures total impact: 1.5x on 10 queries (5.0) > 3x on 1 query (2.0)

# Examples:
or_to_union:         (2.98-1) * 4 / 10 = 0.79  # Highest: great speedup + coverage
date_cte_isolate:    (2.67-1) * 5 / 10 = 0.84  # Best: good speedup + broad coverage
correlated_to_cte:   (2.81-1) * 1 / 10 = 0.18  # High speedup but narrow
```

### Opportunity Detection

The knowledge base provides lightweight pattern detection:

```python
from qt_sql.optimization.knowledge_base import detect_opportunities

opportunities = detect_opportunities(sql)

for opp in opportunities:
    print(f"{opp.pattern.code}: {opp.pattern.name}")
    print(f"  Trigger: {opp.trigger_match}")
    print(f"  Expected: {opp.pattern.benchmark_queries}")
```

Detection patterns include:
- OR conditions in WHERE clause
- Correlated subqueries with aggregates
- Date dimension filtering with fact tables
- Multiple scans of same table
- Repeated subquery patterns

### Usage in Prompts

```python
from qt_sql.optimization.knowledge_base import format_opportunities_for_prompt

prompt_text = format_opportunities_for_prompt(opportunities)

# Output includes:
# - Pattern code and name
# - Trigger description
# - Rewrite hint
# - Expected speedup and benchmark queries
```

### Accessor Functions

```python
from qt_sql.optimization.knowledge_base import (
    get_transform,              # Get by ID
    get_all_transforms,         # Get all (filtered by enabled/category)
    get_high_value_transforms,  # Get proven 2x+ patterns
    get_transform_ids,          # Get all IDs for MCTS
)
```

---

## 5. DSPy Implementations

Qt-SQL uses **DSPy** (Declarative Self-improving Python) for structured LLM interactions. DSPy provides signature-based prompting with validation and retry logic.

File: `packages/qt-sql/qt_sql/optimization/dspy_optimizer.py`

### Three Core Signatures

#### 1. SQLOptimizer (Full-Query Rewrite)

```python
class SQLOptimizer(dspy.Signature):
    """Optimize SQL query for better execution performance."""

    # Inputs
    original_query: str = dspy.InputField(
        desc="The original SQL query to optimize"
    )
    execution_plan: str = dspy.InputField(
        desc="Parsed execution plan showing operator costs and row counts"
    )
    row_estimates: str = dspy.InputField(
        desc="Table scan statistics: table name, rows scanned, filter status"
    )
    optimization_hints: str = dspy.InputField(
        desc="Detected optimization opportunities with rewrite patterns",
        default=""
    )
    constraints: str = dspy.InputField(
        desc="Model and DB-specific constraints to follow",
        default=""
    )

    # Outputs
    optimized_query: str = dspy.OutputField(
        desc="The optimized SQL query with identical semantics"
    )
    optimization_rationale: str = dspy.OutputField(
        desc="Explanation of what was optimized and why it improves performance"
    )
```

**Use case**: Rewrite entire query when structure needs major changes.

#### 2. SQLDagOptimizer (Node-Level Rewrites)

```python
class SQLDagOptimizer(dspy.Signature):
    """Optimize SQL by rewriting specific DAG nodes."""

    # Inputs
    query_dag: str = dspy.InputField(
        desc="DAG structure showing nodes (CTEs, subqueries, main_query) and dependencies"
    )
    node_sql: str = dspy.InputField(
        desc="SQL for each node in the DAG"
    )
    execution_plan: str = dspy.InputField(...)
    optimization_hints: str = dspy.InputField(...)
    constraints: str = dspy.InputField(...)

    # Outputs
    rewrites: str = dspy.OutputField(
        desc='JSON object: {"node_id": "new SELECT statement", ...}'
    )
    explanation: str = dspy.OutputField(...)
```

**Use case**: Targeted rewrites for large queries (reduces token usage).

#### 3. SQLOptimizerWithFeedback (Retry with Context)

```python
class SQLOptimizerWithFeedback(dspy.Signature):
    """Optimize SQL using a DIFFERENT strategy after a failed attempt."""

    # Additional inputs beyond SQLOptimizer
    previous_attempt: str = dspy.InputField(
        desc="Previous optimization that FAILED - do NOT repeat this approach"
    )
    failure_reason: str = dspy.InputField(
        desc="Why it failed (e.g. wrong row count)"
    )

    # Output warns about repetition
    optimized_query: str = dspy.OutputField(
        desc="A DIFFERENT optimization using a different strategy. If unsure, return the original query unchanged."
    )
```

**Use case**: Retry after validation failure with failure context.

### ValidatedOptimizationPipeline

The main DSPy pipeline with validation and retry logic:

```python
class ValidatedOptimizationPipeline(dspy.Module):
    def __init__(
        self,
        validator_fn=None,        # (orig_sql, opt_sql) -> (correct, error)
        max_retries: int = 2,
        model_name: str = None,   # For model config
        db_name: str = None,      # For DB config
        use_few_shot: bool = True,
        num_examples: int = 3,
        use_assertions: bool = True
    ):
        super().__init__()
        self.optimizer = dspy.ChainOfThought(SQLOptimizer)
        self.retry_optimizer = dspy.ChainOfThought(SQLOptimizerWithFeedback)
        self.validator_fn = validator_fn
        self.max_retries = max_retries
        self.constraints = build_system_prompt(model_name, db_name)

        # Load few-shot examples
        if use_few_shot:
            examples = load_gold_examples(num_examples)
            self.optimizer.predict.demos = examples
```

**Flow:**
1. Generate optimized query with few-shot examples
2. Validate semantics and performance
3. If fails, retry with failure feedback (up to max_retries)
4. Apply soft constraints via dspy.Suggest assertions
5. Return best valid result or failure

### Configuration System

**Model Configs** (`research/knowledge_base/model_configs/*.yaml`):

```yaml
# deepseek.yaml
constraints:
  - "Avoid overly complex nested CTEs - prefer flat structure"
  - "DeepSeek-V3 is strong at predicate pushdown"

strengths:
  - "Excellent at identifying redundant operations"
  - "Strong logical reasoning for join reordering"

failure_patterns:
  - "May over-apply CTE materialization"

prompt_suffix: |
  DEEPSEEK-V3 STRENGTHS:
  - Predicate pushdown and filter optimization
  - Join reordering and redundant operation removal

  CAUTION:
  - Avoid over-nesting CTEs
```

**DB Configs** (`research/knowledge_base/db_configs/*.yaml`):

```yaml
# duckdb.yaml
hints:
  - text: "DuckDB automatically pushes filters through JOINs"
    category: "optimizer"
  - text: "Use QUALIFY for window function filtering"
    category: "syntax"

syntax_notes:
  - "GROUP BY ALL infers grouping columns"
  - "ASOF JOIN for temporal queries"

limitations:
  - "No materialized views"
  - "Limited optimizer hints"

strengths:
  - "Excellent at columnar scan optimization"
  - "Strong parallel execution"
```

### Few-Shot Examples

Gold examples provide proven patterns for LLM learning:

```python
def load_gold_examples(num_examples: int = 3) -> List[dspy.Example]:
    """Load verified examples from research/knowledge_base/duckdb/"""
    examples = [
        dspy.Example(
            original_query="SELECT ... WHERE col > (SELECT AVG(...) ...)",
            execution_plan="...",
            row_estimates="...",
            optimized_query="WITH avg_cte AS (...) SELECT ... JOIN avg_cte ...",
            optimization_rationale="Decorrelated subquery to pre-computed CTE"
        ).with_inputs("original_query", "execution_plan", "row_estimates")
    ]
    return examples[:num_examples]
```

### Usage Example

```python
from qt_sql.optimization.dspy_optimizer import (
    configure_lm,
    create_duckdb_validator,
    ValidatedOptimizationPipeline
)

# Configure LLM
configure_lm(provider="deepseek")

# Create validator
validator = create_duckdb_validator(db_path="data.duckdb")

# Create pipeline
pipeline = ValidatedOptimizationPipeline(
    validator_fn=validator,
    max_retries=2,
    model_name="deepseek",
    db_name="duckdb"
)

# Optimize
result = pipeline(
    query=original_sql,
    plan=execution_plan,
    rows=row_estimates
)

print(f"Optimized: {result.optimized_sql}")
print(f"Valid: {result.correct}")
print(f"Attempts: {result.attempts}")
```

---

## 6. DAG-Based Optimization

DAG-based optimization represents queries as directed acyclic graphs (CTEs, subqueries, main query) and performs targeted node-level rewrites. This approach reduces token usage and preserves unchanged portions exactly.

### Evolution: DAG v1 → v2 → v3

| Feature | v1 | v2 | v3 |
|---------|----|----|-----|
| Node structure | Basic CTE refs | **Contracts** (columns, grain, predicates) | Same as v2 |
| Rewrite granularity | Full SQL | **Atomic rewrite sets** (multi-node) | Same as v2 |
| Slicing | Full DAG | **Subgraph** (target + 1-hop neighbors) | Same as v2 |
| Transform filtering | All transforms | **Allowlist** (7 transforms) | Same as v2 |
| Cost analysis | None | **Cost attribution** per node | Same as v2 |
| Example selection | Random | Gold examples | **KB pattern matching** |
| Example rotation | None | None | **Adaptive rotation** on failure |

### DAG v2 Architecture

File: `packages/qt-sql/qt_sql/optimization/dag_v2.py`

#### Node Contracts

Each DAG node has a contract specifying what it promises to provide:

```python
@dataclass
class NodeContract:
    node_id: str
    output_columns: List[str]      # Columns this node outputs
    grain: List[str]                # Grouping keys (empty if not aggregated)
    required_predicates: List[str]  # Predicates that must stay
    nullable_columns: List[str]
```

**Purpose**: Ensures rewrites preserve semantic guarantees.

#### Node Usage

Tracks how nodes are consumed downstream:

```python
@dataclass
class NodeUsage:
    node_id: str
    downstream_refs: List[str]  # Columns actually used by consumers
    consumers: List[str]         # Node IDs that reference this node
```

**Purpose**: Safe projection pruning (remove unused columns).

#### Cost Attribution

Maps execution plan operators to DAG nodes:

```python
@dataclass
class NodeCost:
    node_id: str
    cost_pct: float              # Percentage of total query cost
    row_estimate: int
    operators: List[str]         # Plan operators (SEQ_SCAN, HASH_JOIN, etc.)
    has_filter: bool
    join_type: Optional[str]     # hash, nested_loop, merge
```

**Purpose**: Identify hot nodes for optimization targeting.

#### Rewrite Sets

Atomic coordinated changes across multiple nodes:

```python
@dataclass
class RewriteSet:
    id: str                      # e.g., "rs_01"
    nodes: Dict[str, str]        # node_id -> new SQL
    invariants_kept: List[str]   # What semantics are preserved
    transform_type: str          # decorrelate, pushdown, or_to_union
    expected_speedup: str        # e.g., "2.5x"
    risk: str                    # "low", "medium", "high"
```

**Purpose**: Multi-node transformations as single atomic unit.

#### Transform Allowlist

Only 7 transforms permitted (reduces hallucination):

```python
ALLOWED_TRANSFORMS = [
    "pushdown",           # Push filters into CTEs
    "decorrelate",        # Correlated subquery → window/join
    "or_to_union",        # OR conditions → UNION ALL
    "in_to_exists",       # IN subquery → EXISTS
    "projection_prune",   # Remove unused columns
    "early_filter",       # Add filter CTE for selective predicates
    "semantic_rewrite",   # Semantics-aware with domain assumptions
]
```

#### Subgraph Slicing

Extract target node + 1-hop neighbors for focused rewrites:

```python
class SubgraphSlicer:
    def get_slice(self, target_id: str) -> Dict[str, DagNode]:
        """Get target node + 1-hop neighbors (parents + children)."""
        slice_nodes = {target_id: self.dag.nodes[target_id]}

        # Add parents (nodes target references)
        for ref in target.refs:
            slice_nodes[ref] = self.dag.nodes[ref]

        # Add children (nodes that reference target)
        for node_id, node in self.dag.nodes.items():
            if target_id in node.refs:
                slice_nodes[node_id] = node

        return slice_nodes
```

#### DagBuilder

Parses SQL into DAG structure:

```python
dag = DagBuilder(sql).build()

# Result: QueryDag
# - nodes: Dict[str, DagNode]  (CTEs + main_query)
# - edges: List[Tuple[str, str]]  (dependencies)
# - contracts, usage, costs computed automatically
```

### DAG v3 Pattern Matching

File: `packages/qt-sql/qt_sql/optimization/dag_v3.py`

#### Gold Example Loading

Examples stored as JSON files:

```
packages/qt-sql/qt_sql/optimization/examples/
├── or_to_union.json          # QT-OPT-001
├── decorrelate.json          # QT-OPT-002
├── date_cte_isolate.json     # QT-OPT-003
└── ...
```

Example structure:

```json
{
  "id": "decorrelate",
  "name": "Correlated Subquery to Pre-computed CTE",
  "description": "Replace correlated aggregate with CTE + JOIN",
  "benchmark_queries": ["Q1"],
  "verified_speedup": "2.81x",
  "example": {
    "opportunity": "DECORRELATE + PUSHDOWN",
    "input_slice": "...",
    "output": {
      "rewrite_sets": [...]
    },
    "key_insight": "Push filter EARLY, compute average as SEPARATE CTE"
  }
}
```

#### KB-to-Example Mapping

```python
KB_TO_EXAMPLE = {
    "or_to_union": "or_to_union",
    "correlated_to_cte": "decorrelate",
    "date_cte_isolate": "date_cte_isolate",
    "push_pred": "quantity_range_pushdown",
    "consolidate_scans": "early_filter",
    "multi_push_pred": "multi_push_predicate",
    "materialize_cte": "materialize_cte",
    "flatten_subq": "flatten_subquery",
    "reorder_join": "reorder_join",
    "inline_cte": "inline_cte",
    "remove_redundant": "remove_redundant",
}
```

#### Adaptive Example Selection

```python
class DagV3ExampleSelector:
    def __init__(self, sql: str, examples_per_prompt: int = 3):
        # Detect KB patterns in SQL
        self.examples = get_matching_examples(sql)  # Matched first, by score
        self.current_index = 0
        self.examples_per_prompt = examples_per_prompt

    def rotate(self) -> Optional[GoldExample]:
        """Move to next example batch after failure."""
        self.current_index += self.examples_per_prompt
        return self.current_example

    def get_prompt(self, base_prompt: str, ...) -> str:
        """Build prompt with current examples."""
        return build_prompt_with_examples(
            base_prompt,
            self.current_examples,
            ...
        )
```

**Strategy**:
1. First prompt: Top 3 examples matching detected KB patterns
2. On failure: Rotate to next 3 examples
3. Ensures 5 retries cover diverse strategies

### Rewrite Assembler

Applies rewrites and reconstructs SQL:

```python
class RewriteAssembler:
    def apply_rewrite_set(self, rewrite_set: RewriteSet) -> str:
        """Apply rewrites and produce optimized SQL."""

        # Handle NEW CTEs added by LLM
        new_nodes = dict(rewrite_set.nodes)

        # Fill in unchanged nodes
        for node_id, node in self.dag.nodes.items():
            if node_id not in new_nodes:
                new_nodes[node_id] = node.sql

        # Reassemble with dependency ordering
        return self._assemble_sql(new_nodes)

    def _assemble_sql(self, nodes: Dict[str, str]) -> str:
        """Reassemble full SQL with topological sort."""
        # Build dependency graph
        # Topological sort CTEs
        # Construct: WITH cte1 AS (...), cte2 AS (...) SELECT ...
```

**Handles**:
- New CTEs added by LLM
- CTE dependency ordering (topological sort)
- Comment stripping (LLM echoes)
- Malformed WITH clauses

### Usage Example

```python
from qt_sql.optimization.dag_v2 import DagV2Pipeline

# Build pipeline
pipeline = DagV2Pipeline(sql, plan_json=None)

# Get optimization prompt
prompt = pipeline.get_prompt(target_nodes=["hot_cte"])

# Apply LLM response
optimized_sql = pipeline.apply_response(llm_response_json)
```

---

## 7. Adaptive Rewriter v5

The Adaptive Rewriter v5 implements **parallel fan-out** with 5 workers to maximize coverage of the optimization space. It's the fastest mode for finding valid optimizations.

File: `packages/qt-sql/qt_sql/optimization/adaptive_rewriter_v5.py`

### Architecture: Parallel Fan-Out

```
                          ┌────────────────────┐
                          │   INPUT: SQL       │
                          │   + Sample DB      │
                          └──────────┬─────────┘
                                     │
                    ┌────────────────┴────────────────┐
                    │   Get KB-matched Examples       │
                    │   Split into 3-example batches  │
                    └────────────────┬────────────────┘
                                     │
            ┌────────────────────────┼───────────────────────┐
            │                        │                       │
    ┌───────▼───────┐       ┌───────▼───────┐      ┌───────▼───────┐
    │   Worker 1    │       │   Worker 2    │      │   Worker 3    │
    │               │       │               │      │               │
    │ Examples 0-2  │       │ Examples 3-5  │      │ Examples 6-8  │
    │ (Coverage)    │       │ (Coverage)    │      │ (Coverage)    │
    └───────┬───────┘       └───────┬───────┘      └───────┬───────┘
            │                        │                       │
    ┌───────▼───────┐       ┌───────────────────────────────┘
    │   Worker 4    │       │
    │               │       │
    │ Examples 9-11 │       │
    │ (Coverage)    │       │
    └───────┬───────┘       │
            │               │
    ┌───────▼───────────────▼─────┐
    │      Worker 5 (Explore)     │
    │                              │
    │  No examples                 │
    │  Full EXPLAIN plan           │
    │  "Be adversarial"            │
    └──────────────┬───────────────┘
                   │
          ┌────────┴────────┐
          │ All workers     │
          │ complete        │
          │ in parallel     │
          └────────┬────────┘
                   │
          ┌────────▼────────┐
          │ Validate on     │
          │ sample DB       │
          │ (parallel)      │
          └────────┬────────┘
                   │
          ┌────────▼────────┐
          │ Return first    │
          │ valid or all    │
          │ if none pass    │
          └─────────────────┘
```

### Worker Strategies

**Coverage Workers (1-4)**: Each gets a different batch of 3 KB-matched examples
- Worker 1: Examples 0-2 (highest-scoring KB matches)
- Worker 2: Examples 3-5
- Worker 3: Examples 6-8
- Worker 4: Examples 9-11

**Explore Worker (5)**: No examples, adversarial mode
- No gold examples (pure LLM reasoning)
- Full EXPLAIN plan details
- Prompt: "Be adversarial. Exploit transforms the DB engine is unlikely to do automatically."
- Goal: Discover novel optimizations beyond known patterns

### Two Modes: JSON vs DSPy

**JSON Mode** (`optimize_v5_json`):
- Uses raw JSON prompts with gold examples
- Faster (no DSPy overhead)
- Less structured output

**DSPy Mode** (`optimize_v5_dspy`):
- Uses DSPy signatures for structured I/O
- Gold examples loaded as DSPy demos
- Better retry handling with type checking

### Worker Implementation

```python
def _worker_json(
    worker_id: int,
    sql: str,
    base_prompt: str,
    plan_summary: str,
    examples: List[GoldExample],
    sample_db: str,
    retry: bool = True,
    explore: bool = False,
    plan_details: Optional[str] = None,
) -> CandidateResult:
    """Single worker: Build prompt → LLM → Validate → Retry if needed."""

    # Build prompt with examples
    if explore:
        history = "Be adversarial. Exploit transforms DB won't do automatically."
        if plan_details:
            history += f"\n## Full Plan\n{plan_details}"

    full_prompt = build_prompt_with_examples(
        base_prompt, examples, plan_summary, history
    )

    # Call LLM
    response = lm(full_prompt)

    # Parse response
    optimized_sql = pipeline.apply_response(response)

    # Validate on sample DB
    result = validator.validate(sql, optimized_sql)

    # Retry once if failed
    if result.status != PASS and retry:
        history = f"Previous attempt FAILED: {error}\nTry DIFFERENT approach."
        full_prompt = build_prompt_with_examples(...)
        response = lm(full_prompt)
        optimized_sql = pipeline.apply_response(response)
        result = validator.validate(sql, optimized_sql)

    return CandidateResult(...)
```

### ThreadPoolExecutor Usage

```python
with ThreadPoolExecutor(max_workers=5) as pool:
    tasks = []

    # Coverage workers (4)
    for i, batch in enumerate(coverage_batches):
        tasks.append(pool.submit(
            _worker_json,
            i + 1,           # worker_id
            sql,
            base_prompt,
            plan_summary,
            batch,           # 3 examples
            sample_db,
            True,            # retry
            False,           # not explore
            None,
        ))

    # Explore worker (1)
    tasks.append(pool.submit(
        _worker_json,
        5,               # worker_id
        sql,
        base_prompt,
        plan_summary,
        [],              # no examples
        sample_db,
        True,
        True,            # explore mode
        plan_text,       # full EXPLAIN
    ))

    # Wait for all
    results = [t.result() for t in as_completed(tasks)]
```

### Queue Mode: Sequential Full DB Validation

`optimize_v5_json_queue()` runs v5 in two phases:

**Phase 1**: Parallel on sample DB (fast)
```python
valid_candidates = [r for r in results if r.status == PASS]
```

**Phase 2**: Sequential on full DB (accurate)
```python
full_validator = SQLValidator(database=full_db)
for cand in valid_candidates:
    full_result = full_validator.validate(sql, cand.optimized_sql)
    if full_result.status == PASS and full_result.speedup >= target_speedup:
        return cand  # First to hit target wins
```

**Benefits**:
- Sample DB screens out bad candidates quickly (parallel)
- Full DB validates only promising candidates (sequential to avoid timeout)
- Early exit when target speedup achieved

### Usage Example

```python
from qt_sql.optimization.adaptive_rewriter_v5 import optimize_v5_json

# Simple mode: best candidate from sample DB
result = optimize_v5_json(
    sql=query,
    sample_db="sample.duckdb",
    max_workers=5
)

print(f"Worker {result.worker_id}: {result.speedup:.2f}x")
print(f"Status: {result.status.value}")

# Queue mode: full DB validation
valid, full_results, winner = optimize_v5_json_queue(
    sql=query,
    sample_db="sample.duckdb",
    full_db="full.duckdb",
    max_workers=5,
    target_speedup=2.0
)

if winner:
    print(f"Winner: {winner.full_speedup:.2f}x on full DB")
```

---

## 8. MCTS Optimizer

The Monte Carlo Tree Search (MCTS) optimizer implements hybrid tree search combining **deterministic AST transforms** with **DSPy policy priors** for action selection. It uses PUCT (Polynomial Upper Confidence Trees) for exploration-exploitation balance.

Files:
- `packages/qt-sql/qt_sql/optimization/mcts/optimizer.py` - Main entry point
- `packages/qt-sql/qt_sql/optimization/mcts/tree.py` - PUCT tree implementation
- `packages/qt-sql/qt_sql/optimization/mcts/transforms.py` - 11 AST transforms
- `packages/qt-sql/qt_sql/optimization/mcts/policy.py` - DSPy action priors
- `packages/qt-sql/qt_sql/optimization/mcts/benchmark.py` - Trimmed-mean timing

### Architecture

| Component | Purpose | Implementation |
|-----------|---------|----------------|
| **MCTSSQLOptimizer** | Main entry point | Orchestrates tree search |
| **MCTSTree** | Tree state | PUCT selection, backpropagation |
| **PolicyNetwork** | Action priors | DSPy signatures for P(action\|state) |
| **BenchmarkRunner** | Reward function | 3-run trimmed-mean timing |
| **Transforms** | Action space | 11 deterministic AST rewrites |

### PUCT Selection Formula

```python
def _select_child(self, node: MCTSNode) -> MCTSNode:
    """Select child using PUCT formula with DSPy priors."""

    best_score = -float('inf')
    best_child = None

    for child in node.children:
        if child.visit_count == 0:
            # Prioritize unvisited with prior boost
            score = float('inf')
        else:
            # PUCT formula
            exploit = child.total_reward / child.visit_count
            explore = self.config.c_puct * child.prior * (
                math.sqrt(node.visit_count) / (1 + child.visit_count)
            )
            score = exploit + explore

        if score > best_score:
            best_score = score
            best_child = child

    return best_child
```

**Parameters**:
- `c_puct`: Exploration constant (default 1.0)
- `exploit`: Average reward from visits
- `explore`: Prior-weighted exploration bonus
- `prior`: DSPy action prior P(action|state)

### 11 Transform Library

Transforms are mapped 1:1 to knowledge base patterns:

| Transform ID | KB Code | Function | Description |
|-------------|---------|----------|-------------|
| `push_pred` | QT-OPT-004 | `apply_push_predicate` | Push WHERE filters into CTEs |
| `reorder_join` | QT-OPT-009 | `apply_reorder_join` | Put selective tables first |
| `materialize_cte` | QT-OPT-007 | `apply_materialize_cte` | Add MATERIALIZED hint |
| `inline_cte` | QT-OPT-010 | `apply_inline_cte` | Inline single-use CTEs |
| `flatten_subq` | QT-OPT-008 | `apply_flatten_subquery` | EXISTS → SEMI JOIN |
| `remove_redundant` | QT-OPT-011 | `apply_remove_redundant` | Remove DISTINCT, ORDER BY |
| `multi_push_pred` | QT-OPT-006 | `apply_multi_push_predicate` | Push through multiple CTEs |
| `or_to_union` | QT-OPT-001 | `apply_or_to_union` | OR → UNION ALL branches |
| `correlated_to_cte` | QT-OPT-002 | `apply_correlated_to_cte` | Decorrelate via CTE |
| `date_cte_isolate` | QT-OPT-003 | `apply_date_cte_isolation` | Early date filter CTE |
| `consolidate_scans` | QT-OPT-005 | `apply_consolidate_scans` | Merge table scans |

Example transform:

```python
def apply_push_predicate(sql: str) -> Optional[str]:
    """Push WHERE filters into CTEs."""
    try:
        parsed = sqlglot.parse_one(sql, dialect="duckdb")

        # Find main WHERE predicates
        main_where = parsed.find(exp.Where)
        if not main_where:
            return None

        # Find CTEs
        ctes = list(parsed.find_all(exp.CTE))
        if not ctes:
            return None

        # For each predicate, try pushing into CTE
        for predicate in main_where.find_all(exp.Predicate):
            # Check if predicate references CTE columns
            # If yes, inject into CTE WHERE clause
            ...

        return parsed.sql(dialect="duckdb")
    except Exception:
        return None
```

### Iteration Loop

```python
for iteration in range(max_iterations):
    # 1. SELECT: Traverse tree using PUCT
    node = tree.root
    while not node.is_leaf():
        node = tree._select_child(node)

    # 2. EXPAND: Generate children via transforms
    if node.visit_count > 0 and not node.is_terminal:
        tree._expand(node)
        if node.children:
            node = random.choice(node.children)

    # 3. SIMULATE: Evaluate node
    reward = tree._simulate(node)

    # 4. BACKPROPAGATE: Update ancestors
    tree._backpropagate(node, reward)
```

### Policy Network (DSPy Priors)

Uses DSPy to predict which transforms are most promising:

```python
class PolicyNetwork:
    def __init__(self, config: PolicyConfig):
        self.config = config
        self.signature = dspy.ChainOfThought("sql_text, available_transforms -> action_scores")

    def predict_priors(self, sql: str, transform_ids: List[str]) -> Dict[str, float]:
        """Predict prior probabilities for each transform."""

        if not self.config.use_policy:
            # Uniform priors
            return {tid: 1.0 / len(transform_ids) for tid in transform_ids}

        # Get KB pattern matches
        opportunities = detect_opportunities(sql)
        matched_patterns = {opp.pattern.id for opp in opportunities}

        # Boost matched patterns
        priors = {}
        for tid in transform_ids:
            if tid in matched_patterns:
                priors[tid] = self.config.prior_boost  # 2.0
            else:
                priors[tid] = 1.0

        # Normalize
        total = sum(priors.values())
        return {tid: p / total for tid, p in priors.items()}
```

**Strategy**:
- Detect KB patterns in SQL
- Boost priors for matched transforms (2x)
- Fallback to uniform if no matches

### Benchmark Runner

3-run pattern with trimmed-mean:

```python
class BenchmarkRunner:
    def measure(self, sql: str, runs: int = 3) -> float:
        """Return average time (seconds), excluding first run."""
        times = []

        for i in range(runs):
            start = time.perf_counter()
            self.conn.execute(sql).fetchall()
            elapsed = time.perf_counter() - start
            times.append(elapsed)

        # Discard first (warmup), average remaining
        if len(times) > 1:
            return sum(times[1:]) / (len(times) - 1)
        return times[0]
```

### Tree Statistics

```python
tree.get_stats() = {
    "total_nodes": 147,
    "max_depth": 8,
    "best_speedup": 2.45,
    "total_visits": 532,
    "avg_branching_factor": 3.2,
    "transforms_applied": ["push_pred", "or_to_union"],
}
```

### Usage Example

```python
from qt_sql.optimization.mcts.optimizer import MCTSSQLOptimizer
from qt_sql.optimization.mcts.tree import MCTSConfig

config = MCTSConfig(
    c_puct=1.0,              # Exploration constant
    max_depth=10,
    prior_boost=2.0,
    use_cache=True,
)

with MCTSSQLOptimizer(
    database="data.duckdb",
    mcts_config=config
) as optimizer:
    result = optimizer.optimize(
        query=sql,
        max_iterations=30
    )

print(f"Speedup: {result.speedup:.2f}x")
print(f"Transforms: {result.transforms_applied}")
print(f"Iterations: {result.iterations}")
print(f"Tree stats: {result.tree_stats}")
```

---

## 9. Validation Pipeline

The validation pipeline ensures optimized queries are **semantically equivalent** and **performant** through a two-phase process: equivalence checking + performance benchmarking.

Files:
- `packages/qt-sql/qt_sql/validation/equivalence_checker.py` - Checksum & value comparison
- `packages/qt-sql/qt_sql/validation/benchmarker.py` - 3-run timing pattern
- `packages/qt-sql/qt_sql/validation/sql_validator.py` - Combined validation
- `packages/qt-sql/qt_sql/validation/schemas.py` - Result types

### Two-Phase Validation

```
┌────────────────────────────────────────────────────┐
│ Phase 1: EQUIVALENCE CHECKING                      │
├────────────────────────────────────────────────────┤
│ 1. Execute both queries                            │
│ 2. Compare row counts (fast fail)                  │
│ 3. Compute MD5 checksums of sorted rows (fast)     │
│ 4. If mismatch: detailed value comparison          │
│    - Float tolerance (1e-9)                        │
│    - NULL handling                                 │
│    - NaN/Inf handling                              │
└────────────┬───────────────────────────────────────┘
             │ ✓ Equivalent
             ▼
┌────────────────────────────────────────────────────┐
│ Phase 2: PERFORMANCE BENCHMARKING                  │
├────────────────────────────────────────────────────┤
│ 1. Run original 3 times (warmup + 2 measured)     │
│ 2. Run optimized 3 times (warmup + 2 measured)    │
│ 3. Calculate trimmed-mean (exclude first run)     │
│ 4. Compute speedup = orig_time / opt_time         │
└────────────┬───────────────────────────────────────┘
             │
             ▼
     VALIDATION RESULT
```

### Equivalence Checker

File: `packages/qt-sql/qt_sql/validation/equivalence_checker.py`

#### Checksum Comparison (Fast)

```python
class EquivalenceChecker:
    def compute_checksum(self, rows: list[dict]) -> str:
        """Compute MD5 checksum of sorted, normalized rows."""

        # Get consistent column order
        columns = sorted(rows[0].keys())

        # Normalize all rows
        normalized = []
        for row in rows:
            norm_row = tuple(
                self._normalize_value(row.get(col))
                for col in columns
            )
            normalized.append(norm_row)

        # Sort for deterministic order
        normalized.sort()

        # MD5 hash
        serialized = json.dumps(normalized, sort_keys=True)
        return hashlib.md5(serialized.encode()).hexdigest()
```

**Normalization rules**:
- NULL → `"__NULL__"`
- NaN → `"__NAN__"`
- Inf → `"__INF_pos__"` or `"__INF_neg__"`
- Float → rounded to 9 decimals
- Integer → zero-padded for string sorting
- String → stripped whitespace

#### Value Comparison (Detailed)

Only runs if checksum fails:

```python
def compare_values(
    self,
    original_rows: list[dict],
    optimized_rows: list[dict],
    max_differences: int = 10,
) -> ValueComparisonResult:
    """Compare values row-by-row with float tolerance."""

    # Sort both for order-independent comparison
    original_sorted = sorted(original_rows, key=lambda r: self._row_to_tuple(r, columns))
    optimized_sorted = sorted(optimized_rows, key=lambda r: self._row_to_tuple(r, columns))

    differences = []
    for i, (orig_row, opt_row) in enumerate(zip(original_sorted, optimized_sorted)):
        for col in columns:
            if not self._values_equal(orig_row[col], opt_row[col]):
                differences.append(ValueDifference(
                    row_index=i,
                    column=col,
                    original_value=orig_row[col],
                    optimized_value=opt_row[col]
                ))

                if len(differences) >= max_differences:
                    break

    return ValueComparisonResult(
        match=len(differences) == 0,
        differences=differences,
        total_compared=len(original_rows) * len(columns)
    )
```

**Float tolerance**: `abs(v1 - v2) <= 1e-9`

### Benchmarker

File: `packages/qt-sql/qt_sql/validation/benchmarker.py`

#### 3-Run Pattern

```python
class QueryBenchmarker:
    def benchmark(self, sql: str, runs: int = 3) -> float:
        """Benchmark query with warmup run excluded."""
        times = []

        for i in range(runs):
            start = time.perf_counter()
            self.conn.execute(sql).fetchall()
            end = time.perf_counter()
            times.append((end - start) * 1000)  # milliseconds

        # Trimmed-mean: exclude first run (warmup)
        if len(times) > 1:
            return sum(times[1:]) / (len(times) - 1)
        return times[0]
```

**Why 3 runs?**
- First run: warmup (page cache, JIT compilation)
- Second/third runs: measured (stable performance)
- Trimmed-mean: average of runs 2-3

### SQLValidator (Combined)

File: `packages/qt-sql/qt_sql/validation/sql_validator.py`

```python
class SQLValidator:
    def validate(
        self,
        original_sql: str,
        optimized_sql: str,
        benchmark: bool = True
    ) -> ValidationResult:
        """Full validation: equivalence + performance."""

        # Execute both queries
        try:
            orig_rows = self.execute(original_sql)
            opt_rows = self.execute(optimized_sql)
        except Exception as e:
            return ValidationResult(
                status=ValidationStatus.EXECUTION_ERROR,
                errors=[str(e)],
                ...
            )

        # Phase 1: Equivalence
        checker = EquivalenceChecker(float_tolerance=1e-9)

        # Row count check
        if len(orig_rows) != len(opt_rows):
            return ValidationResult(
                status=ValidationStatus.ROW_COUNT_MISMATCH,
                errors=[f"Row count: {len(orig_rows)} vs {len(opt_rows)}"],
                ...
            )

        # Checksum check
        checksum_result = checker.compare_checksums(orig_rows, opt_rows)
        if not checksum_result.match:
            # Detailed comparison
            value_result = checker.compare_values(orig_rows, opt_rows)
            if not value_result.match:
                return ValidationResult(
                    status=ValidationStatus.VALUE_MISMATCH,
                    errors=[f"{len(value_result.differences)} value differences"],
                    ...
                )

        # Phase 2: Performance (if requested)
        if benchmark:
            benchmarker = QueryBenchmarker(self.conn)
            orig_time = benchmarker.benchmark(original_sql)
            opt_time = benchmarker.benchmark(optimized_sql)
            speedup = orig_time / opt_time if opt_time > 0 else 1.0
        else:
            orig_time = opt_time = speedup = 0.0

        return ValidationResult(
            status=ValidationStatus.PASS,
            speedup=speedup,
            original_time_ms=orig_time,
            optimized_time_ms=opt_time,
            ...
        )
```

### ValidationStatus Enum

```python
class ValidationStatus(str, Enum):
    PASS = "pass"                        # Equivalent + benchmarked
    EXECUTION_ERROR = "execution_error"  # SQL syntax error
    ROW_COUNT_MISMATCH = "row_count_mismatch"
    VALUE_MISMATCH = "value_mismatch"
    CHECKSUM_MISMATCH = "checksum_mismatch"
    TIMEOUT = "timeout"
```

### Usage Example

```python
from qt_sql.validation.sql_validator import SQLValidator

validator = SQLValidator(database="data.duckdb")

result = validator.validate(
    original_sql="SELECT * FROM orders WHERE amount > 100",
    optimized_sql="SELECT * FROM orders WHERE amount > 100.0",
    benchmark=True
)

if result.status == ValidationStatus.PASS:
    print(f"✓ Valid: {result.speedup:.2f}x speedup")
    print(f"  Original: {result.original_time_ms:.1f}ms")
    print(f"  Optimized: {result.optimized_time_ms:.1f}ms")
else:
    print(f"✗ Failed: {result.status.value}")
    for error in result.errors:
        print(f"  - {error}")
```

---

## 10. Optimization Modes

Qt-SQL provides 5 optimization modes with different trade-offs between speed, coverage, and success rate.

### Mode Comparison

| Mode | Description | Speed | Success Rate | Best For |
|------|-------------|-------|--------------|----------|
| **standard** | Full-query DSPy rewrite | Fast | Medium (40%) | Simple queries |
| **dag** | Node-level DAG rewrites | Fast | Medium (45%) | Structured CTEs |
| **adaptive** | Parallel fan-out (v5) | Fast | **High (55%)** | **Production** |
| **mcts** | Tree search with transforms | Slow | Medium (42%) | Research, novel patterns |
| **dag_v2** | DAG with contracts + cost | Medium | High (50%) | Complex CTEs |

### Mode Selection Decision Tree

```
Query complexity?
    │
    ├─ Simple (< 3 CTEs, no correlation)
    │   └─> standard or dag (fast)
    │
    ├─ Medium (3-6 CTEs, some correlation)
    │   └─> adaptive (best success rate)
    │
    └─ Complex (> 6 CTEs, deep nesting, correlation)
        └─> dag_v2 (best for complex structure)

Need novel patterns not in knowledge base?
    └─> mcts (research mode)

Need fastest time-to-solution?
    └─> adaptive (parallel workers)
```

### 1. Standard Mode

**Command**: `qt-sql optimize query.sql` (default)

**Approach**:
- Full-query rewrite using DSPy `SQLOptimizer` signature
- Few-shot examples from gold patterns
- Model + DB config constraints
- 2 retry attempts with feedback

**Pros**:
- Simple, fast
- Good for straightforward optimizations

**Cons**:
- Rewrites entire query (high token usage for large queries)
- May miss targeted opportunities in complex CTEs

**Code**:
```python
from qt_sql.optimization.dspy_optimizer import optimize_query_with_validation

result = optimize_query_with_validation(
    original_sql=sql,
    execution_plan=plan,
    row_estimates=scans,
    db_path="data.duckdb",
    provider="deepseek",
    max_retries=2
)
```

### 2. DAG Mode

**Command**: `qt-sql optimize query.sql --mode dag`

**Approach**:
- Parse query into DAG (CTEs → nodes)
- Use DSPy `SQLDagOptimizer` signature for node-level rewrites
- Output: JSON with node IDs → new SQL
- Preserve unchanged nodes exactly

**Pros**:
- Lower token usage (only rewrite changed nodes)
- Preserves working parts of query

**Cons**:
- Requires parseable SQL (some dialects fail)
- Less holistic view of query

**Code**:
```python
from qt_sql.optimization.dspy_optimizer import optimize_with_dag

result = optimize_with_dag(
    sql=sql,
    plan=plan,
    db_path="data.duckdb",
    provider="deepseek"
)
```

### 3. Adaptive Mode (Recommended)

**Command**: `qt-sql optimize query.sql --mode adaptive`

**Approach**:
- **5 parallel workers** (ThreadPoolExecutor)
- Workers 1-4: Different example batches (coverage)
- Worker 5: No examples + full EXPLAIN (explore)
- Each worker validates on sample DB
- Return first valid or all if none pass

**Pros**:
- **Highest success rate** (55% on TPC-DS)
- Parallel = fast despite 5 attempts
- Covers diverse strategies

**Cons**:
- Higher LLM API cost (5 concurrent calls)
- Requires sample DB

**Code**:
```python
from qt_sql.optimization.adaptive_rewriter_v5 import optimize_v5_json

result = optimize_v5_json(
    sql=sql,
    sample_db="sample.duckdb",
    max_workers=5
)
```

**With full DB validation**:
```python
from qt_sql.optimization.adaptive_rewriter_v5 import optimize_v5_json_queue

valid, full_results, winner = optimize_v5_json_queue(
    sql=sql,
    sample_db="sample.duckdb",
    full_db="full.duckdb",
    target_speedup=2.0
)
```

### 4. MCTS Mode

**Command**: `qt-sql optimize query.sql --mode mcts`

**Approach**:
- Monte Carlo tree search with PUCT selection
- 11 deterministic AST transforms
- DSPy policy network for action priors
- 30 iterations (configurable)

**Pros**:
- Discovers novel combinations of transforms
- No dependency on LLM for transforms (deterministic)
- Good for research

**Cons**:
- **Slowest** (30+ iterations × benchmarking)
- Requires many DB executions
- Medium success rate

**Code**:
```python
from qt_sql.optimization.mcts.optimizer import MCTSSQLOptimizer

with MCTSSQLOptimizer(database="data.duckdb") as optimizer:
    result = optimizer.optimize(
        query=sql,
        max_iterations=30
    )
```

### 5. DAG v2 Mode

**Command**: `qt-sql optimize query.sql --mode dag_v2`

**Approach**:
- Parse into DAG with **node contracts** (output columns, grain, predicates)
- **Cost attribution** from EXPLAIN plan
- **Subgraph slicing** (target + 1-hop neighbors)
- **Transform allowlist** (7 safe transforms)
- Gold examples with KB pattern matching (DAG v3)

**Pros**:
- Best for complex CTEs (preserves contracts)
- Cost-aware targeting (optimize hot nodes)
- Reduced hallucination (allowlist)

**Cons**:
- Slower than adaptive (sequential)
- More setup code

**Code**:
```python
from qt_sql.optimization.dag_v2 import DagV2Pipeline

pipeline = DagV2Pipeline(sql, plan_json=plan_json)
prompt = pipeline.get_prompt(target_nodes=None)  # Auto-detects hot nodes
llm_response = call_llm(prompt)
optimized_sql = pipeline.apply_response(llm_response)
```

### CLI Usage Examples

```bash
# Standard mode (default)
qt-sql optimize query.sql

# DAG mode
qt-sql optimize query.sql --mode dag

# Adaptive mode (recommended)
qt-sql optimize query.sql --mode adaptive

# MCTS mode
qt-sql optimize query.sql --mode mcts --iterations 50

# DAG v2 mode
qt-sql optimize query.sql --mode dag_v2

# Specify LLM provider
qt-sql optimize query.sql --mode adaptive --provider groq

# Output to file
qt-sql optimize query.sql --mode adaptive -o optimized.sql

# Generate HTML report
qt-sql optimize query.sql --mode adaptive --report report.html
```

### Mode Performance Summary (TPC-DS SF100)

| Mode | Queries Validated | Avg Speedup | Wins (≥1.2x) | Top Speedup |
|------|-------------------|-------------|--------------|-------------|
| adaptive | 47/99 (47%) | 1.17x | 15 | 2.81x (Q1) |
| dag_v2 | 45/99 (45%) | 1.15x | 14 | 2.67x (Q15) |
| standard | 40/99 (40%) | 1.14x | 12 | 2.44x (Q39) |
| dag | 42/99 (42%) | 1.13x | 13 | 2.26x (Q45) |
| mcts | 38/99 (38%) | 1.11x | 10 | 2.06x (Q92) |

*Note: Results vary by LLM provider. Above uses DeepSeek V3.*

---

## 11. Execution & Plan Analysis

Qt-SQL extracts execution plans from database engines and parses them to inform optimization decisions.

Files:
- `packages/qt-sql/qt_sql/execution/duckdb_executor.py` - DuckDB execution
- `packages/qt-sql/qt_sql/execution/postgres_executor.py` - PostgreSQL execution
- `packages/qt-sql/qt_sql/execution/plan_analyzer.py` - EXPLAIN plan parsing

### Supported Databases

| Database | EXPLAIN Format | Features |
|----------|----------------|----------|
| **DuckDB** | JSON tree | Row estimates, cost, filters, join types |
| **PostgreSQL** | JSON tree | Cost, row estimates, node types |

### DuckDB Executor

```python
class DuckDBExecutor:
    def explain_analyze(self, sql: str) -> dict:
        """Get EXPLAIN ANALYZE output as JSON."""
        conn = duckdb.connect(self.db_path, read_only=True)

        # EXPLAIN ANALYZE returns detailed plan
        result = conn.execute(f"EXPLAIN ANALYZE {sql}").fetchall()
        plan_text = "\n".join(row[1] for row in result)

        # Parse JSON from EXPLAIN
        explain_json = conn.execute(f"EXPLAIN (FORMAT JSON) {sql}").fetchone()[0]

        conn.close()

        return {
            "plan_text": plan_text,
            "plan_json": json.loads(explain_json)
        }
```

### OptimizationContext

Parsed representation of execution plan:

```python
@dataclass
class TableScan:
    table: str
    rows_scanned: int
    rows_out: int
    has_filter: bool
    filter_selectivity: float

@dataclass
class JoinInfo:
    join_type: str          # "HASH_JOIN", "NESTED_LOOP", "MERGE"
    left_table: str
    right_table: str
    output_rows: int
    is_late: bool          # Join after aggregation?

@dataclass
class OptimizationContext:
    table_scans: List[TableScan]
    joins: List[JoinInfo]
    operators: List[dict]   # {operator, cost_pct, rows}
    cardinality_misestimates: List[dict]  # {operator, estimated, actual, ratio}
    total_cost: float

    def get_top_operators(self, n: int = 5) -> List[dict]:
        """Get n most expensive operators by cost_pct."""
        return sorted(self.operators, key=lambda x: x['cost_pct'], reverse=True)[:n]
```

### Plan Analyzer

File: `packages/qt-sql/qt_sql/execution/plan_analyzer.py`

```python
def analyze_plan_for_optimization(
    plan_json: dict,
    sql: str
) -> OptimizationContext:
    """Parse EXPLAIN plan and extract optimization hints."""

    table_scans = []
    joins = []
    operators = []
    misestimates = []

    def traverse(node: dict, depth: int = 0):
        node_type = node.get("name", "")

        # Extract table scans
        if "SEQ_SCAN" in node_type or "TABLE_SCAN" in node_type:
            table = node.get("extra_info", {}).get("table", "unknown")
            rows_in = node.get("cardinality_estimate", 0)
            rows_out = node.get("cardinality", rows_in)
            has_filter = "Filters" in node.get("extra_info", {})

            table_scans.append(TableScan(
                table=table,
                rows_scanned=rows_in,
                rows_out=rows_out,
                has_filter=has_filter,
                filter_selectivity=rows_out / rows_in if rows_in > 0 else 1.0
            ))

        # Extract joins
        if "JOIN" in node_type:
            join_type = node_type
            output_rows = node.get("cardinality", 0)

            # Heuristic: late join if depth > 3 (after aggregations)
            is_late = depth > 3

            joins.append(JoinInfo(
                join_type=join_type,
                left_table="left",  # Would need deeper parsing
                right_table="right",
                output_rows=output_rows,
                is_late=is_late
            ))

        # Extract operators with cost
        operators.append({
            "operator": node_type,
            "cost_pct": node.get("cost_pct", 0),
            "rows": node.get("cardinality", 0)
        })

        # Detect cardinality misestimates
        estimated = node.get("cardinality_estimate", 0)
        actual = node.get("cardinality", estimated)
        if estimated > 0:
            ratio = actual / estimated
            if ratio > 5 or ratio < 0.2:  # 5x overestimate or 5x underestimate
                misestimates.append({
                    "operator": node_type,
                    "estimated": estimated,
                    "actual": actual,
                    "ratio": round(ratio, 1)
                })

        # Recurse
        for child in node.get("children", []):
            traverse(child, depth + 1)

    traverse(plan_json)

    return OptimizationContext(
        table_scans=table_scans,
        joins=joins,
        operators=operators,
        cardinality_misestimates=misestimates,
        total_cost=sum(op["cost_pct"] for op in operators)
    )
```

### How Plans Inform Optimization

**1. Table Scans → Filter Pushdown**
```python
# High-cardinality scan without filter
if scan.rows_scanned > 1_000_000 and not scan.has_filter:
    hint = f"Push filter into {scan.table} to reduce scan"
```

**2. Late Joins → Join Reordering**
```python
# Join after aggregation
if join.is_late and join.output_rows > 100_000:
    hint = f"Move {join.join_type} earlier in plan"
```

**3. Cardinality Misestimates → CTE Materialization**
```python
# Underestimate suggests repeated evaluation
if misestimate["ratio"] < 0.2:
    hint = f"Materialize CTE to cache {misestimate['operator']} result"
```

**4. Top Operators → Hot Node Targeting**
```python
# Focus optimization on expensive operators
top_ops = ctx.get_top_operators(3)
for op in top_ops:
    if op["cost_pct"] > 20:
        hint = f"Target {op['operator']} (consumes {op['cost_pct']}% of cost)"
```

### Usage in Prompts

```python
from qt_sql.execution.plan_analyzer import analyze_plan_for_optimization

ctx = analyze_plan_for_optimization(plan_json, sql)

prompt = f"""
## Execution Plan Analysis

Top Operators:
{chr(10).join(f"- {op['operator']}: {op['cost_pct']}% cost, {op['rows']:,} rows"
             for op in ctx.get_top_operators(5))}

Table Scans:
{chr(10).join(f"- {scan.table}: {scan.rows_scanned:,} → {scan.rows_out:,} rows ({'filtered' if scan.has_filter else 'no filter'})"
             for scan in ctx.table_scans)}

Optimization Opportunities:
- High-cardinality scans without filters
- Joins after aggregation
- Cardinality misestimates
"""
```

---

## 12. Report Generation

Qt-SQL generates HTML reports with syntax highlighting, interactive sections, and responsive design.

File: `packages/qt-sql/qt_sql/reporting/html_generator.py`

### Report Sections

| Section | Content | Purpose |
|---------|---------|---------|
| **Summary** | Speedup, query stats, validation status | Quick overview |
| **Findings** | AST rule violations with severity | Static analysis results |
| **Optimization** | Original vs optimized SQL side-by-side | Review changes |
| **Performance** | Execution times, row counts, speedup chart | Measure improvement |
| **Execution Plans** | Before/after EXPLAIN plans | Understand what changed |

### Template Location

`packages/qt-sql/qt_sql/reporting/templates/sql_report.html.j2`

### Features

**Syntax Highlighting**: Uses Prism.js for SQL
```html
<pre><code class="language-sql">
SELECT * FROM orders
WHERE amount > 100
</code></pre>
```

**Responsive Design**: Mobile-friendly with CSS Grid
```css
.comparison-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 2rem;
}

@media (max-width: 768px) {
    .comparison-grid {
        grid-template-columns: 1fr;
    }
}
```

**Interactive Sections**: Collapsible details
```html
<details>
    <summary>Execution Plan (click to expand)</summary>
    <pre>{{ execution_plan }}</pre>
</details>
```

**Speedup Visualization**: SVG chart
```html
<svg viewBox="0 0 400 300">
    <rect x="50" y="50" width="{{ original_time / max_time * 250 }}" height="40" fill="#e74c3c"/>
    <text x="310" y="75">{{ original_time }}ms</text>

    <rect x="50" y="110" width="{{ optimized_time / max_time * 250 }}" height="40" fill="#27ae60"/>
    <text x="310" y="135">{{ optimized_time }}ms</text>

    <text x="50" y="180" font-size="24">{{ speedup }}x speedup</text>
</svg>
```

### Data Structure

```python
@dataclass
class ReportData:
    query_name: str
    original_sql: str
    optimized_sql: str
    speedup: float
    original_time_ms: float
    optimized_time_ms: float
    original_rows: int
    optimized_rows: int
    validation_status: str
    ast_findings: List[RuleMatch]  # From AST detector
    original_plan: str
    optimized_plan: str
    rationale: str                 # Optimization explanation
    timestamp: str
```

### Generation Example

```python
from qt_sql.reporting.html_generator import generate_html_report

report_data = ReportData(
    query_name="Q1_correlated_subquery",
    original_sql=original_sql,
    optimized_sql=optimized_sql,
    speedup=2.81,
    original_time_ms=1250.3,
    optimized_time_ms=445.2,
    original_rows=100,
    optimized_rows=100,
    validation_status="PASS",
    ast_findings=detector.analyze(original_sql),
    original_plan=original_plan,
    optimized_plan=optimized_plan,
    rationale="Decorrelated subquery to pre-computed CTE",
    timestamp="2026-02-04 10:30:45"
)

html = generate_html_report(report_data)

with open("report.html", "w") as f:
    f.write(html)
```

### Sample Output

```html
<!DOCTYPE html>
<html>
<head>
    <title>Qt-SQL Optimization Report</title>
    <link rel="stylesheet" href="prism.css">
    <style>
        /* Modern CSS with grid, flexbox, animations */
    </style>
</head>
<body>
    <header>
        <h1>Query Optimization Report</h1>
        <p class="timestamp">2026-02-04 10:30:45</p>
    </header>

    <section class="summary">
        <div class="metric">
            <span class="value">2.81x</span>
            <span class="label">Speedup</span>
        </div>
        <div class="metric">
            <span class="value">1250ms → 445ms</span>
            <span class="label">Execution Time</span>
        </div>
        <div class="metric success">
            <span class="value">✓ PASS</span>
            <span class="label">Validation</span>
        </div>
    </section>

    <section class="findings">
        <h2>Static Analysis Findings (5)</h2>
        <ul>
            <li class="warning">
                <strong>SQL-SUB-001</strong>: Correlated subquery in WHERE
                <p class="hint">Replace with JOIN or CTE</p>
            </li>
            <!-- More findings -->
        </ul>
    </section>

    <section class="comparison">
        <h2>SQL Comparison</h2>
        <div class="comparison-grid">
            <div>
                <h3>Original</h3>
                <pre><code class="language-sql">...</code></pre>
            </div>
            <div>
                <h3>Optimized</h3>
                <pre><code class="language-sql">...</code></pre>
            </div>
        </div>
    </section>

    <!-- More sections -->
</body>
</html>
```

---

## 13. CLI & API

### CLI Commands

File: `packages/qt-sql/cli/main.py`

#### 1. qt-sql audit

Static analysis only (no optimization).

```bash
qt-sql audit query.sql

# Options
--include-style      # Include style rules (default: high-precision only)
--format json        # Output format (text, json)
--output report.json # Save to file
```

**Output**:
```
Qt-SQL Audit Report
===================
Query: query.sql
Rules: 82 high-precision rules

Findings (5):
  [SQL-SUB-001] WARNING: Correlated subquery in WHERE clause
    Line 15: WHERE amount > (SELECT AVG(amount) FROM ...)
    Fix: Replace with pre-computed CTE + JOIN

  [SQL-JOIN-007] INFO: Too many joins (8 tables)
    Consider breaking into CTEs

  ...
```

#### 2. qt-sql optimize

Full optimization with validation.

```bash
qt-sql optimize query.sql

# Options
--mode [standard|dag|adaptive|mcts|dag_v2]  # Optimization mode (default: standard)
--provider [deepseek|groq|gemini|anthropic] # LLM provider (default: deepseek)
--db DATABASE                               # Database path for validation
--sample-db SAMPLE                          # Sample DB for adaptive mode
--output optimized.sql                      # Save optimized SQL
--report report.html                        # Generate HTML report
--iterations N                              # MCTS iterations (default: 30)
--max-retries N                             # DSPy retries (default: 2)
```

**Output**:
```
Qt-SQL Optimizer
================
Mode: adaptive (5 parallel workers)
Provider: deepseek-chat
Database: tpcds_sf100.duckdb

Analyzing query...
  - 3 CTEs detected
  - 8 table scans
  - Correlated subquery found

Optimizing with 5 workers...
  ✓ Worker 1: 2.81x speedup (decorrelate)
  ✗ Worker 2: validation failed (row count mismatch)
  ✓ Worker 3: 1.45x speedup (pushdown)
  ✗ Worker 4: execution error
  ✓ Worker 5: 2.15x speedup (or_to_union)

Best result: Worker 1 (2.81x speedup)

Validation:
  ✓ Semantic equivalence: PASS
  ✓ Row count: 100 rows (original = optimized)
  ✓ Performance: 1250ms → 445ms (2.81x speedup)

Saved to: optimized.sql
Report: report.html
```

#### 3. qt-sql validate

Validate a rewrite without optimization.

```bash
qt-sql validate original.sql optimized.sql --db data.duckdb

# Options
--benchmark         # Include performance benchmarking (default: true)
--runs N            # Number of benchmark runs (default: 3)
--tolerance FLOAT   # Float comparison tolerance (default: 1e-9)
```

**Output**:
```
Qt-SQL Validation
=================
Original: original.sql
Optimized: optimized.sql
Database: data.duckdb

Equivalence Check:
  ✓ Row count: 100 (match)
  ✓ Checksum: match
  ✓ Semantic equivalence: PASS

Performance Benchmark (3 runs, warmup excluded):
  Original:  1250.3ms (avg of runs 2-3)
  Optimized: 445.2ms (avg of runs 2-3)
  Speedup:   2.81x

Result: PASS
```

### FastAPI Endpoints

File: `packages/qt-sql/api/main.py`

Base URL: `http://localhost:8002/api/v1`

#### POST /optimize

Optimize a SQL query.

**Request**:
```json
{
  "sql": "SELECT * FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)",
  "mode": "adaptive",
  "provider": "deepseek",
  "database": "/data/sample.duckdb",
  "options": {
    "max_retries": 2,
    "iterations": 30
  }
}
```

**Response**:
```json
{
  "status": "success",
  "original_sql": "SELECT ...",
  "optimized_sql": "WITH avg_cte AS (...) SELECT ...",
  "speedup": 2.81,
  "validation": {
    "status": "PASS",
    "original_time_ms": 1250.3,
    "optimized_time_ms": 445.2,
    "row_count": 100
  },
  "rationale": "Decorrelated subquery to pre-computed CTE",
  "mode": "adaptive",
  "worker_id": 1
}
```

#### POST /audit

Static analysis only.

**Request**:
```json
{
  "sql": "SELECT * FROM orders WHERE ...",
  "include_style": false
}
```

**Response**:
```json
{
  "status": "success",
  "findings": [
    {
      "rule_id": "SQL-SUB-001",
      "severity": "warning",
      "message": "Correlated subquery in WHERE clause",
      "line": 15,
      "fix_hint": "Replace with pre-computed CTE + JOIN"
    }
  ],
  "total_rules": 82,
  "high_severity": 2,
  "medium_severity": 3,
  "low_severity": 0
}
```

#### POST /validate

Validate a rewrite.

**Request**:
```json
{
  "original_sql": "SELECT ...",
  "optimized_sql": "WITH ... SELECT ...",
  "database": "/data/sample.duckdb",
  "benchmark": true
}
```

**Response**:
```json
{
  "status": "PASS",
  "row_count": {
    "original": 100,
    "optimized": 100,
    "match": true
  },
  "checksum": {
    "original": "a1b2c3d4...",
    "optimized": "a1b2c3d4...",
    "match": true
  },
  "performance": {
    "original_time_ms": 1250.3,
    "optimized_time_ms": 445.2,
    "speedup": 2.81
  }
}
```

#### GET /opportunities

Detect optimization opportunities.

**Request**:
```
GET /opportunities?sql=SELECT%20...
```

**Response**:
```json
{
  "opportunities": [
    {
      "pattern_id": "QT-OPT-002",
      "pattern_name": "Correlated Subquery to Pre-computed CTE",
      "trigger": "Correlated subquery with aggregate comparison",
      "expected_speedup": "2.81x",
      "benchmark_queries": ["Q1"]
    }
  ]
}
```

---

## 14. Testing

### Test Directory Structure

```
packages/qt-sql/tests/
├── unit/
│   ├── test_ast_detector.py       # AST rule tests
│   ├── test_knowledge_base.py     # Pattern detection tests
│   ├── test_equivalence_checker.py # Validation tests
│   └── test_transforms.py          # MCTS transform tests
│
├── integration/
│   ├── test_dspy_optimizer.py     # End-to-end DSPy tests
│   ├── test_dag_pipeline.py       # DAG optimization tests
│   ├── test_adaptive_v5.py        # Adaptive rewriter tests
│   └── test_validation_pipeline.py # Full validation tests
│
└── benchmarks/
    ├── test_tpcds.py              # TPC-DS benchmark runner
    └── fixtures/
        ├── sample.duckdb          # Small test database
        └── queries/               # Test SQL queries
```

### Test Categories

**Unit Tests**: Fast, isolated
```bash
pytest tests/unit/ -v
```

**Integration Tests**: Database required
```bash
pytest tests/integration/ -v --db tests/fixtures/sample.duckdb
```

**Benchmark Tests**: Slow, full validation
```bash
pytest tests/benchmarks/ -v --db /data/tpcds_sf100.duckdb --slow
```

### Example Test: AST Detector

```python
def test_correlated_subquery_detection():
    """Test detection of correlated subquery pattern."""
    sql = """
    SELECT * FROM orders
    WHERE amount > (
        SELECT AVG(amount)
        FROM orders o2
        WHERE o2.customer_id = orders.customer_id
    )
    """

    detector = ASTDetector()
    matches = detector.analyze(sql)

    # Should detect correlated subquery
    correlated = [m for m in matches if m.rule_id == "SQL-SUB-001"]
    assert len(correlated) == 1
    assert "correlated" in correlated[0].message.lower()

    # Should suggest fix
    assert "CTE" in correlated[0].fix_hint or "JOIN" in correlated[0].fix_hint
```

### Example Test: Validation

```python
def test_equivalence_checker_float_tolerance():
    """Test float comparison with tolerance."""
    checker = EquivalenceChecker(float_tolerance=1e-9)

    original = [{"amount": 100.0000000001}]
    optimized = [{"amount": 100.0000000002}]

    result = checker.check_equivalence(original, optimized, detailed=True)

    # Should match within tolerance
    assert result[0] is True
    assert result[1] is None  # No differences
```

### Example Test: Knowledge Base

```python
def test_or_to_union_detection():
    """Test OR to UNION pattern detection."""
    from qt_sql.optimization.knowledge_base import detect_opportunities

    sql = """
    SELECT * FROM catalog_sales
    WHERE cs_sales_price > 500
       OR cs_item_sk IN (1, 2, 3)
       OR cs_sold_date_sk = 100
    """

    opportunities = detect_opportunities(sql)

    # Should detect OR pattern
    or_patterns = [o for o in opportunities if o.pattern.id == "or_to_union"]
    assert len(or_patterns) == 1
    assert or_patterns[0].pattern.code == "QT-OPT-001"
```

### Running Tests

```bash
# All tests
pytest tests/ -v

# Specific category
pytest tests/unit/ -v
pytest tests/integration/ -v

# Specific test file
pytest tests/unit/test_ast_detector.py -v

# Specific test function
pytest tests/unit/test_ast_detector.py::test_correlated_subquery_detection -v

# With coverage
pytest tests/ --cov=qt_sql --cov-report=html

# Parallel execution
pytest tests/ -n auto

# Skip slow tests
pytest tests/ -v -m "not slow"
```

---

## 15. Performance & Benchmarks

### TPC-DS SF100 Results

**Benchmark**: 99 standard TPC-DS queries on 100GB DuckDB
**Provider**: Kimi K2.5 (DAG v2 mode)
**Date**: 2026-02-02

Full details: `BENCHMARKS.md` in project root

#### Summary Statistics

- **Queries Validated**: 47/99 (47%)
- **Average Speedup**: 1.17x
- **Wins (≥1.2x)**: 15 queries
- **Top Speedup**: 2.81x (Q1)

#### Top 10 Speedups

| Query | Speedup | Transform | Pattern |
|-------|---------|-----------|---------|
| Q1 | **2.81x** | decorrelate | Correlated subquery → pre-computed CTE |
| Q93 | **2.71x** | early_filter | Dimension filter before fact join |
| Q15 | **2.67x** | or_to_union | OR → UNION ALL + date CTE |
| Q90 | **1.84x** | early_filter | Early reason dimension filter |
| Q74 | **1.42x** | pushdown | Year filter into CTE |
| Q95 | **1.36x** | cte_opt | Date filter optimization |
| Q80 | **1.24x** | early_filter | Store returns filter |
| Q73 | **1.24x** | pushdown | Date range filter |
| Q27 | **1.23x** | early_filter | State filter to dimension |
| Q78 | **1.21x** | projection_prune | Unused column elimination |

#### Mode Comparison (All Providers)

| Mode | Validated | Avg Speedup | Wins | Top |
|------|-----------|-------------|------|-----|
| adaptive | 47/99 | 1.17x | 15 | 2.81x |
| dag_v2 | 45/99 | 1.15x | 14 | 2.67x |
| standard | 40/99 | 1.14x | 12 | 2.44x |
| dag | 42/99 | 1.13x | 13 | 2.26x |
| mcts | 38/99 | 1.11x | 10 | 2.06x |

### Benchmark Data Location

```
research/experiments/benchmarks/kimi_benchmark_20260202_221828/
├── REPORT.md                    # Human-readable report
├── summary.json                 # Machine-readable summary
└── q1/
    ├── original.sql             # Original TPC-DS query
    ├── optimized.sql            # Optimized query
    ├── timing.json              # Benchmark times
    ├── validation.txt           # Validation result
    └── llm_response.txt         # LLM optimization rationale
```

### Running Your Own Benchmarks

```bash
# Full TPC-DS benchmark
python -m qt_sql.benchmarks.tpcds \
    --queries /data/tpcds_queries/ \
    --database /data/tpcds_sf100.duckdb \
    --mode adaptive \
    --provider deepseek \
    --output results/

# Single query
qt-sql optimize /data/tpcds_queries/query1.sql \
    --mode adaptive \
    --db /data/tpcds_sf100.duckdb \
    --report report.html
```

### Key Insights from Benchmarks

**1. Correlated Subqueries**: Biggest wins (2-3x)
- Pattern: `WHERE col > (SELECT AGG(...) WHERE correlated)`
- Fix: Pre-compute CTE with GROUP BY, then JOIN
- Example: Q1 (2.81x)

**2. OR to UNION ALL**: High impact when columns differ
- Pattern: `WHERE col_a = X OR col_b = Y OR col_c = Z`
- Fix: Split into UNION ALL branches
- Example: Q15 (2.67x), Q23 (2.33x)

**3. Early Filtering**: Push dimension filters before fact joins
- Pattern: Filter on dimension table after joining to fact
- Fix: Filter dimension first, then join
- Example: Q93 (2.71x), Q90 (1.84x)

**4. Date CTE Isolation**: Extract date filters to small CTE
- Pattern: `date_dim` joined with `d_year/d_qoy` filter
- Fix: `WITH date_cte AS (SELECT d_date_sk FROM date_dim WHERE ...)`
- Example: Q15 (2.67x), Q6 (1.5x)

---

## 16. Configuration

### Environment Variables

Required variables in `.env` or shell:

```bash
# Database
QT_DATABASE_URL=postgresql://user:pass@localhost:5432/querytorque

# Auth0 (for web app)
QT_AUTH0_DOMAIN=your-tenant.auth0.com
QT_AUTH0_API_AUDIENCE=https://api.querytorque.com
QT_AUTH0_CLIENT_ID=xxx

# Stripe (for billing)
QT_STRIPE_API_KEY=sk_xxx
QT_STRIPE_WEBHOOK_SECRET=whsec_xxx

# LLM Providers
QT_LLM_PROVIDER=groq              # Default provider
QT_GROQ_API_KEY=xxx
QT_DEEPSEEK_API_KEY=xxx
QT_ANTHROPIC_API_KEY=xxx
QT_OPENAI_API_KEY=xxx
QT_GEMINI_API_KEY=xxx
```

### Model Configs

Location: `research/knowledge_base/model_configs/*.yaml`

**DeepSeek Config** (`deepseek.yaml`):

```yaml
model_name: "deepseek-chat"
provider: "deepseek"
base_url: "https://api.deepseek.com"

constraints:
  - "Avoid overly complex nested CTEs - prefer flat structure"
  - "DeepSeek-V3 is strong at predicate pushdown"
  - "Be conservative with IN to EXISTS conversion"

strengths:
  - "Excellent at identifying redundant operations"
  - "Strong logical reasoning for join reordering"
  - "Good at correlated subquery decorrelation"

failure_patterns:
  - "May over-apply CTE materialization"
  - "Sometimes misses early filter opportunities"

prompt_suffix: |
  DEEPSEEK-V3 STRENGTHS:
  - Predicate pushdown and filter optimization
  - Join reordering and redundant operation removal
  - Logical simplification of complex predicates

  CAUTION:
  - Avoid over-nesting CTEs (keep structure flat)
  - Validate IN to EXISTS conversions carefully
```

**Groq Config** (`groq.yaml`):

```yaml
model_name: "llama-3.3-70b-versatile"
provider: "groq"

constraints:
  - "Keep transformations simple and focused"
  - "Llama 3.3 works best with explicit instructions"

strengths:
  - "Fast inference (good for parallel workers)"
  - "Reliable for standard patterns"

failure_patterns:
  - "May miss complex multi-CTE optimizations"
  - "Occasional formatting issues in JSON output"

prompt_suffix: |
  LLAMA 3.3 STRENGTHS:
  - Fast, reliable for standard optimizations
  - Good at following explicit patterns

  INSTRUCTIONS:
  - Output valid JSON only
  - One optimization per node
```

### DB Configs

Location: `research/knowledge_base/db_configs/*.yaml`

**DuckDB Config** (`duckdb.yaml`):

```yaml
database: "duckdb"
dialect: "duckdb"

hints:
  - text: "DuckDB automatically pushes filters through JOINs"
    category: "optimizer"
    impact: "high"

  - text: "Use QUALIFY for window function filtering"
    category: "syntax"
    impact: "medium"

  - text: "ASOF JOIN for temporal queries"
    category: "feature"
    impact: "medium"

syntax_notes:
  - "GROUP BY ALL infers grouping columns"
  - "EXCLUDE (col1, col2) to exclude specific columns"
  - "No LIMIT in CTEs (not standard SQL)"

limitations:
  - "No materialized views (use temp tables)"
  - "Limited optimizer hints (PRAGMA only)"
  - "No query rewrite hints"

strengths:
  - "Excellent at columnar scan optimization"
  - "Strong parallel execution (automatic)"
  - "Good at filter pushdown through joins"
  - "Smart GROUP BY optimization"

optimizer_behaviors:
  - "Automatically parallelizes large scans"
  - "Pushes predicates aggressively"
  - "May not decorrelate complex subqueries"
```

**PostgreSQL Config** (`postgres.yaml`):

```yaml
database: "postgresql"
dialect: "postgres"

hints:
  - text: "Use LATERAL for correlated subqueries"
    category: "syntax"
    impact: "high"

  - text: "Index JSONB columns with GIN"
    category: "indexing"
    impact: "high"

  - text: "Consider PARALLEL query hints for large scans"
    category: "performance"
    impact: "medium"

syntax_notes:
  - "NULLS FIRST/LAST for ORDER BY NULL handling"
  - "DISTINCT ON for row deduplication"
  - "CTEs are optimization fences (use subqueries for pushdown)"

limitations:
  - "CTEs block predicate pushdown (prior to v12)"
  - "Limited window function optimization"

strengths:
  - "Excellent JOIN algorithms (hash, merge, nested loop)"
  - "Good cost-based optimizer"
  - "Parallel query execution (if configured)"
```

### Configuration Loading

```python
from qt_sql.optimization.dspy_optimizer import load_model_config, load_db_config

# Load configs
model_config = load_model_config("deepseek")
db_config = load_db_config("duckdb")

# Build system prompt
prompt_suffix = model_config.get("prompt_suffix", "")
hints = db_config.get("hints", [])

system_prompt = f"{prompt_suffix}\n\nDB Hints:\n" + "\n".join(
    f"- {h['text']}" for h in hints
)
```

---

## Appendices

### A. Glossary

| Term | Definition |
|------|------------|
| **AST** | Abstract Syntax Tree - parsed representation of SQL |
| **CTE** | Common Table Expression - WITH clause in SQL |
| **DAG** | Directed Acyclic Graph - query as nodes and edges |
| **DSPy** | Declarative Self-improving Python - LLM framework |
| **MCTS** | Monte Carlo Tree Search - tree-based optimization |
| **PUCT** | Polynomial Upper Confidence Trees - MCTS selection algorithm |
| **Sargable** | Search ARGument ABLE - predicate that can use indexes |
| **TPC-DS** | Transaction Processing Performance Council Decision Support - analytical benchmark |
| **Decorrelation** | Converting correlated subquery to JOIN or window function |
| **Predicate Pushdown** | Moving filters closer to data sources |
| **Checksum** | MD5 hash of query results for equivalence checking |
| **Trimmed-Mean** | Average excluding outliers (e.g., first run) |
| **Few-Shot Learning** | Providing examples to LLM for pattern learning |
| **Gold Example** | Verified optimization with proven speedup |

### B. File Reference Index

| Component | Primary File |
|-----------|--------------|
| **Knowledge Base** | `packages/qt-sql/qt_sql/optimization/knowledge_base.py` |
| **AST Registry** | `packages/qt-sql/qt_sql/analyzers/ast_detector/registry.py` |
| **DSPy Signatures** | `packages/qt-sql/qt_sql/optimization/dspy_optimizer.py` |
| **DAG v2 Builder** | `packages/qt-sql/qt_sql/optimization/dag_v2.py` |
| **DAG v3 Examples** | `packages/qt-sql/qt_sql/optimization/dag_v3.py` |
| **Adaptive v5** | `packages/qt-sql/qt_sql/optimization/adaptive_rewriter_v5.py` |
| **MCTS Optimizer** | `packages/qt-sql/qt_sql/optimization/mcts/optimizer.py` |
| **MCTS Tree** | `packages/qt-sql/qt_sql/optimization/mcts/tree.py` |
| **MCTS Transforms** | `packages/qt-sql/qt_sql/optimization/mcts/transforms.py` |
| **Equivalence Checker** | `packages/qt-sql/qt_sql/validation/equivalence_checker.py` |
| **Benchmarker** | `packages/qt-sql/qt_sql/validation/benchmarker.py` |
| **SQL Validator** | `packages/qt-sql/qt_sql/validation/sql_validator.py` |
| **Plan Analyzer** | `packages/qt-sql/qt_sql/execution/plan_analyzer.py` |
| **HTML Generator** | `packages/qt-sql/qt_sql/reporting/html_generator.py` |
| **CLI** | `packages/qt-sql/cli/main.py` |
| **API** | `packages/qt-sql/api/main.py` |

### C. Code Examples

#### Quick Start: Python API

```python
from qt_sql.optimization.dspy_optimizer import optimize_query_with_validation

# Basic optimization
result = optimize_query_with_validation(
    original_sql="SELECT * FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)",
    execution_plan="",  # Optional
    row_estimates="",   # Optional
    db_path="data.duckdb",
    provider="deepseek"
)

print(f"Speedup: {result.speedup:.2f}x")
print(f"Valid: {result.correct}")
print(f"Optimized:\n{result.optimized_sql}")
```

#### Knowledge Base Pattern Detection

```python
from qt_sql.optimization.knowledge_base import detect_opportunities

opportunities = detect_opportunities(sql)

for opp in opportunities:
    print(f"{opp.pattern.code}: {opp.pattern.name}")
    print(f"  Expected speedup: {opp.pattern.benchmark_queries}")
    print(f"  Rewrite: {opp.pattern.rewrite_hint}")
```

#### Adaptive Rewriter (Parallel)

```python
from qt_sql.optimization.adaptive_rewriter_v5 import optimize_v5_json_queue

valid, full_results, winner = optimize_v5_json_queue(
    sql=query,
    sample_db="sample.duckdb",
    full_db="full.duckdb",
    max_workers=5,
    target_speedup=2.0
)

if winner:
    print(f"Winner: {winner.full_speedup:.2f}x")
    print(f"Worker: {winner.sample.worker_id}")
```

#### MCTS Tree Search

```python
from qt_sql.optimization.mcts.optimizer import MCTSSQLOptimizer

with MCTSSQLOptimizer(database="data.duckdb") as optimizer:
    result = optimizer.optimize(query=sql, max_iterations=30)

    print(f"Speedup: {result.speedup:.2f}x")
    print(f"Transforms: {result.transforms_applied}")
    print(f"Tree stats: {result.tree_stats}")
```

### D. Troubleshooting

#### Issue: Validation fails with "row count mismatch"

**Cause**: Optimized query returns different number of rows.

**Solutions**:
1. Check AST rules - might have detected issue
2. Review LLM rationale for logic errors
3. Try different mode (adaptive has highest success rate)
4. Add more specific constraints in model config

```python
# Retry with different mode
result = optimize_v5_json(sql, sample_db="sample.duckdb")
```

#### Issue: "DSPy LM not configured"

**Cause**: LLM not initialized before calling optimizer.

**Solution**:
```python
from qt_sql.optimization.dspy_optimizer import configure_lm

configure_lm(provider="deepseek")
# Now call optimizer
```

#### Issue: Slow MCTS optimization

**Cause**: 30 iterations with benchmarking is expensive.

**Solutions**:
1. Reduce iterations: `max_iterations=10`
2. Use sample DB instead of full DB
3. Disable benchmarking in tree search (policy-only)
4. Use adaptive mode instead (faster)

#### Issue: LLM returns invalid JSON

**Cause**: Some models struggle with structured output.

**Solutions**:
1. Use DSPy mode (structured signatures)
2. Add JSON formatting to model config prompt_suffix
3. Try different model (Gemini/Claude better at JSON)

```yaml
# model_configs/custom.yaml
prompt_suffix: |
  CRITICAL: Output ONLY valid JSON. No markdown, no comments.

  ```json
  {
    "rewrite_sets": [...]
  }
  ```
```

#### Issue: High token usage

**Cause**: Full-query rewrites include entire SQL in prompt.

**Solutions**:
1. Use DAG mode (node-level rewrites)
2. Use subgraph slicing (dag_v2)
3. Reduce number of gold examples

```python
# DAG mode with targeted slicing
from qt_sql.optimization.dag_v2 import DagV2Pipeline

pipeline = DagV2Pipeline(sql, plan_json=plan)
prompt = pipeline.get_prompt(target_nodes=["expensive_cte"])
```

---

## Document Metadata

**Version**: 1.0
**Last Updated**: 2026-02-04
**Maintainers**: QueryTorque Team
**License**: See project LICENSE file

**Feedback**: Report issues or suggest improvements at the project repository.

**Next Steps**:
- Review [BENCHMARKS.md](../BENCHMARKS.md) for latest results
- Check [CLAUDE.md](../CLAUDE.md) for project overview
- See [examples/](../examples/) for code samples
- Visit [research/knowledge_base/](../research/knowledge_base/) for configs

---

*This document catalogs the qt-sql implementation as of February 2026. For the latest code, refer to the source files listed in Appendix B.*

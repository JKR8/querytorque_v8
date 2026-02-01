# QueryTorque Research

LLM-based SQL optimization research and development.

## Key Finding

**AST-based rule detection has 0% overlap with actual performance bottlenecks.**

Detected 9+ issues per query, but none were the actual problem. The real optimizations came from analyzing execution plans and data flow.

## Proven Results (TPC-DS SF100)

| Query | Optimization | Speedup | Model |
|-------|--------------|---------|-------|
| Q1 | Predicate Pushdown | 2.64x | Manual |
| Q2 | Filter Pushdown | 2.09x | Gemini |
| Q23 | Join Elimination | 2.18x | Gemini |

## Directory Structure

```
research/
├── README.md                 # This file
│
├── docs/                     # All documentation
│   ├── CORE_FINDINGS.md     # Main research conclusions
│   ├── OPTIMIZATION_PATTERNS.md
│   ├── PROMPT_ARCHITECTURE.md
│   ├── AGENTIC_LOOP_DESIGN.md
│   └── papers/              # Research PDFs
│
├── knowledge_base/          # Structured knowledge for agents
│   ├── winning_patterns.yaml  # Single source of truth for patterns
│   ├── OPTIMIZATION_PATTERNS.md
│   ├── AGENT_INSTRUCTIONS.md
│   └── examples/            # Q1, Q2, Q23 optimization docs
│
├── prompts/                 # All prompt templates
│   ├── templates/           # Master prompt template
│   ├── batch/               # 99 TPC-DS prompts + manifest.json
│   └── archived/            # Old prompt versions
│
├── experiments/             # Model testing results
│   ├── deepseek/           # DeepSeek responses + results
│   ├── gemini/             # Gemini responses + results
│   └── kimi/               # Kimi responses + results
│
├── optimized_queries/       # Validated SQL optimizations
│   ├── verified/           # Working optimizations
│   ├── failed/             # Failed attempts (for learning)
│   └── RESULTS.md
│
├── scripts/                 # Python scripts
│   ├── generate_prompts.py # Batch prompt generator
│   ├── run_optimization.py # Main optimization runner
│   └── test_optimization.py
│
└── archive/                 # Old/superseded files
    ├── payload_comparison/
    ├── tpcds_optimizations/
    └── old_prompts/
```

## Quick Start

### 1. Use Batch Prompts

99 pre-generated prompts for TPC-DS queries:

```bash
cat prompts/batch/q1_prompt.txt | pbcopy  # Copy Q1 prompt
```

### 2. Check Patterns

```python
import yaml
with open('knowledge_base/winning_patterns.yaml') as f:
    patterns = yaml.safe_load(f)

for p in patterns['patterns']:
    print(f"{p['name']}: {p['speedup']}")
```

### 3. Run Optimization

```bash
python scripts/run_optimization.py --query q1 --model deepseek
```

## Core Algorithm

The prompt that works:

```
1. ANALYZE: Find where rows/cost are largest in the plan.
2. OPTIMIZE: For each bottleneck, ask "what could reduce it earlier?"
   - Can a filter be pushed inside a CTE instead of applied after?
   - Can a small table join happen inside an aggregation?
   - Is there a correlated subquery? Convert to CTE + JOIN.
3. VERIFY: Result must be semantically equivalent.

Principle: Reduce rows as early as possible.
```

## Winning Patterns

| Pattern | Speedup | Description |
|---------|---------|-------------|
| Predicate Pushdown | 2.1-2.5x | Move selective filters INTO CTE before GROUP BY |
| Filter Pushdown | 2.09x | Add filters early before aggregation |
| Join Elimination | 2.18x | Replace FK-only joins with IS NOT NULL |
| Scan Consolidation | 1.25x | Combine multiple scans with CASE WHEN |

See `knowledge_base/winning_patterns.yaml` for details.

## Related Files

- `packages/qt-sql/qt_sql/optimization/` - Production implementation
- `docs/CORE_FINDINGS.md` - Full research writeup
- `docs/papers/` - Academic papers on LLM SQL optimization

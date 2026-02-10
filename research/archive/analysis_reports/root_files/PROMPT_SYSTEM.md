# QueryTorque Prompt System — Canonical Reference

This is the single source of truth for how optimization prompts are built, sent to LLMs, and responses parsed.

## Prompt Assembly Order

```
1. Gold Examples (2-3 per query)
2. Constraints (no emojis)
3. DAG Base Prompt (system rules + target nodes + subgraph slice + contracts)
4. Execution Plan (parsed EXPLAIN ANALYZE)
5. Previous Attempt History (with speedup-derived status)
```

## Canonical Files

### Prompt Builder (CORRECT VERSION)

**`packages/qt-sql/ado/prompt_builder.py`** — The latest prompt builder.

- `build_prompt_with_examples(base_prompt, examples, execution_plan, history, include_constraints)` — Main assembly function
- `format_example_for_prompt(example)` — Formats one gold example
- `format_constraints_for_prompt(constraints)` — Formats constraints with `### ID [SEVERITY]` (NO emojis)
- `load_all_constraints()` — Loads from `ado/constraints/`
- `load_example(id)` — Loads from `ado/examples/` (DSB patterns)
- `get_dag_base_prompt(sql)` — Wraps DagV2Pipeline

### Constraints (NO EMOJIS)

**`packages/qt-sql/ado/constraints/literal_preservation.json`**
- Format: `### LITERAL_PRESERVATION [CRITICAL]`
- Rule: Copy ALL literals exactly from original query

**`packages/qt-sql/ado/constraints/or_to_union_limit.json`**
- Format: `### OR_TO_UNION_LIMIT [HIGH]`
- Rule: Max 3 UNION branches, no cartesian expansion

### Gold Examples (TPC-DS)

**`packages/qt-sql/qt_sql/optimization/examples/`** — 13 verified TPC-DS gold patterns:

| File | Pattern | Speedup |
|------|---------|---------|
| `single_pass_aggregation.json` | Consolidate repeated scans | 4.47x |
| `date_cte_isolate.json` | Pre-filter date_dim into CTE | 4.00x |
| `early_filter.json` | Push filters into CTEs | 4.00x |
| `prefetch_fact_join.json` | Pre-join filtered dates with fact | 3.77x |
| `or_to_union.json` | OR to UNION ALL (max 3 branches) | 3.17x |
| `decorrelate.json` | Correlated subquery to JOIN | 2.92x |
| `multi_dimension_prefetch.json` | Pre-filter date + store dims | 2.71x |
| `multi_date_range_cte.json` | Separate CTEs per date alias | 2.35x |
| `pushdown.json` | Push filters outer to inner | 2.11x |
| `dimension_cte_isolate.json` | Pre-filter ALL dimensions | 1.93x |
| `intersect_to_exists.json` | INTERSECT to EXISTS | 1.83x |
| `materialize_cte.json` | Force CTE materialization | 1.37x |
| `union_cte_split.json` | Split UNION ALL by year | 1.36x |

Each JSON has: `id`, `name`, `description`, `verified_speedup`, `example.input_slice`, `example.output` (rewrite_sets JSON), `example.key_insight`

### Gold Examples (DSB / ADO)

**`packages/qt-sql/ado/examples/`** — 37 patterns from DSB catalog (PostgreSQL-focused).

Different set from TPC-DS. Includes: `correlated_scalar_to_left_join`, `aggregate_push_below_join`, `star_schema_dimension_filter_first`, etc.

### DAG Decomposition

**`packages/qt-sql/qt_sql/optimization/dag_v2.py`** — `DagV2Pipeline`

- `DagV2Pipeline(sql, plan_context=ctx)` — Parse SQL into DAG nodes
- `.get_prompt()` — Generate base prompt with:
  - System prompt (rules, allowed transforms, JSON output format)
  - Target Nodes (hottest nodes by cost)
  - Subgraph Slice (SQL per node)
  - Node Contracts (output_columns, grain, required_predicates)
  - Downstream Usage (which columns consumed)
  - Cost Attribution (% cost, row estimates, operators)
  - Detected Opportunities (KB patterns, node-specific hints)
- `.apply_response(llm_response)` — Parse JSON response, assemble optimized SQL

### EXPLAIN Plan Parsing

**`packages/qt-sql/qt_sql/execution/database_utils.py`**
- `run_explain_analyze(db_path, sql)` → `{plan_json, plan_text, execution_time_ms}`
- Uses `EXPLAIN (ANALYZE, FORMAT JSON)` for DuckDB

**`packages/qt-sql/qt_sql/optimization/plan_analyzer.py`**
- `analyze_plan_for_optimization(plan_json, sql)` → `OptimizationContext`
- Extracts operators, costs, row estimates, misestimates

**`packages/qt-sql/qt_sql/optimization/adaptive_rewriter_v5.py`**
- `_format_plan_summary(ctx)` → Structured text summary:
  - Operators by cost
  - Scans (table × count, rows filtered)
  - Misestimates (estimated vs actual)
  - Joins (type, cardinality)

### Prompt Generator (Batch)

**`research/state/generate_prompts_v2.py`** — Generates all 99 TPC-DS prompts.

Uses:
- `ado.prompt_builder.build_prompt_with_examples` (assembly, no emojis)
- `qt_sql.optimization.dag_v3.load_example` (loads TPC-DS gold patterns)
- `qt_sql.optimization.dag_v2.DagV2Pipeline` (DAG decomposition)
- `qt_sql.execution.database_utils.run_explain_analyze` (EXPLAIN)
- `qt_sql.optimization.plan_analyzer.analyze_plan_for_optimization` (plan parsing)
- `qt_sql.optimization.adaptive_rewriter_v5._format_plan_summary` (plan formatting)
- State analysis recommendations for example selection (not FAISS)
- History from `research/state_histories_all_99/*.yaml` with speedup-derived status

## LLM Clients

**`packages/qt-shared/qt_shared/llm/`**

| Provider | File | Default Model | Max Tokens |
|----------|------|---------------|------------|
| Anthropic | `anthropic.py` | claude-sonnet-4-20250514 | 4096 |
| DeepSeek | `deepseek.py` | deepseek-reasoner | 16384 |
| OpenAI | `openai.py` | gpt-4o | 8192 |
| OpenRouter | via `openai.py` | moonshotai/kimi-k2.5 | 8192 |
| Gemini | `gemini.py` | gemini-3-flash-preview | 8192 |
| Groq | `groq.py` | llama-3.3-70b-versatile | 8192 |

**Factory**: `packages/qt-shared/qt_shared/llm/factory.py`
- `create_llm_client(provider, model, api_key)` — Creates any client
- Reads from `.env` via `QT_LLM_PROVIDER`, `QT_LLM_MODEL`, `QT_*_API_KEY`

**All clients**: `client.analyze(prompt) → str` (returns raw text response)

**System prompt** (in each client): "You are a senior SQL performance engineer. Return ONLY valid JSON — no markdown fences, no commentary."

## Response Format (Expected from LLM)

```json
{
  "rewrite_sets": [
    {
      "id": "rs_01",
      "transform": "decorrelate",
      "nodes": {
        "filtered_returns": "SELECT ...",
        "main_query": "SELECT ..."
      },
      "invariants_kept": ["same result rows", "same ordering"],
      "expected_speedup": "2.90x",
      "risk": "low"
    }
  ],
  "explanation": "what was changed and why"
}
```

## Response → Optimized SQL

**`DagV2Pipeline.apply_response(llm_response)`** at `dag_v2.py:964`
1. Extract JSON from response (strips ```json fences or finds raw JSON)
2. `assembler.apply_from_json(json_str)` — Assembles CTEs from `nodes` dict into `WITH ... SELECT ...`

## OUTDATED FILES (DO NOT USE)

| File | Problem |
|------|---------|
| `qt_sql/optimization/dag_v3.py` `format_constraints_for_prompt()` | Has emojis 🚨 ⚠️, puts constraints FIRST (wrong order) |
| `qt_sql/optimization/constraints/*.json` | Same content as ado/ but loaded by outdated dag_v3 |
| `research/state/generate_prompts.py` | V1 garbage prompts — no DAG, no parsed EXPLAIN |

## Validation Rules

Only 2 valid methods:
1. **3-run**: Run 3x, discard 1st (warmup), average last 2
2. **5-run trimmed mean**: Run 5x, remove min/max, average middle 3

Never use single-run comparisons.

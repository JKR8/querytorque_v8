# Detection and Matching: How Knowledge Finds Queries

> **Status**: Target state specification
> **Replaces**: `knowledge.py:TagRecommender` matching logic (tag overlap scoring)
> **Builds on**: `tag_index.py:extract_tags()`, `tag_index.py:classify_category()`

---

## Architecture

```
Query arrives
    |
    v
Feature Extractor (sqlglot)     →  fixed vocabulary of ~25 features
                                     |
Detection Rules (in engine profile) →  which gaps fire on this query?
                                     |
Predicate Evaluator (runtime)   →  triggered gaps + confidence
    |                                |
    v                                v
Gap-Weighted Example Scoring    Gold Examples (pre-indexed with demonstrates_gaps[])
    |
    v
KnowledgeResponse (matched_examples, relevant_gaps, relevant_strengths)
```

**No LLM at query time.** All matching is deterministic. Detection rules are human-authored.

---

## Feature Vocabulary

Every query produces a feature vector. ~25 structural features extracted via sqlglot. Features are typed and bounded so the predicate evaluator is trivial.

### SQL-Level Features (always available)

| Feature | Type | Range | Description |
|---------|------|-------|-------------|
| `join_style` | enum | explicit, implicit_comma, mixed, none | How tables are joined |
| `table_count` | int | 1-50 | Total distinct tables |
| `dimension_table_count` | int | 0-20 | Tables joined on PK, small relative to fact |
| `is_star_schema` | bool | — | One large table joined to 2+ smaller tables |
| `fact_table_max_scans` | int | 1-20 | Highest scan count of any single table |
| `tables_with_multiple_scans` | int | 0-10 | Tables scanned more than once |
| `correlated_subquery_count` | int | 0-10 | Correlated subqueries (references outer scope) |
| `correlated_with_aggregate` | int | 0-10 | Correlated subqueries containing aggregate |
| `correlated_exists_count` | int | 0-10 | Correlated EXISTS / NOT EXISTS |
| `scalar_subquery_in_select` | int | 0-10 | Scalar subqueries in SELECT list |
| `where_filters_on_dimension_tables` | int | 0-10 | Filters on dimension tables in WHERE |
| `or_chain_count` | int | 0-10 | Number of OR groups in WHERE |
| `or_branches_max` | int | 0-20 | Maximum branches in any OR chain |
| `or_branches_touch_different_indexes` | bool | — | OR branches reference different tables/indexes |
| `cte_count` | int | 0-20 | CTEs defined |
| `multi_ref_cte_count` | int | 0-10 | CTEs referenced more than once |
| `cte_max_depth` | int | 0-5 | Maximum CTE nesting depth |
| `conditional_aggregate_count` | int | 0-20 | SUM(CASE WHEN ...) etc. |
| `aggregation_type` | enum | none, simple, conditional, nested, multi_stage | Aggregation pattern |
| `has_having` | bool | — | Uses HAVING clause |
| `has_window_functions` | bool | — | Uses window functions |
| `self_join_count` | int | 0-5 | Tables joined to themselves |
| `union_branch_count` | int | 0-10 | UNION / UNION ALL branches |
| `has_lateral` | bool | — | Uses LATERAL join (PG-specific) |
| `estimated_complexity` | enum | simple, moderate, complex | Heuristic classification |

### Runtime Features (require EXPLAIN, PG only)

| Feature | Type | Range | Description |
|---------|------|-------|-------------|
| `has_disk_sort` | bool | — | Sort Method: external merge Disk |
| `disk_sort_size_mb` | float | 0-10000 | Disk sort spill size in MB |
| `has_large_seqscan` | bool | — | Sequential scan on table >1M rows |
| `large_seqscan_tables` | int | 0-10 | Count of tables with large seq scans |
| `has_jit` | bool | — | JIT compilation enabled |
| `baseline_ms` | float | 0-300000 | Baseline execution time |
| `nested_loop_on_dimension_pk` | bool | — | Nested loop + index scan for dim PK |
| `parallel_workers_used` | int | 0-16 | Parallel workers in plan |

### Relationship to Existing Tag Extraction

`tag_index.py:extract_tags()` already detects most of these features as string tags. The feature vocabulary formalizes this into typed, bounded values that support predicate evaluation.

---

## Detection Rules

Stored as JSON. You author them, code validates against feature vocabulary.

### Format

```json
{
  "detect": {
    "match": {
      "ALL": [
        {"feature": "join_style", "op": "in", "value": ["implicit_comma", "mixed"]},
        {"feature": "dimension_table_count", "op": ">=", "value": 2},
        {"feature": "where_filters_on_dimension_tables", "op": ">=", "value": 1}
      ]
    },
    "confidence": {
      "high_when": {
        "ANY": [
          {"feature": "dimension_table_count", "op": ">=", "value": 3},
          {"feature": "where_filters_on_dimension_tables", "op": ">=", "value": 2}
        ]
      },
      "low_when": {
        "ANY": [
          {"feature": "table_count", "op": "<=", "value": 2},
          {"feature": "dimension_table_count", "op": "==", "value": 1}
        ]
      }
    },
    "skip": {
      "ANY": [
        {"feature": "join_style", "op": "==", "value": "explicit"},
        {"feature": "table_count", "op": "==", "value": 1}
      ]
    }
  }
}
```

### Semantics

```
match:    ALL conditions must be true for gap to fire
skip:     ANY condition true → gap does NOT fire (overrides match)
confidence.high_when: ANY condition true → confidence = "high"
confidence.low_when:  ANY condition true → confidence = "low"
default confidence:   "medium"
```

### Operators

| Op | Types | Example |
|----|-------|---------|
| `==` | any | `{"feature": "is_star_schema", "op": "==", "value": true}` |
| `!=` | any | `{"feature": "join_style", "op": "!=", "value": "none"}` |
| `>=` | int, float | `{"feature": "dimension_table_count", "op": ">=", "value": 2}` |
| `<=` | int, float | `{"feature": "baseline_ms", "op": "<=", "value": 100}` |
| `>` | int, float | `{"feature": "fact_table_max_scans", "op": ">", "value": 2}` |
| `<` | int, float | `{"feature": "table_count", "op": "<", "value": 3}` |
| `in` | enum, list | `{"feature": "join_style", "op": "in", "value": ["implicit_comma", "mixed"]}` |

### Combinators

`ALL` = every child predicate must be true (AND)
`ANY` = at least one child predicate must be true (OR)

Combinators can nest.

---

## Predicate Evaluator

~40 lines of Python. No dependencies beyond the feature dict and the rule JSON.

```python
def evaluate_rule(rule: dict, features: dict) -> dict | None:
    """Evaluate a gap's detection rule against query features.
    Returns {"gap_id": ..., "confidence": ...} or None if not triggered.
    """
    detect = rule.get("detect")
    if not detect:
        return None

    if "skip" in detect and _eval_predicate(detect["skip"], features):
        return None

    if "match" not in detect or not _eval_predicate(detect["match"], features):
        return None

    confidence = "medium"
    conf = detect.get("confidence", {})
    if "high_when" in conf and _eval_predicate(conf["high_when"], features):
        confidence = "high"
    elif "low_when" in conf and _eval_predicate(conf["low_when"], features):
        confidence = "low"

    return {"gap_id": rule["id"], "confidence": confidence, "priority": rule["priority"]}


def _eval_predicate(pred: dict, features: dict) -> bool:
    """Evaluate a predicate tree (ALL/ANY/leaf)."""
    if "ALL" in pred:
        return all(_eval_predicate(c, features) for c in pred["ALL"])
    if "ANY" in pred:
        return any(_eval_predicate(c, features) for c in pred["ANY"])

    actual = features.get(pred["feature"])
    if actual is None:
        return False
    op, expected = pred["op"], pred["value"]
    if op == "==":  return actual == expected
    if op == "!=":  return actual != expected
    if op == ">=":  return actual >= expected
    if op == "<=":  return actual <= expected
    if op == ">":   return actual > expected
    if op == "<":   return actual < expected
    if op == "in":  return actual in expected
    return False
```

---

## Gap-Weighted Example Scoring

### Current Approach (`TagRecommender`)

From `knowledge.py:31`:
1. `extract_tags(sql, dialect)` → set of tag strings
2. For each example, compute `tag_overlap = len(query_tags & example_tags)`
3. Sort by overlap count, return top k

Pure tag overlap — no awareness of which gaps the query triggers.

### Target Approach

Replace tag-overlap with gap-weighted scoring:

```python
def score_example(query_features: dict, query_gaps: list[dict], example: dict) -> float:
    score = 0.0

    # Dominant signal: gap overlap (5x weight)
    query_gap_ids = {g["gap_id"] for g in query_gaps}
    example_gap_ids = set(example.get("demonstrates_gaps", []))
    gap_overlap = len(query_gap_ids & example_gap_ids)
    score += gap_overlap * 5.0

    # Secondary: archetype match
    if query_features.get("estimated_complexity") == example.get("classification", {}).get("complexity"):
        score += 1.0

    # Tertiary: star schema match
    if query_features.get("is_star_schema") and example.get("classification", {}).get("archetype", "").startswith("star_schema"):
        score += 1.0

    # Tertiary: table count similarity
    q_tables = query_features.get("table_count", 0)
    e_tables = example.get("precomputed_features", {}).get("table_count", 0)
    score += max(0, 1.0 - abs(q_tables - e_tables) * 0.2)

    return score
```

### Gold Example Pre-Indexing

When you promote a gold example, extract features and run detection rules to populate `demonstrates_gaps[]`:

```python
def index_gold_example(example: dict, engine_profile: dict) -> dict:
    features = extract_features(example["original_sql"], example["dialect"])
    triggered_gaps = evaluate_all_gaps(engine_profile, features)
    return {
        "precomputed_features": features,
        "demonstrates_gaps": [g["gap_id"] for g in triggered_gaps],
    }
```

---

## Detection Rule Validation

Before storing a rule, validate against the feature vocabulary:

```python
def validate_detection_rule(rule: dict, vocabulary: dict) -> list[str]:
    errors = []

    def check_predicate(pred, path=""):
        if "ALL" in pred:
            for i, child in enumerate(pred["ALL"]):
                check_predicate(child, f"{path}.ALL[{i}]")
        elif "ANY" in pred:
            for i, child in enumerate(pred["ANY"]):
                check_predicate(child, f"{path}.ANY[{i}]")
        elif "feature" in pred:
            fname = pred["feature"]
            all_features = {**vocabulary["features"], **vocabulary.get("runtime_features", {})}
            if fname not in all_features:
                errors.append(f"{path}: unknown feature '{fname}'")
            if pred.get("op") not in ("==", "!=", ">=", "<=", ">", "<", "in"):
                errors.append(f"{path}: unknown operator '{pred.get('op')}'")
        else:
            errors.append(f"{path}: predicate must have ALL, ANY, or feature")

    detect = rule.get("detect", {})
    if "match" not in detect:
        errors.append("detect.match is required")
    else:
        check_predicate(detect["match"], "match")
    if "skip" in detect:
        check_predicate(detect["skip"], "skip")
    for key in ("high_when", "low_when"):
        if key in detect.get("confidence", {}):
            check_predicate(detect["confidence"][key], f"confidence.{key}")
    return errors
```

---

## Detection Rule Storage

Detection rules are stored as separate JSON files, one per gap:

```
constraints/detection_rules/{dialect}/{GAP_ID}.json
```

They reference the gap ID from the engine profile. The `qt validate-profile` command also validates that every gap has a corresponding detection rule (or explicitly opts out).

---

## Query-Time Flow

```
1. Query SQL arrives

2. Extract features (sqlglot, ~25 features)
   → { join_style: "implicit_comma", dimension_table_count: 3, ... }

3. Evaluate all gap detection rules
   → [
       { gap_id: "CROSS_CTE_PREDICATE_BLINDNESS", confidence: "high", priority: "HIGH" },
       { gap_id: "REDUNDANT_SCAN_ELIMINATION", confidence: "medium", priority: "HIGH" }
     ]

4. Score gold examples using gap-weighted scoring
   → q6_date_cte: 5.0 (gap) + 1.0 (star schema) + 0.8 (tables) = 6.8
   → q9_single_pass: 5.0 (gap) + 1.0 (star schema) + 0.6 (tables) = 6.6
   → q1_decorrelate: 0.0 (no gap) + 1.0 (star schema) + 0.8 (tables) = 1.8

5. Build KnowledgeResponse
   → matched_examples: top 3-5 by score
   → relevant_gaps: triggered gaps with confidence
```

All deterministic. No LLM call. Microsecond evaluation.

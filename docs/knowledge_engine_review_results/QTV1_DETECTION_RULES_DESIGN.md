# LLM-Generated Detection Rules

> **Key decision**: The LLM produces gap detection rules during compression.
> The feature extractor provides a fixed vocabulary.
> A dumb predicate evaluator matches at query time. No LLM call for matching.

---

## The Contract

```
Feature Extractor (sqlglot)     →  fixed vocabulary of ~25 features
                                     ↕ (shared contract)
Detection Rules (LLM-generated) →  structured predicates over those features
                                     ↕
Predicate Evaluator (runtime)   →  which gaps fire on this query?
```

The feature vocabulary is the interface between the deterministic extractor and
the LLM-generated rules. The LLM can only write rules using features that exist
in the vocabulary. If a new gap needs a new feature, you add it to the extractor.
That's the only manual step — but it's adding an extraction (one function), not a
detection rule (one per gap per engine, growing over time).

---

## Feature Vocabulary

Every query produces this feature set. Features are typed and bounded so the
predicate evaluator is trivial.

```json
{
  "$id": "query_features_vocabulary",
  "version": "1.0",

  "features": {

    "join_style": {
      "type": "enum",
      "values": ["explicit", "implicit_comma", "mixed", "none"],
      "description": "How tables are joined",
      "extraction": "Classify FROM clause: comma-separated = implicit, JOIN ON = explicit, both = mixed"
    },

    "table_count": {
      "type": "int",
      "range": [1, 50],
      "description": "Total distinct tables referenced"
    },

    "dimension_table_count": {
      "type": "int",
      "range": [0, 20],
      "description": "Tables that appear to be dimensions (joined on PK, small relative to fact)"
    },

    "is_star_schema": {
      "type": "bool",
      "description": "One large table joined to 2+ smaller tables on their PKs"
    },

    "fact_table_max_scans": {
      "type": "int",
      "range": [1, 20],
      "description": "Highest scan count of any single table (across subqueries, UNION branches, etc.)"
    },

    "tables_with_multiple_scans": {
      "type": "int",
      "range": [0, 10],
      "description": "Count of tables scanned more than once"
    },

    "correlated_subquery_count": {
      "type": "int",
      "range": [0, 10],
      "description": "Total correlated subqueries (references outer scope)"
    },

    "correlated_with_aggregate": {
      "type": "int",
      "range": [0, 10],
      "description": "Correlated subqueries that contain an aggregate function"
    },

    "correlated_exists_count": {
      "type": "int",
      "range": [0, 10],
      "description": "Correlated EXISTS / NOT EXISTS subqueries"
    },

    "scalar_subquery_in_select": {
      "type": "int",
      "range": [0, 10],
      "description": "Scalar subqueries in SELECT list"
    },

    "where_filters_on_dimension_tables": {
      "type": "int",
      "range": [0, 10],
      "description": "Filters in WHERE clause applied to dimension tables (not join conditions)"
    },

    "or_chain_count": {
      "type": "int",
      "range": [0, 10],
      "description": "Number of OR groups in WHERE clause"
    },

    "or_branches_max": {
      "type": "int",
      "range": [0, 20],
      "description": "Maximum branches in any single OR chain"
    },

    "or_branches_touch_different_indexes": {
      "type": "bool",
      "description": "OR branches reference columns from different tables or different index paths"
    },

    "cte_count": {
      "type": "int",
      "range": [0, 20],
      "description": "Number of CTEs defined"
    },

    "multi_ref_cte_count": {
      "type": "int",
      "range": [0, 10],
      "description": "CTEs referenced more than once in the query"
    },

    "cte_max_depth": {
      "type": "int",
      "range": [0, 5],
      "description": "Maximum CTE nesting depth"
    },

    "conditional_aggregate_count": {
      "type": "int",
      "range": [0, 20],
      "description": "SUM(CASE WHEN ...), COUNT(CASE WHEN ...) etc."
    },

    "aggregation_type": {
      "type": "enum",
      "values": ["none", "simple", "conditional", "nested", "multi_stage"],
      "description": "Type of aggregation pattern"
    },

    "has_having": {
      "type": "bool",
      "description": "Query uses HAVING clause"
    },

    "has_window_functions": {
      "type": "bool",
      "description": "Query uses window functions (ROW_NUMBER, RANK, SUM OVER, etc.)"
    },

    "self_join_count": {
      "type": "int",
      "range": [0, 5],
      "description": "Tables joined to themselves"
    },

    "union_branch_count": {
      "type": "int",
      "range": [0, 10],
      "description": "Number of UNION / UNION ALL branches"
    },

    "has_lateral": {
      "type": "bool",
      "description": "Query uses LATERAL join (PG-specific)"
    },

    "estimated_complexity": {
      "type": "enum",
      "values": ["simple", "moderate", "complex"],
      "description": "Heuristic: simple (<3 tables, no subqueries), moderate, complex (5+ tables or correlated subqueries or 3+ CTEs)"
    }
  },

  "runtime_features": {
    "_note": "These require EXPLAIN output. Only available for PostgreSQL. Null for DuckDB.",

    "has_disk_sort": {
      "type": "bool",
      "description": "EXPLAIN shows Sort Method: external merge Disk"
    },

    "disk_sort_size_mb": {
      "type": "float",
      "range": [0, 10000],
      "description": "Size of disk sort spill in MB (null if no disk sort)"
    },

    "has_large_seqscan": {
      "type": "bool",
      "description": "Sequential scan on table with >1M estimated rows"
    },

    "large_seqscan_tables": {
      "type": "int",
      "range": [0, 10],
      "description": "Count of tables with large sequential scans"
    },

    "has_jit": {
      "type": "bool",
      "description": "JIT compilation enabled in plan"
    },

    "baseline_ms": {
      "type": "float",
      "range": [0, 300000],
      "description": "Baseline execution time from EXPLAIN ANALYZE"
    },

    "nested_loop_on_dimension_pk": {
      "type": "bool",
      "description": "Plan uses nested loop + index scan for dimension PK lookups"
    },

    "parallel_workers_used": {
      "type": "int",
      "range": [0, 16],
      "description": "Number of parallel workers in the plan"
    }
  }
}
```

---

## Detection Rule Format

Stored inside each engine profile gap. Produced by DeepSeek R1 during L3→L4 compression.

```json
{
  "id": "IMPLICIT_JOIN_PUSHDOWN",
  "priority": "CRITICAL",
  "what": "...",
  "why": "...",

  "detect": {
    "match": {
      "ALL": [
        {"feature": "join_style", "op": "in", "value": ["implicit_comma", "mixed"]},
        {"feature": "dimension_table_count", "op": ">=", "value": 2},
        {"feature": "where_filters_on_dimension_tables", "op": ">=", "value": 1},
        {"feature": "is_star_schema", "op": "==", "value": true}
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
  },

  "what_worked": ["..."],
  "what_didnt_work": ["..."],
  "field_notes": ["..."]
}
```

### Rule Semantics

```
match:    ALL conditions must be true for this gap to fire
skip:     ANY condition true → gap does NOT fire (overrides match)
confidence.high_when:  ANY condition true → confidence = "high"
confidence.low_when:   ANY condition true → confidence = "low"
default confidence:    "medium"
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

### Combinator Semantics

`ALL` = every child predicate must be true (AND)
`ANY` = at least one child predicate must be true (OR)

Combinators can nest:
```json
{
  "ALL": [
    {"feature": "is_star_schema", "op": "==", "value": true},
    {"ANY": [
      {"feature": "correlated_with_aggregate", "op": ">=", "value": 1},
      {"feature": "fact_table_max_scans", "op": ">=", "value": 3}
    ]}
  ]
}
```

---

## Evaluator

~40 lines of Python. No dependencies beyond the feature dict and the rule JSON.

```python
def evaluate_rule(rule: dict, features: dict) -> dict | None:
    """Evaluate a single gap's detection rule against query features.

    Returns {"gap_id": ..., "confidence": ...} or None if not triggered.
    """
    detect = rule.get("detect")
    if not detect:
        return None

    # Check skip conditions first
    if "skip" in detect:
        if _eval_predicate(detect["skip"], features):
            return None

    # Check match conditions
    if "match" not in detect:
        return None
    if not _eval_predicate(detect["match"], features):
        return None

    # Determine confidence
    confidence = "medium"
    conf = detect.get("confidence", {})
    if "high_when" in conf and _eval_predicate(conf["high_when"], features):
        confidence = "high"
    elif "low_when" in conf and _eval_predicate(conf["low_when"], features):
        confidence = "low"

    return {
        "gap_id": rule["id"],
        "confidence": confidence,
        "priority": rule["priority"]
    }


def _eval_predicate(pred: dict, features: dict) -> bool:
    """Evaluate a predicate tree (ALL/ANY/leaf)."""

    if "ALL" in pred:
        return all(_eval_predicate(child, features) for child in pred["ALL"])

    if "ANY" in pred:
        return any(_eval_predicate(child, features) for child in pred["ANY"])

    # Leaf predicate
    feature_name = pred["feature"]
    op = pred["op"]
    expected = pred["value"]

    actual = features.get(feature_name)
    if actual is None:
        return False  # missing feature = condition not met

    if op == "==":  return actual == expected
    if op == "!=":  return actual != expected
    if op == ">=":  return actual >= expected
    if op == "<=":  return actual <= expected
    if op == ">":   return actual > expected
    if op == "<":   return actual < expected
    if op == "in":  return actual in expected

    return False


def evaluate_all_gaps(engine_profile: dict, features: dict) -> list[dict]:
    """Run all gap detection rules against query features."""
    triggered = []
    for gap in engine_profile.get("gaps", []):
        result = evaluate_rule(gap, features)
        if result:
            triggered.append(result)

    # Sort by priority
    priority_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    triggered.sort(key=lambda g: priority_order.get(g["priority"], 4))
    return triggered
```

---

## Complete Example: All Gap Detection Rules for DuckDB

This is what the engine profile would look like with detection rules.
Each gap now has a `detect` field that the LLM produced during compression.

```json
{
  "engine": "duckdb",
  "gaps": [
    {
      "id": "IMPLICIT_JOIN_PUSHDOWN",
      "priority": "CRITICAL",
      "what": "Cannot push predicates through implicit comma-join syntax",
      "why": "Optimizer's pushdown logic operates on JOIN tree, not comma-separated FROM",
      "opportunity": "Convert implicit comma-joins to explicit JOIN ON with dimension CTEs",

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
            "ALL": [
              {"feature": "is_star_schema", "op": "==", "value": true},
              {"feature": "dimension_table_count", "op": ">=", "value": 3}
            ]
          },
          "low_when": {
            "ANY": [
              {"feature": "dimension_table_count", "op": "==", "value": 1},
              {"feature": "where_filters_on_dimension_tables", "op": "==", "value": 0}
            ]
          }
        },
        "skip": {
          "ANY": [
            {"feature": "join_style", "op": "==", "value": "explicit"},
            {"feature": "table_count", "op": "<=", "value": 2}
          ]
        }
      },

      "what_worked": ["q88: 6.28x", "q6: 2.67x", "q15: 2.1x", "q27: 3.41x"],
      "what_didnt_work": ["q48: 0.92x (only 2 dims, low selectivity)"],
      "field_notes": [
        "Only apply when: implicit comma-join + 2 or more dimension tables + selective filters",
        "The key diagnostic: EXPLAIN shows full fact table scan despite dimension filters in WHERE"
      ],
      "source_patterns": ["PATTERN-DIM-CTE-001"],
      "source_findings": ["F-DUCK-042", "F-DUCK-038"]
    },

    {
      "id": "CORRELATED_SUBQUERY_DECORRELATION",
      "priority": "HIGH",
      "what": "Does not automatically decorrelate subqueries that reference outer columns",
      "why": "Lacks a general decorrelation pass. Correlated subqueries execute per outer row.",
      "opportunity": "Rewrite as pre-computed CTEs with JOINs or window functions",

      "detect": {
        "match": {
          "ANY": [
            {"feature": "correlated_with_aggregate", "op": ">=", "value": 1},
            {"feature": "correlated_exists_count", "op": ">=", "value": 1},
            {"feature": "scalar_subquery_in_select", "op": ">=", "value": 1}
          ]
        },
        "confidence": {
          "high_when": {
            "feature": "correlated_with_aggregate", "op": ">=", "value": 1
          },
          "low_when": {
            "feature": "estimated_complexity", "op": "==", "value": "simple"
          }
        },
        "skip": {
          "ALL": [
            {"feature": "correlated_subquery_count", "op": "==", "value": 0}
          ]
        }
      },

      "what_worked": ["q1: 2.81x (correlated AVG → CTE)", "q23: 1.95x (EXISTS → semi-join)"],
      "what_didnt_work": ["q4: no benefit on small outer table (<1K rows)"],
      "field_notes": [
        "Check outer table cardinality — only decorrelate when outer > 10K rows",
        "Window function rewrite often cleaner than CTE + JOIN"
      ],
      "source_patterns": ["PATTERN-DECORR-001"],
      "source_findings": ["F-DUCK-029", "F-DUCK-033"]
    },

    {
      "id": "REDUNDANT_SCAN_ELIMINATION",
      "priority": "HIGH",
      "what": "Does not detect multiple scans of same fact table across subqueries/branches",
      "why": "Each subquery/UNION branch scans independently. No scan merging.",
      "opportunity": "Consolidate to single-pass with CASE expressions or CTE pre-filter",

      "detect": {
        "match": {
          "ANY": [
            {"feature": "fact_table_max_scans", "op": ">=", "value": 3},
            {
              "ALL": [
                {"feature": "fact_table_max_scans", "op": ">=", "value": 2},
                {"feature": "conditional_aggregate_count", "op": ">=", "value": 2}
              ]
            }
          ]
        },
        "confidence": {
          "high_when": {
            "feature": "fact_table_max_scans", "op": ">=", "value": 4
          }
        },
        "skip": {
          "feature": "table_count", "op": "==", "value": 1
        }
      },

      "what_worked": ["q88: 8 scans → 1 = 6.28x", "q90: 3 scans → 1 = 1.84x"],
      "what_didnt_work": ["q95: different join topologies per scan, CTE approach better"],
      "field_notes": ["Count fact table scans in EXPLAIN — if 3+, consolidation almost always wins"],
      "source_patterns": ["PATTERN-SCAN-001"],
      "source_findings": ["F-DUCK-044", "F-DUCK-047"]
    },

    {
      "id": "OR_DECOMPOSITION",
      "priority": "MEDIUM",
      "what": "Selective OR branches not split into UNION for independent optimization",
      "why": "Optimizer evaluates OR as single filter — cannot use different access paths per branch",
      "opportunity": "Rewrite as UNION ALL with one branch per OR condition",

      "detect": {
        "match": {
          "ALL": [
            {"feature": "or_chain_count", "op": ">=", "value": 1},
            {"feature": "or_branches_max", "op": ">=", "value": 2},
            {"feature": "or_branches_touch_different_indexes", "op": "==", "value": true}
          ]
        },
        "confidence": {
          "high_when": {
            "feature": "or_branches_max", "op": ">=", "value": 3
          }
        },
        "skip": {
          "feature": "or_chain_count", "op": "==", "value": 0
        }
      },

      "what_worked": ["q15: 2.98x", "q23: 2.1x"],
      "what_didnt_work": [],
      "field_notes": ["Only when OR branches touch different tables/indexes — same-table OR is fine"],
      "source_patterns": ["PATTERN-OR-UNION-001"],
      "source_findings": ["F-DUCK-058"]
    }
  ]
}
```

---

## LLM Compression Prompt for Detection Rules

During L3→L4 compression, DeepSeek R1 receives the current engine profile plus
newly promoted patterns and must produce/update detection rules.

```
You are updating the engine profile for {engine}.

A new gap is being added (or an existing gap is being updated with new evidence):

Gap ID: {gap_id}
What: {what}
Why: {why}
Queries where this was exploited successfully: {what_worked with query SQL summaries}
Queries where this failed: {what_didnt_work with query SQL summaries}

Your task: produce a DETECTION RULE that identifies when a new, unseen query
exposes this gap.

You must write the rule using ONLY these features (this is the complete feature
vocabulary — you cannot reference features that aren't listed):

{feature_vocabulary_json}

The rule format is:
{
  "match": { "ALL" or "ANY": [ predicates ] },
  "confidence": {
    "high_when": { predicate },    // optional
    "low_when": { predicate }      // optional
  },
  "skip": { predicate }            // optional: when NOT to fire
}

Each predicate is: {"feature": "name", "op": "==|!=|>=|<=|>|<|in", "value": ...}
Combinators: {"ALL": [predicates]} (AND), {"ANY": [predicates]} (OR)

GUIDELINES:
- Study the winning and losing queries. What structural features do ALL winners share?
- What structural features do the losers have that winners don't? → use these for skip/low_when
- Be specific. "has subquery" is too broad. "correlated subquery with aggregate" is good.
- If you're unsure whether a feature matters, leave it out of match and put it in confidence
- The rule must have at least one match condition
- Test mentally: would this rule fire on the winning queries? Would it skip the losing ones?

Output the detection rule as JSON only.
```

### Validation of LLM-Generated Rules

Before storing a new detection rule, validate:

```python
def validate_detection_rule(rule: dict, vocabulary: dict) -> list[str]:
    """Validate an LLM-generated detection rule against the feature vocabulary."""
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
            if fname not in vocabulary["features"] and fname not in vocabulary.get("runtime_features", {}):
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

    for key in ("skip", ):
        if key in detect:
            check_predicate(detect[key], key)

    conf = detect.get("confidence", {})
    for key in ("high_when", "low_when"):
        if key in conf:
            check_predicate(conf[key], f"confidence.{key}")

    return errors
```

If the LLM produces a rule with unknown features, the rule is rejected and the
gap is stored without a detection rule (falls back to manual matching or
full-profile-send-only mode — the gap still appears in the profile, it just
doesn't fire proactively).

---

## Gold Example Indexing

When a gold example is promoted to L4, extract its features and run it through
ALL current gap detection rules to pre-compute which gaps it demonstrates.

```python
def index_gold_example(example: GoldExample, engine_profile: dict) -> dict:
    """Pre-compute index fields for a gold example."""

    # Extract features from the ORIGINAL sql (what the query looked like before optimization)
    features = extract_features(example.original_sql, example.dialect)
    feature_dict = features_to_dict(features)

    # Run detection rules to find which gaps this example's original query triggered
    triggered_gaps = evaluate_all_gaps(engine_profile, feature_dict)

    return {
        "precomputed_features": feature_dict,
        "precomputed_gap_ids": [g["gap_id"] for g in triggered_gaps],
        "precomputed_archetype": feature_dict.get("estimated_complexity"),
        "precomputed_is_star_schema": feature_dict.get("is_star_schema"),
    }
```

### Example Scoring at Query Time

```python
def score_example(query_features: dict, query_gaps: list[dict], example: GoldExample) -> float:
    """Score how relevant a gold example is to this query."""
    score = 0.0

    # Dominant signal: gap overlap
    query_gap_ids = {g["gap_id"] for g in query_gaps}
    example_gap_ids = set(example.precomputed_gap_ids)
    gap_overlap = len(query_gap_ids & example_gap_ids)
    score += gap_overlap * 5.0

    # Secondary: archetype match
    if query_features.get("estimated_complexity") == example.precomputed_archetype:
        score += 1.0

    # Tertiary: star schema match
    if query_features.get("is_star_schema") and example.precomputed_is_star_schema:
        score += 1.0

    # Tertiary: table count similarity
    q_tables = query_features.get("table_count", 0)
    e_tables = example.precomputed_features.get("table_count", 0)
    score += max(0, 1.0 - abs(q_tables - e_tables) * 0.2)

    return score
```

---

## Lifecycle: How Detection Rules Evolve

### New Gap Discovered

1. L2 compression extracts finding: "DuckDB can't do X under conditions Y"
2. L3 aggregates: pattern confirmed across 5+ queries, promoted
3. L4 compression: LLM receives pattern + example queries, produces:
   - Gap entry (what/why/opportunity/field_notes) — human-readable
   - Detection rule (match/skip/confidence) — machine-readable
   - Both stored in engine profile
4. Validation: rule checked against feature vocabulary, tested against known winning queries
5. Gold examples re-indexed: any existing examples whose original SQL triggers this new rule get `precomputed_gap_ids` updated

### Existing Gap Updated with New Evidence

1. New blackboard entries show additional queries where this gap applies/fails
2. L3 updates pattern stats (new wins, new failures, updated success_rate)
3. L4 re-compression: LLM receives updated pattern + detection rule
4. LLM may TIGHTEN rule (add conditions based on new failures) or LOOSEN rule (remove conditions that excluded valid wins)
5. New rule validated, stored, examples re-indexed

### Gap Deprecated

1. Engine update makes gap obsolete (e.g., DuckDB adds decorrelation pass)
2. Retesting shows success_rate dropped below threshold
3. Gap status → deprecated, detection rule removed
4. Gold examples that only demonstrated this gap → deprecated

### New Feature Needed

If the LLM produces a detection rule referencing a feature that doesn't exist:

1. Rule validation catches it → rule rejected
2. Developer reviews the gap description + intended rule
3. If the feature is genuinely needed: add to vocabulary + extractor (~30 lines)
4. Re-run compression to produce valid rule
5. All existing gaps checked: could any of their rules use the new feature?

This should be RARE — the ~25 features cover the structural patterns we've
observed across 400+ optimization outcomes. A new feature means we've discovered
a genuinely new class of optimizer limitation.

---

## Integration Summary

```
Query arrives
    │
    ▼
Feature Extractor (sqlglot, ~25 features)
    │
    ▼
Predicate Evaluator (engine profile gap rules)
    │                          │
    ▼                          ▼
Triggered Gaps           Feature Dict
    │                          │
    ├──→ Attention Summary     │
    │    (prompt prefix)       │
    │                          │
    ├──→ Gold Example Scoring ◄┘
    │    (gap overlap × 5.0 + feature similarity)
    │
    ├──→ Profile Relevance
    │    (full profile sent, but triggered gaps highlighted)
    │
    ▼
KnowledgeResponse
    → exploit_report (attention_summary + triggered_gaps)
    → engine_profile (full)
    → matched_examples (top 3-5 by gap-weighted score)
    → tuning_rules (PG: filtered by runtime signals)
```

No LLM call in this path. All deterministic. All ~microsecond evaluation.
The only LLM work is during compression (offline, batch, after validation completes).

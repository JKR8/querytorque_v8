# QueryTorque V1 — Implementation Patches

**Companion to:** QueryTorque V1 Engineering Review  
**Purpose:** Copy-pasteable fixes for each action item  
**Rule:** Every patch is additive. No existing content is modified or removed.

---

## Patch 1: Q-Error Graceful Degradation

**File:** `analyst_briefing.py` — §6 Step 2, after the Q-ERROR ROUTING paragraph  
**Trigger:** §2b-i absent (no EXPLAIN ANALYZE JSON available)

Insert after the line `- **Ignore magnitude/severity** — Q-Error size does NOT predict...`:

```markdown
   **FALLBACK (when §2b-i is not available):** Use Path B — Structural routing
   (the SYMPTOM ROUTING table in §4) as your primary hypothesis anchor.
   Inspect the EXPLAIN tree directly: count scans per table, trace row count
   flow, classify each spine node. Proceed through the same DIAGNOSE →
   HYPOTHESIZE → CALIBRATE flow using structural observations instead of
   quantitative Q-Error locus. Path B is less precise but still directional.
```

**Validation:** Grep rendered prompts for "§2b-i" — if the section is absent, this paragraph should be present.

---

## Patch 2: Literal Arithmetic Standardization

**File:** `analyst_briefing.py` — §3a LITERAL_PRESERVATION constraint  
**Also:** Worker prompt NODE_CONTRACTS generation

Append to LITERAL_PRESERVATION:

```markdown
Arithmetic expressions in the original (e.g., `<= -1+2`, `<= 4+2`) SHOULD be
preserved in their original form. The optimizer evaluates constant expressions
at plan time — there is no performance difference. Preserving the original form
makes the rewrite auditable against the source query.
```

Additionally: when generating NODE_CONTRACTS for workers, use the original expression form (`hd_vehicle_count <= -1+2`) not the pre-computed form (`hd_vehicle_count <= 1`).

---

## Patch 3: PG Tuner No-EXPLAIN Fallback

**File:** `build_pg_tuner_prompt()` — after the "Analysis Instructions" section  
**Trigger:** `plan_json is None`

Insert:

```markdown
## Structural Analysis (No EXPLAIN Available)

When EXPLAIN output is not available, infer plan characteristics from query structure:

1. **Count join operations per subquery and total.** Each comma-join table pair
   likely becomes a hash join. Multiply by subquery count for total operations.
   Size work_mem: (available_memory / total_hash_sort_ops) = safe work_mem.

2. **Detect comma joins.** Implicit comma-separated FROM tables trigger
   PostgreSQL's COMMA_JOIN_WEAKNESS. For 4+ tables in comma-join, consider
   increasing `from_collapse_limit` and `join_collapse_limit` to ensure the
   planner considers all join orderings.

3. **Assess parallel opportunity.** Independent scalar subqueries can each
   benefit from parallel scans on large fact tables. Consider
   `max_parallel_workers_per_gather` up to the system cap.

4. **Evaluate JIT risk.** Queries with many similar subqueries generate high
   expression counts. For sub-2s baseline queries, JIT compilation overhead
   may exceed benefit — consider `jit=off`.

5. **Check random_page_cost.** If system uses SSD and current setting is 4.0,
   reducing to 1.0-1.5 may improve index scan selection. But ONLY if the query
   has highly selective predicates on indexed columns. Do NOT change speculatively.

When recommending without EXPLAIN evidence, note "STRUCTURAL_INFERENCE" in your
reasoning and flag lower confidence.
```

---

## Patch 4: Expert Mode Depth Guidance

**File:** `analyst_briefing.py` — §6, conditional on `mode="expert"`  
**Insert:** Before strategy selection rules

```markdown
### Expert Mode: Depth Over Breadth

You are assigning a single worker. Invest your reasoning budget in DEPTH:

- **60% — Primary bottleneck:** What is the single highest-cost operation?
  Why does the optimizer handle it suboptimally? What is the most effective
  intervention? Go deep on mechanism, not broad on alternatives.
- **30% — Composition:** Does combining 2-3 transforms from different
  categories create compound benefit? The biggest wins (42.9x Q22, 6.24x Q88)
  are compound strategies. Test whether your primary intervention combines
  well with a secondary transform.
- **10% — Risk assessment:** What is the specific regression risk for your
  chosen strategy on THIS query? Check the regression registry.

Do NOT enumerate alternative strategies. Select the single best approach and
invest your full reasoning depth in specifying it precisely.

Select the 3 most relevant examples. Relevance over coverage — 3 closely
matched examples are better than 3 diverse but loosely related ones.
```

---

## Patch 5: EXPLAIN Tree Compression

**File:** `format_duckdb_explain_tree()` in the EXPLAIN formatter  
**Trigger:** N ≥ 4 structurally similar subtrees detected

This is a code change, not a prompt-text change. Pseudocode:

```python
def format_duckdb_explain_tree(plan_json, max_lines=150):
    tree = parse_tree(plan_json)
    
    # Detect repeated subtrees
    subtrees = find_leaf_subtrees(tree)
    groups = group_by_structural_similarity(subtrees, threshold=0.9)
    
    for group in groups:
        if len(group) >= 4:
            # Compress: show one representative + variant summary
            representative = group[0]
            variants = extract_variant_fields(group)  # e.g., time_dim filters
            
            yield format_single_subtree(representative, label="Representative subtree")
            yield f"\nRepeated {len(group)}× with variant: {variants}\n"
            yield format_variant_table(group, variants)
        else:
            # Render normally
            for subtree in group:
                yield format_single_subtree(subtree)
```

The output format should look like:

```
8 structurally identical subtrees connected via CROSS_PRODUCT.
Representative subtree (s1):
  UNGROUPED_AGGREGATE (count_star)
    └── HASH_JOIN (ss_hdemo_sk = hd_demo_sk) → ~30,905 rows
        ├── HASH_JOIN (ss_store_sk = s_store_sk) → ~154,525 rows
        │   ├── HASH_JOIN (ss_sold_time_sk = t_time_sk)
        │   │   ├── SEQ_SCAN store_sales (~1.4M rows)
        │   │   └── FILTER time_dim [VARIANT]
        │   └── FILTER store (s_store_name = 'ese') → ~11 rows
        └── FILTER household_demographics → ~1,440 rows

Variant field across 8 subtrees: time_dim filter
  s1: t_hour=8 AND t_minute>=30     s2: t_hour=9 AND t_minute<30
  s3: t_hour=9 AND t_minute>=30     s4: t_hour=10 AND t_minute<30
  s5: t_hour=10 AND t_minute>=30    s6: t_hour=11 AND t_minute<30
  s7: t_hour=11 AND t_minute>=30    s8: t_hour=12 AND t_minute<30
```

---

## Patch 6: Snipe Analyst All-Pass Handling

**File:** `build_snipe_analyst_prompt()` — task description  
**Trigger:** All workers achieved target

Add after "3. SYNTHESIZE":

```markdown
**When all workers reached target:**
Your diagnostic focus shifts from failure analysis to ceiling analysis:
1. DIAGNOSE the performance spread — what structural difference between the
   best and worst approaches caused the gap? Which technique contributed most?
2. IDENTIFY remaining headroom — is there an untried structural approach, or
   have the workers converged on the same solution from different angles?
3. SYNTHESIZE — if the macro structure is solved (all workers found the same
   core optimization), guide the sniper toward micro-optimizations: join order
   tuning, column projection, CTE inlining vs materialization, or alternative
   aggregation patterns.
```

---

## Patch 7: Composition Patterns Table

**File:** Exploit algorithm (`knowledge/duckdb.md`) — after PATHOLOGIES section  
**Source:** Existing win data — no new experiments needed

```markdown
## TESTED COMPOSITIONS

Compound strategies that combined multiple pathologies in a single rewrite.
Listed only when the combination was explicitly tested and measured.

| Pathologies | Query | Result | Pattern |
|-------------|-------|--------|---------|
| P0 + P1     | Q88   | 6.24x  | Dimension CTEs (P0) + single-pass aggregation (P1) |
| P0 + P3     | Q22   | 42.9x  | Early dimension filter (P0) + pre-aggregate before join (P3) |
| P0 + P2     | Q35   | 2.42x  | Shared date CTE (P0) + decorrelate EXISTS to CTE+JOIN (P2) |

Composition rule: apply Phase 1 (P0) first, then Phase 2/3. Each phase
changes the plan shape — re-evaluate later pathologies after applying earlier ones.
```

Populate from existing win data as more compositions are validated.

---

## Patch 8: Token Budget Alignment

**File:** `analyst_briefing.py` — §7a format spec AND §7b validation checklist

Change both to the same range:

```
SEMANTIC_CONTRACT: 50-200 tokens
```

This accommodates simple queries (Q88: ~80 tokens) and complex queries (multi-fact correlated: ~180 tokens).

---

## Patch 9: Fan-Out Regression Routing

**File:** `build_fan_out_prompt()` — regression warnings section

Replace generic warnings with strategy-routed warnings:

```markdown
## Regression Warnings — Strategy-Routed

Review each warning. Apply ONLY to the worker whose strategy could trigger it.

- **regression_q90_materialize_cte** (0.59x): APPLIES TO Worker 4 (Novel)
  RULE: Do not split same-column OR into UNION ALL branches.
  
- **regression_q25_date_cte_isolate** (0.50x): APPLIES TO Workers 2-3
  RULE: Do not pre-filter fact tables when query has 3+ fact table joins.

- **regression_q95_semantic_rewrite** (0.54x): APPLIES TO Worker 4 (Novel)
  RULE: Do not decompose correlated EXISTS/NOT EXISTS pairs into independent CTEs.
```

---

## Patch 10: Unseen Query Validation

**Action:** Not a prompt change — a testing action.

```bash
# Generate sample pack for a query NOT in any pathology win list
# Candidates: queries where the exploit algorithm routes to NO MATCH
cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8

# Find queries not in win lists
python3 -c "
from qt_sql.knowledge import load_exploit_algorithm
algo = load_exploit_algorithm('duckdb')
win_queries = extract_win_queries(algo)  # Q6, Q9, Q11, Q22, Q39, Q88, etc.
all_tpcds = set(f'query_{i}' for i in range(1, 100))
unseen = all_tpcds - win_queries
print('Candidate queries for reasoning validation:', sorted(unseen)[:10])
"

# Generate sample pack for first candidate
PYTHONPATH=packages/qt-shared:packages/qt-sql:. \
  python3 -m qt_sql.prompts.samples.generate_sample <candidate_query> \
  --version V1_20260213
```

Review the rendered prompt to verify the "NO MATCH — First-Principles Reasoning" path is exercised and the reasoning chain produces coherent strategy assignments without a pre-encoded answer.

---

## Summary

| Patch | Lines Changed | Risk | Can Ship Independently |
|-------|--------------|------|----------------------|
| 1. Q-Error fallback | ~6 | None | Yes |
| 2. Literal arithmetic | ~4 | None | Yes |
| 3. PG no-EXPLAIN | ~20 | None | Yes |
| 4. Expert depth | ~15 | None | Yes |
| 5. EXPLAIN compression | ~50 (code) | Low | Yes |
| 6. Snipe all-pass | ~8 | None | Yes |
| 7. Composition table | ~10 | None | Yes |
| 8. Token alignment | ~2 | None | Yes |
| 9. Fan-out routing | ~10 | None | Yes |
| 10. Unseen validation | Testing only | None | Yes |

All patches are independent. Ship in any order. Patches 1–4 are highest priority.

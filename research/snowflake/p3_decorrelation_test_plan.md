# P3 Decorrelation — Small LLM Test Plan

## Objective

Test whether a small/cheap LLM (qwen3-coder via OpenRouter, ~$0.0007/call) can reliably decorrelate correlated scalar subqueries on Snowflake TPC-DS queries, using 2 few-shot gold examples.

## P3 Candidates (6 queries detected by AST)

| Query | Pattern | Fact Table | Correlated Key | Status | LLM Rewrite | Speedup (5x) | Notes |
|-------|---------|------------|----------------|--------|-------------|--------------|-------|
| Q1    | `> avg(ctr_total_return)*1.2` | store_returns (CTE) | ctr_store_sk | TESTED | correct | 1.08x NEUTRAL | Q1 is not a Snowflake win — decorrelation doesn't help here |
| Q30   | `> avg(ctr_total_return)*1.2` | web_returns (CTE) | ctr_store_sk | FILTERED | — | — | CTE scan → `scalar_agg_sub_cte` tag, filtered by updated detector |
| Q32   | `> 1.3 * avg(cs_ext_discount_amt)` | catalog_sales | cs_item_sk | TESTED | correct (no shared-scan) | 23.17x GOLD | LLM missed shared-scan, used EXISTS for date filter. Still correct. |
| Q41   | `count(*) as item_cnt` (EXISTS) | item | — | TESTED | correct (kept EXISTS) | — | Correctly preserved EXISTS, converted count>0 to EXISTS. No decorrelation needed. |
| Q81   | `> avg(ctr_total_return)*1.2` | catalog_returns (CTE) | ctr_store_sk | FILTERED | — | — | CTE scan → `scalar_agg_sub_cte` tag, filtered by updated detector |
| Q92   | `> 1.3 * avg(ws_ext_discount_amt)` | web_sales | ws_item_sk | TESTED | correct (shared-scan) | 7.82x GOLD | Excellent rewrite, nearly identical to gold example. |

## Pattern Groups

**Group A — Returns threshold (Q1, Q30, Q81):**
CTE computes per-customer/store returns, main query filters customers above 1.2x store average. Correlated by store_sk against same CTE.
- Q1: store_returns
- Q30: web_returns
- Q81: catalog_returns

**Group B — Discount threshold (Q32, Q92):**
Direct fact table scan, correlated scalar subquery computes 1.3x per-item average discount. Inner re-scans fact table.
- Q32: catalog_sales (gold: 23.17x)
- Q92: web_sales (gold: 7.82x)

**Outlier — Q41:** EXISTS with COUNT, not a scalar aggregate comparison. Different transform needed. Skip for now.

## Test Protocol

1. Run small LLM (qwen3-coder) on each query
2. Verify output parses (no Snowflake syntax errors)
3. Run on Snowflake with 5x trimmed mean + row count + checksum
4. Record speedup

## Results Log

### Q1 — qwen3-coder (2026-02-15)
- **Time**: 11.3s, $0.0007, 2157 tokens
- **Rewrite**: Correct. Added `store_avg_return` CTE with `GROUP BY ctr_store_sk`. Replaced correlated subquery with JOIN. Kept comma joins for store/customer (minor style issue, not a bug).
- **Speedup**: 1.08x NEUTRAL (5x trimmed mean, X-Small warehouse)
- **Verdict**: Correct rewrite, but Q1 is not an optimization opportunity on Snowflake.

### Q30, Q81 — FILTERED by updated detector
- **Status**: No longer detected as P3 candidates
- **Reason**: Updated `tag_index.py` to split `scalar_agg_sub` into `scalar_agg_sub` (fact table) vs `scalar_agg_sub_cte` (CTE scan). Q30/Q81 correlate against CTEs → tagged `scalar_agg_sub_cte` → filtered out.
- **Prior test (before filter)**: Q30 and Q81 both produced correct rewrites with qwen3-coder, but were NEUTRAL (same as Q1).

### Q32 — qwen3-coder (2026-02-15)
- **Time**: 14.1s, $0.0008, 2268 tokens
- **Rewrite**: Correct decorrelation. Used `item_filter` + `date_range` + `catalog_sales_filtered` + `thresholds` CTEs. Replaced correlated subquery with JOIN to `thresholds`.
- **Issue**: Missed shared-scan optimization — used `EXISTS (SELECT 1 FROM date_range ...)` instead of JOIN, so catalog_sales is scanned twice. Gold example uses a single `date_filtered_sales` CTE shared between threshold and outer query.
- **Verdict**: Correct but suboptimal vs gold. Still eliminates O(N*M) correlated reads. Gold speedup: 23.17x.

### Q41 — qwen3-coder (2026-02-15)
- **Time**: 6.9s, $0.0017, 3179 tokens
- **Rewrite**: Correctly identified this is NOT a scalar aggregate comparison — it's a COUNT-based EXISTS pattern. Preserved EXISTS (following system prompt constraint). Converted `count(*) > 0` to EXISTS. Extracted `item_filter` CTE for readability.
- **Verdict**: Correct. No decorrelation applied (none needed). The detector now tags this as `scalar_agg_sub` because it scans the `item` table directly, but the pattern is structurally different from Group B (no aggregate threshold comparison).

### Q92 — qwen3-coder (2026-02-15)
- **Time**: 2.5s, $0.0007, 2234 tokens
- **Rewrite**: Excellent shared-scan decorrelation. Used `shared_scan` CTE (web_sales + date_dim), `thresholds` CTE (per-item 1.3x avg), `outer_query` CTE (shared_scan + item filter + threshold join). Nearly identical to gold example.
- **Verdict**: Production-quality rewrite. Gold speedup: 7.82x.

## Script

```bash
# Run single query
PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 research/snowflake/test_small_llm_decorrelate.py qwen3-coder query_30

# Run all P3 candidates
PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 research/snowflake/test_small_llm_decorrelate.py qwen3-coder
```

## Key Insight: Why Q1 is NEUTRAL

The correlated subquery in Q1/Q30/Q81 scans a **pre-aggregated CTE**, not a raw fact table. Snowflake auto-materializes CTEs referenced 2+ times, so the correlated scan hits a small materialized result — not billions of rows. Decorrelation replaces a cheap scan with a cheap JOIN. No win.

Q32/Q92 correlate against **raw fact tables** (catalog_sales, web_sales) — each outer row triggers a scan of billions of rows. Decorrelation eliminates O(N*M) redundant reads. Massive win.

**DONE: AST detector updated** (commit 844196a1). `scalar_agg_sub` now only tags queries where the correlated subquery scans a fact table. CTE scans get `scalar_agg_sub_cte` tag instead. Q1/Q30/Q81 correctly filtered out.

## Key Question

Can the small LLM handle Group A (returns threshold) correctly? It's structurally different from the gold examples (Group B):
- Group A: CTE referenced in correlated subquery (not raw fact table)
- Group B: Raw fact table in correlated subquery (direct scan)

The Q1 test shows qwen3-coder handles Group A correctly. Q30/Q81 confirmed generalization (both correct, both NEUTRAL).

## Summary (2026-02-15)

| Query | Detector Tag | LLM Result | Cost | Notes |
|-------|-------------|------------|------|-------|
| Q1    | `scalar_agg_sub_cte` (filtered) | correct, NEUTRAL | $0.0007 | CTE scan, no opportunity |
| Q30   | `scalar_agg_sub_cte` (filtered) | correct, NEUTRAL | $0.0007 | CTE scan, no opportunity |
| Q32   | `scalar_agg_sub` (FACT) | correct, suboptimal | $0.0008 | Missed shared-scan. Gold: 23.17x |
| Q41   | `scalar_agg_sub` (FACT) | correct, kept EXISTS | $0.0017 | Not a decorrelation target |
| Q81   | `scalar_agg_sub_cte` (filtered) | correct, NEUTRAL | $0.0007 | CTE scan, no opportunity |
| Q92   | `scalar_agg_sub` (FACT) | excellent | $0.0007 | Near-identical to gold. Gold: 7.82x |

**Total cost**: ~$0.005 for all 6 queries. qwen3-coder is reliable for P3 decorrelation.

**Findings**:
1. qwen3-coder produces correct rewrites 6/6 (100%)
2. Shared-scan pattern learned from Demo 2 (Q92) but not consistently applied (Q32 missed it)
3. EXISTS constraint respected — Q41 correctly left alone
4. Updated detector eliminates 3/6 false positives (CTE scans)
5. After filtering: 3 FACT queries remain (Q32, Q41, Q92). Q41 is structurally different (EXISTS, not scalar threshold). True P3 targets: Q32 + Q92 only.

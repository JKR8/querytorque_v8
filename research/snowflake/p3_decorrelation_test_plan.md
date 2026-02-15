# P3 Decorrelation — Small LLM Test Plan

## Objective

Test whether a small/cheap LLM (qwen3-coder via OpenRouter, ~$0.0007/call) can reliably decorrelate correlated scalar subqueries on Snowflake TPC-DS queries, using 2 few-shot gold examples.

## P3 Candidates (6 queries detected by AST)

| Query | Pattern | Fact Table | Correlated Key | Status | LLM Rewrite | Speedup (5x) | Notes |
|-------|---------|------------|----------------|--------|-------------|--------------|-------|
| Q1    | `> avg(ctr_total_return)*1.2` | store_returns (CTE) | ctr_store_sk | TESTED | correct | 1.08x NEUTRAL | Q1 is not a Snowflake win — decorrelation doesn't help here |
| Q30   | `> avg(ctr_total_return)*1.2` | web_returns (CTE) | ctr_store_sk | TODO | | | Same template as Q1, different returns table |
| Q32   | `> 1.3 * avg(cs_ext_discount_amt)` | catalog_sales | cs_item_sk | GOLD | gold example | 23.17x (MEDIUM) | Gold example: inline_decorrelate |
| Q41   | `count(*) as item_cnt` (EXISTS) | item | — | SKIP | | | Different pattern — count-based EXISTS, not scalar comparison |
| Q81   | `> avg(ctr_total_return)*1.2` | catalog_returns (CTE) | ctr_store_sk | TODO | | | Same template as Q1/Q30, different returns table |
| Q92   | `> 1.3 * avg(ws_ext_discount_amt)` | web_sales | ws_item_sk | GOLD | gold example | 7.82x (MEDIUM) | Gold example: shared_scan_decorrelate |

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

### Q30 — TODO

### Q81 — TODO

### Q32 — GOLD (skip, already validated 23.17x)

### Q92 — GOLD (skip, already validated 7.82x)

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

**TODO: Update AST detector** — P3 gate should require the correlated subquery to re-scan a raw fact table, not a CTE. This would filter out Q1/Q30/Q81 before wasting an LLM call. Low priority since LLM calls are ~$0.0007.

## Key Question

Can the small LLM handle Group A (returns threshold) correctly? It's structurally different from the gold examples (Group B):
- Group A: CTE referenced in correlated subquery (not raw fact table)
- Group B: Raw fact table in correlated subquery (direct scan)

The Q1 test shows qwen3-coder handles Group A correctly. Q30/Q81 will confirm generalization.

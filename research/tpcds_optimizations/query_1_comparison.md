# TPC-DS Query 1 - Optimization Comparison

## Benchmark Results (SF100)

| Version | Run 1 | Run 2 | Average | Speedup |
|---------|-------|-------|---------|---------|
| Original | 0.902s | 0.273s | 0.588s | 1.0x |
| DeepSeek v1 (no explain) | 0.252s | 0.255s | 0.253s | 2.3x |
| Manual | 0.111s | 0.113s | 0.112s | 5.25x |
| **DeepSeek v2 (with explain)** | 0.108s | 0.100s | **0.104s** | **5.65x** |

**All versions produce identical results.**

---

## Full Benchmark History

| Version | Method | Avg Time | Speedup |
|---------|--------|----------|---------|
| Original | - | 1.122s | 1.0x |
| DeepSeek v1 | Query only (broken CLI) | 0.253s | 2.3x |
| Manual | Hand-optimized | 0.112s | 5.25x |
| **DeepSeek v2** | Query + EXPLAIN feedback | **0.140s** | **8.02x** |
| DeepSeek v3 | New payload builder w/ schema | 0.327s | 3.44x |

---

## Key Finding: Explain Plans Enable LLM Semantic Optimization

When DeepSeek was given only the SQL query, it:
- Eliminated the correlated subquery (good)
- Missed the predicate pushdown opportunity

When DeepSeek was given the SQL + EXPLAIN plan showing row counts:
- Immediately identified: *"filter early, aggregate late"* violation
- Applied predicate pushdown via `store_filtered` CTE
- Achieved 5.65x speedup (better than manual)

---

## Analysis

### DeepSeek's Approach
- Eliminated correlated subquery via separate `store_avg_return` CTE
- Converted implicit joins to explicit JOIN syntax
- **Missed:** Predicate pushdown for `s_state = 'SD'`

```sql
-- DeepSeek: Aggregates ALL stores, filters AFTER
WITH customer_total_return AS (
    SELECT ... FROM store_returns
    JOIN date_dim ON ...
    WHERE d_year = 2000  -- Only date filter
    GROUP BY ...
),
store_avg_return AS (...)
SELECT ...
WHERE s_state = 'SD'  -- Filter applied LATE
```

### Manual Approach
- Eliminated correlated subquery via window function
- **Applied predicate pushdown** - filter `s_state = 'SD'` BEFORE aggregation
- Early join with `store` table enables the pushdown

```sql
-- Manual: Filters to SD stores BEFORE aggregation
WITH sd_store_returns AS (
    SELECT ... FROM store_returns
    JOIN date_dim ON ...
    JOIN store ON sr_store_sk = s_store_sk  -- Early join
    WHERE d_year = 2000
      AND s_state = 'SD'  -- Filter applied EARLY
    GROUP BY ...
)
```

---

## Why Predicate Pushdown Matters

| Metric | DeepSeek | Manual |
|--------|----------|--------|
| Rows aggregated | ~29M (all stores) | ~300K (SD only) |
| Store filter timing | After aggregation | Before aggregation |
| Data reduction | Late | Early |

The TPC-DS SF100 `store_returns` table has ~29M rows.
- DeepSeek aggregates all of them, then discards non-SD stores
- Manual filters to SD stores first (~1% of data), then aggregates

This 100x reduction in input data before aggregation explains the 2x performance gap.

---

## Lessons

1. **Correlated subquery elimination** is necessary but not sufficient
2. **Predicate pushdown** can have larger impact than query restructuring
3. **Join order matters** - join `store` early to enable filter pushdown
4. LLMs often optimize syntax/structure but miss data-volume optimizations

# TPC-DS Q2 Optimization

**Status:** ✅ SUCCESS (Gemini) | ❌ FAILED (OSS)
**Speedup:** 2.09x (Gemini, cache-warmed)
**Best Model:** Gemini

---

## Original Query

```sql
with wscs as
 (select sold_date_sk
        ,sales_price
  from (select ws_sold_date_sk sold_date_sk
              ,ws_ext_sales_price sales_price
        from web_sales
        union all
        select cs_sold_date_sk sold_date_sk
              ,cs_ext_sales_price sales_price
        from catalog_sales)),
 wswscs as
 (select d_week_seq,
        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
 from wscs
     ,date_dim
 where d_date_sk = sold_date_sk
 group by d_week_seq)
 select d_week_seq1
       ,round(sun_sales1/sun_sales2,2)
       ,round(mon_sales1/mon_sales2,2)
       ,round(tue_sales1/tue_sales2,2)
       ,round(wed_sales1/wed_sales2,2)
       ,round(thu_sales1/thu_sales2,2)
       ,round(fri_sales1/fri_sales2,2)
       ,round(sat_sales1/sat_sales2,2)
 from
 (select wswscs.d_week_seq d_week_seq1
        ,sun_sales sun_sales1
        ,mon_sales mon_sales1
        ,tue_sales tue_sales1
        ,wed_sales wed_sales1
        ,thu_sales thu_sales1
        ,fri_sales fri_sales1
        ,sat_sales sat_sales1
  from wswscs,date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and
        d_year = 1998) y,
 (select wswscs.d_week_seq d_week_seq2
        ,sun_sales sun_sales2
        ,mon_sales mon_sales2
        ,tue_sales tue_sales2
        ,wed_sales wed_sales2
        ,thu_sales thu_sales2
        ,fri_sales fri_sales2
        ,sat_sales sat_sales2
  from wswscs
      ,date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and
        d_year = 1998+1) z
 where d_week_seq1=d_week_seq2-53
 order by d_week_seq1;
```

---

## Prompt Used

```
Optimize this SQL query.

## Algorithm

1. ANALYZE: Find where rows/cost are largest in the plan.
2. OPTIMIZE: For each bottleneck, ask "what could reduce it earlier?"
   - Can a filter be pushed inside a CTE instead of applied after?
   - Can a small table join happen inside an aggregation to filter before GROUP BY?
   - Is there a correlated subquery? Convert to CTE + JOIN.
3. VERIFY: Result must be semantically equivalent.

Principle: Reduce rows as early as possible.

## Plan

Operators by cost:
- SEQ_SCAN catalog_sales: 49% cost, 1,727,964,780 rows
- SEQ_SCAN web_sales: 31% cost, 864,014,844 rows
- HASH_GROUP_BY: 8% cost, 264 rows

Scans:
- catalog_sales: 1,727,964,780 rows (no filter)
- web_sales: 864,014,844 rows (no filter)
- date_dim: 73,049 → 1,840 rows (filtered)

## Data Flow

- CTE wscs: [web_sales, catalog_sales] via UNION ALL
- CTE wswscs: [wscs, date_dim] → GROUP BY → 264 rows
- Main query: joins wswscs twice with date_dim WHERE d_year=1998 and d_year=1999

## SQL

<original query>

## Output

Return JSON:
{"patches": [{"search": "...", "replace": "...", "description": "..."}], "explanation": "..."}
```

---

## ✅ Gemini Response (SUCCESS)

```json
{
  "patches": [
    {
      "search": " from wscs\n     ,date_dim\n where d_date_sk = sold_date_sk\n group by d_week_seq)",
      "replace": " from wscs\n     ,date_dim\n where d_date_sk = sold_date_sk\n   and d_week_seq in (select d_week_seq from date_dim where d_year in (1998, 1999))\n group by d_week_seq)",
      "description": "Push filter down: Restrict the heavy aggregation in the CTE to only process weeks belonging to the years 1998 and 1999."
    }
  ],
  "explanation": "The original query aggregates sales data for all years in the wswscs CTE before filtering for 1998 and 1999 in the main query. By injecting a filter on d_week_seq into the CTE, we enable the optimizer to discard the vast majority of sales rows (from other years) before the expensive Group By operation."
}
```

### Gemini Optimized Query

```sql
with wscs as
 (select sold_date_sk
        ,sales_price
  from (select ws_sold_date_sk sold_date_sk
              ,ws_ext_sales_price sales_price
        from web_sales
        union all
        select cs_sold_date_sk sold_date_sk
              ,cs_ext_sales_price sales_price
        from catalog_sales)),
 wswscs as
 (select d_week_seq,
        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
 from wscs
     ,date_dim
 where d_date_sk = sold_date_sk
   and d_week_seq in (select d_week_seq from date_dim where d_year in (1998, 1999))
 group by d_week_seq)
 select d_week_seq1
       ,round(sun_sales1/sun_sales2,2)
       ,round(mon_sales1/mon_sales2,2)
       ,round(tue_sales1/tue_sales2,2)
       ,round(wed_sales1/wed_sales2,2)
       ,round(thu_sales1/thu_sales2,2)
       ,round(fri_sales1/fri_sales2,2)
       ,round(sat_sales1/sat_sales2,2)
 from
 (select wswscs.d_week_seq d_week_seq1
        ,sun_sales sun_sales1
        ,mon_sales mon_sales1
        ,tue_sales tue_sales1
        ,wed_sales wed_sales1
        ,thu_sales thu_sales1
        ,fri_sales fri_sales1
        ,sat_sales sat_sales1
  from wswscs,date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and
        d_year = 1998) y,
 (select wswscs.d_week_seq d_week_seq2
        ,sun_sales sun_sales2
        ,mon_sales mon_sales2
        ,tue_sales tue_sales2
        ,wed_sales wed_sales2
        ,thu_sales thu_sales2
        ,fri_sales fri_sales2
        ,sat_sales sat_sales2
  from wswscs
      ,date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and
        d_year = 1998+1) z
 where d_week_seq1=d_week_seq2-53
 order by d_week_seq1;
```

---

## ❌ OSS Response (FAILED - broke semantics)

```json
{
  "patches": [
    {
      "search": "from wscs\n join date_dim...",
      "replace": "...where date_dim.d_year IN (1998,1999)\n group by d_year, d_week_seq)",
      "description": "Push the year filter into the CTE"
    },
    {
      "search": "<entire main query>",
      "replace": "select y.d_week_seq...\nfrom wswscs y\njoin wswscs z\n on y.d_week_seq = z.d_week_seq - 53\n and y.d_year = 1998\n and z.d_year = 1999...",
      "description": "Self-join on wswscs instead of duplicate scans"
    }
  ],
  "explanation": "..."
}
```

**Why it failed:** Added `d_year` to GROUP BY, changing the aggregation granularity. Returned 52 rows instead of 2513.

---

## Benchmark

| Model | Time | Speedup | Correct? |
|-------|------|---------|----------|
| Original | 1.067s | 1.00x | - |
| **Gemini** | **0.511s** | **2.09x** | ✅ 2513 rows |
| OSS | 0.307s | 3.48x | ❌ 52 rows |

---

## AST Issues Detected (not useful)

```
- [QT-AGG-002] Aggregate After Join
- [SQL-JOIN-002] Implicit Join Syntax
- [SQL-JOIN-001] Cartesian Join
- [SQL-JOIN-010] Complex Subquery in JOIN
- [SQL-ORD-003] ORDER BY Without LIMIT
- [SQL-CTE-001] CTE Referenced Multiple Times
- [SQL-CTE-006] Aggregate CTE Reused
- [SQL-CTE-005] Simple CTE Used Once
- [QT-CTE-002] Single-Use CTE
```

**13 issues detected. 0 were the actual problem.**

---

## Key Insight

**Conservative optimization wins:** Gemini added a single filter without restructuring the query. OSS tried to be clever by restructuring and broke semantics.

The optimization was **filter pushdown**: adding `d_week_seq IN (select ... where d_year in (1998, 1999))` to restrict 2.6B rows to only the needed weeks BEFORE aggregation.

**Lesson:** Simpler patches that preserve query structure are safer AND often faster.

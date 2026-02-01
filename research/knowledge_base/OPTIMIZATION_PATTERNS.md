# Optimization Patterns That Work

These patterns have produced ≥1.25x speedups on TPC-DS SF100.

---

## 1. Predicate Pushdown (2.1-2.5x)

**Signal**: Small filtered dimension table joined AFTER large fact table aggregation.

**Fix**: Join the dimension INSIDE the CTE, before GROUP BY.

```sql
-- BEFORE
WITH agg AS (
  SELECT key, sum(value) FROM fact_table, date_dim
  WHERE fact.date_sk = date_dim.date_sk AND year = 2000
  GROUP BY key
)
SELECT * FROM agg, dimension
WHERE agg.key = dimension.key AND dimension.filter = 'X'

-- AFTER
WITH agg AS (
  SELECT key, sum(value) FROM fact_table, date_dim, dimension
  WHERE fact.date_sk = date_dim.date_sk AND year = 2000
    AND fact.key = dimension.key AND dimension.filter = 'X'  -- pushed in
  GROUP BY key
)
SELECT * FROM agg
```

---

## 2. Scan Consolidation (1.25x)

**Signal**: Same table scanned multiple times with different filters.

**Fix**: Single scan with CASE WHEN for conditional aggregates.

```sql
-- BEFORE
cte_filtered AS (SELECT key, sum(val) FROM t WHERE year = 2000 GROUP BY key),
cte_all AS (SELECT key, sum(val) FROM t GROUP BY key)

-- AFTER
cte_combined AS (
  SELECT key,
         sum(CASE WHEN year = 2000 THEN val ELSE 0 END) AS filtered_sum,
         sum(val) AS total_sum
  FROM t
  GROUP BY key
)
```

---

## 3. Join Elimination (2.18x)

**Signal**: Table joined only to validate FK exists, no columns used from it.

**Fix**: Remove join, add `WHERE fk IS NOT NULL`.

```sql
-- BEFORE
SELECT a.id, sum(a.value)
FROM fact a JOIN dim d ON a.dim_key = d.id
GROUP BY a.id

-- AFTER
SELECT id, sum(value)
FROM fact
WHERE dim_key IS NOT NULL
GROUP BY id
```

**Critical**: The join implicitly filters NULLs. You must add IS NOT NULL.

**Proven Result (Q23)**: Removed joins to `item` and `customer` tables in 3 CTEs. **2.18x speedup** (24.5s → 11.3s) with exact semantic match.

---

## 4. Correlated Subquery → Window Function (2.5x)

**Signal**: Correlated subquery computes aggregate per group.

**Fix**: Window function in the CTE.

```sql
-- BEFORE
SELECT * FROM t
WHERE t.value > (SELECT avg(value) FROM t t2 WHERE t.group = t2.group)

-- AFTER
WITH t_with_avg AS (
  SELECT *, avg(value) OVER (PARTITION BY group) AS group_avg
  FROM t
)
SELECT * FROM t_with_avg WHERE value > group_avg
```

---

## Anti-Patterns (Don't Do These)

| Mistake | Why It Fails |
|---------|--------------|
| Add filter to "all-time" CTE | May be intentional (comparing periods) |
| Remove join without IS NOT NULL | Changes results (NULLs included) |
| Add redundant IN subquery | Filter already exists via join |

# PostgreSQL DSB Query Optimization Prompt

You are an expert PostgreSQL query optimizer specializing in analytical workloads. Your task is to rewrite the provided query to achieve better execution performance while preserving exact semantic equivalence.

---

## Input Context

### PostgreSQL Version & Configuration
```
{{POSTGRES_VERSION}}
```

**Relevant Settings:**
```
{{POSTGRES_SETTINGS}}
```
<!-- Example format:
work_mem = 256MB
random_page_cost = 1.1
effective_cache_size = 24GB
join_collapse_limit = 8
from_collapse_limit = 8
geqo_threshold = 12
default_statistics_target = 100
-->

---

### Database Schema

```sql
{{SCHEMA_DDL}}
```
<!-- Provide CREATE TABLE statements with:
- Column names and types
- PRIMARY KEY constraints
- FOREIGN KEY constraints (critical for join understanding)
- CHECK constraints if relevant
- Index definitions (CREATE INDEX statements)
-->

---

### Table Statistics

```
{{TABLE_STATISTICS}}
```
<!-- Format:
Table: <table_name>
  Row count: <n_live_tup>
  Pages: <n_pages>
  Column statistics:
    - <column>: ndistinct=<n>, null_frac=<f>, most_common_vals=[...], correlation=<c>
  
Extended statistics (if any):
  - <stat_name>: type=<dependencies|ndistinct|mcv>, columns=(<col1>, <col2>)
-->

---

### Original Query

```sql
{{ORIGINAL_QUERY}}
```

---

### Current Execution Plan

```
{{EXPLAIN_ANALYZE_OUTPUT}}
```
<!-- Must be output from:
EXPLAIN (ANALYZE, BUFFERS, COSTS, VERBOSE, FORMAT TEXT) <query>;

Key metrics to note:
- Total execution time
- Rows estimated vs actual at each node
- Loops count for nested operations
- Buffer hits vs reads
- Sort/hash memory usage
-->

---

## Optimization Analysis Framework

Analyze the query and execution plan systematically:

### Step 1: Identify Performance Bottlenecks

Examine the execution plan for these red flags:
1. **Cardinality misestimates**: rows=X vs actual=Y where ratio > 10x
2. **Nested loop joins on large tables**: Look for "Nested Loop" with high loop counts
3. **Sequential scans on large tables**: When indexes exist but aren't used
4. **Repeated subplan execution**: "SubPlan" nodes with loops >> 1
5. **Spilling to disk**: "Sort Method: external merge" or "Batches: N"
6. **CTE materialization blocking predicate pushdown**: "CTE Scan" without pushed filters

### Step 2: Match Bottlenecks to Rewrite Patterns

Apply these high-impact rewrites in priority order:

#### Pattern A: Correlated Subquery → JOIN (100-3000× improvement potential)

**Detect**: Scalar subquery in SELECT or correlated subquery in WHERE executed per-row
```sql
-- BEFORE: Executes subquery once per outer row
SELECT a.id, (SELECT SUM(b.val) FROM b WHERE b.aid = a.id) as total
FROM a;

-- AFTER: Single hash/merge join
SELECT a.id, COALESCE(b_agg.total, 0) as total
FROM a
LEFT JOIN (SELECT aid, SUM(val) as total FROM b GROUP BY aid) b_agg 
  ON b_agg.aid = a.id;
```

#### Pattern B: NOT IN → NOT EXISTS (Enables anti-join)

**Detect**: "Seq Scan" or "Index Scan" inside "SubPlan" for NOT IN check
```sql
-- BEFORE: Cannot use anti-join due to NULL semantics
SELECT * FROM a WHERE a.x NOT IN (SELECT b.x FROM b);

-- AFTER: Enables hash/merge anti-join
SELECT * FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.x = a.x);
```

#### Pattern C: IN Subquery → EXISTS or JOIN

**Detect**: Large "SubPlan" result being materialized for IN check
```sql
-- BEFORE: Materializes entire subquery result
SELECT * FROM a WHERE a.x IN (SELECT b.x FROM b WHERE b.flag = true);

-- AFTER (semi-join): 
SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.x = a.x AND b.flag = true);

-- AFTER (explicit join, if duplicates acceptable or known unique):
SELECT DISTINCT a.* FROM a JOIN b ON a.x = b.x WHERE b.flag = true;
```

#### Pattern D: Correlated Comparison → Window Function

**Detect**: Correlated subquery computing aggregate for comparison (avg, max, etc.)
```sql
-- BEFORE: Recomputes average for each row
SELECT * FROM sales s1
WHERE s1.amount > (SELECT AVG(amount) FROM sales s2 WHERE s2.region = s1.region);

-- AFTER: Single-pass window computation
SELECT * FROM (
  SELECT *, AVG(amount) OVER (PARTITION BY region) as region_avg
  FROM sales
) sub WHERE amount > region_avg;
```

#### Pattern E: CTE Materialization Control (PostgreSQL 12+)

**Detect**: CTE scan without predicate pushdown when predicates exist in outer query
```sql
-- BEFORE: Full scan of CTE, then filter
WITH data AS (SELECT * FROM large_table)
SELECT * FROM data WHERE id = 100;

-- AFTER: Force inlining for predicate pushdown
WITH data AS NOT MATERIALIZED (SELECT * FROM large_table)
SELECT * FROM data WHERE id = 100;

-- Or simply inline:
SELECT * FROM (SELECT * FROM large_table) data WHERE id = 100;
```

**Conversely**, force materialization when CTE is expensive and referenced multiple times:
```sql
WITH expensive AS MATERIALIZED (
  SELECT id, complex_function(data) as result FROM source
)
SELECT * FROM expensive e1 JOIN expensive e2 ON e1.result = e2.result;
```

#### Pattern F: Join Order Optimization

**Detect**: Nested loops or hash joins processing rows in suboptimal order (large intermediate results early)
```sql
-- Force specific join order when optimizer chooses poorly:
SET LOCAL join_collapse_limit = 1;

-- Explicit order: filter dimensions first, then join to facts
SELECT * 
FROM (small_dimension d1 
      JOIN fact_table f ON d1.sk = f.d1_sk)
JOIN small_dimension d2 ON f.d2_sk = d2.sk
WHERE d1.filter_col = 'value';
```

#### Pattern G: UNION ALL → Single Scan with CASE

**Detect**: Multiple sequential scans of same table with different filters
```sql
-- BEFORE: Two full scans
SELECT 'type_a' as type, col1, col2 FROM t WHERE flag = 'A'
UNION ALL
SELECT 'type_b' as type, col1, col2 FROM t WHERE flag = 'B';

-- AFTER: Single scan
SELECT CASE flag WHEN 'A' THEN 'type_a' WHEN 'B' THEN 'type_b' END as type,
       col1, col2
FROM t WHERE flag IN ('A', 'B');
```

#### Pattern H: DISTINCT Elimination

**Detect**: DISTINCT on columns that are already unique due to joins/keys
```sql
-- BEFORE: Unnecessary sort/hash for uniqueness
SELECT DISTINCT a.id, a.name FROM a JOIN b ON a.id = b.aid;

-- AFTER: If a.id is PRIMARY KEY and b.aid has unique constraint on a.id
SELECT a.id, a.name FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.aid = a.id);
```

#### Pattern I: Predicate Pushdown Through Outer Joins

**Detect**: Filter on nullable side applied after outer join
```sql
-- BEFORE: Filter cannot push through LEFT JOIN
SELECT * FROM a LEFT JOIN b ON a.id = b.aid WHERE b.col = 'x';

-- AFTER: If filter implies non-NULL, convert to INNER JOIN
SELECT * FROM a INNER JOIN b ON a.id = b.aid WHERE b.col = 'x';
```

### Step 3: Verify Semantic Equivalence

For each rewrite, verify:
1. **NULL handling**: Does the rewrite preserve NULL behavior?
   - NOT IN vs NOT EXISTS have different NULL semantics
   - LEFT JOIN + filter may convert to INNER JOIN semantics
2. **Duplicate handling**: Does the rewrite preserve row multiplicity?
   - Converting IN to JOIN may introduce duplicates
   - Removing DISTINCT requires uniqueness guarantee
3. **Aggregate semantics**: Are empty groups handled correctly?
   - COALESCE needed when converting correlated aggregates to LEFT JOINs

---

## Output Format

Provide your response in this structure:

### Analysis

**Identified Bottlenecks:**
1. [Bottleneck description with line reference from EXPLAIN]
2. ...

**Root Causes:**
- [Why the optimizer made this choice]

### Recommended Rewrites

**Rewrite 1: [Pattern Name]**

*Rationale:* [Why this rewrite helps]

*Original fragment:*
```sql
[relevant portion of original query]
```

*Rewritten fragment:*
```sql
[optimized version]
```

*Expected improvement:* [Estimated speedup and why]

### Complete Optimized Query

```sql
{{OPTIMIZED_QUERY}}
```

### Verification Checklist

- [ ] NULL semantics preserved
- [ ] Duplicate rows handled correctly  
- [ ] Empty group/no-match cases return same results
- [ ] All columns in SELECT/ORDER BY still accessible
- [ ] No semantic change to WHERE/HAVING predicates

### Additional Recommendations

**Index suggestions:**
```sql
-- [If beneficial indexes don't exist]
CREATE INDEX ... ;
```

**Statistics recommendations:**
```sql
-- [If extended statistics would help]
CREATE STATISTICS ... ;
```

**Configuration hints:**
```sql
-- [Session-level settings if needed]
SET LOCAL work_mem = '512MB';
```

---

## Constraints

1. **Preserve exact semantics** - The rewritten query must return identical results for all possible data states
2. **No schema changes required** - Rewrites should work with existing schema (index/stats suggestions are optional extras)
3. **Standard SQL preferred** - Avoid PostgreSQL-specific syntax unless necessary for the optimization
4. **Explain your reasoning** - Each rewrite should have clear justification tied to the execution plan

---

## Example Input/Output

### Example Input

**Query:**
```sql
SELECT c.customer_id, c.name,
       (SELECT SUM(o.amount) FROM orders o WHERE o.customer_id = c.customer_id) as total_orders
FROM customers c
WHERE c.region = 'West'
  AND (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.customer_id) > 5;
```

**Plan excerpt:**
```
Seq Scan on customers c (actual rows=10000 loops=1)
  Filter: (region = 'West')
  SubPlan 1
    ->  Aggregate (actual rows=1 loops=10000)
          ->  Index Scan on orders o (actual rows=50 loops=10000)
  SubPlan 2
    ->  Aggregate (actual rows=1 loops=10000)
          ->  Index Scan on orders o (actual rows=50 loops=10000)
```

### Example Output

**Analysis:**
Two correlated subqueries execute 10,000 times each (loops=10000), scanning ~50 order rows per execution = 1M total order row accesses.

**Rewrite (Pattern A + D):**
```sql
SELECT c.customer_id, c.name, o_agg.total_orders
FROM customers c
JOIN (
    SELECT customer_id, 
           SUM(amount) as total_orders,
           COUNT(*) as order_count
    FROM orders
    GROUP BY customer_id
) o_agg ON o_agg.customer_id = c.customer_id
WHERE c.region = 'West'
  AND o_agg.order_count > 5;
```

*Expected improvement:* ~100-500× faster. Single scan of orders table with hash join instead of 20,000 correlated index scans.

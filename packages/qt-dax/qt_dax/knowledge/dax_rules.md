# QueryTorque / Dialect Labs: Complete Rule Definitions
## 49 Rules (29 DAX + 15 Model + 5 Calculation Groups)

Comprehensive DAX, Model, and Calculation Group anti-pattern detection.

---

<!-- RULE_SUMMARY_START -->

## Rule Summary Table

| Rule ID | Name | Severity | Category | Confirmed | Summary |
|---|---|---|---|---|---|
| CG001 | COMPLEX_DAX_IN_CALCULATION_ITEMS | high | dax_performance | no | Calculation item contains complex DAX (nested CALCULATE or many iterators) |
| CG002 | OVERLAPPING_PRECEDENCE_VALUES | medium | dax_maintainability | no | Calculation groups share the same precedence value |
| CG003 | SELECTEDMEASURE_OVERHEAD | medium | dax_performance | no | SELECTEDMEASURE() used with IF without ISSELECTEDMEASURE guard |
| CG004 | UNUSED_CALCULATION_ITEMS | low | dax_maintainability | no | Calculation item appears unused or usage cannot be verified |
| CG005 | MISSING_ISSELECTEDMEASURE_GUARD | medium | dax_correctness | no | Hardcoded measure references without ISSELECTEDMEASURE guard |
| DAX001 | FILTER_TABLE_ITERATOR | critical | dax_anti_pattern | no | FILTER iterating entire table instead of column |
| DAX002 | SUMX_FILTER_COMBO | critical | dax_anti_pattern | no | SUMX/AVERAGEX with FILTER creates row-by-row iteration |
| DAX003 | DEEP_CALCULATE_NESTING | critical | dax_performance | no | Excessive CALCULATE nesting causes exponential context transitions |
| DAX004 | DIVISION_WITHOUT_DIVIDE | high | dax_correctness | no | Division operator (/) without DIVIDE function risks divide-by-zero |
| DAX005 | CALCULATE_NESTING_WARNING | high | dax_performance | no | Multiple CALCULATE statements may indicate suboptimal pattern |
| DAX006 | MISSING_VAR_COMPLEX_MEASURE | high | dax_performance | no | Complex measure without VAR causes repeated expression evaluation |
| DAX007 | IF_INSTEAD_OF_SWITCH | medium | dax_maintainability | no | Multiple nested IF statements should use SWITCH for clarity |
| DAX008 | HARDCODED_FILTER_VALUES | low | dax_maintainability | no | Hardcoded string literals in filters reduce maintainability |
| DAX009 | COMMENTED_CODE | low | dax_maintainability | no | Commented-out code indicates technical debt |
| DAX010 | DISTINCTCOUNT_FUSION_BLOCKER | info | dax_performance | no | DISTINCTCOUNT prevents query fusion with other aggregations |
| DAX011 | USERELATIONSHIP_AGGREGATION_BLOCKER | medium | dax_performance | no | USERELATIONSHIP prevents aggregation table hits in composite models |
| DAX012 | SELECTEDVALUE_EXCESSIVE | medium | dax_maintainability | no | Excessive SELECTEDVALUE usage may indicate architectural issues |
| DAX013 | ISINSCOPE_MATRIX_LOGIC | info | dax_maintainability | no | ISINSCOPE for matrix-level logic is complex and hard to maintain |
| DAX014 | ALLEXCEPT_PATTERN | info | dax_correctness | no | ALLEXCEPT may not behave as expected with expanded tables |
| DAX015 | CONTEXT_TRANSITION_IN_ITERATOR | info | dax_performance | no | Measure reference inside iterator causes context transition per row |
| DAX016 | TIME_INTELLIGENCE_WITHOUT_DATE_TABLE | high | dax_correctness | no | Time intelligence functions require a proper date dimension table |
| DAX017 | DATESBETWEEN_PERFORMANCE | medium | dax_performance | no | DATESBETWEEN can be slow; consider DATESINPERIOD for relative ranges |
| DAX018 | CALENDAR_FUNCTION_IN_MEASURE | high | dax_correctness | no | CALENDAR/CALENDARAUTO should be in calculated table, not measure |
| DAX019 | BLANK_NOT_HANDLED | medium | dax_correctness | no | Measure may return unexpected results when source is blank |
| DAX020 | RELATED_IN_MEASURE | medium | dax_correctness | no | RELATED requires row context; may cause unexpected behavior in measures |
| DAX021 | LOOKUPVALUE_SLOW | medium | dax_performance | no | LOOKUPVALUE is slow for large tables; consider relationship-based approach |
| DAX022 | EXCESSIVE_MEASURE_LENGTH | medium | dax_maintainability | no | Very long measure is hard to maintain and debug |
| DAX023 | NO_MEASURE_DESCRIPTION | low | dax_maintainability | no | Measure lacks description; reduces discoverability and maintainability |
| DAX024 | MULTIPLE_CALCULATE_MODIFIERS | medium | dax_maintainability | no | Multiple modifiers in CALCULATE can be confusing and error-prone |
| DAX025 | RANK_TOPN_COMBINATION | medium | dax_performance | no | TOPN with RANKX can be inefficient; consider simpler patterns |
| DAX026 | GROUPBY_SUMX_CONDITIONAL | high | dax_performance | no | GROUPBY + SUMX inside IF/SWITCH creates heavy Formula Engine iteration |
| DAX027 | MEASURE_CHAIN_DEPTH | medium | dax_performance | no | Deep measure reference chain (>5 levels) causes repeated table scans |
| DAX028 | SUM_OF_RATIOS_PATTERN | medium | dax_correctness | no | Division inside SUMX/AVERAGEX produces incorrect weighted averages and poor performance |
| DAXC001 | ROW_ITERATION_OWNERSHIP_CARBON | high | dax_performance | yes | Row-by-row SUMX over assets with inline ownership+carbon calc; likely missing grain-first materialization |
| MDL001 | AUTO_DATE_TIME_ENABLED | critical | date_table | no | Auto date/time creates redundant date tables per date column |
| MDL002 | REFERENTIAL_INTEGRITY_VIOLATION | critical | relationship | no | Missing keys in relationship causes incorrect aggregations |
| MDL003 | HIGH_CARDINALITY_COLUMN | high | cardinality | no | Column with very high cardinality consumes excessive memory |
| MDL004 | INEFFICIENT_ENCODING | medium | encoding | no | Integer column using HASH encoding instead of VALUE |
| MDL005 | HIGH_DICTIONARY_RATIO | medium | cardinality | no | Dictionary dominates column storage (high cardinality indicator) |
| MDL006 | BIDIRECTIONAL_RELATIONSHIP | medium | relationship | no | Bi-directional relationships can cause ambiguity and performance issues |
| MDL007 | SNOWFLAKE_DIMENSION | medium | architecture | no | Snowflaked dimension tables increase join complexity and reduce performance |
| MDL008 | MISSING_DIMENSION_KEY | high | relationship | no | Dimension table lacks a proper unique key column |
| MDL009 | CALCULATED_COLUMN_HIGH_CARDINALITY | medium | cardinality | no | Calculated column has high cardinality; consider as measure |
| MDL010 | UNUSED_COLUMN | low | model_size | no | Column is not used in any measure, relationship, or visual |
| MDL011 | UNUSED_TABLE | medium | model_size | no | Table is not referenced by any relationship or measure |
| MDL012 | HIDDEN_USED_COLUMN | info | architecture | no | Hidden column is used in calculations but not visible to users |
| MDL013 | MULTIPLE_DATE_TABLES | medium | date_table | no | Multiple date tables can cause confusion and inefficiency |
| MDL014 | DATE_TABLE_MISSING_COLUMNS | low | date_table | no | Date table lacks standard fiscal or calendar columns |
| MDL015 | LARGE_TABLE_NO_AGGREGATION | high | model_size | no | Very large table without aggregation or summary tables |

Note: default analysis emits only confirmed rules; enable heuristic mode to include all.

<!-- RULE_SUMMARY_END -->



## DAX Anti-Pattern Rules (29)

### Critical Severity (Blocks Deployment)

#### DAX001: FILTER_TABLE_ITERATOR
**Detection:** `FILTER\s*\(\s*'?[A-Za-z][A-Za-z0-9_ ]*'?\s*,`
**Exclude:** `FILTER\s*\(\s*(ALL|VALUES|DISTINCT|CALCULATETABLE|ADDCOLUMNS)`

**Why Critical:**
FILTER(Table, condition) forces row-by-row iteration in the single-threaded Formula Engine. The Storage Engine cannot optimize this pattern, preventing parallelization and efficient bitmap filtering.

**Performance Impact:** 10-100x slower than CALCULATE filter arguments

**Fix Strategy:**
```dax
-- BEFORE
SUMX(FILTER('Sales', 'Sales'[Amount] > 100), 'Sales'[Amount])

-- AFTER
CALCULATE(SUM('Sales'[Amount]), 'Sales'[Amount] > 100)
```

---

#### DAX002: SUMX_FILTER_COMBO
**Detection:** `(SUMX|AVERAGEX|MAXX|MINX|COUNTX)\s*\(\s*FILTER\s*\(`

**Why Critical:**
Creates two levels of single-threaded iteration:
1. FILTER scans entire table row-by-row
2. SUMX iterates the filtered result row-by-row

With 1M rows, this can be 100-1000x slower than equivalent CALCULATE.

**Fix Strategy:**
```dax
-- BEFORE
SUMX(FILTER('Sales', 'Sales'[Status] = "Completed"), 'Sales'[Amount])

-- AFTER
VAR _FilteredSales = CALCULATETABLE('Sales', 'Sales'[Status] = "Completed")
RETURN SUMX(_FilteredSales, 'Sales'[Amount])
-- Or better:
CALCULATE(SUM('Sales'[Amount]), 'Sales'[Status] = "Completed")
```

---

#### DAX003: NESTED_ITERATORS
**Detection:** AST analysis - SUMX/AVERAGEX/etc containing another iterator

**Why Critical:**
Creates Cartesian product. Customer x Product with 1000 x 1000 = 1,000,000 iterations, all single-threaded in Formula Engine.

**Fix Strategy:**
```dax
-- BEFORE: 1M iterations for 1000 customers x 1000 products
SUMX(
    'Customer',
    SUMX(
        RELATEDTABLE('Sales'),
        'Sales'[Amount]
    )
)

-- AFTER: Use a single iterator with proper context
VAR _CustomerSales =
    ADDCOLUMNS(
        'Customer',
        "@TotalSales", CALCULATE(SUM('Sales'[Amount]))
    )
RETURN
SUMX(_CustomerSales, [@TotalSales])
-- Or simply:
SUM('Sales'[Amount])  -- if aggregation is the goal
```

---

#### DAX004: DEEP_CALCULATE_NESTING
**Detection:** Count of `CALCULATE\s*\(` >= 4 in single measure

**Why Critical:**
Each CALCULATE triggers a context transition. With N nested CALCULATEs, this creates O(2^N) context operations. 12 nested = potentially 4096x overhead.

**Fix Strategy:**
```dax
-- BEFORE: 4 nested CALCULATE calls
CALCULATE(
    CALCULATE(
        CALCULATE(
            CALCULATE([Base Measure],
                'Product'[Category] = "A"),
            'Date'[Year] = 2024),
        'Region'[Country] = "US"),
    'Customer'[Segment] = "Enterprise")

-- AFTER: Flatten with combined filter arguments
VAR _Filters =
    'Product'[Category] = "A" &&
    'Date'[Year] = 2024 &&
    'Region'[Country] = "US" &&
    'Customer'[Segment] = "Enterprise"
RETURN
CALCULATE([Base Measure], KEEPFILTERS(_Filters))

-- ALTERNATIVE: Use variables for intermediate results
VAR _BaseResult = [Base Measure]
VAR _FilteredResult =
    CALCULATE(_BaseResult,
        'Product'[Category] = "A",
        'Date'[Year] = 2024,
        'Region'[Country] = "US",
        'Customer'[Segment] = "Enterprise"
    )
RETURN _FilteredResult
```

---

#### DAX025: CIRCULAR_DEPENDENCY_RISK
**Detection:** Dependency graph analysis - cycle detection

**Why Critical:**
Causes infinite recursion or undefined behavior. Often hidden across multiple measures.

---

### High Severity (Warning, Review Required)

#### DAX005: CONTEXT_TRANSITION_IN_ITERATOR
**Detection:** Measure reference `\[[^\]]+\]` inside SUMX/FILTER body

**Why High:**
Each row triggers full context transition. With 1M rows = 1M context transitions, each saving/restoring filter context.

**Fix Strategy:**
```dax
-- BEFORE: Measure reference inside iterator (1M context transitions)
SUMX(
    'Sales',
    [Unit Price] * 'Sales'[Quantity]  -- [Unit Price] triggers context transition per row
)

-- AFTER: Replace measure with column reference or pre-calculate
VAR _UnitPrice = [Unit Price]  -- Evaluate once outside iterator
RETURN
SUMX(
    'Sales',
    _UnitPrice * 'Sales'[Quantity]
)

-- OR: Use column directly if available
SUMX(
    'Sales',
    'Product'[Unit Price] * 'Sales'[Quantity]
)
```

---

#### DAX006: IF_INSIDE_ITERATOR
**Detection:** `(SUMX|AVERAGEX|FILTER).*IF\s*\(` or `SWITCH\s*\(` inside iterator

**Why High:**
Creates CallbackDataID - complex expressions inside Storage Engine queries force Formula Engine callbacks, disabling caching and forcing single-threaded processing.

**Fix Strategy:**
```dax
-- BEFORE: IF inside iterator forces CallbackDataID
SUMX(
    'Sales',
    IF('Sales'[Status] = "Completed", 'Sales'[Amount], 0)
)

-- AFTER: Move condition to CALCULATE filter or use column flag
CALCULATE(
    SUM('Sales'[Amount]),
    'Sales'[Status] = "Completed"
)

-- OR: Pre-filter the table
VAR _CompletedSales = FILTER('Sales', 'Sales'[Status] = "Completed")
RETURN SUMX(_CompletedSales, 'Sales'[Amount])

-- ALTERNATIVE: Use a calculated column if condition is static
-- Add column: [Is Completed] = IF([Status] = "Completed", 1, 0)
-- Then: SUM('Sales'[Amount] * 'Sales'[Is Completed])
```

---

#### DAX007: DIVISION_WITHOUT_DIVIDE
**Detection:** `/` operator not inside DIVIDE function
**Exclude:** `//` comments, `/*` block comments

**Why High:**
Division by zero causes errors. DIVIDE handles gracefully with alternate result.

**Fix Strategy:**
```dax
-- BEFORE
[Numerator] / [Denominator]

-- AFTER
DIVIDE([Numerator], [Denominator], 0)  -- or BLANK()
```

---

#### DAX008: CALLBACK_DATA_ID_RISK
**Detection:** Inside large iterator (>10K rows estimated):
- IF/SWITCH conditions
- Division/modulo operations
- String operations (CONCATENATE, FORMAT)
- Date operations

**Why High:**
Forces Formula Engine callback for each row, preventing Storage Engine optimization.

**Fix Strategy:**
```dax
-- BEFORE: String operation inside iterator causes CallbackDataID
SUMX(
    'Sales',
    IF(
        FORMAT('Sales'[Date], "MMMM") = "December",
        'Sales'[Amount] * 1.1,
        'Sales'[Amount]
    )
)

-- AFTER: Move complex logic to calculated column or pre-filter
-- Option 1: Use date function in filter context
CALCULATE(
    SUM('Sales'[Amount]) * 1.1,
    MONTH('Sales'[Date]) = 12
) +
CALCULATE(
    SUM('Sales'[Amount]),
    MONTH('Sales'[Date]) <> 12
)

-- Option 2: Add calculated column [Is December]
-- Then simple iteration: SUMX('Sales', 'Sales'[Amount] * (1 + 'Sales'[Is December] * 0.1))
```

---

#### DAX009: MISSING_VAR_COMPLEX
**Detection:** Measure length > 300 characters AND no `VAR` keyword

**Why High:**
Repeated sub-expressions evaluated multiple times. Each measure reference re-executes the full calculation.

**Fix Strategy:**
```dax
-- BEFORE: Repeated expressions evaluated multiple times
DIVIDE(
    CALCULATE(SUM('Sales'[Amount]), 'Sales'[Status] = "Complete"),
    CALCULATE(SUM('Sales'[Amount]))
) +
DIVIDE(
    CALCULATE(SUM('Sales'[Amount]), 'Sales'[Status] = "Pending"),
    CALCULATE(SUM('Sales'[Amount]))
)

-- AFTER: VAR stores intermediate results, evaluated once
VAR _TotalSales = CALCULATE(SUM('Sales'[Amount]))
VAR _CompleteSales = CALCULATE(SUM('Sales'[Amount]), 'Sales'[Status] = "Complete")
VAR _PendingSales = CALCULATE(SUM('Sales'[Amount]), 'Sales'[Status] = "Pending")
RETURN
DIVIDE(_CompleteSales, _TotalSales) + DIVIDE(_PendingSales, _TotalSales)
```

---

#### DAX020: SUMMARIZE_WITH_AGGREGATION
**Detection:** `SUMMARIZE\s*\(.*,\s*"[^"]+"\s*,` (named column pattern)

**Why High:**
SUMMARIZE with aggregations deprecated. Use SUMMARIZECOLUMNS or ADDCOLUMNS + GROUPBY.

**Fix Strategy:**
```dax
-- BEFORE
SUMMARIZE('Sales', 'Product'[Category], "Total", SUM('Sales'[Amount]))

-- AFTER
SUMMARIZECOLUMNS('Product'[Category], "Total", SUM('Sales'[Amount]))
```

---

#### DAX021: CALCULATE_TABLE_FILTER
**Detection:** `CALCULATE\s*\(.*FILTER\s*\(\s*'?[A-Za-z]`

**Why High:**
Combining CALCULATE with FILTER(Table) often indicates misunderstanding of filter propagation.

---

#### DAX023: TIME_INTELLIGENCE_DQ
**Detection:** `(DATESYTD|DATESMTD|DATESQTD|TOTALYTD|SAMEPERIODLASTYEAR)` in DirectQuery model

**Why High:**
Built-in time intelligence functions force day-level data retrieval in DirectQuery, generating complex SQL.

---

#### DAX024: MEASURE_BRANCHING_EXPLOSION
**Detection:** 5+ branches (IF/SWITCH cases) in single measure

**Why High:**
All branches may be evaluated. Consider calculation groups or measure decomposition.

---

#### DAX026: GROUPBY_SUMX_CONDITIONAL
**Detection:** `GROUPBY\s*\(.*SUMX` inside IF/SWITCH branches

**Why High:**
GROUPBY + SUMX combination forces heavy Formula Engine iteration. When placed inside conditional branches (IF/SWITCH), this pattern multiplies overhead and prevents query plan optimization.

**Performance Impact:** 10-100x slower than grain-first approach

**Fix Strategy:**
```dax
-- BEFORE: GROUPBY + SUMX inside branch
IF(
    _IsBenchmark,
    SUMX(
        GROUPBY(
            'Assets',
            'Assets'[Sector],
            "@SectorTotal",
            SUMX(CURRENTGROUP(), [Value])
        ),
        [@SectorTotal]
    )
)

-- AFTER: Grain-first approach
VAR _PreAggregated =
    ADDCOLUMNS(
        SUMMARIZE('Assets', 'Assets'[Sector]),
        "@SectorTotal", CALCULATE(SUM('Assets'[Value]))
    )
RETURN
IF(_IsBenchmark, SUMX(_PreAggregated, [@SectorTotal]))
```

---

### Medium Severity

#### DAX010: FIRSTDATE_LASTDATE_SCALAR
**Detection:** `(FIRSTDATE|LASTDATE)\s*\(`

**Why Medium:**
Returns a table with one row, one column. Often misused expecting scalar. Use MIN/MAX for scalars.

---

#### DAX011: AND_MULTI_COLUMN_FILTER
**Detection:** `&&` or `AND\s*\(` spanning multiple columns in FILTER

**Why Medium:**
Multiple column filter in single predicate less efficient than separate filter arguments.

---

#### DAX014: EXCESSIVE_SELECTEDVALUE
**Detection:** Count of `SELECTEDVALUE\s*\(` >= 4

**Why Medium:**
Indicates measure tightly coupled to slicer state. Consider refactoring for reusability.

---

#### DAX018: USERELATIONSHIP_AGG_BLOCK
**Detection:** `USERELATIONSHIP\s*\(`

**Why Medium:**
Prevents use of aggregation tables. Document if intentional.

---

#### DAX022: TREATAS_VS_RELATIONSHIP
**Detection:** Heavy `TREATAS\s*\(` usage (>=3 per measure)

**Why Medium:**
Virtual relationships via TREATAS are powerful but bypass model relationships. Ensure intentional.

---

#### DAX027: MEASURE_CHAIN_DEPTH
**Detection:** Measure dependency chain > 5 levels (graph analysis)

**Why Medium:**
Deep measure chains create multiple execution plans with repeated table scans. Each level adds context transition overhead and prevents engine optimization.

**Performance Impact:** Exponential degradation with chain depth

**Detection Method:**
- Parse measure definitions to find measure references `[MeasureName]`
- Build dependency graph
- Flag chains where depth exceeds 5

**Fix Strategy:**
```dax
-- BEFORE: 6+ level measure chain
[Level6] -> [Level5] -> [Level4] -> [Level3] -> [Level2] -> [Level1]

-- AFTER: Collapsed orchestrator with VARs
Level6 Optimized =
VAR _L1 = (Level1 logic)
VAR _L2 = (Level2 logic using _L1)
VAR _L3 = (Level3 logic using _L2)
VAR _L4 = (Level4 logic using _L3)
VAR _L5 = (Level5 logic using _L4)
RETURN (Level6 logic using _L5)
```

---

#### DAX028: SUM_OF_RATIOS_PATTERN
**Detection:** Division operator `/` or `DIVIDE` inside `SUMX` iterator body

**Why Medium:**
Computing ratios per row then summing produces mathematically incorrect results for intensity metrics. Also forces CallbackDataID overhead for the division operation.

**Performance Impact:** 2-10x slower + incorrect results

**Detection Pattern:**
```regex
SUMX\s*\([^)]+,\s*[^)]*(/|DIVIDE)[^)]*\)
```

**Fix Strategy:**
```dax
-- BEFORE: Sum of ratios (WRONG)
Intensity Bad =
SUMX(
    'Assets',
    [Weight] * DIVIDE([Carbon], [Revenue])
)

-- AFTER: Ratio of sums (CORRECT)
Intensity Good =
VAR _WeightedCarbon = SUMX('Assets', [Weight] * [Carbon])
VAR _WeightedRevenue = SUMX('Assets', [Weight] * [Revenue])
RETURN DIVIDE(_WeightedCarbon, _WeightedRevenue)
```

**Mathematical Reasoning:**
- Sum-of-ratios: S(C_i/R_i) - mixes bases, incorrect weighted average
- Ratio-of-sums: S(W*C) / S(W*R) - proper weighted intensity

---

#### DAXC001: ROW_ITERATION_OWNERSHIP_CARBON
**Detection:** `SUMX('GS Asset', ...)` with inline ownership + carbon/revenue logic and scope switch
**Confirmed:** yes

**Why High:**
This pattern forces repeated per-row storage engine scans and context transitions. Ownership, carbon, and revenue are recomputed per asset instead of being materialized once at the grain, which can explode Formula Engine cost on large models.

**Performance Impact:** 10-50x slower than grain-first materialization on large asset universes

**Detection Heuristics:**
- `SUMX('GS Asset', ...)` present
- Scope logic inside iterator (e.g., `Scope_Type_Code` / `SELECTEDVALUE('Scope Emission Types'...)`)
- Ownership logic inline (`MV_OWNERSHIP`, `BENCHMARK_WEIGHT_EOD`, `Market_Cap_Base`, `EVIC_Base`)
- Carbon/Revenue sums inline (`Carbon_Scope_*`, `Absolute_GHG_*`, `Revenue_*`)
- No `NATURALINNERJOIN` or `ADDCOLUMNS(... "@Ownership"/"@Carbon")` materialization

**Fix Strategy:**
```dax
-- BEFORE: per-row ownership + carbon inside SUMX
SUMX(
    'GS Asset',
    VAR _Carbon = SWITCH(... CALCULATE(SUM(...)) ...)
    VAR _Ownership = IF(_IsBenchmark, ... , ...)
    RETURN _Ownership * _Carbon
)

-- AFTER: materialize once, join, then aggregate
VAR CarbonByAsset =
    ADDCOLUMNS(VALUES('GS Asset'[ISIN]), "@Carbon", ... )
VAR OwnershipByAsset =
    CALCULATETABLE(
        ADDCOLUMNS(VALUES('GS Asset'[ISIN]), "@Ownership", ...),
        KEEPFILTERS('Benchmark Portfolio Mapping'[Position_Type] = "Portfolio")
    )
VAR Joined = NATURALINNERJOIN(OwnershipByAsset, CarbonByAsset)
RETURN SUMX(Joined, [@Ownership] * [@Carbon])
```

**Correctness Notes:**
- Apply benchmark/portfolio filters consistently in numerator and denominator
- If totals exclude asset types, apply the same filters inside per-asset materialization

---

### Low Severity

#### DAX012: DEAD_CODE_UNUSED_VAR
**Detection:** VAR declaration not referenced in RETURN statement

**Why Low:**
Technical debt. Variable computed but never used.

---

#### DAX013: IF_INSTEAD_OF_SWITCH
**Detection:** 3+ nested IF statements

**Why Low:**
Maintainability issue. SWITCH(TRUE(), ...) more readable.

---

#### DAX015: HARDCODED_FILTER_VALUES
**Detection:** 3+ string literals in measure

**Why Low:**
Maintenance risk. Consider dimension table or parameter table.

---

#### DAX016: COMMENTED_CODE
**Detection:** `//` or `/*` with DAX keywords inside

**Why Low:**
Technical debt indicator. Remove or document.

---

### Info (Awareness Only)

#### DAX017: DISTINCTCOUNT_FUSION_BLOCK
**Detection:** `DISTINCTCOUNT\s*\(`

**Why Info:**
Prevents query fusion optimization. Not necessarily bad, but be aware.

---

#### DAX019: ALLEXCEPT_EXPANDED_TABLE
**Detection:** `ALLEXCEPT\s*\(` with table having relationships

**Why Info:**
ALLEXCEPT returns expanded table including related columns. Ensure intended behavior.

---

## Model Structure Rules (15)

### Critical Severity

#### MDL001: AUTO_DATE_TIME_ENABLED
**Detection:** LocalDateTable count > 0 in VPAX

**Why Critical:**
Creates hidden date table for every date column. Typical waste: 50-80% of model size.

**Impact Example:** Your ESG model: 53.3 MB wasted (74.6% of total)

**Fix Strategy:**
```
-- NOT A DAX FIX - This requires Power BI Desktop settings change:

1. Disable Auto Date/Time:
   File -> Options -> Data Load -> Time Intelligence -> Uncheck "Auto Date/Time"

2. Create a single shared Date table:
   Date = CALENDAR(DATE(2020, 1, 1), DATE(2030, 12, 31))

3. Add columns manually:
   Year = YEAR('Date'[Date])
   Month = FORMAT('Date'[Date], "MMMM")
   Quarter = "Q" & QUARTER('Date'[Date])

4. Mark as Date table:
   Table Tools -> Mark as Date Table -> Select date column

-- Result: One date table instead of 50+ hidden LocalDateTables
```

---

#### MDL002: REFERENTIAL_INTEGRITY_VIOLATION
**Detection:** Relationship MissingKeys > 0

**Why Critical:**
Foreign keys pointing to non-existent primary keys cause:
- Incorrect aggregations
- Silent data loss in visuals
- Unpredictable filter behavior

**Fix Strategy:**
```sql
-- OPTION 1: Add "Unknown" member to dimension (in source query)
SELECT *
FROM DimProduct
UNION ALL
SELECT -1 AS ProductKey, 'Unknown' AS ProductName, ...

-- OPTION 2: Filter out orphan rows in Power Query (M)
= Table.SelectRows(Sales, each [ProductKey] <> null
    and List.Contains(Product[ProductKey], [ProductKey]))

-- OPTION 3: Add calculated column to handle missing relationships
IF(ISBLANK(RELATED('Product'[Name])), "Unknown", RELATED('Product'[Name]))

-- BEST PRACTICE: Fix at source data level, not in DAX
-- DAX measures should assume data integrity
```

---

### High Severity

#### MDL003: HIGH_CARDINALITY_COLUMN
**Detection:** Column cardinality > 100,000 (Warning) or > 1,000,000 (Critical)

**Why High:**
High cardinality = large dictionary = memory explosion. DateTime with timestamps is common culprit.

**Fix:**
- Split date and time components
- Round timestamps to hour/minute
- Consider aggregation tables

---

#### MDL005: MANY_TO_MANY_NO_BRIDGE
**Detection:** Relationship cardinality = ManyToMany without intermediate table

**Why High:**
Native M:M without bridge table can cause incorrect aggregations and ambiguous paths.

---

#### MDL008: DATETIME_WITH_TIME
**Detection:** DateTime column with cardinality approaching row count

**Why High:**
Each unique timestamp creates dictionary entry. 1M rows with second precision = 1M dictionary entries.

**Fix:** Separate Date and Time columns

---

#### MDL013: RLS_PERFORMANCE_RISK
**Detection:** RLS expression with complex DAX (CALCULATE, LOOKUPVALUE, etc.)

**Why High:**
RLS evaluated on every query. Complex RLS = performance on every single report.

---

### Medium Severity

#### MDL004: BIDIRECTIONAL_RELATIONSHIP
**Detection:** CrossFilteringBehavior = Both

**Why Medium:**
Documented 7x performance degradation in some scenarios. Also causes ambiguous filter paths.

**When OK:** Intentional M:M bridge tables

---

#### MDL006: SNOWFLAKE_ANTI_PATTERN
**Detection:** Dimension -> Dimension relationship chains (3+ hops)

**Why Medium:**
Star schema preferred. Snowflake adds relationship traversal overhead.

---

#### MDL007: UNUSED_COLUMNS
**Detection:** Column with:
- No relationships referencing it
- No measures referencing it
- Not in any hierarchy

**Why Medium:**
Bloat. Each column consumes memory even if unused.

---

#### MDL009: HIGH_DICTIONARY_RATIO
**Detection:** Column dictionary size > 80% of total column size

**Why Medium:**
Indicates low compression efficiency. May benefit from encoding change or data transformation.

---

#### MDL010: INEFFICIENT_ENCODING
**Detection:** Integer column using HASH encoding instead of VALUE

**Why Medium:**
VALUE encoding more efficient for sorted integers. Check if data can be pre-sorted.

---

#### MDL012: CALCULATED_COLUMN_PERF
**Detection:** Calculated column with RELATED/RELATEDTABLE/LOOKUPVALUE

**Why Medium:**
Computed at refresh time, but complex DAX in calculated columns can slow refresh significantly.

---

### Low Severity

#### MDL011: MISSING_SORT_BY_COLUMN
**Detection:** Text column used in visuals without SortByColumn

**Why Low:**
Alphabetical sort may not match business expectation (Jan, Feb, Mar vs Apr, Aug, Dec).

---

#### MDL014: INACTIVE_RELATIONSHIP_UNUSED
**Detection:** Inactive relationship never activated via USERELATIONSHIP

**Why Low:**
Dead relationship. Either use it or remove it.

---

#### MDL015: TABLE_NOT_IN_STAR_SCHEMA
**Detection:** Fact table with relationship to another fact table

**Why Low:**
Not always wrong, but review if intentional. Usually indicates modeling issue.

---

## Calculation Group Rules (5)

### High Severity

#### CG001: CALC_ITEM_PERFORMANCE
**Detection:** Calculation item with complex DAX (nested iterators, multiple CALCULATE)

**Why High:**
Calculation item DAX applied to EVERY measure using it. Complex = multiplied slowness.

---

#### CG002: CALC_GROUP_INTERACTION
**Detection:** Multiple calculation groups with overlapping precedence

**Why High:**
Calculation group interactions can cause unexpected results. Review precedence carefully.

---

### Medium Severity

#### CG003: SELECTEDMEASURE_OVERHEAD
**Detection:** SELECTEDMEASURE() wrapping already-expensive measures

**Why Medium:**
If base measure is slow, calculation group makes it slower for every variant.

---

### Low Severity

#### CG004: CALC_ITEM_DEAD_CODE
**Detection:** Calculation item not used in any report

**Why Low:**
Unused code. Remove if no longer needed.

---

### Info

#### CG005: ISSELECTEDMEASURE_PATTERN
**Detection:** Heavy use of ISSELECTEDMEASURE() checks

**Why Info:**
Common pattern, but consider if formatting strings or separate measures would be cleaner.

---

## Detection Priority Matrix

| Priority | Rules | Rationale |
|----------|-------|-----------|
| **P0** (Block) | DAX001-004, MDL001-002 | Direct performance/correctness impact |
| **P1** (Warn) | DAX005-009, MDL003-005, CG001-002 | Significant but context-dependent |
| **P2** (Info) | DAX010-024, MDL006-015, CG003-005 | Best practice, not critical |

---

## Implementation Notes

### Regex Patterns

```python
PATTERNS = {
    "FILTER_TABLE": r"FILTER\s*\(\s*'?[A-Za-z][A-Za-z0-9_ ]*'?\s*,",
    "SUMX_FILTER": r"(SUMX|AVERAGEX|MAXX|MINX|COUNTX)\s*\(\s*FILTER\s*\(",
    "CALCULATE": r"\bCALCULATE\s*\(",
    "DIVISION": r"(?<![A-Za-z])/(?![/\*])",
    "VAR": r"\bVAR\b",
    "MEASURE_REF": r"\[[^\]]+\]",
    "IF_STATEMENT": r"\bIF\s*\(",
    "SWITCH_STATEMENT": r"\bSWITCH\s*\(",
    "ITERATOR": r"(SUMX|AVERAGEX|MAXX|MINX|COUNTX|FILTER)\s*\(",
}
```

### Cardinality Thresholds

```python
THRESHOLDS = {
    "cardinality_warning": 100_000,
    "cardinality_critical": 1_000_000,
    "dictionary_ratio_warning": 0.80,
    "measure_complexity_chars": 300,
    "calculate_nesting_warning": 2,
    "calculate_nesting_critical": 4,
    "if_nesting_warning": 3,
    "selectedvalue_warning": 4,
    "hardcoded_strings_warning": 3,
}
```

---

*Total Rules: 48 (28 DAX + 15 Model + 5 Calculation Groups)*

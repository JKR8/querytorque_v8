# STATE ANALYSIS REPORT - Quick Start Guide

**Generated**: 2026-02-06
**File**: `/research/STATE_ANALYSIS_REPORT.md`
**Lines**: 1,456

## What Is This Report?

A comprehensive analysis of all 99 TPC-DS queries that identifies **which queries to optimize next** and **which transforms to try**, prioritizing by absolute time savings potential (runtime), not just speedup percentage.

## Key Innovation: Runtime-Based Prioritization

**Principle**: A 1.2x speedup on a 10,000ms query saves more absolute time (833ms) than a 3x speedup on a 100ms query (67ms).

**Priority Score Formula**:
```
Priority = Runtime_Percentile(50pts) + Gap_To_Expectation(20pts)
         + Win_Potential(20pts) + Untried_Patterns(5pts) + Category_Bonus(15pts)
```

**Runtime Percentiles**:
- Top 20% longest-running queries: **50 points** (TIER 1 - HIGH VALUE)
- Top 21-50% by runtime: **25 points** (TIER 2 - MEDIUM VALUE)
- Bottom 50% by runtime: **0 points** (TIER 3 - LOW VALUE)

## How to Use This Report

### 1. Read the Executive Dashboard (Page 1)

See:
- Progress summary (how many WIN/NEUTRAL/REGRESSION)
- Top 20 longest-running queries (highest business value)
- Transform effectiveness (which patterns have highest success rates)

### 2. Focus on Tier 1 (Page 2+)

**Priority Score > 70** = Longest-running + high optimization potential

For each query, you'll see:
- **Runtime**: Baseline execution time in milliseconds
- **Time Savings Potential**: Absolute time saved at 2x, 3x speedup
  - Example: Q23 (24,404ms) can save 12,202ms at 2x or 16,269ms at 3x
- **Current Best**: Current best speedup achieved
- **Gap to Expectation**: How far from expected speedup
- **State History**: All attempts so far (baseline → kimi → v2 → W1-W4)
- **Top Recommendations**: 2-5 next moves ranked by confidence

### 3. Check Recommendations

Each recommendation shows:
```
[Transform Name] [CONFIDENCE: XX%] [RISK: LOW/MEDIUM/HIGH]
- Expected speedup: X.XXx
- Success rate: YY% (based on historical data)
- Rationale: Why it should work for THIS query
```

**Confidence Interpretation**:
- **90-100%**: Very high confidence - proceed with high priority
- **75-89%**: High confidence - likely to work
- **60-74%**: Good confidence - moderate risk
- **40-59%**: Moderate confidence - experimental
- **<40%**: Low confidence - use as last resort

### 4. Move to Tier 2 After Exhausting Tier 1

Tier 2 (Priority 40-70) contains medium-value targets:
- Shorter runtimes than Tier 1
- But still longer than typical queries
- Good for incremental improvements

### 5. Skip Tier 3

Tier 3 (Priority < 40) are not recommended for immediate focus:
- Short baseline runtimes (<500ms)
- Limited absolute time savings even with 3x speedup
- Already at or near expected speedup targets

## Example: Q23 (Tier 1, Priority 90.0)

```
### Q23: Q23
**Runtime**: 24,404ms baseline (TOP_20%)
**Time Savings Potential**: 12,202ms at 2x, 16,269ms at 3x
**Current Best**: 1.06x (baseline)
**Gap to Expectation**: 1.44x

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has proven high success rate

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0% (not yet tried on this query)
```

**Action**: Try `prefetch_fact_join` first (93% confidence, 100% success rate, LOW risk)

## Validation

After applying a recommended transform, validate using:

**3-run method** (recommended):
1. Run query 3 times
2. Discard 1st run (warmup)
3. Calculate average of runs 2-3
4. Compare: new_avg / original_avg = speedup

Or **5-run trimmed mean**:
1. Run query 5 times
2. Remove min and max (outliers)
3. Average remaining 3
4. Calculate speedup

## Top Targets by Runtime (Start Here)

| Rank | Query | Runtime | Current | Gap | Savings @ 2x | Savings @ 3x |
|------|-------|---------|---------|-----|--------------|--------------|
| 1 | Q23 | 24,404ms | 1.06x | 1.44x | 12,202ms | 16,269ms |
| 2 | Q4 | 10,209ms | 1.03x | 1.97x | 5,105ms | 6,806ms |
| 3 | Q14 | 9,211ms | 0.95x | 1.92x | 4,606ms | 6,141ms |
| 4 | Q78 | 9,002ms | 1.01x | 1.98x | 4,501ms | 6,001ms |
| 5 | Q51 | 7,935ms | 1.00x | 1.80x | 3,968ms | 5,290ms |

## What's in the Full Report?

1. **Executive Dashboard**
   - Progress summary
   - Top 20 longest-running queries
   - Transform effectiveness matrix

2. **TIER 1: HIGH-VALUE TARGETS** (Priority > 70)
   - 20-30 longest-running, highest-opportunity queries
   - Each with complete analysis and recommendations

3. **TIER 2: INCREMENTAL OPPORTUNITIES** (Priority 40-70)
   - 30-40 medium-value targets
   - Good for follow-up work after Tier 1

4. **TIER 3: MATURE WINS** (Priority < 40)
   - 50 queries with low priority
   - Mostly short-running or already optimized

5. **APPENDIX: Methodology**
   - Priority scoring formula
   - Confidence score interpretation
   - Risk assessment
   - How to use this report

## Key Insights

- **Runtime is King**: Focus on longest-running queries for maximum business impact
- **Top 4 Patterns** by success rate:
  - prefetch_fact_join: 100% success (3.77x avg)
  - union_cte_split: 100% success (1.36x avg)
  - early_filter: Higher potential if successful
  - date_cte_isolate: High speedup but lower success rate
- **Avoid**: Transforms with <50% success rate unless desperate

## Next Steps

1. Pick first query from Tier 1 (e.g., Q23, Q4, or Q14)
2. Try highest-confidence recommendation (usually prefetch_fact_join or date_cte_isolate)
3. Validate using 3-run or 5-run trimmed mean
4. Record results
5. Move to next query or try next recommendation on same query
6. Track progress in learning system

## Related Files

- **Master Leaderboard**: `/research/CONSOLIDATED_BENCHMARKS/DuckDB_TPC-DS_Master_v2_20260206.csv`
- **Gold Patterns**: `packages/qt-sql/qt_sql/optimization/examples/`
- **Failure Analysis**: `research/state_histories/failure_analysis.yaml`
- **Validation Scripts**: `research/validate_duckdb_tpcds.py`

---

**Remember**: Always validate improvements using proper statistical methodology. Single-run timing comparisons are unreliable.

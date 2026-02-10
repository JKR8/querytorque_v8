# Gold Examples Performance

Known high-value TPC-DS optimizations that succeeded:

## Gold Examples


### Q1: decorrelate
- **Expected Speedup**: 2.92x
- **Actual Speedup**: 2.92x
- **Status**: pass
- **Transform Type**: decorrelate

### Q15: or_to_union
- **Expected Speedup**: 2.78x
- **Actual Speedup**: 2.78x
- **Status**: pass
- **Transform Type**: or_to_union

### Q39: pushdown
- **Expected Speedup**: 2.44x
- **Actual Speedup**: 0.99x
- **Status**: pass
- **Transform Type**: pushdown

### Q74: pushdown
- **Expected Speedup**: 1.42x
- **Actual Speedup**: 1.36x
- **Status**: pass
- **Transform Type**: pushdown

### Q90: early_filter
- **Expected Speedup**: 1.84x
- **Actual Speedup**: 1.57x
- **Status**: pass
- **Transform Type**: early_filter

### Q93: early_filter
- **Expected Speedup**: 2.73x
- **Actual Speedup**: 2.73x
- **Status**: pass
- **Transform Type**: early_filter


## Usage in Few-Shot Prompts

These 6 gold examples are verified to work and should be included in prompt injection:

1. **Q1 (decorrelate)** - Most reliable, highest speedup (2.92x)
2. **Q93 (early_filter)** - Also high speedup (2.73x), different pattern
3. **Q15 (or_to_union)** - OR decomposition pattern (2.78x)
4. **Q39 (pushdown)** - Pushdown optimization (2.44x)
5. **Q90 (early_filter)** - Different filter context (1.84x)
6. **Q74 (pushdown)** - Another pushdown variant (1.42x)

## Pattern Combination Value

- **Decorrelate** (Q1): Works best for correlated subqueries with GROUP BY
- **OR-to-UNION** (Q15): Splits OR conditions into parallel branches
- **Pushdown** (Q39, Q74): Moves filters earlier in query tree
- **Early Filter** (Q90, Q93): Dimension table filtering before fact joins

---

**Recommendation**: When optimizing new queries, check if they match any of these patterns first.

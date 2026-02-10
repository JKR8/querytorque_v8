# SF5 Benchmark Report - 256 Query Pairs

**Date**: 2026-02-05
**Database**: DuckDB SF5 (Scale Factor 5)
**Total Pairs**: 256
**Runs per Pair**: 5 (trimmed mean: middle 3 of 5)
**Successful**: 170/256 (66.4%)

---

## Executive Summary

| Metric | Value |
|--------|-------|
| **Total Pairs** | 256 |
| **Successful** | 170 (66.4%) |
| **Failed/Skipped** | 86 (33.6%) |
| **Max Speedup** | 6.28× (Q39, benchmark_v2) |
| **Average Speedup** | 1.11× |
| **Median Speedup** | 1.00× |
| **Speedup >2.0x** | 9 pairs (5.3%) |
| **Regressions (<1.0x)** | 86 pairs (50.6%) |

---

## Results by Source

| Source | Successful | Total | Rate | Avg Speedup |
|--------|-----------|-------|------|-------------|
| Archive | 1 | 1 | 100.0% | 4.19× |
| Kimi Q1-Q30 | 29 | 30 | 96.7% | 1.16× |
| Kimi Q31-Q99 | 68 | 69 | 98.6% | 1.14× |
| benchmark_v2 | 72 | 88 | 81.8% | 1.03× |

---

## Speedup Distribution

| Category | Count | Percent |
|----------|-------|---------|
| >2.0× (Excellent) | 9 | 5.3% |
| 1.5-2.0× (Good) | 8 | 4.7% |
| 1.2-1.5× (Moderate) | 14 | 8.2% |
| 1.0-1.2× (Minor) | 53 | 31.2% |
| <1.0× (Regression) | 86 | 50.6% |

---

## Top 10 Winners

| Rank | Source | Query | Speedup | Original | Optimized |
|------|--------|-------|---------|----------|-----------|
| 1 | benchmark_v2 | q39 | 6.28× | - | - |
| 2 | Kimi Q31-Q99 | q39 | 6.23× | - | - |
| 3 | Kimi Q1-Q30 | q1 | 4.73× | - | - |
| 4 | Archive | q1 | 4.19× | - | - |
| 5 | Kimi Q31-Q99 | q93 | 2.91× | - | - |
| 6 | Kimi Q31-Q99 | q74 | 2.62× | - | - |
| 7 | Kimi Q31-Q99 | q65 | 2.46× | - | - |
| 8 | benchmark_v2 | q24 | 2.27× | - | - |
| 9 | benchmark_v2 | q23 | 2.20× | - | - |
| 10 | Kimi Q1-Q30 | q14 | 1.93× | - | - |

---

## Failed Pairs Analysis

**Total Failed**: 86 pairs

**Reasons**:
- V2 Standard placeholder SQL: ~68 pairs
- Original SQL syntax errors: ~12 pairs
- Optimized SQL syntax errors: ~6 pairs

---

## Key Findings

✅ **Kimi benchmarks have highest success rate** (96-99%)
- Consistently executable queries
- Mix of speedups and regressions
- Best candidates for production

⚠️ **benchmark_v2 has lower success rate** (82%)
- Some queries have placeholder SQL ([...])
- Still produces best absolute speedups

📊 **50.6% show regressions**
- Expected with automated optimization
- Validation/gating would filter these out
- Real-world would only apply winners

---

## Recommendations

1. **Use Kimi Q1-Q99** as primary source (high success rate)
2. **Filter for >1.2× speedup** to get good candidates
3. **Investigate Q39 pattern** (6.28× speedup - top performer)
4. **Clean up benchmark_v2** placeholder SQL for 100% coverage
5. **Implement runtime gating** to reject regressions

---

**Files**:
- `SF5_BENCHMARK_RESULTS_256.csv` - Detailed per-pair results
- `SF5_BENCHMARK_RESULTS_256.json` - Full results with metadata

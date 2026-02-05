# Retry Candidates: Speedup < 0.95
**Date**: 2026-02-05
**Threshold**: speedup < 0.95 (severe regression)
**Total Candidates**: 25 queries

---

## Already Retried (6 queries)
These were included in the first retry batch. See RETRY_RESULTS_DETAILED.json:

| Query | Speedup | Source | Status | Result |
|-------|---------|--------|--------|--------|
| q32 | 0.7977 | Kimi Q31-Q99 | REGRESS | ❌ ERROR (Ambiguous cs_item_sk) |
| q40 | 0.9240 | Kimi Q31-Q99 | REGRESS | ❌ ERROR (Missing cs_sold_date_sk) |
| q57 | 0.2255 | Kimi Q31-Q99 | REGRESS | ❌ ERROR (Missing d_year) |
| q70 | 0.7965 | Kimi Q31-Q99 | REGRESS | ❌ ERROR (Window in HAVING) |
| q72 | 0.9703 | Kimi Q31-Q99 | REGRESS | ❌ ERROR (Missing sold_dates table) |
| q89 | 0.6909 | Kimi Q31-Q99 | REGRESS | ❌ ERROR (Missing d_moy) |

---

## Pending Retry (19 queries)
**Status**: MARKED FOR RETRY

### Q1-Q30 Range (5 queries)

| Query | Speedup | Source | Pattern | Attempts | Reason |
|-------|---------|--------|---------|----------|--------|
| **q4** | 0.8467 | Kimi Q1-Q30 | Kimi_LLM | 1 | FOR RETRY |
| **q5** | 0.8999 | Kimi Q1-Q30 | Kimi_LLM | 1 | FOR RETRY |
| **q9** | 0.4720 | Kimi Q1-Q30 | Kimi_LLM | 1 | FOR RETRY |
| **q12** | 0.9391 | Kimi Q1-Q30 | Kimi_LLM | 1 | FOR RETRY |
| **q16** | 0.0564 | Kimi Q1-Q30 | Kimi_LLM | 2 | FOR RETRY |

### Q31-Q99 Range (11 queries)

| Query | Speedup | Source | Pattern | Attempts | Reason |
|-------|---------|--------|---------|----------|--------|
| **q34** | 0.3175 | Kimi Q31-Q99 | Kimi_LLM | 2 | FOR RETRY |
| **q37** | 0.9676 | Kimi Q31-Q99 | Kimi_LLM | 2 | FOR RETRY |
| **q38** | 0.9639 | Kimi Q31-Q99 | Kimi_LLM | 2 | FOR RETRY |
| **q42** | 0.9072 | Kimi Q31-Q99 | Kimi_LLM | 2 | FOR RETRY |
| **q43** | 0.9538 | V2_Auto | benchmark_v2 | 2 | FOR RETRY |
| **q48** | 0.9599 | Kimi Q31-Q99 | Kimi_LLM | 2 | FOR RETRY |
| **q53** | 0.9823 | V2_Auto | benchmark_v2 | 2 | FOR RETRY |
| **q58** | 0.9137 | Kimi Q31-Q99 | Kimi_LLM | 2 | FOR RETRY |
| **q63** | 0.9400 | Kimi Q31-Q99 | Kimi_LLM | 2 | FOR RETRY |
| **q82** | 0.9650 | Kimi Q31-Q99 | Kimi_LLM | 2 | FOR RETRY |
| **q91** | 0.9231 | V2_Auto | benchmark_v2 | 2 | FOR RETRY |

### Benchmark_v2 Only (3 queries)

| Query | Speedup | Source | Pattern | Attempts | Reason |
|-------|---------|--------|---------|----------|--------|
| **q26** | 0.6024 | V2_Auto | benchmark_v2 | 2 | FOR RETRY |
| **q29** | 0.9386 | Kimi Q1-Q30 | Kimi_LLM | 2 | FOR RETRY |
| **q94** | 0.2475 | V2_Auto | benchmark_v2 | 2 | FOR RETRY |

---

## Summary by Source

### Kimi Q1-Q30: 5 candidates
q4 (0.85×), q5 (0.90×), q9 (0.47×), q12 (0.94×), q16 (0.06×)

### Kimi Q31-Q99: 11 candidates
q34 (0.32×), q37 (0.97×), q38 (0.96×), q42 (0.91×), q48 (0.96×), q58 (0.91×), q63 (0.94×), q82 (0.97×)

### Benchmark_v2: 5 candidates
q26 (0.60×), q29 (0.94×), q43 (0.95×), q53 (0.98×), q91 (0.92×), q94 (0.25×)

---

## Severity Breakdown

### Critical (< 0.5×): 3 queries
- q16: 0.0564× (99.4% regression)
- q34: 0.3175× (68.3% regression)
- q94: 0.2475× (75.2% regression)

### Severe (0.5-0.8×): 3 queries
- q4: 0.8467× (15.3% regression)
- q5: 0.8999× (10.0% regression)
- q26: 0.6024× (39.8% regression)

### High (0.8-0.95×): 13 queries
- q9, q12, q29, q37, q38, q42, q43, q48, q53, q58, q63, q82, q91

---

## Next Step
When ready, run RETRY_LOW_SPEEDUP_DETAILED.json batch with detailed error capture for all 19 pending queries.


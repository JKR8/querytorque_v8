# Query Optimization Recommendations Report

**Generated**: 2026-02-04
**Dataset**: TPC-DS SF100 (99 queries)
**Model**: Hybrid ML (Pattern Weights + FAISS Similarity)

## Executive Summary

- **Total queries analyzed**: 99
- **Queries with gold patterns**: 73 (73.7%)
- **Queries with recommendations**: 73
- **Queries with actual wins**: 12 (12.1%)
- **Top-1 hit rate**: 50.0% (ML's #1 recommendation matches actual best)
- **Top-3 hit rate**: 58.3% (actual best in ML's top 3)

## Methodology

For each query, recommendations are generated using:

1. **Pattern Detection**: AST analysis identifies gold patterns (GLD-001 to GLD-007)
2. **Pattern Weights**: Historical pattern→transform mappings with confidence scores
3. **Similarity Search**: FAISS finds structurally similar queries with speedups
4. **Combined Ranking**: Weighted combination of pattern confidence (70%) and similarity evidence (30%)

**Ranking Formula**:
```
combined_confidence = 0.7 × pattern_confidence + 0.3 × (similar_count / 5)
estimated_speedup = 0.7 × pattern_avg_speedup + 0.3 × similar_avg_speedup
final_score = combined_confidence × estimated_speedup
```

---

## Per-Query Recommendations

### Queries with Recommendations (73)

#### Q1

**✓ Actual Result**: 2.92x speedup with `decorrelate`

**Gold Patterns Detected**: GLD-001, GLD-005, GLD-004, GLD-003

**Top 3 Recommendations**:

1. **decorrelate** ✓ **MATCH**
   - Combined confidence: 76%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-001
       - Historical: 2.92x avg, 2.92x max (1 cases)
     - Similarity-based: 1 similar query
       - Average speedup: 2.92x
       - Examples:
         - q1: 2.92x speedup (similarity: 100%)

2. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q1**: 2.92x speedup with `decorrelate`
   - Similarity: 100%
   - Distance: 0.0001
2. **q95**: 1.37x speedup with ``
   - Similarity: 100%
   - Distance: 0.0028
3. **q6**: 1.33x speedup with ``
   - Similarity: 100%
   - Distance: 0.0043

---

#### Q10

**Actual Result**: 1.02x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q74**: 1.36x speedup with `union_cte_split`
   - Similarity: 100%
   - Distance: 0.0050

---

#### Q11

**Actual Result**: 0.98x (no significant speedup)

**Gold Patterns Detected**: GLD-004, GLD-006

**Top 3 Recommendations**:

1. **union_cte_split**
   - Combined confidence: 76%
   - Estimated speedup: 1.36x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-006
       - Historical: 1.36x avg, 1.36x max (1 cases)
     - Similarity-based: 1 similar query
       - Average speedup: 1.36x
       - Examples:
         - q74: 1.36x speedup (similarity: 100%)

2. **decorrelate**
   - Combined confidence: 35%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 2.92x avg, 2.92x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q74**: 1.36x speedup with `union_cte_split`
   - Similarity: 100%
   - Distance: 0.0030

---

#### Q13

**Actual Result**: 1.01x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q14

**Actual Result**: 0.95x (no significant speedup)

**Gold Patterns Detected**: GLD-003, GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q15

**✓ Actual Result**: 2.78x speedup with `or_to_union`

**Gold Patterns Detected**: GLD-003, GLD-002

**Top 3 Recommendations**:

1. **or_to_union** ✓ **MATCH**
   - Combined confidence: 76%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-002
       - Historical: 2.78x avg, 2.78x max (1 cases)
     - Similarity-based: 1 similar query
       - Average speedup: 2.78x
       - Examples:
         - q15: 2.78x speedup (similarity: 100%)

2. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

3. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q15**: 2.78x speedup with `or_to_union`
   - Similarity: 100%
   - Distance: 0.0001
2. **q84**: 1.22x speedup with ``
   - Similarity: 100%
   - Distance: 0.0053

---

#### Q17

**Actual Result**: 1.19x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q18

**Actual Result**: 1.14x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q74**: 1.36x speedup with `union_cte_split`
   - Similarity: 100%
   - Distance: 0.0047

---

#### Q19

**Actual Result**: 1.04x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q2

**Actual Result**: 1.00x (no significant speedup)

**Gold Patterns Detected**: GLD-004

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 35%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 2.92x avg, 2.92x max (1 cases)

2. **union_cte_split**
   - Combined confidence: 35%
   - Estimated speedup: 1.36x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 1.36x avg, 1.36x max (1 cases)


---

#### Q23

**Actual Result**: 1.06x (no significant speedup)

**Gold Patterns Detected**: GLD-003, GLD-004

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 35%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 2.92x avg, 2.92x max (1 cases)

2. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q83**: 1.24x speedup with ``
   - Similarity: 100%
   - Distance: 0.0032
2. **q95**: 1.37x speedup with ``
   - Similarity: 100%
   - Distance: 0.0037

---

#### Q24

**Actual Result**: 0.87x (no significant speedup)

**Gold Patterns Detected**: GLD-004, GLD-003

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 35%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 2.92x avg, 2.92x max (1 cases)

2. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q25

**Actual Result**: 0.98x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q26

**Actual Result**: 1.00x (no significant speedup)

**Gold Patterns Detected**: GLD-003, GLD-002

**Top 3 Recommendations**:

1. **or_to_union**
   - Combined confidence: 70%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-002
       - Historical: 2.78x avg, 2.78x max (1 cases)

2. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

3. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)


---

#### Q27

**Actual Result**: 1.01x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter** ✓ **MATCH**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q28

**✓ Actual Result**: 1.33x speedup with ``

**Gold Patterns Detected**: GLD-002, GLD-002, GLD-002, GLD-002, GLD-002, GLD-002

**Top 3 Recommendations**:

1. **or_to_union**
   - Combined confidence: 70%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-002
       - Historical: 2.78x avg, 2.78x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q28**: 1.33x speedup with ``
   - Similarity: 100%
   - Distance: 0.0000

---

#### Q29

**Actual Result**: 0.95x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q3

**Actual Result**: 0.98x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q30

**Actual Result**: 1.00x (no significant speedup)

**Gold Patterns Detected**: GLD-001, GLD-005, GLD-004, GLD-003

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 70%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-001
       - Historical: 2.92x avg, 2.92x max (1 cases)

2. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)


---

#### Q31

**Actual Result**: 1.04x (no significant speedup)

**Gold Patterns Detected**: GLD-004

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 35%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 2.92x avg, 2.92x max (1 cases)

2. **union_cte_split**
   - Combined confidence: 41%
   - Estimated speedup: 1.36x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 1.36x avg, 1.36x max (1 cases)
     - Similarity-based: 1 similar query
       - Average speedup: 1.36x
       - Examples:
         - q74: 1.36x speedup (similarity: 100%)

**Gold Example Matches** (structurally similar winning queries):

1. **q74**: 1.36x speedup with `union_cte_split`
   - Similarity: 100%
   - Distance: 0.0014

---

#### Q32

**Actual Result**: 0.27x (no significant speedup)

**Gold Patterns Detected**: GLD-003, GLD-001, GLD-005

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 70%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-001
       - Historical: 2.92x avg, 2.92x max (1 cases)

2. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

3. **or_to_union**
   - Combined confidence: 24%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)
     - Similarity-based: 1 similar query
       - Average speedup: 2.78x
       - Examples:
         - q15: 2.78x speedup (similarity: 100%)

**Gold Example Matches** (structurally similar winning queries):

1. **q15**: 2.78x speedup with `or_to_union`
   - Similarity: 100%
   - Distance: 0.0059

---

#### Q33

**Actual Result**: 1.05x (no significant speedup)

**Gold Patterns Detected**: GLD-003, GLD-003, GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q95**: 1.37x speedup with ``
   - Similarity: 100%
   - Distance: 0.0046

---

#### Q34

**Actual Result**: 0.29x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q35

**Actual Result**: 1.00x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q36

**Actual Result**: 0.96x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q62**: 1.23x speedup with ``
   - Similarity: 100%
   - Distance: 0.0056

---

#### Q39

**Actual Result**: 0.99x (no significant speedup)

**Gold Patterns Detected**: GLD-004, GLD-003

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 35%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 2.92x avg, 2.92x max (1 cases)

2. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q4

**Actual Result**: 1.03x (no significant speedup)

**Gold Patterns Detected**: GLD-004, GLD-006

**Top 3 Recommendations**:

1. **union_cte_split**
   - Combined confidence: 76%
   - Estimated speedup: 1.36x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-006
       - Historical: 1.36x avg, 1.36x max (1 cases)
     - Similarity-based: 1 similar query
       - Average speedup: 1.36x
       - Examples:
         - q74: 1.36x speedup (similarity: 100%)

2. **decorrelate**
   - Combined confidence: 35%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 2.92x avg, 2.92x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q74**: 1.36x speedup with `union_cte_split`
   - Similarity: 100%
   - Distance: 0.0025

---

#### Q41

**Actual Result**: 1.14x (no significant speedup)

**Gold Patterns Detected**: GLD-001, GLD-005

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 70%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-001
       - Historical: 2.92x avg, 2.92x max (1 cases)


---

#### Q42

**Actual Result**: 0.94x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q43

**Actual Result**: 0.98x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q62**: 1.23x speedup with ``
   - Similarity: 100%
   - Distance: 0.0057

---

#### Q45

**Actual Result**: 1.08x (no significant speedup)

**Gold Patterns Detected**: GLD-003, GLD-002

**Top 3 Recommendations**:

1. **or_to_union**
   - Combined confidence: 76%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-002
       - Historical: 2.78x avg, 2.78x max (1 cases)
     - Similarity-based: 1 similar query
       - Average speedup: 2.78x
       - Examples:
         - q15: 2.78x speedup (similarity: 100%)

2. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

3. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q15**: 2.78x speedup with `or_to_union`
   - Similarity: 100%
   - Distance: 0.0023

---

#### Q46

**Actual Result**: 1.02x (no significant speedup)

**Gold Patterns Detected**: GLD-003, GLD-002

**Top 3 Recommendations**:

1. **or_to_union**
   - Combined confidence: 70%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-002
       - Historical: 2.78x avg, 2.78x max (1 cases)

2. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

3. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)


---

#### Q47

**Actual Result**: 1.00x (no significant speedup)

**Gold Patterns Detected**: GLD-004, GLD-003

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 35%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 2.92x avg, 2.92x max (1 cases)

2. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q48

**Actual Result**: 1.00x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q49

**Actual Result**: 1.02x (no significant speedup)

**Gold Patterns Detected**: GLD-002

**Top 3 Recommendations**:

1. **or_to_union**
   - Combined confidence: 70%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-002
       - Historical: 2.78x avg, 2.78x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q62**: 1.23x speedup with ``
   - Similarity: 100%
   - Distance: 0.0073

---

#### Q5

**Actual Result**: 1.09x (no significant speedup)

**Gold Patterns Detected**: GLD-004

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 35%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 2.92x avg, 2.92x max (1 cases)

2. **union_cte_split**
   - Combined confidence: 35%
   - Estimated speedup: 1.36x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 1.36x avg, 1.36x max (1 cases)


---

#### Q50

**Actual Result**: 0.91x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q62**: 1.23x speedup with ``
   - Similarity: 100%
   - Distance: 0.0045

---

#### Q51

**Actual Result**: 1.00x (no significant speedup)

**Gold Patterns Detected**: GLD-004

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 35%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 2.92x avg, 2.92x max (1 cases)

2. **union_cte_split**
   - Combined confidence: 35%
   - Estimated speedup: 1.36x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 1.36x avg, 1.36x max (1 cases)


---

#### Q52

**Actual Result**: 1.08x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q54

**Actual Result**: 1.03x (no significant speedup)

**Gold Patterns Detected**: GLD-003, GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q55

**Actual Result**: 0.94x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q56

**Actual Result**: 0.92x (no significant speedup)

**Gold Patterns Detected**: GLD-003, GLD-003, GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q95**: 1.37x speedup with ``
   - Similarity: 100%
   - Distance: 0.0040

---

#### Q57

**Actual Result**: 1.02x (no significant speedup)

**Gold Patterns Detected**: GLD-004, GLD-003

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 35%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 2.92x avg, 2.92x max (1 cases)

2. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q58

**Actual Result**: 1.06x (no significant speedup)

**Gold Patterns Detected**: GLD-003, GLD-005, GLD-003, GLD-005, GLD-003, GLD-005

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 70%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-005
       - Historical: 2.92x avg, 2.92x max (1 cases)

2. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q83**: 1.24x speedup with ``
   - Similarity: 100%
   - Distance: 0.0019

---

#### Q59

**Actual Result**: 1.00x (no significant speedup)

**Gold Patterns Detected**: GLD-004

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 35%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 2.92x avg, 2.92x max (1 cases)

2. **union_cte_split**
   - Combined confidence: 35%
   - Estimated speedup: 1.36x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 1.36x avg, 1.36x max (1 cases)


---

#### Q6

**✓ Actual Result**: 1.33x speedup with ``

**Gold Patterns Detected**: GLD-003, GLD-001, GLD-005

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 76%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-001
       - Historical: 2.92x avg, 2.92x max (1 cases)
     - Similarity-based: 1 similar query
       - Average speedup: 2.92x
       - Examples:
         - q1: 2.92x speedup (similarity: 100%)

2. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q6**: 1.33x speedup with ``
   - Similarity: 100%
   - Distance: 0.0001
2. **q95**: 1.37x speedup with ``
   - Similarity: 100%
   - Distance: 0.0030
3. **q1**: 2.92x speedup with `decorrelate`
   - Similarity: 100%
   - Distance: 0.0043

---

#### Q60

**Actual Result**: 1.02x (no significant speedup)

**Gold Patterns Detected**: GLD-003, GLD-003, GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q95**: 1.37x speedup with ``
   - Similarity: 100%
   - Distance: 0.0045

---

#### Q61

**Actual Result**: 0.40x (no significant speedup)

**Gold Patterns Detected**: GLD-003, GLD-003, GLD-002

**Top 3 Recommendations**:

1. **or_to_union**
   - Combined confidence: 70%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-002
       - Historical: 2.78x avg, 2.78x max (1 cases)

2. **early_filter**
   - Combined confidence: 41%
   - Estimated speedup: 1.98x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)
     - Similarity-based: 1 similar query
       - Average speedup: 1.57x
       - Examples:
         - q90: 1.57x speedup (similarity: 100%)

3. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q90**: 1.57x speedup with `early_filter`
   - Similarity: 100%
   - Distance: 0.0089
2. **q84**: 1.22x speedup with ``
   - Similarity: 100%
   - Distance: 0.0116

---

#### Q64

**Actual Result**: 1.01x (no significant speedup)

**Gold Patterns Detected**: GLD-004

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 35%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 2.92x avg, 2.92x max (1 cases)

2. **union_cte_split**
   - Combined confidence: 35%
   - Estimated speedup: 1.36x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 1.36x avg, 1.36x max (1 cases)


---

#### Q66

**✓ Actual Result**: 1.23x speedup with ``

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q66**: 1.23x speedup with ``
   - Similarity: 100%
   - Distance: 0.0000

---

#### Q68

**Actual Result**: 0.95x (no significant speedup)

**Gold Patterns Detected**: GLD-003, GLD-002

**Top 3 Recommendations**:

1. **or_to_union**
   - Combined confidence: 70%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-002
       - Historical: 2.78x avg, 2.78x max (1 cases)

2. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

3. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)


---

#### Q69

**Actual Result**: 1.03x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q7

**Actual Result**: 1.00x (no significant speedup)

**Gold Patterns Detected**: GLD-003, GLD-002

**Top 3 Recommendations**:

1. **or_to_union**
   - Combined confidence: 70%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-002
       - Historical: 2.78x avg, 2.78x max (1 cases)

2. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

3. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)


---

#### Q70

**Actual Result**: 0.75x (no significant speedup)

**Gold Patterns Detected**: GLD-005

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 70%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-005
       - Historical: 2.92x avg, 2.92x max (1 cases)


---

#### Q71

**Actual Result**: 0.96x (no significant speedup)

**Gold Patterns Detected**: GLD-003, GLD-003, GLD-003, GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)


---

#### Q72

**Actual Result**: 0.97x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q73

**Actual Result**: 1.03x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q74

**✓ Actual Result**: 1.36x speedup with `union_cte_split`

**Gold Patterns Detected**: GLD-004, GLD-006

**Top 3 Recommendations**:

1. **union_cte_split** ✓ **MATCH**
   - Combined confidence: 76%
   - Estimated speedup: 1.36x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-006
       - Historical: 1.36x avg, 1.36x max (1 cases)
     - Similarity-based: 1 similar query
       - Average speedup: 1.36x
       - Examples:
         - q74: 1.36x speedup (similarity: 100%)

2. **decorrelate**
   - Combined confidence: 35%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 2.92x avg, 2.92x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q74**: 1.36x speedup with `union_cte_split`
   - Similarity: 100%
   - Distance: 0.0000

---

#### Q75

**Actual Result**: 0.94x (no significant speedup)

**Gold Patterns Detected**: GLD-004, GLD-003

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 35%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 2.92x avg, 2.92x max (1 cases)

2. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q77

**Actual Result**: 1.01x (no significant speedup)

**Gold Patterns Detected**: GLD-004

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 35%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 2.92x avg, 2.92x max (1 cases)

2. **union_cte_split**
   - Combined confidence: 35%
   - Estimated speedup: 1.36x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 1.36x avg, 1.36x max (1 cases)


---

#### Q78

**Actual Result**: 1.01x (no significant speedup)

**Gold Patterns Detected**: GLD-002, GLD-004

**Top 3 Recommendations**:

1. **or_to_union**
   - Combined confidence: 70%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-002
       - Historical: 2.78x avg, 2.78x max (1 cases)

2. **decorrelate**
   - Combined confidence: 35%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **union_cte_split**
   - Combined confidence: 35%
   - Estimated speedup: 1.36x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 1.36x avg, 1.36x max (1 cases)


---

#### Q79

**Actual Result**: 1.05x (no significant speedup)

**Gold Patterns Detected**: GLD-003, GLD-002

**Top 3 Recommendations**:

1. **or_to_union**
   - Combined confidence: 70%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-002
       - Historical: 2.78x avg, 2.78x max (1 cases)

2. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

3. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)


---

#### Q8

**Actual Result**: 1.03x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q80

**Actual Result**: 1.03x (no significant speedup)

**Gold Patterns Detected**: GLD-004, GLD-003, GLD-003, GLD-003

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 35%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 2.92x avg, 2.92x max (1 cases)

2. **early_filter** ✓ **MATCH**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)


---

#### Q81

**Actual Result**: 1.00x (no significant speedup)

**Gold Patterns Detected**: GLD-001, GLD-005, GLD-004, GLD-003

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 70%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-001
       - Historical: 2.92x avg, 2.92x max (1 cases)

2. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)


---

#### Q85

**Actual Result**: 1.00x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q88

**Actual Result**: 0.99x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 41%
   - Estimated speedup: 1.98x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)
     - Similarity-based: 1 similar query
       - Average speedup: 1.57x
       - Examples:
         - q90: 1.57x speedup (similarity: 100%)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q90**: 1.57x speedup with `early_filter`
   - Similarity: 100%
   - Distance: 0.0161

---

#### Q90

**✓ Actual Result**: 1.57x speedup with `early_filter`

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter** ✓ **MATCH**
   - Combined confidence: 41%
   - Estimated speedup: 1.98x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)
     - Similarity-based: 1 similar query
       - Average speedup: 1.57x
       - Examples:
         - q90: 1.57x speedup (similarity: 100%)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q90**: 1.57x speedup with `early_filter`
   - Similarity: 100%
   - Distance: 0.0000
2. **q6**: 1.33x speedup with ``
   - Similarity: 100%
   - Distance: 0.0065
3. **q95**: 1.37x speedup with ``
   - Similarity: 100%
   - Distance: 0.0080

---

#### Q91

**Actual Result**: 0.66x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q92

**Actual Result**: 0.95x (no significant speedup)

**Gold Patterns Detected**: GLD-003, GLD-001, GLD-005

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 70%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-001
       - Historical: 2.92x avg, 2.92x max (1 cases)

2. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)


---

#### Q93

**✓ Actual Result**: 2.73x speedup with `early_filter`

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter** ✓ **MATCH**
   - Combined confidence: 41%
   - Estimated speedup: 2.32x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)
     - Similarity-based: 1 similar query
       - Average speedup: 2.73x
       - Examples:
         - q93: 2.73x speedup (similarity: 100%)

2. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

3. **or_to_union**
   - Combined confidence: 18%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q93**: 2.73x speedup with `early_filter`
   - Similarity: 100%
   - Distance: 0.0000

---

#### Q95

**✓ Actual Result**: 1.37x speedup with ``

**Gold Patterns Detected**: GLD-005, GLD-004

**Top 3 Recommendations**:

1. **decorrelate**
   - Combined confidence: 76%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 100% confidence
       - Detected: GLD-005
       - Historical: 2.92x avg, 2.92x max (1 cases)
     - Similarity-based: 1 similar query
       - Average speedup: 2.92x
       - Examples:
         - q1: 2.92x speedup (similarity: 100%)

2. **union_cte_split**
   - Combined confidence: 35%
   - Estimated speedup: 1.36x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-004
       - Historical: 1.36x avg, 1.36x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q95**: 1.37x speedup with ``
   - Similarity: 100%
   - Distance: 0.0000
2. **q1**: 2.92x speedup with `decorrelate`
   - Similarity: 100%
   - Distance: 0.0028
3. **q6**: 1.33x speedup with ``
   - Similarity: 100%
   - Distance: 0.0030

---

#### Q96

**Actual Result**: 1.01x (no significant speedup)

**Gold Patterns Detected**: GLD-003

**Top 3 Recommendations**:

1. **early_filter**
   - Combined confidence: 35%
   - Estimated speedup: 2.15x
   - **Methodology**:
     - Pattern-based: 50% confidence
       - Detected: GLD-003
       - Historical: 2.15x avg, 2.73x max (2 cases)

2. **or_to_union**
   - Combined confidence: 24%
   - Estimated speedup: 2.78x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.78x avg, 2.78x max (1 cases)
     - Similarity-based: 1 similar query
       - Average speedup: 2.78x
       - Examples:
         - q15: 2.78x speedup (similarity: 100%)

3. **decorrelate**
   - Combined confidence: 18%
   - Estimated speedup: 2.92x
   - **Methodology**:
     - Pattern-based: 25% confidence
       - Detected: GLD-003
       - Historical: 2.92x avg, 2.92x max (1 cases)

**Gold Example Matches** (structurally similar winning queries):

1. **q84**: 1.22x speedup with ``
   - Similarity: 100%
   - Distance: 0.0066
2. **q15**: 2.78x speedup with `or_to_union`
   - Similarity: 100%
   - Distance: 0.0067

---


### Queries with No Gold Patterns (26)

These queries do not match any verified optimization patterns.

Query IDs: q12, q16, q20, q21, q22, q37, q38, q40, q44, q53, q62, q63, q65, q67, q76, q82, q83, q84, q86, q87, q89, q9, q94, q97, q98, q99


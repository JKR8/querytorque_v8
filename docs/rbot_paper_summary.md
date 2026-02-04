# R-Bot Paper Results (VLDB 2025) - Extracted Numbers

Source: https://www.vldb.org/pvldb/vol18/p5031-li.pdf

## Did they use TPC-DS?
- Not directly. They evaluate on TPC-H, DSB, and Calcite.
- DSB is described as adapted from TPC-DS (TPC-DS-like), not TPC-DS itself.

---

## Table 4: Query Latency (seconds)
Reported as Avg / Median / p90.

### TPC-H 10x
- Origin: 104.86 / 10.60 / 300.00
- LearnedRewrite: 69.60 / 12.26 / 300.00
- GPT-3.5: 85.98 / 10.60 / 300.00
- GPT-4: 67.10 / 10.60 / 300.00
- R-Bot (GPT-3.5): 55.71 / 10.41 / 300.00
- R-Bot (GPT-4): 57.60 / 10.37 / 300.00

### DSB 10x
- Origin: 37.76 / 5.28 / 300.00
- LearnedRewrite: 30.47 / 5.28 / 55.02
- GPT-3.5: 37.75 / 5.36 / 300.00
- GPT-4: 37.77 / 4.92 / 300.00
- R-Bot (GPT-3.5): 26.19 / 4.61 / 35.25
- R-Bot (GPT-4): 25.35 / 4.58 / 17.17

### Calcite (uniform)
- Origin: 109.73 / 56.35 / 300.00
- LearnedRewrite: 79.07 / 5.24 / 300.00
- GPT-3.5: 55.41 / 22.74 / 230.99
- GPT-4: 60.86 / 20.06 / 300.00
- R-Bot (GPT-3.5): 37.71 / 8.37 / 65.67
- R-Bot (GPT-4): 12.45 / 5.04 / 48.30

---

## Table 5: Query Improvement Ratio
Counts of improved queries.

- LearnedRewrite:
  - TPC-H 10x: 7/44 (15.9%)
  - DSB 10x: 4/76 (5.3%)
  - Calcite (uni): 29/44 (63.6%)
- GPT-3.5:
  - TPC-H 10x: 3/44 (6.8%)
  - DSB 10x: 4/76 (5.3%)
  - Calcite (uni): 16/44 (36.4%)
- GPT-4:
  - TPC-H 10x: 6/44 (13.6%)
  - DSB 10x: 4/76 (5.3%)
  - Calcite (uni): 21/44 (47.7%)
- R-Bot (GPT-3.5):
  - TPC-H 10x: 21/44 (47.7%)
  - DSB 10x: 16/76 (21.0%)
  - Calcite (uni): 31/44 (70.4%)
- R-Bot (GPT-4):
  - TPC-H 10x: 17/44 (38.6%)
  - DSB 10x: 18/76 (23.7%)
  - Calcite (uni): 39/44 (88.6%)

---

## Table 6: Calcite (zipf) Query Latency (seconds)
Reported as Avg / Median / p90.

- Origin: 106.31 / 37.91 / 300.00
- LearnedRewrite: 71.24 / 5.04 / 300.00
- GPT-3.5: 58.33 / 20.04 / 300.00
- GPT-4: 61.80 / 14.15 / 300.00
- R-Bot (GPT-3.5): 32.44 / 6.58 / 57.40
- R-Bot (GPT-4): 7.56 / 4.96 / 18.08

---

## Other explicit claims in the paper
- Overall latency reduction on TPC-H 50x and DSB 50x: 1.82x and 1.68x respectively.
- Rewrite latency around ~1 minute average for evidence retrieval + iterative LLM steps.
- Real-world claim: Huawei deployment reduced workload latency by 3.7x on 20 critical queries.

---

## Query-by-query breakdown
- The paper does not provide per-query latency or per-query improvement tables.
- Only aggregate metrics and improved-query counts are reported.

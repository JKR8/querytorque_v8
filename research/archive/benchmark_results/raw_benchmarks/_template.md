# {PROVIDER} - TPC-DS Optimization Results

**Provider:** {PROVIDER_NAME}
**Database:** DuckDB SF100 (100GB)
**Optimizer:** DSPy pipeline
**Date:** {YYYY-MM-DD}

---

## Summary

| Metric | Value |
|--------|-------|
| Queries Optimized | X/99 |
| Average Speedup | X.XXx |
| Wins (>=1.2x) | X |
| Regressions (<1.0x) | X |
| **Validated (Sample)** | **X/X** |
| **Failed Validation** | **X** |

---

## Full Results

| Query | Status | Original | Optimized | Speedup | Pattern | Sample |
|-------|--------|----------|-----------|---------|---------|--------|
| q1 | pass/fail | Xms | Xms | X.XXx | `PATTERN` | pass/fail |
| q2 | pass/fail | Xms | Xms | X.XXx | `PATTERN` | pass/fail |
| ... | | | | | | |
| q99 | pass/fail | Xms | Xms | X.XXx | `PATTERN` | pass/fail |

---

## Failed Validations (Semantic Errors)

| Query | Pattern | Issue |
|-------|---------|-------|
| qX | `PATTERN` | Description |

---

## Top Wins (>=1.2x speedup)

| Query | Speedup | Pattern | Notes |
|-------|---------|---------|-------|
| qX | X.XXx | `PATTERN` | Description |

---

## Run Configuration

```yaml
optimizer: DSPy
model: {model_name}
database: DuckDB SF100
validation: 1-1-2-2 benchmark checksum
warmup_runs: 1
benchmark_runs: 3
```

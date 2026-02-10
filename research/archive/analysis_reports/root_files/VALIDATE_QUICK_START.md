# DuckDB Validation - Quick Start

## TL;DR

**Collections to validate**:
- `retry_neutrals/`: 43 queries, expected 1.71x avg, Q88 at 5.25x
- `retry_collect/`: 7 queries, expected 56% recovery, Q9 at 4.47x

## 30-Second Start

```bash
# 1. Check summary (no queries run)
python3 research/validate_summary.py

# 2. Run validation (need DuckDB installed)
pip install duckdb
python3 research/validate_duckdb_tpcds.py --collection both --scale 0.1

# 3. Check results
cat validation_report.json | python3 -m json.tool | head -100
```

## What's Being Validated

| Collection | Queries | Status | Speedup | Win Rate |
|-----------|---------|--------|---------|----------|
| retry_neutrals | 43 | 83.7% pass | 1.71x avg | 48.8% ≥1.5x |
| retry_collect | 7 | Expected: 56% improve | 4.47x best | - |

## Top Winners

- **Q88**: 5.25x (time_bucket_aggregation) ⭐
- **Q40**: 3.35x (multi_cte_chain)
- **Q46**: 3.23x (triple_dimension_isolate)
- **Q42**: 2.80x (dual_dimension_isolate)
- **Q9**: 4.47x (single_pass_aggregation) - NEW PATTERN

## Key Scripts

1. **validate_summary.py** - Quick stats (no queries)
   ```bash
   python3 research/validate_summary.py
   ```

2. **validate_duckdb_tpcds.py** - Full validation (5-run trimmed mean)
   ```bash
   # Fast (SF0.1, ~30 min)
   python3 research/validate_duckdb_tpcds.py --scale 0.1

   # Accurate (SF1.0, ~2 hours)
   python3 research/validate_duckdb_tpcds.py --scale 1.0
   ```

## Validation Rules (CRITICAL)

✓ **VALID**:
- 5-run trimmed mean (remove min/max, average 3)
- 3-run approach (discard warmup, average 2)

✗ **INVALID**:
- Single-run timing
- All 5 runs without trimming
- No warmup

## Expected Results

**Pass criteria**: Actual speedup within ±15% of expected
- Example: Expected 2.92x, Actual 2.68x → 8.2% deviation ✓ PASS

## Failed Queries (from retry_neutrals)

- Q4, Q8, Q13, Q33, Q45, Q49, Q60, Q79, Q85
- Status: Semantic errors or no improvement on all workers
- Action: Needs different optimization approach

## Documentation

- `VALIDATION_GUIDE.md` - Complete guide with methodology
- `VALIDATION_SUMMARY.md` - Overview and expectations
- `validate_duckdb_tpcds.py` - Main script with 5-run trimmed mean
- `validate_summary.py` - Quick summary without queries

## Troubleshooting

**DuckDB not found?**
```bash
pip install duckdb
```

**TPC-DS extension not loading?**
```bash
duckdb -c "INSTALL tpcds; LOAD tpcds"
```

**Out of memory?**
- Use `--scale 0.1` (100MB) instead of 1.0 (10GB)
- Or run one collection at a time

---

**Status**: ✓ Ready for full DuckDB validation

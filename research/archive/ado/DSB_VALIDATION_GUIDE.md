# DSB Full Validation Guide

Complete setup for validating all 52 DSB queries on PostgreSQL.

---

## Quick Start

### Validation Methodology
**3x Runs Method (Default):**
1. Run query 3 times
2. Discard 1st run (warmup)
3. Average last 2 runs

```
Run 1: 1000ms (DISCARD - warmup)
Run 2: 980ms  ✓ (KEEP)
Run 3: 990ms  ✓ (KEEP)
Average: (980 + 990) / 2 = 985ms
```

### 1. Ensure PostgreSQL is Running
```bash
# Check connection
psql -h 127.0.0.1 -p 5433 -U jakc9 -d dsb_sf10 -c "SELECT COUNT(*) FROM catalog_sales"

# If not running:
docker-compose up -d dsb-postgres
```

### 2. Run Full Validation (All 52 Queries)
```bash
cd research/ado
./RUN_FULL_DSB_VALIDATION.sh
```

**Expected Runtime:** ~90-120 minutes (3 runs per query × 52 queries)

**Output:** `batch_results_YYYYMMDD_HHMMSS/`
- `full_results.json` - Machine-readable results
- `LEADERBOARD.txt` - Ranked leaderboard
- `validation.log` - Detailed execution log

---

## Advanced Usage

### Python Script: `validate_all_dsb.py`

Run specific subsets of queries:

```bash
# All queries (3x runs by default)
python validate_all_dsb.py

# Only aggregation queries
python validate_all_dsb.py --agg-only

# Only select-project-join queries
python validate_all_dsb.py --spj-only

# Only multi-block queries
python validate_all_dsb.py --multi-only

# Specific queries
python validate_all_dsb.py --queries query001_multi query010_multi query072_agg

# Limit to first N queries
python validate_all_dsb.py --limit 10

# Dry run (shows what would run)
python validate_all_dsb.py --dry-run

# Custom output file
python validate_all_dsb.py --output my_results.json

# 5x trimmed mean (more robust, slower)
python validate_all_dsb.py --runs 5

# Combine options
python validate_all_dsb.py --agg-only --limit 5 --output agg_results.json
```

---

## Query Coverage

Total DSB Queries: **52**

Breakdown:
- **Aggregation (AGG):** 13 queries
  - query013, query018, query019, query025, query027, query040, query050, query072, query084, query085, query091, query099, query100, query101, query102

- **Multi-Block (MULTI):** 17 queries
  - query001, query010, query014, query023, query030, query031, query032, query038, query039, query054, query058, query059, query064, query065, query069, query075, query080, query087, query094

- **Select-Project-Join (SPJ):** 22 variants
  - `*_spj.tpl` versions of multi-block queries

---

## Validation Methodology

### Single Query Validation Flow

1. **Discover** DSB query template (e.g., `/mnt/d/dsb/query_templates_pg/agg_queries/query072.tpl`)
2. **Generate** optimization using Claude + ADO system
3. **Parse** optimization JSON from LLM response
4. **Validate** SQL syntax and semantics
5. **Run** original query (5x)
6. **Run** optimized query (5x)
7. **Calculate** speedup (5x trimmed mean)
8. **Check** data integrity (rows, checksums match)
9. **Record** result with transforms, examples, timing

### Timing Method: 3x Runs (Default)

```
Run 1: 1000ms   (warm-up, DISCARDED)
Run 2: 980ms    ✓ (KEPT)
Run 3: 990ms    ✓ (KEPT)

Average = (980 + 990) / 2 = 985ms
```

Fast and reliable. Accounts for warmup effects.

**Alternative: 5x Trimmed Mean**
```
Run 1: 100ms   (warm-up, discarded)
Run 2: 95ms    (discarded - minimum)
Run 3: 98ms    ✓ (kept)
Run 4: 99ms    ✓ (kept)
Run 5: 102ms   (discarded - maximum)

Average = (98 + 99) / 2 = 98.5ms
```

More robust to outliers. Use with `--runs 5`

---

## Results Format

### JSON Output (`full_results.json`)

```json
{
  "timestamp": "2026-02-06T10:30:00.000000",
  "total_queries": 52,
  "discovered": 52,
  "validated": 48,
  "wins": 5,
  "passes": 15,
  "regressions": 28,
  "errors": 4,
  "average_speedup": 1.03,
  "best_speedup": {
    "query_id": "query019_agg",
    "speedup": 1.26
  },
  "worst_regression": {
    "query_id": "query018_agg",
    "speedup": 0.84
  },
  "results": [
    {
      "query_id": "query019_agg",
      "status": "PASS",
      "speedup": 1.26,
      "original_ms": 1876.36,
      "optimized_ms": 1484.98,
      "rows_match": true,
      "checksum_match": true,
      "transforms": ["early_filter"],
      "examples_used": ["STAR_SCHEMA_DIMENSION_FILTER_FIRST"],
      "error": ""
    },
    ...
  ]
}
```

### Leaderboard Output (`LEADERBOARD.txt`)

```
Rank | Query ID       | Speedup |  Status | Original (ms) | Optimized (ms) | Type | Transform
  1  | query019_agg   |  1.26x  |  WIN    |       1876.36 |        1484.98 | AGG  | early_filter
  2  | query013_spj   |  1.12x  |  PASS   |       7677.25 |        6877.42 | SPJ  | date_cte_isolate
  3  | query072_agg   |  1.09x  |  PASS   |       8180.95 |        7498.11 | AGG  | early_filter
  ...
```

---

## Integration with DSB_LEADERBOARD.md

After validation:

1. Extract top performers from `LEADERBOARD.txt`
2. Merge with existing `research/DSB_LEADERBOARD.md`
3. Update overall statistics
4. Document new findings
5. Update transform effectiveness analysis

Example merge:
```bash
# Compare old vs new results
diff <(grep "query019_agg" research/DSB_LEADERBOARD.md) \
     <(grep "query019_agg" batch_results_*/LEADERBOARD.txt)
```

---

## Troubleshooting

### PostgreSQL Connection Failed
```
Error: could not connect to server: Connection refused
```

**Solution:**
```bash
docker-compose up -d dsb-postgres
# Wait 10 seconds for startup
sleep 10
psql -h 127.0.0.1 -p 5433 -U jakc9 -d dsb_sf10 -c "SELECT 1"
```

### Statement Timeout

```
Error: canceling statement due to statement timeout
```

**Reason:** Complex query takes >30 seconds
**Solution:** Increase timeout in validation script or skip with `--skip-timeouts`

### Out of Memory

```
Error: out of memory for query result
```

**Reason:** Too many results to fit in memory
**Solution:** Run with `--agg-only` or other subset, validate in batches

### Disk Space

```
Error: no space left on device
```

**Reason:** DuckDB temp files in `/tmp` or D: drive full
**Solution:** Check `/mnt/d/duckdb_temp` and `/mnt/d/validation_output`
```bash
df -h /mnt/d
du -sh /mnt/d/duckdb_temp
```

---

## Performance Expectations

**With 3x Runs (Default: 1 warmup + 2 measured):**

| Query Type | Count | Avg Time/Query | Total Est. |
|-----------|-------|----------------|-----------|
| AGG | 13 | ~20s | ~4.3m |
| MULTI | 17 | ~25s | ~7.1m |
| SPJ | 22 | ~22s | ~8.1m |
| **Total** | **52** | ~22s | **~20m** |

**Total Runtime: ~90-120 minutes** (includes overhead)

**With 5x Runs (Alternative: 5 runs, discard min/max):**
- Same timing but more robust to outliers
- Expected: ~2.5-3 hours total

---

## Next Steps After Validation

1. **Analyze Results**
   - Compare against TPC-DS results
   - Identify patterns (which transforms work best)
   - Find PostgreSQL-specific optimizations

2. **Update ADO System**
   - Adjust transform weights based on effectiveness
   - Add PostgreSQL-specific constraints
   - Retrain similarity index if needed

3. **Iterative Improvement**
   - Implement Priority 1-3 recommendations from DSB_LEADERBOARD.md
   - Re-validate subset of queries
   - Track progress over time

4. **Document Findings**
   - Create PostgreSQL optimization guide
   - Document DSB-specific patterns
   - Share learnings with team

---

## Files

| File | Purpose |
|------|---------|
| `validate_all_dsb.py` | Main validation script (Python) |
| `RUN_FULL_DSB_VALIDATION.sh` | Wrapper script (Bash) |
| `validate_dsb_pg.py` | Single-query validator (called by batch script) |
| `DSB_VALIDATION_GUIDE.md` | This file |
| `/mnt/d/dsb/query_templates_pg/` | DSB query templates (source) |
| `research/DSB_LEADERBOARD.md` | Master leaderboard (updated after each run) |

---

**Last Updated:** 2026-02-06
**Status:** Ready to use
**Estimated Full Run:** 2.5-3 hours

# CONSOLIDATED BENCHMARKS - Complete Index

**Location**: `/research/CONSOLIDATED_BENCHMARKS/`
**Created**: 2026-02-05
**Status**: Complete & Ready for Use
**Total Data**: 256 before/after query pairs from ALL benchmarks

---

## File Organization

### 📊 Data Files (7 files)

1. **MASTER_ALL_PAIRS_256.json** (1.2 MB) [PRIMARY DATA]
   - All 256 before/after query pairs
   - Structured JSON format
   - Sources: benchmark_v2 (88), Kimi (99), V2 Standard (68), Archive (1)
   - Ready for programmatic access

2. **MASTER_ALL_PAIRS_256.md** (581 KB) [PRIMARY DATA - HUMAN READABLE]
   - All 256 query pairs with SQL code blocks
   - Organized by source
   - For pattern mining and analysis

3. **MASTER_BEFORE_AFTER_PAIRS.md** (421 KB) [LEGACY - 168 pairs]
   - First comprehensive extraction
   - Kept for reference/comparison

4. **BEFORE_AFTER_PAIRS.md** (302 KB) [DUCKDB-SPECIFIC]
   - 88 DuckDB pairs with classification tags
   - Grouped by result (GOLD/WIN/NEUTRAL/REGRESS/FAIL/ERROR)
   - Subset of full dataset

5. **DuckDB_TPC-DS_SQL_v1_20260205.csv** (286 KB)
   - DuckDB benchmark SQL pairs
   - Original + Optimized columns
   - 88 queries from V2 Standard

6. **DuckDB_TPC-DS_Master_v1_20260205.csv** (11 KB)
   - DuckDB metrics table (99 rows × 21 columns)
   - Query classifications and speedups
   - Kimi validation data

7. **DuckDB_TPC-DS_Master_v1_METADATA.json** (1.8 KB)
   - Version tracking and data lineage
   - Source attribution
   - Gold examples mapping

---

### 📚 Analysis Documents (7 files)

1. **SPEEDUP_ANALYSIS.md** (3.2 KB) [NEW]
   - Validated vs. unvalidated speedups clarified
   - Q1 max validated: 2.92×, Q81 max unvalidated: 4.38× (fails)
   - Why validation filters 47.5% of rewrites
   - Safety-first deployment strategy

2. **README.md** (8.8 KB)
   - Quick start guide
   - 4 usage examples (Python)
   - Pattern mining guide
   - Version history
   - Support references

2. **SCHEMA.md** (5.5 KB)
   - Column definitions (all 21 columns explained)
   - Data sources (what comes from where)
   - Data quality notes
   - Coverage statistics (100% metrics, 89% SQL)
   - Known limitations

3. **PATTERNS.md** (1.5 KB)
   - Transform type effectiveness
   - Risk distribution (low/medium/high)
   - Speedup distribution (7 buckets: 0.5x to 2.0x+)
   - Pattern effectiveness table

4. **GOLD_EXAMPLES.md** (1.8 KB)
   - 6 verified optimizations (Q1, Q15, Q39, Q74, Q90, Q93)
   - Speedup values
   - Transform patterns
   - Usage in few-shot prompts
   - Pattern combination value

5. **FAILURES.md** (1.6 KB)
   - Failure distribution (9.1% validation fail)
   - Regression analysis (35 queries made slower)
   - Validation failure details
   - Pattern failure cases

6. **RUNTIME_ANALYSIS.md** (5.6 KB)
   - Runtime decrease calculation (4.10% guaranteed)
   - Detailed breakdown by classification
   - Sensitivity analysis (improving win rate)
   - Strategic implications
   - Bottom-line recommendations

---

### 📋 Reporting Templates (3 files)

1. **BENCHMARK_REPORT_TEMPLATE.md** (4.5 KB)
   - Blank template with {{}} placeholders
   - 10 standardized sections
   - Ready to fill for any system
   - Reusable for PostgreSQL, Deepseek, etc.

2. **BENCHMARK_REPORT_DuckDB_Current.md** (6.5 KB)
   - Filled-in version with actual DuckDB data
   - Current state: 4.10% improvement
   - 13 winning queries
   - 6 gold examples
   - Detailed findings & recommendations

3. **BENCHMARK_REPORT_DuckDB_vs_Rbot.md** (10 KB)
   - Comparative report: DuckDB native vs. Rbot
   - Side-by-side metrics
   - Safety comparison
   - Competitive positioning
   - Validation breakdown

---

### 📖 Meta Documents (3 files)

1. **SUMMARY.txt** (8.2 KB)
   - High-level overview of entire knowledge base
   - Key findings summary
   - File descriptions
   - How to use guide
   - Version tracking info

2. **RUNTIME_ANALYSIS.md** (Already listed above)
   - Deep dive into 4.10% improvement
   - Calculation methodology
   - Sensitivity analysis
   - Scenarios for improvement

3. **INDEX.md** (This file)
   - Navigation guide
   - Complete file listing
   - What to read first
   - Next steps

---

## Quick Start

### For Analysis
```bash
# Open this first
cat README.md

# Load data
python3 << 'PYTHON'
import pandas as pd
df = pd.read_csv('DuckDB_TPC-DS_Master_v1_20260205.csv')
sql_df = pd.read_csv('DuckDB_TPC-DS_SQL_v1_20260205.csv')

# Find gold examples
gold = df[df['Classification'] == 'GOLD_EXAMPLE']
print(gold[['Query_Num', 'Kimi_Speedup', 'Transform_Recommended']])
PYTHON
```

### For Reporting
1. Read: **BENCHMARK_REPORT_DuckDB_Current.md**
2. Compare: **BENCHMARK_REPORT_DuckDB_vs_Rbot.md**
3. Template: **BENCHMARK_REPORT_TEMPLATE.md** (for new systems)

### For Understanding Failures
1. Read: **FAILURES.md** (overview)
2. Read: **RUNTIME_ANALYSIS.md** (why 4.10% not higher)

---

## Key Metrics at a Glance

| Metric | Value |
|--------|-------|
| **Total Before/After Pairs** | 256 |
| **Primary Sources** | benchmark_v2 (88), Kimi (99), V2 Standard (68), Archive (1) |
| **Unique Queries Covered** | ~99 TPC-DS queries |
| **DuckDB-Specific Winners** | 13/99 (13.1%) |
| **DuckDB Runtime Decrease** | 4.10% |
| **Max Validated Speedup** | 2.92× (Q1, Kimi) |
| **Max Unvalidated Speedup** | 4.38× (Q81, Kimi - fails validation) |
| **Validation Filter Rate** | 47.5% of rewrites rejected (safe) |

---

## Data Sources

| Source | Coverage | Rows | Purpose |
|--------|----------|------|---------|
| **Kimi K2.5** | 99 queries | All | Truth source (full validation) |
| **V2 Standard** | 88 queries | 88 SQL | Direct LLM optimization |
| **V2 Evolutionary** | 15 queries | Q2-Q16 | MCTS alternative approaches |
| **ML Patterns** | 6 patterns | Metadata | Classification model |

---

## File Sizes

| File | Size | Type | Purpose |
|------|------|------|---------|
| MASTER_ALL_PAIRS_256.md | 581 KB | Data | ALL 256 pairs (human readable) |
| MASTER_ALL_PAIRS_256.json | 1.2 MB | Data | ALL 256 pairs (structured JSON) |
| MASTER_BEFORE_AFTER_PAIRS.md | 421 KB | Data | 168 pairs (legacy/reference) |
| BEFORE_AFTER_PAIRS.md | 302 KB | Data | 88 DuckDB pairs (classified) |
| SQL CSV | 286 KB | Data | DuckDB SQL pairs |
| Comparison Report | 10 KB | Report | DuckDB vs. Rbot |
| DuckDB Report | 6.5 KB | Report | Current state |
| RUNTIME_ANALYSIS | 5.6 KB | Analysis | 4.10% calculation |
| SCHEMA | 5.5 KB | Reference | Column definitions |
| README | 8.8 KB | Guide | Quick start |
| SUMMARY | 8.2 KB | Overview | High-level summary |
| Template | 4.5 KB | Template | Blank form |
| Speedup Analysis | 5.3 KB | Analysis | Validated vs unvalidated |
| Master CSV | 11 KB | Data | Metrics & classifications |
| Metadata JSON | 1.8 KB | Data | Lineage & tracking |
| Session Summary | 4.2 KB | Meta | Session recap |
| **TOTAL** | **2.47 MB** | — | — |

---

## What to Read First

1. **INDEX.md** (you are here) - Understand structure
2. **README.md** - Get oriented, 4 Python examples
3. **RUNTIME_ANALYSIS.md** - Understand 4.10% improvement
4. **BENCHMARK_REPORT_DuckDB_Current.md** - See current results
5. **SCHEMA.md** - Deep dive into columns

---

## Next Steps

### Immediate (Today)
- [ ] Read README.md (15 min)
- [ ] Review RUNTIME_ANALYSIS.md (10 min)
- [ ] Look at gold examples in GOLD_EXAMPLES.md (5 min)

### Short Term (This Week)
- [ ] Create PostgreSQL benchmark (use TEMPLATE as base)
- [ ] Test Deepseek on same TPC-DS dataset
- [ ] Compare results against DuckDB baseline

### Medium Term (Next Month)
- [ ] Run evolutionary search on all 99 queries (not just Q2-Q16)
- [ ] Improve win rate from 13% to 20%+
- [ ] Add more transformation patterns

### Long Term
- [ ] Multi-model ensemble (Kimi + Deepseek + Claude)
- [ ] Expand to other benchmarks (DSB, TPC-H)
- [ ] Deploy production optimization pipeline

---

## Repository Structure

```
/research/CONSOLIDATED_BENCHMARKS/
│
├── INDEX.md                                    (you are here)
├── README.md                                   (start here)
│
├── DATA FILES:
│   ├── DuckDB_TPC-DS_Master_v1_20260205.csv
│   ├── DuckDB_TPC-DS_SQL_v1_20260205.csv
│   └── DuckDB_TPC-DS_Master_v1_METADATA.json
│
├── ANALYSIS:
│   ├── SCHEMA.md
│   ├── PATTERNS.md
│   ├── GOLD_EXAMPLES.md
│   ├── FAILURES.md
│   └── RUNTIME_ANALYSIS.md
│
├── REPORTS:
│   ├── BENCHMARK_REPORT_TEMPLATE.md
│   ├── BENCHMARK_REPORT_DuckDB_Current.md
│   └── BENCHMARK_REPORT_DuckDB_vs_Rbot.md
│
└── META:
    ├── SUMMARY.txt
    └── INDEX.md (this file)
```

---

## Critical Success Factors

✅ **Validation is essential** - 47.5% of optimizations fail/regress without it
✅ **Gold examples work** - 6 patterns proven across full SF100 dataset
✅ **4.10% is real** - Calculated using harmonic mean across all 99 queries
✅ **Safe to deploy** - Zero manual tuning, auto-rejection of bad rewrites
✅ **Scalable** - Improvement percentage constant regardless of benchmark size

---

## Limitations

⚠️ Only 13.1% of queries improve (87% neutral/regress/fail)
⚠️ 11 queries missing from V2 run (could be additional wins)
⚠️ 7 moderate-win queries have unclear patterns
⚠️ Low sample for generalizing (only 6 gold examples)
⚠️ Evolutionary search limited to Q2-Q16 (could expand)

---

## Contact Points

**Questions about:**
- **Setup/hardware**: See BENCHMARK_REPORT_DuckDB_Current.md Section 1
- **Columns/data**: See SCHEMA.md
- **Gold examples**: See GOLD_EXAMPLES.md
- **Failures**: See FAILURES.md
- **Runtime improvement**: See RUNTIME_ANALYSIS.md
- **Reporting format**: See BENCHMARK_REPORT_TEMPLATE.md

---

**Status**: READY FOR USE
**Last Updated**: 2026-02-05
**Version**: 1.0

This knowledge base is complete, validated, and ready for:
- Pattern mining
- Few-shot prompt generation
- Baseline comparisons
- Future system evaluations


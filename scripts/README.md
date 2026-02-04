# Scripts Usage Guide

## ⚠️ Always Use Virtual Environment

All Python scripts **require** the virtual environment to be activated.

### Quick Start (Recommended)

Use the helper script that handles venv automatically:

```bash
./scripts/run_ml_pipeline.sh
```

### Manual Method

If running scripts individually, **always activate venv first**:

```bash
# Activate venv (do this ONCE per terminal session)
source .venv/bin/activate

# Now you can run scripts
python3 scripts/generate_ml_training_data.py
python3 scripts/normalize_sql.py
python3 scripts/vectorize_queries.py
```

### What Happens Without Venv

Scripts will **immediately exit** with this error:

```
❌ ERROR: Not running in virtual environment!
Please run: source .venv/bin/activate
```

## ML Pipeline Scripts

### 1. generate_ml_training_data.py
**Purpose:** Extract training data from benchmark results

**Input:** `research/experiments/benchmarks/kimi_benchmark_20260202_221828/`
**Output:** `research/ml_pipeline/data/ml_training_data.csv`

**Features extracted:**
- Query ID and speedup factor
- AST rules detected
- Winning transform (if any)
- Structural indicators (CTE, UNION, subquery)

### 2. normalize_sql.py
**Purpose:** Strip domain-specific semantics from SQL

**Input:** Same benchmark directory
**Output:** `research/ml_pipeline/data/normalized_queries.json`

**Transformations:**
- Tables: `store_sales` → `fact_table_1`
- Columns: `d_year` → `dim_col_1`
- Literals: `2001` → `<INT>`

### 3. vectorize_queries.py
**Purpose:** Convert SQL to 90-dim feature vectors

**Input:** `research/ml_pipeline/data/normalized_queries.json`
**Output:**
- `research/ml_pipeline/vectors/query_vectors.npz` (vectors)
- `research/ml_pipeline/vectors/query_vectors_metadata.json` (feature names)

**Features:**
- 40 node type counts
- 5 depth metrics
- 10 cardinality features
- 30 pattern indicators
- 5 complexity scores

## Gold Detector Tests

Test scripts for validating gold detectors:

```bash
source .venv/bin/activate

# Test all 7 gold detectors
python3 test_all_gold_coverage.py

# Test specific detectors (Q74, Q73)
python3 test_q74_q73_detectors.py
```

## Helper Scripts

### run_ml_pipeline.sh
Runs complete ML pipeline with automatic venv activation:
1. Checks for venv existence
2. Activates venv
3. Runs all 3 pipeline scripts in sequence
4. Shows summary

### check_venv.py
Standalone venv checker:

```bash
python3 scripts/check_venv.py
```

## Troubleshooting

### "ModuleNotFoundError"
**Problem:** Script can't find required packages
**Solution:** Activate venv first: `source .venv/bin/activate`

### "Virtual environment not found"
**Problem:** `.venv` directory doesn't exist
**Solution:** Create it: `python3 -m venv .venv && pip install -r requirements.txt`

### "Permission denied" on .sh script
**Problem:** Script not executable
**Solution:** `chmod +x scripts/run_ml_pipeline.sh`

## Best Practices

✅ **DO:**
- Always activate venv before running Python scripts
- Use `./scripts/run_ml_pipeline.sh` for full pipeline
- Check venv is active: `echo $VIRTUAL_ENV` should show path

❌ **DON'T:**
- Run scripts with system Python (they will fail)
- Install packages with `pip` outside venv (use venv pip)
- Forget to activate venv in new terminal sessions

## Dependencies

Required packages (installed in venv):
- sqlglot (SQL parsing)
- numpy (vectorization)
- pandas (data handling)
- All packages from `packages/qt-sql/requirements.txt`

---

*For ML pipeline architecture, see: `docs/ml_pipeline_plan.md`*

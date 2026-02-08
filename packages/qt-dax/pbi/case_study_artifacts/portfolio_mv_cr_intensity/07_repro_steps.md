# Repro Steps (ADO-style)

1. Preconditions
- Windows machine with Power BI Desktop open on the target model.
- Python environment with `pyadomd` and ADOMD client available.

2. Load artifacts
- Original measure forest: `01_original_measure_forest.dax`
- Optimized measure: `02_optimized_measure_raw.dax`
- Helper columns: `03_helper1_calc_column.dax`, `05_helper2_calc_column_reconstructed.dax`

3. Model prep
- Add helper calculated columns in `Daily Position` if missing.
- Ensure optimized measure name and references match current model naming.

4. Execution harness
- Use `packages/qt-dax/test_manual_optimization.py` as runner baseline.
- Apply the same filter context query used by your visual (if available).

5. Validation output
- Record runs in `06_validation_template.json` format:
  - warmup discarded
  - 3 timed runs minimum
  - exact row/value match
  - baseline vs optimized min time and speedup

# DAX CR Intensity Case Review (Complete)

Date: 2026-02-08
Scope: `qt-dax` manual case study and extracted optimization dataset.

## 1. Executive Summary

- The headline win is present in source data: `Portfolio_Asset_Matrix MV CR Intensity Switch_BM` improved from **60s to 0.4s** (~150x).
- The win came from replacing a deep measure chain with a grain-first pipeline: precompute ownership and carbon/revenue at `ISIN` grain, join once, then compute ratio-of-sums.
- For this case, the large "related measure" block is not random bloat: it is effectively the transitive dependency closure of the original measure path.
- You do **not** need all model measures for optimization. You need:
  - target measure + dependency closure for semantic grounding,
  - visual filter context query for validation,
  - any model-precompute artifacts used by the optimized formula.
- Reproducibility is currently partially blocked by data quality gaps in extracted artifacts (details below).

## 2. Evidence Reviewed

Primary files:
- `packages/qt-dax/pbi/dax_performance_case_study.md`
- `packages/qt-dax/dax_optimizations_extracted.jsonl`
- `packages/qt-dax/dax_optimizations_extracted.csv`
- `packages/qt-dax/test_manual_optimization.py`
- `packages/qt-dax/test_pa_query_optimization.py`
- `packages/qt-dax/qt_dax/knowledge/dax_rulebook.yaml`
- `packages/qt-dax/pbi/DS-ESG Dataset.SemanticModel/definition/tables/ESG Trucost Climate.tmdl`

## 3. What Changed Technically (Why 60s -> 0.4s)

Before (slow path):
- Scope switch branching (`SELECTEDVALUE` + `SWITCH`) across many dependent measures.
- Nested iterators (`SUMX`) and branch-specific `GROUPBY + SUMX` patterns.
- Repeated ownership/carbon/revenue calculations across branches and measure levels.

After (fast path):
- Cache slicer selections once.
- Build compact `OwnershipByAsset` and `CarbonRevByAsset` tables at `ISIN` grain.
- `NATURALINNERJOIN` once.
- Compute numerator/denominator once each and finish with `DIVIDE(numerator, denominator)`.

This matches the rulebook entry `DAX-PERF-001` and the case study doc principles.

## 4. Is the Huge Related-Measure Context Useful or a Hindrance?

### Finding
For this case, the dumped measure set is the dependency closure, not irrelevant extras.

- `Portfolio_Asset_Matrix MV CR Intensity Switch_BM`: 38/38 dumped measures are in-chain.
- `Matrix MV CR Intensity Switch_BM`: 37/37 dumped measures are in-chain.

### Practical answer
- Useful: as a semantic source-of-truth for exact behavior and safe rewrite/validation.
- Hindrance: if sent raw to LLM without structure, it increases token load/noise and can reduce rewrite quality.

### Recommended compromise
Use **structured minimal context** instead of raw full dumps:
1. Root target measure expression.
2. Transitive dependency closure only.
3. Visual filter-context query (`single_measure_dax`) when available.
4. Any helper/precompute definitions required by optimized measure.
5. Validation contract (timing method + exact-match rules).

## 5. Reproducibility Status (Current Repo)

### Confirmed
- Case exists with recorded timings (`60s -> 0.4s`).
- Original and optimized expressions are stored.
- Test harnesses for PBI Desktop execution and exact-match validation are present.

### Blockers / Gaps
1. Missing PA replay query for the main 60s case:
   - `single_measure_dax` is empty in that row.
2. Helper definition truncation:
   - `helper2_dax` for `Ownership_Factor_EVIC` is cut off (missing closing syntax).
3. Model mismatch for optimized expression:
   - Optimized formula references `Daily Position[Ownership_Factor_MC]` and `Daily Position[Ownership_Factor_EVIC]`.
   - Those columns are not present in the checked-in PBIP TMDL snapshot.

Conclusion: exact replay is not one-command reproducible yet; it needs minor artifact and model hygiene first.

## 6. ADO-Style Repro Artifact Pack Added

Created:
- `packages/qt-dax/pbi/case_study_artifacts/portfolio_mv_cr_intensity/00_case_summary.json`
- `packages/qt-dax/pbi/case_study_artifacts/portfolio_mv_cr_intensity/01_original_measure_forest.dax`
- `packages/qt-dax/pbi/case_study_artifacts/portfolio_mv_cr_intensity/02_optimized_measure_raw.dax`
- `packages/qt-dax/pbi/case_study_artifacts/portfolio_mv_cr_intensity/03_helper1_calc_column.dax`
- `packages/qt-dax/pbi/case_study_artifacts/portfolio_mv_cr_intensity/04_helper2_calc_column_raw_truncated.dax`
- `packages/qt-dax/pbi/case_study_artifacts/portfolio_mv_cr_intensity/05_helper2_calc_column_reconstructed.dax`
- `packages/qt-dax/pbi/case_study_artifacts/portfolio_mv_cr_intensity/06_validation_template.json`
- `packages/qt-dax/pbi/case_study_artifacts/portfolio_mv_cr_intensity/07_repro_steps.md`

This mirrors ADO’s artifact style (numbered inputs/outputs + validation json contract).

## 7. Pipeline Readiness Fix Applied

Issue found during review:
- `MeasureDependencyAnalyzer` failed to detect dependencies when measure names contained symbols like `&`, `(`, `)`, `%`, etc. (common in this case).

Fix applied:
- Updated dependency reference regex in:
  - `packages/qt-dax/qt_dax/analyzers/measure_dependencies.py`
- Added regression test:
  - `packages/qt-dax/tests/test_integration.py` (`test_dependency_with_special_char_measure_names`)
- Verification:
  - `pytest -q packages/qt-dax/tests/test_integration.py -k "dependency"` -> passed.

Impact:
- Dependency closure for this case now resolves correctly (38 / 37 chain sizes), enabling reliable context extraction for the replication pipeline.

## 8. Recommended Next Step to Replicate the Pipeline

1. Standardize one canonical case record schema (`case_id`, root measure, closure measures, filter query, helpers, validation result).
2. Backfill missing `single_measure_dax` for the 60s case from PA export.
3. Decide model strategy explicitly:
   - either persist helper columns in model,
   - or inline equivalent logic in optimized measure (no extra columns).
4. Run reproducibility pass with exact-match validator and store results in artifact JSON.
5. Use this case as state-0 seed exemplar for automated qt-dax optimization runs.


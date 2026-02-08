# qt-dax

Power BI / DAX analysis and optimization package for QueryTorque.

This package covers:
- Static model analysis from VPAX/PBIP
- DAX anti-pattern detection and scoring
- Measure dependency analysis
- Optional LLM-assisted DAX optimization
- Live validation against Power BI Desktop (Windows)
- Case-study export into ADO-style reproducible artifact folders

## Start Here

If you are new to this folder:

1. Run static analysis on a model (`audit`)
2. Export case-study artifacts (`export-cases`)
3. Use exported artifacts to reproduce and validate optimizations

## Folder Map

- `qt_dax/analyzers/`: model and DAX analyzers
- `qt_dax/validation/`: syntax + equivalence validation
- `qt_dax/connections/`: Power BI Desktop connection
- `qt_dax/case_exporter.py`: normalized case exporter (new)
- `cli/main.py` and `qt_dax/cli/main.py`: CLI entrypoints (kept in sync)
- `dax_optimizations_extracted.jsonl`: raw case-study source records
- `pbi/case_study_artifacts/`: exported/review artifacts
- `tests/`: package tests, including security guard tests

## Core CLI Commands

```bash
qt-dax audit <model.vpax|model.pbip|SemanticModel_dir>
qt-dax optimize <model.vpax|model.pbip|SemanticModel_dir>
qt-dax connect --list
qt-dax diff <model_v1.vpax> <model_v2.vpax>
qt-dax validate <orig.dax> <opt.dax>
qt-dax export-cases
```

### Audit

```bash
qt-dax audit model.vpax
qt-dax audit model.pbip --json
qt-dax audit model.SemanticModel -v
```

### Optimize (LLM)

```bash
qt-dax optimize model.vpax --provider deepseek
qt-dax optimize model.vpax --dspy --port <pbi_port>
```

### Validate Two DAX Files

```bash
qt-dax validate original.dax optimized.dax --port <pbi_port>
```

### Export Case Studies (ADO-style Artifacts)

```bash
qt-dax export-cases \
  --input-path packages/qt-dax/dax_optimizations_extracted.jsonl \
  --output-dir packages/qt-dax/pbi/case_study_artifacts/exports
```

Filter or limit:

```bash
qt-dax export-cases -m "Portfolio_Asset_Matrix MV CR Intensity Switch_BM" --limit 1
```

## Exported Case Artifact Schema

Each case folder contains:

- `00_case_summary.json`: normalized metadata, timing fields, dependency summary, quality flags
- `01_original_measure_forest.dax`: original measure graph dump
- `02_optimized_measure_raw.dax`: optimized expression from source
- `03_helper1_calc_column.dax`: helper definition 1 (if present)
- `04_helper2_calc_column.dax`: helper definition 2 (if present)
- `05_single_measure_query.dax`: visual/PA query context (if present)
- `06_validation_template.json`: run result template for reproducibility
- `07_repro_steps.md`: minimal replay steps
- `08_dependency_closure.json`: extracted dependency closure for the target measure
- `manifest.json` (at export root): global export manifest

## Environment and Keys

Use environment variables only. Do not hardcode secrets.

```bash
export QT_DEEPSEEK_API_KEY=...
```

Compatibility fallback still supported in scripts:
- `DEEPSEEK_API_KEY` (legacy)

## Security Guardrails

This package includes a guard test that fails if key-like `sk-...` literals are committed:

```bash
pytest -q packages/qt-dax/tests/test_secret_guard.py
```

## Useful Test Commands

```bash
pytest -q packages/qt-dax/tests/test_case_exporter.py
pytest -q packages/qt-dax/tests/test_secret_guard.py
pytest -q packages/qt-dax/tests/test_integration.py -k "dependency"
pytest -q packages/qt-dax/tests/test_cli.py -k "help or command_exists"
```

## Power BI Desktop Notes

- Live connection/validation requires Windows + `pyadomd` + Power BI Desktop open with a loaded model.
- Static audit/export workflows run without live Power BI.

## Case Study References

- Main review report: `packages/qt-dax/pbi/DAX_CR_INTENSITY_COMPLETE_REVIEW.md`
- Existing manual case artifacts: `packages/qt-dax/pbi/case_study_artifacts/portfolio_mv_cr_intensity`

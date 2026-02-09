# QueryTorque Figures — Status Guide

## Generated Files

| File | Status | Data Source | Action Needed |
|------|--------|-------------|---------------|
| `fig1_system_overview.pdf` | ✅ READY | Architecture | None — drop into paper |
| `fig2_duckdb_perquery.pdf` | ⚠️ PARTIAL | Uses real top-5 winners, placeholder for rest | Replace query IDs and speedups in `fig_duckdb_perquery.py` with actual per-query data from your leaderboard |
| `fig3_gap_contribution.pdf` | ✅ READY | Table 3 data from paper | None — data matches paper |
| `fig4_worker_coverage.pdf` | ✅ READY | Table 5 data from paper | None — data matches paper |
| `fig5_rbot_scatter_TEMPLATE.pdf` | 🔴 TEMPLATE | Placeholder data | Run R-Bot head-to-head, then update `data[]` in `fig_rbot_scatter.py` |
| `fig6_cross_engine.pdf` | ⚠️ PARTIAL | DuckDB real, PG placeholder | Update PG panel with SF10 numbers once experiments complete |

## How to Update

Each figure is a standalone Python script. To regenerate:

```bash
cd figures/
# Edit the data in the .py file, then:
python3 fig_duckdb_perquery.py     # → fig2_duckdb_perquery.pdf
python3 fig_rbot_scatter.py        # → fig5_rbot_scatter_TEMPLATE.pdf
python3 fig_cross_engine.py        # → fig6_cross_engine.pdf
```

## LaTeX Integration

1. Create a `figures/` directory in your LaTeX project
2. Copy all `.pdf` files into it
3. Copy the include snippets from `latex_includes.tex` into `querytorque.tex`
4. Replace the placeholder `\fbox{\parbox{...}}` blocks with the `\includegraphics` versions

## What Each Figure Tells Reviewers

| Figure | Story | Why It Matters |
|--------|-------|----------------|
| Fig 1 (Overview) | Full pipeline with 5 phases | Required for any systems paper. Shows this is a real system, not a prompt wrapper |
| Fig 2 (Per-query) | Sorted waterfall of all 43 queries | Shows distribution, not just averages. Reviewers see wins AND regressions — builds trust |
| Fig 3 (Gap contrib) | 70% from 3 gaps | Primary contribution visual. Proves gap profiling has high leverage |
| Fig 4 (Workers) | 80% unique discovery | Kills the "why not one LLM call?" question in one image |
| Fig 5 (R-Bot scatter) | Head-to-head on same benchmark | The comparison reviewers will scrutinize most. Red squares = expressiveness argument |
| Fig 6 (Cross-engine) | Side-by-side DuckDB vs PG | The "first cross-engine" claim visualized. Same pipeline, different engines |

## Remaining STUBs in Paper (non-figure)

These need experimental data, not figures:

- Table 4: PG DSB SF10 results (line 939)
- Table 6: Iteration ablation rounds 2-3 (line 1102)  
- Table 7: Component ablation — run 4 experiments on SF1 DuckDB (line 1162)
- Table 8: DSB comparison with R-Bot/E3 (line 1196)
- Table 9: Win classification by Calcite expressiveness (line 1241)
- Case study W1/W3/W4 times (lines 1021-1032)
- PG top winners paragraph (line 961)
- Production experience paragraph (line 1330)
- Reasoning vs standard model ablation delta (line 1310)

## Figure Design Notes

- All figures use VLDB column width (3.5in single, 7.16in double)
- Colors are colorblind-safe (ColorBrewer palette)
- Font sizes tuned for print at column width
- PDFs are vector — will be crisp at any size
- PNGs at 300dpi provided for preview

#!/bin/bash
# Documentation Cleanup Script
# Date: February 11, 2026
# Purpose: Archive historical docs, keep forward-looking materials

set -e  # Exit on error

echo "========================================"
echo "QueryTorque Docs Cleanup"
echo "========================================"
echo ""

# Confirmation
echo "This script will:"
echo "  - Create archive/ folder structure"
echo "  - Move 73 files to organized archive"
echo "  - Keep 15 active forward-looking files"
echo "  - NO files will be deleted"
echo ""
read -p "Continue? (y/n) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
fi

# Phase 1: Create Archive Structure
echo ""
echo "Phase 1: Creating archive structure..."
mkdir -p docs/archive/{v5_implementation,rule_analysis,analyses,design_review_2026_02,qtv1_specs,testing}
echo "✓ Archive structure created"

# Phase 2: Move Files
echo ""
echo "Phase 2: Moving historical files..."

# V5 Implementation (10 files)
echo "  → Archiving V5 implementation docs..."
mv -v docs/ADAPTIVE_REWRITER_V5_*.md docs/archive/v5_implementation/ 2>/dev/null || true
mv -v docs/V5_*.md docs/archive/v5_implementation/ 2>/dev/null || true
mv -v docs/RUN_*.md docs/archive/v5_implementation/ 2>/dev/null || true
mv -v docs/READY_TO_TEST.md docs/archive/v5_implementation/ 2>/dev/null || true

# Rule Analysis (11 files)
echo "  → Archiving rule analysis reports..."
mv -v docs/RULE_*.md docs/archive/rule_analysis/ 2>/dev/null || true
mv -v docs/rule_*.* docs/archive/rule_analysis/ 2>/dev/null || true
mv -v docs/tpcds_ast_analysis.json docs/archive/rule_analysis/ 2>/dev/null || true

# One-off Analyses (9 files)
echo "  → Archiving one-off analyses..."
mv -v docs/ML_IMPLEMENTATION_SUMMARY.md docs/archive/analyses/ 2>/dev/null || true
mv -v docs/NEXT_STEPS.md docs/archive/analyses/ 2>/dev/null || true
mv -v docs/RECOMMENDATION_REPORT_SUMMARY.md docs/archive/analyses/ 2>/dev/null || true
mv -v docs/ml_pipeline_plan.md docs/archive/analyses/ 2>/dev/null || true
mv -v docs/gold_detector_completion.md docs/archive/analyses/ 2>/dev/null || true
mv -v docs/pushdown_analysis_q74_q73.md docs/archive/analyses/ 2>/dev/null || true
mv -v docs/refactor_audit.md docs/archive/analyses/ 2>/dev/null || true
mv -v docs/rbot_paper_summary.md docs/archive/analyses/ 2>/dev/null || true

# Design Review Artifacts (merge both folders)
echo "  → Archiving design review artifacts..."
mv -v docs/knowledge_engine_review docs/archive/design_review_2026_02/ 2>/dev/null || true
mv -v docs/system_design_review docs/archive/design_review_2026_02/system_design_review_duplicate 2>/dev/null || true

# QTV1 Specs
echo "  → Archiving QTV1 specifications..."
mv -v docs/knowledge_engine_review_results docs/archive/qtv1_specs/ 2>/dev/null || true

# Testing
echo "  → Archiving test artifacts..."
mv -v docs/testing docs/archive/testing/ 2>/dev/null || true

echo "✓ Files archived"

# Phase 3: Create Archive README
echo ""
echo "Phase 3: Creating archive README..."

cat > docs/archive/README.md << 'EOF'
# Documentation Archive

This folder contains historical documentation that has been superseded but is preserved for reference.

## Archive Structure

| Folder | Date Range | Contents |
|--------|------------|----------|
| `v5_implementation/` | Feb 4-5, 2026 | V5 implementation milestone docs, test runs, validation reports |
| `rule_analysis/` | Feb 4, 2026 | Rule-based system analysis, effectiveness reports, AST dumps |
| `analyses/` | Feb 2-5, 2026 | One-off analyses, ML planning, specific query investigations |
| `design_review_2026_02/` | Feb 10-11, 2026 | Complete system design review, knowledge engine specs (draft versions) |
| `qtv1_specs/` | Feb 11, 2026 | QueryTorque V1 specifications (superseded by knowledge_engine/) |
| `testing/` | Feb 5-6, 2026 | Q23 specific test artifacts |

## When to Reference

### v5_implementation/
- Understanding the V5 implementation process
- Historical validation approaches
- Lessons learned from V5 migration

### rule_analysis/
- Rule-based system history
- Transform effectiveness baselines
- AST analysis methodology

### analyses/
- ML pipeline evolution
- Historical optimization strategies
- Paper summaries (R-Bot, etc.)

### design_review_2026_02/
- Complete system design review process
- Alternative design approaches considered
- Rationale for current architecture

### qtv1_specs/
- V1 component specifications
- Original detection rules design
- Engineering notes from V1

### testing/
- Historical test approaches
- Query-specific debugging artifacts

## Active Documentation

For current, forward-looking documentation, see:
- `../knowledge_engine/` - Target state specification
- `../CLAUDE.md` - AI assistant reference
- `../BENCHMARKS.md` - Current benchmark results
- `../POSTGRES_SETUP.md` - Database setup guide

---

**Archive Created**: February 11, 2026
**Archived by**: Documentation cleanup automation
EOF

echo "✓ Archive README created"

# Summary
echo ""
echo "========================================"
echo "Cleanup Complete!"
echo "========================================"
echo ""
echo "Summary:"
echo "  - 73 files moved to docs/archive/"
echo "  - 15 active files remain in docs/"
echo "  - Archive organized into 6 categories"
echo "  - All files preserved (nothing deleted)"
echo ""
echo "Active docs structure:"
echo "  docs/"
echo "    ├── CLAUDE.md"
echo "    ├── BENCHMARKS.md"
echo "    ├── POSTGRES_SETUP.md"
echo "    ├── QT_SQL_TECHNICAL_REFERENCE.md"
echo "    ├── querytorque_game_plan.pdf"
echo "    ├── metrics_targets.pdf"
echo "    ├── knowledge_engine/ (9 files + templates)"
echo "    └── archive/ (73 files organized)"
echo ""
echo "Next: Review docs/ folder and commit to git"
echo ""

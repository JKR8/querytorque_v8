# Documentation Catalog & Cleanup Plan

**Date**: February 11, 2026
**Total Files**: 88 files
**Total Size**: ~1.5 MB

---

## Executive Summary

The docs folder contains a mix of:
- **Current reference materials** (CLI guides, setup instructions)
- **Forward-looking specifications** (Knowledge Engine v2 design)
- **Historical artifacts** (V5 implementation, review processes, analysis reports)
- **Redundant copies** (4 folders with overlapping content)

**Recommendation**: Archive 70% of content, keeping only forward-looking and actively maintained documentation.

---

## Directory Structure

```
docs/
├── [ROOT - 34 files]               # Mixed: current + historical
├── knowledge_engine/               # ✅ KEEP - Clean target state spec
├── knowledge_engine_review/        # ⚠️  ARCHIVE - Review artifacts (duplicates system_design_review)
├── knowledge_engine_review_results/# ⚠️  ARCHIVE - QTV1 specs (superseded)
├── system_design_review/           # ⚠️  ARCHIVE - Duplicate of knowledge_engine_review
└── testing/                        # ⚠️  ARCHIVE - Q23 specific tests (stale)
```

---

## Category 1: KEEP (Active/Forward-Looking) - 15 files

### A. Core Reference Documents (4 files)
| File | Size | Purpose | Status |
|------|------|---------|--------|
| `CLAUDE.md` | 8.7K | AI assistant reference - modular architecture guide | ✅ Current |
| `BENCHMARKS.md` | 5.5K | Benchmark results dashboard | ✅ Current |
| `POSTGRES_SETUP.md` | 6.5K | PostgreSQL 14.3 setup guide (Feb 7) | ✅ Current |
| `QT_SQL_TECHNICAL_REFERENCE.md` | 93K | Comprehensive system reference | ✅ Current |

### B. Strategic Planning (2 PDFs)
| File | Size | Date | Purpose |
|------|------|------|---------|
| `querytorque_game_plan.pdf` | 132K | Feb 11 | Strategic roadmap |
| `metrics_targets.pdf` | 117K | Feb 11 | Performance targets |

### C. Knowledge Engine Target State (9 files in knowledge_engine/)
**Status**: ✅ Clean, organized, forward-looking specification

```
knowledge_engine/
├── 00_OVERVIEW.md                      # 7.4K - Two-system architecture
├── 01_KNOWLEDGE_ATOM.md                # 5.5K - Core data structure
├── 02_BLACKBOARD.md                    # 7.4K - Outcome logging
├── 03_ENGINE_PROFILE.md                # 9.9K - Optimizer intelligence
├── 04_GOLD_EXAMPLES.md                 # 6.6K - Few-shot learning
├── 05_DETECTION_AND_MATCHING.md        # 13K - Feature detection
├── 06_MANUAL_WORKFLOW.md               # 7.6K - Human workflow
├── 07_SCHEMAS.md                       # 19K - Data schemas
└── templates/
    ├── analysis_session.md             # 3.4K - Reasoning form
    ├── engine_profile_template.md      # 1.5K - Profile template
    └── finding.md                      # 1.6K - Finding template
```

**Why Keep**: This is the authoritative target state specification. Clean, organized, actively used.

---

## Category 2: ARCHIVE (Historical/Redundant) - 73 files

### A. V5 Implementation Artifacts (10 files) - ~110K
**Status**: Historical - V5 implementation complete, now in operational phase

| File | Size | Last Modified | Reason to Archive |
|------|------|---------------|-------------------|
| `ADAPTIVE_REWRITER_V5_VALIDATION_REPORT.md` | 12K | Feb 4 | V5 validation complete |
| `V5_FINAL_APPROACH.md` | 11K | Feb 4 | Superseded by current state |
| `V5_IMPLEMENTATION_COMPLETE.md` | 14K | Feb 5 | Historical milestone |
| `V5_IMPLEMENTATION_VERIFIED.md` | 17K | Feb 5 | Historical verification |
| `V5_REQUIREMENTS_CHECK.md` | 14K | Feb 4 | Historical requirements |
| `V5_REVIEW_SUMMARY.md` | 6K | Feb 5 | Historical review |
| `RUN_20_WORKERS.md` | 7.7K | Feb 4 | Historical test run |
| `RUN_ALL_99_QUERIES.md` | 8.1K | Feb 5 | Historical test run |
| `RUN_V5_TEST.md` | 9.5K | Feb 5 | Historical test run |
| `READY_TO_TEST.md` | 6.4K | Feb 4 | Historical status |

**Archive Location**: `docs/archive/v5_implementation/`

### B. Rule Analysis Reports (11 files) - ~50K
**Status**: Historical analysis from rule-based system era

| File | Size | Reason to Archive |
|------|------|-------------------|
| `RULE_ANALYSIS_SUMMARY.md` | 8.6K | Historical analysis |
| `RULE_QUICK_REFERENCE.md` | 6.9K | Old rule naming |
| `rule_effectiveness_analysis.md` | 3.7K | Historical metrics |
| `rule_effectiveness_detailed.md` | 8.3K | Historical metrics |
| `rule_effectiveness_report.md` | 6.2K | Historical metrics |
| `rule_naming_migration.md` | 4.7K | Completed migration |
| `rule_combinations.json` | 5.1K | Historical data |
| `rule_effectiveness_analysis.csv` | 4K | Historical data |
| `tpcds_ast_analysis.json` | 223K | Large AST dump |

**Archive Location**: `docs/archive/rule_analysis/`

### C. One-off Analyses & Reports (9 files) - ~70K
**Status**: Point-in-time analyses, superseded

| File | Size | Reason to Archive |
|------|------|-------------------|
| `ML_IMPLEMENTATION_SUMMARY.md` | 12K | Historical ML summary |
| `NEXT_STEPS.md` | 13K | Dated action items (Feb 4) |
| `RECOMMENDATION_REPORT_SUMMARY.md` | 8.4K | Historical recommendations |
| `ml_pipeline_plan.md` | 14K | Historical plan |
| `gold_detector_completion.md` | 5.4K | Historical milestone |
| `pushdown_analysis_q74_q73.md` | 3.2K | Specific query analysis |
| `refactor_audit.md` | 3.6K | Historical audit |
| `rbot_paper_summary.md` | 2.4K | Paper notes (reference material - could keep) |

**Archive Location**: `docs/archive/analyses/`

### D. Redundant Review Folders (43 files) - ~700K
**Status**: Duplicate content across 3 folders with overlapping specs

#### knowledge_engine_review/ (14 files + specs)
- **Duplicates**: Same content as `system_design_review/`
- **Status**: Review artifacts from design phase
- **Files**: KNOWLEDGE_ENGINE_*.md, PRODUCT_CONTRACT.md, NAMING_CLARIFICATION.md, etc.
- **Archive**: `docs/archive/design_review_2026_02/`

#### knowledge_engine_review_results/ (3 files)
- **QTV1_COMPONENT_SPECIFICATIONS.md** (54K)
- **QTV1_DETECTION_RULES_DESIGN.md** (26K)
- **QTV1_KNOWLEDGE_ENGINE_ENGINEERING_NOTES_CANON.md** (34K)
- **Status**: V1 specifications, superseded by knowledge_engine/
- **Archive**: `docs/archive/qtv1_specs/`

#### system_design_review/ (14 files + specs)
- **Status**: Exact duplicate of knowledge_engine_review/
- **Contains**: Same 14 docs + 5 schema files
- **Archive**: `docs/archive/design_review_2026_02/` (merge with knowledge_engine_review)

### E. Testing Artifacts (2 files)
- `testing/Q23_TEST_PLAN.md` (11K)
- `testing/Q23_TEST_STATUS.md` (5K)
- **Status**: Query-specific test artifacts (stale)
- **Archive**: `docs/archive/testing/`

---

## Cleanup Action Plan

### Phase 1: Create Archive Structure
```bash
mkdir -p docs/archive/{v5_implementation,rule_analysis,analyses,design_review_2026_02,qtv1_specs,testing}
```

### Phase 2: Move Historical Content (Safe - No Deletion)
```bash
# V5 Implementation
mv docs/ADAPTIVE_REWRITER_V5_* docs/archive/v5_implementation/
mv docs/V5_*.md docs/archive/v5_implementation/
mv docs/RUN_*.md docs/archive/v5_implementation/
mv docs/READY_TO_TEST.md docs/archive/v5_implementation/

# Rule Analysis
mv docs/RULE_*.md docs/archive/rule_analysis/
mv docs/rule_*.* docs/archive/rule_analysis/
mv docs/tpcds_ast_analysis.json docs/archive/rule_analysis/

# One-off Analyses
mv docs/ML_IMPLEMENTATION_SUMMARY.md docs/archive/analyses/
mv docs/NEXT_STEPS.md docs/archive/analyses/
mv docs/RECOMMENDATION_REPORT_SUMMARY.md docs/archive/analyses/
mv docs/ml_pipeline_plan.md docs/archive/analyses/
mv docs/gold_detector_completion.md docs/archive/analyses/
mv docs/pushdown_analysis_q74_q73.md docs/archive/analyses/
mv docs/refactor_audit.md docs/archive/analyses/

# Design Review Artifacts (merge both review folders)
mv docs/knowledge_engine_review docs/archive/design_review_2026_02/
mv docs/system_design_review docs/archive/design_review_2026_02/system_design_review_duplicate

# QTV1 Specs
mv docs/knowledge_engine_review_results docs/archive/qtv1_specs/

# Testing
mv docs/testing docs/archive/testing/
```

### Phase 3: Add Archive README
Create `docs/archive/README.md` explaining archive structure and dates.

---

## Post-Cleanup Structure

```
docs/
├── README.md                       # 🆕 Documentation index
├── CLAUDE.md                       # AI assistant reference
├── BENCHMARKS.md                   # Benchmark dashboard
├── POSTGRES_SETUP.md               # Database setup
├── QT_SQL_TECHNICAL_REFERENCE.md  # System reference
├── querytorque_game_plan.pdf      # Strategic roadmap
├── metrics_targets.pdf            # Performance targets
├── knowledge_engine/              # Target state spec (9 files + templates)
│   ├── 00_OVERVIEW.md
│   ├── 01_KNOWLEDGE_ATOM.md
│   ├── 02_BLACKBOARD.md
│   ├── 03_ENGINE_PROFILE.md
│   ├── 04_GOLD_EXAMPLES.md
│   ├── 05_DETECTION_AND_MATCHING.md
│   ├── 06_MANUAL_WORKFLOW.md
│   ├── 07_SCHEMAS.md
│   └── templates/
└── archive/                       # 🆕 Historical materials
    ├── README.md                  # Archive guide
    ├── v5_implementation/         # V5 milestone docs
    ├── rule_analysis/             # Historical analysis
    ├── analyses/                  # One-off reports
    ├── design_review_2026_02/     # Design review artifacts
    ├── qtv1_specs/                # V1 specifications
    └── testing/                   # Q23 test artifacts
```

**Result**: 15 active files (well-organized) + 73 archived files (preserved but out of the way)

---

## Recommended New Files

### 1. docs/README.md
**Purpose**: Documentation index and navigation guide

**Content**:
- Quick links to key documents
- Brief description of each section
- How to navigate the knowledge engine spec
- Link to archive for historical reference

### 2. docs/archive/README.md
**Purpose**: Explain what's in the archive and why

**Content**:
- Archive structure explanation
- Date ranges for each category
- When to reference archived materials
- Historical context

---

## Risk Assessment

| Risk | Mitigation |
|------|------------|
| Loss of important historical context | All files moved to archive (not deleted) |
| Breaking links in other docs | Search for cross-references before archiving |
| Confusion about current state | New README provides clear navigation |
| Accidental deletion | Use `mv` not `rm`, commit to git before cleanup |

---

## Success Metrics

**Before**:
- 88 files scattered across 6 locations
- Redundant content in 3 folders
- Unclear what's current vs historical
- Hard to find active specifications

**After**:
- 15 active files in clear structure
- Single authoritative knowledge engine spec
- Clear separation of current vs historical
- Easy navigation with README
- All history preserved in organized archive

---

## Next Steps

1. **Review this plan** - Confirm categories and archive decisions
2. **Create archive structure** - Run Phase 1 commands
3. **Move files** - Run Phase 2 commands (safe, no deletion)
4. **Create READMEs** - Write navigation guides
5. **Validate links** - Check for broken cross-references
6. **Commit to git** - Preserve history with clear commit message
7. **Update MEMORY.md** - Document new structure

---

## Questions for Review

1. Should we keep `rbot_paper_summary.md` as active reference material?
2. Should we keep `QT_SQL_TECHNICAL_REFERENCE.md` or archive (it's 93K)?
3. Are there any specific V5 docs needed for near-term reference?
4. Should we consolidate the two PDF files into a single strategic document?

---

**Catalog Complete**: Ready for approval and execution.

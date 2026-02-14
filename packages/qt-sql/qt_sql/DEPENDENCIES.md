# qt_sql Dependency Map

## Knowledge Engine -> Prompt Sections

```
KNOWLEDGE ENGINE                          PROMPT SECTIONS
===============                          ===============

constraints/engine_profile_{dialect}.json
+-- strengths[] --------------------------> Section III "Handles Well" table
+-- gaps[] -------------------------------> Section III "Blind Spots" table
|   +-- detect, gates -------------------> Section VII.A case detection rules
|   +-- what_worked[] -------------------> Section VII.A treatments
|   +-- what_didnt_work[] ---------------> Section VII.A failures + VII.C registry
|   +-- field_notes[] -------------------> Section VII.A inline notes
+-- Loaded by: prompter._load_engine_profile()

knowledge/{dialect}.md
+-- ENGINE STRENGTHS ---------------------> (redundant with engine_profile)
+-- GLOBAL GUARDS ------------------------> Section VII.F verification checklist
+-- DOCUMENTED CASES P0-P9 ---------------> Section VII.A (same data as engine_profile gaps)
+-- PRUNING GUIDE ------------------------> Section VII.E what doesn't apply
+-- REGRESSION REGISTRY ------------------> Section VII.C detailed table
+-- Loaded by: prompter.load_exploit_algorithm()

knowledge/transforms.json (27 entries)
+-- detection.py::detect_transforms() ----> Section VII.D structural matches
    +-- tag_index.py::extract_precondition_features()

examples/{dialect}/*.json (gold + regression)
+-- tag_index.py -> models/similarity_tags.json
    +-- knowledge/__init__.py::TagRecommender
        +-- find_similar_examples() ------> Section VII.B transform->example mapping
        +-- find_relevant_regressions()

constraints/*.json (constraint files)
+-- prompter._load_constraint_files() ----> Section IV constraints
```

## Module Import Tree

```
CLI (qt_sql.cli)
  +-- pipeline.py (ORCHESTRATOR)
        +-- schemas.py (data classes)
        +-- store.py (Store)
        +-- learn.py (Learner)
        +-- generate.py (CandidateGenerator)
        +-- analyst_session.py (V1 session)
        +-- dag.py (logical tree)
        +-- prompter.py
        |     +-- loads: knowledge/{dialect}.md
        |     +-- loads: constraints/engine_profile_{dialect}.json
        |     +-- loads: constraints/*.json
        +-- sql_rewriter.py
        +-- validate.py
        +-- tag_index.py
        |     +-- loads: examples/{dialect}/*.json
        +-- detection.py
        |     +-- loads: knowledge/transforms.json
        +-- explain_signals.py
        +-- qerror.py
        +-- sessions/
              +-- base_session.py (abstract base)
              +-- swarm_session.py (DEFAULT)
              |     +-- prompts/v2_analyst_briefing.py (Sections I-VII)
              |     +-- prompts/v2_worker.py
              |     +-- prompts/v2_swarm_parsers.py
              |     +-- prompts/v2_briefing_checks.py
              |     +-- prompts/swarm_fan_out.py
              |     +-- prompts/swarm_snipe.py
              |     |     +-- prompts/analyst_briefing.py (V1 utilities)
              |     |     +-- prompts/worker.py (V1 utilities)
              |     |     +-- prompts/briefing_checks.py (V1 utilities)
              |     +-- generate.py (CandidateGenerator)
              +-- expert_session.py
              |     +-- analyst_session.py
              +-- oneshot_session.py
```

## V1 vs V2 Prompt Modules

The codebase maintains two prompt generations:

| Module | Version | Used By |
|--------|---------|---------|
| `prompts/analyst_briefing.py` | V1 | `config_boost.py`, `swarm_snipe.py`, `pipeline.py` |
| `prompts/worker.py` | V1 | `swarm_snipe.py` |
| `prompts/briefing_checks.py` | V1 | `swarm_snipe.py`, `analyst_briefing.py` |
| `prompts/swarm_common.py` | V1 | `prompts/__init__.py` |
| `prompts/swarm_parsers.py` | V1 | `prompts/__init__.py` |
| `prompts/v2_analyst_briefing.py` | V2 | `swarm_session.py` |
| `prompts/v2_worker.py` | V2 | `swarm_session.py` |
| `prompts/v2_swarm_parsers.py` | V2 | `swarm_session.py` |
| `prompts/v2_briefing_checks.py` | V2 | `swarm_session.py` |

V1 modules are still actively used by the snipe pipeline (`swarm_snipe.py`).

## Archived Files

| File | Original Location | Reason |
|------|-------------------|--------|
| `_archive/script_parser.py` | `qt_sql/script_parser.py` | Only imported by non-production `samples/generate_sample.py` |
| `_archive/prompt_samples/` | `qt_sql/prompts/samples/` | Non-production demo scripts and rendered prompt examples |

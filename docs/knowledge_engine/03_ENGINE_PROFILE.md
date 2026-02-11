# Engine Profile: Markdown-Native Optimizer Intelligence

> **Status**: Target state specification
> **Current**: `constraints/engine_profile_duckdb.json` (12KB, ~3000 tokens), `constraints/engine_profile_postgresql.json` (16KB, ~4000 tokens)
> **Target**: Markdown files loaded and injected into analyst prompt as-is. What you write = what the LLM reads.
> **Injected at**: `analyst_briefing.py` lines 996-1064

---

## The Problem With JSON

The current engine profile goes through a lossy double-translation:

```
You write JSON → analyst_briefing.py formats to markdown → LLM reads markdown
```

Three problems:
1. **JSON is token-wasteful.** Keys, brackets, quotes — pure overhead for what the LLM reads. ~40% overhead vs equivalent structured markdown.
2. **The formatter silently truncates.** `what_worked` capped to 4, `what_didnt_work` to 3. You add a 5th example — silently dropped.
3. **Schema and rendering drift apart.** You add a JSON field, forget to update the formatter, field is stored but invisible to the LLM.

## The Fix: Markdown-Native

**Two artifacts per profile:**

```
constraints/engine_profile_duckdb.md       ← What the LLM reads (what you author)
constraints/engine_profile_duckdb.schema   ← Validates structure (for tooling)
```

The markdown IS the profile. The schema validates it. No translation. No truncation.

---

## Profile Structure

See `templates/engine_profile_template.md` for the blank template. Here's the structure:

### Header
```markdown
## DuckDB Optimizer Profile (v1.1+, 88 TPC-DS queries SF1-SF10)

Field intelligence. Use to guide analysis, apply your own judgment.
```

**Requirements**: Engine name, version, benchmark source, entry count. 1-2 sentence briefing note.

### Strengths Table
```markdown
### Strengths — DO NOT fight these

| ID | Summary | Note |
|----|---------|------|
| INTRA_SCAN_PUSHDOWN | WHERE filters push into SEQ_SCAN at scan time | If EXPLAIN shows filter inside scan node, don't CTE it |
| SAME_COLUMN_OR | Same-column OR handled in single scan | Splitting: 0.59x Q90, 0.23x Q13 (9 branches) |
```

**Requirements**: SCREAMING_SNAKE ID, 1-sentence summary, tactical field note with evidence. Table format — compact, high signal density.

### Gap Blocks
```markdown
### Gaps — Hunt for these

#### [HIGH] CROSS_CTE_PREDICATE_BLINDNESS
**What**: Cannot push predicates from outer query into CTE definitions.
**Why**: CTEs planned as independent subplans, no data lineage tracing.
**Hunt**: Move selective predicates INTO the CTE. Pre-filter dims/facts before materialization.

Won: Q6 4.00x (date→CTE) · Q63 3.77x (pre-join filtered dates) · Q93 2.97x (dim filter before LEFT JOIN) · Q26 1.93x (all dims pre-filtered)
Lost: Q25 0.50x (31ms baseline, CTE overhead) · Q31 0.49x (over-decomposed)

Rules:
- Check EXPLAIN: filter AFTER large scan/join → push earlier via CTE
- Fast queries (<100ms): CTE overhead can negate savings
- ~35% of all wins exploit this. Most reliable on star-join + late dim filters
- Unfiltered CTEs = pure overhead. Always include WHERE
- NEVER CROSS JOIN 3+ dim CTEs — Q80 hit 0.0076x (132x slower)
- Limit cascading fact CTEs to 2 levels — Q4 hit 0.78x from triple chain
- Remove orphaned CTEs — they still materialize
```

**Requirements per gap**:
- Priority: HIGH, MEDIUM, or LOW
- ID: SCREAMING_SNAKE, unique
- **What**: 1 sentence — what the optimizer fails to do (falsifiable)
- **Why**: 1 sentence — internal mechanism
- **Hunt**: 1 sentence — what the worker should try
- **Won**: query ID + speedup + technique (all entries, no truncation)
- **Lost**: query ID + regression + why it failed (all entries, no truncation)
- **Rules**: Actionable, condition-scoped field notes. At minimum:
  - 1 diagnostic (what EXPLAIN signal to look for)
  - 1 safety rule (what NOT to do, with evidence)

### SET LOCAL Config Intel (PG only)
```markdown
### SET LOCAL Config Intel

Config is ADDITIVE to SQL rewrite, not a substitute.

| Rule | Trigger (EXPLAIN signal) | Config | Evidence | Risk |
|------|--------------------------|--------|----------|------|
| SORT_SPILL | Sort Space = 'Disk' | work_mem: ≤2 ops→1G, 3-5→512M, 6-10→256M | Q100: 6.82x | LOW |
| JIT_OVERHEAD | JIT >5% exec or >500ms | jit = 'off' | Q010: 1.07x | LOW |
| FORCED_PARALLEL | NEVER on <500ms | Do NOT set max_parallel_workers | Q039: 7.34x REGRESSION | CRITICAL |

Key findings:
- work_mem for sort spills is the single biggest config lever (6.8x)
- Forced parallelism is DANGEROUS on fast queries — use cost reduction instead
```

### Footer
```markdown
### Scale Warning

PostgreSQL optimizations validated at SF5 do NOT reliably predict SF10.

---

**Profile version**: 2026.02.11-v3
**Last validated**: 2026-02-11
**Analysis sessions**: AS-DUCK-001, AS-DUCK-002, AS-DUCK-003
```

---

## Token Budget

| Format | DuckDB Est. Tokens | PG Est. Tokens |
|--------|-------------------|----------------|
| Current JSON (raw) | ~3,000 | ~4,100 |
| Current formatted markdown | ~2,200 | ~3,500 |
| **Target markdown-native** | **~1,600** | **~2,400** |

27-30% reduction with zero information loss. Actually MORE information — no truncation.

### Analyst Prompt Token Budget

Based on measured prompt sizes:

| Section | Tokens | % of Total |
|---------|--------|------------|
| Role + task framing | ~100 | 1% |
| Query SQL + EXPLAIN | ~2,000-5,000 | 20-30% |
| Matched examples | ~2,000-4,000 | 15-25% |
| **Engine profile** | **~1,600-2,400** | **10-15%** |
| Global knowledge | ~300 | 2% |
| Constraints + output format | ~2,000 | 12% |
| Transform catalog + strategy | ~2,500 | 15% |
| **Total** | **~12,000-18,000** | |

### Prompt Position Strategy

LLMs weigh beginning and end more heavily ("lost in the middle" effect). The engine profile should be positioned early:

```
[BEGINNING — high attention]
  Role + task framing
  Engine profile                    ← MOVE HERE (currently in middle)

[MIDDLE — lower attention]
  Query SQL
  EXPLAIN plan
  Matched examples
  Global knowledge

[END — high attention]
  Output format
  Constraints + checklist
```

This requires a minor change to `analyst_briefing.py` — move the engine profile section before the query SQL section.

---

## Schema Validation

The schema validates the **structure of the markdown**, not JSON:

```python
@dataclass
class EngineProfileStructure:
    """Validates that an engine profile markdown has all required sections."""
    engine: str                                     # "duckdb" | "postgresql"
    version_tested: str                             # "1.1+"
    strengths: list[StrengthEntry]                  # 5-10 entries
    gaps: list[GapEntry]                            # 3-8 entries
    set_local_intel: list[ConfigRule] | None        # PG only
    profile_version: str                            # "YYYY.MM.DD-vN"
    last_validated: str                             # "YYYY-MM-DD"

@dataclass
class GapEntry:
    id: str                                         # SCREAMING_SNAKE, unique
    priority: Literal["HIGH", "MEDIUM", "LOW"]
    what: str                                       # 1 sentence, falsifiable
    why: str                                        # 1 sentence, mechanism
    hunt: str                                       # 1 sentence, actionable
    won: list[WonLost]                              # query + speedup + technique
    lost: list[WonLost]                             # query + regression + why
    rules: list[str]                                # condition-scoped field notes
    # Validation: rules must contain at least 1 diagnostic + 1 safety

@dataclass
class WonLost:
    query: str                                      # "Q88"
    speedup: float                                  # 6.28 or 0.23
    technique: str                                  # brief description

@dataclass
class StrengthEntry:
    id: str                                         # SCREAMING_SNAKE
    summary: str                                    # 1 sentence
    note: str                                       # tactical field note

@dataclass
class ConfigRule:
    id: str
    trigger: str                                    # EXPLAIN signal
    config: str                                     # SET LOCAL statement
    evidence: str                                   # query + speedup
    risk: Literal["LOW", "MEDIUM", "HIGH", "CRITICAL"]
```

A CLI tool (`qt validate-profile duckdb`) parses the markdown and validates against this structure. If you forget a `Rules:` section on a gap, validation catches it.

---

## How to Update the Profile

1. Run a benchmark batch
2. Review blackboard outcomes in an analysis session (`templates/analysis_session.md`)
3. Record findings and decide on profile updates
4. Edit `constraints/engine_profile_{dialect}.md` directly
5. Run `qt validate-profile {dialect}` to check structure
6. Commit with analysis session ID in message

### What You Can Do
- Add/remove/edit any gap, strength, or config rule
- Rewrite any field note or rule
- Reorder gaps by priority
- Add Won/Lost entries with evidence
- Change priority on any gap

### Quality Standard

The 7 rules on `CROSS_CTE_PREDICATE_BLINDNESS` are the gold standard:

1. **Condition-scoped**: Each rule starts with when/what/if
2. **Evidence-backed**: References specific queries and speedups
3. **Actionable**: Tells the worker exactly what to do or not do
4. **Diagnostic**: Provides the EXPLAIN signal that confirms the opportunity
5. **Failure-aware**: Documents what didn't work and why

Every gap must have at least one diagnostic rule and one safety rule.

---

## Migration from JSON

1. Convert existing JSON profiles to markdown format (one-time)
2. Update `_load_engine_profile()` in `prompter.py:86` to load `.md` instead of `.json`
3. Update `analyst_briefing.py` to inject markdown directly (remove the JSON-to-markdown formatter)
4. Add `qt validate-profile` CLI command
5. Existing JSON files kept as archive

The formatter code in `analyst_briefing.py:996-1064` becomes a simple text injection — load the file, inject it. No field-by-field formatting.

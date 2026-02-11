# Manual Workflow: Human-Driven Knowledge Refinement

> **Status**: Target state specification
> **Replaces**: `06_COMPRESSION_PIPELINE.md` (autonomous LLM compression — killed)
> **Templates**: `templates/analysis_session.md`, `templates/finding.md`, `templates/gold_example_template.json`, `templates/engine_profile_template.md`

---

## Design Principle

You do all the reasoning. The system provides structured inputs (blackboard) and validated outputs (engine profile, gold examples, detection rules). Every knowledge artifact has a schema, a template, and a validation step.

No LLM in the knowledge engine. No autonomous promotion. No compression pipeline. You review outcomes, you identify patterns, you update the profile.

---

## The Workflow

```
1. Pipeline runs, captures outcomes to blackboard (automatic)
         ↓
2. You review a blackboard batch (manual)
         ↓
3. You fill in an Analysis Session form (manual, structured)
    → Findings: observations about optimizer behavior
    → Actions: profile updates, gold example promotions
         ↓
4. You update artifacts (manual, schema-validated)
    → Engine profile markdown
    → Gold example JSON
    → Detection rule JSON
         ↓
5. You validate artifacts (tooling)
    → qt validate-profile {dialect}
    → qt validate-example {id}
    → qt validate-rules {dialect}
         ↓
6. Next pipeline run reads updated knowledge (automatic)
```

---

## Step 1: Pipeline Captures Outcomes

Automatic. After a benchmark run completes:

```bash
qt blackboard build {bench_dir}
```

This runs the 3-phase extraction pipeline (`build_blackboard.py`), producing JSONL in `data/layer1/{engine}_{benchmark}/{date}/outcomes.jsonl`.

**What you get**: One `BlackboardEntry` per optimization attempt. Status, speedup, transforms, worker reasoning, EXPLAIN evidence. Raw data, no interpretation.

---

## Step 2: Review a Blackboard Batch

Open the blackboard output. Look for:

1. **WINs and CRITICAL_HITs** — what worked and why?
2. **REGRESSIONs** — what failed and why?
3. **Patterns** — do multiple queries show the same behavior?
4. **Surprises** — anything unexpected?

Tools for review:

```bash
# Summary stats
qt leaderboard {bench_dir}

# Filter to wins only
qt blackboard query --status WIN --engine duckdb

# View specific entry
qt blackboard show q88_w2_20260211
```

---

## Step 3: Fill In Analysis Session

Copy `templates/analysis_session.md` into your analysis sessions directory:

```
data/analysis_sessions/{engine}/{AS-ENGINE-NNN}.md
```

Fill in:

### Batch Summary
Quick stats — total entries, WINs, CRITICAL_HITs, regressions, top speedup, worst regression.

### Findings
For each observation worth recording:

1. **Claim** — one falsifiable sentence about optimizer behavior
2. **Evidence** — table of queries that support/contradict with speedups
3. **Mechanism** — WHY the optimizer behaves this way
4. **Boundary conditions** — when it applies, when it doesn't, diagnostic EXPLAIN signal
5. **Confidence** — high/medium/low with rationale

### Actions
For each finding, decide:
- Update existing gap (add Won/Lost/Rule)
- Propose new gap
- Add new strength
- Promote gold example
- No action (with reason)

Record the exact text you'll add to the profile.

### Gold Example Drafts
If promoting examples, draft the 4-part explanation (what/why/when/when_not) in the session before creating the full JSON.

---

## Step 4: Update Artifacts

### Engine Profile

Edit `constraints/engine_profile_{dialect}.md` directly. This is the markdown file the LLM reads.

Common updates:
- Add `Won:` entry with query ID, speedup, technique
- Add `Lost:` entry with query ID, regression, reason
- Add or modify a Rule (condition-scoped field note)
- Change gap priority based on new evidence
- Add a new gap or strength

### Gold Example

Copy `templates/gold_example_template.json` to `qt_sql/examples/{dialect}/{id}.json`. Fill in all fields, especially:

- `explanation.what/why/when/when_not` — the 4-part explanation you wrote in the analysis session
- `demonstrates_gaps` — which profile gaps this example exploits
- `outcome` — validated speedup, timing, confidence

### Detection Rule

Create or update `constraints/detection_rules/{dialect}/{GAP_ID}.json`. Write predicates over the feature vocabulary. See `05_DETECTION_AND_MATCHING.md` for format.

---

## Step 5: Validate

```bash
# Validate engine profile structure
qt validate-profile duckdb

# Validate gold example schema
qt validate-example q88_channel_bitmap

# Validate detection rules against feature vocabulary
qt validate-rules duckdb

# Run detection rules against known queries to sanity-check
qt test-rules duckdb --queries q6,q9,q88
```

Validation checks:
- Profile has all required sections (strengths table, gap blocks with What/Why/Hunt/Won/Lost/Rules)
- Every gap has at least 1 diagnostic rule and 1 safety rule
- Gold example has all required fields including 4-part explanation
- Detection rules only reference valid features and operators
- Detection rules fire on queries where the gap is known to be active

---

## Step 6: Commit and Deploy

```bash
git add constraints/engine_profile_duckdb.md
git add qt_sql/examples/duckdb/new_example.json
git add constraints/detection_rules/duckdb/NEW_GAP.json
git commit -m "AS-DUCK-003: Add REDUNDANT_SCAN rule, promote Q9 example"
```

The next pipeline run automatically picks up the updated knowledge through Interface A.

---

## Storage Layout

```
data/
├── layer1/{engine}_{benchmark}/{date}/outcomes.jsonl   # Blackboard (auto)
└── analysis_sessions/{engine}/AS-{ENGINE}-{NNN}.md     # Your reasoning (manual)

constraints/
├── engine_profile_duckdb.md                            # Engine profile (manual)
├── engine_profile_postgresql.md                        # Engine profile (manual)
└── detection_rules/
    ├── duckdb/{GAP_ID}.json                            # Detection rules (manual)
    └── postgresql/{GAP_ID}.json

qt_sql/examples/
├── duckdb/*.json                                       # Gold examples (manual)
├── duckdb/regressions/*.json                           # Regression examples (manual)
└── postgres/*.json
```

Layer 1 (blackboard) and analysis sessions are append-only — never delete, always add.
Engine profiles, gold examples, and detection rules are version-controlled — git tracks changes.

---

## Cadence

No fixed schedule. Run an analysis session when:

- You complete a benchmark batch (natural trigger)
- You observe unexpected behavior in pipeline results
- You add a new benchmark or engine
- Enough time has passed since last session that you want to consolidate learnings

Suggested rhythm: one analysis session per benchmark batch. Takes 30-60 minutes to review ~400 entries and record 3-5 findings.

---

## Artifact Summary

| Artifact | Schema | Template | Validation | Storage |
|----------|--------|----------|------------|---------|
| BlackboardEntry | `07_SCHEMAS.md` § 1 | Auto-generated | Schema check | JSONL |
| Analysis Session | `07_SCHEMAS.md` § 2 | `templates/analysis_session.md` | Sections present | Markdown |
| Finding | `07_SCHEMAS.md` § 3 | `templates/finding.md` | Required fields | Embedded in session |
| Engine Profile | `07_SCHEMAS.md` § 4 | `templates/engine_profile_template.md` | `qt validate-profile` | Markdown |
| Gold Example | `07_SCHEMAS.md` § 5 | `templates/gold_example_template.json` | `qt validate-example` | JSON |
| Detection Rule | `07_SCHEMAS.md` § 6 | See `05_DETECTION_AND_MATCHING.md` | `qt validate-rules` | JSON |
| Feature Vector | `07_SCHEMAS.md` § 7 | Computed at runtime | Vocabulary check | Not stored |

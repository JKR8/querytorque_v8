# Knowledge Atom: The Fundamental Unit of Knowledge

> **Status**: Target state specification
> **Builds on**: `KnowledgePrinciple` (`build_blackboard.py:131`), `BlackboardEntry` (`build_blackboard.py:51`), engine profile gap format

---

## What Is a Knowledge Atom?

A knowledge atom is the combination of clues that make an optimization observation **replicable**. It answers: "Given a new, unseen query, could I recognize the opportunity and apply this technique correctly?"

A complete atom has five components:

| Component | Question | Example (Q9: 4.47x) |
|-----------|----------|----------------------|
| **SQL Structure** | What pattern was present? | 8 correlated subqueries scanning `store_sales` with identical dimension joins |
| **Semantic Intent** | What was the query doing? | Conditional time-bucket aggregation — computing separate aggregates per time period |
| **EXPLAIN Evidence** | What did the plan reveal? | 8 separate sequential scans of `store_sales` (28.7M rows each), nested loop per subquery |
| **Principle** | Why did the technique work? | Single-pass aggregation consolidates 8 scans into 1 by using CASE inside SUM — 8x I/O reduction |
| **Conditions** | When does this apply / not apply? | Apply when 3+ scans of same fact table with conditional aggregation; don't apply when scans have different join topologies |

A single observation with all five components is a complete knowledge atom. It is immediately useful.

---

## Where Each Component Lives

| Atom Component | Captured In | Gap |
|----------------|-------------|-----|
| SQL Structure | `BlackboardEntry.fingerprint` (string label) | Needs structural features (FeatureVector) |
| Semantic Intent | `BlackboardEntry.query_intent` (usually empty) | Needs consistent population |
| EXPLAIN Evidence | Loaded in Phase 2, never persisted | Needs capture in blackboard |
| Principle (what/why) | `KnowledgePrinciple.what/why` | `why` from hardcoded lookup — you replace with real reasoning in analysis sessions |
| Conditions (when/when_not) | `KnowledgePrinciple.when/when_not` | `when` from hardcoded lookup — you derive from evidence in analysis sessions |

The knowledge atom is not a new data structure. It is the union of fields across `BlackboardEntry` (auto-captured), `KnowledgePrinciple` (currently hardcoded, target: human-derived), and engine profile gaps (human-authored).

---

## How You Build a Complete Atom

### Step 1: Pipeline captures the raw outcome (automatic)

The pipeline produces a `BlackboardEntry` with: query ID, worker ID, speedup, status, transforms, examples used, SET LOCAL config. This is auto-captured. No human action.

### Step 2: You review the blackboard entry

Open the analysis session form (`templates/analysis_session.md`). For an interesting outcome (WIN or CRITICAL_HIT), you look at:

- The original SQL (structure)
- The optimized SQL (what changed)
- The worker reasoning text (why they did what they did)
- The EXPLAIN plan (what the optimizer was doing wrong)

### Step 3: You fill in the finding

In the analysis session, you record:

- **Claim**: "DuckDB scans store_sales 8 times because it cannot consolidate correlated subqueries that share identical dimension joins"
- **Mechanism**: "Each correlated subquery is planned independently. The optimizer cannot detect that all 8 subqueries scan the same table with the same join topology."
- **Boundary conditions**: "Applies when 3+ correlated subqueries scan the same fact table with identical joins. Does NOT apply when join topologies differ between subqueries."
- **Evidence**: Q9 4.47x, Q88 6.28x — both consolidated repeated scans.

### Step 4: You update the engine profile

Based on the finding, you edit the engine profile markdown:
- Add a `Won:` entry to `REDUNDANT_SCAN_ELIMINATION`
- Add a rule about correlated subquery consolidation
- Tighten or loosen boundary conditions based on new evidence

### Step 5: You promote a gold example (if warranted)

If Q9 is the best example of this technique, fill in the gold example template with the 4-part explanation (what/why/when/when_not) and add it to `qt_sql/examples/duckdb/`.

---

## Quality Gate: When Is an Atom Complete?

An atom is complete when you can answer all five questions:

1. **What structural pattern?** → Specific, extractable features (not "star schema" alone, but "star schema with 8 repeated scans of store_sales via correlated subqueries")
2. **What was the query doing?** → Business-level intent that distinguishes this from similar-looking queries
3. **What did the plan show?** → The key EXPLAIN signal that confirms the structural diagnosis
4. **Why did this technique work?** → A mechanism explanation, not just "it was faster"
5. **When does it apply / not apply?** → Specific conditions derived from evidence, not generic rules

If any component is missing, the atom is incomplete but still useful — it just can't be reliably replicated on new queries until you fill the gap.

---

## Win Classification

| Tier | Speedup | Label | Meaning |
|------|---------|-------|---------|
| Standard | 1.1x - 1.99x | IMPROVED | Measurable improvement, captured in blackboard |
| Win | 2.0x+ | WIN | Priority for analysis session review |
| Critical Hit | 5.0x+ | CRITICAL_HIT | High-value atom, priority for gold example promotion |
| Goal | 10.0x+ | LEGENDARY | Exceptional — warrants detailed case study |

A single observation is sufficient. A 2x+ win appears once in the blackboard and is immediately useful for enriching the engine profile and as a gold example candidate.

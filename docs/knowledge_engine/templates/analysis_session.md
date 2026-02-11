# Analysis Session

**Date**: ____-__-__
**Analyst**: ___
**Engine**: duckdb | postgresql
**Benchmark**: tpcds | dsb
**Blackboard source**: `data/layer1/___/___/outcomes.jsonl`
**Entries reviewed**: ___
**Session ID**: AS-{ENGINE_SHORT}-{NNN}   (e.g. AS-DUCK-003)

---

## Batch Summary

Before diving into findings, record what you saw at a glance.

| Metric | Value |
|--------|-------|
| Total entries reviewed | |
| WINs (2x+) | |
| CRITICAL_HITs (5x+) | |
| REGRESSIONs | |
| ERRORs | |
| Top speedup (query, worker, transform) | |
| Worst regression (query, worker, transform) | |

---

## Findings

Each finding is an observation about engine optimizer behavior. Not a recommendation — an observation. Copy this block for each finding.

---

### Finding 1: ___title___

**ID**: F-{ENGINE_SHORT}-{NNN}  (e.g. F-DUCK-042)

**Claim**: ___one falsifiable sentence about optimizer behavior___

**Category**: scan_method | join_optimization | aggregation | subquery | config_tuning | index_usage | parallelism | materialization

**Evidence**:

| Query | Worker | Speedup | Transform | Supports / Contradicts | Notes |
|-------|--------|---------|-----------|----------------------|-------|
|       |        |         |           |                      |       |
|       |        |         |           |                      |       |
|       |        |         |           |                      |       |

Supporting: ___ / Contradicting: ___

**Mechanism** (WHY does the optimizer behave this way?):

___

**Boundary Conditions**:
- Applies when:
  - ___
  - ___
- Does NOT apply when:
  - ___
  - ___
- Diagnostic signal (what to look for in EXPLAIN):
  - ___

**Contradictions explained** (if any — WHY did some queries contradict?):

___

**Confidence**: high | medium | low
**Rationale**: ___

---

### Finding 2: ___title___

(copy the finding block above)

---

## Actions

For each finding, decide what to do with it. Check all that apply.

### Finding 1 → ___action summary___

**Profile update**:
- [ ] Add to existing gap `Won:` → Gap ID: ___
- [ ] Add to existing gap `Lost:` → Gap ID: ___
- [ ] Add rule to existing gap → Gap ID: ___
- [ ] Propose new gap → Draft ID: ___
- [ ] Add new strength → Draft ID: ___

**Gold example**:
- [ ] Promote gold example → Query: ___, Speedup: ___, Worker: ___

**No action**:
- [ ] Insufficient evidence — need more data
- [ ] Already covered by existing gap/rule
- [ ] Other: ___

**Exact profile text** (paste what you'll add or modify):

```markdown

```

---

### Finding 2 → ___action summary___

(copy the action block above)

---

## Gold Example Drafts

If promoting a gold example, fill in the 4-part explanation here. Use the gold example template for the full JSON.

### Example: Q___ → ___id___

**What** (specific transforms applied):

___

**Why** (performance mechanism):

___

**When** (conditions for application + diagnostic signal):

___

**When NOT** (counter-indications with evidence):

___

**Demonstrates gaps**: [ ___, ___ ]

---

## Open Questions

Anything unresolved that needs future investigation:

1. ___
2. ___

---

## Session Summary

| Item | Count |
|------|-------|
| Findings recorded | |
| Profile gaps updated | |
| Profile gaps proposed | |
| Profile strengths added | |
| Gold examples promoted | |
| Rules added/modified | |

**Profile version before**: ___
**Profile version after**: ___

**Key takeaway** (1-2 sentences):

___

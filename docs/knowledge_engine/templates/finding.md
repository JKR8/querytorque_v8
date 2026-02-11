# Finding: ___title___

**ID**: F-{ENGINE_SHORT}-{NNN}
**Session**: AS-{ENGINE_SHORT}-{NNN}
**Date**: ____-__-__
**Engine**: duckdb | postgresql
**Version tested**: ___

---

## Claim

___one falsifiable sentence about optimizer behavior___

## Category

scan_method | join_optimization | aggregation | subquery | config_tuning | index_usage | parallelism | materialization

## Evidence

| Query | Worker | Speedup | Transform | Supports / Contradicts | Key detail |
|-------|--------|---------|-----------|----------------------|------------|
|       |        |         |           |                      |            |
|       |        |         |           |                      |            |
|       |        |         |           |                      |            |
|       |        |         |           |                      |            |

**Total**: ___ supporting, ___ contradicting

## Mechanism

WHY does the optimizer behave this way? Reference internal optimizer behavior, not just "it was faster."

___

## Boundary Conditions

### Applies when
- ___
- ___
- ___

### Does NOT apply when
- ___
- ___

### Diagnostic signal (EXPLAIN)
- ___

## Contradictions

If any evidence contradicts the claim, explain WHY (different conditions, not random noise):

___

## Confidence

**Level**: high | medium | low

**Rationale**: ___

## Implication

What does this mean for future optimization attempts?

___

## Linked Profile Entry

- **Maps to existing gap**: ___ (or "none — propose new gap")
- **Proposed action**: update_gap | new_gap | new_strength | no_action

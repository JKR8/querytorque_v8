# Pathology Card Template
#
# Use this template when adding new pathology cards to decisions.md.
# Each card starts from what you SEE in the execution plan (the symptom),
# then reasons through WHY (the engine gap), WHAT IT MEANS (the implication),
# HOW TO DETECT, HOW TO FIX, and WHAT CAN GO WRONG.
#
# The IMPLICATION section is the most critical part — it generalizes from
# the specific symptom to ALL SQL shapes where this gap manifests.
# Without it, the card is a one-off fix. With it, it's transferable knowledge.

---

## PATHOLOGY N: [What you see in the plan — the symptom]

SURFACE COST:
  [What is expensive? How many rows? What work is wasted?
   Quantify: "N rows enter, M survive" or "scanned K times instead of once".]

ENGINE GAP: [OPTIMIZER_GAP_NAME or "none"]
  [What can't the optimizer do? Why does the plan look this way?
   Name the mechanism, not just the symptom.]

IMPLICATION: This [gap/limitation] fires whenever:
  - [SQL shape 1 where this manifests]
  - [SQL shape 2 — different syntax, same root cause]
  - [SQL shape 3 — the non-obvious case]
  - [SQL shape N — how far does this generalize?]

  It does NOT fire when:
  - [Exception 1 — when the optimizer already handles it]
  - [Exception 2 — when the pattern looks similar but the mechanism differs]

  [THE IMPLICATION IS THE MOST IMPORTANT SECTION. It explains how one engine
   gap manifests across MULTIPLE SQL shapes. A narrow list = narrow card.
   A well-reasoned list = transferable intelligence that works on unseen queries.
   Ask: "What is the GENERAL principle that makes this optimization work?"]

DETECTION:
  In EXPLAIN ANALYZE, look for:
  [check] [What specific plan node / pattern to look for]
  [check] [Row counts, ratios, or cost indicators]
  [check] [What confirms the optimizer missed the optimization]

  If [counter-signal in EXPLAIN]:
  -> Optimizer ALREADY handled it -> no benefit, STOP

  In SQL, look for:
  [check] [Structural patterns visible from SQL text alone]
  [check] [Table references, join types, subquery shapes]

  STOP signals:
  [cross] [Pattern that looks similar but will regress — with evidence]

RESTRUCTURING:
  [How to fix it. Show the data flow, not just "rewrite the query."
   Use the arrow notation to show the pipeline:]

  step_1 -> CTE or intermediate (description of what it computes)
  step_2 -> join / aggregate (description)
  -> final output

  [Variant if applicable:]
  [Describe the variant and how the pipeline differs]

RISK:
  [table with columns: Scenario | Expected | Worst seen | How to detect]
  [Include at least: best case, known regression scenario, hard stop]

  Guard rules:
  - [Rule 1 — what MUST be true for safety]
  - [Rule 2 — what to NEVER do, with evidence query]

TRANSFORMS: [transform_id_1, transform_id_2, ...]

GOLD EXAMPLES:
  Win: [Q (speedup)], [Q (speedup)]
  Win (PG): [Q (speedup)] (if cross-engine)
  Regression: [Q (ratio — root cause)]

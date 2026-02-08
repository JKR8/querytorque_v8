# REVIEW_PROMPT (Use this with another LLM agent)

You are conducting a **manual, query-by-query forensic SQL optimization review**.
Do not rely on aggregate stats. Inspect each file directly.

## Inputs
- One packet folder: `packets/<query_id>/`
- Must read: `00_context.json`, `01_original.sql`, `02_prev_winner.sql`
- If present, also read: `03_swarm_best.sql`, all `swarm_artifacts/*.json`, `swarm_artifacts/*response.txt`, and relevant worker SQL files.

## Required reasoning procedure
0. Validate output schema strictness first:
   - same column count
   - same column names
   - same column order
   - if this fails, candidate is not equivalent
1. Derive the true optimization principle from the previous winning SQL itself.
2. Check whether swarm explored this exact principle in:
   - assignments
   - reanalyze output
   - worker SQL implementations
3. Determine outcome path:
   - not explored
   - explored but implemented incorrectly
   - explored correctly but still slower
   - explored and fast but not selected / invalidated
4. Verify semantic integrity risks:
   - literal/date changes
   - metric column changes
   - altered join keys or aggregation grain
5. Provide a final verdict for WHY swarm did not beat previous efforts for this query.
6. Validate equivalence evidence quality:
   - confirm strict schema equivalence (same columns and order)
   - confirm rowcount evidence
   - confirm checksum/hash evidence
   - if missing, mark review as incomplete and explain exactly what is missing

## Equivalence policy
- Target standard: strict equivalence (`schema + rowcount + checksum`).
- No exception pathway for review signoff.
- If strict equivalence evidence is missing or fails, mark as `fail` or `blocked` (not `pass`).

## Output constraints
- Write results in `packets/<query_id>/review_result.md`
- Use exact file references.
- Include concise SQL snippets only when necessary.
- Provide confidence level and unresolved uncertainties.
- Do not mark a review as complete without explicit schema+rowcount+checksum validation evidence, or a hard blocker note.

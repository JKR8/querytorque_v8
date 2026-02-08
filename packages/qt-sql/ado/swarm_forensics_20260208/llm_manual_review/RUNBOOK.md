# RUNBOOK: LLM-Based Manual Review Handoff

## Goal
Enable another LLM agent to perform strict manual forensic review for every prev-winning query.

## Folder Structure
- `review_index.csv`: tracker with status/assignee.
- `review_queue.md`: priority queue ordered by most negative stored delta.
- `REVIEW_PROMPT.md`: prompt to use with reviewing LLM.
- `REVIEW_OUTPUT_TEMPLATE.md`: required output format.
- `packets/<query_id>/`: one packet per query with SQL + swarm artifacts.

## Required Process
1. Pick next `not_started` query from `review_index.csv`.
2. Give reviewing LLM:
   - `REVIEW_PROMPT.md`
   - packet folder path.
3. Save output into `packets/<query_id>/review_result.md` using `REVIEW_OUTPUT_TEMPLATE.md`.
4. Ensure the review includes explicit strict-equivalence evidence:
   - schema parity (same columns, same order)
   - rowcount parity
   - checksum/hash parity
   or a blocker note.
5. Update `review_index.csv` status:
   - `completed` only if validation status is `pass`.
   - `blocked` if rowcount/checksum evidence is missing or cannot be produced.
6. Record reviewer name and blocker details in `notes` when applicable.
7. Repeat for all queries.

## Completion Criteria
- All queries have non-placeholder `review_result.md`.
- Every query has explicit validation status (`pass` or `blocked`) with evidence.
- `pass` is allowed only when strict schema+rowcount+checksum parity is documented.
- `review_index.csv` has no `not_started` rows.
- Final synthesis should be written only after all per-query manual reviews are complete.

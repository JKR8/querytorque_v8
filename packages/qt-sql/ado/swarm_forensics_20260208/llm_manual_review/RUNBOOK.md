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
3. Save output into `packets/<query_id>/review_result.md`.
4. Update `review_index.csv` status to `completed` and reviewer name.
5. Repeat for all 52 queries.

## Completion Criteria
- All queries have non-placeholder `review_result.md`.
- `review_index.csv` has `status=completed` for all rows.
- Final synthesis should be written only after all per-query manual reviews are complete.

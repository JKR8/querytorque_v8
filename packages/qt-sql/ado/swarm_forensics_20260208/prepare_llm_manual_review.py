#!/usr/bin/env python3
import csv
import json
import re
import shutil
from pathlib import Path
from typing import Optional

ROOT = Path('/mnt/c/Users/jakc9/Documents/QueryTorque_V8')
BENCH = ROOT / 'packages/qt-sql/ado/benchmarks/duckdb_tpcds'
SWARM_BATCH = BENCH / 'swarm_batch_20260208_102033'
OUT = ROOT / 'packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review'
PACKETS = OUT / 'packets'


def read_json(path: Path):
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding='utf-8'))


def safe_copy(src: Path, dst: Path) -> bool:
    if not src.exists():
        return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return True


def parse_worker_id(worker_label: Optional[str]) -> Optional[int]:
    if not worker_label:
        return None
    m = re.match(r'^W(\d+)$', worker_label.strip())
    return int(m.group(1)) if m else None


def worker_sql_filename(worker_id: int) -> str:
    if worker_id in {1, 2, 3, 4}:
        return f'worker_{worker_id}_sql.sql'
    if worker_id == 5:
        return 'snipe_worker_sql.sql'
    if worker_id == 6:
        return 'final_worker_sql.sql'
    return f'worker_{worker_id}_sql.sql'


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding='utf-8')


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    PACKETS.mkdir(parents=True, exist_ok=True)

    comparison = read_json(BENCH / 'swarm_comparison.json')
    if not comparison:
        raise SystemExit('Missing swarm_comparison.json')

    prev_cases = [q for q in comparison['queries'] if q.get('winner') == 'prev']

    # Sort by most negative stored delta first (biggest reported regressions first), N/A last.
    def delta_key(q):
        d = q.get('delta')
        if not d or d == 'N/A':
            return 999
        return float(d.replace('x', '').replace('+', ''))

    prev_cases.sort(key=delta_key)

    index_rows = []
    manifest_queries = []

    for rank, q in enumerate(prev_cases, start=1):
        qid = q['query_id']
        q_suffix = qid[1:]
        query_dir_name = f'query_{q_suffix}'
        packet = PACKETS / qid
        packet.mkdir(parents=True, exist_ok=True)

        best_meta = read_json(BENCH / 'best' / f'{qid}.json') or {}
        prev_sql_source = best_meta.get('sql_source_path')
        prev_sql_path = Path(prev_sql_source) if prev_sql_source else (BENCH / 'best' / f'{qid}.sql')
        if not prev_sql_path.exists():
            prev_sql_path = BENCH / 'best' / f'{qid}.sql'

        original_sql_path = BENCH / 'queries' / f'query_{q_suffix}.sql'

        swarm_dir = SWARM_BATCH / query_dir_name
        swarm_worker_id = parse_worker_id(q.get('swarm_worker'))
        swarm_best_sql_path = None
        if swarm_worker_id is not None:
            candidate = swarm_dir / worker_sql_filename(swarm_worker_id)
            if candidate.exists():
                swarm_best_sql_path = candidate

        # Core SQL files for manual review.
        copied_original = safe_copy(original_sql_path, packet / '01_original.sql')
        copied_prev = safe_copy(prev_sql_path, packet / '02_prev_winner.sql')
        copied_swarm = False
        if swarm_best_sql_path:
            copied_swarm = safe_copy(swarm_best_sql_path, packet / '03_swarm_best.sql')

        # Copy all swarm artifacts for this query (json/txt/sql) for full LLM manual traceability.
        swarm_files = []
        if swarm_dir.exists():
            swarm_target = packet / 'swarm_artifacts'
            for src in sorted(swarm_dir.rglob('*')):
                if src.is_file() and src.suffix.lower() in {'.json', '.txt', '.sql'}:
                    rel = src.relative_to(swarm_dir)
                    dst = swarm_target / rel
                    safe_copy(src, dst)
                    swarm_files.append(str(rel))

        context = {
            'query_id': qid,
            'priority_rank': rank,
            'comparison_updated_at': comparison.get('updated_at'),
            'comparison_row': q,
            'prev_best_metadata': best_meta,
            'paths': {
                'original_sql': str(original_sql_path),
                'prev_winner_sql': str(prev_sql_path),
                'swarm_query_dir': str(swarm_dir),
                'swarm_best_sql': str(swarm_best_sql_path) if swarm_best_sql_path else None,
            },
            'copied_files': {
                '01_original.sql': copied_original,
                '02_prev_winner.sql': copied_prev,
                '03_swarm_best.sql': copied_swarm,
                'swarm_artifacts_count': len(swarm_files),
            },
            'swarm_artifacts_files': swarm_files,
            'manual_review_requirements': {
                'must_use_llm_reasoning': True,
                'must_read_sql': ['01_original.sql', '02_prev_winner.sql'],
                'must_read_if_exists': ['03_swarm_best.sql', 'swarm_artifacts/benchmark_iter0.json', 'swarm_artifacts/benchmark_iter1.json', 'swarm_artifacts/benchmark_iter2.json', 'swarm_artifacts/assignments.json', 'swarm_artifacts/reanalyze_parsed.json'],
                'must_provide': [
                    'winning_principle_from_prev_sql',
                    'whether_swarm_explored_that_principle',
                    'evidence_paths_and_quotes',
                    'root_cause_for_swarm_failure_on_this_query'
                ]
            }
        }
        write_text(packet / '00_context.json', json.dumps(context, indent=2))

        query_task = f"""# Manual LLM Review Task: {qid}

Status: `not_started`
Priority Rank: `{rank}` (lower is higher priority)

## Required Inputs
- `00_context.json`
- `01_original.sql`
- `02_prev_winner.sql`
- `03_swarm_best.sql` (if present)
- `swarm_artifacts/*` (especially benchmark/assignment/reanalysis/worker outputs)

## Review Questions (answer all)
1. What is the exact optimization principle used by the previous winning SQL?
2. Did swarm explicitly explore that principle in assignments, reanalysis, or generated SQL?
3. If explored, why did it still lose? If not explored, why was it missed?
4. Is there any evidence of semantic drift / query intent change in prev or swarm SQL?
5. What is the minimal change needed in swarm strategy to recover this query?

## Output
Write results to `review_result.md` in this folder using `../REVIEW_OUTPUT_TEMPLATE.md`.
"""
        write_text(packet / 'TASK.md', query_task)
        write_text(packet / 'review_result.md', '# Pending\n\nNot reviewed yet.\n')

        index_rows.append({
            'priority_rank': rank,
            'query_id': qid,
            'status': 'not_started',
            'reviewer': '',
            'stored_delta': q.get('delta'),
            'prev_speedup': q.get('prev_speedup'),
            'swarm_speedup': q.get('swarm_speedup'),
            'baseline_ms': q.get('baseline_ms'),
            'prev_ms': q.get('prev_ms'),
            'swarm_ms': q.get('swarm_ms'),
            'prev_source': q.get('prev_source'),
            'prev_transforms': '|'.join(q.get('prev_transforms') or []),
            'swarm_transforms': '|'.join(q.get('swarm_transforms') or []),
            'swarm_worker': q.get('swarm_worker') or '',
            'packet_path': f'packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/{qid}',
            'notes': ''
        })

        manifest_queries.append({
            'query_id': qid,
            'priority_rank': rank,
            'packet': f'packets/{qid}',
            'has_swarm_sql': copied_swarm,
            'swarm_artifacts_count': len(swarm_files)
        })

    # Index CSV.
    csv_path = OUT / 'review_index.csv'
    with csv_path.open('w', newline='', encoding='utf-8') as f:
        fieldnames = [
            'priority_rank', 'query_id', 'status', 'reviewer', 'stored_delta', 'prev_speedup',
            'swarm_speedup', 'baseline_ms', 'prev_ms', 'swarm_ms', 'prev_source',
            'prev_transforms', 'swarm_transforms', 'swarm_worker', 'packet_path', 'notes'
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(index_rows)

    # Queue markdown.
    queue_lines = [
        '# LLM Manual Review Queue',
        '',
        f'Total queries to manually review: **{len(index_rows)}**',
        '',
        '| Priority | Query | Stored Delta | Prev | Swarm | Status | Packet |',
        '|---:|---|---:|---:|---:|---|---|',
    ]
    for r in index_rows:
        queue_lines.append(
            f"| {r['priority_rank']} | `{r['query_id']}` | {r['stored_delta']} | {r['prev_speedup']}x | {r['swarm_speedup']}x | `{r['status']}` | `{r['packet_path']}` |"
        )
    write_text(OUT / 'review_queue.md', '\n'.join(queue_lines) + '\n')

    # Reviewer prompt and template.
    prompt_text = """# REVIEW_PROMPT (Use this with another LLM agent)

You are conducting a **manual, query-by-query forensic SQL optimization review**.
Do not rely on aggregate stats. Inspect each file directly.

## Inputs
- One packet folder: `packets/<query_id>/`
- Must read: `00_context.json`, `01_original.sql`, `02_prev_winner.sql`
- If present, also read: `03_swarm_best.sql`, all `swarm_artifacts/*.json`, `swarm_artifacts/*response.txt`, and relevant worker SQL files.

## Required reasoning procedure
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

## Output constraints
- Write results in `packets/<query_id>/review_result.md`
- Use exact file references.
- Include concise SQL snippets only when necessary.
- Provide confidence level and unresolved uncertainties.
"""
    write_text(OUT / 'REVIEW_PROMPT.md', prompt_text)

    template_text = """# Review Result: <query_id>

## Verdict
- Primary reason swarm lost:
- Secondary contributors:

## Previous Winner Principle (manual SQL-derived)
- Principle:
- Evidence:

## Swarm Exploration Trace
- Assignment evidence:
- Reanalyze evidence:
- Worker SQL evidence:
- Conclusion: explored / partially explored / not explored

## Performance/Validity Outcome
- What happened in benchmark iterations:
- Was the principle implemented correctly:
- If slower, why:

## Semantic Integrity Check
- Drift risks observed:
- Risk severity: low / medium / high

## Minimal Fix for Swarm
- Tactical change needed:
- Where to apply (fan-out, assignments, reanalyze, final selection):

## Evidence References
- `packets/<query_id>/01_original.sql`
- `packets/<query_id>/02_prev_winner.sql`
- `packets/<query_id>/03_swarm_best.sql` (if present)
- Additional files:

## Confidence
- Confidence: high / medium / low
- Uncertainties:
"""
    write_text(OUT / 'REVIEW_OUTPUT_TEMPLATE.md', template_text)

    runbook = """# RUNBOOK: LLM-Based Manual Review Handoff

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
"""
    write_text(OUT / 'RUNBOOK.md', runbook)

    manifest = {
        'source_comparison': str(BENCH / 'swarm_comparison.json'),
        'comparison_updated_at': comparison.get('updated_at'),
        'packet_count': len(manifest_queries),
        'packets': manifest_queries,
    }
    write_text(OUT / 'manifest.json', json.dumps(manifest, indent=2))

    print(f'Prepared manual review handoff at: {OUT}')
    print(f'Packets: {len(manifest_queries)}')
    print(f'Index: {csv_path}')


if __name__ == '__main__':
    main()

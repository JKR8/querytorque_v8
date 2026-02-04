"""Adaptive Rewriter v5 (JSON v5).

Parallel fan-out of DAG v2 prompts with gold JSON examples.
Uses parsed EXPLAIN plan summary and validates on sample DB.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import logging

from qt_sql.optimization.dag_v2 import DagV2Pipeline
from qt_sql.optimization.dag_v3 import build_prompt_with_examples, GoldExample, get_matching_examples
from qt_sql.optimization.plan_analyzer import analyze_plan_for_optimization, OptimizationContext
from qt_sql.execution.database_utils import run_explain_analyze
from qt_sql.validation.sql_validator import SQLValidator
from qt_sql.validation.schemas import ValidationStatus

logger = logging.getLogger(__name__)


def _create_llm_client(provider: Optional[str], model: Optional[str]):
    try:
        from qt_shared.llm import create_llm_client
    except Exception as exc:
        raise RuntimeError(
            "qt_shared.llm is required for JSON v5 optimization"
        ) from exc

    client = create_llm_client(provider=provider, model=model)
    if client is None:
        raise RuntimeError(
            "No LLM provider configured. "
            "Set QT_LLM_PROVIDER and API key environment variables."
        )
    return client


@dataclass
class CandidateResult:
    worker_id: int
    optimized_sql: str
    status: ValidationStatus
    speedup: float
    error: Optional[str]
    prompt: str
    response: str


@dataclass
class FullRunResult:
    sample: CandidateResult
    full_status: ValidationStatus
    full_speedup: float
    full_error: Optional[str]


def _format_plan_summary(ctx: OptimizationContext) -> str:
    """Compact plan summary with deduped scans and labeled operators."""
    lines: list[str] = []

    scan_counts: dict[str, int] = {}
    scan_by_table: dict[str, list] = {}
    for scan in ctx.table_scans:
        scan_counts[scan.table] = scan_counts.get(scan.table, 0) + 1
        scan_by_table.setdefault(scan.table, []).append(scan)

    for table in scan_by_table:
        scan_by_table[table].sort(
            key=lambda s: (s.rows_scanned, s.rows_out),
            reverse=True,
        )

    top_ops = ctx.get_top_operators(5)
    if top_ops:
        lines.append("Operators by cost:")
        for op in top_ops:
            label = op["operator"]
            if "SCAN" in label.upper() and scan_by_table:
                top_table = max(
                    scan_by_table.items(),
                    key=lambda kv: (kv[1][0].rows_scanned, kv[1][0].rows_out),
                )[0]
                label = f"{label}({top_table})"
            lines.append(
                f"- {label}: {op['cost_pct']}% cost, {op['rows']:,} rows"
            )
        lines.append("")

    if scan_by_table:
        lines.append("Scans:")
        for table, scans in sorted(
            scan_by_table.items(),
            key=lambda kv: (kv[1][0].rows_scanned, kv[1][0].rows_out),
            reverse=True,
        )[:8]:
            s = scans[0]
            count = scan_counts[table]
            if s.has_filter:
                lines.append(
                    f"- {table} x{count}: {s.rows_scanned:,} → {s.rows_out:,} rows (filtered)"
                )
            else:
                lines.append(f"- {table} x{count}: {s.rows_scanned:,} rows (no filter)")
        lines.append("")

    if ctx.cardinality_misestimates:
        lines.append("Misestimates:")
        for mis in ctx.cardinality_misestimates:
            lines.append(
                f"- {mis['operator']}: est {mis['estimated']:,} vs actual {mis['actual']:,} ({mis['ratio']}x)"
            )
        lines.append("")

    if ctx.joins:
        lines.append("Joins:")
        for j in ctx.joins[:5]:
            late = " (late)" if j.is_late else ""
            lines.append(
                f"- {j.join_type}: {j.left_table} x {j.right_table} -> {j.output_rows:,} rows{late}"
            )
        lines.append("")

    return "\n".join(lines).strip() or "(execution plan not available)"


def _format_plan_details(ctx: OptimizationContext) -> str:
    """More detailed plan summary for explore worker."""
    lines: list[str] = []

    top_ops = ctx.get_top_operators(10)
    if top_ops:
        lines.append("Operators by cost (top 10):")
        for op in top_ops:
            lines.append(
                f"- {op['operator']}: {op['cost_pct']}% cost, {op['rows']:,} rows"
            )
        lines.append("")

    if ctx.table_scans:
        lines.append("All scans:")
        for scan in ctx.table_scans:
            if scan.has_filter:
                lines.append(
                    f"- {scan.table}: {scan.rows_scanned:,} → {scan.rows_out:,} rows (filtered)"
                )
            else:
                lines.append(f"- {scan.table}: {scan.rows_scanned:,} rows (no filter)")
        lines.append("")

    if ctx.cardinality_misestimates:
        lines.append("Misestimates:")
        for mis in ctx.cardinality_misestimates:
            lines.append(
                f"- {mis['operator']}: est {mis['estimated']:,} vs actual {mis['actual']:,} ({mis['ratio']}x)"
            )
        lines.append("")

    if ctx.joins:
        lines.append("Joins:")
        for j in ctx.joins:
            late = " (late)" if j.is_late else ""
            lines.append(
                f"- {j.join_type}: {j.left_table} x {j.right_table} -> {j.output_rows:,} rows{late}"
            )
        lines.append("")

    return "\n".join(lines).strip() or "(execution plan not available)"


def _get_plan_context(db_path: str, sql: str) -> tuple[str, str, Optional[dict]]:
    """Return (summary, raw_plan_text, plan_json)."""
    result = run_explain_analyze(db_path, sql) or {}
    plan_json = result.get("plan_json")
    plan_text = result.get("plan_text") or "(execution plan not available)"
    if not plan_json:
        return "(execution plan not available)", plan_text, None

    ctx = analyze_plan_for_optimization(plan_json, sql)
    return _format_plan_summary(ctx), plan_text, plan_json


def _build_base_prompt(sql: str, plan_json: Optional[dict]) -> str:
    pipeline = DagV2Pipeline(sql, plan_json=plan_json)
    return pipeline.get_prompt()


def _split_example_batches(examples: List[GoldExample], batch_size: int = 3) -> List[List[GoldExample]]:
    batches: list[list[GoldExample]] = []
    for i in range(0, len(examples), batch_size):
        batches.append(examples[i:i + batch_size])
    return batches


def _build_prompt_with_examples(
    base_prompt: str,
    examples: List[GoldExample],
    plan_summary: str,
    history: str = "",
) -> str:
    return build_prompt_with_examples(base_prompt, examples, plan_summary, history)


def _build_history_section(previous_response: str, error: str) -> str:
    return (
        "## Previous Attempt (FAILED)\n\n"
        f"Failure reason: {error}\n\n"
        "Previous rewrites:\n"
        f"```\n{previous_response}\n```\n\n"
        "Try a DIFFERENT approach."
    )


def _worker_json(
    worker_id: int,
    sql: str,
    base_prompt: str,
    plan_summary: str,
    examples: List[GoldExample],
    sample_db: str,
    retry: bool = True,
    explore: bool = False,
    plan_details: Optional[str] = None,
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> CandidateResult:
    llm_client = _create_llm_client(provider, model)

    history = ""
    if explore:
        history = (
            "## Explore Mode\n"
            "Be adversarial. Exploit transforms the DB engine is unlikely to do automatically.\n"
            "Prioritize structural rewrites that reduce scans/aggregation work.\n"
        )
        if plan_details:
            history += f"\n## Plan (Full EXPLAIN)\n{plan_details}\n"

    full_prompt = _build_prompt_with_examples(base_prompt, examples, plan_summary, history)

    response_text = llm_client.analyze(full_prompt)

    pipeline = DagV2Pipeline(sql)
    optimized_sql = pipeline.apply_response(response_text)

    validator = SQLValidator(database=sample_db)
    result = validator.validate(sql, optimized_sql)

    if result.status != ValidationStatus.PASS and retry:
        error = result.errors[0] if result.errors else "Validation failed"
        history = _build_history_section(response_text, error)
        full_prompt = _build_prompt_with_examples(base_prompt, examples, plan_summary, history)

        response_text = llm_client.analyze(full_prompt)

        optimized_sql = pipeline.apply_response(response_text)
        result = validator.validate(sql, optimized_sql)

    error = result.errors[0] if result.errors else None
    return CandidateResult(
        worker_id=worker_id,
        optimized_sql=optimized_sql,
        status=result.status,
        speedup=result.speedup,
        error=error,
        prompt=full_prompt,
        response=response_text,
    )


def optimize_v5_json(
    sql: str,
    sample_db: str,
    max_workers: int = 5,
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> CandidateResult:
    """Parallel JSON-example v5 optimizer. Returns best candidate (legacy behavior)."""
    plan_summary, plan_text, plan_json = _get_plan_context(sample_db, sql)
    base_prompt = _build_base_prompt(sql, plan_json)

    # Order examples by KB matches (dag_v3 does this) then split into batches.
    examples = get_matching_examples(sql)
    batches = _split_example_batches(examples, batch_size=3)

    # Use first 4 batches for coverage (12 examples total)
    coverage_batches = batches[:4]
    while len(coverage_batches) < 4:
        coverage_batches.append([])

    tasks = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        # Coverage workers
        for i, batch in enumerate(coverage_batches):
            tasks.append(pool.submit(
                _worker_json,
                i + 1,
                sql,
                base_prompt,
                plan_summary,
                batch,
                sample_db,
                True,
                False,
                None,
                provider,
                model,
            ))

        # Explore worker (no examples, detailed plan)
        tasks.append(pool.submit(
            _worker_json,
            5,
            sql,
            base_prompt,
            plan_summary,
            [],
            sample_db,
            True,
            True,
            plan_text,
            provider,
            model,
        ))

        results = [t.result() for t in as_completed(tasks)]

    # Return first valid candidate (sample DB speedup is kept for reference but not used for selection)
    valid = [r for r in results if r.status == ValidationStatus.PASS]
    return valid[0] if valid else results[0]


def optimize_v5_json_queue(
    sql: str,
    sample_db: str,
    full_db: str,
    max_workers: int = 5,
    target_speedup: float = 2.0,
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> tuple[list[CandidateResult], list[FullRunResult], Optional[FullRunResult]]:
    """Run v5 in parallel on sample, then validate all valid candidates on full DB in sequence.

    Returns: (valid_candidates, full_results, first_over_target)
    """
    plan_summary, plan_text, plan_json = _get_plan_context(sample_db, sql)
    base_prompt = _build_base_prompt(sql, plan_json)

    examples = get_matching_examples(sql)
    batches = _split_example_batches(examples, batch_size=3)
    coverage_batches = batches[:4]
    while len(coverage_batches) < 4:
        coverage_batches.append([])

    tasks = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for i, batch in enumerate(coverage_batches):
            tasks.append(pool.submit(
                _worker_json,
                i + 1,
                sql,
                base_prompt,
                plan_summary,
                batch,
                sample_db,
                True,
                False,
                None,
                provider,
                model,
            ))

        tasks.append(pool.submit(
            _worker_json,
            5,
            sql,
            base_prompt,
            plan_summary,
            [],
            sample_db,
            True,
            True,
            plan_text,
            provider,
            model,
        ))

        results = [t.result() for t in as_completed(tasks)]

    valid = [r for r in results if r.status == ValidationStatus.PASS]

    full_validator = SQLValidator(database=full_db)
    full_results: list[FullRunResult] = []
    winner: Optional[FullRunResult] = None

    for cand in valid:
        full = full_validator.validate(sql, cand.optimized_sql)
        full_err = full.errors[0] if full.errors else None
        full_result = FullRunResult(
            sample=cand,
            full_status=full.status,
            full_speedup=full.speedup,
            full_error=full_err,
        )
        full_results.append(full_result)

        if full.status == ValidationStatus.PASS and full.speedup >= target_speedup:
            winner = full_result
            break

    return valid, full_results, winner

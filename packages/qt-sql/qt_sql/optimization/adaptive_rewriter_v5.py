"""Adaptive Rewriter v5 (JSON v5).

Parallel fan-out of DAG v2 prompts with gold JSON examples.
Uses parsed EXPLAIN plan summary and validates on sample DB.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import logging

from qt_sql.optimization.dag_v2 import DagV2Pipeline
from qt_sql.optimization.dag_v3 import build_prompt_with_examples, GoldExample, get_matching_examples, load_example, load_all_examples
from qt_sql.optimization.query_recommender import get_query_recommendations
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


def _get_plan_context(db_path: str, sql: str) -> tuple[str, str, Optional[Any]]:
    """Return (summary, raw_plan_text, plan_context).

    Returns:
        tuple: (plan_summary_str, raw_plan_text, OptimizationContext)
    """
    result = run_explain_analyze(db_path, sql) or {}
    plan_json = result.get("plan_json")
    plan_text = result.get("plan_text") or "(execution plan not available)"
    if not plan_json:
        return "(execution plan not available)", plan_text, None

    ctx = analyze_plan_for_optimization(plan_json, sql)
    return _format_plan_summary(ctx), plan_text, ctx


def _build_base_prompt(sql: str, plan_context: Optional[Any]) -> str:
    pipeline = DagV2Pipeline(sql, plan_context=plan_context)
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


def _worker_full_sql(
    worker_id: int,
    sql: str,
    full_explain_plan: str,
    sample_db: str,
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> CandidateResult:
    """Worker 5: Direct SQL output (no DAG JSON).

    This worker:
    - Has no examples (explore mode)
    - Gets full EXPLAIN plan (not summary)
    - Is instructed to be adversarial/creative
    - Outputs full SQL directly (not JSON)
    """
    llm_client = _create_llm_client(provider, model)

    # Build prompt for full SQL output
    prompt = f"""You are a SQL optimizer. Rewrite the ENTIRE query for maximum performance.

## Adversarial Explore Mode
Be creative and aggressive. Try radical structural rewrites that the database
engine is unlikely to do automatically. Don't be constrained by incremental changes.

## Original Query
```sql
{sql}
```

## Full Execution Plan (EXPLAIN ANALYZE)
```
{full_explain_plan}
```

## Instructions
1. Analyze the execution plan bottlenecks
2. Rewrite the entire query for maximum performance
3. Try transforms like:
   - Decorrelating subqueries
   - Converting OR to UNION ALL
   - Pushing down filters aggressively
   - Materializing CTEs strategically
   - Reordering joins
   - Eliminating redundant operations

## Output Format
Return ONLY the complete optimized SQL query. No JSON. No explanation. Just SQL.

Example output:
WITH cte1 AS (
  SELECT ...
)
SELECT ...
FROM cte1
...
"""

    response_text = llm_client.analyze(prompt)

    # Extract SQL (no JSON parsing needed)
    optimized_sql = response_text.strip()

    # Remove markdown code blocks if present
    if optimized_sql.startswith('```'):
        lines = optimized_sql.split('\n')
        # Remove first line (```sql or ```) and last line (```)
        if lines[0].strip().startswith('```'):
            lines = lines[1:]
        if lines and lines[-1].strip() == '```':
            lines = lines[:-1]
        optimized_sql = '\n'.join(lines)

    # Validate on sample DB
    validator = SQLValidator(database=sample_db)
    result = validator.validate(sql, optimized_sql)

    error = result.errors[0] if result.errors else None
    return CandidateResult(
        worker_id=worker_id,
        optimized_sql=optimized_sql,
        status=result.status,
        speedup=result.speedup,
        error=error,
        prompt=prompt,
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
    plan_summary, plan_text, plan_context = _get_plan_context(sample_db, sql)
    base_prompt = _build_base_prompt(sql, plan_context)

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
    query_id: Optional[str] = None,
    max_workers: int = 5,
    target_speedup: float = 2.0,
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> tuple[list[CandidateResult], list[FullRunResult], Optional[FullRunResult]]:
    """Run v5 in parallel on sample, then validate all valid candidates on full DB in sequence.

    Args:
        sql: SQL query to optimize
        sample_db: Path to sample database
        full_db: Path to full database
        query_id: Optional query ID (e.g., 'q1', 'q15') for ML-based example selection
        max_workers: Number of parallel workers
        target_speedup: Target speedup threshold
        provider: LLM provider
        model: LLM model

    Returns: (valid_candidates, full_results, first_over_target)
    """
    plan_summary, plan_text, plan_context = _get_plan_context(sample_db, sql)
    base_prompt = _build_base_prompt(sql, plan_context)

    # Get query-specific examples if query_id provided
    if query_id:
        # 1. Get ML recommendations (up to 12)
        ml_recs = get_query_recommendations(query_id, top_n=12)
        logger.info(f"Query {query_id}: Got {len(ml_recs)} ML recommendations: {ml_recs[:3]}")

        # 2. Pad with remaining examples if needed
        all_examples = load_all_examples()
        all_example_ids = [ex.id for ex in all_examples]

        padded_recs = ml_recs.copy()
        for ex_id in all_example_ids:
            if len(padded_recs) >= 12:
                break
            if ex_id not in padded_recs:
                padded_recs.append(ex_id)

        # 3. Load example objects
        example_objects = []
        for ex_id in padded_recs[:12]:
            ex = load_example(ex_id)
            if ex:
                example_objects.append(ex)

        # 4. Split into 4 batches of 3 for diversity
        batches = [
            example_objects[0:3],   # Worker 1: Top ML recs
            example_objects[3:6],   # Worker 2
            example_objects[6:9],   # Worker 3
            example_objects[9:12],  # Worker 4
        ]
        logger.info(f"Query {query_id}: Split into 4 batches of {[len(b) for b in batches]} examples")
    else:
        # Fallback: Use KB pattern matching (old behavior)
        examples = get_matching_examples(sql)
        batches = _split_example_batches(examples, batch_size=3)
        batches = batches[:4]

    # Ensure we have 4 batches
    while len(batches) < 4:
        batches.append([])

    tasks = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        # Workers 1-4: DAG JSON with examples
        for i, batch in enumerate(batches):
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

        # Worker 5: Full SQL output (no examples, no JSON)
        tasks.append(pool.submit(
            _worker_full_sql,
            5,
            sql,
            plan_text,  # Full EXPLAIN plan
            sample_db,
            provider,
            model,
        ))

        results = [t.result() for t in as_completed(tasks)]

    valid = [r for r in results if r.status == ValidationStatus.PASS]
    logger.info(f"Sample validation: {len(valid)}/{len(results)} workers produced valid results")

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
            logger.info(f"Found winner: Worker {cand.worker_id} with {full.speedup:.2f}x speedup")
            break

    return valid, full_results, winner


# ============================================================================
# Mode 1: Retry (Single Worker with Retries)
# ============================================================================

def optimize_v5_retry(
    sql: str,
    sample_db: str,
    full_db: str,
    query_id: Optional[str] = None,
    max_retries: int = 3,
    target_speedup: float = 2.0,
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> tuple[Optional[CandidateResult], Optional[FullRunResult], list[dict]]:
    """
    Mode 1: Retry - Single worker with error feedback retries.

    Args:
        sql: SQL query to optimize
        sample_db: Path to sample database
        full_db: Path to full database
        query_id: Optional query ID for ML-based example selection
        max_retries: Maximum retry attempts (default: 3)
        target_speedup: Target speedup threshold (default: 2.0)
        provider: LLM provider
        model: LLM model

    Returns: (final_candidate, full_result, attempts_history)
    """
    logger.info(f"Starting Mode 1 (Retry) optimization with max {max_retries} attempts")

    plan_summary, plan_text, plan_context = _get_plan_context(sample_db, sql)
    base_prompt = _build_base_prompt(sql, plan_context)

    # Get ML-recommended examples
    if query_id:
        ml_recs = get_query_recommendations(query_id, top_n=3)
        examples = []
        for ex_id in ml_recs[:3]:
            ex = load_example(ex_id)
            if ex:
                examples.append(ex)
    else:
        examples = get_matching_examples(sql)[:3]

    attempts_history = []
    error_history = []

    for attempt in range(1, max_retries + 1):
        logger.info(f"Attempt {attempt}/{max_retries}")

        # Build history section from previous errors
        history = ""
        if attempt > 1:
            if attempt == 2:
                # Single previous error
                history = error_history[0]
            else:
                # Multiple errors - show all
                history = "## All Previous Attempts\n\n"
                for i, err_hist in enumerate(error_history, 1):
                    history += f"### Attempt {i}\n{err_hist}\n\n"

        # Call worker
        result = _worker_json(
            worker_id=attempt,
            sql=sql,
            base_prompt=base_prompt + "\n\n" + history if history else base_prompt,
            plan_summary=plan_summary,
            examples=examples,
            sample_db=sample_db,
            retry=False,  # We handle retry logic here
            explore=False,
            plan_details=None,
            provider=provider,
            model=model,
        )

        attempts_history.append({
            "attempt": attempt,
            "status": result.status.value,
            "speedup": result.speedup if result.status == ValidationStatus.PASS else None,
            "error": result.error,
        })

        if result.status == ValidationStatus.PASS:
            # Success! Benchmark on full DB
            logger.info(f"Attempt {attempt} succeeded on sample DB, benchmarking on full DB")
            full_validator = SQLValidator(database=full_db)
            full = full_validator.validate(sql, result.optimized_sql)
            full_result = FullRunResult(
                sample=result,
                full_status=full.status,
                full_speedup=full.speedup,
                full_error=full.errors[0] if full.errors else None,
            )

            if full.status == ValidationStatus.PASS and full.speedup >= target_speedup:
                logger.info(f"✅ Target met after {attempt} attempts: {full.speedup:.2f}x")
                return result, full_result, attempts_history
            else:
                logger.info(f"⚠️ Full DB validation passed but speedup {full.speedup:.2f}x below target {target_speedup}x")
                # Continue trying

        # Failed - add to error history for next attempt
        if result.error:
            error_hist = _build_history_section(result.response, result.error)
            error_history.append(error_hist)
            logger.info(f"Attempt {attempt} failed: {result.error}")

    logger.info(f"❌ All {max_retries} attempts exhausted without success")
    return None, None, attempts_history


# ============================================================================
# Mode 3: Evolutionary (Iterative Improvement with Stacking)
# ============================================================================

def optimize_v5_evolutionary(
    sql: str,
    full_db: str,
    query_id: Optional[str] = None,
    max_iterations: int = 5,
    target_speedup: float = 2.0,
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> tuple[Optional[CandidateResult], Optional[FullRunResult], list[dict]]:
    """
    Mode 3: Evolutionary - Iterative improvement with stacking optimizations.

    Args:
        sql: SQL query to optimize
        full_db: Path to full database (no sample DB - benchmark every iteration)
        query_id: Optional query ID for ML-based example selection
        max_iterations: Maximum iterations (default: 5)
        target_speedup: Target speedup threshold (default: 2.0)
        provider: LLM provider
        model: LLM model

    Returns: (best_candidate, best_full_result, iterations_history)
    """
    logger.info(f"Starting Mode 3 (Evolutionary) optimization with max {max_iterations} iterations")

    # Example rotation sets
    all_examples = load_all_examples()
    example_rotation = [
        all_examples[0:3],   # Iteration 1
        all_examples[3:6],   # Iteration 2
        all_examples[6:9],   # Iteration 3
        all_examples[9:12],  # Iteration 4
        all_examples[0:3],   # Iteration 5 (wrap around)
    ]

    current_sql = sql
    best_speedup = 1.0
    best_candidate = None
    best_full_result = None
    iterations_history = []

    for iteration in range(1, max_iterations + 1):
        logger.info(f"Iteration {iteration}/{max_iterations}")

        # Get plan for current SQL
        plan_summary, plan_text, plan_context = _get_plan_context(full_db, current_sql)
        base_prompt = _build_base_prompt(current_sql, plan_context)

        # Get examples for this iteration
        examples = example_rotation[(iteration - 1) % len(example_rotation)]

        # Build success history from previous iterations
        history = ""
        if iteration > 1:
            history = "## Previous Iterations\n\n"
            for it in iterations_history:
                history += f"### Iteration {it['iteration']}: {it['speedup']:.2f}x speedup ✓\n"
                history += f"**Transform:** {it['transform']}\n"
                if it.get('key_changes'):
                    history += f"**Key changes:** {it['key_changes']}\n"
                history += "\n"

            history += f"## Current Challenge\n"
            history += f"**Current best:** {best_speedup:.2f}x speedup\n"
            history += f"**Target:** {target_speedup}x\n"
            history += f"**Gap:** {target_speedup - best_speedup:.2f}x\n\n"
            history += "Now try to improve upon the current best while preserving all previous optimizations.\n\n"

        # Call worker (no sample DB - directly benchmark on full)
        prompt = base_prompt + "\n\n" + history if history else base_prompt

        # Generate optimization
        llm_client = _create_llm_client(provider, model)
        response = llm_client.call(prompt)

        # Assemble SQL
        pipeline = DagV2Pipeline()
        try:
            optimized_sql = pipeline.assemble_from_response(response, current_sql)
        except Exception as e:
            logger.error(f"Iteration {iteration} failed to assemble: {e}")
            iterations_history.append({
                "iteration": iteration,
                "status": "failed",
                "error": str(e),
            })
            continue

        # Benchmark on full DB
        validator = SQLValidator(database=full_db)
        full = validator.validate(current_sql, optimized_sql)

        if full.status == ValidationStatus.PASS:
            speedup = full.speedup
            logger.info(f"Iteration {iteration}: {speedup:.2f}x speedup")

            # Create candidate result
            candidate = CandidateResult(
                worker_id=iteration,
                optimized_sql=optimized_sql,
                status=full.status,
                speedup=speedup,
                error=None,
                prompt=prompt,
                response=response,
            )

            # Check if improved
            if speedup > best_speedup:
                best_speedup = speedup
                best_candidate = candidate
                best_full_result = FullRunResult(
                    sample=candidate,
                    full_status=full.status,
                    full_speedup=speedup,
                    full_error=None,
                )
                # Update current SQL to best so far
                current_sql = optimized_sql
                logger.info(f"✅ New best: {best_speedup:.2f}x (iteration {iteration})")

            iterations_history.append({
                "iteration": iteration,
                "status": "success",
                "speedup": speedup,
                "transform": "extracted_from_response",  # Would parse from response
                "improved": speedup > best_speedup,
            })

            # Check if target met
            if speedup >= target_speedup:
                logger.info(f"🏆 Target met after {iteration} iterations: {speedup:.2f}x")
                break
        else:
            logger.error(f"Iteration {iteration} failed validation: {full.errors}")
            iterations_history.append({
                "iteration": iteration,
                "status": "failed",
                "error": full.errors[0] if full.errors else "Unknown error",
            })

    if best_candidate:
        logger.info(f"✅ Best result: {best_speedup:.2f}x after {len(iterations_history)} iterations")
    else:
        logger.info(f"❌ No successful optimizations in {max_iterations} iterations")

    return best_candidate, best_full_result, iterations_history

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
from qt_sql.optimization.query_recommender import get_recommendations_for_sql
from qt_sql.optimization.plan_analyzer import analyze_plan_for_optimization, OptimizationContext
from qt_sql.execution.database_utils import run_explain_analyze
from qt_sql.validation.sql_validator import SQLValidator
from qt_sql.validation.schemas import ValidationStatus

logger = logging.getLogger(__name__)


# ============================================================================
# VERIFIED TRANSFORMS - Only transforms with proven speedups on TPC-DS
# ============================================================================

VERIFIED_TRANSFORMS = {
    "decorrelate": {"speedup": 2.92, "query": "Q1", "description": "Decorrelate subquery to CTE with GROUP BY"},
    "or_to_union": {"speedup": 3.17, "query": "Q15", "description": "Split OR conditions into UNION ALL branches"},
    "early_filter": {"speedup": 4.00, "query": "Q93", "description": "Filter dimension tables FIRST before fact joins"},
    "pushdown": {"speedup": 2.11, "query": "Q9", "description": "Push predicates into CTEs/subqueries"},
    "date_cte_isolate": {"speedup": 4.00, "query": "Q6", "description": "Extract date filtering into separate CTE"},
    "union_cte_split": {"speedup": 1.36, "query": "Q74", "description": "Split generic UNION CTE into year-specific CTEs"},
    "materialize_cte": {"speedup": 1.37, "query": "Q95", "description": "Extract repeated subquery into CTE"},
}


# AST hints for detected patterns without full verified examples
AST_HINTS = {
    "projection_prune": "Consider removing unused columns from CTE projections",
    "reorder_join": "Consider reordering joins - filter dimensions first, then join to facts",
    "flatten_subquery": "Consider flattening IN/EXISTS subquery to JOIN",
    "inline_cte": "Consider inlining single-use CTE for optimizer flexibility",
}


def get_verified_example_ids() -> List[str]:
    """Return list of verified transform IDs."""
    return list(VERIFIED_TRANSFORMS.keys())


def is_verified_transform(transform_id: str) -> bool:
    """Check if a transform ID has verified speedup."""
    return transform_id in VERIFIED_TRANSFORMS


def get_verified_examples(sql: str) -> List[GoldExample]:
    """Return only examples with verified speedups.

    Args:
        sql: SQL query (used to get matching examples)

    Returns:
        List of GoldExample objects for verified transforms only
    """
    all_examples = get_matching_examples(sql)
    return [ex for ex in all_examples if ex.id in VERIFIED_TRANSFORMS]


def filter_to_verified(recommendations: List[str]) -> List[str]:
    """Filter ML recommendations to only verified transforms.

    Args:
        recommendations: List of transform IDs from ML recommender

    Returns:
        Filtered list containing only verified transform IDs
    """
    return [r for r in recommendations if r in VERIFIED_TRANSFORMS]


def format_ast_hint(pattern_id: str) -> str:
    """Format detected pattern as hint without full example.

    Used for patterns detected by AST analysis but without verified examples.

    Args:
        pattern_id: The pattern identifier

    Returns:
        Formatted hint string
    """
    hint = AST_HINTS.get(pattern_id, f"Consider applying {pattern_id} transform")
    return f"**AST Hint:** {hint}"


def get_ast_hints_for_query(sql: str) -> List[str]:
    """Get AST hints for unverified patterns detected in query.

    Args:
        sql: SQL query to analyze

    Returns:
        List of formatted hint strings
    """
    from .knowledge_base import detect_opportunities

    hints = []
    kb_hits = detect_opportunities(sql)

    for hit in kb_hits:
        pattern_id = hit.pattern.id if hasattr(hit, 'pattern') else str(hit)
        # Only add hints for patterns without verified examples
        if pattern_id in AST_HINTS and pattern_id not in VERIFIED_TRANSFORMS:
            hints.append(format_ast_hint(pattern_id))

    return hints


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


def _is_postgres_dsn(dsn: str) -> bool:
    """Check if DSN is for PostgreSQL."""
    dsn_lower = dsn.lower()
    return dsn_lower.startswith("postgres://") or dsn_lower.startswith("postgresql://")


def _load_prompt_template(template_name: str) -> str:
    """Load a prompt template from the prompts/templates directory."""
    from pathlib import Path
    template_path = Path(__file__).parent / "prompts" / "templates" / template_name
    if template_path.exists():
        return template_path.read_text()
    return ""


def _build_postgres_prompt(sql: str, sample_db: str, full_explain_plan: str) -> str:
    """Build PostgreSQL-specific prompt with full context.

    Uses pg_context_builder to extract schema, stats, and settings
    for only the tables referenced in the query.
    """
    from qt_sql.execution.factory import create_executor_from_dsn
    from qt_sql.optimization.pg_context_builder import build_pg_optimization_context

    # Load template
    template = _load_prompt_template("full_sql_postgres.txt")
    if not template:
        logger.warning("PostgreSQL prompt template not found, using generic")
        return ""

    # Build context using executor
    try:
        executor = create_executor_from_dsn(sample_db)
        executor.connect()

        context = build_pg_optimization_context(
            executor=executor,
            sql=sql,
            explain_output=full_explain_plan,
        )

        executor.close()

        # Fill template placeholders
        prompt = template.format(
            postgres_version=context["postgres_version"],
            postgres_settings=context["postgres_settings"],
            schema_ddl=context["schema_ddl"],
            table_statistics=context["table_statistics"],
            original_query=context["original_query"],
            explain_analyze_output=context["explain_analyze_output"],
        )
        return prompt

    except Exception as e:
        logger.warning(f"Failed to build PostgreSQL context: {e}")
        return ""


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
    - Uses PostgreSQL-specific prompt with schema/stats when available
    """
    llm_client = _create_llm_client(provider, model)

    # Try PostgreSQL-specific prompt if database is PostgreSQL
    prompt = ""
    if _is_postgres_dsn(sample_db):
        prompt = _build_postgres_prompt(sql, sample_db, full_explain_plan)

    # Fall back to generic prompt if PostgreSQL prompt failed or not applicable
    if not prompt:
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
    output_dir: Optional[str] = None,
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
        output_dir: Optional directory to save intermediate SQL files immediately after LLM response

    Returns: (valid_candidates, full_results, first_over_target)
    """
    plan_summary, plan_text, plan_context = _get_plan_context(sample_db, sql)
    base_prompt = _build_base_prompt(sql, plan_context)

    # Get ML recommendations based on SQL similarity (works with ANY query)
    # LIVE FAISS search - vectorizes input SQL and finds similar verified queries
    ml_recs = get_recommendations_for_sql(sql, top_n=12)
    verified_recs = filter_to_verified(ml_recs)
    logger.info(f"LIVE FAISS: ML recs={len(ml_recs)}, verified={len(verified_recs)}: {verified_recs}")

    # Fallback to KB-matched verified examples if ML has few
    if len(verified_recs) < 6:
        kb_examples = get_matching_examples(sql)
        kb_verified = [ex.id for ex in kb_examples if ex.id in VERIFIED_TRANSFORMS]
        for ex_id in kb_verified:
            if ex_id not in verified_recs:
                verified_recs.append(ex_id)
        logger.info(f"Padded with KB-verified to {len(verified_recs)}: {verified_recs}")

    # Load example objects (verified only)
    example_objects = []
    for ex_id in verified_recs:
        ex = load_example(ex_id)
        if ex:
            example_objects.append(ex)

    # Split into 4 batches of up to 3 for diversity
    batches = [
        example_objects[0:3],   # Worker 1: Top verified ML recs
        example_objects[3:6],   # Worker 2
        example_objects[6:9],   # Worker 3 (may be empty)
        example_objects[9:12],  # Worker 4 (may be empty)
    ]
    logger.info(f"Split into 4 batches of {[len(b) for b in batches]} verified examples")

    if not example_objects:
        # Final fallback: Use KB pattern matching with verified filter
        kb_examples = get_matching_examples(sql)
        verified_examples = [ex for ex in kb_examples if ex.id in VERIFIED_TRANSFORMS]
        batches = _split_example_batches(verified_examples, batch_size=3)
        batches = batches[:4]
        logger.info(f"Final fallback: Using {len(verified_examples)} KB-verified examples")

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

    # Save all worker SQL immediately after receiving from LLM, before full validation
    if output_dir:
        from pathlib import Path
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        for r in results:
            sql_file = output_path / f"parallel_worker_{r.worker_id}.sql"
            sql_file.write_text(r.optimized_sql)
            logger.info(f"Saved Worker {r.worker_id} SQL to {sql_file}")

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
    output_dir: Optional[str] = None,
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
        output_dir: Optional directory to save intermediate SQL files immediately after LLM response

    Returns: (final_candidate, full_result, attempts_history)
    """
    logger.info(f"Starting Mode 1 (Retry) optimization with max {max_retries} attempts")

    plan_summary, plan_text, plan_context = _get_plan_context(sample_db, sql)
    base_prompt = _build_base_prompt(sql, plan_context)

    # Get ML-recommended examples using LIVE FAISS similarity (VERIFIED ONLY)
    ml_recs = get_recommendations_for_sql(sql, top_n=6)
    verified_recs = filter_to_verified(ml_recs)
    logger.info(f"LIVE FAISS: ML recs={len(ml_recs)}, verified={len(verified_recs)}")

    examples = []
    for ex_id in verified_recs:
        if len(examples) >= 3:
            break
        ex = load_example(ex_id)
        if ex:
            examples.append(ex)
            logger.debug(f"Loaded verified ML example: {ex_id}")

    # Pad with KB-matched VERIFIED examples if needed
    if len(examples) < 3:
        fallback = get_matching_examples(sql)
        for ex in fallback:
            if len(examples) >= 3:
                break
            if ex not in examples and ex.id in VERIFIED_TRANSFORMS:
                examples.append(ex)
                logger.debug(f"Added verified fallback example: {ex.id}")

    logger.info(f"Using {len(examples)} verified examples: {[e.id for e in examples]}")

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

        # Save SQL immediately after LLM response, before validation
        if output_dir:
            from pathlib import Path
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            sql_file = output_path / f"retry_attempt_{attempt}.sql"
            sql_file.write_text(result.optimized_sql)
            logger.info(f"Saved SQL to {sql_file}")

        attempts_history.append({
            "attempt": attempt,
            "status": result.status.value,
            "speedup": result.speedup if result.status == ValidationStatus.PASS else None,
            "error": result.error,
        })

        if result.status == ValidationStatus.PASS:
            # Success - validation passed! Benchmark on full DB and return
            logger.info(f"Attempt {attempt} succeeded on sample DB, benchmarking on full DB")
            full_validator = SQLValidator(database=full_db)
            full = full_validator.validate(sql, result.optimized_sql)
            full_result = FullRunResult(
                sample=result,
                full_status=full.status,
                full_speedup=full.speedup,
                full_error=full.errors[0] if full.errors else None,
            )

            if full.status == ValidationStatus.PASS:
                logger.info(f"✅ Mode 1 succeeded after {attempt} attempts: {full.speedup:.2f}x speedup")
                return result, full_result, attempts_history
            else:
                # Full DB validation failed - treat as error and retry
                logger.info(f"⚠️ Sample passed but full DB failed: {full_result.full_error}")
                error_hist = _build_history_section(result.response, full_result.full_error or "Full DB validation failed")
                error_history.append(error_hist)
        else:
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
    output_dir: Optional[str] = None,
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
        output_dir: Optional directory to save intermediate SQL files immediately after LLM response

    Returns: (best_candidate, best_full_result, iterations_history)
    """
    logger.info(f"Starting Mode 3 (Evolutionary) optimization with max {max_iterations} iterations")

    # Get ML-recommended examples using LIVE FAISS similarity (VERIFIED ONLY)
    all_examples = load_all_examples()
    verified_examples = [ex for ex in all_examples if ex.id in VERIFIED_TRANSFORMS]

    # Use LIVE FAISS ML recommendations to prioritize VERIFIED examples
    ml_recs = get_recommendations_for_sql(sql, top_n=12)
    verified_recs = filter_to_verified(ml_recs)
    logger.info(f"LIVE FAISS: ML recs={len(ml_recs)}, verified={len(verified_recs)}")

    # Map verified transform names to example objects
    example_by_id = {ex.id: ex for ex in verified_examples}

    # Build prioritized example list from verified ML recs
    prioritized_examples = []
    for rec_id in verified_recs:
        if rec_id in example_by_id:
            prioritized_examples.append(example_by_id[rec_id])

    # Pad with remaining verified examples
    for ex in verified_examples:
        if ex not in prioritized_examples:
            prioritized_examples.append(ex)

    logger.info(f"{len(prioritized_examples)} verified examples prioritized by FAISS similarity")

    # Create rotation sets from prioritized examples
    example_rotation = [
        prioritized_examples[0:3],   # Iteration 1: Top ML recs
        prioritized_examples[3:6],   # Iteration 2
        prioritized_examples[6:9],   # Iteration 3
        prioritized_examples[9:12],  # Iteration 4
        prioritized_examples[0:3],   # Iteration 5 (wrap around to top recs)
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
            successful_iterations = [it for it in iterations_history if it.get('status') == 'success']
            failed_iterations = [it for it in iterations_history if it.get('status') != 'success']

            if successful_iterations:
                history = "## Previous Successful Iterations\n\n"
                for it in successful_iterations:
                    history += f"### Iteration {it['iteration']}: {it.get('speedup', 0):.2f}x speedup ✓\n"
                    if it.get('transform'):
                        history += f"**Transform:** {it['transform']}\n"
                    history += "\n"

            if failed_iterations:
                history += "## Failed Attempts (avoid these approaches)\n"
                for it in failed_iterations:
                    history += f"- Iteration {it['iteration']}: {it.get('error', 'Unknown error')}\n"
                history += "\n"

            history += f"## Current Challenge\n"
            history += f"**Current best:** {best_speedup:.2f}x speedup\n"
            history += f"**Target:** {target_speedup}x\n"
            if best_speedup < target_speedup:
                history += f"**Gap:** {target_speedup - best_speedup:.2f}x needed\n\n"
            history += "Try a DIFFERENT approach to improve upon the current best.\n\n"

        # Call worker (no sample DB - directly benchmark on full)
        full_prompt = _build_prompt_with_examples(base_prompt, examples, plan_summary, history)

        # Generate optimization
        llm_client = _create_llm_client(provider, model)
        response = llm_client.analyze(full_prompt)

        # Save API response immediately, BEFORE any processing
        if output_dir:
            from pathlib import Path
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            # Save raw response
            response_file = output_path / f"iteration_{iteration}_response.txt"
            response_file.write_text(response)
            # Save prompt too
            prompt_file = output_path / f"iteration_{iteration}_prompt.txt"
            prompt_file.write_text(full_prompt)
            logger.info(f"Saved iteration {iteration} response to {response_file}")

        # Assemble SQL from response
        pipeline = DagV2Pipeline(current_sql, plan_context=plan_context)
        try:
            optimized_sql = pipeline.apply_response(response)
        except Exception as e:
            logger.error(f"Iteration {iteration} failed to assemble: {e}")
            iterations_history.append({
                "iteration": iteration,
                "status": "assembly_failed",
                "error": str(e),
            })
            continue

        # Save SQL immediately after assembly, before validation
        if output_dir:
            sql_file = output_path / f"iteration_{iteration}_optimized.sql"
            sql_file.write_text(optimized_sql)
            logger.info(f"Saved iteration {iteration} SQL to {sql_file}")

        # Benchmark on full DB
        validator = SQLValidator(database=full_db)
        full = validator.validate(current_sql, optimized_sql)

        if full.status == ValidationStatus.PASS:
            speedup = full.speedup
            improved = speedup > best_speedup
            logger.info(f"Iteration {iteration}: {speedup:.2f}x speedup {'(NEW BEST)' if improved else ''}")

            # Create candidate result
            candidate = CandidateResult(
                worker_id=iteration,
                optimized_sql=optimized_sql,
                status=full.status,
                speedup=speedup,
                error=None,
                prompt=full_prompt,
                response=response,
            )

            # Always save validated SQL with speedup info
            if output_dir:
                validated_file = output_path / f"iteration_{iteration}_validated_{speedup:.2f}x.sql"
                validated_file.write_text(optimized_sql)

            # Check if improved
            if improved:
                best_speedup = speedup
                best_candidate = candidate
                best_full_result = FullRunResult(
                    sample=candidate,
                    full_status=full.status,
                    full_speedup=speedup,
                    full_error=None,
                )
                # Update current SQL to best so far for next iteration
                current_sql = optimized_sql
                logger.info(f"New best: {best_speedup:.2f}x (iteration {iteration})")

            iterations_history.append({
                "iteration": iteration,
                "status": "success",
                "speedup": speedup,
                "improved": improved,
            })

            # Check if target met - early exit
            if speedup >= target_speedup:
                logger.info(f"Target {target_speedup}x met after {iteration} iterations: {speedup:.2f}x")
                break
        else:
            error_msg = full.errors[0] if full.errors else "Unknown validation error"
            logger.warning(f"Iteration {iteration} failed validation: {error_msg}")
            iterations_history.append({
                "iteration": iteration,
                "status": "validation_failed",
                "error": error_msg,
            })

    if best_candidate:
        logger.info(f"✅ Best result: {best_speedup:.2f}x after {len(iterations_history)} iterations")
    else:
        logger.info(f"❌ No successful optimizations in {max_iterations} iterations")

    return best_candidate, best_full_result, iterations_history

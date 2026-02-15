"""Test harness for oneshot patch optimization on Snowflake.

End-to-end flow:
1. Load query + build IR
2. Build oneshot prompt with all 5 families + gold examples
3. Call LLM to get 4 patch plans
4. Validate each patch (parse, columns, semantics, speedup)
5. Generate report with correlation analysis
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional

from qt_sql.ir import build_script_ir, render_ir_node_map, Dialect
from qt_sql.patches.oneshot_patch_prompt_builder import (
    build_oneshot_patch_prompt,
    load_gold_examples,
)
from qt_sql.patches.oneshot_patch_validator import (
    OnehotPatchValidator,
    save_validation_report,
)
from qt_shared.config import get_settings


logger = logging.getLogger(__name__)


# ── Configuration ────────────────────────────────────────────────────────────

SNOWFLAKE_BENCHMARK_PATH = Path(
    "packages/qt-sql/qt_sql/benchmarks/snowflake_tpcds"
)
GOLD_EXAMPLES_PATH = Path("packages/qt-sql/qt_sql/examples")
OUTPUT_PATH = Path("research/oneshot_patch_results")


# ── Query Loader ─────────────────────────────────────────────────────────────

def load_query(query_id: str, dialect: str = "snowflake") -> str:
    """Load query SQL from disk.

    Args:
        query_id: Query identifier (e.g., "query_21", "query_1")
        dialect: SQL dialect (snowflake, duckdb, postgres)

    Returns:
        Full query SQL as string
    """
    if dialect == "snowflake":
        query_path = SNOWFLAKE_BENCHMARK_PATH / "queries" / f"{query_id}.sql"
    elif dialect == "duckdb":
        query_path = Path(
            "packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds/queries"
        ) / f"{query_id}.sql"
    elif dialect == "postgres":
        query_path = Path(
            "packages/qt-sql/qt_sql/benchmarks/postgres_dsb_156/queries"
        ) / f"{query_id}.sql"
    else:
        raise ValueError(f"Unknown dialect: {dialect}")

    if not query_path.exists():
        raise FileNotFoundError(f"Query not found: {query_path}")

    return query_path.read_text()


def get_explain_plan(executor, sql: str) -> str:
    """Get EXPLAIN ANALYZE output for query.

    executor.explain(sql) already prepends EXPLAIN internally and
    returns a dict. We convert the dict to a readable text string.

    Args:
        executor: Database executor
        sql: Query SQL (raw — do NOT prepend EXPLAIN)

    Returns:
        EXPLAIN ANALYZE text output as string
    """
    try:
        import json as _json

        plan_dict = executor.explain(sql, analyze=True)
        # explain() returns dict — convert to readable text
        return _json.dumps(plan_dict, indent=2)
    except Exception as e:
        logger.warning(f"Could not get EXPLAIN plan: {e}")
        return "(EXPLAIN not available)"


# ── LLM Interface ────────────────────────────────────────────────────────────

def call_llm(prompt: str) -> str:
    """Call LLM with prompt, return raw response.

    Uses configured LLM provider (deepseek, anthropic, etc.)

    Args:
        prompt: Full prompt text

    Returns:
        Raw LLM response (JSON string with 4 patches)
    """
    settings = get_settings()

    if settings.llm_provider == "deepseek":
        from anthropic import Anthropic

        client = Anthropic(
            api_key=settings.deepseek_api_key,
            base_url="https://api.deepseek.com",
        )

        message = client.messages.create(
            model=settings.llm_model,
            max_tokens=8000,
            messages=[{"role": "user", "content": prompt}],
        )

        return message.content[0].text

    elif settings.llm_provider == "anthropic":
        from anthropic import Anthropic

        client = Anthropic(api_key=settings.anthropic_api_key)

        message = client.messages.create(
            model=settings.llm_model,
            max_tokens=8000,
            messages=[{"role": "user", "content": prompt}],
        )

        return message.content[0].text

    else:
        raise ValueError(f"Unknown LLM provider: {settings.llm_provider}")


# ── Main Test Functions ──────────────────────────────────────────────────────

def test_oneshot_patches(
    query_id: str,
    dialect: str = "snowflake",
    executor_dsn: Optional[str] = None,
    save_results: bool = True,
) -> Dict[str, Any]:
    """End-to-end test of oneshot patch optimization.

    Process:
    1. Load query and build IR
    2. Generate EXPLAIN ANALYZE
    3. Build oneshot prompt with all 5 families
    4. Call LLM for 4 patch plans
    5. Validate all patches
    6. Save results

    Args:
        query_id: Query identifier (e.g., "query_21")
        dialect: SQL dialect (snowflake, duckdb, postgres)
        executor_dsn: Database DSN (if None, skips speedup validation)
        save_results: Whether to save results to JSON file

    Returns:
        Dictionary with overall summary and per-patch results
    """

    logger.info(f"Testing oneshot patches for {query_id} ({dialect})")

    # Step 1: Load query
    logger.info("Step 1: Loading query...")
    try:
        original_sql = load_query(query_id, dialect)
        logger.debug(f"Query loaded, length={len(original_sql)} chars")
    except FileNotFoundError as e:
        logger.error(f"Query load failed: {e}")
        return {"status": "error", "error": str(e)}

    # Step 2: Build IR
    logger.info("Step 2: Building IR...")
    try:
        ir = build_script_ir(
            original_sql, Dialect[dialect.upper()]
        )
        logger.info("IR built successfully")
    except Exception as e:
        logger.error(f"IR build failed: {e}")
        return {"status": "error", "error": f"IR build: {str(e)}"}

    # Step 3: Get EXPLAIN (optional)
    logger.info("Step 3: Generating EXPLAIN ANALYZE...")
    if executor_dsn:
        try:
            from qt_sql.execution.factory import create_executor_from_dsn

            executor = create_executor_from_dsn(executor_dsn)
            explain_text = get_explain_plan(executor, original_sql)
            logger.debug(f"EXPLAIN retrieved, length={len(explain_text)} chars")
        except Exception as e:
            logger.warning(f"EXPLAIN generation failed: {e}")
            explain_text = "(EXPLAIN unavailable)"
    else:
        explain_text = "(No executor; EXPLAIN skipped)"

    # Step 4: Render IR node map
    logger.info("Step 4: Rendering IR node map...")
    try:
        ir_node_map = render_ir_node_map(ir)
        logger.debug(f"Node map rendered, {ir_node_map.count(chr(10))} lines")
    except Exception as e:
        logger.warning(f"Node map rendering failed: {e}")
        ir_node_map = "(Node map unavailable)"

    # Step 5: Load gold examples
    logger.info("Step 5: Loading gold examples for all 5 families...")
    try:
        gold_examples = load_gold_examples(dialect, str(GOLD_EXAMPLES_PATH))
        num_examples = len(gold_examples)
        logger.info(f"Loaded {num_examples} gold examples")
        if num_examples < 5:
            logger.warning(f"Only {num_examples} examples loaded (expected 5)")
    except Exception as e:
        logger.warning(f"Gold example loading failed: {e}")
        gold_examples = {}

    # Step 6: Build oneshot prompt
    logger.info("Step 6: Building oneshot prompt...")
    try:
        prompt = build_oneshot_patch_prompt(
            query_id=query_id,
            original_sql=original_sql,
            explain_text=explain_text,
            ir_node_map=ir_node_map,
            all_5_examples=gold_examples,
            dialect=dialect,
        )
        logger.info(f"Prompt built, length={len(prompt)} chars")
    except Exception as e:
        logger.error(f"Prompt building failed: {e}")
        return {"status": "error", "error": f"Prompt build: {str(e)}"}

    # Step 7: Call LLM
    logger.info("Step 7: Calling LLM for 4 patch plans...")
    try:
        llm_response = call_llm(prompt)
        logger.info(f"LLM response received, length={len(llm_response)} chars")
    except Exception as e:
        logger.error(f"LLM call failed: {e}")
        return {"status": "error", "error": f"LLM call: {str(e)}"}

    # Step 8: Validate patches
    logger.info("Step 8: Validating patches...")
    if executor_dsn:
        try:
            validator = OnehotPatchValidator(executor_dsn, dialect)
            report = validator.validate_response(
                query_id=query_id,
                original_sql=original_sql,
                original_ir=ir,
                llm_response=llm_response,
            )
            logger.info(
                f"Validation complete: {report.pass_count}/{len(report.patches)} passed"
            )
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return {
                "status": "error",
                "error": f"Validation: {str(e)}",
                "llm_response_preview": llm_response[:500],
            }
    else:
        logger.info("Skipping validation (no executor)")
        report = None

    # Step 9: Save results
    if save_results and report:
        logger.info("Step 9: Saving results...")
        try:
            OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
            output_file = (
                OUTPUT_PATH
                / f"{query_id}_{dialect}_oneshot_results.json"
            )
            save_validation_report(report, output_file)
            logger.info(f"Results saved to {output_file}")
        except Exception as e:
            logger.warning(f"Could not save results: {e}")

    # Return summary
    summary = {
        "status": "success",
        "query_id": query_id,
        "dialect": dialect,
        "prompt_length": len(prompt),
        "llm_response_length": len(llm_response),
    }

    if report:
        summary.update(
            {
                "overall_status": report.overall_status,
                "pass_count": report.pass_count,
                "fail_count": report.fail_count,
                "neutral_count": report.neutral_count,
                "avg_speedup": round(report.avg_speedup, 2),
                "llm_chosen_families": report.llm_chosen_families,
                "patches": [
                    {
                        "patch_id": p.patch_id,
                        "family": p.family,
                        "transform": p.transform,
                        "status": p.status,
                        "speedup": round(p.speedup, 2) if p.speedup else None,
                        "correlation": p.correlation_note,
                    }
                    for p in report.patches
                ],
            }
        )

    return summary


def test_multiple_queries(
    query_ids: list,
    dialect: str = "snowflake",
    executor_dsn: Optional[str] = None,
) -> Dict[str, Any]:
    """Test multiple queries in sequence.

    Args:
        query_ids: List of query IDs (e.g., ["query_21", "query_55"])
        dialect: SQL dialect
        executor_dsn: Database connection string

    Returns:
        Summary of all tests
    """
    results = {}
    success_count = 0
    fail_count = 0

    for query_id in query_ids:
        logger.info(f"\n{'='*60}")
        logger.info(f"Testing {query_id}...")
        logger.info(f"{'='*60}")

        try:
            result = test_oneshot_patches(
                query_id=query_id,
                dialect=dialect,
                executor_dsn=executor_dsn,
                save_results=True,
            )
            results[query_id] = result

            if result.get("status") == "success":
                success_count += 1
            else:
                fail_count += 1

        except Exception as e:
            logger.exception(f"Test failed for {query_id}")
            results[query_id] = {"status": "error", "error": str(e)}
            fail_count += 1

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"Success: {success_count}/{len(query_ids)}")
    logger.info(f"Failed: {fail_count}/{len(query_ids)}")

    return {
        "total_queries": len(query_ids),
        "success_count": success_count,
        "fail_count": fail_count,
        "results": results,
    }


# ── CLI Entry Point ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    if len(sys.argv) < 2:
        print("Usage: python test_oneshot_snowflake_patches.py <query_id> [dialect] [dsn]")
        print("  Example: python test_oneshot_snowflake_patches.py query_21 snowflake <DSN>")
        sys.exit(1)

    query_id = sys.argv[1]
    dialect = sys.argv[2] if len(sys.argv) > 2 else "snowflake"
    executor_dsn = sys.argv[3] if len(sys.argv) > 3 else None

    result = test_oneshot_patches(
        query_id=query_id,
        dialect=dialect,
        executor_dsn=executor_dsn,
        save_results=True,
    )

    print(json.dumps(result, indent=2))

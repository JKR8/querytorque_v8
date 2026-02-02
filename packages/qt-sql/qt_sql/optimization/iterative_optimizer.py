"""SQL optimizer with one-shot and iterative modes.

Two modes:
1. ONE-SHOT: Best possible prompt for single LLM call
2. AGENTIC LOOP: Iterative optimization with sample DB feedback

The key insight from Q23 research:
- Simple optimizations (predicate pushdown): one-shot works
- Complex optimizations (scan consolidation): need iteration

Usage:
    # One-shot
    prompt = build_oneshot_prompt(sql, plan_summary)
    response = llm.complete(prompt)
    result = apply_operations(sql, parse_response(response))

    # Agentic loop
    result = run_optimization_loop(sql, sample_db_path, llm_callback)
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import duckdb
import sqlglot
from sqlglot import exp

from .block_map import build_full_prompt, generate_block_map, format_block_map


@dataclass
class TestResult:
    """Result of testing an optimization on sample DB."""
    original_time: float
    optimized_time: float
    original_result: Any
    optimized_result: Any
    speedup: float
    semantically_correct: bool
    error: Optional[str] = None


def test_optimization(
    original_sql: str,
    optimized_sql: str,
    db_path: str,
    runs: int = 3,
) -> TestResult:
    """Test an optimization on the database.

    Runs both queries multiple times, discards first run (cache warmup),
    averages the rest. Compares results for semantic correctness.
    """
    conn = duckdb.connect(db_path, read_only=True)

    try:
        # Run original
        orig_times = []
        orig_result = None
        for i in range(runs):
            t1 = time.time()
            result = conn.execute(original_sql).fetchall()
            elapsed = time.time() - t1
            orig_times.append(elapsed)
            if i == 0:
                orig_result = result

        # Run optimized
        opt_times = []
        opt_result = None
        for i in range(runs):
            t1 = time.time()
            try:
                result = conn.execute(optimized_sql).fetchall()
                elapsed = time.time() - t1
                opt_times.append(elapsed)
                if i == 0:
                    opt_result = result
            except Exception as e:
                return TestResult(
                    original_time=sum(orig_times[1:]) / max(len(orig_times) - 1, 1),
                    optimized_time=0,
                    original_result=orig_result,
                    optimized_result=None,
                    speedup=0,
                    semantically_correct=False,
                    error=str(e),
                )

        # Calculate averages (discard first run)
        orig_avg = sum(orig_times[1:]) / max(len(orig_times) - 1, 1)
        opt_avg = sum(opt_times[1:]) / max(len(opt_times) - 1, 1)

        # Check semantic correctness
        semantically_correct = orig_result == opt_result

        speedup = orig_avg / opt_avg if opt_avg > 0 else 0

        return TestResult(
            original_time=orig_avg,
            optimized_time=opt_avg,
            original_result=orig_result,
            optimized_result=opt_result,
            speedup=speedup,
            semantically_correct=semantically_correct,
        )

    finally:
        conn.close()


def format_test_feedback(result: TestResult, iteration: int) -> str:
    """Format test result as feedback for the LLM."""
    lines = [f"## Iteration {iteration} Result"]

    if result.error:
        lines.append(f"❌ ERROR: {result.error}")
        lines.append("")
        lines.append("Fix the SQL syntax error and try again.")
        return "\n".join(lines)

    lines.append(f"Original:  {result.original_time:.3f}s")
    lines.append(f"Optimized: {result.optimized_time:.3f}s")
    lines.append(f"Speedup:   {result.speedup:.2f}x")

    if result.semantically_correct:
        lines.append("Semantics: ✅ CORRECT")
    else:
        lines.append("Semantics: ❌ INCORRECT")
        lines.append(f"  Original:  {result.original_result}")
        lines.append(f"  Optimized: {result.optimized_result}")
        lines.append("")
        lines.append("The optimization changed the query semantics. Common causes:")
        lines.append("- Removed a join without adding IS NOT NULL for the FK column")
        lines.append("- Added a filter that was intentionally missing (e.g., all-time vs period)")
        lines.append("")
        lines.append("Revert the breaking change and try a different approach.")

    if result.semantically_correct:
        if result.speedup < 1.0:
            lines.append("")
            lines.append("⚠️ Optimization made query SLOWER. Try a different approach:")
            lines.append("- Scan consolidation (CASE WHEN for conditional aggregates)")
            lines.append("- Join reordering (filter by smallest result first)")
        elif result.speedup < 1.1:
            lines.append("")
            lines.append("⚠️ Negligible speedup (<10%). Try more aggressive optimization:")
            lines.append("- Can multiple scans of same table be consolidated?")
            lines.append("- Can a join be eliminated with IS NOT NULL?")
        elif result.speedup >= 1.5:
            lines.append("")
            lines.append("✅ Good speedup achieved! Consider if further improvement possible.")

    return "\n".join(lines)


def apply_operations(sql: str, operations: list[dict]) -> str:
    """Apply a list of operations to SQL using AST manipulation.

    Operations are applied sequentially.

    Supported operations:
    - add_cte: {name, sql, after?} - Insert new CTE
    - delete_cte: {name} - Remove CTE
    - replace_cte: {name, sql} - Replace entire CTE body
    - replace_clause: {target, sql} - Replace specific clause
    - patch: {target, patches[]} - Search/replace within clause
    """
    result = sql

    for op in operations:
        op_type = op.get("op")

        if op_type == "add_cte":
            result = _add_cte(result, op["name"], op["sql"], op.get("after"))
        elif op_type == "delete_cte":
            result = _delete_cte(result, op["name"])
        elif op_type == "replace_cte":
            # Replace entire CTE body - uses same logic as replace_clause with CTE name
            result = _replace_cte(result, op["name"], op["sql"])
        elif op_type == "replace_clause":
            result = _replace_clause(result, op["target"], op["sql"])
        elif op_type == "patch":
            result = _apply_patches(result, op["target"], op["patches"])

    return result


def _add_cte(sql: str, name: str, cte_sql: str, after: Optional[str]) -> str:
    """Add a new CTE to the query using AST."""
    try:
        parsed = sqlglot.parse_one(sql)
        new_cte_query = sqlglot.parse_one(cte_sql)

        # Create the new CTE node
        new_cte = exp.CTE(this=new_cte_query, alias=exp.TableAlias(this=exp.Identifier(this=name)))

        # Find existing WITH clause
        with_clause = parsed.find(exp.With)

        if with_clause:
            ctes = list(with_clause.expressions)

            if after:
                # Find position after specified CTE
                insert_idx = len(ctes)
                for i, cte in enumerate(ctes):
                    if cte.alias and cte.alias.lower() == after.lower():
                        insert_idx = i + 1
                        break
                ctes.insert(insert_idx, new_cte)
            else:
                # Insert at beginning
                ctes.insert(0, new_cte)

            with_clause.set("expressions", ctes)
        else:
            # No WITH clause, create one
            new_with = exp.With(expressions=[new_cte])
            parsed.set("with", new_with)

        return parsed.sql(pretty=True)
    except Exception as e:
        # Fallback to original on parse error
        return sql


def _delete_cte(sql: str, name: str) -> str:
    """Delete a CTE from the query using AST."""
    try:
        parsed = sqlglot.parse_one(sql)
        with_clause = parsed.find(exp.With)

        if not with_clause:
            return sql

        ctes = list(with_clause.expressions)
        ctes = [cte for cte in ctes if not (cte.alias and cte.alias.lower() == name.lower())]

        if ctes:
            with_clause.set("expressions", ctes)
        else:
            # No CTEs left, remove WITH clause
            parsed.set("with", None)

        return parsed.sql(pretty=True)
    except Exception:
        return sql


def _replace_cte(sql: str, name: str, new_sql: str) -> str:
    """Replace the entire body of a CTE using AST.

    Args:
        sql: Original SQL query
        name: Name of CTE to replace
        new_sql: New CTE body (SELECT statement only, no CTE name)

    Returns:
        Modified SQL with CTE body replaced
    """
    try:
        parsed = sqlglot.parse_one(sql)
        with_clause = parsed.find(exp.With)

        if not with_clause:
            return sql

        for cte in with_clause.expressions:
            if cte.alias and cte.alias.lower() == name.lower():
                # Parse new CTE body
                new_body = sqlglot.parse_one(new_sql)
                cte.set("this", new_body)
                return parsed.sql(pretty=True)

        return sql  # CTE not found
    except Exception as e:
        # Fallback to original on parse error
        return sql


def _replace_clause(sql: str, target: str, new_sql: str) -> str:
    """Replace a clause in a block using AST.

    Target format:
    - "cte_name.clause" - replace specific clause
    - "cte_name" - replace entire CTE body
    - "main_query.clause" - replace main query clause
    """
    parts = target.split(".")

    # If just CTE name with no clause, replace entire CTE body
    if len(parts) == 1:
        cte_name = parts[0]
        if cte_name == "main_query":
            return sql  # Can't replace entire main query this way
        try:
            parsed = sqlglot.parse_one(sql)
            with_clause = parsed.find(exp.With)
            if with_clause:
                for cte in with_clause.expressions:
                    if cte.alias and cte.alias.lower() == cte_name.lower():
                        # Parse new CTE body
                        new_body = sqlglot.parse_one(new_sql)
                        cte.set("this", new_body)
                        return parsed.sql(pretty=True)
        except Exception:
            pass
        return sql

    block_name = parts[0]
    clause_name = parts[-1]

    try:
        parsed = sqlglot.parse_one(sql)

        # Find the target SELECT node
        target_select = None

        if block_name == "main_query":
            # Find the main query (not inside a CTE)
            for node in parsed.walk():
                if isinstance(node, exp.Select):
                    # Check it's not inside a CTE
                    parent = node.parent
                    in_cte = False
                    while parent:
                        if isinstance(parent, exp.CTE):
                            in_cte = True
                            break
                        parent = parent.parent
                    if not in_cte:
                        target_select = node
                        break
        else:
            # Find the CTE with this name
            with_clause = parsed.find(exp.With)
            if with_clause:
                for cte in with_clause.expressions:
                    if cte.alias and cte.alias.lower() == block_name.lower():
                        # Find the SELECT inside this CTE
                        target_select = cte.this
                        if not isinstance(target_select, exp.Select):
                            for sel in cte.this.walk():
                                if isinstance(sel, exp.Select):
                                    target_select = sel
                                    break
                        break

        if not target_select:
            return sql

        # Replace the clause
        if clause_name == "select":
            new_exprs = sqlglot.parse_one(f"SELECT {new_sql}").expressions
            target_select.set("expressions", new_exprs)
        elif clause_name == "from":
            # Parse the new FROM clause
            new_select = sqlglot.parse_one(f"SELECT x FROM {new_sql}")
            new_from = new_select.find(exp.From)
            # Get only direct joins (not nested in subqueries)
            new_joins = new_select.args.get("joins") or []

            # Set FROM and direct joins only
            target_select.set("from", new_from)
            target_select.set("joins", list(new_joins) if new_joins else None)
        elif clause_name == "where":
            if new_sql:
                new_where = sqlglot.parse_one(f"SELECT x WHERE {new_sql}").find(exp.Where)
                target_select.set("where", new_where)
            else:
                target_select.set("where", None)
        elif clause_name == "group_by":
            if new_sql:
                new_group = sqlglot.parse_one(f"SELECT x GROUP BY {new_sql}").find(exp.Group)
                target_select.set("group", new_group)
            else:
                target_select.set("group", None)
        elif clause_name == "having":
            if new_sql:
                new_having = sqlglot.parse_one(f"SELECT x HAVING {new_sql}").find(exp.Having)
                target_select.set("having", new_having)
            else:
                target_select.set("having", None)
        elif clause_name == "order_by":
            if new_sql:
                new_order = sqlglot.parse_one(f"SELECT x ORDER BY {new_sql}").find(exp.Order)
                target_select.set("order", new_order)
            else:
                target_select.set("order", None)

        return parsed.sql(pretty=True)
    except Exception as e:
        # Fallback to original on error
        return sql


def _apply_patches(sql: str, target: str, patches: list[dict]) -> str:
    """Apply search/replace patches to SQL (simple string replacement)."""
    result = sql
    for patch in patches:
        search = patch.get("search", "")
        replace = patch.get("replace", "")
        if search and search in result:
            result = result.replace(search, replace, 1)
    return result


def parse_response(response: str) -> dict:
    """Parse LLM response to extract operations JSON."""
    text = response.strip()

    # Remove markdown code blocks
    if text.startswith("```"):
        first_newline = text.find("\n")
        if first_newline != -1:
            text = text[first_newline + 1:]
        if text.endswith("```"):
            text = text[:-3].strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to find JSON object
        start = text.find("{")
        end = text.rfind("}") + 1
        if start != -1 and end > start:
            try:
                return json.loads(text[start:end])
            except json.JSONDecodeError:
                pass

        return {"error": "Could not parse JSON", "operations": []}


def run_optimization_loop(
    sql: str,
    sample_db_path: str,
    llm_callback: Callable[[str], str],
    max_iterations: int = 5,
    target_speedup: float = 1.5,
    plan_summary: Optional[dict] = None,
) -> dict:
    """Run iterative optimization loop.

    1. Generate initial prompt with Block Map
    2. Get LLM response with operations
    3. Apply operations and test on sample DB
    4. If not good enough, feed back results and iterate

    Args:
        sql: Original SQL query
        sample_db_path: Path to sample database for testing
        llm_callback: Function(prompt) -> response string with JSON operations
        max_iterations: Maximum optimization iterations
        target_speedup: Stop early if we reach this speedup
        plan_summary: Optional execution plan summary

    Returns:
        dict with best_sql, best_speedup, iterations, semantically_correct
    """
    current_sql = sql
    best_sql = sql
    best_speedup = 1.0
    history = []

    # Build initial prompt
    prompt = build_full_prompt(sql, plan_summary)

    for iteration in range(1, max_iterations + 1):
        # Get LLM response
        response = llm_callback(prompt)

        # Parse operations
        parsed = parse_response(response)
        operations = parsed.get("operations", [])
        explanation = parsed.get("explanation", "")

        if not operations:
            history.append({
                "iteration": iteration,
                "error": "No operations returned",
                "response": response[:500],
            })
            break

        # Apply operations
        try:
            optimized_sql = apply_operations(current_sql, operations)
        except Exception as e:
            history.append({
                "iteration": iteration,
                "error": f"Failed to apply operations: {e}",
                "operations": operations,
            })
            # Add error feedback and continue
            prompt += f"\n\n---\n\n## Iteration {iteration} Error\n"
            prompt += f"Failed to apply operations: {e}\n"
            prompt += "Check operation syntax and try again."
            continue

        # Test on sample DB
        result = test_optimization(sql, optimized_sql, sample_db_path)

        history.append({
            "iteration": iteration,
            "operations": operations,
            "explanation": explanation,
            "speedup": result.speedup,
            "correct": result.semantically_correct,
            "error": result.error,
        })

        # Update best if semantically correct and faster
        if result.semantically_correct and result.speedup > best_speedup:
            best_sql = optimized_sql
            best_speedup = result.speedup
            current_sql = optimized_sql

        # Check if we've reached target
        if result.speedup >= target_speedup and result.semantically_correct:
            break

        # Generate feedback for next iteration
        feedback = format_test_feedback(result, iteration)
        prompt += f"\n\n---\n\n{feedback}"

        # Add Block Map of current state for context
        if result.semantically_correct and result.speedup > 1.0:
            prompt += "\n\nCurrent optimized query structure:\n"
            block_map = generate_block_map(current_sql)
            prompt += format_block_map(block_map)

    return {
        "original_sql": sql,
        "best_sql": best_sql,
        "best_speedup": best_speedup,
        "semantically_correct": best_speedup > 1.0,  # If we improved, it's correct
        "iterations": history,
    }

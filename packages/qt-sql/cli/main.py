"""QueryTorque SQL CLI.

Command-line interface for SQL analysis and optimization.

Commands:
    qt-sql audit <file.sql>              Static analysis, generate report
    qt-sql audit <file.sql> --calcite    Include Calcite optimization
    qt-sql optimize <file.sql>           LLM-powered optimization
    qt-sql validate <orig.sql> <opt.sql> Validate optimization equivalence
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.syntax import Syntax
from rich.markdown import Markdown

from qt_sql.analyzers.sql_antipattern_detector import SQLAntiPatternDetector, SQLAnalysisResult
from qt_sql.calcite_client import CalciteClient, get_calcite_client, CalciteResult

console = Console()


def read_sql_file(file_path: str) -> str:
    """Read SQL from file."""
    path = Path(file_path)
    if not path.exists():
        raise click.ClickException(f"File not found: {file_path}")
    if not path.suffix.lower() in (".sql", ".txt"):
        raise click.ClickException(f"Expected .sql file, got: {path.suffix}")
    return path.read_text(encoding="utf-8")


def display_analysis_result(result: SQLAnalysisResult, verbose: bool = False) -> None:
    """Display analysis result with rich formatting."""
    # Score panel
    score_color = "green" if result.final_score >= 80 else "yellow" if result.final_score >= 60 else "red"
    score_text = f"[bold {score_color}]{result.final_score}/100[/bold {score_color}]"

    severity_summary = (
        f"Critical: {result.critical_count} | "
        f"High: {result.high_count} | "
        f"Medium: {result.medium_count} | "
        f"Low: {result.low_count}"
    )

    console.print(Panel(
        f"Score: {score_text}\n{severity_summary}",
        title="SQL Analysis Result",
        border_style=score_color
    ))

    if not result.issues:
        console.print("[green]No issues detected.[/green]")
        return

    # Issues table
    table = Table(title="Detected Issues", show_header=True, header_style="bold")
    table.add_column("Severity", style="bold", width=10)
    table.add_column("Rule", width=15)
    table.add_column("Description", width=50)
    table.add_column("Penalty", justify="right", width=8)

    severity_colors = {
        "critical": "red",
        "high": "orange3",
        "medium": "yellow",
        "low": "blue",
        "info": "dim",
    }

    for issue in result.issues:
        color = severity_colors.get(issue.severity, "white")
        table.add_row(
            f"[{color}]{issue.severity.upper()}[/{color}]",
            issue.rule_id,
            issue.description[:50] + "..." if len(issue.description) > 50 else issue.description,
            str(issue.penalty)
        )

    console.print(table)

    # Detailed issues in verbose mode
    if verbose:
        console.print("\n[bold]Issue Details:[/bold]\n")
        for i, issue in enumerate(result.issues, 1):
            console.print(f"[bold]{i}. {issue.name}[/bold] ({issue.rule_id})")
            console.print(f"   [dim]Category:[/dim] {issue.category}")
            console.print(f"   [dim]Description:[/dim] {issue.description}")
            if issue.suggestion:
                console.print(f"   [dim]Suggestion:[/dim] {issue.suggestion}")
            if issue.location:
                console.print(f"   [dim]Location:[/dim] {issue.location}")
            console.print()


def display_calcite_result(result: CalciteResult) -> None:
    """Display Calcite optimization result."""
    if not result.success:
        console.print(f"[red]Calcite optimization failed: {result.error}[/red]")
        return

    if not result.query_changed:
        console.print("[yellow]Calcite: No optimization opportunities found.[/yellow]")
        return

    console.print(Panel(
        f"[green]Query optimized by Calcite[/green]\n"
        f"Rules applied: {', '.join(result.rules_applied) if result.rules_applied else 'N/A'}",
        title="Calcite Optimization",
        border_style="green"
    ))

    if result.optimized_sql:
        console.print("\n[bold]Optimized SQL:[/bold]")
        console.print(Syntax(result.optimized_sql, "sql", theme="monokai", line_numbers=True))

    if result.improvement_percent is not None:
        console.print(f"\n[green]Performance improvement: {result.improvement_percent:.1f}%[/green]")

    if result.original_cost is not None and result.optimized_cost is not None:
        console.print(
            f"Cost reduction: {result.original_cost:.2f} -> {result.optimized_cost:.2f}"
        )


@click.group()
@click.version_option(version="0.1.0", prog_name="qt-sql")
def cli():
    """QueryTorque SQL - SQL Analysis and Optimization CLI."""
    pass


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("--calcite", is_flag=True, help="Include Calcite optimization")
@click.option("--dialect", default="generic", help="SQL dialect (generic, snowflake, postgres, duckdb, tsql)")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed issue information")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
@click.option("--calcite-url", default=None, help="Calcite service URL (default: http://localhost:8001)")
def audit(
    file: str,
    calcite: bool,
    dialect: str,
    verbose: bool,
    output_json: bool,
    calcite_url: Optional[str]
):
    """Analyze SQL file for anti-patterns and issues.

    Performs static analysis on the SQL file and generates a report
    with detected issues, severity levels, and improvement suggestions.

    Examples:
        qt-sql audit query.sql
        qt-sql audit query.sql --calcite
        qt-sql audit query.sql --dialect snowflake -v
    """
    try:
        sql = read_sql_file(file)
    except click.ClickException as e:
        console.print(f"[red]Error: {e.message}[/red]")
        sys.exit(1)

    # Run static analysis
    detector = SQLAntiPatternDetector(dialect=dialect)
    result = detector.analyze(sql, include_structure=True)

    if output_json:
        import json
        output = result.to_dict()
        output["file"] = file
        output["dialect"] = dialect
        console.print_json(json.dumps(output))
        return

    console.print(f"\n[bold]Analyzing:[/bold] {file}")
    console.print(f"[dim]Dialect: {dialect}[/dim]\n")

    display_analysis_result(result, verbose=verbose)

    # Run Calcite optimization if requested
    if calcite:
        console.print("\n[bold]Running Calcite optimization...[/bold]")

        async def run_calcite():
            client = get_calcite_client(base_url=calcite_url)
            return await client.optimize(sql)

        calcite_result = asyncio.run(run_calcite())
        display_calcite_result(calcite_result)

    # Exit with non-zero if critical/high issues found
    if result.critical_count > 0 or result.high_count > 0:
        sys.exit(1)


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("--dialect", default="duckdb", help="SQL dialect (duckdb, postgres, snowflake, generic)")
@click.option("--database", "-d", type=click.Path(), help="DuckDB database path for schema/execution plan context")
@click.option("--provider", default=None, help="LLM provider (anthropic, deepseek, openai, groq, gemini, openrouter)")
@click.option("--model", default=None, help="LLM model name")
@click.option("--output", "-o", type=click.Path(), help="Output file for optimized SQL")
@click.option("--dry-run", is_flag=True, help="Show prompt without calling LLM")
@click.option("--show-prompt", is_flag=True, help="Display the full prompt sent to LLM")
@click.option(
    "--mode",
    type=click.Choice(["dag", "full", "mcts"]),
    default="dag",
    help="Optimization mode: dag (default), full (full SQL), mcts (tree search)"
)
@click.option("--mcts-iterations", default=30, help="Maximum MCTS iterations (default: 30)")
def optimize(
    file: str,
    dialect: str,
    database: Optional[str],
    provider: Optional[str],
    model: Optional[str],
    output: Optional[str],
    dry_run: bool,
    show_prompt: bool,
    mode: str,
    mcts_iterations: int,
):
    """Optimize SQL using LLM-powered analysis.

    Modes:
    - dag-v2 (default): DAG-based node-level optimization with contracts
    - full: Full SQL replacement optimization
    - mcts: MCTS-guided tree search with validation

    When a --database is provided, includes schema context and execution plan
    in the prompt for better optimization recommendations.

    Examples:
        qt-sql optimize query.sql
        qt-sql optimize query.sql --database /path/to/data.duckdb
        qt-sql optimize query.sql -d mydata.duckdb --provider deepseek
        qt-sql optimize query.sql -o optimized.sql --mode dag-v2
        qt-sql optimize query.sql -d data.duckdb --mode mcts --mcts-iterations 30
    """
    try:
        sql = read_sql_file(file)
    except click.ClickException as e:
        console.print(f"[red]Error: {e.message}[/red]")
        sys.exit(1)

    # Run static analysis
    detector = SQLAntiPatternDetector(dialect=dialect)
    result = detector.analyze(sql, include_structure=True)

    console.print(f"\n[bold]Analyzing:[/bold] {file}")
    console.print(f"[dim]Dialect: {dialect}[/dim]")
    if database:
        console.print(f"[dim]Database: {database}[/dim]")
    console.print()

    display_analysis_result(result, verbose=False)

    # Convert issues to dict format for payload builder
    issues = [
        {
            "rule_id": issue.rule_id,
            "severity": issue.severity,
            "title": issue.name,
            "description": issue.description,
            "suggestion": issue.suggestion,
            "location": issue.location,
        }
        for issue in result.issues
    ]

    # Fetch schema and execution plan if database provided
    schema_context = None
    execution_plan = None
    engine_info = None

    if database:
        console.print("[dim]Fetching schema and execution plan...[/dim]")
        try:
            from qt_sql.execution.database_utils import (
                fetch_schema_with_stats,
                run_explain_text,
                get_duckdb_engine_info,
            )

            schema_context = fetch_schema_with_stats(database, sql)
            if schema_context:
                table_count = len(schema_context.get("tables", []))
                console.print(f"  [green]Schema loaded:[/green] {table_count} tables")

            explain_text = run_explain_text(database, sql)
            if explain_text:
                execution_plan = {"explain_analyze_text": explain_text}
                console.print(f"  [green]Execution plan captured[/green]")

            engine_info = get_duckdb_engine_info(database)

        except Exception as e:
            console.print(f"  [yellow]Warning: Could not fetch context: {e}[/yellow]")

    # Build optimization payload
    try:
        from qt_sql.optimization.payload_builder import build_optimization_payload_v2

        payload_result = build_optimization_payload_v2(
            code=sql,
            query_type="sql",
            file_name=Path(file).name,
            issues=issues,
            schema_context=schema_context,
            explain_analyze_text=execution_plan.get("explain_analyze_text") if execution_plan else None,
            engine_info=engine_info,
        )
        prompt = payload_result.payload_yaml
        console.print(f"[dim]Prompt size: ~{payload_result.estimated_tokens} tokens[/dim]\n")

    except ImportError:
        # Fallback to simple prompt if payload builder not available
        console.print("[yellow]Using simple prompt (payload builder not available)[/yellow]\n")
        issues_text = "\n".join(
            f"- {issue['title']} ({issue['severity']}): {issue['description']}"
            for issue in issues
        )
        prompt = f"""You are a SQL optimization expert. Optimize the following query.

## SQL
```sql
{sql}
```

## Detected Issues
{issues_text}

## Instructions
Provide an optimized SQL query that fixes the issues while preserving semantics.
Return your response as JSON:
{{"optimized_sql": "<your optimized query>", "explanation": "<what you changed>"}}
"""

    if show_prompt:
        console.print("[bold]Full Prompt:[/bold]")
        console.print(Panel(prompt, title="LLM Prompt", border_style="dim"))

    if dry_run:
        console.print("\n[yellow]Dry run mode - LLM call skipped.[/yellow]")
        if not show_prompt:
            console.print("[dim]Use --show-prompt to see the full prompt.[/dim]")
        return

    # MCTS optimization mode
    if mode == "mcts":
        if not database:
            console.print("[red]MCTS optimization requires --database for validation.[/red]")
            sys.exit(1)

        try:
            from qt_sql.optimization import MCTS_AVAILABLE, MCTSSQLOptimizer

            if not MCTS_AVAILABLE:
                console.print("[red]MCTS optimizer not available. Check dependencies.[/red]")
                sys.exit(1)

            console.print(f"\n[bold]Running MCTS optimization (max {mcts_iterations} iterations)...[/bold]")

            with MCTSSQLOptimizer(
                database=database,
                provider=provider,
                model=model,
            ) as optimizer:
                mcts_result = optimizer.optimize(
                    query=sql,
                    max_iterations=mcts_iterations,
                )

            # Display results
            if mcts_result.valid:
                status_color = "green" if mcts_result.speedup > 1.0 else "yellow"
                console.print(Panel(
                    f"[bold {status_color}]Speedup: {mcts_result.speedup:.2f}x[/bold {status_color}]\n"
                    f"Method: {mcts_result.method}\n"
                    f"Iterations: {mcts_result.iterations}\n"
                    f"Time: {mcts_result.elapsed_time:.1f}s",
                    title="MCTS Optimization Result",
                    border_style=status_color,
                ))

                if mcts_result.transforms_applied:
                    console.print(f"\n[dim]Transforms: {' -> '.join(mcts_result.transforms_applied)}[/dim]")

                console.print("\n[bold]Optimized SQL:[/bold]")
                console.print(Syntax(mcts_result.optimized_sql, "sql", theme="monokai", line_numbers=True))

                if output:
                    Path(output).write_text(mcts_result.optimized_sql, encoding="utf-8")
                    console.print(f"\n[green]Optimized SQL saved to: {output}[/green]")
            else:
                console.print("[yellow]No valid optimization found.[/yellow]")

            # Show tree stats
            stats = mcts_result.tree_stats
            console.print(f"\n[dim]Tree stats: {stats.get('tree_size', 0)} nodes, "
                         f"{stats.get('successful_expansions', 0)} successful transforms, "
                         f"{stats.get('validation_calls', 0)} validations[/dim]")

        except Exception as e:
            console.print(f"[red]MCTS optimization failed: {e}[/red]")
            import traceback
            console.print(f"[dim]{traceback.format_exc()}[/dim]")
            sys.exit(1)

        return

    # DAG v2 optimization mode (default)
    if mode == "dag-v2":
        try:
            from qt_sql.optimization.dag_v2 import DagV2Pipeline, get_dag_v2_examples

            console.print(f"\n[bold]Running DAG v2 optimization...[/bold]")

            # Build pipeline
            pipeline = DagV2Pipeline(sql)

            # Show DAG structure
            console.print(f"[dim]{pipeline.get_dag_summary()}[/dim]\n")

            # Build prompt with few-shot examples
            examples = get_dag_v2_examples()
            few_shot_parts = []
            for ex in examples[:2]:
                few_shot_parts.append(f"### Example: {ex['opportunity']}")
                few_shot_parts.append(f"Input:\n{ex['input_slice']}")
                import json as json_mod
                few_shot_parts.append(f"Output:\n```json\n{json_mod.dumps(ex['output'], indent=2)}\n```")
                if 'key_insight' in ex:
                    few_shot_parts.append(f"Key insight: {ex['key_insight']}")
                few_shot_parts.append("")

            few_shot = "\n".join(few_shot_parts)
            base_prompt = pipeline.get_prompt()
            full_prompt = f"## Examples\n\n{few_shot}\n\n---\n\n## Your Task\n\n{base_prompt}"

            if show_prompt:
                console.print("[bold]Full Prompt:[/bold]")
                console.print(Panel(full_prompt, title="DAG v2 Prompt", border_style="dim"))

            if dry_run:
                console.print("\n[yellow]Dry run mode - LLM call skipped.[/yellow]")
                return

            # Call LLM
            from qt_shared.llm import create_llm_client
            llm_client = create_llm_client(provider=provider, model=model)

            if llm_client is None:
                console.print(
                    "[red]No LLM provider configured. "
                    "Set QT_LLM_PROVIDER and API key environment variables.[/red]"
                )
                sys.exit(1)

            console.print("[bold]Requesting LLM optimization...[/bold]")
            response = llm_client.analyze(full_prompt)

            # Apply response to get optimized SQL
            optimized_sql = pipeline.apply_response(response)

            console.print("\n[bold green]DAG v2 Optimization Result:[/bold green]")
            console.print(Syntax(optimized_sql, "sql", theme="monokai", line_numbers=True))

            if output:
                Path(output).write_text(optimized_sql, encoding="utf-8")
                console.print(f"\n[green]Optimized SQL saved to: {output}[/green]")

        except ImportError as e:
            console.print(f"[red]DAG v2 optimizer not available: {e}[/red]")
            sys.exit(1)
        except Exception as e:
            console.print(f"[red]DAG v2 optimization failed: {e}[/red]")
            import traceback
            console.print(f"[dim]{traceback.format_exc()}[/dim]")
            sys.exit(1)

        return

    # Full SQL optimization mode
    if not issues and not database:
        console.print("\n[green]No issues found - query already looks optimal.[/green]")
        return

    # Create LLM client for full mode
    try:
        from qt_shared.llm import create_llm_client

        llm_client = create_llm_client(provider=provider, model=model)

        if llm_client is None:
            console.print(
                "[red]No LLM provider configured. "
                "Set QT_LLM_PROVIDER and API key environment variables.[/red]"
            )
            sys.exit(1)

    except ImportError:
        console.print("[red]qt_shared package not installed. Cannot use LLM optimization.[/red]")
        sys.exit(1)

    console.print("[bold]Requesting LLM optimization...[/bold]")

    try:
        response = llm_client.analyze(prompt)

        console.print("\n[bold green]LLM Optimization Result:[/bold green]")
        console.print(Markdown(response))

        # Extract optimized SQL if output file requested
        if output:
            import re
            # Try JSON format first
            import json
            try:
                # Find JSON in response
                json_match = re.search(r'\{[^{}]*"optimized_sql"[^{}]*\}', response, re.DOTALL)
                if json_match:
                    parsed = json.loads(json_match.group())
                    optimized_sql = parsed.get("optimized_sql", "").strip()
                    if optimized_sql:
                        Path(output).write_text(optimized_sql, encoding="utf-8")
                        console.print(f"\n[green]Optimized SQL saved to: {output}[/green]")
                        return
            except (json.JSONDecodeError, AttributeError):
                pass

            # Fallback to markdown code block
            sql_match = re.search(r"```sql\s*(.*?)\s*```", response, re.DOTALL)
            if sql_match:
                optimized_sql = sql_match.group(1).strip()
                Path(output).write_text(optimized_sql, encoding="utf-8")
                console.print(f"\n[green]Optimized SQL saved to: {output}[/green]")
            else:
                console.print("[yellow]Could not extract optimized SQL from response.[/yellow]")

    except Exception as e:
        console.print(f"[red]LLM optimization failed: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.argument("original", type=click.Path(exists=True))
@click.argument("optimized", type=click.Path(exists=True))
@click.option("--database", "-d", default=":memory:", help="DuckDB database path for validation")
@click.option("--schema", type=click.Path(exists=True), help="SQL file with schema creation statements")
@click.option(
    "--mode",
    type=click.Choice(["sample", "full"]),
    default="sample",
    help="Validation mode: sample (1%% DB, signal) or full (full DB, confidence)"
)
@click.option("--sample-pct", type=float, default=1.0, help="Sample percentage for sample mode (default: 1.0)")
@click.option(
    "--limit-strategy",
    type=click.Choice(["add_order", "remove_limit"]),
    default="add_order",
    help="Strategy for LIMIT without ORDER BY"
)
@click.option("--verbose", "-v", is_flag=True, help="Show detailed output")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def validate(
    original: str,
    optimized: str,
    database: str,
    schema: Optional[str],
    mode: str,
    sample_pct: float,
    limit_strategy: str,
    verbose: bool,
    output_json: bool,
):
    """Validate that optimized SQL is equivalent to original.

    Uses the 1-1-2-2 benchmarking pattern:
    - Run original (warmup), run original (measure)
    - Run optimized (warmup), run optimized (measure)

    Two modes:
    - sample (default): Uses sample DB for signal (fast but approximate)
    - full: Uses full DB for confidence (slower but accurate)

    Both modes validate row counts AND values (checksum comparison).

    Examples:
        qt-sql validate original.sql optimized.sql --database tpcds.duckdb
        qt-sql validate original.sql optimized.sql -d data.duckdb --mode full
        qt-sql validate original.sql optimized.sql --schema schema.sql
        qt-sql validate original.sql optimized.sql -d data.duckdb --verbose
    """
    try:
        original_sql = read_sql_file(original)
        optimized_sql = read_sql_file(optimized)
        schema_sql = read_sql_file(schema) if schema else None
    except click.ClickException as e:
        console.print(f"[red]Error: {e.message}[/red]")
        sys.exit(1)

    # Import validation module
    try:
        from qt_sql.validation import (
            SQLValidator,
            ValidationMode,
            ValidationStatus,
            LimitStrategy,
        )
    except ImportError as e:
        console.print(f"[red]Validation module not available: {e}[/red]")
        sys.exit(1)

    # Parse options
    validation_mode = ValidationMode.FULL if mode == "full" else ValidationMode.SAMPLE
    limit_strat = LimitStrategy.REMOVE_LIMIT if limit_strategy == "remove_limit" else LimitStrategy.ADD_ORDER

    if not output_json:
        console.print(f"\n[bold]Validating optimization equivalence[/bold]")
        console.print(f"Original: {original}")
        console.print(f"Optimized: {optimized}")
        console.print(f"Database: {database}")
        console.print(f"Mode: {mode}")
        console.print()

    # Run validation
    try:
        with SQLValidator(
            database=database,
            mode=validation_mode,
            sample_pct=sample_pct,
            limit_strategy=limit_strat,
        ) as validator:
            result = validator.validate(original_sql, optimized_sql, schema_sql)
    except Exception as e:
        if output_json:
            import json
            console.print_json(json.dumps({"status": "error", "error": str(e)}))
        else:
            console.print(f"[red]Validation failed: {e}[/red]")
        sys.exit(1)

    # Output results
    if output_json:
        import json
        console.print_json(json.dumps(result.to_dict()))
        sys.exit(0 if result.status == ValidationStatus.PASS else 1)

    # Rich console output
    status_color = {
        ValidationStatus.PASS: "green",
        ValidationStatus.FAIL: "red",
        ValidationStatus.WARN: "yellow",
        ValidationStatus.ERROR: "red",
    }.get(result.status, "white")

    # Status panel
    console.print(Panel(
        f"[bold {status_color}]{result.status.value.upper()}[/bold {status_color}]",
        title="Validation Result",
        border_style=status_color,
    ))

    # Results table
    table = Table(show_header=True, header_style="bold")
    table.add_column("Metric", width=20)
    table.add_column("Original", justify="right", width=15)
    table.add_column("Optimized", justify="right", width=15)
    table.add_column("Status", width=20)

    # Row counts
    row_status = "[green]match[/green]" if result.row_counts_match else "[red]MISMATCH[/red]"
    table.add_row(
        "Row Count",
        str(result.original_row_count),
        str(result.optimized_row_count),
        row_status,
    )

    # Timing
    speedup_str = f"{result.speedup:.2f}x"
    if result.speedup > 1:
        timing_status = f"[green]{speedup_str} faster[/green]"
    elif result.speedup < 1:
        timing_status = f"[yellow]{speedup_str} slower[/yellow]"
    else:
        timing_status = "unchanged"
    table.add_row(
        "Timing",
        f"{result.original_timing_ms:.1f}ms",
        f"{result.optimized_timing_ms:.1f}ms",
        timing_status,
    )

    # Cost
    if result.cost_reduction_pct > 0:
        cost_status = f"[green]-{result.cost_reduction_pct:.1f}%[/green]"
    elif result.cost_reduction_pct < 0:
        cost_status = f"[yellow]+{abs(result.cost_reduction_pct):.1f}%[/yellow]"
    else:
        cost_status = "unchanged"
    table.add_row(
        "Cost",
        f"{result.original_cost:.0f}",
        f"{result.optimized_cost:.0f}",
        cost_status,
    )

    # Values
    if result.checksum_match:
        values_status = "[green]checksum match[/green]"
    elif result.values_match:
        values_status = "[green]values match[/green]"
    else:
        values_status = f"[red]MISMATCH ({len(result.value_differences)} diffs)[/red]"
    table.add_row("Values", "-", "-", values_status)

    console.print(table)

    # LIMIT normalization warning
    if result.limit_detected:
        console.print(
            f"\n[yellow]Note: LIMIT without ORDER BY detected. "
            f"Applied '{result.limit_strategy_applied.value if result.limit_strategy_applied else 'none'}' strategy.[/yellow]"
        )

    # Verbose output
    if verbose:
        if result.normalized_original_sql:
            console.print("\n[bold]Normalized Original SQL:[/bold]")
            console.print(Syntax(result.normalized_original_sql, "sql", theme="monokai"))

        if result.normalized_optimized_sql:
            console.print("\n[bold]Normalized Optimized SQL:[/bold]")
            console.print(Syntax(result.normalized_optimized_sql, "sql", theme="monokai"))

        if result.value_differences:
            console.print(f"\n[bold]Value Differences (first {len(result.value_differences)}):[/bold]")
            for diff in result.value_differences[:5]:
                console.print(
                    f"  Row {diff.row_index}, Column '{diff.column}': "
                    f"{diff.original_value!r} vs {diff.optimized_value!r}"
                )

    # Warnings
    for warning in result.warnings:
        console.print(f"[yellow]Warning: {warning}[/yellow]")

    # Errors
    for error in result.errors:
        console.print(f"[red]Error: {error}[/red]")

    # Exit code
    if result.status == ValidationStatus.PASS:
        console.print("\n[green]Validation passed.[/green]")
        sys.exit(0)
    else:
        console.print("\n[red]Validation failed.[/red]")
        sys.exit(1)


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()

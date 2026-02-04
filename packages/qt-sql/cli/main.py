"""QueryTorque SQL CLI.

Command-line interface for SQL analysis and optimization.

Commands:
    qt-sql audit <file.sql>              Static analysis, generate report
    qt-sql optimize <file.sql>           LLM-powered optimization
    qt-sql validate <orig.sql> <opt.sql> Validate optimization equivalence
"""

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


@click.group()
@click.version_option(version="0.1.0", prog_name="qt-sql")
def cli():
    """QueryTorque SQL - SQL Analysis and Optimization CLI."""
    pass


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("--dialect", default="generic", help="SQL dialect (generic, snowflake, postgres, duckdb, tsql)")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed issue information")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def audit(
    file: str,
    dialect: str,
    verbose: bool,
    output_json: bool
):
    """Analyze SQL file for anti-patterns and issues.

    Performs static analysis on the SQL file and generates a report
    with detected issues, severity levels, and improvement suggestions.

    Examples:
        qt-sql audit query.sql
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

    # Exit with non-zero if critical/high issues found
    if result.critical_count > 0 or result.high_count > 0:
        sys.exit(1)


def _run_validation(
    original_sql: str,
    optimized_sql: str,
    database: str,
    mode: str,
    console,
    out_dir: Optional[Path] = None
) -> bool:
    """Run validation and display results. Returns True if passed."""
    import json

    console.print(f"\n[bold]Validating optimization ({mode} mode)...[/bold]")

    try:
        from qt_sql.validation import SQLValidator, ValidationMode, ValidationStatus

        validation_mode = ValidationMode.FULL if mode == "full" else ValidationMode.SAMPLE

        with SQLValidator(database=database, mode=validation_mode) as validator:
            result = validator.validate(original_sql, optimized_sql)

        # Save validation result
        if out_dir:
            validation_data = {
                "status": result.status.value,
                "speedup": result.speedup,
                "mode": mode,
                "row_counts_match": result.row_counts_match,
                "values_match": result.values_match or result.checksum_match,
                "original_timing_ms": result.original_timing_ms,
                "optimized_timing_ms": result.optimized_timing_ms,
                "original_row_count": result.original_row_count,
                "optimized_row_count": result.optimized_row_count,
                "errors": result.errors,
                "warnings": result.warnings,
            }
            (out_dir / "validation.json").write_text(json.dumps(validation_data, indent=2))

        # Display result
        if result.status == ValidationStatus.PASS:
            speedup_color = "green" if result.speedup >= 1.0 else "yellow"
            console.print(Panel(
                f"[bold green]PASSED[/bold green]\n"
                f"Speedup: [{speedup_color}]{result.speedup:.2f}x[/{speedup_color}]\n"
                f"Rows: {result.original_row_count} → {result.optimized_row_count}\n"
                f"Time: {result.original_timing_ms:.1f}ms → {result.optimized_timing_ms:.1f}ms",
                title="Validation Result",
                border_style="green",
            ))
            return True
        else:
            console.print(Panel(
                f"[bold red]FAILED[/bold red]\n"
                f"Row match: {result.row_counts_match}\n"
                f"Value match: {result.values_match or result.checksum_match}",
                title="Validation Result",
                border_style="red",
            ))
            for err in result.errors:
                console.print(f"  [red]{err}[/red]")
            return False

    except ImportError as e:
        console.print(f"[yellow]Validation skipped (module not available): {e}[/yellow]")
        return False
    except Exception as e:
        console.print(f"[yellow]Validation failed: {e}[/yellow]")
        return False


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("--dialect", default="duckdb", help="SQL dialect (duckdb, postgres, snowflake, generic)")
@click.option("--database", "-d", type=click.Path(), help="DuckDB database path for schema/execution plan context")
@click.option("--provider", default=None, help="LLM provider (anthropic, deepseek, openai, groq, gemini, openrouter)")
@click.option("--model", default=None, help="LLM model name")
@click.option("--output-dir", "-o", type=click.Path(), help="Output directory (default: ./qt-output/<filename>/)")
@click.option("--no-save", is_flag=True, help="Don't save output files")
@click.option("--dry-run", is_flag=True, help="Show prompt without calling LLM")
@click.option("--show-prompt", is_flag=True, help="Display the full prompt sent to LLM")
@click.option(
    "--mode",
    type=click.Choice(["dag", "full"]),
    default="dag",
    help="Optimization mode: dag (default), full (full SQL)"
)
@click.option("--no-validate", is_flag=True, help="Skip validation after optimization")
@click.option(
    "--validate-mode",
    type=click.Choice(["sample", "full"]),
    default="full",
    help="Validation mode when --database provided (default: full)"
)
def optimize(
    file: str,
    dialect: str,
    database: Optional[str],
    provider: Optional[str],
    model: Optional[str],
    output_dir: Optional[str],
    no_save: bool,
    dry_run: bool,
    show_prompt: bool,
    mode: str,
    no_validate: bool,
    validate_mode: str,
):
    """Optimize SQL using LLM-powered analysis.

    Modes:
    - dag (default): DAG-based node-level optimization with contracts
    - full: Full SQL replacement optimization

    Output files (saved to ./qt-output/<filename>/ by default):
    - original.sql      The input SQL
    - prompt.txt        Full prompt sent to LLM
    - response.txt      Raw LLM response
    - optimized.sql     Extracted optimized SQL
    - validation.json   Validation results (if --database provided)

    Examples:
        qt-sql optimize query.sql -d data.duckdb
        qt-sql optimize query.sql -d data.duckdb -o ./my-output/
        qt-sql optimize query.sql -d data.duckdb --no-save
        qt-sql optimize query.sql -d data.duckdb --validate-mode sample
    """
    from datetime import datetime
    import json

    try:
        sql = read_sql_file(file)
    except click.ClickException as e:
        console.print(f"[red]Error: {e.message}[/red]")
        sys.exit(1)

    # Setup output directory
    if no_save:
        out_dir = None
    else:
        if output_dir:
            out_dir = Path(output_dir)
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_stem = Path(file).stem
            out_dir = Path("./qt-output") / f"{file_stem}_{timestamp}"
        out_dir.mkdir(parents=True, exist_ok=True)
        # Save original
        (out_dir / "original.sql").write_text(sql, encoding="utf-8")

    # Run static analysis
    detector = SQLAntiPatternDetector(dialect=dialect)
    result = detector.analyze(sql, include_structure=True)

    console.print(f"\n[bold]Analyzing:[/bold] {file}")
    console.print(f"[dim]Dialect: {dialect}[/dim]")
    if database:
        console.print(f"[dim]Database: {database}[/dim]")
    if out_dir:
        console.print(f"[dim]Output: {out_dir}[/dim]")
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

    # DAG optimization mode (default)
    if mode == "dag":
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

            # Save prompt
            if out_dir:
                (out_dir / "prompt.txt").write_text(full_prompt, encoding="utf-8")

            if show_prompt:
                console.print("[bold]Full Prompt:[/bold]")
                console.print(Panel(full_prompt, title="DAG v2 Prompt", border_style="dim"))

            if dry_run:
                console.print("\n[yellow]Dry run mode - LLM call skipped.[/yellow]")
                if out_dir:
                    console.print(f"[dim]Prompt saved to: {out_dir / 'prompt.txt'}[/dim]")
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

            # Save response
            if out_dir:
                (out_dir / "response.txt").write_text(response, encoding="utf-8")

            # Apply response to get optimized SQL
            optimized_sql = pipeline.apply_response(response)

            # Save optimized SQL
            if out_dir:
                (out_dir / "optimized.sql").write_text(optimized_sql, encoding="utf-8")

            console.print("\n[bold green]DAG Optimization Result:[/bold green]")
            console.print(Syntax(optimized_sql, "sql", theme="monokai", line_numbers=True))

            if out_dir:
                console.print(f"\n[green]Files saved to: {out_dir}[/green]")

            # Auto-validate if database provided
            if database and not no_validate:
                _run_validation(sql, optimized_sql, database, validate_mode, console, out_dir)

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

    # Save prompt for full mode
    if out_dir:
        (out_dir / "prompt.txt").write_text(prompt, encoding="utf-8")

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

        # Save response
        if out_dir:
            (out_dir / "response.txt").write_text(response, encoding="utf-8")

        console.print("\n[bold green]LLM Optimization Result:[/bold green]")
        console.print(Markdown(response))

        # Extract optimized SQL from response
        import re
        optimized_sql = None

        # Try JSON format first
        try:
            json_match = re.search(r'\{[^{}]*"optimized_sql"[^{}]*\}', response, re.DOTALL)
            if json_match:
                parsed = json.loads(json_match.group())
                optimized_sql = parsed.get("optimized_sql", "").strip()
        except (json.JSONDecodeError, AttributeError):
            pass

        # Fallback to markdown code block
        if not optimized_sql:
            sql_match = re.search(r"```sql\s*(.*?)\s*```", response, re.DOTALL)
            if sql_match:
                optimized_sql = sql_match.group(1).strip()

        if optimized_sql:
            # Save optimized SQL
            if out_dir:
                (out_dir / "optimized.sql").write_text(optimized_sql, encoding="utf-8")
                console.print(f"\n[green]Files saved to: {out_dir}[/green]")

            # Auto-validate if database provided
            if database and not no_validate:
                _run_validation(sql, optimized_sql, database, validate_mode, console, out_dir)
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


@cli.command()
@click.argument("input_dir", type=click.Path(exists=True))
@click.option("--database", "-d", required=True, type=click.Path(exists=True), help="DuckDB database path")
@click.option("--output", "-o", type=click.Path(), help="Output directory for results")
@click.option("--provider", default=None, help="LLM provider (anthropic, deepseek, openai, groq, gemini, openrouter)")
@click.option("--model", default=None, help="LLM model name")
@click.option("--pattern", default="*.sql", help="Glob pattern for SQL files (default: *.sql)")
@click.option(
    "--mode",
    type=click.Choice(["sample", "full"]),
    default="full",
    help="Validation mode: sample (1%% DB) or full (default)"
)
@click.option("--parallel", "-p", type=int, default=10, help="Max parallel API calls (default: 10)")
@click.option("--skip-optimize", is_flag=True, help="Skip optimization, just run validation on existing files")
@click.option("--json", "output_json", is_flag=True, help="Output final report as JSON")
def batch(
    input_dir: str,
    database: str,
    output: Optional[str],
    provider: Optional[str],
    model: Optional[str],
    pattern: str,
    mode: str,
    parallel: int,
    skip_optimize: bool,
    output_json: bool,
):
    """Batch optimize and validate multiple SQL files.

    Fires all LLM optimization calls in parallel, then runs validation
    sequentially on the database (to avoid resource contention).

    Input can be:
    - Directory with SQL files (uses --pattern to filter)
    - Results from previous batch run (with --skip-optimize)

    Examples:
        qt-sql batch /path/to/queries -d data.duckdb
        qt-sql batch ./queries -d data.duckdb --mode sample --parallel 20
        qt-sql batch ./queries -d data.duckdb --provider openrouter --model moonshotai/kimi-k2.5
        qt-sql batch ./results -d data.duckdb --skip-optimize  # Re-validate existing results
    """
    import glob
    import json
    import time
    import traceback
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from dataclasses import dataclass, asdict, field
    from datetime import datetime
    from typing import List, Dict, Any

    @dataclass
    class BatchResult:
        """Result for a single query in batch."""
        name: str
        status: str  # success, failed, error, skipped
        original_sql: str = ""
        optimized_sql: str = ""
        speedup: float = 0.0
        validation_passed: bool = False
        error: Optional[str] = None
        optimize_latency_ms: float = 0
        validate_latency_ms: float = 0

        def to_dict(self) -> dict:
            return asdict(self)

    # Setup paths
    input_path = Path(input_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = Path(output) if output else input_path / f"batch_results_{timestamp}"
    output_path.mkdir(parents=True, exist_ok=True)

    # Find SQL files
    sql_files = sorted(input_path.glob(pattern))
    if not sql_files:
        console.print(f"[red]No SQL files found matching {pattern} in {input_dir}[/red]")
        sys.exit(1)

    console.print(f"\n[bold]Batch Optimization & Validation[/bold]")
    console.print(f"Input: {input_dir} ({len(sql_files)} files)")
    console.print(f"Database: {database}")
    console.print(f"Mode: {mode}")
    console.print(f"Output: {output_path}")
    if not skip_optimize:
        console.print(f"Parallel API calls: {parallel}")
    console.print()

    results: List[BatchResult] = []

    # Phase 1: Parallel LLM optimization
    if not skip_optimize:
        console.print("[bold]Phase 1: Optimizing (parallel API calls)...[/bold]")

        try:
            from qt_shared.llm import create_llm_client
            from qt_sql.optimization.dag_v2 import DagV2Pipeline, get_dag_v2_examples

            llm_client = create_llm_client(provider=provider, model=model)
            if llm_client is None:
                console.print("[red]No LLM provider configured.[/red]")
                sys.exit(1)

        except ImportError as e:
            console.print(f"[red]Required modules not available: {e}[/red]")
            sys.exit(1)

        def optimize_one(sql_file: Path) -> BatchResult:
            """Optimize a single SQL file."""
            name = sql_file.stem
            result = BatchResult(name=name, status="pending")

            try:
                sql = sql_file.read_text(encoding="utf-8")
                result.original_sql = sql

                # Build DAG pipeline
                pipeline = DagV2Pipeline(sql)

                # Build prompt with few-shot examples
                examples = get_dag_v2_examples()
                few_shot_parts = []
                for ex in examples[:2]:
                    few_shot_parts.append(f"### Example: {ex['opportunity']}")
                    few_shot_parts.append(f"Input:\n{ex['input_slice']}")
                    few_shot_parts.append(f"Output:\n```json\n{json.dumps(ex['output'], indent=2)}\n```")
                    if 'key_insight' in ex:
                        few_shot_parts.append(f"Key insight: {ex['key_insight']}")
                    few_shot_parts.append("")

                few_shot = "\n".join(few_shot_parts)
                base_prompt = pipeline.get_prompt()
                full_prompt = f"## Examples\n\n{few_shot}\n\n---\n\n## Your Task\n\n{base_prompt}"

                # Call LLM
                start = time.perf_counter()
                response = llm_client.analyze(full_prompt)
                result.optimize_latency_ms = (time.perf_counter() - start) * 1000

                # Apply response
                optimized_sql = pipeline.apply_response(response)
                result.optimized_sql = optimized_sql
                result.status = "optimized"

                # Save files
                query_dir = output_path / name
                query_dir.mkdir(exist_ok=True)
                (query_dir / "original.sql").write_text(sql)
                (query_dir / "optimized.sql").write_text(optimized_sql)
                (query_dir / "llm_response.txt").write_text(response)

            except Exception as e:
                result.status = "error"
                result.error = f"{type(e).__name__}: {str(e)}"

            return result

        # Fire parallel optimization
        with ThreadPoolExecutor(max_workers=parallel) as executor:
            futures = {executor.submit(optimize_one, f): f for f in sql_files}

            for future in as_completed(futures):
                sql_file = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    status_char = "✓" if result.status == "optimized" else "✗"
                    console.print(f"  {result.name}: {status_char} ({result.optimize_latency_ms:.0f}ms)")
                except Exception as e:
                    console.print(f"  {sql_file.stem}: [red]EXCEPTION ({e})[/red]")
                    results.append(BatchResult(
                        name=sql_file.stem,
                        status="error",
                        error=str(e)
                    ))

        results.sort(key=lambda r: r.name)
        optimized_count = len([r for r in results if r.status == "optimized"])
        console.print(f"\n  Optimized: {optimized_count}/{len(results)}")

    else:
        # Load existing results for validation
        console.print("[bold]Phase 1: Loading existing optimizations...[/bold]")
        for sql_file in sql_files:
            name = sql_file.stem
            query_dir = input_path / name
            if query_dir.is_dir():
                orig_file = query_dir / "original.sql"
                opt_file = query_dir / "optimized.sql"
                if orig_file.exists() and opt_file.exists():
                    results.append(BatchResult(
                        name=name,
                        status="optimized",
                        original_sql=orig_file.read_text(),
                        optimized_sql=opt_file.read_text(),
                    ))
                    console.print(f"  {name}: loaded")
                else:
                    console.print(f"  {name}: [yellow]missing files[/yellow]")
            else:
                # Single file, no optimized version
                console.print(f"  {name}: [yellow]no optimized version[/yellow]")

    # Phase 2: Sequential validation
    console.print(f"\n[bold]Phase 2: Validating (sequential on {mode} DB)...[/bold]")

    optimized_results = [r for r in results if r.status == "optimized"]
    if not optimized_results:
        console.print("[yellow]No optimized queries to validate.[/yellow]")
    else:
        try:
            from qt_sql.validation import SQLValidator, ValidationMode, ValidationStatus

            validation_mode = ValidationMode.FULL if mode == "full" else ValidationMode.SAMPLE

            with SQLValidator(database=database, mode=validation_mode) as validator:
                for result in optimized_results:
                    try:
                        start = time.perf_counter()
                        val_result = validator.validate(
                            result.original_sql,
                            result.optimized_sql,
                        )
                        result.validate_latency_ms = (time.perf_counter() - start) * 1000
                        result.speedup = val_result.speedup
                        result.validation_passed = val_result.status == ValidationStatus.PASS

                        if result.validation_passed:
                            result.status = "success"
                            status_str = f"[green]✓ {result.speedup:.2f}x[/green]"
                        else:
                            result.status = "failed"
                            status_str = f"[red]✗ validation failed[/red]"

                        console.print(f"  {result.name}: {status_str} ({result.validate_latency_ms/1000:.1f}s)")

                        # Save validation result
                        query_dir = output_path / result.name
                        query_dir.mkdir(exist_ok=True)
                        (query_dir / "validation.json").write_text(json.dumps({
                            "status": result.status,
                            "speedup": result.speedup,
                            "validation_passed": result.validation_passed,
                            "mode": mode,
                        }, indent=2))

                    except Exception as e:
                        result.status = "error"
                        result.error = f"Validation error: {e}"
                        console.print(f"  {result.name}: [red]ERROR ({e})[/red]")

        except ImportError as e:
            console.print(f"[red]Validation module not available: {e}[/red]")

    # Summary report
    console.print(f"\n[bold]{'='*60}[/bold]")
    console.print("[bold]Summary[/bold]")

    success = [r for r in results if r.status == "success"]
    failed = [r for r in results if r.status == "failed"]
    errors = [r for r in results if r.status == "error"]

    console.print(f"  Total: {len(results)}")
    console.print(f"  [green]Passed: {len(success)}[/green]")
    console.print(f"  [red]Failed: {len(failed)}[/red]")
    console.print(f"  [yellow]Errors: {len(errors)}[/yellow]")

    if success:
        speedups = [r.speedup for r in success]
        avg_speedup = sum(speedups) / len(speedups)
        wins = [r for r in success if r.speedup >= 1.2]
        regressions = [r for r in success if r.speedup < 1.0]
        console.print(f"\n  Avg speedup: {avg_speedup:.2f}x")
        console.print(f"  Wins (>=1.2x): {len(wins)}")
        console.print(f"  Regressions (<1.0x): {len(regressions)}")

        if wins:
            console.print("\n  [bold]Top wins:[/bold]")
            top_wins = sorted(wins, key=lambda r: r.speedup, reverse=True)[:10]
            for r in top_wins:
                console.print(f"    {r.name}: {r.speedup:.2f}x")

    # Save final report
    report = {
        "timestamp": timestamp,
        "input_dir": str(input_dir),
        "database": str(database),
        "mode": mode,
        "total": len(results),
        "success": len(success),
        "failed": len(failed),
        "errors": len(errors),
        "avg_speedup": sum(r.speedup for r in success) / len(success) if success else 0,
        "results": [r.to_dict() for r in results],
    }

    report_file = output_path / "report.json"
    report_file.write_text(json.dumps(report, indent=2))
    console.print(f"\n  Report saved: {report_file}")

    if output_json:
        console.print_json(json.dumps(report))


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()

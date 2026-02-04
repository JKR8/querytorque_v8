"""QueryTorque SQL CLI.

Command-line interface for SQL analysis and optimization.

Commands:
    qt-sql audit <file.sql>              Static analysis, generate report
    qt-sql optimize <file.sql>           LLM-powered optimization
    qt-sql optimize <file.sql> --dag     DAG v2 + JSON v5 node-level rewrites (recommended)
    qt-sql optimize <file.sql> --dag --mcts-on-failure   DAG + MCTS fallback
    qt-sql validate <orig.sql> <opt.sql> Validate optimization equivalence
"""

import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.syntax import Syntax
from rich.markdown import Markdown
from rich.progress import Progress, SpinnerColumn, TextColumn

from qt_sql.analyzers.sql_antipattern_detector import SQLAntiPatternDetector, SQLAnalysisResult
from qt_sql.analyzers.opportunity_detector import detect_opportunities, OpportunityResult
from qt_sql.validation.schemas import ValidationStatus

console = Console()
logger = logging.getLogger(__name__)


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


def display_assessment(result: SQLAnalysisResult, verbose: bool = False) -> None:
    """Display optimization assessment (new format focused on opportunities).

    This is the new primary display format that focuses on:
    - Number of optimization opportunities
    - Execution metrics (when database is provided)
    - Efficiency ratio
    - Bottleneck identification
    - Actionable rewrite suggestions

    Args:
        result: SQLAnalysisResult with opportunities and optional execution data
        verbose: Show additional details
    """
    # Header with opportunity count
    opp_count = len(result.opportunities)
    if opp_count == 0:
        header = "No Optimization Opportunities Detected"
        header_style = "green"
    else:
        header = f"{opp_count} Optimization Opportunit{'y' if opp_count == 1 else 'ies'} Found"
        header_style = "yellow"

    # Build panel content
    lines = [f"[bold]{header}[/bold]"]

    # Execution metrics (if available)
    if result.has_execution_data:
        lines.append("")
        time_str = f"{result.execution_time_ms:,.0f}ms" if result.execution_time_ms else "N/A"
        scanned_str = f"{result.rows_scanned:,}" if result.rows_scanned else "N/A"
        returned_str = f"{result.rows_returned:,}" if result.rows_returned else "N/A"
        lines.append(f"Execution: {time_str} | Scanned: {scanned_str} rows | Returned: {returned_str}")

        if result.efficiency_ratio is not None:
            if result.efficiency_ratio >= 0.5:
                eff_color = "green"
                eff_desc = "Good"
            elif result.efficiency_ratio >= 0.1:
                eff_color = "yellow"
                eff_desc = f"Moderate ({1/result.efficiency_ratio:.0f}x scan ratio)"
            elif result.efficiency_ratio >= 0.01:
                eff_color = "red"
                scan_mult = int(1 / result.efficiency_ratio)
                eff_desc = f"Poor (scanning {scan_mult:,}x more rows than needed)"
            elif result.efficiency_ratio > 0:
                eff_color = "red"
                scan_mult = int(1 / result.efficiency_ratio)
                eff_desc = f"Very poor ({scan_mult:,}x scan-to-return ratio)"
            else:
                eff_color = "dim"
                eff_desc = "No rows returned"
            lines.append(f"Efficiency: [{eff_color}]{result.efficiency_ratio:.4%}[/{eff_color}] - {eff_desc}")
    else:
        lines.append("")
        lines.append("[dim]Run with -d <database> for execution metrics[/dim]")

    console.print(Panel(
        "\n".join(lines),
        title="SQL Optimization Assessment",
        border_style=header_style
    ))

    # Bottleneck
    if result.bottleneck:
        bn = result.bottleneck
        console.print(f"\n[bold]Bottleneck:[/bold] {bn.get('op', 'Unknown')} ({bn.get('cost_pct', 0):.0f}% of cost)")
        if bn.get("details"):
            console.print(f"  {bn['details']}")

    # Opportunities
    if result.opportunities:
        console.print("\n[bold]Opportunities:[/bold]")
        for i, opp in enumerate(result.opportunities, 1):
            if hasattr(opp, 'pattern_name'):
                # OpportunityResult object
                console.print(f"\n  {i}. [bold]{opp.pattern_name}[/bold]")
                console.print(f"     {opp.trigger}")
                console.print(f"     [green]Rewrite:[/green] {opp.rewrite_hint}")
                if opp.expected_benefit:
                    console.print(f"     [dim]Benefit: {opp.expected_benefit}[/dim]")
            else:
                # Dict format
                console.print(f"\n  {i}. [bold]{opp.get('pattern_name', 'Unknown')}[/bold]")
                console.print(f"     {opp.get('trigger', '')}")
                console.print(f"     [green]Rewrite:[/green] {opp.get('rewrite_hint', '')}")
                if opp.get('expected_benefit'):
                    console.print(f"     [dim]Benefit: {opp['expected_benefit']}[/dim]")

    # Also show static issues if any (in verbose mode or if no opportunities)
    if verbose and result.issues:
        console.print(f"\n[dim]Static Analysis: {len(result.issues)} issues detected[/dim]")
        for issue in result.issues[:5]:  # Show first 5
            console.print(f"  [dim]- {issue.rule_id}: {issue.name}[/dim]")
        if len(result.issues) > 5:
            console.print(f"  [dim]... and {len(result.issues) - 5} more[/dim]")


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
@click.option("-d", "--database", type=click.Path(), help="DuckDB database for execution plan analysis")
@click.option("--style", is_flag=True, help="Include style/convention rules (noisy)")
def audit(
    file: str,
    dialect: str,
    verbose: bool,
    output_json: bool,
    database: Optional[str],
    style: bool
):
    """Analyze SQL file for anti-patterns and optimization opportunities.

    Performs static analysis on the SQL file and generates a report
    with detected issues, severity levels, and improvement suggestions.

    By default, only high-precision rules are used to reduce noise.
    Use --style to include style/convention rules (SELECT *, implicit joins, etc.)

    When -d/--database is provided:
    - Runs EXPLAIN ANALYZE to get execution metrics
    - Reports efficiency ratio (rows returned / rows scanned)
    - Identifies bottleneck operators
    - Shows actionable optimization opportunities

    Examples:
        qt-sql audit query.sql
        qt-sql audit query.sql -d mydb.duckdb
        qt-sql audit query.sql --style -v
    """
    try:
        sql = read_sql_file(file)
    except click.ClickException as e:
        console.print(f"[red]Error: {e.message}[/red]")
        sys.exit(1)

    # Run static analysis (include_style=False by default for cleaner output)
    detector = SQLAntiPatternDetector(dialect=dialect, include_style=style)
    result = detector.analyze(sql, include_structure=True)

    # Detect optimization opportunities
    opportunities = detect_opportunities(sql)
    result.opportunities = opportunities

    # Get execution plan analysis if database provided
    if database:
        try:
            from qt_sql.execution.database_utils import run_explain_analyze
            from qt_sql.execution.plan_parser import build_plan_summary

            explain_result = run_explain_analyze(database, sql)
            if explain_result and explain_result.get("plan_json"):
                plan_summary = build_plan_summary(explain_result["plan_json"])

                # Populate execution metrics
                result.execution_time_ms = plan_summary.get("total_time_ms")
                result.rows_scanned = plan_summary.get("rows_scanned")
                result.rows_returned = plan_summary.get("rows_returned")
                result.efficiency_ratio = plan_summary.get("efficiency_ratio")
                result.top_operators = plan_summary.get("top_operators", [])
                result.bottleneck = plan_summary.get("bottleneck")

        except Exception as e:
            console.print(f"[yellow]Warning: Could not analyze execution plan: {e}[/yellow]")

    if output_json:
        import json
        output = result.to_dict()
        output["file"] = file
        output["dialect"] = dialect
        console.print_json(json.dumps(output))
        return

    console.print(f"\n[bold]Analyzing:[/bold] {file}")
    console.print(f"[dim]Dialect: {dialect}[/dim]")
    if database:
        console.print(f"[dim]Database: {database}[/dim]")
    console.print()

    # Use new assessment display when we have opportunities or execution data
    if opportunities or result.has_execution_data:
        display_assessment(result, verbose=verbose)
    else:
        # Fall back to old display for backward compatibility
        display_analysis_result(result, verbose=verbose)

    # Exit with non-zero if critical/high issues found
    if result.critical_count > 0 or result.high_count > 0:
        sys.exit(1)


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("--dialect", default="generic", help="SQL dialect")
@click.option("--provider", default=None, help="LLM provider (anthropic, deepseek, openai, groq, gemini)")
@click.option("--model", default=None, help="LLM model name")
@click.option("--output", "-o", type=click.Path(), help="Output file for optimized SQL")
@click.option("--dry-run", is_flag=True, help="Show what would be optimized without calling LLM")
@click.option("--database", "-d", type=click.Path(), help="DuckDB database path for schema/execution plan context")
@click.option("--show-prompt", is_flag=True, help="Display the full prompt sent to LLM")
# DAG and MCTS options
@click.option("--dag", is_flag=True, help="Use DAG v2 + JSON v5 node-level rewrites (recommended)")
@click.option("--mcts", is_flag=True, help="Use MCTS optimizer directly")
@click.option("--mcts-on-failure", is_flag=True, help="Escalate to MCTS if DAG v2 JSON v5 fails")
@click.option("--mcts-iterations", default=30, help="Max MCTS iterations (default: 30)")
@click.option("--mcts-parallel", default=4, help="MCTS parallel workers (default: 4)")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
def optimize(
    file: str,
    dialect: str,
    provider: Optional[str],
    model: Optional[str],
    output: Optional[str],
    dry_run: bool,
    database: Optional[str],
    show_prompt: bool,
    dag: bool,
    mcts: bool,
    mcts_on_failure: bool,
    mcts_iterations: int,
    mcts_parallel: int,
    verbose: bool
):
    """Optimize SQL using LLM-powered analysis.

    Analyzes the SQL file and uses an LLM to suggest optimizations
    based on detected anti-patterns and best practices.

    OPTIMIZATION MODES:

    \b
    --dag              DAG v2 + JSON v5 node-level rewrites with validation.
                       Recommended for complex queries with CTEs/subqueries.
                       Requires --database for validation.

    \b
    --mcts             Direct MCTS tree search optimization.
                       Explores multiple transformation strategies.
                       Requires --database for validation.

    \b
    --mcts-on-failure  Escalate to MCTS if DAG optimization fails validation.
                       Combines DAG's efficiency with MCTS's robustness.

    When --database is provided, includes:
    - Schema with row counts for referenced tables
    - Execution plan with row estimates (enables semantic optimizations)

    Examples:
        qt-sql optimize query.sql
        qt-sql optimize query.sql --provider deepseek
        qt-sql optimize query.sql -d /path/to/db.duckdb --dag
        qt-sql optimize query.sql -d /path/to/db.duckdb --dag --mcts-on-failure
        qt-sql optimize query.sql -d /path/to/db.duckdb --mcts
        qt-sql optimize query.sql -o optimized.sql
    """
    # Setup logging
    if verbose:
        logging.basicConfig(level=logging.DEBUG, format='%(message)s')
    else:
        logging.basicConfig(level=logging.WARNING)

    try:
        sql = read_sql_file(file)
    except click.ClickException as e:
        console.print(f"[red]Error: {e.message}[/red]")
        sys.exit(1)

    # First, run analysis
    detector = SQLAntiPatternDetector(dialect=dialect)
    result = detector.analyze(sql, include_structure=True)

    console.print(f"\n[bold]Analyzing:[/bold] {file}")
    console.print(f"[dim]Dialect: {dialect}[/dim]\n")

    display_analysis_result(result, verbose=False)

    # Validate mode requirements
    if (dag or mcts) and not database:
        console.print("[red]Error: --database is required for --dag and --mcts modes[/red]")
        sys.exit(1)

    if database:
        console.print(f"\n[bold]Database:[/bold] {database}")

    # ============================================================
    # MODE 1: DAG-based optimization (recommended)
    # ============================================================
    if dag:
        optimized_sql = _run_dag_optimization(
            sql=sql,
            database=database,
            provider=provider,
            model=model,
            mcts_on_failure=mcts_on_failure,
            mcts_iterations=mcts_iterations,
            mcts_parallel=mcts_parallel,
            verbose=verbose,
            show_prompt=show_prompt,
        )

        if optimized_sql:
            _output_result(optimized_sql, output, verbose)
        return

    # ============================================================
    # MODE 2: Direct MCTS optimization
    # ============================================================
    if mcts:
        optimized_sql = _run_mcts_optimization(
            sql=sql,
            database=database,
            provider=provider,
            iterations=mcts_iterations,
            parallel=mcts_parallel,
            verbose=verbose,
        )

        if optimized_sql:
            _output_result(optimized_sql, output, verbose)
        return

    # ============================================================
    # MODE 3: Simple LLM optimization (default/legacy)
    # ============================================================
    if not result.issues:
        console.print("\n[green]No issues found - query already looks optimal.[/green]")
        return

    # Gather database context if provided
    schema_context = None
    plan_summary = None

    if database:
        console.print(f"\n[bold]Gathering database context from:[/bold] {database}")

        try:
            from qt_sql.execution.database_utils import (
                fetch_schema_with_stats,
                run_explain_analyze,
            )
            from qt_sql.execution.plan_parser import build_plan_summary

            # Get schema with row counts for referenced tables
            schema_context = fetch_schema_with_stats(database, sql)
            if schema_context and schema_context.get("tables"):
                table_count = len(schema_context["tables"])
                total_rows = sum(t.get("row_count", 0) for t in schema_context["tables"])
                console.print(f"  Schema: {table_count} tables, {total_rows:,} total rows")

            # Run EXPLAIN ANALYZE and build summary
            explain_result = run_explain_analyze(database, sql)
            if explain_result and explain_result.get("plan_json"):
                plan_summary = build_plan_summary(explain_result["plan_json"])
                console.print(f"  Execution plan: {plan_summary.get('total_time_ms', 0):.1f}ms")
                if plan_summary.get("scans"):
                    for scan in plan_summary["scans"]:
                        console.print(f"    - Scan on {scan['table']}: {scan['rows']:,} rows")
            elif explain_result and explain_result.get("plan_text"):
                console.print("  [dim]Text-only plan available (no row estimates)[/dim]")

        except Exception as e:
            console.print(f"  [yellow]Warning: Could not gather database context: {e}[/yellow]")

    # Build compact plan info (just row counts per scan)
    scan_info = ""
    if plan_summary and plan_summary.get("scans"):
        scan_lines = ["Table scans (row counts):"]
        for scan in plan_summary["scans"]:
            scan_lines.append(f"  - {scan['table']}: {scan['rows']:,} rows")
        scan_info = "\n".join(scan_lines)

    prompt = f"""Optimize this SQL query.

{scan_info}

```sql
{sql}
```

## Optimization Process

Follow this process to optimize:

1. ANALYZE: Look at the row counts. Find where rows are largest.

2. OPTIMIZE: For each large row source, ask "what could reduce it earlier?"
   - Can a filter be moved inside a CTE/subquery instead of applied after?
   - Can a join with a small table happen INSIDE an aggregation to filter before GROUP BY?
   - Is there a correlated subquery that runs repeatedly? Convert to a single CTE + JOIN.

3. VERIFY: The result must be semantically equivalent.

Key principle: **Reduce rows as early as possible.** Move any filtering operation inside aggregations, not after them.

## Output

Return ONLY the optimized SQL in a code block. No explanation needed.

```sql
"""

    if show_prompt:
        console.print("\n[bold]Full Prompt Sent to LLM:[/bold]")
        console.print(Panel(prompt, title="LLM Prompt", border_style="blue"))

    if dry_run:
        console.print("\n[yellow]Dry run mode - LLM optimization skipped.[/yellow]")
        console.print("Issues that would be addressed:")
        for issue in result.issues:
            console.print(f"  - {issue.name}: {issue.suggestion or issue.description}")
        return

    # Create LLM client
    try:
        from qt_shared.llm import create_llm_client
        from qt_shared.config import get_settings

        settings = get_settings()
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

    console.print("\n[bold]Requesting LLM optimization...[/bold]")

    try:
        response = llm_client.analyze(prompt)

        console.print("\n[bold green]LLM Optimization Result:[/bold green]")
        console.print(Markdown(response))

        # Extract optimized SQL if output file requested
        if output:
            import re
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


def _run_dag_optimization(
    sql: str,
    database: str,
    provider: Optional[str],
    model: Optional[str],
    mcts_on_failure: bool,
    mcts_iterations: int,
    mcts_parallel: int,
    verbose: bool,
    show_prompt: bool,
) -> Optional[str]:
    """Run DAG v2 + JSON v5 optimization with optional MCTS escalation."""
    console.print("\n[bold blue]Running DAG v2 + JSON v5 optimization...[/bold blue]")

    try:
        from qt_sql.optimization import optimize_v5_json

        start_time = time.time()
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            progress.add_task("Optimizing with DAG v2 + JSON v5...", total=None)
            result = optimize_v5_json(
                sql=sql,
                sample_db=database,
                provider=provider,
                model=model,
            )
        elapsed = time.time() - start_time

        console.print(f"  Completed in {elapsed:.1f}s, status={result.status.value}")

        if show_prompt:
            console.print("\n[bold]DAG v2 Prompt Sent to LLM:[/bold]")
            console.print(Panel(result.prompt, title="LLM Prompt", border_style="blue"))

        if result.status == ValidationStatus.PASS and result.optimized_sql:
            console.print("[green]DAG optimization succeeded![/green]")
            return result.optimized_sql

        error_preview = (result.error or "Unknown")[:60]
        console.print(f"[yellow]DAG optimization failed: {error_preview}...[/yellow]")

        if mcts_on_failure:
            console.print("\n[bold yellow]Escalating to MCTS...[/bold yellow]")
            return _run_mcts_optimization(
                sql=sql,
                database=database,
                provider=provider,
                iterations=mcts_iterations,
                parallel=mcts_parallel,
                verbose=verbose,
            )

        console.print("[dim]Use --mcts-on-failure to escalate to MCTS search[/dim]")
        return None

    except Exception as e:
        console.print(f"[red]DAG optimization error: {e}[/red]")
        if verbose:
            import traceback
            console.print(f"[dim]{traceback.format_exc()}[/dim]")

        if mcts_on_failure:
            console.print("\n[bold yellow]Escalating to MCTS after error...[/bold yellow]")
            return _run_mcts_optimization(
                sql=sql,
                database=database,
                provider=provider,
                iterations=mcts_iterations,
                parallel=mcts_parallel,
                verbose=verbose,
            )
        return None


def _run_mcts_optimization(
    sql: str,
    database: str,
    provider: Optional[str],
    iterations: int,
    parallel: int,
    verbose: bool,
) -> Optional[str]:
    """Run MCTS-based optimization.

    Args:
        sql: Original SQL query
        database: Path to DuckDB database
        provider: LLM provider name
        iterations: Max MCTS iterations
        parallel: Number of parallel workers
        verbose: Verbose output

    Returns:
        Optimized SQL string, or None if optimization failed
    """
    console.print(f"\n[bold blue]Running MCTS optimization...[/bold blue]")
    console.print(f"  Iterations: {iterations}, Parallel: {parallel}")

    try:
        from qt_sql.optimization.mcts import MCTSSQLOptimizer

        start_time = time.time()

        with MCTSSQLOptimizer(database=database, provider=provider) as optimizer:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task(
                    f"Searching optimization space ({iterations} iterations)...",
                    total=None
                )

                if parallel > 1:
                    result = optimizer.optimize_parallel(
                        query=sql,
                        max_iterations=iterations,
                        num_parallel=parallel,
                    )
                else:
                    result = optimizer.optimize(
                        query=sql,
                        max_iterations=iterations,
                    )

        elapsed = time.time() - start_time
        console.print(f"  Completed in {elapsed:.1f}s, {result.iterations} iterations")

        if result.valid and result.speedup > 1.0:
            console.print(
                f"[green]MCTS optimization succeeded![/green] "
                f"Speedup: {result.speedup:.2f}x"
            )
            if verbose:
                console.print(f"  Method: {result.method}")
                if result.transforms_applied:
                    console.print(f"  Transforms: {', '.join(result.transforms_applied)}")
            return result.optimized_sql
        else:
            console.print(
                f"[yellow]MCTS found no improvement[/yellow] "
                f"(speedup: {result.speedup:.2f}x)"
            )
            return None

    except ImportError as e:
        console.print(f"[red]Error: Missing dependency for MCTS mode: {e}[/red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]MCTS optimization error: {e}[/red]")
        if verbose:
            import traceback
            console.print(f"[dim]{traceback.format_exc()}[/dim]")
        return None


def _output_result(optimized_sql: str, output: Optional[str], verbose: bool) -> None:
    """Output the optimization result.

    Args:
        optimized_sql: The optimized SQL
        output: Output file path (if any)
        verbose: Whether to show full SQL
    """
    if output:
        Path(output).write_text(optimized_sql, encoding="utf-8")
        console.print(f"\n[green]Optimized SQL saved to: {output}[/green]")
    else:
        console.print("\n[bold]Optimized SQL:[/bold]")
        console.print(Syntax(optimized_sql, "sql", theme="monokai", line_numbers=True))


@cli.command()
@click.argument("original", type=click.Path(exists=True))
@click.argument("optimized", type=click.Path(exists=True))
@click.option("--dialect", default="generic", help="SQL dialect")
@click.option("--database", default=":memory:", help="DuckDB database path for validation")
@click.option("--schema", type=click.Path(exists=True), help="SQL file with schema creation statements")
def validate(
    original: str,
    optimized: str,
    dialect: str,
    database: str,
    schema: Optional[str]
):
    """Validate that optimized SQL is equivalent to original.

    Compares execution plans and optionally runs both queries
    to verify result equivalence.

    Examples:
        qt-sql validate original.sql optimized.sql
        qt-sql validate original.sql optimized.sql --schema schema.sql
    """
    try:
        original_sql = read_sql_file(original)
        optimized_sql = read_sql_file(optimized)
    except click.ClickException as e:
        console.print(f"[red]Error: {e.message}[/red]")
        sys.exit(1)

    console.print(f"\n[bold]Validating optimization equivalence[/bold]")
    console.print(f"Original: {original}")
    console.print(f"Optimized: {optimized}")
    console.print()

    try:
        from qt_sql.execution.duckdb_executor import DuckDBExecutor
    except ImportError:
        console.print("[red]DuckDB not installed. Install with: pip install duckdb[/red]")
        sys.exit(1)

    validation_results = {
        "syntax_valid": {"original": False, "optimized": False},
        "plans_obtained": {"original": False, "optimized": False},
        "cost_comparison": None,
        "results_match": None,
    }

    try:
        with DuckDBExecutor(database) as db:
            # Load schema if provided
            if schema:
                schema_sql = read_sql_file(schema)
                console.print("[dim]Loading schema...[/dim]")
                db.execute_script(schema_sql)

            # Validate syntax by getting execution plans
            console.print("[bold]Checking syntax and execution plans...[/bold]")

            try:
                original_plan = db.explain(original_sql, analyze=False)
                validation_results["syntax_valid"]["original"] = True
                validation_results["plans_obtained"]["original"] = True
                console.print("  Original: [green]Valid[/green]")
            except Exception as e:
                console.print(f"  Original: [red]Invalid - {e}[/red]")

            try:
                optimized_plan = db.explain(optimized_sql, analyze=False)
                validation_results["syntax_valid"]["optimized"] = True
                validation_results["plans_obtained"]["optimized"] = True
                console.print("  Optimized: [green]Valid[/green]")
            except Exception as e:
                console.print(f"  Optimized: [red]Invalid - {e}[/red]")

            # Compare costs if both plans obtained
            if (
                validation_results["plans_obtained"]["original"] and
                validation_results["plans_obtained"]["optimized"]
            ):
                console.print("\n[bold]Comparing execution costs...[/bold]")
                cost_comparison = db.compare_cost(original_sql, optimized_sql)
                validation_results["cost_comparison"] = cost_comparison

                original_cost = cost_comparison["original_cost"]
                optimized_cost = cost_comparison["optimized_cost"]
                reduction = cost_comparison["reduction_ratio"] * 100

                if cost_comparison["improved"]:
                    console.print(
                        f"  [green]Cost reduced by {reduction:.1f}%[/green] "
                        f"({original_cost:.0f} -> {optimized_cost:.0f})"
                    )
                elif reduction < 0:
                    console.print(
                        f"  [yellow]Cost increased by {abs(reduction):.1f}%[/yellow] "
                        f"({original_cost:.0f} -> {optimized_cost:.0f})"
                    )
                else:
                    console.print(f"  Cost unchanged ({original_cost:.0f})")

            # Try to compare actual results if schema is provided
            if schema:
                console.print("\n[bold]Comparing query results...[/bold]")
                try:
                    original_results = db.execute(original_sql)
                    optimized_results = db.execute(optimized_sql)

                    if original_results == optimized_results:
                        validation_results["results_match"] = True
                        console.print("  [green]Results match exactly[/green]")
                    else:
                        validation_results["results_match"] = False
                        console.print("  [red]Results differ[/red]")
                        console.print(f"  Original rows: {len(original_results)}")
                        console.print(f"  Optimized rows: {len(optimized_results)}")

                except Exception as e:
                    console.print(f"  [yellow]Could not compare results: {e}[/yellow]")

    except Exception as e:
        console.print(f"[red]Validation failed: {e}[/red]")
        sys.exit(1)

    # Summary
    console.print("\n[bold]Validation Summary:[/bold]")

    all_valid = (
        validation_results["syntax_valid"]["original"] and
        validation_results["syntax_valid"]["optimized"]
    )

    if all_valid:
        console.print("[green]Both queries are syntactically valid.[/green]")
        if validation_results["cost_comparison"] and validation_results["cost_comparison"]["improved"]:
            console.print("[green]Optimization shows cost improvement.[/green]")
        if validation_results["results_match"]:
            console.print("[green]Query results are equivalent.[/green]")
        elif validation_results["results_match"] is False:
            console.print("[red]WARNING: Query results differ![/red]")
            sys.exit(1)
    else:
        console.print("[red]Validation failed - one or both queries are invalid.[/red]")
        sys.exit(1)


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("-d", "--database", required=True, type=click.Path(exists=True), help="DuckDB database for plan analysis")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def assess(
    file: str,
    database: str,
    verbose: bool,
    output_json: bool
):
    """Assess SQL query for optimization opportunities.

    This command requires a database connection to analyze the execution plan.
    It provides:
    - Execution time and row statistics
    - Efficiency ratio (rows returned / rows scanned)
    - Bottleneck operator identification
    - Concrete optimization opportunities with rewrite hints

    Use 'audit' for static-only analysis without a database.

    Examples:
        qt-sql assess query.sql -d mydb.duckdb
        qt-sql assess query.sql -d /path/to/tpcds.duckdb -v
    """
    try:
        sql = read_sql_file(file)
    except click.ClickException as e:
        console.print(f"[red]Error: {e.message}[/red]")
        sys.exit(1)

    # Run static analysis (high-precision rules only)
    detector = SQLAntiPatternDetector(dialect="duckdb", include_style=False)
    result = detector.analyze(sql, include_structure=True)

    # Detect optimization opportunities
    opportunities = detect_opportunities(sql)
    result.opportunities = opportunities

    # Get execution plan analysis
    try:
        from qt_sql.execution.database_utils import run_explain_analyze
        from qt_sql.execution.plan_parser import build_plan_summary

        explain_result = run_explain_analyze(database, sql)
        if explain_result and explain_result.get("plan_json"):
            plan_summary = build_plan_summary(explain_result["plan_json"])

            # Populate execution metrics
            result.execution_time_ms = plan_summary.get("total_time_ms")
            result.rows_scanned = plan_summary.get("rows_scanned")
            result.rows_returned = plan_summary.get("rows_returned")
            result.efficiency_ratio = plan_summary.get("efficiency_ratio")
            result.top_operators = plan_summary.get("top_operators", [])
            result.bottleneck = plan_summary.get("bottleneck")

    except Exception as e:
        console.print(f"[red]Error analyzing execution plan: {e}[/red]")
        sys.exit(1)

    if output_json:
        import json
        output = result.to_dict()
        output["file"] = file
        output["database"] = database
        console.print_json(json.dumps(output))
        return

    console.print(f"\n[bold]Assessing:[/bold] {file}")
    console.print(f"[dim]Database: {database}[/dim]\n")

    display_assessment(result, verbose=verbose)


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()

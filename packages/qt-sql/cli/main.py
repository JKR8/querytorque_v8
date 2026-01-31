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
@click.option("--dialect", default="generic", help="SQL dialect")
@click.option("--provider", default=None, help="LLM provider (anthropic, deepseek, openai, groq, gemini)")
@click.option("--model", default=None, help="LLM model name")
@click.option("--output", "-o", type=click.Path(), help="Output file for optimized SQL")
@click.option("--dry-run", is_flag=True, help="Show what would be optimized without calling LLM")
def optimize(
    file: str,
    dialect: str,
    provider: Optional[str],
    model: Optional[str],
    output: Optional[str],
    dry_run: bool
):
    """Optimize SQL using LLM-powered analysis.

    Analyzes the SQL file and uses an LLM to suggest optimizations
    based on detected anti-patterns and best practices.

    Examples:
        qt-sql optimize query.sql
        qt-sql optimize query.sql --provider anthropic
        qt-sql optimize query.sql -o optimized.sql
    """
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

    if not result.issues:
        console.print("\n[green]No issues found - query already looks optimal.[/green]")
        return

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

    # Build optimization prompt
    issues_text = "\n".join(
        f"- {issue.name} ({issue.severity}): {issue.description}"
        for issue in result.issues
    )

    prompt = f"""You are a SQL optimization expert. Analyze the following SQL query and optimize it.

Original SQL:
```sql
{sql}
```

Detected issues:
{issues_text}

Please provide:
1. An optimized version of the SQL query
2. Explanation of the changes made
3. Expected performance improvements

Format your response as:
## Optimized SQL
```sql
<optimized query>
```

## Changes Made
<bullet list of changes>

## Expected Improvements
<description of performance benefits>
"""

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


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()

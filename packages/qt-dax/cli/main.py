"""QueryTorque DAX CLI.

Command-line interface for DAX/Power BI analysis and optimization.

Commands:
    qt-dax audit <model.vpax>    Analyze VPAX model for anti-patterns
    qt-dax optimize <model.vpax> LLM-powered DAX optimization
    qt-dax connect               Connect to Power BI Desktop
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

from qt_dax.analyzers.vpax_analyzer import VPAXAnalyzer, VPAXAnalysisResult

console = Console()


def read_vpax_file(file_path: str) -> Path:
    """Validate and return VPAX file path."""
    path = Path(file_path)
    if not path.exists():
        raise click.ClickException(f"File not found: {file_path}")
    if not path.suffix.lower() == ".vpax":
        raise click.ClickException(f"Expected .vpax file, got: {path.suffix}")
    return path


def display_analysis_result(result: VPAXAnalysisResult, verbose: bool = False) -> None:
    """Display analysis result with rich formatting."""
    # Score panel
    score = result.torque_score
    score_color = "green" if score >= 90 else "cyan" if score >= 70 else "yellow" if score >= 50 else "red"

    # Determine quality gate
    if score >= 90:
        gate = "Peak Torque"
        gate_desc = "Excellent - ready for production"
    elif score >= 70:
        gate = "Power Band"
        gate_desc = "Good - minor optimizations recommended"
    elif score >= 50:
        gate = "Stall Zone"
        gate_desc = "Fair - review before deployment"
    else:
        gate = "Redline"
        gate_desc = "Critical - requires remediation"

    severity_summary = (
        f"Critical: {result.critical_count} | "
        f"High: {result.high_count} | "
        f"Medium: {result.medium_count} | "
        f"Low: {result.low_count}"
    )

    console.print(Panel(
        f"Torque Score: [bold {score_color}]{score}/100[/bold {score_color}]\n"
        f"Quality Gate: [{score_color}]{gate}[/{score_color}] - {gate_desc}\n"
        f"{severity_summary}",
        title="DAX Analysis Result",
        border_style=score_color
    ))

    # Model summary
    if result.model_stats:
        stats = result.model_stats
        console.print("\n[bold]Model Summary:[/bold]")
        table = Table(show_header=False, box=None)
        table.add_column("Metric", style="dim")
        table.add_column("Value")
        table.add_row("Total Size", f"{stats.get('total_size_mb', 0):.1f} MB")
        table.add_row("Tables", str(stats.get('table_count', 0)))
        table.add_row("Columns", str(stats.get('column_count', 0)))
        table.add_row("Measures", str(stats.get('measure_count', 0)))
        table.add_row("Relationships", str(stats.get('relationship_count', 0)))
        console.print(table)

    if not result.issues:
        console.print("\n[green]No issues detected.[/green]")
        return

    # Issues table
    console.print()
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
            if issue.affected_object:
                console.print(f"   [dim]Affected:[/dim] {issue.affected_object}")
            if issue.suggestion:
                console.print(f"   [dim]Suggestion:[/dim] {issue.suggestion}")
            console.print()


@click.group()
@click.version_option(version="0.1.0", prog_name="qt-dax")
def cli():
    """QueryTorque DAX - Power BI/DAX Analysis and Optimization CLI."""
    pass


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("--verbose", "-v", is_flag=True, help="Show detailed issue information")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
@click.option("--output", "-o", type=click.Path(), help="Output report to HTML file")
def audit(
    file: str,
    verbose: bool,
    output_json: bool,
    output: Optional[str]
):
    """Analyze VPAX file for anti-patterns and issues.

    Performs static analysis on the Power BI model export (VPAX) and generates
    a report with detected issues, severity levels, and improvement suggestions.

    Examples:
        qt-dax audit model.vpax
        qt-dax audit model.vpax -v
        qt-dax audit model.vpax -o report.html
    """
    try:
        vpax_path = read_vpax_file(file)
    except click.ClickException as e:
        console.print(f"[red]Error: {e.message}[/red]")
        sys.exit(1)

    console.print(f"\n[bold]Analyzing:[/bold] {file}\n")

    try:
        # Run VPAX analysis
        analyzer = VPAXAnalyzer()
        result = analyzer.analyze(vpax_path)

        if output_json:
            import json
            output_data = result.to_dict()
            output_data["file"] = file
            console.print_json(json.dumps(output_data))
            return

        display_analysis_result(result, verbose=verbose)

        # Generate HTML report if requested
        if output:
            try:
                from qt_dax.renderers import DAXRenderer

                renderer = DAXRenderer()
                renderer.render_to_file(result, output)
                console.print(f"\n[green]Report saved to: {output}[/green]")
            except ImportError:
                console.print("[yellow]Renderer not available. Install jinja2 for HTML reports.[/yellow]")
            except Exception as e:
                console.print(f"[red]Failed to generate HTML report: {e}[/red]")

        # Exit with non-zero if critical/high issues found
        if result.critical_count > 0 or result.high_count > 0:
            sys.exit(1)

    except Exception as e:
        console.print(f"[red]Analysis failed: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("--provider", default=None, help="LLM provider (anthropic, deepseek, openai, groq, gemini)")
@click.option("--model", default=None, help="LLM model name")
@click.option("--output", "-o", type=click.Path(), help="Output file for optimized measures")
@click.option("--dry-run", is_flag=True, help="Show what would be optimized without calling LLM")
@click.option("--measure", "-m", multiple=True, help="Specific measure(s) to optimize")
def optimize(
    file: str,
    provider: Optional[str],
    model: Optional[str],
    output: Optional[str],
    dry_run: bool,
    measure: tuple
):
    """Optimize DAX measures using LLM-powered analysis.

    Analyzes the VPAX file and uses an LLM to suggest DAX optimizations
    based on detected anti-patterns and best practices.

    Examples:
        qt-dax optimize model.vpax
        qt-dax optimize model.vpax --provider anthropic
        qt-dax optimize model.vpax -m "Total Sales" -m "Profit %"
    """
    try:
        vpax_path = read_vpax_file(file)
    except click.ClickException as e:
        console.print(f"[red]Error: {e.message}[/red]")
        sys.exit(1)

    console.print(f"\n[bold]Analyzing:[/bold] {file}\n")

    try:
        # Run VPAX analysis
        analyzer = VPAXAnalyzer()
        result = analyzer.analyze(vpax_path)

        display_analysis_result(result, verbose=False)

        if not result.issues:
            console.print("\n[green]No issues found - model already looks optimal.[/green]")
            return

        # Filter measures if specified
        measures_to_optimize = list(measure) if measure else None

        if dry_run:
            console.print("\n[yellow]Dry run mode - LLM optimization skipped.[/yellow]")
            console.print("Issues that would be addressed:")
            for issue in result.issues:
                if measures_to_optimize and issue.affected_object not in measures_to_optimize:
                    continue
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

        # Get measures with issues
        measure_issues = {}
        for issue in result.issues:
            if issue.category == "dax_anti_pattern" and issue.affected_object:
                if measures_to_optimize and issue.affected_object not in measures_to_optimize:
                    continue
                if issue.affected_object not in measure_issues:
                    measure_issues[issue.affected_object] = []
                measure_issues[issue.affected_object].append(issue)

        if not measure_issues:
            console.print("\n[yellow]No DAX measure issues to optimize.[/yellow]")
            return

        console.print(f"\n[bold]Optimizing {len(measure_issues)} measure(s)...[/bold]")

        # Get measure definitions from analysis
        measures_data = result.raw_data.get("measures", [])
        measure_defs = {m["name"]: m.get("expression", "") for m in measures_data}

        optimizations = []

        for measure_name, issues in measure_issues.items():
            console.print(f"\n[bold]Optimizing: {measure_name}[/bold]")

            original_dax = measure_defs.get(measure_name, "")
            if not original_dax:
                console.print(f"  [yellow]Could not find definition, skipping.[/yellow]")
                continue

            issues_text = "\n".join(
                f"- {issue.name} ({issue.severity}): {issue.description}"
                for issue in issues
            )

            prompt = f"""You are a DAX optimization expert. Optimize the following DAX measure.

Measure: {measure_name}

Original DAX:
```dax
{original_dax}
```

Detected issues:
{issues_text}

Please provide:
1. An optimized version of the DAX measure
2. Brief explanation of changes
3. Expected performance improvement

Format your response as:
## Optimized DAX
```dax
<optimized measure>
```

## Changes Made
<brief list>

## Expected Improvement
<description>
"""

            try:
                response = llm_client.analyze(prompt)

                console.print(Markdown(response))

                # Extract optimized DAX
                import re
                dax_match = re.search(r"```dax\s*(.*?)\s*```", response, re.DOTALL)
                if dax_match:
                    optimized_dax = dax_match.group(1).strip()
                    optimizations.append({
                        "measure": measure_name,
                        "original": original_dax,
                        "optimized": optimized_dax,
                    })

            except Exception as e:
                console.print(f"  [red]Optimization failed: {e}[/red]")

        # Save optimizations if output specified
        if output and optimizations:
            output_path = Path(output)

            if output_path.suffix == ".json":
                import json
                output_path.write_text(json.dumps(optimizations, indent=2), encoding="utf-8")
            else:
                # Write as DAX text file
                content = []
                for opt in optimizations:
                    content.append(f"-- Measure: {opt['measure']}")
                    content.append(f"-- Original:")
                    content.append(f"-- {opt['original'].replace(chr(10), chr(10) + '-- ')}")
                    content.append(f"")
                    content.append(f"{opt['measure']} =")
                    content.append(opt['optimized'])
                    content.append("")
                    content.append("")

                output_path.write_text("\n".join(content), encoding="utf-8")

            console.print(f"\n[green]Optimizations saved to: {output}[/green]")

    except Exception as e:
        console.print(f"[red]Optimization failed: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.option("--port", "-p", type=int, default=None, help="Connect to specific port")
@click.option("--list", "list_instances", is_flag=True, help="List running PBI Desktop instances")
@click.option("--query", "-q", type=str, help="Execute DAX query")
@click.option("--validate", "-v", type=str, help="Validate DAX expression")
def connect(
    port: Optional[int],
    list_instances: bool,
    query: Optional[str],
    validate: Optional[str]
):
    """Connect to a running Power BI Desktop instance.

    Discovers and connects to local Power BI Desktop instances via XMLA.
    Requires Windows and the pyadomd package.

    Examples:
        qt-dax connect --list
        qt-dax connect -q "EVALUATE ROW('Test', 1+1)"
        qt-dax connect --validate "SUM('Sales'[Amount])"
    """
    try:
        from qt_dax.connections import (
            PBIDesktopConnection,
            find_pbi_instances,
            validate_dax_against_desktop,
        )
    except ImportError as e:
        console.print(f"[red]Connection module not available: {e}[/red]")
        console.print("[dim]Note: Requires Windows and pyadomd package.[/dim]")
        sys.exit(1)

    # List instances
    if list_instances:
        try:
            instances = find_pbi_instances()
            if not instances:
                console.print("[yellow]No Power BI Desktop instances found.[/yellow]")
                console.print("[dim]Make sure Power BI Desktop is running with a model loaded.[/dim]")
                return

            console.print(f"\n[bold]Found {len(instances)} Power BI Desktop instance(s):[/bold]\n")

            table = Table(show_header=True, header_style="bold")
            table.add_column("Port", justify="right")
            table.add_column("Workspace")
            table.add_column("Path")

            for inst in instances:
                table.add_row(str(inst.port), inst.name, inst.workspace_path)

            console.print(table)

        except OSError as e:
            console.print(f"[red]{e}[/red]")
            sys.exit(1)

        return

    # Quick validation
    if validate:
        console.print(f"\n[bold]Validating DAX:[/bold] {validate}\n")

        try:
            valid, error, used_port = validate_dax_against_desktop(validate)

            if valid:
                console.print(f"[green]Valid[/green] (tested on port {used_port})")
            else:
                console.print(f"[red]Invalid:[/red] {error}")
                sys.exit(1)

        except Exception as e:
            console.print(f"[red]Validation failed: {e}[/red]")
            sys.exit(1)

        return

    # Execute query
    if query:
        try:
            instances = find_pbi_instances()
            if not instances:
                console.print("[red]No Power BI Desktop instances found.[/red]")
                sys.exit(1)

            target_port = port or instances[0].port
            console.print(f"\n[bold]Executing on port {target_port}:[/bold]\n")
            console.print(Syntax(query, "sql", theme="monokai"))
            console.print()

            with PBIDesktopConnection(target_port) as conn:
                results = conn.execute_dax(query)

                if not results:
                    console.print("[dim]Query returned no results.[/dim]")
                    return

                # Display as table
                table = Table(show_header=True, header_style="bold")
                for col in results[0].keys():
                    table.add_column(col)

                for row in results[:100]:  # Limit to 100 rows
                    table.add_row(*[str(v) for v in row.values()])

                console.print(table)

                if len(results) > 100:
                    console.print(f"\n[dim]Showing 100 of {len(results)} rows.[/dim]")

        except Exception as e:
            console.print(f"[red]Query failed: {e}[/red]")
            sys.exit(1)

        return

    # Default: show connection info
    try:
        instances = find_pbi_instances()
        if not instances:
            console.print("[yellow]No Power BI Desktop instances found.[/yellow]")
            console.print("\n[dim]To use this command:")
            console.print("  1. Open Power BI Desktop")
            console.print("  2. Load a .pbix file")
            console.print("  3. Run: qt-dax connect --list[/dim]")
            return

        inst = instances[0]
        console.print(f"\n[bold]Connected to Power BI Desktop[/bold]")
        console.print(f"Port: {inst.port}")
        console.print(f"Workspace: {inst.name}")

        with PBIDesktopConnection(inst.port) as conn:
            summary = conn.get_model_summary()
            console.print(f"\n[bold]Model Summary:[/bold]")
            console.print(f"  Tables: {summary['table_count']}")
            console.print(f"  Measures: {summary['measure_count']}")

            if summary['tables']:
                console.print(f"\n[bold]Tables:[/bold]")
                for t in summary['tables'][:10]:
                    console.print(f"  - {t}")
                if len(summary['tables']) > 10:
                    console.print(f"  ... and {len(summary['tables']) - 10} more")

    except Exception as e:
        console.print(f"[red]Connection failed: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.argument("vpax1", type=click.Path(exists=True))
@click.argument("vpax2", type=click.Path(exists=True))
@click.option("--output", "-o", type=click.Path(), help="Output diff report to HTML file")
def diff(vpax1: str, vpax2: str, output: Optional[str]):
    """Compare two VPAX files and show differences.

    Useful for tracking model changes between versions or deployments.

    Examples:
        qt-dax diff model_v1.vpax model_v2.vpax
        qt-dax diff before.vpax after.vpax -o diff_report.html
    """
    try:
        path1 = read_vpax_file(vpax1)
        path2 = read_vpax_file(vpax2)
    except click.ClickException as e:
        console.print(f"[red]Error: {e.message}[/red]")
        sys.exit(1)

    console.print(f"\n[bold]Comparing:[/bold]")
    console.print(f"  Base: {vpax1}")
    console.print(f"  New:  {vpax2}\n")

    try:
        from qt_dax.analyzers.vpax_differ import VPAXDiffer

        differ = VPAXDiffer()
        diff_result = differ.compare(path1, path2)

        # Display summary
        summary = diff_result.summary
        console.print(Panel(
            f"Added: [green]{summary.added}[/green] | "
            f"Modified: [yellow]{summary.modified}[/yellow] | "
            f"Removed: [red]{summary.removed}[/red]\n"
            f"Score Change: {summary.score_delta.old_score} -> {summary.score_delta.new_score} "
            f"({'[green]+' if summary.score_delta.delta >= 0 else '[red]'}{summary.score_delta.delta}[/])",
            title="Diff Summary",
            border_style="blue"
        ))

        # Show changes
        if diff_result.changes:
            console.print("\n[bold]Changes:[/bold]")

            table = Table(show_header=True, header_style="bold")
            table.add_column("Type", width=10)
            table.add_column("Category", width=12)
            table.add_column("Object", width=30)
            table.add_column("Change", width=40)

            change_colors = {
                "added": "green",
                "modified": "yellow",
                "removed": "red",
            }

            for change in diff_result.changes[:50]:
                color = change_colors.get(change.change_type.value, "white")
                table.add_row(
                    f"[{color}]{change.change_type.value.upper()}[/{color}]",
                    change.category.value,
                    change.object_name[:30],
                    (change.description[:40] + "...") if len(change.description) > 40 else change.description
                )

            console.print(table)

            if len(diff_result.changes) > 50:
                console.print(f"\n[dim]Showing 50 of {len(diff_result.changes)} changes.[/dim]")

        else:
            console.print("[green]No differences found.[/green]")

        # Generate HTML report if requested
        if output:
            try:
                from qt_dax.analyzers.vpax_differ import DiffReportGenerator

                generator = DiffReportGenerator()
                html = generator.generate_html_report(diff_result)
                Path(output).write_text(html, encoding="utf-8")
                console.print(f"\n[green]Diff report saved to: {output}[/green]")

            except Exception as e:
                console.print(f"[red]Failed to generate diff report: {e}[/red]")

    except ImportError:
        console.print("[red]VPAX differ not available.[/red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Diff failed: {e}[/red]")
        sys.exit(1)


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()

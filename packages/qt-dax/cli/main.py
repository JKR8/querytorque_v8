"""QueryTorque DAX CLI.

Command-line interface for DAX/Power BI analysis and optimization.

Commands:
    qt-dax audit <model.vpax>    Analyze VPAX model for anti-patterns
    qt-dax optimize <model.vpax> LLM-powered DAX optimization
    qt-dax connect               Connect to Power BI Desktop
    qt-dax validate <orig.dax> <opt.dax> Validate equivalence + performance
"""

import asyncio
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.syntax import Syntax
from rich.markdown import Markdown

from qt_dax.analyzers.vpax_analyzer import ReportGenerator, DiagnosticReport
from qt_dax.analyzers.pbip_analyzer import PBIPReportGenerator

console = Console()


def read_vpax_file(file_path: str) -> Path:
    """Validate and return VPAX file path."""
    path = Path(file_path)
    if not path.exists():
        raise click.ClickException(f"File not found: {file_path}")
    if not path.suffix.lower() == ".vpax":
        raise click.ClickException(f"Expected .vpax file, got: {path.suffix}")
    return path


def resolve_model_input(file_path: str) -> tuple[str, Path]:
    """Resolve a model input path to VPAX or PBIP semantic model."""
    path = Path(file_path)
    if not path.exists():
        raise click.ClickException(f"File not found: {file_path}")

    if path.is_file() and path.suffix.lower() == ".vpax":
        return "vpax", path

    if path.is_file() and path.suffix.lower() == ".pbip":
        semantic_model = path.with_suffix(".SemanticModel")
        if semantic_model.exists():
            return "pbip", semantic_model
        raise click.ClickException(f"Semantic model folder not found for: {file_path}")

    if path.is_dir():
        if path.name.endswith(".SemanticModel") and (path / "definition" / "model.tmdl").exists():
            return "pbip", path
        if path.name == "definition" and (path / "model.tmdl").exists():
            return "pbip", path.parent
        semantic_models = list(path.glob("*.SemanticModel"))
        if len(semantic_models) == 1:
            return "pbip", semantic_models[0]

    raise click.ClickException(
        "Expected a .vpax file, .pbip file, or a .SemanticModel folder"
    )


def read_dax_file(file_path: str) -> str:
    """Read DAX from file."""
    path = Path(file_path)
    if not path.exists():
        raise click.ClickException(f"File not found: {file_path}")
    if not path.suffix.lower() in (".dax", ".txt"):
        raise click.ClickException(f"Expected .dax file, got: {path.suffix}")
    return path.read_text(encoding="utf-8")


def display_analysis_result(result: DiagnosticReport, verbose: bool = False) -> None:
    """Display analysis result with rich formatting."""
    summary = result.summary

    # Score panel
    score = summary.torque_score
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
        f"Critical: {summary.critical_count} | "
        f"High: {summary.high_count} | "
        f"Medium: {summary.medium_count} | "
        f"Low: {summary.low_count}"
    )

    console.print(Panel(
        f"Torque Score: [bold {score_color}]{score}/100[/bold {score_color}]\n"
        f"Quality Gate: [{score_color}]{gate}[/{score_color}] - {gate_desc}\n"
        f"{severity_summary}",
        title="DAX Analysis Result",
        border_style=score_color
    ))

    # Model summary
    console.print("\n[bold]Model Summary:[/bold]")
    table = Table(show_header=False, box=None)
    table.add_column("Metric", style="dim")
    table.add_column("Value")
    table.add_row("Total Size", f"{summary.total_size_bytes / 1024 / 1024:.1f} MB")
    table.add_row("Tables", str(summary.total_tables))
    table.add_row("Columns", str(summary.total_columns))
    table.add_row("Measures", str(summary.total_measures))
    table.add_row("Relationships", str(summary.total_relationships))
    console.print(table)

    if not result.all_issues:
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

    for issue in result.all_issues:
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
        for i, issue in enumerate(result.all_issues, 1):
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
        model_kind, model_path = resolve_model_input(file)
    except click.ClickException as e:
        console.print(f"[red]Error: {e.message}[/red]")
        sys.exit(1)

    console.print(f"\n[bold]Analyzing:[/bold] {file}\n")

    try:
        # Run analysis
        if model_kind == "vpax":
            generator = ReportGenerator(str(model_path))
        else:
            generator = PBIPReportGenerator(str(model_path))
        result = generator.generate()

        if output_json:
            import json
            from dataclasses import asdict
            output_data = asdict(result)
            output_data["file"] = file
            console.print_json(json.dumps(output_data, default=str))
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
    if result.summary.critical_count > 0 or result.summary.high_count > 0:
        sys.exit(1)

    except Exception as e:
        console.print(f"[red]Analysis failed: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("--provider", default=None, help="LLM provider (deepseek, openrouter)")
@click.option("--model", default=None, help="LLM model name")
@click.option("--output", "-o", type=click.Path(), help="Output file for optimized measures")
@click.option("--dry-run", is_flag=True, help="Show what would be optimized without calling LLM")
@click.option("--measure", "-m", multiple=True, help="Specific measure(s) to optimize")
@click.option("--dspy", is_flag=True, help="Use DSPy optimizer with validation")
@click.option("--port", "-p", type=int, default=None, help="Power BI Desktop port for validation")
@click.option("--max-retries", default=2, show_default=True, help="Max DSPy retries on validation failure")
@click.option("--warmup-runs", default=2, show_default=True, help="Runs per query (min time used)")
@click.option("--max-rows", default=10000, show_default=True, help="Max rows to compare")
@click.option("--sample-limit", default=5, show_default=True, help="Max mismatches to show")
@click.option("--tolerance", default=1e-9, show_default=True, help="Float comparison tolerance")
def optimize(
    file: str,
    provider: Optional[str],
    model: Optional[str],
    output: Optional[str],
    dry_run: bool,
    measure: tuple,
    dspy: bool,
    port: Optional[int],
    max_retries: int,
    warmup_runs: int,
    max_rows: int,
    sample_limit: int,
    tolerance: float,
):
    """Optimize DAX measures using LLM-powered analysis.

    Analyzes the VPAX file and uses an LLM to suggest DAX optimizations
    based on detected anti-patterns and best practices.

    Examples:
        qt-dax optimize model.vpax
        qt-dax optimize model.vpax --provider openrouter
        qt-dax optimize model.vpax -m "Total Sales" -m "Profit %"
        qt-dax optimize model.vpax --dspy --port 54000
    """
    try:
        model_kind, model_path = resolve_model_input(file)
    except click.ClickException as e:
        console.print(f"[red]Error: {e.message}[/red]")
        sys.exit(1)

    console.print(f"\n[bold]Analyzing:[/bold] {file}\n")

    try:
        # Run analysis
        if model_kind == "vpax":
            generator = ReportGenerator(str(model_path))
        else:
            generator = PBIPReportGenerator(str(model_path))
        result = generator.generate()

        display_analysis_result(result, verbose=False)

        if not result.all_issues:
            console.print("\n[green]No issues found - model already looks optimal.[/green]")
            return

        # Filter measures if specified
        measures_to_optimize = list(measure) if measure else None

        if dry_run:
            console.print("\n[yellow]Dry run mode - LLM optimization skipped.[/yellow]")
            console.print("Issues that would be addressed:")
            for issue in result.all_issues:
                if measures_to_optimize and issue.affected_object not in measures_to_optimize:
                    continue
                console.print(f"  - {issue.name}: {issue.suggestion or issue.description}")
            return

        # Get measures with issues
        measure_issues = {}
        for issue in result.all_issues:
            if issue.category == "dax_anti_pattern" and issue.affected_object:
                if measures_to_optimize and issue.affected_object not in measures_to_optimize:
                    continue
                if issue.affected_object not in measure_issues:
                    measure_issues[issue.affected_object] = []
                measure_issues[issue.affected_object].append(issue)

        if dspy:
            try:
                import dspy  # noqa: F401
                from qt_dax.connections import PBIDesktopConnection, find_pbi_instances
                from qt_dax.validation import DAXEquivalenceValidator
                from qt_dax.optimization import optimize_measure_with_validation
            except ImportError as e:
                console.print(f"[red]DSPy optimization not available: {e}[/red]")
                console.print("[dim]Install with: pip install dspy-ai[/dim]")
                sys.exit(1)

            try:
                instances = find_pbi_instances()
            except OSError as e:
                console.print(f"[red]{e}[/red]")
                sys.exit(1)

            if not instances:
                console.print("[red]No Power BI Desktop instances found.[/red]")
                console.print("[dim]Open Power BI Desktop with a model loaded and retry.[/dim]")
                sys.exit(1)

            target_port = port or instances[0].port
            console.print(f"[dim]Using Power BI Desktop port: {target_port}[/dim]")

            if not measure_issues:
                console.print("\n[yellow]No DAX measure issues to optimize.[/yellow]")
                return

            console.print(f"\n[bold]Optimizing {len(measure_issues)} measure(s) with DSPy...[/bold]")

            measures_data = result.measures if hasattr(result, "measures") else []
            measure_defs = {m.get("name"): m.get("expression", "") for m in measures_data}

            optimizations = []

            with PBIDesktopConnection(target_port) as conn:
                validator = DAXEquivalenceValidator(
                    connection=conn,
                    tolerance=tolerance,
                    max_rows_to_compare=max_rows,
                    sample_mismatch_limit=sample_limit,
                    warmup_runs=warmup_runs,
                )

                for measure_name, issues in measure_issues.items():
                    console.print(f"\n[bold]Optimizing: {measure_name}[/bold]")

                    original_dax = measure_defs.get(measure_name, "")
                    if not original_dax:
                        console.print("  [yellow]Could not find definition, skipping.[/yellow]")
                        continue

                    issues_text = "\n".join(
                        f"- {issue.name} ({issue.severity}): {issue.description}"
                        for issue in issues
                    )

                    result_opt = optimize_measure_with_validation(
                        measure_name=measure_name,
                        original_dax=original_dax,
                        issues_text=issues_text,
                        validator=validator,
                        provider=provider or "deepseek",
                        model=model,
                        max_retries=max_retries,
                    )

                    if result_opt.correct:
                        console.print(
                            f"  [green]Validated[/green] "
                            f"({result_opt.speedup_ratio:.2f}x, attempts={result_opt.attempts})"
                        )
                        console.print(Syntax(result_opt.optimized_dax, "sql", theme="monokai"))
                        optimizations.append({
                            "measure": measure_name,
                            "original": original_dax,
                            "optimized": result_opt.optimized_dax,
                        })
                    else:
                        console.print(
                            f"  [red]Validation failed[/red] "
                            f"(attempts={result_opt.attempts})"
                        )
                        if result_opt.error:
                            console.print(f"  [dim]{result_opt.error}[/dim]")

            if output and optimizations:
                output_path = Path(output)
                if output_path.suffix == ".json":
                    import json
                    output_path.write_text(json.dumps(optimizations, indent=2), encoding="utf-8")
                else:
                    content = []
                    for opt in optimizations:
                        content.append(f\"-- Measure: {opt['measure']}\")
                        content.append(\"-- Original:\")
                        content.append(f\"-- {opt['original'].replace(chr(10), chr(10) + '-- ')}\")
                        content.append(\"\")
                        content.append(f\"{opt['measure']} =\")
                        content.append(opt[\"optimized\"])
                        content.append(\"\")
                        content.append(\"\")

                    output_path.write_text(\"\\n\".join(content), encoding=\"utf-8\")

                console.print(f\"\\n[green]Optimizations saved to: {output}[/green]\")

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

        if not measure_issues:
            console.print("\n[yellow]No DAX measure issues to optimize.[/yellow]")
            return

        console.print(f"\n[bold]Optimizing {len(measure_issues)} measure(s)...[/bold]")

        # Get measure definitions from analysis
        measures_data = result.measures if hasattr(result, "measures") else []
        measure_defs = {m.get("name"): m.get("expression", "") for m in measures_data}

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
@click.argument("original", type=click.Path(exists=True))
@click.argument("optimized", type=click.Path(exists=True))
@click.option("--port", "-p", type=int, default=None, help="Connect to specific port")
@click.option("--warmup-runs", default=2, show_default=True, help="Runs per query (min time used)")
@click.option("--max-rows", default=10000, show_default=True, help="Max rows to compare")
@click.option("--sample-limit", default=5, show_default=True, help="Max mismatches to show")
@click.option("--tolerance", default=1e-9, show_default=True, help="Float comparison tolerance")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed output")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON")
def validate(
    original: str,
    optimized: str,
    port: Optional[int],
    warmup_runs: int,
    max_rows: int,
    sample_limit: int,
    tolerance: float,
    verbose: bool,
    output_json: bool,
):
    """Validate that optimized DAX is equivalent to original.

    Executes both DAX expressions/queries against a local Power BI Desktop
    instance and compares results and performance.

    Examples:
        qt-dax validate original.dax optimized.dax
        qt-dax validate original.dax optimized.dax --port 54000 --warmup-runs 3
        qt-dax validate original.dax optimized.dax --json
    """
    try:
        original_dax = read_dax_file(original)
        optimized_dax = read_dax_file(optimized)
    except click.ClickException as e:
        console.print(f"[red]Error: {e.message}[/red]")
        sys.exit(1)

    try:
        from qt_dax.connections import PBIDesktopConnection, find_pbi_instances
        from qt_dax.validation import DAXEquivalenceValidator
    except ImportError as e:
        console.print(f"[red]Validation module not available: {e}[/red]")
        console.print("[dim]Note: Requires Windows and pyadomd package.[/dim]")
        sys.exit(1)

    # Find PBI Desktop instance
    try:
        instances = find_pbi_instances()
    except OSError as e:
        console.print(f"[red]{e}[/red]")
        sys.exit(1)

    if not instances:
        console.print("[red]No Power BI Desktop instances found.[/red]")
        console.print("[dim]Open Power BI Desktop with a model loaded and retry.[/dim]")
        sys.exit(1)

    target_port = port or instances[0].port

    if not output_json:
        console.print(f"\n[bold]Validating DAX equivalence[/bold]")
        console.print(f"Original: {original}")
        console.print(f"Optimized: {optimized}")
        console.print(f"Port: {target_port}")
        console.print()

    try:
        with PBIDesktopConnection(target_port) as conn:
            validator = DAXEquivalenceValidator(
                connection=conn,
                tolerance=tolerance,
                max_rows_to_compare=max_rows,
                sample_mismatch_limit=sample_limit,
                warmup_runs=warmup_runs,
            )
            result = validator.validate(original_dax, optimized_dax)
    except Exception as e:
        if output_json:
            import json
            console.print_json(json.dumps({"status": "error", "error": str(e)}))
        else:
            console.print(f"[red]Validation failed: {e}[/red]")
        sys.exit(1)

    if output_json:
        import json
        console.print_json(json.dumps(result.to_dict()))
        sys.exit(0 if result.status == "pass" else 1)

    status_color = {
        "pass": "green",
        "fail": "red",
        "error": "red",
        "skip": "yellow",
    }.get(result.status, "white")

    console.print(Panel(
        f"[bold {status_color}]{result.status.upper()}[/bold {status_color}]",
        title="Validation Result",
        border_style=status_color,
    ))

    table = Table(show_header=True, header_style="bold")
    table.add_column("Metric", width=20)
    table.add_column("Original", justify="right", width=15)
    table.add_column("Optimized", justify="right", width=15)
    table.add_column("Status", width=20)

    row_status = "[green]match[/green]" if result.row_count_match else "[red]MISMATCH[/red]"
    table.add_row(
        "Row Count",
        str(result.original_row_count),
        str(result.optimized_row_count),
        row_status,
    )

    speedup = result.speedup_ratio
    speedup_str = f"{speedup:.2f}x"
    if speedup > 1:
        timing_status = f"[green]{speedup_str} faster[/green]"
    elif speedup < 1:
        timing_status = f"[yellow]{speedup_str} slower[/yellow]"
    else:
        timing_status = "unchanged"

    table.add_row(
        "Timing",
        f"{result.original_execution_time_ms:.1f}ms",
        f"{result.optimized_execution_time_ms:.1f}ms",
        timing_status,
    )

    console.print(table)

    if result.sample_mismatches:
        console.print(f"\n[bold]Sample Mismatches (first {len(result.sample_mismatches)}):[/bold]")
        for mismatch in result.sample_mismatches:
            console.print(
                f"  Row {mismatch['row']}, Column '{mismatch['column']}': "
                f"{mismatch['original']} vs {mismatch['optimized']}"
            )

    for warning in result.warnings:
        console.print(f"[yellow]Warning: {warning}[/yellow]")

    for error in result.errors:
        console.print(f"[red]Error: {error}[/red]")

    if verbose:
        console.print("\n[bold]Run Times (ms):[/bold]")
        console.print(f"  Original: {', '.join(f'{t:.1f}' for t in result.original_run_times_ms)}")
        console.print(f"  Optimized: {', '.join(f'{t:.1f}' for t in result.optimized_run_times_ms)}")

    sys.exit(0 if result.status == "pass" else 1)


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
        from qt_dax.analyzers.vpax_analyzer import ReportGenerator

        # Generate reports for both files
        generator1 = ReportGenerator(str(path1))
        report1 = generator1.generate()

        generator2 = ReportGenerator(str(path2))
        report2 = generator2.generate()

        # Convert to dicts for comparison
        report1_dict = asdict(report1)
        report2_dict = asdict(report2)

        differ = VPAXDiffer()
        diff_result = differ.compare(report1_dict, report2_dict, str(path1), str(path2))

        # Display summary
        summary = diff_result.summary
        total_added = summary.measures_added + summary.tables_added + summary.columns_added + summary.relationships_added
        total_removed = summary.measures_removed + summary.tables_removed + summary.columns_removed + summary.relationships_removed
        total_modified = summary.measures_modified + summary.tables_modified
        console.print(Panel(
            f"Added: [green]{total_added}[/green] | "
            f"Modified: [yellow]{total_modified}[/yellow] | "
            f"Removed: [red]{total_removed}[/red]\n"
            f"Score Change: {summary.score_delta.old_torque_score} -> {summary.score_delta.new_torque_score} "
            f"({'[green]+' if summary.score_delta.delta >= 0 else '[red]'}{summary.score_delta.delta}[/])",
            title="Diff Summary",
            border_style="blue"
        ))

        # Show changes
        if diff_result.all_changes:
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

            for change in diff_result.all_changes[:50]:
                color = change_colors.get(change.change_type.value, "white")
                table.add_row(
                    f"[{color}]{change.change_type.value.upper()}[/{color}]",
                    change.category.value,
                    change.object_name[:30],
                    (change.description[:40] + "...") if len(change.description) > 40 else change.description
                )

            console.print(table)

            if len(diff_result.all_changes) > 50:
                console.print(f"\n[dim]Showing 50 of {len(diff_result.all_changes)} changes.[/dim]")

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

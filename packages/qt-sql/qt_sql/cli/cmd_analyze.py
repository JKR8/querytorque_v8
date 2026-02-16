"""qt analyze — scan beam sessions and surface promotion candidates.

Usage:
    qt analyze postgres_dsb_76
    qt analyze postgres_dsb_76 --min-speedup 3.0
    qt analyze postgres_dsb_76 --export candidates.json
    qt analyze postgres_dsb_76 --dedup
"""

from __future__ import annotations

import json
from pathlib import Path

import click

from ._common import (
    console,
    engine_from_config,
    resolve_benchmark,
)


@click.command()
@click.argument("benchmark")
@click.option(
    "--min-speedup",
    default=2.0,
    show_default=True,
    help="Minimum speedup for win candidates.",
)
@click.option(
    "--max-regression",
    default=0.90,
    show_default=True,
    help="Maximum speedup for regression candidates.",
)
@click.option(
    "--export",
    "export_path",
    type=click.Path(),
    default=None,
    help="Export candidates to JSON file.",
)
@click.option(
    "--dedup",
    is_flag=True,
    help="Deduplicate: keep best per (query, transform) pair.",
)
@click.pass_context
def analyze(
    ctx: click.Context,
    benchmark: str,
    min_speedup: float,
    max_regression: float,
    export_path: str | None,
    dedup: bool,
) -> None:
    """Scan beam sessions and surface promotion candidates."""
    from qt_sql.beam_analyzer import (
        analyze_beam_sessions,
        candidates_to_json,
        deduplicate_candidates,
        format_candidates_report,
    )

    bench_dir = resolve_benchmark(benchmark)
    sessions_dir = bench_dir / "beam_sessions"

    if not sessions_dir.exists():
        console.print(
            f"[bold red]No beam_sessions/ directory in {bench_dir.name}[/bold red]"
        )
        raise SystemExit(1)

    engine = engine_from_config(bench_dir)

    console.print(f"\n[bold cyan]Analyzing beam sessions: {benchmark}[/bold cyan]")
    console.print(f"  Engine: {engine}")
    console.print(f"  Sessions dir: {sessions_dir}")
    console.print(f"  Min speedup: {min_speedup:.1f}x")
    console.print(f"  Max regression: {max_regression:.2f}x")
    console.print()

    results = analyze_beam_sessions(
        sessions_dir=sessions_dir,
        benchmark_dir=bench_dir,
        min_speedup=min_speedup,
        max_regression=max_regression,
    )

    if dedup:
        results["wins"] = deduplicate_candidates(results["wins"])
        results["regressions"] = deduplicate_candidates(results["regressions"])
        console.print("[dim]Deduplicated: keeping best per (query, transform)[/dim]\n")

    # Print report
    report = format_candidates_report(
        results,
        benchmark_name=benchmark,
        sessions_dir=str(sessions_dir),
    )
    console.print(report)

    # Export if requested
    if export_path:
        all_candidates = results["wins"] + results["regressions"]
        export_data = {
            "benchmark": benchmark,
            "engine": engine,
            "min_speedup": min_speedup,
            "max_regression": max_regression,
            "wins": candidates_to_json(results["wins"]),
            "regressions": candidates_to_json(results["regressions"]),
        }
        out = Path(export_path)
        out.write_text(json.dumps(export_data, indent=2), encoding="utf-8")
        console.print(f"\n[green]Exported {len(all_candidates)} candidates to {out}[/green]")

    # Print next steps
    console.print("\n[bold]Next steps:[/bold]")
    if not export_path:
        console.print(f"  qt analyze {benchmark} --export candidates.json")
    console.print(f"  qt promote {benchmark} --from candidates.json --ids w1,w2,r1")

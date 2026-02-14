"""CLI command: qt workload — fleet-level optimization."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import click

from ._common import resolve_benchmark

logger = logging.getLogger(__name__)


@click.command()
@click.argument("benchmark")
@click.option(
    "--target-size", "-t",
    default="",
    help="Target warehouse/instance size (e.g., 'X-Small', '2vCPU').",
)
@click.option(
    "--scenario", "-s",
    default="",
    help="Scenario card name (e.g., 'xsmall_survival', 'postgres_small_instance').",
)
@click.option(
    "--max-tier3", "-m",
    default=20,
    show_default=True,
    help="Maximum number of queries for Tier 3 deep optimization.",
)
@click.option(
    "--output", "-o",
    default="",
    help="Output file for scorecard (markdown). Prints to stdout if omitted.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Only run triage and fleet detection, skip optimization.",
)
def workload(
    benchmark: str,
    target_size: str,
    scenario: str,
    max_tier3: int,
    output: str,
    dry_run: bool,
) -> None:
    """Run workload-level optimization on a benchmark.

    Triages all queries by pain × frequency × tractability,
    applies fleet-level actions, then optimizes individual queries
    through Tier 2 (light) and Tier 3 (deep) pipelines.

    Example:
        qt workload duckdb_tpcds --target-size Small --scenario duckdb_embedded
    """
    bench_dir = resolve_benchmark(benchmark)
    config_path = bench_dir / "config.json"

    if not config_path.exists():
        click.echo(f"Error: config.json not found in {bench_dir}", err=True)
        raise SystemExit(1)

    # Load queries from benchmark
    config = json.loads(config_path.read_text())
    queries_dir = bench_dir / "queries"
    queries = []

    if queries_dir.exists():
        for qf in sorted(queries_dir.glob("*.sql")):
            qid = qf.stem
            queries.append({
                "query_id": qid,
                "sql": qf.read_text(),
                "duration_ms": None,
                "timed_out": False,
                "spills_remote": False,
                "meets_sla": False,
            })
    else:
        click.echo(f"Warning: No queries/ directory in {bench_dir}", err=True)
        click.echo("Provide queries as SQL files in a queries/ subdirectory.")
        raise SystemExit(1)

    click.echo(f"Workload: {len(queries)} queries from {benchmark}")

    if dry_run:
        # Just triage
        from ..workload.triage import triage_workload
        triage = triage_workload(queries)
        click.echo(f"\nTriage Results:")
        click.echo(f"  Skip: {len(triage.skipped)}")
        click.echo(f"  Tier 2: {len(triage.tier_2_queries)}")
        click.echo(f"  Tier 3: {len(triage.tier_3_queries)}")
        click.echo(f"  Quick-win: {len(triage.quick_wins)}")

        # Fleet detection
        from ..workload.fleet import detect_fleet_patterns
        fleet = detect_fleet_patterns(queries, engine=config.get("engine", "duckdb"))
        if fleet.actions:
            click.echo(f"\nFleet Actions:")
            for a in fleet.actions:
                click.echo(f"  [{a.action_type}] {a.action} ({len(a.queries_affected)} queries)")
        return

    # Full workload session
    from ..workload.session import WorkloadSession, WorkloadConfig

    wconfig = WorkloadConfig(
        benchmark_dir=str(bench_dir),
        engine=config.get("engine", "duckdb"),
        scenario=scenario,
        original_warehouse=config.get("warehouse_size", ""),
        target_warehouse=target_size,
        max_tier3_queries=max_tier3,
    )

    # Try to create pipeline for actual optimization
    pipeline = None
    try:
        from ..pipeline import Pipeline
        pipeline = Pipeline(benchmark_dir=bench_dir)
    except Exception as e:
        click.echo(f"Warning: Could not create pipeline: {e}", err=True)
        click.echo("Running in triage-only mode (no actual optimization).")

    session = WorkloadSession(wconfig, queries, pipeline=pipeline)
    scorecard = session.run()

    # Render scorecard
    from ..workload.scorecard import render_scorecard_markdown
    md = render_scorecard_markdown(scorecard)

    if output:
        Path(output).write_text(md)
        click.echo(f"Scorecard written to {output}")
    else:
        click.echo(md)

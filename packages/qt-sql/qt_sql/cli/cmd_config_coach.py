"""qt config-coach — iterative SET LOCAL + pg_hint_plan tuning with LLM reflection."""

from __future__ import annotations

import json

import click


@click.command("config-coach")
@click.argument("benchmark")
@click.option("--max-iterations", default=3, show_default=True,
              type=click.IntRange(1, 5),
              help="Maximum coaching iterations.")
@click.option("--max-candidates", default=8, show_default=True,
              type=click.IntRange(1, 20),
              help="Max candidates per iteration.")
@click.option("--min-speedup", default=1.05, show_default=True,
              help="Target speedup to declare a WIN.")
@click.option("--dry-run", is_flag=True,
              help="Call LLM and parse, but skip benchmarking.")
@click.option("-q", "--query", multiple=True,
              help="Query filter (repeatable, prefix match).")
@click.option("--model", default=None,
              help="LLM model override. Value depends on provider in .env: "
                   "openrouter: deepseek/deepseek-chat, deepseek/deepseek-r1; "
                   "deepseek: deepseek-chat, deepseek-reasoner.")
@click.pass_context
def config_coach(
    ctx: click.Context,
    benchmark: str,
    max_iterations: int,
    max_candidates: int,
    min_speedup: float,
    dry_run: bool,
    query: tuple,
    model: str,
) -> None:
    """Iterative config + hint tuning via LLM reflection loop.

    Proposes SET LOCAL config changes and pg_hint_plan directives.
    Benchmarks each candidate with interleaved timing, then feeds
    results back to the LLM for up to 3 iterations.

    Complements SQL rewriting — no SQL changes, only config/hints.

    Results are written to config_coach_results.json.
    """
    from ._common import (
        console,
        resolve_benchmark,
        load_benchmark_config,
        parse_query_filter,
        print_header,
        print_error,
        print_success,
    )
    from ..config_coach import coach_benchmark, CoachConfig

    bench_dir = resolve_benchmark(benchmark)
    cfg = load_benchmark_config(bench_dir)
    dsn = cfg.get("dsn", "")

    if not dsn or not dsn.startswith("postgres"):
        print_error("Config coach requires a PostgreSQL benchmark (DSN must start with postgres)")
        raise SystemExit(1)

    tag = " [DRY RUN]" if dry_run else ""
    model_tag = f", model={model}" if model else ""
    print_header(
        f"Config Coach{tag}: {bench_dir.name} "
        f"(iters={max_iterations}, candidates={max_candidates}, "
        f"target={min_speedup}x{model_tag})"
    )

    # Resolve query filter
    query_ids = None
    if query:
        query_ids = parse_query_filter(query, bench_dir)
        if not query_ids:
            print_error("No queries matched the filter")
            raise SystemExit(1)
        console.print(f"  Queries: {', '.join(query_ids)}")

    coach_cfg = CoachConfig(
        max_iterations=max_iterations,
        max_candidates=max_candidates,
        min_speedup=min_speedup,
        dry_run=dry_run,
        model=model,
    )

    # Run coaching
    results = coach_benchmark(
        benchmark_dir=bench_dir,
        dsn=dsn,
        config=coach_cfg,
        query_ids=query_ids,
    )

    # Print results
    console.print()
    for r in results:
        _print_result(console, r, dry_run)

    # Write results file
    output_path = bench_dir / "config_coach_results.json"
    output_data = {
        "benchmark": bench_dir.name,
        "config": {
            "max_iterations": max_iterations,
            "max_candidates": max_candidates,
            "min_speedup": min_speedup,
        },
        "results": [_serialize_result(r) for r in results],
    }
    output_path.write_text(json.dumps(output_data, indent=2, default=str))
    console.print(f"\n  Wrote {output_path}")

    # Summary
    console.print()
    wins = sum(1 for r in results if r.status == "WIN")
    no_gain = sum(1 for r in results if r.status == "NO_GAIN")
    errors = sum(1 for r in results if r.status == "ERROR")
    dry = sum(1 for r in results if r.status == "DRY_RUN")
    total_candidates = sum(r.total_candidates_tested for r in results)
    total_calls = sum(r.total_api_calls for r in results)

    if dry_run:
        print_success(
            f"Dry run complete: {len(results)} queries, "
            f"{total_calls} LLM calls, "
            f"{total_candidates} candidates parsed"
        )
    else:
        print_success(
            f"Coach complete: {wins} wins, {no_gain} no gain, "
            f"{errors} errors | "
            f"{total_candidates} candidates tested, {total_calls} LLM calls"
        )


def _print_result(console, r, dry_run: bool) -> None:
    """Print a single coach result."""
    if r.status == "WIN":
        config_str = _format_config(r)
        console.print(
            f"  {r.query_id}: [green]WIN[/green] {r.best_speedup:.2f}x "
            f"({r.baseline_ms:.0f}ms baseline) | {config_str}"
        )
        if r.best_candidate:
            console.print(
                f"    Hypothesis: {r.best_candidate.get('hypothesis', '')}"
            )
    elif r.status == "NO_GAIN":
        console.print(
            f"  {r.query_id}: [yellow]NO_GAIN[/yellow] "
            f"best={r.best_speedup:.2f}x ({r.baseline_ms:.0f}ms baseline) | "
            f"{r.total_candidates_tested} candidates tested"
        )
    elif r.status == "DRY_RUN":
        n = r.total_candidates_tested
        console.print(
            f"  {r.query_id}: [cyan]DRY_RUN[/cyan] "
            f"{n} candidates parsed, {r.total_api_calls} LLM calls"
        )
    elif r.status == "ERROR":
        console.print(
            f"  {r.query_id}: [red]ERROR[/red] "
            f"({r.total_candidates_tested} candidates tested)"
        )
    else:
        console.print(f"  {r.query_id}: {r.status}")


def _format_config(r) -> str:
    """Format the winning config as a compact string."""
    parts = []
    if r.best_config_commands:
        for cmd in r.best_config_commands:
            # Strip "SET LOCAL " prefix for compact display
            parts.append(cmd.replace("SET LOCAL ", ""))
    if r.best_hint_comment:
        parts.append(r.best_hint_comment)
    return " | ".join(parts) if parts else "none"


def _serialize_result(r) -> dict:
    """Serialize a CoachResult to JSON-safe dict."""
    return {
        "query_id": r.query_id,
        "status": r.status,
        "baseline_ms": round(r.baseline_ms, 1),
        "best_speedup": round(r.best_speedup, 3),
        "best_candidate": r.best_candidate,
        "best_config_commands": r.best_config_commands,
        "best_hint_comment": r.best_hint_comment,
        "total_candidates_tested": r.total_candidates_tested,
        "total_api_calls": r.total_api_calls,
        "duration_seconds": round(r.duration_seconds, 1),
        "iterations": r.iterations,
    }

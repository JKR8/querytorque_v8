"""qt scan — plan-space scanner (PostgreSQL only)."""

from __future__ import annotations

import click


@click.command()
@click.argument("benchmark")
@click.option("-q", "--query", multiple=True, help="Query filter (repeatable, prefix match).")
@click.option("--explore", is_flag=True, help="Run plan-space exploration (ANALYZE first).")
@click.option("--explain-only", is_flag=True, help="Scan using EXPLAIN costs only (fast, ~30s).")
@click.option("--timeout-ms", type=int, default=120_000, show_default=True,
              help="Timeout per query in milliseconds.")
@click.pass_context
def scan(
    ctx: click.Context,
    benchmark: str,
    query: tuple,
    explore: bool,
    explain_only: bool,
    timeout_ms: int,
) -> None:
    """Scan plan space by toggling planner flags (PostgreSQL only).

    Discovers alternative execution plans via SET LOCAL planner flags.
    Results are saved to benchmark/plan_scanner/ and plan_explore/.
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

    bench_dir = resolve_benchmark(benchmark)
    cfg = load_benchmark_config(bench_dir)

    if cfg["engine"] not in ("postgresql", "postgres"):
        print_error("Plan scanner is PostgreSQL only.")
        raise SystemExit(1)

    query_ids = parse_query_filter(query, bench_dir) or None
    print_header(f"Scanning plan space [{bench_dir.name}]")

    from ..plan_scanner import scan_corpus, scan_corpus_explain_only, explore_corpus

    if explain_only:
        console.print("  Mode: EXPLAIN-only (cost-based, no execution)")
        results = scan_corpus_explain_only(bench_dir, query_ids=query_ids)
        console.print(f"  Scanned {len(results)} queries")
    elif explore:
        console.print("  Mode: Plan-space exploration (ANALYZE)")
        results = explore_corpus(bench_dir, query_ids=query_ids)
        console.print(f"  Explored {len(results)} queries")
    else:
        console.print(f"  Mode: Full scan (timeout={timeout_ms}ms)")
        results = scan_corpus(bench_dir, query_ids=query_ids, timeout_ms=timeout_ms)
        console.print(f"  Scanned {len(results)} queries")

    print_success(f"Done. Results saved to {bench_dir.name}/plan_scanner/ and plan_explore/")

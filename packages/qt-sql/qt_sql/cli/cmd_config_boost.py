"""qt config-boost — post-rewrite SET LOCAL config tuning from EXPLAIN."""

from __future__ import annotations

import json

import click


@click.command("config-boost")
@click.argument("benchmark")
@click.option("--min-speedup", default=1.05, show_default=True,
              help="Min rewrite speedup to attempt boost.")
@click.option("--dry-run", is_flag=True,
              help="Show proposed configs without benchmarking.")
@click.option("-q", "--query", multiple=True,
              help="Query filter (repeatable, prefix match).")
@click.option("--use-llm", is_flag=True,
              help="Use LLM for config analysis instead of rules.")
@click.pass_context
def config_boost(
    ctx: click.Context,
    benchmark: str,
    min_speedup: float,
    dry_run: bool,
    query: tuple,
    use_llm: bool,
) -> None:
    """Boost winning rewrites with SET LOCAL config tuning.

    Reads leaderboard.json (if available) or swarm_sessions/ to find
    winning rewrites. Analyzes EXPLAIN ANALYZE plans and proposes
    SET LOCAL config changes (work_mem, jit, parallelism, etc.).
    Benchmarks each proposed config with interleaved 3-variant timing.

    Results are written to config_boost_results.json.
    """
    from ._common import (
        console,
        resolve_benchmark,
        load_benchmark_config,
        print_header,
        print_error,
        print_success,
    )

    bench_dir = resolve_benchmark(benchmark)
    cfg = load_benchmark_config(bench_dir)
    dsn = cfg.get("dsn", "")

    if not dsn or not dsn.startswith("postgres"):
        print_error("Config boost requires a PostgreSQL benchmark (DSN must start with postgres)")
        raise SystemExit(1)

    method_tag = " [LLM]" if use_llm else ""
    tag = " [DRY RUN]" if dry_run else ""
    print_header(f"Config Boost{method_tag}{tag}: {bench_dir.name} (min_speedup={min_speedup}x)")

    # Resolve query filter
    query_ids = None
    if query:
        from ._common import parse_query_filter
        query_ids = parse_query_filter(query, bench_dir)

    # Try leaderboard.json first, fall back to swarm_sessions
    lb_path = bench_dir / "leaderboard.json"
    if lb_path.exists():
        from ..config_boost import boost_from_leaderboard

        console.print(f"  Reading from leaderboard.json")
        results = boost_from_leaderboard(
            benchmark_dir=bench_dir,
            dsn=dsn,
            min_speedup=min_speedup,
            dry_run=dry_run,
            use_llm=use_llm,
            query_ids=query_ids,
        )
        for result in results:
            _print_result(console, result, dry_run)
    else:
        # Legacy path: read from swarm_sessions
        from ..config_boost import boost_session, boost_benchmark

        console.print(f"  No leaderboard.json, reading from swarm_sessions/")
        if query_ids:
            results = []
            sessions_dir = bench_dir / "swarm_sessions"
            for qid in query_ids:
                session_dir = sessions_dir / qid
                if not session_dir.exists():
                    console.print(f"  {qid}: [dim]no session[/dim]")
                    continue
                result = boost_session(session_dir, dsn, min_speedup, dry_run)
                if result:
                    results.append(result)
                    _print_result(console, result, dry_run)
        else:
            results = boost_benchmark(bench_dir, dsn, min_speedup, dry_run, query_ids)
            for result in results:
                _print_result(console, result, dry_run)

    # Write results file
    output_path = bench_dir / "config_boost_results.json"
    output_path.write_text(json.dumps({
        "benchmark": bench_dir.name,
        "config_method": "llm" if use_llm else "rules",
        "results": results,
    }, indent=2, default=str))
    console.print(f"\n  Wrote {output_path}")

    # Summary
    console.print()
    boosted = sum(1 for r in results if r.get("status") == "BOOSTED")
    no_gain = sum(1 for r in results if r.get("status") == "NO_GAIN")
    no_rules = sum(1 for r in results if r.get("status") == "NO_RULES")
    skipped = sum(1 for r in results if r.get("status") == "SKIPPED")
    errors = sum(1 for r in results if r.get("status") in ("ERROR", "BENCHMARK_ERROR"))
    dry = sum(1 for r in results if r.get("status") == "DRY_RUN")

    if dry_run:
        print_success(
            f"Dry run complete: {dry} configs proposed, "
            f"{no_rules} no rules matched, {skipped} skipped"
        )
    else:
        print_success(
            f"Config boost complete: {boosted} boosted, {no_gain} no gain, "
            f"{no_rules} no rules, {skipped} skipped, {errors} errors"
        )


def _print_result(console, result: dict, dry_run: bool) -> None:
    """Print a single config boost result."""
    qid = result.get("query_id", "?")
    status = result.get("status", "?")

    if status == "SKIPPED":
        reason = result.get("reason", "")
        console.print(f"  {qid}: [dim]{reason}[/dim]")
        return

    if status == "NO_RULES":
        console.print(f"  {qid}: [dim]no rules matched[/dim]")
        return

    rules = result.get("rules_fired", [])
    config = result.get("config_proposed", {})
    config_str = ", ".join(f"{k}={v}" for k, v in config.items())

    if status == "DRY_RUN":
        console.print(f"  {qid}: [cyan]PROPOSE[/cyan] {config_str} (rules: {', '.join(rules)})")
        reasons = result.get("reasons", {})
        for param, reason in reasons.items():
            console.print(f"    {param}: {reason}")
        return

    if status == "BOOSTED":
        bench = result.get("benchmark", {})
        additive = bench.get("config_additive", 1.0)
        config_speedup = bench.get("config_speedup", 0)
        rewrite_speedup = bench.get("rewrite_speedup", 0)
        console.print(
            f"  {qid}: [green]BOOSTED[/green] {rewrite_speedup:.2f}x → "
            f"{config_speedup:.2f}x (+{additive:.2f}x) | {config_str}"
        )
        return

    if status == "NO_GAIN":
        bench = result.get("benchmark", {})
        additive = bench.get("config_additive", 1.0)
        console.print(
            f"  {qid}: [yellow]NO_GAIN[/yellow] config additive={additive:.2f}x | {config_str}"
        )
        return

    if status in ("ERROR", "BENCHMARK_ERROR"):
        err = result.get("error", result.get("benchmark_error", "?"))
        console.print(f"  {qid}: [red]{status}[/red] {err}")
        return

    console.print(f"  {qid}: {status}")

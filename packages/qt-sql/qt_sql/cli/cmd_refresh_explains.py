"""qt refresh-explains — regenerate EXPLAIN ANALYZE cache with full JSON plans."""

from __future__ import annotations

import json
import time
from pathlib import Path

import click


@click.command("refresh-explains")
@click.argument("benchmark")
@click.option("-q", "--query", multiple=True, help="Query filter (repeatable, prefix match).")
@click.option("--force", is_flag=True, help="Overwrite even if plan_json is already populated.")
@click.option("--dry-run", is_flag=True, help="Show what would be regenerated without running.")
@click.pass_context
def refresh_explains(
    ctx: click.Context,
    benchmark: str,
    query: tuple,
    force: bool,
    dry_run: bool,
) -> None:
    """Regenerate EXPLAIN ANALYZE cache with full JSON plans.

    Iterates all queries in the benchmark, runs EXPLAIN (ANALYZE, FORMAT JSON)
    via database_utils.run_explain_analyze(), and overwrites the cached explain
    files with proper plan_json (the full analyzed_plan tree).

    Existing plan_text (ASCII art) is preserved since run_explain_analyze captures both.
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
    db_path = cfg.get("db_path") or cfg.get("db_path_or_dsn", "")
    query_ids = parse_query_filter(query, bench_dir)

    if not query_ids:
        print_error("No queries found.")
        raise SystemExit(1)

    explains_dir = bench_dir / "explains"
    explains_dir.mkdir(parents=True, exist_ok=True)

    # Check which need refresh
    to_refresh = []
    already_ok = 0
    for qid in query_ids:
        cache_path = explains_dir / f"{qid}.json"
        if cache_path.exists() and not force:
            try:
                data = json.loads(cache_path.read_text())
                plan_json = data.get("plan_json")
                if plan_json and isinstance(plan_json, dict) and plan_json != {}:
                    already_ok += 1
                    continue
            except Exception:
                pass
        to_refresh.append(qid)

    print_header(
        f"Refresh EXPLAIN cache [{bench_dir.name}]: "
        f"{len(to_refresh)} to refresh, {already_ok} already OK"
    )

    if dry_run:
        for qid in to_refresh:
            console.print(f"  Would refresh: {qid}")
        return

    if not to_refresh:
        print_success("All explain caches already have plan_json. Use --force to regenerate.")
        return

    from ..execution.database_utils import run_explain_analyze

    refreshed = 0
    errors = []
    t0 = time.time()

    for i, qid in enumerate(to_refresh, 1):
        sql_path = bench_dir / "queries" / f"{qid}.sql"
        if not sql_path.exists():
            errors.append((qid, "SQL file not found"))
            continue

        sql = sql_path.read_text().strip()
        # Strip EXPLAIN prefix if present in the cached SQL
        sql_clean = sql
        for prefix in ("EXPLAIN ANALYZE ", "EXPLAIN "):
            if sql_clean.upper().startswith(prefix):
                sql_clean = sql_clean[len(prefix):]

        try:
            result = run_explain_analyze(db_path, sql_clean)
            if result:
                cache_path = explains_dir / f"{qid}.json"

                # Preserve provenance from existing file if any
                provenance = None
                if cache_path.exists():
                    try:
                        old = json.loads(cache_path.read_text())
                        provenance = old.get("provenance")
                    except Exception:
                        pass

                if provenance:
                    result["provenance"] = provenance

                cache_path.write_text(json.dumps(result, indent=2, default=str))

                has_json = bool(result.get("plan_json") and result["plan_json"] != {})
                status = "[green]OK[/green]" if has_json else "[yellow]text-only[/yellow]"
                time_ms = result.get("execution_time_ms", 0) or 0
                console.print(
                    f"  [{i}/{len(to_refresh)}] {qid} {status} "
                    f"{time_ms:.0f}ms"
                )
                if has_json:
                    refreshed += 1
            else:
                errors.append((qid, "run_explain_analyze returned None"))
                console.print(f"  [{i}/{len(to_refresh)}] {qid} [red]FAILED[/red]")

        except Exception as e:
            errors.append((qid, str(e)))
            console.print(f"  [{i}/{len(to_refresh)}] {qid} [red]ERROR[/red] {e}")

    elapsed = time.time() - t0
    console.print()
    print_success(
        f"Refreshed {refreshed}/{len(to_refresh)} explains in {elapsed:.1f}s"
    )
    if errors:
        print_error(f"{len(errors)} errors:")
        for qid, msg in errors[:10]:
            console.print(f"  {qid}: {msg}")

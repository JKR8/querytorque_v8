"""qt run — full optimization pipeline (LLM calls)."""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import click


@click.command()
@click.argument("benchmark")
@click.option("-q", "--query", multiple=True, help="Query filter (repeatable, prefix match).")
@click.option(
    "--mode",
    type=click.Choice(["swarm", "expert", "oneshot"]),
    default="swarm",
    show_default=True,
    help="Optimization mode.",
)
@click.option("--max-iterations", type=int, default=3, show_default=True,
              help="Max optimization rounds per query.")
@click.option("--target-speedup", type=float, default=2.0, show_default=True,
              help="Stop early when this speedup is reached.")
@click.option("--fan-out-only", is_flag=True,
              help="Run fan-out (state 0) only, skip refinement.")
@click.option("--resume", is_flag=True,
              help="Resume from last checkpoint in the run directory.")
@click.option("-o", "--output-dir", type=click.Path(), default=None,
              help="Custom output directory (default: benchmark/runs/<timestamp>).")
@click.option("--concurrency", type=int, default=0,
              help="Parallel LLM generation concurrency (0=serial, N=two-phase with N threads).")
@click.option("--config-boost", "config_boost", is_flag=True,
              help="Run config boost (SET LOCAL tuning) on winners after validation.")
@click.pass_context
def run(
    ctx: click.Context,
    benchmark: str,
    query: tuple,
    mode: str,
    max_iterations: int,
    target_speedup: float,
    fan_out_only: bool,
    resume: bool,
    output_dir: str | None,
    concurrency: int,
    config_boost: bool,
) -> None:
    """Run the full optimization pipeline (requires LLM API key).

    With --concurrency N: Phase 1 generates all candidates in parallel (N threads),
    then Phase 2 validates each query serially (avoids DB contention).
    Without --concurrency (or =0): sequential per-query execution (original behavior).
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
    query_ids = parse_query_filter(query, bench_dir)

    if not query_ids:
        print_error("No queries found.")
        raise SystemExit(1)

    parallel_tag = f" concurrency={concurrency}" if concurrency > 0 else ""
    print_header(f"Running {len(query_ids)} queries [{bench_dir.name}] mode={mode}{parallel_tag}")

    # Output directory
    if output_dir:
        out = Path(output_dir)
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = bench_dir / "runs" / f"run_{ts}"
    out.mkdir(parents=True, exist_ok=True)

    # Checkpoint support
    checkpoint_path = out / "checkpoint.json"
    completed_ids: set[str] = set()
    if resume and checkpoint_path.exists():
        cp = json.loads(checkpoint_path.read_text())
        completed_ids = set(cp.get("completed", []))
        console.print(f"  Resuming: {len(completed_ids)} already completed")

    from ..pipeline import Pipeline
    from ..schemas import OptimizationMode

    mode_map = {
        "swarm": OptimizationMode.SWARM,
        "expert": OptimizationMode.EXPERT,
        "oneshot": OptimizationMode.ONESHOT,
    }

    pipeline = Pipeline(bench_dir)
    iters = 1 if fan_out_only else max_iterations
    n_workers = cfg.get("workers_state_0", 4)

    results = []
    errors = []
    t0 = time.time()

    # Two-phase parallel execution (swarm mode only)
    if concurrency > 0 and mode == "swarm":
        _run_two_phase(
            pipeline=pipeline,
            query_ids=query_ids,
            completed_ids=completed_ids,
            bench_dir=bench_dir,
            out=out,
            checkpoint_path=checkpoint_path,
            max_iterations=iters,
            target_speedup=target_speedup,
            concurrency=concurrency,
            results=results,
            errors=errors,
            console=console,
        )
    else:
        _run_serial(
            pipeline=pipeline,
            query_ids=query_ids,
            completed_ids=completed_ids,
            bench_dir=bench_dir,
            out=out,
            checkpoint_path=checkpoint_path,
            max_iterations=iters,
            target_speedup=target_speedup,
            n_workers=n_workers,
            mode_enum=mode_map[mode],
            results=results,
            errors=errors,
            console=console,
        )

    elapsed = time.time() - t0

    # Phase 3: Config Boost (optional — SET LOCAL tuning on winners)
    if config_boost:
        dsn = cfg.get("dsn", "")
        if dsn and dsn.startswith("postgres"):
            console.print(f"\n{'='*60}")
            console.print(f"  PHASE 3: Config Boost (SET LOCAL tuning on winners)")
            console.print(f"{'='*60}")

            from ..config_boost import boost_benchmark as _boost_benchmark
            boost_results = _boost_benchmark(bench_dir, dsn, min_speedup=1.05, query_ids=query_ids)
            boosted = sum(1 for r in boost_results if r.get("status") == "BOOSTED")
            no_gain = sum(1 for r in boost_results if r.get("status") == "NO_GAIN")
            console.print(
                f"  Config boost: {boosted} boosted, {no_gain} no gain, "
                f"{len(boost_results) - boosted - no_gain} other"
            )
        else:
            console.print("  --config-boost: skipped (requires PostgreSQL DSN)")

    # Summary
    summary = {
        "benchmark": bench_dir.name,
        "mode": mode,
        "concurrency": concurrency,
        "total": len(query_ids),
        "completed": len(results),
        "errors": len(errors),
        "elapsed_seconds": round(elapsed, 1),
        "results": results,
        "error_details": [{"query_id": qid, "error": msg} for qid, msg in errors],
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2))

    console.print()
    print_success(f"Completed {len(results)}/{len(query_ids)} in {elapsed:.1f}s → {out}")
    if errors:
        print_error(f"{len(errors)} errors")
        raise SystemExit(2)


def _run_serial(
    *,
    pipeline,
    query_ids,
    completed_ids,
    bench_dir,
    out,
    checkpoint_path,
    max_iterations,
    target_speedup,
    n_workers,
    mode_enum,
    results,
    errors,
    console,
) -> None:
    """Original serial execution: each query end-to-end."""
    for i, qid in enumerate(query_ids, 1):
        if qid in completed_ids:
            console.print(f"  [{i}/{len(query_ids)}] {qid} [dim]skipped (checkpoint)[/dim]")
            continue

        sql_path = bench_dir / "queries" / f"{qid}.sql"
        if not sql_path.exists():
            errors.append((qid, "SQL file not found"))
            continue

        sql = sql_path.read_text().strip()
        console.print(f"  [{i}/{len(query_ids)}] {qid} ...", end=" ")

        try:
            result = pipeline.run_optimization_session(
                query_id=qid,
                sql=sql,
                max_iterations=max_iterations,
                target_speedup=target_speedup,
                n_workers=n_workers,
                mode=mode_enum,
            )

            _save_query_result(result, qid, out, checkpoint_path, completed_ids, results)
            speedup = getattr(result, "best_speedup", None) or getattr(result, "speedup", None)
            status_str = getattr(result, "status", "?")
            console.print(f"[green]{status_str}[/green] {speedup or '?'}x")

        except Exception as e:
            errors.append((qid, str(e)))
            console.print(f"[red]ERROR[/red] {e}")


def _run_two_phase(
    *,
    pipeline,
    query_ids,
    completed_ids,
    bench_dir,
    out,
    checkpoint_path,
    max_iterations,
    target_speedup,
    concurrency,
    results,
    errors,
    console,
) -> None:
    """Two-phase execution: generate all candidates in parallel, then validate serially."""
    from ..sessions.swarm_session import SwarmSession

    # Build sessions for queries that need work
    sessions: list[tuple[str, SwarmSession]] = []
    error_qids: set[str] = set()

    sessions_dir = bench_dir / "swarm_sessions"
    for qid in query_ids:
        if qid in completed_ids:
            continue
        # Skip queries that already have a completed session.json
        if (sessions_dir / qid / "session.json").exists():
            completed_ids.add(qid)
            console.print(f"  {qid} [dim]already complete (session.json)[/dim]")
            continue
        sql_path = bench_dir / "queries" / f"{qid}.sql"
        if not sql_path.exists():
            errors.append((qid, "SQL file not found"))
            error_qids.add(qid)
            continue

        sql = sql_path.read_text().strip()
        session = SwarmSession(
            pipeline=pipeline,
            query_id=qid,
            original_sql=sql,
            max_iterations=max_iterations,
            target_speedup=target_speedup,
            n_workers=4,
        )
        sessions.append((qid, session))

    if not sessions:
        console.print("  No queries to process.")
        return

    # ── Phase 1: Generate candidates (reload from disk or LLM) ─────────
    # Try to reload existing candidates from disk first (no API cost)
    need_workers_only: list[tuple[str, SwarmSession]] = []
    need_full_generation: list[tuple[str, SwarmSession]] = []
    reloaded = 0

    for qid, session in sessions:
        if session.reload_candidates_from_disk():
            reloaded += 1
        else:
            # Check if analyst response exists on disk (can skip analyst LLM)
            session_dir = bench_dir / "swarm_sessions" / qid / "iteration_00_fan_out"
            if (session_dir / "analyst_response.txt").exists():
                need_workers_only.append((qid, session))
            else:
                need_full_generation.append((qid, session))

    if reloaded:
        console.print(f"  Reloaded {reloaded} sessions from disk (no LLM calls)")

    # Resume sessions that have analyst on disk but need worker generation
    if need_workers_only:
        console.print(
            f"\n{'='*60}\n"
            f"  PHASE 1a: Resume {len(need_workers_only)} queries from saved analyst (workers only, {concurrency} threads)\n"
            f"{'='*60}",
        )
        t_gen = time.time()

        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {}
            for qid, session in need_workers_only:
                futures[pool.submit(session.resume_from_analyst)] = qid

            for future in as_completed(futures):
                qid = futures[future]
                try:
                    ok = future.result()
                    if ok:
                        console.print(f"  [resumed] {qid}")
                        reloaded += 1
                    else:
                        console.print(f"  [resume-fail] {qid} — will regenerate fully")
                        # Find the session and move to full generation
                        for q, s in need_workers_only:
                            if q == qid:
                                need_full_generation.append((q, s))
                                break
                except Exception as e:
                    errors.append((qid, f"Resume failed: {e}"))
                    error_qids.add(qid)
                    console.print(f"  [resume-error] {qid}: {e}")

        console.print(f"\n  Phase 1a complete: {reloaded} resumed ({time.time() - t_gen:.1f}s)")

    # Full generation for sessions with nothing on disk
    if need_full_generation:
        console.print(
            f"\n{'='*60}\n"
            f"  PHASE 1b: Generate candidates ({len(need_full_generation)} queries, {concurrency} threads)\n"
            f"{'='*60}",
        )
        t_gen = time.time()

        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {}
            for qid, session in need_full_generation:
                futures[pool.submit(session.prepare_candidates)] = qid

            for future in as_completed(futures):
                qid = futures[future]
                try:
                    future.result()
                    console.print(f"  [generated] {qid}")
                except Exception as e:
                    errors.append((qid, f"Generation failed: {e}"))
                    error_qids.add(qid)
                    console.print(f"  [gen-error] {qid}: {e}")

        gen_elapsed = time.time() - t_gen
        console.print(f"\n  Phase 1b complete: {len(need_full_generation) - len(error_qids)} generated, "
                      f"{len(error_qids)} errors ({gen_elapsed:.1f}s)")

    if not need_workers_only and not need_full_generation:
        console.print(f"\n  Phase 1 skipped: all {reloaded} sessions reloaded from disk")

    gen_ok = len(sessions) - len(error_qids)

    # Free large prompt/response text from memory (already on disk)
    for _qid, session in sessions:
        if _qid not in error_qids:
            session.compact_gen_result()

    # ── Phase 2: Validate with limited parallelism ─────────────────────
    val_sessions = [(qid, s) for qid, s in sessions if qid not in error_qids]

    if not val_sessions:
        console.print("\n  No successful generations to validate.")
        return

    val_concurrency = min(4, len(val_sessions))
    console.print(
        f"\n{'='*60}\n"
        f"  PHASE 2: Validate & snipe ({len(val_sessions)} queries, {val_concurrency} parallel)\n"
        f"{'='*60}",
    )

    def _validate_one(qid_session):
        qid, session = qid_session
        result = session.validate_and_finish()
        return qid, result

    with ThreadPoolExecutor(max_workers=val_concurrency) as pool:
        futures = {pool.submit(_validate_one, qs): qs[0] for qs in val_sessions}

        for future in as_completed(futures):
            qid = futures[future]
            try:
                qid, result = future.result()
                _save_query_result(result, qid, out, checkpoint_path, completed_ids, results)
                speedup = getattr(result, "best_speedup", None) or getattr(result, "speedup", None)
                status_str = getattr(result, "status", "?")
                console.print(f"  {qid}: [green]{status_str}[/green] {speedup or '?'}x")

            except Exception as e:
                errors.append((qid, f"Validation failed: {e}"))
                console.print(f"  {qid}: [red]ERROR[/red] {e}")


def _save_query_result(result, qid, out, checkpoint_path, completed_ids, results):
    """Save per-query result and update checkpoint."""
    query_out = out / qid
    query_out.mkdir(exist_ok=True)
    (query_out / "result.json").write_text(
        json.dumps(
            result.__dict__ if hasattr(result, "__dict__") else str(result),
            indent=2, default=str,
        )
    )

    speedup = getattr(result, "best_speedup", None) or getattr(result, "speedup", None)
    status_str = getattr(result, "status", "?")
    results.append({"query_id": qid, "status": str(status_str), "speedup": speedup})

    completed_ids.add(qid)
    checkpoint_path.write_text(json.dumps({
        "completed": sorted(completed_ids),
        "last_updated": datetime.now().isoformat(),
    }, indent=2))

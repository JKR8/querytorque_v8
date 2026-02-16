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
    type=click.Choice(["beam", "fleet"]),
    default="beam",
    show_default=True,
    help="Execution mode: beam (analyst/worker loop) or fleet (multi-query orchestration).",
)
@click.option("--max-iterations", type=int, default=3, show_default=True,
              help="Max optimization rounds per query.")
@click.option("--target-speedup", type=float, default=100.0, show_default=True,
              help="Stop early when this speedup is reached.")
@click.option("--single-iteration", is_flag=True,
              help="Run one analyst/worker/snipe round only.")
@click.option("--fan-out-only", "fan_out_only_legacy", is_flag=True, hidden=True,
              help="Deprecated alias for --single-iteration.")
@click.option("--resume", is_flag=True,
              help="Resume from last checkpoint in the run directory.")
@click.option("-o", "--output-dir", type=click.Path(), default=None,
              help="Custom output directory (default: benchmark/runs/<timestamp>).")
@click.option("--concurrency", type=int, default=0,
              help="Parallel query concurrency (0=serial).")
@click.option("--config-boost", "config_boost", is_flag=True,
              help="Run config boost (SET LOCAL tuning) on winners after validation.")
@click.option("--bootstrap", is_flag=True,
              help="Allow first-run mode: skip intelligence gates (no gold examples/global knowledge required).")
@click.option("--scenario", default="",
              help="Scenario card name (e.g., 'postgres_small_instance', 'xsmall_survival').")
@click.option("--engine-version", default="",
              help="Engine version override (e.g., '17' for PG, '1.2' for DuckDB).")
@click.option("--output-contract", is_flag=True,
              help="Emit structured QueryOutputContract JSON alongside results.")
@click.option("--patch", "patch_mode", is_flag=True, hidden=True,
              help="Deprecated. Beam always runs tiered patch flow.")
@click.option("--fleet", "fleet_legacy", is_flag=True, hidden=True,
              help="Deprecated alias for --mode fleet.")
@click.option("--dry-run", "dry_run", is_flag=True,
              help="With --mode fleet: run survey + triage only, no LLM calls.")
@click.option("--live", "live_dashboard", is_flag=True,
              help="With --mode fleet: launch Fleet C2 browser dashboard with live WebSocket updates.")
@click.pass_context
def run(
    ctx: click.Context,
    benchmark: str,
    query: tuple,
    mode: str,
    max_iterations: int,
    target_speedup: float,
    single_iteration: bool,
    fan_out_only_legacy: bool,
    resume: bool,
    output_dir: str | None,
    concurrency: int,
    config_boost: bool,
    bootstrap: bool,
    scenario: str,
    engine_version: str,
    output_contract: bool,
    patch_mode: bool,
    fleet_legacy: bool,
    dry_run: bool,
    live_dashboard: bool,
) -> None:
    """Run the full optimization pipeline (requires LLM API key).

    Beam mode uses the tiered analyst/worker/snipe patch pipeline.
    Fleet mode runs survey/triage/parallel execute/scorecard across many queries.
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

    if fleet_legacy:
        mode = "fleet"

    if dry_run and mode != "fleet":
        print_error("--dry-run is only valid with --mode fleet.")
        raise SystemExit(2)

    # Beam is the canonical tiered analyst/worker/snipe flow.
    patch_mode = True
    single_iteration = single_iteration or fan_out_only_legacy

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

    if bootstrap:
        import os
        os.environ["QT_ALLOW_INTELLIGENCE_BOOTSTRAP"] = "1"

    mode_map = {
        "beam": OptimizationMode.BEAM,
    }

    pipeline = Pipeline(bench_dir)
    iters = 1 if single_iteration else max_iterations
    n_workers = cfg.get("workers_state_0", 4)

    # ── Fleet mode: survey → triage → parallel execute → scorecard ────
    if mode == "fleet":
        from ..fleet.orchestrator import FleetOrchestrator
        from ..fleet.dashboard import FleetDashboard

        # Load queries dict for survey
        queries_dict = {}
        for qid in query_ids:
            sql_path = bench_dir / "queries" / f"{qid}.sql"
            if sql_path.exists():
                queries_dict[qid] = sql_path.read_text().strip()

        dashboard = FleetDashboard()
        orch = FleetOrchestrator(
            pipeline=pipeline,
            benchmark_dir=bench_dir,
            concurrency=concurrency or 4,
            dashboard=dashboard,
        )

        # Phase 0: Survey (collects baselines via EXPLAIN ANALYZE)
        console.print("\n[bold]Phase 0: Survey[/bold] (collecting baselines...)")

        def _survey_progress(qid, done, total):
            if done == 1 or done == total or done % 10 == 0:
                console.print(f"  [{done}/{total}] {qid}", end="\r")

        surveys = orch.survey(query_ids, queries_dict, on_progress=_survey_progress)

        # Report timing sources
        sources = {}
        for sv in surveys.values():
            src = sv.timing_source or "unknown"
            sources[src] = sources.get(src, 0) + 1
        source_str = ", ".join(f"{v} {k}" for k, v in sorted(sources.items()))
        console.print(f"  Surveyed {len(surveys)} queries ({source_str})")

        # Phase 1: Triage
        console.print("\n[bold]Phase 1: Triage[/bold]")
        triaged = orch.triage(surveys, queries_dict)

        # Print triage summary
        buckets = {}
        for t in triaged:
            buckets[t.bucket] = buckets.get(t.bucket, 0) + 1
        console.print(
            f"  {buckets.get('HIGH', 0)} HIGH, "
            f"{buckets.get('MEDIUM', 0)} MEDIUM, "
            f"{buckets.get('LOW', 0)} LOW, "
            f"{buckets.get('SKIP', 0)} SKIP"
        )
        console.print(
            f"  [dim]Skipping {buckets.get('SKIP', 0)} queries < 100ms[/dim]"
        )

        # ── Live Fleet C2 dashboard ──────────────────────────────────
        # Launch before dry-run check so --dry-run --live shows real data
        event_bus = None
        if live_dashboard:
            import threading as _threading
            import webbrowser
            from ..fleet.event_bus import EventBus, EventType, triage_to_fleet_c2
            from ..fleet.ws_server import run_server_in_thread

            fleet_c2_data = triage_to_fleet_c2(triaged)

            event_bus = EventBus()
            triage_gate = _threading.Event()
            pause_event = _threading.Event()
            pause_event.set()  # not paused by default

            # Attach to orchestrator
            orch.event_bus = event_bus
            orch.triage_gate = triage_gate
            orch.pause_event = pause_event

            # Locate HTML file
            html_path = Path(__file__).resolve().parent.parent / "dashboard" / "fleet_c2.html"

            port = 8765
            console.print(f"\n[bold]Fleet C2 Dashboard[/bold]")
            run_server_in_thread(
                event_bus=event_bus,
                triage_gate=triage_gate,
                html_path=html_path,
                initial_data=fleet_c2_data,
                pause_event=pause_event,
                port=port,
                benchmark_dir=bench_dir,
            )
            url = f"http://127.0.0.1:{port}"
            console.print(f"  Dashboard: [link={url}]{url}[/link]")
            try:
                webbrowser.open(url)
            except Exception:
                pass

            if dry_run:
                console.print("  Dashboard serving real triage data (dry-run: no execution).")
                console.print("  Press Ctrl+C to stop.")
                try:
                    _threading.Event().wait()  # block forever until Ctrl+C
                except KeyboardInterrupt:
                    console.print("\n  Stopped.")
                return

            console.print("  Waiting for triage approval in browser...")
            approved = orch.wait_for_triage_approval(timeout=3600)
            if not approved:
                console.print("[yellow]  Triage approval timed out (1h). Aborting.[/yellow]")
                return

            console.print("  [green]Triage approved![/green] Starting execution...")

        if dry_run:
            # Print triage table and exit (no --live)
            console.print("\n[bold]Triage Results (dry-run):[/bold]")
            for t in triaged:
                transforms_str = ""
                if t.survey.matched_transforms:
                    transforms_str = t.survey.matched_transforms[0].id
                console.print(
                    f"  {t.query_id:30s} "
                    f"{t.bucket:8s} "
                    f"{t.survey.runtime_ms:>10.0f}ms "
                    f"iters={t.max_iterations} "
                    f"score={t.priority_score:.1f} "
                    f"tract={t.survey.tractability} "
                    f"{transforms_str}"
                )
            console.print(f"\n[dim]Dry run complete. Use without --dry-run to execute.[/dim]")
            return

        # Phase 2: Execute
        console.print(f"\n[bold]Phase 2: Execute[/bold] (concurrency={concurrency or 4})")
        t0 = time.time()
        dashboard.start()
        try:
            results = orch.execute(triaged, completed_ids, out, checkpoint_path)
        finally:
            dashboard.stop()

        # Phase 3: Compile scorecard
        scorecard_md = orch.compile(results, triaged)
        (out / "fleet_scorecard.md").write_text(scorecard_md)

        # Emit fleet done event
        if event_bus is not None:
            from ..fleet.event_bus import EventType
            event_bus.emit(
                EventType.FLEET_DONE,
                total=len(results),
                elapsed_seconds=round(time.time() - t0, 1),
                results=results,
            )

        # Summary JSON (same format as existing runs)
        elapsed = time.time() - t0
        summary = {
            "benchmark": bench_dir.name,
            "mode": "fleet",
            "concurrency": concurrency or 4,
            "total": len(query_ids),
            "completed": len(results),
            "elapsed_seconds": round(elapsed, 1),
            "results": results,
        }
        (out / "summary.json").write_text(json.dumps(summary, indent=2))

        console.print()
        print_success(
            f"Fleet complete: {len(results)}/{len(query_ids)} "
            f"in {elapsed:.1f}s → {out}"
        )
        console.print(f"  Scorecard: {out / 'fleet_scorecard.md'}")
        return

    # Create orchestrator when scenario or engine-version is provided
    orchestrator = None
    if scenario or engine_version:
        from ..orchestrator import Orchestrator
        engine = cfg.get("engine", "duckdb")
        orchestrator = Orchestrator(
            engine=engine,
            scenario=scenario or None,
            engine_version=engine_version or None,
        )
        console.print(f"  Orchestrator: engine={engine}"
                      f"{f', scenario={scenario}' if scenario else ''}"
                      f"{f', version={engine_version}' if engine_version else ''}")

    results = []
    errors = []
    t0 = time.time()

    # Parallel tiered beam: LLM concurrent, benchmark serialized
    if concurrency > 0 and patch_mode:
        _run_patch_parallel(
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
            orchestrator=orchestrator,
            patch_mode=patch_mode,
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

    # Emit output contracts if requested
    if output_contract:
        from ..contracts import QueryOutputContract
        contracts_dir = out / "contracts"
        contracts_dir.mkdir(exist_ok=True)
        n_contracts = 0
        for r in results:
            qid = r.get("query_id", "unknown")
            result_path = out / qid / "result.json"
            if result_path.exists():
                try:
                    result_data = json.loads(result_path.read_text())
                    from ..schemas import SessionResult
                    sr = SessionResult(**{
                        k: result_data[k] for k in SessionResult.__dataclass_fields__
                        if k in result_data
                    })
                    contract = QueryOutputContract.from_session_result(sr)
                    (contracts_dir / f"{qid}.json").write_text(contract.to_json())
                    n_contracts += 1
                except Exception as e:
                    console.print(f"  contract {qid}: [dim]skipped ({e})[/dim]")
        console.print(f"  Output contracts: {n_contracts} written → {contracts_dir}")

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
    orchestrator=None,
    patch_mode=False,
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
                orchestrator=orchestrator,
                patch=patch_mode,
            )

            _save_query_result(result, qid, out, checkpoint_path, completed_ids, results)
            speedup = getattr(result, "best_speedup", None) or getattr(result, "speedup", None)
            status_str = getattr(result, "status", "?")
            console.print(f"[green]{status_str}[/green] {speedup or '?'}x")

        except Exception as e:
            errors.append((qid, str(e)))
            console.print(f"[red]ERROR[/red] {e}")


def _run_patch_parallel(
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
    """Parallel patch mode: LLM phases concurrent, benchmark serialized via shared lock.

    Each query runs its full tiered pipeline (analyst → workers → semantic retry → snipe)
    concurrently. Only the benchmark phase (Phase 5: 3x timed runs) is serialized via a
    shared threading.Lock to prevent timing pollution from concurrent DB load.
    """
    import threading
    from ..schemas import OptimizationMode

    benchmark_lock = threading.Lock()

    # Build work items
    work_items: list[tuple[str, str]] = []
    for qid in query_ids:
        if qid in completed_ids:
            console.print(f"  {qid} [dim]skipped (checkpoint)[/dim]")
            continue
        sql_path = bench_dir / "queries" / f"{qid}.sql"
        if not sql_path.exists():
            errors.append((qid, "SQL file not found"))
            continue
        sql = sql_path.read_text().strip()
        work_items.append((qid, sql))

    if not work_items:
        console.print("  No queries to process.")
        return

    console.print(
        f"\n{'='*60}\n"
        f"  PATCH PARALLEL: {len(work_items)} queries, "
        f"concurrency={concurrency}, benchmark=serial\n"
        f"{'='*60}"
    )

    t0 = time.time()

    def _run_one(qid_sql):
        qid, sql = qid_sql
        return qid, pipeline.run_optimization_session(
            query_id=qid,
            sql=sql,
            max_iterations=max_iterations,
            target_speedup=target_speedup,
            mode=OptimizationMode.BEAM,
            patch=True,
            benchmark_lock=benchmark_lock,
        )

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(_run_one, item): item[0] for item in work_items}

        for future in as_completed(futures):
            qid = futures[future]
            try:
                qid, result = future.result()
                _save_query_result(result, qid, out, checkpoint_path, completed_ids, results)
                speedup = getattr(result, "best_speedup", None) or getattr(result, "speedup", None)
                status_str = getattr(result, "status", "?")
                elapsed_q = time.time() - t0
                console.print(
                    f"  [{len(results)}/{len(work_items)}] {qid}: "
                    f"[green]{status_str}[/green] {speedup or '?'}x "
                    f"({elapsed_q:.0f}s elapsed)"
                )
            except Exception as e:
                errors.append((qid, str(e)))
                console.print(f"  {qid}: [red]ERROR[/red] {e}")

    elapsed = time.time() - t0
    console.print(
        f"\n  Patch parallel complete: {len(results)}/{len(work_items)} "
        f"in {elapsed:.1f}s"
    )


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

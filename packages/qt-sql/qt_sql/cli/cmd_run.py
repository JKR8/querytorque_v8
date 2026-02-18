"""qt run — full optimization pipeline (LLM calls)."""

from __future__ import annotations

import json
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

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
@click.option("--resume", is_flag=True,
              help="Resume from last checkpoint in the run directory.")
@click.option("-o", "--output-dir", type=click.Path(), default=None,
              help="Custom output directory (default: benchmark/runs/<timestamp>).")
@click.option("--concurrency", type=int, default=None,
              help="Parallel query concurrency (default: config api_call_slots, fallback 0=serial).")
@click.option("--benchmark-concurrency", type=int, default=None,
              help="Max concurrent benchmark lanes (default: config benchmark_slots, fallback 4).")
@click.option(
    "--launch-interval-seconds",
    type=float,
    default=1.0,
    show_default=True,
    help="Stagger query launches in patch-parallel mode to improve provider cache locality.",
)
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
@click.option("--dry-run", "dry_run", is_flag=True,
              help="With --mode fleet: run survey + triage only, no LLM calls.")
@click.option("--live", "live_dashboard", is_flag=True,
              help="With --mode fleet: launch Fleet C2 browser dashboard with live WebSocket updates.")
@click.option(
    "--live-port",
    type=int,
    default=8765,
    show_default=True,
    help="With --mode fleet --live: WebSocket/dashboard port. Set different ports to run multiple engines concurrently.",
)
@click.pass_context
def run(
    ctx: click.Context,
    benchmark: str,
    query: tuple,
    mode: str,
    max_iterations: int,
    target_speedup: float,
    single_iteration: bool,
    resume: bool,
    output_dir: str | None,
    concurrency: int | None,
    benchmark_concurrency: int | None,
    launch_interval_seconds: float,
    config_boost: bool,
    bootstrap: bool,
    scenario: str,
    engine_version: str,
    output_contract: bool,
    dry_run: bool,
    live_dashboard: bool,
    live_port: int,
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

    if dry_run and mode != "fleet":
        print_error("--dry-run is only valid with --mode fleet.")
        raise SystemExit(2)
    if launch_interval_seconds < 0:
        print_error("--launch-interval-seconds must be >= 0.")
        raise SystemExit(2)
    if live_port < 1 or live_port > 65535:
        print_error("--live-port must be between 1 and 65535.")
        raise SystemExit(2)

    # Beam is the canonical tiered analyst/worker/snipe flow.

    bench_dir = resolve_benchmark(benchmark)
    cfg = load_benchmark_config(bench_dir)

    # Resolve runtime slots from config when CLI flags are omitted.
    if concurrency is None:
        concurrency = int(cfg.get("api_call_slots", 0) or 0)
    if benchmark_concurrency is None:
        benchmark_concurrency = int(cfg.get("benchmark_slots", 4) or 4)

    if concurrency < 0:
        print_error("--concurrency must be >= 0.")
        raise SystemExit(2)
    if benchmark_concurrency < 1:
        print_error("--benchmark-concurrency must be >= 1.")
        raise SystemExit(2)

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
        out = bench_dir / "runs" / f"run_{mode}_{ts}"
    out.mkdir(parents=True, exist_ok=True)

    # Checkpoint support
    checkpoint_path = out / "checkpoint.json"
    completed_ids: set[str] = set()
    results: list[dict] = []
    allowed_ids = set(query_ids)
    if resume:
        recovered = _load_existing_results(out, allowed_ids)
        results = list(recovered)
        completed_ids = {r["query_id"] for r in recovered}

        if checkpoint_path.exists():
            cp = json.loads(checkpoint_path.read_text())
            checkpoint_completed = set(cp.get("completed", []))
            completed_ids |= (checkpoint_completed & allowed_ids)

        # Heal checkpoint from discovered artifacts.
        _write_checkpoint(checkpoint_path, completed_ids)
        console.print(
            f"  Resuming: {len(completed_ids)} completed recovered "
            f"from checkpoint/results"
        )

    _write_progress_snapshot(
        out=out,
        total=len(query_ids),
        results=results,
        errors=[],
    )

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

            port = _resolve_available_port(live_port)
            if port != live_port:
                console.print(
                    f"  [yellow]Requested --live-port {live_port} is busy; using {port}.[/yellow]"
                )
            run_context = {
                "run_id": out.name,
                "benchmark_name": bench_dir.name,
                "benchmark_dir": str(bench_dir),
                "engine": str(cfg.get("engine", "") or ""),
                "query_count": len(query_ids),
                "fleet_concurrency": concurrency or 4,
                "benchmark_slots": benchmark_concurrency,
                "beam_edit_mode": str(cfg.get("beam_edit_mode", "tree") or "tree"),
                "wide_max_probes": int(cfg.get("wide_max_probes", 16) or 16),
                "beam_workers": int(cfg.get("beam_workers", cfg.get("beam_qwen_workers", 8)) or 8),
                "compiler_rounds": int(cfg.get("compiler_rounds", cfg.get("snipe_rounds", 2)) or 2),
                "mode_summary": (
                    f"beam:{str(cfg.get('beam_edit_mode', 'tree') or 'tree')}"
                    f" w{int(cfg.get('beam_workers', cfg.get('beam_qwen_workers', 8)) or 8)}"
                    f" p{int(cfg.get('wide_max_probes', 16) or 16)}"
                ),
            }
            console.print(f"\n[bold]Fleet C2 Dashboard[/bold]")
            run_server_in_thread(
                event_bus=event_bus,
                triage_gate=triage_gate,
                html_path=html_path,
                initial_data=fleet_c2_data,
                pause_event=pause_event,
                port=port,
                benchmark_dir=bench_dir,
                run_context=run_context,
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
            "total_beam_cost_usd": 0.0,
            "total_beam_cost_priced_calls": 0,
            "total_beam_cost_unpriced_calls": 0,
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

    errors = []
    t0 = time.time()

    # Parallel tiered beam: LLM concurrent, benchmark with bounded concurrency
    if concurrency > 0:
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
            benchmark_concurrency=benchmark_concurrency,
            launch_interval_seconds=launch_interval_seconds,
            total_queries=len(query_ids),
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
            total_queries=len(query_ids),
            results=results,
            errors=errors,
            console=console,
            orchestrator=orchestrator,
            patch_mode=True,
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
    cost_summary = _summarize_costs(results)
    summary = {
        "benchmark": bench_dir.name,
        "mode": mode,
        "concurrency": concurrency,
        "benchmark_concurrency": benchmark_concurrency,
        "launch_interval_seconds": launch_interval_seconds,
        "total": len(query_ids),
        "completed": len(results),
        "errors": len(errors),
        "elapsed_seconds": round(elapsed, 1),
        "total_beam_cost_usd": cost_summary["total_beam_cost_usd"],
        "total_beam_cost_priced_calls": cost_summary["total_beam_cost_priced_calls"],
        "total_beam_cost_unpriced_calls": cost_summary["total_beam_cost_unpriced_calls"],
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
    console.print(
        f"  Beam cost: ${cost_summary['total_beam_cost_usd']:.6f} "
        f"(priced calls={cost_summary['total_beam_cost_priced_calls']}, "
        f"unpriced calls={cost_summary['total_beam_cost_unpriced_calls']})"
    )
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
    total_queries,
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
            _write_progress_snapshot(
                out=out,
                total=total_queries,
                results=results,
                errors=errors,
            )
            speedup = getattr(result, "best_speedup", None) or getattr(result, "speedup", None)
            status_str = getattr(result, "status", "?")
            beam_cost = getattr(result, "beam_cost_usd", 0.0) or 0.0
            console.print(
                f"[green]{status_str}[/green] {speedup or '?'}x "
                f"(beam_cost=${beam_cost:.6f})"
            )

        except Exception as e:
            errors.append((qid, str(e)))
            _write_progress_snapshot(
                out=out,
                total=total_queries,
                results=results,
                errors=errors,
            )
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
    benchmark_concurrency,
    launch_interval_seconds,
    total_queries,
    results,
    errors,
    console,
) -> None:
    """Parallel patch mode: LLM phases concurrent, benchmark in bounded parallel lanes.

    Each query runs its full tiered pipeline (analyst → workers → semantic retry → snipe)
    concurrently. Benchmark phases (Phase 5: 3x timed runs) share a bounded semaphore
    to improve throughput while limiting cross-query timing interference.
    """
    import threading
    from ..schemas import OptimizationMode

    benchmark_gate = threading.Semaphore(max(1, int(benchmark_concurrency)))
    launch_interval_s = max(0.0, float(launch_interval_seconds))
    launch_lock = threading.Lock()
    next_launch_at = [0.0]

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
        f"concurrency={concurrency}, benchmark_concurrency={max(1, int(benchmark_concurrency))}, "
        f"launch_interval={launch_interval_s:.1f}s\n"
        f"{'='*60}"
    )

    t0 = time.time()

    def _run_one(qid_sql):
        qid, sql = qid_sql
        if launch_interval_s > 0:
            with launch_lock:
                now = time.monotonic()
                wait_s = max(0.0, next_launch_at[0] - now)
                if wait_s > 0:
                    time.sleep(wait_s)
                    now = time.monotonic()
                next_launch_at[0] = now + launch_interval_s
        return qid, pipeline.run_optimization_session(
            query_id=qid,
            sql=sql,
            max_iterations=max_iterations,
            target_speedup=target_speedup,
            mode=OptimizationMode.BEAM,
            patch=True,
            benchmark_lock=benchmark_gate,
        )

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(_run_one, item): item[0] for item in work_items}

        for future in as_completed(futures):
            qid = futures[future]
            try:
                qid, result = future.result()
                _save_query_result(result, qid, out, checkpoint_path, completed_ids, results)
                _write_progress_snapshot(
                    out=out,
                    total=total_queries,
                    results=results,
                    errors=errors,
                )
                speedup = getattr(result, "best_speedup", None) or getattr(result, "speedup", None)
                status_str = getattr(result, "status", "?")
                elapsed_q = time.time() - t0
                scoreboard = _summarize_statuses(results)
                costboard = _summarize_costs(results)
                console.print(
                    f"  [{len(results)}/{total_queries}] {qid}: "
                    f"[green]{status_str}[/green] {speedup or '?'}x "
                    f"({elapsed_q:.0f}s elapsed) "
                    f"wins={scoreboard['WIN']} improved={scoreboard['IMPROVED']} "
                    f"neutral={scoreboard['NEUTRAL']} reg={scoreboard['REGRESSION']} "
                    f"beam_cost=${costboard['total_beam_cost_usd']:.4f}"
                )
            except Exception as e:
                errors.append((qid, str(e)))
                _write_progress_snapshot(
                    out=out,
                    total=total_queries,
                    results=results,
                    errors=errors,
                )
                console.print(f"  {qid}: [red]ERROR[/red] {e}")

    elapsed = time.time() - t0
    console.print(
        f"\n  Patch parallel complete: {len(results)}/{total_queries} "
        f"in {elapsed:.1f}s"
    )


def _save_query_result(result, qid, out, checkpoint_path, completed_ids, results):
    """Save per-query result and update checkpoint."""
    query_out = out / qid
    query_out.mkdir(exist_ok=True)
    _write_json_atomic(
        query_out / "result.json",
        result.__dict__ if hasattr(result, "__dict__") else str(result),
    )

    speedup = getattr(result, "best_speedup", None) or getattr(result, "speedup", None)
    status_str = getattr(result, "status", "?")
    _upsert_result(
        results,
        {
            "query_id": qid,
            "status": str(status_str),
            "speedup": speedup,
            "beam_cost_usd": getattr(result, "beam_cost_usd", 0.0) or 0.0,
            "beam_cost_priced_calls": getattr(result, "beam_cost_priced_calls", 0) or 0,
            "beam_cost_unpriced_calls": getattr(result, "beam_cost_unpriced_calls", 0) or 0,
        },
    )

    completed_ids.add(qid)
    _write_checkpoint(checkpoint_path, completed_ids)


def _write_json_atomic(path: Path, payload) -> None:
    """Write JSON atomically to avoid partial files on interruption."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def _write_checkpoint(checkpoint_path: Path, completed_ids: Iterable[str]) -> None:
    _write_json_atomic(
        checkpoint_path,
        {
            "completed": sorted(set(completed_ids)),
            "last_updated": datetime.now().isoformat(),
        },
    )


def _upsert_result(results: list[dict], new_row: dict) -> None:
    qid = str(new_row.get("query_id", ""))
    for i, row in enumerate(results):
        if row.get("query_id") == qid:
            results[i] = new_row
            return
    results.append(new_row)


def _load_existing_results(out: Path, allowed_ids: set[str]) -> list[dict]:
    loaded: list[dict] = []
    for result_file in sorted(out.glob("*/result.json")):
        qid = result_file.parent.name
        if qid not in allowed_ids:
            continue
        try:
            data = json.loads(result_file.read_text(encoding="utf-8"))
            speedup = data.get("best_speedup", data.get("speedup"))
            status = str(data.get("status", "?"))
            loaded.append(
                {
                    "query_id": qid,
                    "status": status,
                    "speedup": speedup,
                    "beam_cost_usd": float(data.get("beam_cost_usd", 0.0) or 0.0),
                    "beam_cost_priced_calls": int(
                        data.get("beam_cost_priced_calls", 0) or 0
                    ),
                    "beam_cost_unpriced_calls": int(
                        data.get("beam_cost_unpriced_calls", 0) or 0
                    ),
                }
            )
        except Exception:
            continue
    return loaded


def _summarize_statuses(results: list[dict]) -> dict[str, int]:
    summary = {"WIN": 0, "IMPROVED": 0, "NEUTRAL": 0, "REGRESSION": 0, "OTHER": 0}
    for row in results:
        status = str(row.get("status", "OTHER")).upper()
        if status in summary:
            summary[status] += 1
        else:
            summary["OTHER"] += 1
    return summary


def _summarize_costs(results: list[dict]) -> dict[str, Any]:
    total_cost = 0.0
    priced_calls = 0
    unpriced_calls = 0
    for row in results:
        beam_cost = row.get("beam_cost_usd", 0.0)
        if isinstance(beam_cost, (int, float)):
            total_cost += float(beam_cost)
        priced = row.get("beam_cost_priced_calls", 0)
        unpriced = row.get("beam_cost_unpriced_calls", 0)
        if isinstance(priced, (int, float)):
            priced_calls += int(priced)
        if isinstance(unpriced, (int, float)):
            unpriced_calls += int(unpriced)
    return {
        "total_beam_cost_usd": round(total_cost, 8),
        "total_beam_cost_priced_calls": priced_calls,
        "total_beam_cost_unpriced_calls": unpriced_calls,
    }


def _write_progress_snapshot(
    *,
    out: Path,
    total: int,
    results: list[dict],
    errors: list[tuple[str, str]],
) -> None:
    completed_ids = {str(r.get("query_id", "")) for r in results if r.get("query_id")}
    error_ids = {str(e[0]) for e in errors if e and e[0]}
    summary = _summarize_statuses(results)
    cost_summary = _summarize_costs(results)
    winners = []
    for r in results:
        speedup = r.get("speedup")
        if isinstance(speedup, (int, float)) and speedup > 1.0:
            winners.append(
                {
                    "query_id": r.get("query_id"),
                    "status": r.get("status"),
                    "speedup": speedup,
                    "beam_cost_usd": r.get("beam_cost_usd", 0.0),
                }
            )
    winners.sort(key=lambda x: x["speedup"], reverse=True)
    payload = {
        "updated_at": datetime.now().isoformat(),
        "total": total,
        "completed": len(completed_ids),
        "failed": len(error_ids),
        "remaining": max(0, total - len(completed_ids) - len(error_ids)),
        "status_counts": summary,
        "cost": cost_summary,
        "winners_so_far": winners[:20],
        "errors": [{"query_id": qid, "error": msg} for qid, msg in errors[-100:]],
    }
    _write_json_atomic(out / "progress.json", payload)


def _resolve_available_port(preferred_port: int, host: str = "127.0.0.1") -> int:
    """Use preferred port when free; otherwise pick the next available."""
    for port in range(preferred_port, min(65536, preferred_port + 100)):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, port))
                return port
            except OSError:
                continue
    raise RuntimeError(
        f"No available live dashboard port in range {preferred_port}-{min(65535, preferred_port + 99)}."
    )

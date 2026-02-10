"""qt run — full optimization pipeline (LLM calls)."""

from __future__ import annotations

import json
import time
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
) -> None:
    """Run the full optimization pipeline (requires LLM API key).

    Wraps Pipeline.run_optimization_session() per query.
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

    print_header(f"Running {len(query_ids)} queries [{bench_dir.name}] mode={mode}")

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

    for i, qid in enumerate(query_ids, 1):
        if qid in completed_ids:
            console.print(f"  [{i}/{len(query_ids)}] {qid} [dim]skipped (checkpoint)[/dim]")
            continue

        sql_path = bench_dir / "queries" / f"{qid}.sql"
        if not sql_path.exists():
            errors.append((qid, f"SQL file not found"))
            continue

        sql = sql_path.read_text().strip()
        console.print(f"  [{i}/{len(query_ids)}] {qid} ...", end=" ")

        try:
            result = pipeline.run_optimization_session(
                query_id=qid,
                sql=sql,
                max_iterations=iters,
                target_speedup=target_speedup,
                n_workers=n_workers,
                mode=mode_map[mode],
            )

            # Save per-query result
            query_out = out / qid
            query_out.mkdir(exist_ok=True)
            (query_out / "result.json").write_text(
                json.dumps(result.__dict__ if hasattr(result, "__dict__") else str(result), indent=2, default=str)
            )

            speedup = getattr(result, "best_speedup", None) or getattr(result, "speedup", None)
            status_str = getattr(result, "status", "?")
            console.print(f"[green]{status_str}[/green] {speedup or '?'}x")

            results.append({"query_id": qid, "status": str(status_str), "speedup": speedup})

            # Update checkpoint
            completed_ids.add(qid)
            checkpoint_path.write_text(json.dumps({
                "completed": sorted(completed_ids),
                "last_updated": datetime.now().isoformat(),
            }, indent=2))

        except Exception as e:
            errors.append((qid, str(e)))
            console.print(f"[red]ERROR[/red] {e}")

    elapsed = time.time() - t0

    # Summary
    summary = {
        "benchmark": bench_dir.name,
        "mode": mode,
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

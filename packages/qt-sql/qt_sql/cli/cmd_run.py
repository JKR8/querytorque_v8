"""qt run — full optimization pipeline (LLM calls)."""

from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import click


@click.command()
@click.argument("benchmark")
@click.option("-q", "--query", multiple=True, help="Query filter (repeatable, prefix match).")
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
@click.option("--benchmark-slots", type=int, default=None,
              help="Max concurrent cloud compute connections (default: config benchmark_slots, fallback 8).")
@click.option("--api-slots", type=int, default=32, show_default=True,
              help="Max concurrent LLM API calls.")
@click.option("--config-boost", "config_boost", is_flag=True,
              help="Run config boost (SET LOCAL tuning) on winners after validation.")
@click.option("--bootstrap", is_flag=True,
              help="Allow first-run mode: skip intelligence gates (no gold examples/global knowledge required).")
@click.option("--output-contract", is_flag=True,
              help="Emit structured QueryOutputContract JSON alongside results.")
@click.option("--api-only", "api_only", is_flag=True,
              help="Run API waves only (1,3), skip all benchmarking.")
@click.pass_context
def run(
    ctx: click.Context,
    benchmark: str,
    query: tuple,
    max_iterations: int,
    target_speedup: float,
    single_iteration: bool,
    resume: bool,
    output_dir: str | None,
    benchmark_slots: int | None,
    api_slots: int,
    config_boost: bool,
    bootstrap: bool,
    output_contract: bool,
    api_only: bool,
) -> None:
    """Run the full optimization pipeline (requires LLM API key).

    Uses the phased wave architecture: API waves (analyst/workers/compiler)
    followed by benchmark waves (correctness + timing).
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

    # Resolve runtime slots from config when CLI flags are omitted.
    if benchmark_slots is None:
        benchmark_slots = int(cfg.get("benchmark_slots", 8) or 8)

    if benchmark_slots < 1:
        print_error("--benchmark-slots must be >= 1.")
        raise SystemExit(2)

    query_ids = parse_query_filter(query, bench_dir)

    if not query_ids:
        print_error("No queries found.")
        raise SystemExit(1)

    print_header(
        f"Running {len(query_ids)} queries [{bench_dir.name}] "
        f"api_slots={api_slots} benchmark_slots={benchmark_slots}"
    )

    # Output directory
    if output_dir:
        out = Path(output_dir)
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = bench_dir / "runs" / f"run_wave_{ts}"
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

    if bootstrap:
        import os
        os.environ["QT_ALLOW_INTELLIGENCE_BOOTSTRAP"] = "1"

    pipeline = Pipeline(bench_dir)

    # ── Wave mode: phased beam pipeline (API waves then DB waves) ───────
    from ..sessions.wave_runner import WaveRunner

    # Filter out already-completed queries
    wave_query_ids = [qid for qid in query_ids if qid not in completed_ids]
    if not wave_query_ids:
        console.print("  All queries already complete.")
        return

    print_header(
        f"WAVE MODE: {len(wave_query_ids)} queries, "
        f"api_slots={api_slots}, db_slots={benchmark_slots}"
    )

    runner = WaveRunner(
        pipeline=pipeline,
        bench_dir=bench_dir,
        api_slots=api_slots,
        db_slots=benchmark_slots,
        resume=resume,
        api_only=api_only,
    )

    t0 = time.time()
    results = runner.run(wave_query_ids, out, console)
    elapsed = time.time() - t0

    # Phase 3: Config Boost (optional — SET LOCAL tuning on winners)
    if config_boost:
        dsn = cfg.get("dsn", "")
        if dsn and dsn.startswith("postgres"):
            console.print(f"\n{'='*60}")
            console.print(f"  CONFIG BOOST: SET LOCAL tuning on winners")
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
        "mode": "wave",
        "api_slots": api_slots,
        "benchmark_slots": benchmark_slots,
        "total": len(query_ids),
        "completed": len(results),
        "elapsed_seconds": round(elapsed, 1),
        "total_beam_cost_usd": cost_summary["total_beam_cost_usd"],
        "total_beam_cost_priced_calls": cost_summary["total_beam_cost_priced_calls"],
        "total_beam_cost_unpriced_calls": cost_summary["total_beam_cost_unpriced_calls"],
        "results": results,
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
    print_success(
        f"Wave complete: {len(results)}/{len(query_ids)} "
        f"in {elapsed:.1f}s → {out}"
    )
    console.print(
        f"  Beam cost: ${cost_summary['total_beam_cost_usd']:.6f} "
        f"(priced calls={cost_summary['total_beam_cost_priced_calls']}, "
        f"unpriced calls={cost_summary['total_beam_cost_unpriced_calls']})"
    )


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

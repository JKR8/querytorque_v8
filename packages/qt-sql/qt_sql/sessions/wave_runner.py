"""Wave mode: phased beam pipeline separating LLM API calls from DB benchmarks.

4-wave pipeline:
  Wave 1 — API Collection:      analyst + worker LLM calls + sqlglot parse + retry (api_slots)
  Wave 2 — Probe Benchmark:     correctness on first run + 3x timing (db_slots)
  Wave 3 — Compiler API:        compiler LLM calls + sqlglot parse + retry (api_slots)
  Wave 4 — Compiler Benchmark:  correctness on first run + 3x timing (db_slots)
  Finalize — write iter0_result.txt per query
"""
from __future__ import annotations

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..pipeline import Pipeline
    from .beam_session import AppliedPatch, BeamSession
    from ._beam_phases import BeamContext

logger = logging.getLogger(__name__)


@dataclass
class QueryState:
    """Per-query state that persists across all 4 waves."""

    query_id: str
    sql: str
    session: Optional[Any] = None  # BeamSession
    ctx: Optional[Any] = None      # BeamContext
    scout_result: Optional[Any] = None
    analyst_prompt: str = ""
    analyst_response: str = ""
    patches: List[Any] = field(default_factory=list)      # AppliedPatch
    compiler_patches: List[Any] = field(default_factory=list)
    total_api_calls: int = 0
    completed: bool = False
    error: Optional[str] = None


class WaveRunner:
    """Phased beam pipeline that separates LLM calls from DB benchmarks."""

    def __init__(
        self,
        pipeline: "Pipeline",
        bench_dir: Path,
        api_slots: int = 32,
        db_slots: int = 8,
        resume: bool = False,
        api_only: bool = False,
    ) -> None:
        self.pipeline = pipeline
        self.bench_dir = bench_dir
        self.api_slots = max(1, api_slots)
        self.db_slots = max(1, db_slots)
        self.resume = resume
        self.api_only = api_only
        self.states: Dict[str, QueryState] = {}

    def run(self, query_ids: List[str], out: Path, console: Any) -> List[dict]:
        """Execute full 4-wave pipeline. Returns list of result dicts."""
        t0 = time.time()

        # Prepare all sessions
        console.print(f"\n  Preparing {len(query_ids)} sessions...")
        self._prepare_all(query_ids, out, console)

        active = [s for s in self.states.values() if not s.completed]
        if not active:
            console.print("  All queries already complete.")
            return self._finalize_all(out, console)

        # Wave 1: API Collection (analyst + workers + sqlglot parse + retry)
        self._print_wave_header(console, 1, "API Collection",
                                f"{len(active)} queries, api_slots={self.api_slots}")
        self._wave1_api_collection(console)
        self._save_wave_checkpoint(out, 1)

        # Wave 2: Probe Benchmark
        if not self.api_only:
            self._print_wave_header(console, 2, "Probe Benchmark",
                                    f"db_slots={self.db_slots}")
            self._run_benchmark_wave(console, probe=True, wave_num=2)
            self._save_wave_checkpoint(out, 2)

        # Wave 3: Compiler API (+ sqlglot parse + retry)
        compiler_count = sum(1 for s in self.states.values()
                             if not s.completed and s.scout_result)
        self._print_wave_header(console, 3, "Compiler API",
                                f"{compiler_count} queries")
        self._wave3_compiler_api(console)
        self._save_wave_checkpoint(out, 3)

        # Wave 4: Compiler Benchmark
        if not self.api_only:
            self._print_wave_header(console, 4, "Compiler Benchmark",
                                    f"db_slots={self.db_slots}")
            self._run_benchmark_wave(console, compiler=True, wave_num=4)
            self._save_wave_checkpoint(out, 4)

        elapsed = time.time() - t0
        if self.api_only:
            console.print(f"\n  API-only waves (1,3) complete in {elapsed:.1f}s — no benchmarking")
        else:
            console.print(f"\n  All 4 waves complete in {elapsed:.1f}s")

        return self._finalize_all(out, console)

    # ── Preparation ───────────────────────────────────────────────────────

    def _prepare_all(self, query_ids: List[str], out: Path, console: Any) -> None:
        """Create BeamSession + BeamContext for each query."""
        from .beam_session import BeamSession

        for qid in query_ids:
            # Skip if already has result in THIS run's output dir (resume only)
            if self.resume and self._find_existing_result(qid, out):
                self.states[qid] = QueryState(query_id=qid, sql="", completed=True)
                continue

            sql_path = self.bench_dir / "queries" / f"{qid}.sql"
            if not sql_path.exists():
                self.states[qid] = QueryState(
                    query_id=qid, sql="", completed=True,
                    error="SQL file not found",
                )
                continue

            sql = sql_path.read_text().strip()

            # Find resumable session dir
            resume_dir = None
            if self.resume:
                resume_dir = BeamSession.find_resumable_session_dir(
                    self.bench_dir, qid
                )

            if not self.pipeline.config.benchmark_dsn:
                self.pipeline.config.benchmark_dsn = self.pipeline.config.db_path_or_dsn

            session = BeamSession(
                pipeline=self.pipeline,
                query_id=qid,
                original_sql=sql,
                target_speedup=getattr(self.pipeline.config, "target_speedup", 10.0),
                max_iterations=1,
                patch=True,
                resume_dir=resume_dir,
            )

            try:
                ctx = session.prepare_context(resume_session_dir=resume_dir)
            except Exception as e:
                logger.warning(f"[{qid}] prepare_context failed: {e}")
                self.states[qid] = QueryState(
                    query_id=qid, sql=sql, completed=True,
                    error=f"Preparation failed: {e}",
                )
                continue

            self.states[qid] = QueryState(
                query_id=qid, sql=sql, session=session, ctx=ctx,
            )

        prepared = sum(1 for s in self.states.values() if s.session and not s.completed)
        skipped = sum(1 for s in self.states.values() if s.completed)
        console.print(f"  {prepared} prepared, {skipped} skipped/complete")

    def _find_existing_result(self, qid: str, out: Path) -> bool:
        """Check if query already has a result in the current run output dir."""
        return (out / qid / "result.json").exists()

    # ── Wave 1: API Collection ────────────────────────────────────────────

    def _wave1_api_collection(self, console: Any) -> None:
        """Analyst + worker LLM calls, sqlglot parse check, inline retry."""
        active = {qid: s for qid, s in self.states.items()
                  if not s.completed and s.session}
        if not active:
            return

        done = [0]
        total = len(active)
        api_lock = threading.Lock()

        def _run_api_one(qid: str, state: QueryState) -> None:
            try:
                session = state.session
                ctx = state.ctx

                # Phase 1: Analyst
                scout_result, analyst_response, analyst_prompt, api_calls = \
                    session.run_analyst_phase(ctx)
                state.scout_result = scout_result
                state.analyst_response = analyst_response
                state.analyst_prompt = analyst_prompt
                state.total_api_calls += api_calls

                if not scout_result or not scout_result.probes:
                    state.error = "Analyst returned no probes"
                    state.completed = True
                    return

                # Phase 2: Workers
                patches, worker_api_calls = session.run_workers_phase(ctx, scout_result)
                state.patches = patches
                state.total_api_calls += worker_api_calls

                # Gate 1: sqlglot parse check
                applied = session.dedup_patches(patches)
                session.check_sqlglot_parse(applied)

                # Inline retry for parse failures
                retry_calls = session.run_retry_phase(
                    ctx, patches, api_only=self.api_only,
                )
                state.total_api_calls += retry_calls

                # Re-dedup after retry
                post_retry = [p for p in patches
                              if p.output_sql and str(p.status or "").strip().upper() != "DEDUP"]
                seen: Dict[str, str] = {}
                for p in post_retry:
                    norm = " ".join((p.output_sql or "").split())
                    if norm in seen:
                        p.status = "DEDUP"
                    else:
                        seen[norm] = p.patch_id

                valid_count = sum(1 for p in applied if p.semantic_passed)
                with api_lock:
                    done[0] += 1
                    console.print(
                        f"  [{done[0]}/{total}] {qid}: {len(patches)} probes, "
                        f"{valid_count}/{len(applied)} valid SQL"
                    )
            except Exception as e:
                state.error = str(e)
                state.completed = True
                with api_lock:
                    done[0] += 1
                    console.print(f"  [{done[0]}/{total}] {qid}: [red]ERROR[/red] {e}")

        with ThreadPoolExecutor(max_workers=self.api_slots) as pool:
            futures = {
                pool.submit(_run_api_one, qid, state): qid
                for qid, state in active.items()
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    qid = futures[future]
                    logger.warning(f"Wave 1 unhandled error for {qid}: {e}")

        total_probes = sum(len(s.patches) for s in active.values())
        valid_probes = sum(
            sum(1 for p in s.patches if p.semantic_passed)
            for s in active.values()
        )
        pct = (valid_probes / total_probes * 100) if total_probes > 0 else 0
        console.print(
            f"  Wave 1 complete: {total} queries, {total_probes} probes, "
            f"{valid_probes} structurally valid ({pct:.1f}%)"
        )

    # ── Wave 3: Compiler API ────────────────────────────────────────────

    def _wave3_compiler_api(self, console: Any) -> None:
        """Run compiler LLM calls using all probe results, with inline parse + retry."""
        active = {qid: s for qid, s in self.states.items()
                  if not s.completed and s.session and s.scout_result}

        done = [0]
        total = len(active)
        compiler_lock = threading.Lock()

        def _run_compiler_one(qid: str, state: QueryState) -> None:
            try:
                # In api_only mode, limit compiler to 1 shot (shot 2 needs
                # benchmark data from shot 1 which we don't have yet).
                if self.api_only:
                    orig_rounds = getattr(state.session.pipeline.config, "compiler_rounds", None)
                    if orig_rounds is None:
                        orig_rounds = getattr(state.session.pipeline.config, "snipe_rounds", 2)
                    state.session.pipeline.config.compiler_rounds = 1

                compiler_patches, api_calls = state.session.run_compiler_phase(
                    state.ctx, state.patches, state.scout_result,
                )

                if self.api_only and orig_rounds is not None:
                    state.session.pipeline.config.compiler_rounds = orig_rounds

                state.compiler_patches = compiler_patches
                state.total_api_calls += api_calls

                # Gate 1: sqlglot parse check for compiler patches
                if compiler_patches:
                    state.session.check_sqlglot_parse(compiler_patches)

                valid = sum(1 for p in compiler_patches if p.semantic_passed)
                with compiler_lock:
                    done[0] += 1
                    console.print(
                        f"  [{done[0]}/{total}] {qid}: "
                        f"{len(compiler_patches)} compiler patches, {valid} valid"
                    )
            except Exception as e:
                with compiler_lock:
                    done[0] += 1
                logger.warning(f"[{qid}] Compiler phase failed: {e}")
                console.print(f"  [{done[0]}/{total}] {qid}: [red]compiler error[/red] {e}")

        with ThreadPoolExecutor(max_workers=self.api_slots) as pool:
            futures = {
                pool.submit(_run_compiler_one, qid, state): qid
                for qid, state in active.items()
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    qid = futures[future]
                    logger.warning(f"Wave 3 unhandled error for {qid}: {e}")

        total_comp = sum(len(s.compiler_patches) for s in active.values())
        console.print(f"  Wave 3 complete: {total_comp} compiler patches across {total} queries")

    # ── Shared Benchmark Wave ────────────────────────────────────────────

    def _run_benchmark_wave(
        self,
        console: Any,
        probe: bool = False,
        compiler: bool = False,
        wave_num: int = 0,
    ) -> None:
        """Benchmark patches across all queries using db_slots connections.

        Concurrency model: ThreadPoolExecutor(max_workers=db_slots) limits
        concurrent queries. Each query opens exactly ONE connection inside
        benchmark_query_patches(). Max open connections = db_slots.
        """
        # Group patches by query for per-query benchmarking
        active = {}
        for qid, state in self.states.items():
            if state.completed or not state.session:
                continue
            patches_to_bench = []
            if probe:
                for p in state.patches:
                    if not p.output_sql or not p.semantic_passed:
                        continue
                    if p.speedup is not None:
                        continue  # already benchmarked
                    patches_to_bench.append(p)
            if compiler:
                for p in state.compiler_patches:
                    if not p.output_sql or not p.semantic_passed:
                        continue
                    if p.speedup is not None:
                        continue
                    patches_to_bench.append(p)
            if patches_to_bench:
                active[qid] = (state, patches_to_bench)

        if not active:
            console.print(f"  Wave {wave_num}: no patches to benchmark")
            return

        total_patches = sum(len(patches) for _, patches in active.values())
        done = [0]
        wins = [0]
        improved = [0]
        neutral = [0]
        regression = [0]
        counter_lock = threading.Lock()

        def _benchmark_query(qid: str, state: QueryState, patches: list) -> None:
            try:
                shot = 0 if probe else 1
                state.session.run_benchmark_patches(
                    patches, state.ctx.db_path,
                    session_dir=state.ctx.session_dir, shot=shot,
                )

                with counter_lock:
                    for p in patches:
                        done[0] += 1
                        status = str(p.status or "?").upper()
                        speedup = p.speedup
                        if status == "WIN":
                            wins[0] += 1
                        elif status == "IMPROVED":
                            improved[0] += 1
                        elif status == "NEUTRAL":
                            neutral[0] += 1
                        elif status == "REGRESSION":
                            regression[0] += 1

                        speedup_str = f"{speedup:.2f}x" if speedup else "?"
                        orig_str = f"{p.original_ms:.0f}ms" if p.original_ms else "?"
                        patch_str = f"{p.patch_ms:.0f}ms" if p.patch_ms else "?"
                        console.print(
                            f"  [{done[0]}/{total_patches}] {qid}/{p.patch_id}: "
                            f"{orig_str}->{patch_str} {speedup_str} {status}"
                        )
            except Exception as e:
                logger.warning(f"[{qid}] Benchmark failed: {e}")
                with counter_lock:
                    for p in patches:
                        done[0] += 1
                        p.status = "ERROR"
                        p.apply_error = str(e)

        # Run benchmarks with db_slots concurrency at the query level
        max_concurrent = min(self.db_slots, len(active))
        with ThreadPoolExecutor(max_workers=max_concurrent) as pool:
            futures = {
                pool.submit(_benchmark_query, qid, state, patches): qid
                for qid, (state, patches) in active.items()
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    qid = futures[future]
                    logger.warning(f"Wave {wave_num} unhandled error for {qid}: {e}")
                    # Mark all patches for this query as failed
                    state, patches = active[qid]
                    with counter_lock:
                        for p in patches:
                            if not p.status or p.status == "?":
                                p.status = "ERROR"
                                p.apply_error = f"Unhandled: {e}"

        console.print(
            f"  Wave {wave_num} complete: {wins[0]} WIN, {improved[0]} IMPROVED, "
            f"{neutral[0]} NEUTRAL, {regression[0]} REGRESSION"
        )

    # ── Finalize ─────────────────────────────────────────────────────────

    def _finalize_all(self, out: Path, console: Any) -> List[dict]:
        """Write results and return summary list."""
        from ..schemas import SessionResult

        results = []
        for qid, state in sorted(self.states.items()):
            if state.error and not state.session:
                results.append({
                    "query_id": qid,
                    "status": "ERROR",
                    "speedup": None,
                    "error": state.error,
                    "beam_cost_usd": 0.0,
                    "beam_cost_priced_calls": 0,
                    "beam_cost_unpriced_calls": 0,
                })
                continue

            if state.completed and not state.session:
                # Already had result from prior run
                results.append({
                    "query_id": qid,
                    "status": "SKIP",
                    "speedup": None,
                    "beam_cost_usd": 0.0,
                    "beam_cost_priced_calls": 0,
                    "beam_cost_unpriced_calls": 0,
                })
                continue

            try:
                session = state.session
                ctx = state.ctx
                result = session.finalize_result(
                    ctx=ctx,
                    patches=state.patches,
                    compiler_patches=state.compiler_patches,
                    analyst_prompt=state.analyst_prompt,
                    analyst_response=state.analyst_response,
                    total_api_calls=state.total_api_calls,
                )

                speedup = getattr(result, "best_speedup", None)
                status = getattr(result, "status", "?")
                cost_fields = session._session_cost_fields()

                results.append({
                    "query_id": qid,
                    "status": str(status),
                    "speedup": speedup,
                    "beam_cost_usd": cost_fields.get("beam_cost_usd", 0.0),
                    "beam_cost_priced_calls": cost_fields.get("beam_cost_priced_calls", 0),
                    "beam_cost_unpriced_calls": cost_fields.get("beam_cost_unpriced_calls", 0),
                })

                # Write result to output dir
                query_out = out / qid
                query_out.mkdir(exist_ok=True)
                result_data = result.__dict__ if hasattr(result, "__dict__") else str(result)
                _write_json_atomic(query_out / "result.json", result_data)

            except Exception as e:
                logger.warning(f"[{qid}] Finalize failed: {e}")
                results.append({
                    "query_id": qid,
                    "status": "ERROR",
                    "speedup": None,
                    "error": str(e),
                    "beam_cost_usd": 0.0,
                    "beam_cost_priced_calls": 0,
                    "beam_cost_unpriced_calls": 0,
                })

        # Print summary
        status_counts: Dict[str, int] = {}
        for r in results:
            s = str(r.get("status", "OTHER")).upper()
            status_counts[s] = status_counts.get(s, 0) + 1

        console.print(
            f"\n  Results: " + ", ".join(
                f"{count} {status}" for status, count in sorted(status_counts.items())
            )
        )

        return results

    # ── Checkpoint ───────────────────────────────────────────────────────

    def _save_wave_checkpoint(self, out: Path, wave_num: int) -> None:
        """Save wave progress to disk."""
        payload = {
            "completed_wave": wave_num,
            "timestamp": datetime.now().isoformat(),
            "queries": {
                qid: {
                    "completed": s.completed,
                    "error": s.error,
                    "total_api_calls": s.total_api_calls,
                    "n_patches": len(s.patches),
                    "n_compiler_patches": len(s.compiler_patches),
                }
                for qid, s in self.states.items()
            },
        }
        _write_json_atomic(out / "wave_checkpoint.json", payload)

    # ── Helpers ──────────────────────────────────────────────────────────

    def _print_wave_header(self, console: Any, wave_num: int, name: str, detail: str) -> None:
        console.print(f"\n{'=' * 59}")
        console.print(f"  WAVE {wave_num}/4: {name} ({detail})")
        console.print(f"{'=' * 59}")


def _write_json_atomic(path: Path, payload) -> None:
    """Write JSON atomically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)

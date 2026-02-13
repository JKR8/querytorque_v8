"""Swarm optimization session — multi-worker fan-out with self-directed retry.

Workflow:
1. Fan-out: Analyst distributes top 20 matched examples across 4 workers
   (3 each, no duplicates). Each worker gets a different strategy.
2. Validate all 4 candidates. If any >= target_speedup, done.
3. Retry: Self-directed worker gets ALL raw evidence (previous results,
   EXPLAIN plans, race timings) and diagnoses + rewrites in one LLM call.
4. Iterate retry (not fan-out) up to max_iterations.

Fan-out only happens ONCE (iteration 1). Subsequent iterations are retries.
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from .base_session import OptimizationSession
from ..session_logging import attach_session_handler, detach_session_handler
from ..schemas import SessionResult, WorkerResult

if TYPE_CHECKING:
    from ..pipeline import Pipeline

logger = logging.getLogger(__name__)


def _fmt_elapsed(seconds: float) -> str:
    """Format elapsed seconds as human-readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    return f"{m}m{s:02d}s"


class SwarmSession(OptimizationSession):
    """Multi-worker fan-out with snipe refinement."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.all_worker_results: List[WorkerResult] = []
        self.iterations_data: List[Dict[str, Any]] = []
        self._run_log_path: Optional[Path] = None
        self._run_log_handler: Optional[logging.Handler] = None
        self._cached_baseline: Optional[Any] = None  # OriginalBaseline from first validation
        # Snipe state — persisted from fan-out for snipe reuse
        self._explain_plan_text: Optional[str] = None
        self._engine_profile: Optional[Dict] = None
        self._constraints: List[Dict] = []
        self._semantic_intents: Optional[Dict] = None
        self._output_columns: List[str] = []
        self._matched_examples: List[Dict] = []
        self._regression_warnings: Optional[List[Dict]] = None
        self._sniper_result: Optional[WorkerResult] = None  # for retry
        self._candidate_explains: Dict[int, str] = {}  # worker_id → explain_text
        self._race_result: Optional[Any] = None  # RaceResult from fan-out race
        self._db_reachable: Optional[bool] = None  # DB connectivity (set once, reused)
        # Two-phase state (set by prepare_candidates, consumed by validate_and_finish)
        self._gen_result: Optional[Dict[str, Any]] = None
        self._dag: Optional[Any] = None
        self._costs: Optional[Dict[str, Any]] = None
        self._prepare_t_session: float = 0.0
        self._generation_done: bool = False

    @staticmethod
    def _stage(query_id: str, msg: str):
        """Print a clear stage message to console."""
        print(f"  [{query_id}] {msg}", flush=True)
        logger.info(f"[{query_id}] {msg}")

    def _setup_run_logging(self, session_dir: Path) -> Path:
        """Attach a per-run file handler so session logs are persisted."""
        session_dir.mkdir(parents=True, exist_ok=True)
        log_path = session_dir / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.log"
        handler = logging.FileHandler(log_path, mode="w")
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(name)s %(levelname)s %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        attach_session_handler(handler)
        self._run_log_handler = handler
        self._run_log_path = log_path
        logger.info(f"[{self.query_id}] Swarm run log: {log_path}")
        return log_path

    def _teardown_run_logging(self) -> None:
        """Remove and close the per-run file handler."""
        if self._run_log_handler is None:
            return
        detach_session_handler(self._run_log_handler)
        self._run_log_handler.close()
        self._run_log_handler = None

    def run(self) -> SessionResult:
        """Execute swarm optimization: fan-out then snipe."""
        t_session = time.time()
        session_dir = (
            self.pipeline.benchmark_dir / "swarm_sessions" / self.query_id
        )
        self._setup_run_logging(session_dir)

        try:
            print(f"\n{'='*60}", flush=True)
            print(f"  SWARM SESSION: {self.query_id}", flush=True)
            print(f"  fan-out=4 workers  snipe=max {self.max_iterations - 1}  target={self.target_speedup:.1f}x", flush=True)
            print(f"{'='*60}", flush=True)

            # Phase 1: Parse logical tree once (shared across all iterations)
            self._stage(self.query_id, "PARSE: Building logical tree...")
            t0 = time.time()
            dag, costs, _explain = self.pipeline._parse_logical_tree(
                self.original_sql, dialect=self.dialect, query_id=self.query_id
            )
            self._stage(self.query_id, f"PARSE: done ({_fmt_elapsed(time.time() - t0)})")

            # Iteration 1: Fan-out phase
            print(f"\n--- Fan-out (4 workers) ---", flush=True)
            t_fanout = time.time()
            fan_out_result = self._fan_out_iteration(dag, costs, t_session)
            self.iterations_data.append(fan_out_result)
            self._stage(self.query_id, f"FAN-OUT: complete ({_fmt_elapsed(time.time() - t_fanout)}) | total {_fmt_elapsed(time.time() - t_session)}")

            best_wr = self._best_worker()
            if best_wr and best_wr.speedup >= self.target_speedup:
                print(f"\n{'='*60}", flush=True)
                print(f"  DONE: {self.query_id} — {best_wr.status} {best_wr.speedup:.2f}x "
                      f"(W{best_wr.worker_id} {best_wr.strategy}) | {_fmt_elapsed(time.time() - t_session)}", flush=True)
                print(f"{'='*60}\n", flush=True)
                self.save_session()
                return self._build_result()

            # Cache baseline for snipe iterations (avoids re-timing original)
            if self._cached_baseline is None and self.max_iterations > 1 and self._db_reachable is not False:
                try:
                    self._cached_baseline = self.pipeline._benchmark_baseline(self.original_sql)
                    self._stage(
                        self.query_id,
                        f"BASELINE: cached ({self._cached_baseline.measured_time_ms:.1f}ms, "
                        f"{self._cached_baseline.row_count} rows)"
                    )
                except Exception as e:
                    logger.warning(f"[{self.query_id}] Baseline caching failed: {e}")

            # Use baseline EXPLAIN as fallback for file-cached EXPLAIN
            if (not self._explain_plan_text
                    and self._cached_baseline is not None
                    and self._cached_baseline.explain_text):
                self._explain_plan_text = self._cached_baseline.explain_text
                self._stage(self.query_id, "EXPLAIN: using baseline EXPLAIN (no cached file)")

            # Iterations 2-N: Snipe phase (wrapped so API failures don't lose fan-out results)
            try:
                for snipe_num in range(1, self.max_iterations):
                    print(f"\n--- Snipe {snipe_num}/{self.max_iterations - 1} ---", flush=True)
                    t_snipe = time.time()
                    snipe_result = self._snipe_iteration(dag, costs, snipe_num, t_session)
                    self.iterations_data.append(snipe_result)
                    self._stage(self.query_id, f"SNIPE {snipe_num}: complete ({_fmt_elapsed(time.time() - t_snipe)}) | total {_fmt_elapsed(time.time() - t_session)}")

                    best_wr = self._best_worker()
                    if best_wr and best_wr.speedup >= self.target_speedup:
                        print(f"\n{'='*60}", flush=True)
                        print(f"  DONE: {self.query_id} — {best_wr.status} {best_wr.speedup:.2f}x "
                              f"(snipe {snipe_num}) | {_fmt_elapsed(time.time() - t_session)}", flush=True)
                        print(f"{'='*60}\n", flush=True)
                        self.save_session()
                        return self._build_result()
            except Exception as e:
                logger.error(f"[{self.query_id}] Snipe failed (saving fan-out results): {e}")
                self._stage(self.query_id, f"SNIPE FAILED: {e} — saving fan-out results")

            # Final summary
            best_wr = self._best_worker()
            print(f"\n{'='*60}", flush=True)
            if best_wr:
                print(f"  DONE: {self.query_id} — {best_wr.status} {best_wr.speedup:.2f}x "
                      f"(W{best_wr.worker_id} {best_wr.strategy}) | {_fmt_elapsed(time.time() - t_session)}", flush=True)
            else:
                print(f"  DONE: {self.query_id} — no valid results | {_fmt_elapsed(time.time() - t_session)}", flush=True)
            print(f"{'='*60}\n", flush=True)
            self.save_session()
            return self._build_result()
        finally:
            if self._run_log_path:
                logger.info(f"[{self.query_id}] Swarm log file saved: {self._run_log_path}")
            self._teardown_run_logging()

    def prepare_candidates(self) -> "SwarmSession":
        """Phase 1: Generate candidates (LLM calls only, no DB validation).

        Parses the logical tree, runs analyst + 4 workers in parallel.
        Stores generation result for later validate_and_finish().
        Thread-safe for concurrent use across multiple sessions.

        Returns self for chaining.
        """
        self._prepare_t_session = time.time()
        session_dir = (
            self.pipeline.benchmark_dir / "swarm_sessions" / self.query_id
        )
        self._setup_run_logging(session_dir)

        print(f"\n  GENERATE: {self.query_id}", flush=True)

        # Parse logical tree
        self._stage(self.query_id, "PARSE: Building logical tree...")
        t0 = time.time()
        self._dag, self._costs, _explain = self.pipeline._parse_logical_tree(
            self.original_sql, dialect=self.dialect, query_id=self.query_id
        )
        self._stage(self.query_id, f"PARSE: done ({_fmt_elapsed(time.time() - t0)})")

        # Generate candidates (Steps 1-5: analyst + 4 workers)
        self._gen_result = self._generate_fan_out(
            self._dag, self._costs, self._prepare_t_session
        )
        self._generation_done = True
        self._stage(
            self.query_id,
            f"GENERATE: complete ({_fmt_elapsed(time.time() - self._prepare_t_session)})"
        )
        return self

    def reload_candidates_from_disk(self) -> bool:
        """Reload Phase 1 results from disk (no LLM calls).

        Returns True if candidates were found and loaded, False otherwise.
        Reconstructs _gen_result so validate_and_finish() can proceed.
        """
        from ..prompts.swarm_parsers import BriefingWorker, BriefingShared, ParsedBriefing

        session_dir = self.pipeline.benchmark_dir / "swarm_sessions" / self.query_id
        iter_dir = session_dir / "iteration_00_fan_out"
        if not iter_dir.exists():
            return False

        # Reload analyst prompt/response
        analyst_prompt = ""
        analyst_response = ""
        ap = iter_dir / "analyst_prompt.txt"
        ar = iter_dir / "analyst_response.txt"
        if ap.exists():
            analyst_prompt = ap.read_text(errors="replace")
        if ar.exists():
            analyst_response = ar.read_text(errors="replace")

        # Reload per-worker candidates
        candidates_by_worker: Dict[int, tuple] = {}
        for w_dir in sorted(iter_dir.glob("worker_*")):
            rj = w_dir / "result.json"
            if not rj.exists():
                continue
            try:
                data = json.loads(rj.read_text())
            except (json.JSONDecodeError, OSError):
                continue

            wid = data.get("worker_id", 0)
            sql_file = w_dir / "optimized.sql"
            optimized_sql = sql_file.read_text(errors="replace") if sql_file.exists() else data.get("optimized_sql", self.original_sql)

            # Reconstruct BriefingWorker stub (enough for validation)
            wb = BriefingWorker(
                worker_id=wid,
                strategy=data.get("strategy", ""),
                examples=data.get("examples_used", []),
                example_adaptation=data.get("hint", ""),
            )
            transforms = data.get("transforms", [])
            set_local_cmds = data.get("set_local_commands", [])
            iface_warns = data.get("interface_warnings", [])

            candidates_by_worker[wid] = (wb, optimized_sql, transforms, "", "", set_local_cmds, iface_warns)

        if not candidates_by_worker:
            return False

        # Reconstruct minimal briefing (shared sections not needed for validation)
        briefing = ParsedBriefing(
            shared=BriefingShared(),
            workers=[candidates_by_worker[wid][0] for wid in sorted(candidates_by_worker)],
            raw=analyst_response,
        )

        # Parse logical tree (needed for snipe)
        self._prepare_t_session = time.time()
        self._dag, self._costs, _explain = self.pipeline._parse_logical_tree(
            self.original_sql, dialect=self.dialect, query_id=self.query_id
        )

        self._gen_result = {
            "_candidates_by_worker": candidates_by_worker,
            "_analyst_prompt": analyst_prompt,
            "_analyst_response": analyst_response,
            "_briefing": briefing,
        }
        self._generation_done = True
        self._setup_run_logging(session_dir)

        self._stage(self.query_id, f"RELOAD: {len(candidates_by_worker)} candidates loaded from disk")
        return True

    def resume_from_analyst(self) -> bool:
        """Resume from saved analyst response — skip analyst LLM, only run workers.

        For sessions where analyst_response.txt exists on disk but worker
        dirs were lost. Re-parses the briefing and generates 4 worker
        candidates (4 LLM calls, no analyst call).

        Returns True if analyst was recovered and workers generated.
        """
        from ..prompts.swarm_parsers import parse_briefing_response

        session_dir = self.pipeline.benchmark_dir / "swarm_sessions" / self.query_id
        iter_dir = session_dir / "iteration_00_fan_out"
        ar = iter_dir / "analyst_response.txt"
        ap = iter_dir / "analyst_prompt.txt"

        if not ar.exists():
            return False

        # Already have worker results? Use full reload instead
        if any(iter_dir.glob("worker_*/result.json")):
            return self.reload_candidates_from_disk()

        analyst_response = ar.read_text(errors="replace")
        analyst_prompt = ap.read_text(errors="replace") if ap.exists() else ""

        # Re-parse analyst briefing
        try:
            briefing = parse_briefing_response(analyst_response)
        except Exception as e:
            logger.warning(f"[{self.query_id}] Failed to parse saved analyst response: {e}")
            return False

        if not briefing.workers or len(briefing.workers) < 2:
            logger.warning(f"[{self.query_id}] Saved analyst response has {len(briefing.workers)} workers, skipping")
            return False

        self._stage(self.query_id, f"RESUME: Re-parsed analyst from disk ({len(briefing.workers)} workers)")

        # Parse logical tree
        self._prepare_t_session = time.time()
        self._setup_run_logging(session_dir)

        self._dag, self._costs, _explain = self.pipeline._parse_logical_tree(
            self.original_sql, dialect=self.dialect, query_id=self.query_id
        )

        # Generate worker candidates (LLM calls — but analyst is skipped)
        self._gen_result = self._generate_workers_from_briefing(
            briefing, analyst_prompt, analyst_response, iter_dir
        )
        self._generation_done = True
        return True

    def _generate_workers_from_briefing(
        self, briefing, analyst_prompt: str, analyst_response: str, iter_dir: Path,
    ) -> Dict[str, Any]:
        """Generate 4 worker candidates from a pre-parsed briefing (no analyst call).

        Reuses the worker generation logic from _generate_fan_out but skips
        the analyst LLM call entirely.
        """
        from ..prompts.worker import build_worker_prompt
        from ..generate import CandidateGenerator
        from ..logic_tree import build_logic_tree

        t_gen = time.time()

        # Build output columns
        output_columns = ""
        if self._dag:
            try:
                final_node = [n for n in self._dag.nodes() if self._dag.out_degree(n) == 0]
                if final_node:
                    cols = self._dag.nodes[final_node[0]].get("columns", [])
                    if cols:
                        output_columns = ", ".join(cols)
            except Exception:
                pass

        # Build logic tree (same as _generate_fan_out)
        original_logic_tree = ""
        try:
            from ..logic_tree import build_logic_tree
            original_logic_tree = build_logic_tree(
                self.original_sql, self._dag, self._costs, self.dialect, {}
            )
        except Exception as e:
            logger.warning(f"[{self.query_id}] Logic tree build failed: {e}")

        candidates_by_worker = {}
        generator = CandidateGenerator()

        def generate_worker(worker_briefing):
            examples = self.pipeline._load_examples_by_id(
                worker_briefing.examples, self.engine
            )
            prompt = build_worker_prompt(
                worker_briefing=worker_briefing,
                shared_briefing=briefing.shared,
                examples=examples,
                original_sql=self.original_sql,
                output_columns=output_columns,
                dialect=self.dialect,
                engine_version=self.pipeline._engine_version,
                original_logic_tree=original_logic_tree,
            )
            example_ids = [e.get("id", "?") for e in examples]
            candidate = generator.generate_one(
                sql=self.original_sql,
                prompt=prompt,
                examples_used=example_ids,
                worker_id=worker_briefing.worker_id,
                dialect=self.dialect,
            )
            optimized_sql = candidate.optimized_sql
            try:
                import sqlglot
                sqlglot.parse_one(optimized_sql, dialect=self.dialect)
            except Exception:
                optimized_sql = self.original_sql
            return (worker_briefing, optimized_sql, candidate.transforms,
                    candidate.prompt, candidate.response, candidate.set_local_commands,
                    candidate.interface_warnings)

        self._stage(self.query_id, f"GENERATE: 4 workers in parallel (analyst from disk)... | total {_fmt_elapsed(time.time() - self._prepare_t_session)}")

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {pool.submit(generate_worker, w): w for w in briefing.workers}
            for future in as_completed(futures):
                try:
                    wb, optimized_sql, transforms, w_prompt, w_response, set_local_cmds, iface_warns = future.result()
                    candidates_by_worker[wb.worker_id] = (
                        wb, optimized_sql, transforms, w_prompt, w_response, set_local_cmds, iface_warns
                    )
                    self._stage(self.query_id, f"GENERATE: W{wb.worker_id} ({wb.strategy}) ready ({_fmt_elapsed(time.time() - t_gen)})")
                except Exception as e:
                    wb = futures[future]
                    logger.error(f"[{self.query_id}] W{wb.worker_id} generation failed: {e}")
                    candidates_by_worker[wb.worker_id] = (wb, self.original_sql, [], "", str(e), [], [])

        self._stage(self.query_id, f"GENERATE: all {len(candidates_by_worker)} workers complete ({_fmt_elapsed(time.time() - t_gen)})")

        # Write ALL worker outputs to disk immediately (same as _generate_fan_out)
        for wid in sorted(candidates_by_worker.keys()):
            wb, sql, transforms, w_prompt, w_response, slc, iw = candidates_by_worker[wid]
            w_dir = iter_dir / f"worker_{wid:02d}"
            w_dir.mkdir(parents=True, exist_ok=True)
            if w_prompt:
                (w_dir / "prompt.txt").write_text(w_prompt, encoding="utf-8")
            if w_response:
                (w_dir / "response.txt").write_text(w_response, encoding="utf-8")
            (w_dir / "optimized.sql").write_text(sql, encoding="utf-8")
            gen_result = {
                "worker_id": wb.worker_id,
                "strategy": wb.strategy,
                "examples_used": wb.examples,
                "optimized_sql": sql,
                "transforms": transforms,
                "hint": wb.example_adaptation[:80] if wb.example_adaptation else "",
                "set_local_commands": slc or [],
                "interface_warnings": iw or [],
                "speedup": 0.0,
                "status": "PENDING",
            }
            (w_dir / "result.json").write_text(
                json.dumps(gen_result, indent=2, default=str), encoding="utf-8"
            )

        return {
            "_candidates_by_worker": candidates_by_worker,
            "_analyst_prompt": analyst_prompt,
            "_analyst_response": analyst_response,
            "_briefing": briefing,
            "_generator": generator,
        }

    def compact_gen_result(self) -> None:
        """Free large text fields from generation result to reduce memory.

        Call after Phase 1 before Phase 2 starts. Prompts/responses are
        already saved to disk by _generate_fan_out.
        """
        if not self._gen_result:
            return
        # Drop the generator object (holds LLM client state)
        self._gen_result.pop("_generator", None)
        # Trim worker prompt/response text from candidates (already on disk)
        cbw = self._gen_result.get("_candidates_by_worker", {})
        for wid in cbw:
            wb, sql, transforms, _prompt, _response, set_local, iface = cbw[wid]
            # Keep sql, transforms, set_local, iface_warns — drop prompt/response text
            cbw[wid] = (wb, sql, transforms, "", "", set_local, iface)

    def validate_and_finish(self) -> SessionResult:
        """Phase 2: Validate candidates and run snipe iterations.

        Must be called after prepare_candidates(). Runs DB validation
        (race or sequential), then snipe iterations if needed.

        Returns SessionResult.
        """
        t_session = self._prepare_t_session
        dag = self._dag
        costs = self._costs

        try:
            print(f"\n  VALIDATE: {self.query_id}", flush=True)

            # Validate fan-out candidates
            t_val = time.time()
            fan_out_result = self._validate_fan_out(
                self._gen_result, dag, costs, t_session
            )
            self.iterations_data.append(fan_out_result)
            self._stage(
                self.query_id,
                f"FAN-OUT: complete ({_fmt_elapsed(time.time() - t_val)}) | "
                f"total {_fmt_elapsed(time.time() - t_session)}"
            )

            best_wr = self._best_worker()
            if best_wr and best_wr.speedup >= self.target_speedup:
                print(
                    f"  DONE: {self.query_id} — {best_wr.status} {best_wr.speedup:.2f}x "
                    f"(W{best_wr.worker_id} {best_wr.strategy}) | "
                    f"{_fmt_elapsed(time.time() - t_session)}",
                    flush=True,
                )
                self.save_session()
                return self._build_result()

            # Cache baseline for snipe iterations
            if self._cached_baseline is None and self.max_iterations > 1 and self._db_reachable is not False:
                try:
                    self._cached_baseline = self.pipeline._benchmark_baseline(
                        self.original_sql
                    )
                    self._stage(
                        self.query_id,
                        f"BASELINE: cached ({self._cached_baseline.measured_time_ms:.1f}ms, "
                        f"{self._cached_baseline.row_count} rows)"
                    )
                except Exception as e:
                    logger.warning(f"[{self.query_id}] Baseline caching failed: {e}")

            if (not self._explain_plan_text
                    and self._cached_baseline is not None
                    and self._cached_baseline.explain_text):
                self._explain_plan_text = self._cached_baseline.explain_text

            # Snipe iterations
            for snipe_num in range(1, self.max_iterations):
                print(f"\n  --- Snipe {snipe_num}/{self.max_iterations - 1} [{self.query_id}] ---", flush=True)
                t_snipe = time.time()
                snipe_result = self._snipe_iteration(dag, costs, snipe_num, t_session)
                self.iterations_data.append(snipe_result)
                self._stage(
                    self.query_id,
                    f"SNIPE {snipe_num}: complete ({_fmt_elapsed(time.time() - t_snipe)}) | "
                    f"total {_fmt_elapsed(time.time() - t_session)}"
                )

                best_wr = self._best_worker()
                if best_wr and best_wr.speedup >= self.target_speedup:
                    print(
                        f"  DONE: {self.query_id} — {best_wr.status} {best_wr.speedup:.2f}x "
                        f"(snipe {snipe_num}) | {_fmt_elapsed(time.time() - t_session)}",
                        flush=True,
                    )
                    self.save_session()
                    return self._build_result()

            # Final
            best_wr = self._best_worker()
            if best_wr:
                print(
                    f"  DONE: {self.query_id} — {best_wr.status} {best_wr.speedup:.2f}x "
                    f"(W{best_wr.worker_id} {best_wr.strategy}) | "
                    f"{_fmt_elapsed(time.time() - t_session)}",
                    flush=True,
                )
            else:
                print(
                    f"  DONE: {self.query_id} — no valid results | "
                    f"{_fmt_elapsed(time.time() - t_session)}",
                    flush=True,
                )
            self.save_session()
            return self._build_result()
        finally:
            if self._run_log_path:
                logger.info(f"[{self.query_id}] Swarm log file saved: {self._run_log_path}")
            self._teardown_run_logging()

    def _fan_out_iteration(
        self, dag: Any, costs: Dict[str, Any], t_session: float,
    ) -> Dict[str, Any]:
        """Iteration 1: Analyst + workers + validation (backward-compat wrapper)."""
        gen_result = self._generate_fan_out(dag, costs, t_session)
        return self._validate_fan_out(gen_result, dag, costs, t_session)

    def _generate_fan_out(
        self, dag: Any, costs: Dict[str, Any], t_session: float,
    ) -> Dict[str, Any]:
        """Steps 1-5: Gather data, analyst LLM, parse briefing, generate 4 workers.

        Returns a dict with all generation artifacts needed for validation.
        Does NOT touch the database for benchmarking.
        """
        from ..prompts import (
            build_analyst_briefing_prompt,
            build_worker_prompt,
            parse_briefing_response,
            validate_parsed_briefing,
        )
        # ── Step 1: Gather all raw data ──────────────────────────────────
        self._stage(self.query_id, "Gathering data (EXPLAIN, knowledge, examples)...")
        t0 = time.time()

        ctx = self.pipeline.gather_analyst_context(
            query_id=self.query_id,
            sql=self.original_sql,
            dialect=self.dialect,
            engine=self.engine,
        )

        # Unpack for local use + snipe reuse
        explain_plan_text = ctx["explain_plan_text"]
        plan_scanner_text = ctx["plan_scanner_text"]
        global_knowledge = ctx["global_knowledge"]
        semantic_intents = ctx["semantic_intents"]
        matched_examples = ctx["matched_examples"]
        engine_profile = ctx["engine_profile"]
        constraints = ctx["constraints"]
        regression_warnings = ctx["regression_warnings"]
        strategy_leaderboard = ctx["strategy_leaderboard"]
        query_archetype = ctx["query_archetype"]
        resource_envelope = ctx["resource_envelope"]
        exploit_algorithm_text = ctx["exploit_algorithm_text"]
        detected_transforms = ctx.get("detected_transforms", [])
        qerror_analysis = ctx.get("qerror_analysis")

        self._stage(
            self.query_id,
            f"Data ready — {len(matched_examples)} examples, "
            f"{len(constraints)} constraints, "
            f"EXPLAIN={'yes' if explain_plan_text else 'no'}, "
            f"GlobalKnowledge={'yes' if global_knowledge else 'no'}"
            f"{f', archetype={query_archetype}' if query_archetype else ''} "
            f"({_fmt_elapsed(time.time() - t0)})"
        )

        # Persist data for snipe reuse
        self._explain_plan_text = explain_plan_text
        self._engine_profile = engine_profile
        self._constraints = constraints
        self._semantic_intents = semantic_intents
        self._matched_examples = matched_examples
        self._regression_warnings = regression_warnings

        # ── Step 2: Build analyst prompt ─────────────────────────────────
        analyst_prompt = build_analyst_briefing_prompt(
            query_id=self.query_id,
            sql=self.original_sql,
            explain_plan_text=explain_plan_text,
            dag=dag,
            costs=costs,
            semantic_intents=semantic_intents,
            global_knowledge=global_knowledge,
            constraints=constraints,
            dialect=self.dialect,
            strategy_leaderboard=strategy_leaderboard,
            query_archetype=query_archetype,
            engine_profile=engine_profile,
            resource_envelope=resource_envelope,
            exploit_algorithm_text=exploit_algorithm_text,
            plan_scanner_text=plan_scanner_text,
            detected_transforms=detected_transforms,
            qerror_analysis=qerror_analysis,
        )

        # ── Step 3: Call analyst LLM ─────────────────────────────────────
        self._stage(self.query_id, "ANALYST: Generating structured briefing...")
        t0 = time.time()

        from ..generate import CandidateGenerator
        generator = CandidateGenerator(
            provider=self.pipeline.provider,
            model=self.pipeline.model,
            analyze_fn=self.pipeline.analyze_fn,
        )

        try:
            analyst_response = generator._analyze_with_max_tokens(
                analyst_prompt, max_tokens=4096
            )
        except Exception as e:
            self._stage(self.query_id, f"ANALYST: failed ({e})")
            raise RuntimeError(f"Analyst call failed: {e}") from e

        self._stage(
            self.query_id,
            f"ANALYST: done ({len(analyst_response)} chars, "
            f"{_fmt_elapsed(time.time() - t0)})"
        )

        # Persist analyst prompt + response before validation (so failures are debuggable)
        iter_dir = self.pipeline.benchmark_dir / "swarm_sessions" / self.query_id / "iteration_00_fan_out"
        iter_dir.mkdir(parents=True, exist_ok=True)
        (iter_dir / "analyst_prompt.txt").write_text(analyst_prompt, encoding="utf-8")
        (iter_dir / "analyst_response.txt").write_text(analyst_response, encoding="utf-8")

        # ── Step 4: Parse briefing (with retry on format errors) ────────
        briefing = parse_briefing_response(analyst_response)
        briefing_issues = validate_parsed_briefing(briefing)

        if briefing_issues:
            # Retry once: feed error back to LLM
            for issue in briefing_issues[:8]:
                self._stage(self.query_id, f"ANALYST BRIEFING ERROR: {issue}")
            self._stage(self.query_id, "ANALYST: Retrying with error feedback...")

            error_feedback = "\n".join(briefing_issues[:8])
            retry_prompt = (
                analyst_prompt
                + "\n\n--- FORMAT ERROR IN YOUR PREVIOUS RESPONSE ---\n"
                + "Your response had the following validation errors:\n"
                + error_feedback
                + "\n\nPlease re-emit the COMPLETE briefing in the correct format. "
                + "All sections (SHARED, WORKER_1 through WORKER_4) must be present "
                + "with all required fields."
            )

            t_retry = time.time()
            try:
                analyst_response = generator._analyze_with_max_tokens(
                    retry_prompt, max_tokens=4096
                )
                self._stage(
                    self.query_id,
                    f"ANALYST: retry done ({len(analyst_response)} chars, "
                    f"{_fmt_elapsed(time.time() - t_retry)})"
                )
                (iter_dir / "analyst_response_retry.txt").write_text(
                    analyst_response, encoding="utf-8"
                )

                briefing = parse_briefing_response(analyst_response)
                briefing_issues = validate_parsed_briefing(briefing)
            except Exception as e:
                self._stage(self.query_id, f"ANALYST: retry failed ({e})")
                briefing_issues = briefing_issues or [str(e)]

            if briefing_issues:
                for issue in briefing_issues[:4]:
                    self._stage(self.query_id, f"ANALYST RETRY ERROR: {issue}")
                raise RuntimeError(
                    "Analyst briefing failed validation (after retry): "
                    + "; ".join(briefing_issues[:4])
                )
            self._stage(self.query_id, "ANALYST: retry succeeded")

        # Log parsed briefing
        for w in briefing.workers:
            self._stage(self.query_id, f"  W{w.worker_id}: {w.strategy}")
        self._stage(
            self.query_id,
            f"  Shared: semantic_contract={len(briefing.shared.semantic_contract)} chars, "
            f"bottleneck={len(briefing.shared.bottleneck_diagnosis)} chars, "
            f"constraints={len(briefing.shared.active_constraints)} chars"
        )

        # ── Step 5: Generate 4 workers in PARALLEL ──────────────────────
        self._stage(self.query_id, f"GENERATE: 4 workers in parallel... | total {_fmt_elapsed(time.time() - t_session)}")
        t_gen = time.time()

        # Extract output columns for column completeness contract
        from ..prompter import Prompter
        output_columns = Prompter._extract_output_columns(dag)
        self._output_columns = output_columns

        # Build Logic Tree once (shared across all workers for DAP context)
        original_logic_tree = None
        try:
            from ..logic_tree import build_logic_tree
            from ..prompter import _build_node_intent_map
            node_intents = _build_node_intent_map(semantic_intents)
            if semantic_intents:
                qi = semantic_intents.get("query_intent", "")
                if qi and "main_query" not in node_intents:
                    node_intents["main_query"] = qi
            original_logic_tree = build_logic_tree(
                self.original_sql, dag, costs, self.dialect, node_intents
            )
        except Exception as e:
            logger.warning(f"[{self.query_id}] Logic tree build failed: {e}")

        candidates_by_worker = {}

        def generate_worker(worker_briefing):
            """Generate a single worker's candidate."""
            # Load examples by ID
            examples = self.pipeline._load_examples_by_id(
                worker_briefing.examples, self.engine
            )

            # Build worker prompt
            prompt = build_worker_prompt(
                worker_briefing=worker_briefing,
                shared_briefing=briefing.shared,
                examples=examples,
                original_sql=self.original_sql,
                output_columns=output_columns,
                dialect=self.dialect,
                engine_version=self.pipeline._engine_version,
                original_logic_tree=original_logic_tree,
            )

            example_ids = [e.get("id", "?") for e in examples]
            candidate = generator.generate_one(
                sql=self.original_sql,
                prompt=prompt,
                examples_used=example_ids,
                worker_id=worker_briefing.worker_id,
                dialect=self.dialect,
            )

            # Syntax check
            optimized_sql = candidate.optimized_sql
            try:
                import sqlglot
                sqlglot.parse_one(optimized_sql, dialect=self.dialect)
            except Exception:
                optimized_sql = self.original_sql

            return (worker_briefing, optimized_sql, candidate.transforms,
                    candidate.prompt, candidate.response, candidate.set_local_commands,
                    candidate.interface_warnings)

        # Parallel LLM generation
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {
                pool.submit(generate_worker, w): w for w in briefing.workers
            }
            for future in as_completed(futures):
                try:
                    wb, optimized_sql, transforms, w_prompt, w_response, set_local_cmds, iface_warns = future.result()
                    candidates_by_worker[wb.worker_id] = (
                        wb, optimized_sql, transforms, w_prompt, w_response, set_local_cmds, iface_warns
                    )
                    if iface_warns:
                        self._stage(
                            self.query_id,
                            f"GENERATE: W{wb.worker_id} interface warnings: {len(iface_warns)}"
                        )
                    self._stage(
                        self.query_id,
                        f"GENERATE: W{wb.worker_id} ({wb.strategy}) ready "
                        f"({_fmt_elapsed(time.time() - t_gen)})"
                    )
                except Exception as e:
                    wb = futures[future]
                    logger.error(f"[{self.query_id}] W{wb.worker_id} generation failed: {e}")
                    candidates_by_worker[wb.worker_id] = (
                        wb, self.original_sql, [], "", str(e), [], []
                    )

        self._stage(self.query_id, f"GENERATE: all 4 workers complete ({_fmt_elapsed(time.time() - t_gen)})")

        # Persist ALL worker outputs to disk immediately after generation.
        # This ensures no LLM results are lost if the process is interrupted
        # before validation completes.
        for wid in sorted(candidates_by_worker.keys()):
            wb, sql, transforms, w_prompt, w_response, slc, iw = candidates_by_worker[wid]
            w_dir = iter_dir / f"worker_{wid:02d}"
            w_dir.mkdir(parents=True, exist_ok=True)
            if w_prompt:
                (w_dir / "prompt.txt").write_text(w_prompt, encoding="utf-8")
            if w_response:
                (w_dir / "response.txt").write_text(w_response, encoding="utf-8")
            # Save optimized SQL and generation metadata (pre-validation)
            (w_dir / "optimized.sql").write_text(sql, encoding="utf-8")
            gen_result = {
                "worker_id": wb.worker_id,
                "strategy": wb.strategy,
                "examples_used": wb.examples,
                "optimized_sql": sql,
                "transforms": transforms,
                "hint": wb.example_adaptation[:80] if wb.example_adaptation else "",
                "set_local_commands": slc or [],
                "interface_warnings": iw or [],
                "speedup": 0.0,
                "status": "PENDING",
            }
            (w_dir / "result.json").write_text(
                json.dumps(gen_result, indent=2, default=str), encoding="utf-8"
            )

        return {
            "_candidates_by_worker": candidates_by_worker,
            "_analyst_prompt": analyst_prompt,
            "_analyst_response": analyst_response,
            "_briefing": briefing,
            "_generator": generator,
        }

    def _validate_fan_out(
        self, gen_result: Dict[str, Any], dag: Any, costs: Dict[str, Any], t_session: float,
    ) -> Dict[str, Any]:
        """Steps 6+: Race/validate candidates, collect EXPLAINs, save learning records.

        Takes the generation result from _generate_fan_out() and runs DB validation.
        """
        candidates_by_worker = gen_result["_candidates_by_worker"]
        analyst_prompt = gen_result["_analyst_prompt"]
        analyst_response = gen_result["_analyst_response"]
        briefing = gen_result["_briefing"]

        # ── Step 6: Validation — Race first, fallback to sequential ─────
        sorted_wids = sorted(candidates_by_worker.keys())
        optimized_sqls = [
            candidates_by_worker[wid][1] for wid in sorted_wids
        ]

        self._stage(self.query_id, f"VALIDATE: Racing original + 4 candidates... | total {_fmt_elapsed(time.time() - t_session)}")
        t_val = time.time()

        # ── Pre-flight: verify DB connectivity ────────────────────────────
        # Skip validation entirely if DB is unreachable (e.g. Snowflake auth pending)
        batch_results = None
        if self._db_reachable is None:
            try:
                from ..execution.factory import create_executor_from_dsn
                test_exec = create_executor_from_dsn(self.pipeline.config.db_path_or_dsn)
                test_exec.execute("SELECT 1")
                test_exec.close()
                self._db_reachable = True
            except Exception as e:
                self._db_reachable = False
                self._stage(self.query_id, f"VALIDATE: DB unreachable ({type(e).__name__}: {str(e)[:80]})")
        if not self._db_reachable:
            self._stage(self.query_id, "VALIDATE: Skipping — returning NEUTRAL for all candidates (generation-only mode)")
            batch_results = [("NEUTRAL", 1.00, ["DB unreachable — validation skipped"], None)] * len(optimized_sqls)

        # Try parallel race first (all 5 queries run simultaneously)
        from ..validate import race_candidates
        cfg = self.pipeline.config
        if batch_results is None:
            try:
                race_result = race_candidates(
                    db_path=cfg.db_path_or_dsn,
                    original_sql=self.original_sql,
                    candidate_sqls=optimized_sqls,
                    worker_ids=sorted_wids,
                    min_runtime_ms=cfg.race_min_runtime_ms,
                    min_margin=cfg.race_min_margin,
                    timeout_ms=cfg.timeout_seconds * 1000,
                )
                if race_result is not None:
                    batch_results = race_result.verdicts
                    # Cache baseline and race result for snipe reuse
                    self._cached_baseline = race_result.baseline
                    self._race_result = race_result

                    # Log race results
                    lane_tags = []
                    for cl in race_result.candidates:
                        if not cl.finished:
                            lane_tags.append(f"W{cl.lane_id}=DNF")
                        elif cl.error:
                            lane_tags.append(f"W{cl.lane_id}=ERR")
                        else:
                            lane_tags.append(f"W{cl.lane_id}={cl.elapsed_ms:.0f}ms")
                    winner_tag = "WINNER" if race_result.has_clear_winner else "NO WINNER"
                    self._stage(
                        self.query_id,
                        f"RACE [{winner_tag}]: original={race_result.original.elapsed_ms:.0f}ms | "
                        + " | ".join(lane_tags)
                    )

                    # Use race baseline EXPLAIN as fallback
                    if (not self._explain_plan_text
                            and race_result.baseline.explain_text):
                        self._explain_plan_text = race_result.baseline.explain_text
                        self._stage(self.query_id, "EXPLAIN: captured from race baseline")

                    # Post-race checksum: verify passing candidates match original
                    batch_results = self._checksum_race_verdicts(
                        batch_results, optimized_sqls, sorted_wids,
                    )
            except Exception as e:
                logger.warning(f"[{self.query_id}] Race failed, falling back: {e}")

        # Fallback: sequential validation (for fast queries < 2s)
        if batch_results is None:
            self._stage(self.query_id, f"VALIDATE: Sequential fallback (query < 2s)...")

            # Cost-rank pre-screening (DuckDB only)
            cost_ranked_indices = None
            if self.dialect == "duckdb":
                try:
                    from ..validate import cost_rank_candidates
                    cost_ranked_indices = cost_rank_candidates(
                        db_path=self.pipeline.config.db_path_or_dsn,
                        original_sql=self.original_sql,
                        candidate_sqls=optimized_sqls,
                        top_k=2,
                    )
                    if cost_ranked_indices and len(cost_ranked_indices) < len(optimized_sqls):
                        ranked_wids = [sorted_wids[i] for i in cost_ranked_indices]
                        self._stage(
                            self.query_id,
                            f"COST SCREEN: Top 2 by EXPLAIN cost: W{ranked_wids[0]}, W{ranked_wids[1]}"
                        )
                except Exception as e:
                    logger.warning(f"[{self.query_id}] Cost pre-screening failed: {e}")
                    cost_ranked_indices = None

            if cost_ranked_indices is not None and len(cost_ranked_indices) < len(optimized_sqls):
                benchmark_sqls = [optimized_sqls[i] for i in cost_ranked_indices]
                real_results, seq_baseline = self.pipeline._validate_batch(
                    self.original_sql, benchmark_sqls, return_baseline=True
                )
                batch_results = []
                real_idx = 0
                for i in range(len(optimized_sqls)):
                    if i in cost_ranked_indices:
                        batch_results.append(real_results[real_idx])
                        real_idx += 1
                    else:
                        batch_results.append(("NEUTRAL", 1.00, [], None))
            else:
                batch_results, seq_baseline = self.pipeline._validate_batch(
                    self.original_sql, optimized_sqls, return_baseline=True
                )

            # Cache baseline from sequential validation (includes EXPLAIN)
            if seq_baseline is not None and self._cached_baseline is None:
                self._cached_baseline = seq_baseline

        worker_results = []
        worker_prompts = {}
        for wid, (status, speedup, error_msgs, error_cat) in zip(sorted_wids, batch_results):
            wb, optimized_sql, transforms, w_prompt, w_response, set_local_cmds, iface_warns = candidates_by_worker[wid]
            worker_prompts[wid] = (w_prompt, w_response)
            result_sql = optimized_sql

            # Prepend interface warnings to error messages so retry worker sees them
            all_errors = list(error_msgs or [])
            if iface_warns:
                all_errors = [f"INTERFACE: {w}" for w in iface_warns] + all_errors

            # Tag Worker 4 as exploratory for separate tracking
            is_exploratory = (wb.worker_id == 4)
            wr = WorkerResult(
                worker_id=wb.worker_id,
                strategy=wb.strategy,
                examples_used=wb.examples,
                optimized_sql=result_sql,
                speedup=speedup,
                status=status,
                transforms=transforms,
                hint=wb.example_adaptation[:80] if wb.example_adaptation else "",
                error_message=" | ".join(all_errors) if all_errors else None,
                error_messages=all_errors,
                error_category=error_cat,
                exploratory=is_exploratory,
            )
            worker_results.append(wr)
            self.all_worker_results.append(wr)

        # Print results table
        self._stage(self.query_id, f"VALIDATE: complete ({_fmt_elapsed(time.time() - t_val)})")
        for wr in sorted(worker_results, key=lambda w: w.worker_id):
            marker = "*" if wr.speedup >= 1.10 else " "
            explore_tag = " [EXPLORE]" if getattr(wr, 'exploratory', False) else ""
            err = f" — {wr.error_message[:60]}" if wr.error_message else ""
            self._stage(
                self.query_id,
                f" {marker} W{wr.worker_id} ({wr.strategy}){explore_tag}: {wr.status} {wr.speedup:.2f}x{err}"
            )

        # ── Step 6.6: Collect candidate EXPLAIN plans for snipe ──────────
        if self.max_iterations > 1:
            self._candidate_explains = self._collect_candidate_explains(worker_results)

        # Learning records
        for wid, (status, speedup, error_msgs, error_cat) in zip(sorted_wids, batch_results):
            wr_match = [w for w in worker_results if w.worker_id == wid]
            if not wr_match:
                continue
            wr = wr_match[0]
            try:
                lr = self.pipeline.learner.create_learning_record(
                    query_id=self.query_id,
                    examples_recommended=wr.examples_used,
                    transforms_recommended=wr.examples_used,
                    status="pass" if wr.status in ("WIN", "IMPROVED", "NEUTRAL") else "error",
                    speedup=wr.speedup,
                    transforms_used=wr.transforms,
                    error_category=error_cat,
                    error_messages=error_msgs,
                )
                self.pipeline.learner.save_learning_record(lr)
            except Exception as e:
                logger.warning(f"[{self.query_id}] Learning record failed: {e}")

        worker_results.sort(key=lambda w: w.worker_id)

        return {
            "iteration": 0,
            "phase": "fan_out",
            "analyst_prompt": analyst_prompt,
            "analyst_response": analyst_response,
            "briefing_shared": {
                "semantic_contract": briefing.shared.semantic_contract,
                "bottleneck_diagnosis": briefing.shared.bottleneck_diagnosis,
                "active_constraints": briefing.shared.active_constraints,
                "regression_warnings": briefing.shared.regression_warnings,
            },
            "worker_prompts": worker_prompts,
            "worker_results": [wr.to_dict() for wr in worker_results],
            "best_speedup": max((wr.speedup for wr in worker_results), default=0.0),
        }

    def _get_explain_plan_text(self, query_id: str) -> Optional[str]:
        """Load EXPLAIN ANALYZE plan text — delegates to Pipeline."""
        return self.pipeline.get_explain_plan_text(query_id, self.dialect)

    def _get_explain_plan_json(self, query_id: str) -> Optional[Any]:
        """Load raw EXPLAIN JSON plan data (for PG tuner prompt).

        Searches: explains/ (flat) → explains/sf10/ → explains/sf5/ (backward compat).
        Returns the plan_json list/dict or None.
        """
        search_paths = [
            self.pipeline.benchmark_dir / "explains" / f"{query_id}.json",
            self.pipeline.benchmark_dir / "explains" / "sf10" / f"{query_id}.json",
            self.pipeline.benchmark_dir / "explains" / "sf5" / f"{query_id}.json",
        ]

        for cache_path in search_paths:
            if cache_path.exists():
                try:
                    data = json.loads(cache_path.read_text())
                    plan_json = data.get("plan_json")
                    if plan_json:
                        return plan_json
                except Exception:
                    pass
        return None

    def _collect_candidate_explains(
        self,
        worker_results: List[WorkerResult],
    ) -> Dict[int, str]:
        """Collect execution plans for all candidates.

        - PASS/WIN/IMPROVED: EXPLAIN ANALYZE (actual execution timings)
        - ERROR/FAIL: EXPLAIN estimate (no execution) — if that also fails
          (e.g. column reference errors), stores the error message so it's
          co-located with the SQL in the retry prompt.

        Returns dict of worker_id → formatted explain text or error context.
        """
        from ..execution.database_utils import run_explain_analyze, run_explain_text
        from ..prompts.analyst_briefing import format_pg_explain_tree

        explains: Dict[int, str] = {}
        dsn = self.pipeline.config.db_path_or_dsn
        t0 = time.time()
        collected = 0

        for wr in worker_results:
            # Skip candidates that fell back to original
            if wr.optimized_sql.strip() == self.original_sql.strip():
                continue

            if wr.status in ("ERROR", "FAIL"):
                # ERROR candidates: try EXPLAIN (estimate only, no execution)
                try:
                    plan_text = run_explain_text(dsn, wr.optimized_sql)
                    if plan_text:
                        explains[wr.worker_id] = (
                            f"[EXPLAIN estimate — query errored at execution]\n{plan_text}"
                        )
                        collected += 1
                        continue
                except Exception:
                    pass

                # EXPLAIN also failed — store the error message as context
                err_msg = wr.error_message or "Unknown error"
                explains[wr.worker_id] = (
                    f"[EXPLAIN failed — planner rejected this SQL]\n"
                    f"Error: {err_msg}"
                )
                collected += 1
                continue

            # PASS/WIN/IMPROVED: full EXPLAIN ANALYZE
            try:
                result = run_explain_analyze(dsn, wr.optimized_sql)
                if not result:
                    continue

                # Format to text
                plan_text = result.get("plan_text")
                if not plan_text:
                    plan_json = result.get("plan_json")
                    if plan_json:
                        plan_text = format_pg_explain_tree(plan_json)

                if plan_text:
                    explains[wr.worker_id] = plan_text
                    collected += 1
            except Exception as e:
                logger.warning(
                    f"[{self.query_id}] EXPLAIN for W{wr.worker_id} failed: {e}"
                )

        elapsed = time.time() - t0
        if collected:
            self._stage(
                self.query_id,
                f"EXPLAIN candidates: {collected} collected ({_fmt_elapsed(elapsed)})"
            )
        return explains

    def _snipe_iteration(
        self, dag: Any, costs: Dict[str, Any], snipe_num: int, t_session: float,
    ) -> Dict[str, Any]:
        """Self-directed retry iteration — single LLM call.

        The retry worker gets all raw evidence (previous results, EXPLAIN plans,
        race timings) plus standard worker context and self-directs through
        diagnose → identify → rewrite in one pass. No analyst intermediary.
        """
        from ..prompts.swarm_snipe import build_retry_worker_prompt
        from ..generate import CandidateGenerator
        from ..prompter import _load_constraint_files, _load_engine_profile, Prompter

        generator = CandidateGenerator(
            provider=self.pipeline.provider,
            model=self.pipeline.model,
            analyze_fn=self.pipeline.analyze_fn,
        )

        # Lazy-load any data missing (e.g., fallback fan-out path)
        if not self._engine_profile:
            self._engine_profile = _load_engine_profile(self.dialect)
        if not self._constraints:
            self._constraints = _load_constraint_files(self.dialect)
        if not self._explain_plan_text:
            self._explain_plan_text = self._get_explain_plan_text(self.query_id)
        if not self._matched_examples:
            self._matched_examples = self.pipeline._find_examples(
                self.original_sql, engine=self.engine, k=20
            )
        if not self._output_columns:
            self._output_columns = Prompter._extract_output_columns(dag)
        if self._regression_warnings is None:
            self._regression_warnings = self.pipeline._find_regression_warnings(
                self.original_sql, engine=self.engine, k=3
            )
        if not self._semantic_intents:
            self._semantic_intents = self.pipeline.get_semantic_intents(self.query_id)

        # ── Step 1: Collect EXPLAIN plans if not already done ─────────────
        if not self._candidate_explains and self.all_worker_results:
            self._candidate_explains = self._collect_candidate_explains(
                self.all_worker_results
            )

        # ── Step 2: Find best worker SQL + load matched examples ─────────
        best_worker_sql = self._find_best_worker_sql()
        examples = self.pipeline._load_examples_by_id(
            [e.get("id", "") for e in self._matched_examples[:6]],
            self.engine,
        )

        # ── Step 3: Recover shared briefing from fan-out ─────────────────
        shared_briefing = None
        if self.iterations_data:
            fan_out = self.iterations_data[0]
            bs = fan_out.get("briefing_shared")
            if bs:
                from ..prompts.swarm_parsers import BriefingShared
                shared_briefing = BriefingShared(
                    semantic_contract=bs.get("semantic_contract", ""),
                    bottleneck_diagnosis=bs.get("bottleneck_diagnosis", ""),
                    active_constraints=bs.get("active_constraints", ""),
                    regression_warnings=bs.get("regression_warnings", ""),
                )

        # ── Step 4: Build self-directed retry worker prompt ──────────────
        self._stage(
            self.query_id,
            f"RETRY WORKER: Self-directed rewrite ({len(self.all_worker_results)} "
            f"prior results) | total {_fmt_elapsed(time.time() - t_session)}"
        )

        previous_retry = self._sniper_result if snipe_num >= 2 else None

        retry_prompt = build_retry_worker_prompt(
            original_sql=self.original_sql,
            worker_results=self.all_worker_results,
            best_worker_sql=best_worker_sql,
            examples=examples,
            output_columns=self._output_columns,
            dag=dag,
            costs=costs,
            explain_plan_text=self._explain_plan_text,
            candidate_explains=self._candidate_explains,
            race_timings=self._build_race_timings(),
            engine_profile=self._engine_profile,
            constraints=self._constraints,
            semantic_intents=self._semantic_intents,
            regression_warnings=self._regression_warnings,
            shared_briefing=shared_briefing,
            dialect=self.dialect,
            engine_version=self.pipeline._engine_version,
            target_speedup=self.target_speedup,
            previous_retry_result=previous_retry,
        )

        # ── Step 5: Single LLM call ─────────────────────────────────────
        return self._run_sniper(
            generator, retry_prompt, examples,
            snipe_num, t_session,
        )

    def _run_sniper(
        self,
        generator: Any,
        retry_prompt: str,
        examples: List[Dict[str, Any]],
        snipe_num: int,
        t_session: float,
    ) -> Dict[str, Any]:
        """Run self-directed retry worker: generate candidate, validate, save."""
        t_gen = time.time()
        example_ids = [e.get("id", "?") for e in examples]
        snipe_worker_id = 4 + snipe_num  # snipe 1 → worker 5, snipe 2 → worker 6, etc.
        candidate = generator.generate_one(
            sql=self.original_sql,
            prompt=retry_prompt,
            examples_used=example_ids,
            worker_id=snipe_worker_id,
            dialect=self.dialect,
        )
        self._stage(
            self.query_id,
            f"RETRY WORKER: generated ({_fmt_elapsed(time.time() - t_gen)})"
        )

        # Syntax check
        optimized_sql = candidate.optimized_sql
        try:
            import sqlglot
            sqlglot.parse_one(optimized_sql, dialect=self.dialect)
        except Exception:
            optimized_sql = self.original_sql

        # Validate
        self._stage(
            self.query_id,
            f"VALIDATE: Timing... | total {_fmt_elapsed(time.time() - t_session)}"
        )
        t_val = time.time()
        if self._db_reachable is False:
            status, speedup = "NEUTRAL", 1.00
            val_errors, val_error_cat = ["DB unreachable — validation skipped"], None
            self._stage(self.query_id, "VALIDATE: Skipping (DB unreachable)")
        elif self._cached_baseline is not None:
            status, speedup, val_errors, val_error_cat = self.pipeline._validate_against_baseline(
                self._cached_baseline, optimized_sql
            )
        else:
            status, speedup, val_errors, val_error_cat = self.pipeline._validate(
                self.original_sql, optimized_sql
            )

        strategy = f"retry_{snipe_num}"

        # Prepend interface warnings to error messages
        all_errors = list(val_errors or [])
        if candidate.interface_warnings:
            all_errors = [f"INTERFACE: {w}" for w in candidate.interface_warnings] + all_errors

        wr = WorkerResult(
            worker_id=snipe_worker_id,
            strategy=strategy,
            examples_used=example_ids,
            optimized_sql=optimized_sql,
            speedup=speedup,
            status=status,
            transforms=candidate.transforms,
            hint="",
            error_message=" | ".join(all_errors) if all_errors else None,
            error_messages=all_errors,
            error_category=val_error_cat,
        )
        self.all_worker_results.append(wr)
        self._sniper_result = wr  # persist for next retry

        err = f" — {val_errors[0][:60]}" if val_errors else ""
        self._stage(
            self.query_id,
            f"VALIDATE: {status} {speedup:.2f}x ({_fmt_elapsed(time.time() - t_val)}){err}"
        )

        # Learning record
        try:
            lr = self.pipeline.learner.create_learning_record(
                query_id=self.query_id,
                examples_recommended=example_ids,
                transforms_recommended=example_ids,
                status="pass" if status in ("WIN", "IMPROVED", "NEUTRAL") else "error",
                speedup=speedup,
                transforms_used=candidate.transforms,
                error_category=val_error_cat,
                error_messages=val_errors,
            )
            self.pipeline.learner.save_learning_record(lr)
        except Exception as e:
            logger.warning(f"[{self.query_id}] Learning record failed: {e}")

        return {
            "iteration": snipe_num,
            "phase": "snipe",
            "worker_prompt": candidate.prompt,
            "worker_response": candidate.response,
            "worker_results": [wr.to_dict()],
            "best_speedup": speedup,
        }

    def _find_best_worker_sql(self) -> Optional[str]:
        """Find the best worker's optimized SQL (if any > 1.0x)."""
        passing = [w for w in self.all_worker_results if w.speedup > 1.0]
        if not passing:
            return None
        best = max(passing, key=lambda w: w.speedup)
        return best.optimized_sql

    def _checksum_race_verdicts(
        self,
        verdicts: List[tuple],
        optimized_sqls: List[str],
        worker_ids: List[int],
    ) -> List[tuple]:
        """Post-race checksum verification for passing candidates.

        Race mode only checks row counts for speed. This runs a single
        execution of the original + each passing candidate, computes MD5
        checksums, and downgrades mismatches to FAIL.
        """
        # Find indices of passing candidates (WIN, IMPROVED, NEUTRAL)
        passing = [
            i for i, (status, *_) in enumerate(verdicts)
            if status in ("WIN", "IMPROVED", "NEUTRAL")
        ]
        if not passing:
            return verdicts

        try:
            from ..execution.factory import create_executor_from_dsn
            from ..validation.equivalence_checker import EquivalenceChecker

            executor = create_executor_from_dsn(self.pipeline.config.db_path_or_dsn)
            executor.connect()
            checker = EquivalenceChecker()

            # Get original checksum
            orig_rows = executor.execute(self.original_sql)
            orig_checksum = checker.compute_checksum(orig_rows)

            updated = list(verdicts)
            for i in passing:
                cand_rows = executor.execute(optimized_sqls[i])
                cand_checksum = checker.compute_checksum(cand_rows)
                if cand_checksum != orig_checksum:
                    wid = worker_ids[i]
                    self._stage(
                        self.query_id,
                        f"CHECKSUM FAIL: W{wid} checksum {cand_checksum} != {orig_checksum}"
                    )
                    updated[i] = (
                        "FAIL", 0.0,
                        [f"Checksum mismatch: original={orig_checksum}, optimized={cand_checksum}"],
                        "semantic",
                    )

            executor.close()
            return updated
        except Exception as e:
            logger.warning(f"[{self.query_id}] Post-race checksum failed: {e}")
            return verdicts

    def _build_race_timings(self) -> Optional[Dict[str, Any]]:
        """Build race timings dict for snipe analyst prompt.

        Returns None if no race was run (fast queries used sequential validation).
        """
        rr = self._race_result
        if rr is None:
            return None

        candidates = {}
        for cl in rr.candidates:
            candidates[cl.lane_id] = {
                "elapsed_ms": cl.elapsed_ms,
                "row_count": cl.row_count,
                "finished": cl.finished,
                "error": cl.error,
            }

        return {
            "original_ms": rr.original.elapsed_ms,
            "original_rows": rr.original.row_count,
            "has_clear_winner": rr.has_clear_winner,
            "candidates": candidates,
        }

    def _build_worker_prompt(
        self,
        worker_id: int,
        strategy: str,
        examples: List[Dict[str, Any]],
        hint: str,
        dag: Any,
        costs: Dict[str, Any],
        global_learnings: Any = None,
        regression_warnings: List[Dict[str, Any]] = None,
    ) -> str:
        """Build specialized prompt for a single worker.

        Prepends strategy-specific guidance to the standard optimization prompt.
        """
        from ..prompts import build_worker_strategy_header

        base_prompt = self.pipeline.prompter.build_prompt(
            query_id=f"{self.query_id}_w{worker_id}",
            full_sql=self.original_sql,
            dag=dag,
            costs=costs,
            history=None,
            examples=examples,
            expert_analysis=None,
            global_learnings=global_learnings,
            regression_warnings=regression_warnings,
            dialect=self.dialect,
            semantic_intents=self.pipeline.get_semantic_intents(self.query_id),
            engine_version=self.pipeline._engine_version,
        )

        return build_worker_strategy_header(strategy, hint) + base_prompt

    def _best_worker(self) -> Optional[WorkerResult]:
        """Return the worker with the highest speedup across all iterations."""
        if not self.all_worker_results:
            return None
        return max(self.all_worker_results, key=lambda w: w.speedup)

    def _build_result(self) -> SessionResult:
        """Build final SessionResult from all iterations."""
        best = self._best_worker()
        if best is None:
            return SessionResult(
                query_id=self.query_id,
                mode="swarm",
                best_speedup=0.0,
                best_sql=self.original_sql,
                original_sql=self.original_sql,
                best_transforms=[],
                status="ERROR",
                iterations=self.iterations_data,
                n_iterations=len(self.iterations_data),
                n_api_calls=self._count_api_calls(),
            )

        return SessionResult(
            query_id=self.query_id,
            mode="swarm",
            best_speedup=best.speedup,
            best_sql=best.optimized_sql,
            original_sql=self.original_sql,
            best_transforms=best.transforms,
            status=best.status,
            iterations=self.iterations_data,
            n_iterations=len(self.iterations_data),
            n_api_calls=self._count_api_calls(),
        )

    def _count_api_calls(self) -> int:
        """Count total API calls made.

        Counts only LLM calls:
        - Fan-out iteration: 1 analyst + N worker generations
        - Snipe (retry): 1 self-directed worker per iteration
        """
        total = 0
        for it_data in self.iterations_data:
            phase = it_data.get("phase", "")
            if phase == "fan_out":
                total += 1  # analyst
                total += len(it_data.get("worker_results", []))  # workers
            elif phase == "snipe":
                total += 1  # self-directed retry worker
        return total

    def save_session(self, output_dir: Optional[Path] = None) -> Path:
        """Save swarm session artifacts for audit trail.

        Saves to benchmark_dir/swarm_sessions/{query_id}/.
        """
        if output_dir is None:
            output_dir = (
                self.pipeline.benchmark_dir
                / "swarm_sessions"
                / self.query_id
            )
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Session metadata
        best = self._best_worker()
        session_data = {
            "query_id": self.query_id,
            "mode": "swarm",
            "target_speedup": self.target_speedup,
            "max_iterations": self.max_iterations,
            "n_iterations": len(self.iterations_data),
            "best_speedup": best.speedup if best else 0.0,
            "best_worker_id": best.worker_id if best else None,
            "best_strategy": best.strategy if best else None,
            "total_workers": len(self.all_worker_results),
            "n_api_calls": self._count_api_calls(),
            "run_log_path": str(self._run_log_path) if self._run_log_path else None,
        }
        (output_dir / "session.json").write_text(
            json.dumps(session_data, indent=2)
        )

        # Save each iteration
        for it_data in self.iterations_data:
            it_num = it_data.get("iteration", 0)
            phase = it_data.get("phase", "unknown")
            it_dir = output_dir / f"iteration_{it_num:02d}_{phase}"
            it_dir.mkdir(parents=True, exist_ok=True)

            # Analyst prompt/response
            if it_data.get("analyst_prompt"):
                (it_dir / "analyst_prompt.txt").write_text(it_data["analyst_prompt"])
            if it_data.get("analyst_response"):
                (it_dir / "analyst_response.txt").write_text(it_data["analyst_response"])

            # Fan-out worker prompts (dict of wid -> (prompt, response))
            worker_prompts = it_data.get("worker_prompts", {})

            # Snipe worker prompt/response (single worker)
            if it_data.get("worker_prompt"):
                (it_dir / "worker_prompt.txt").write_text(it_data["worker_prompt"])
            if it_data.get("worker_response"):
                (it_dir / "worker_response.txt").write_text(it_data["worker_response"])

            # Worker results
            for wr_data in it_data.get("worker_results", []):
                wid = wr_data.get("worker_id", 0)
                w_dir = it_dir / f"worker_{wid:02d}"
                w_dir.mkdir(parents=True, exist_ok=True)

                (w_dir / "result.json").write_text(json.dumps(wr_data, indent=2))
                if wr_data.get("optimized_sql"):
                    (w_dir / "optimized.sql").write_text(wr_data["optimized_sql"])

                # Save worker prompt and LLM response
                if wid in worker_prompts:
                    w_prompt, w_response = worker_prompts[wid]
                    if w_prompt:
                        (w_dir / "prompt.txt").write_text(w_prompt)
                    if w_response:
                        (w_dir / "response.txt").write_text(w_response)

        logger.info(
            f"Saved swarm session: {output_dir} "
            f"({len(self.iterations_data)} iterations, "
            f"{len(self.all_worker_results)} total workers)"
        )
        return output_dir

    def save_to_run_dir(self, run_dir: Path) -> Path:
        """Save session artifacts in standardized runs/ layout.

        Layout: run_dir/{query_id}/
            ├── original.sql
            ├── assignments.json
            ├── worker_{N}_prompt.txt
            ├── worker_{N}_response.txt
            ├── worker_{N}_sql.sql
            ├── snipe_{N}_prompt.txt   (if snipe iterations)
            ├── snipe_{N}_response.txt
            ├── snipe_{N}_sql.sql
            └── validation.json
        """
        query_dir = Path(run_dir) / self.query_id
        query_dir.mkdir(parents=True, exist_ok=True)

        # Original SQL
        (query_dir / "original.sql").write_text(self.original_sql)

        # Process each iteration
        for it_data in self.iterations_data:
            phase = it_data.get("phase", "unknown")
            it_num = it_data.get("iteration", 0)
            worker_prompts = it_data.get("worker_prompts", {})

            if phase == "fan_out":
                # Save assignments from worker results
                assignments = []
                for wr_data in it_data.get("worker_results", []):
                    wid = wr_data.get("worker_id", 0)
                    assignments.append({
                        "worker_id": wid,
                        "strategy": wr_data.get("strategy", ""),
                        "examples_used": wr_data.get("examples_used", []),
                    })
                (query_dir / "assignments.json").write_text(
                    json.dumps(assignments, indent=2)
                )

                # Worker prompts/responses/SQL
                for wr_data in it_data.get("worker_results", []):
                    wid = wr_data.get("worker_id", 0)
                    if wid in worker_prompts:
                        w_prompt, w_response = worker_prompts[wid]
                        if w_prompt:
                            (query_dir / f"worker_{wid}_prompt.txt").write_text(w_prompt)
                        if w_response:
                            (query_dir / f"worker_{wid}_response.txt").write_text(w_response)
                    if wr_data.get("optimized_sql"):
                        (query_dir / f"worker_{wid}_sql.sql").write_text(wr_data["optimized_sql"])

            elif phase in ("snipe", "snipe_retry"):
                # Snipe prompt/response/SQL
                if it_data.get("worker_prompt"):
                    (query_dir / f"snipe_{it_num}_prompt.txt").write_text(it_data["worker_prompt"])
                if it_data.get("worker_response"):
                    (query_dir / f"snipe_{it_num}_response.txt").write_text(it_data["worker_response"])
                for wr_data in it_data.get("worker_results", []):
                    if wr_data.get("optimized_sql"):
                        (query_dir / f"snipe_{it_num}_sql.sql").write_text(wr_data["optimized_sql"])

        # Validation summary
        best = self._best_worker()
        validation = {
            "query_id": self.query_id,
            "status": best.status if best else "ERROR",
            "speedup": best.speedup if best else 0.0,
            "best_worker_id": best.worker_id if best else None,
            "best_strategy": best.strategy if best else None,
            "transforms": best.transforms if best else [],
            "n_api_calls": self._count_api_calls(),
            "all_workers": [wr.to_dict() for wr in self.all_worker_results],
        }
        (query_dir / "validation.json").write_text(
            json.dumps(validation, indent=2)
        )

        return query_dir

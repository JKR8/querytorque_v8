"""Swarm optimization session — multi-worker fan-out with snipe refinement.

Workflow:
1. Fan-out: Analyst distributes top 12 FAISS examples across 4 workers
   (3 each, no duplicates). Each worker gets a different strategy.
2. Validate all 4 candidates. If any >= target_speedup, done.
3. Snipe: If all fail, analyst synthesizes failures into 1 refined worker.
4. Iterate snipe (not fan-out) up to max_iterations.

Fan-out only happens ONCE (iteration 1). Subsequent iterations are snipes.
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

            # Phase 1: Parse DAG once (shared across all iterations)
            self._stage(self.query_id, "PARSE: Building DAG...")
            t0 = time.time()
            dag, costs, _explain = self.pipeline._parse_dag(
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

            # Iterations 2-N: Snipe phase
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

    def _fan_out_iteration(
        self, dag: Any, costs: Dict[str, Any], t_session: float,
    ) -> Dict[str, Any]:
        """Iteration 1: Generate 4 specialized workers with different strategies.

        1. Get top 12 FAISS examples
        2. Call analyst to distribute examples + strategies
        3. Parse 4 worker assignments
        4. Generate 4 parallel workers with different prompts
        5. Validate all 4 candidates
        """
        from ..prompts import build_fan_out_prompt, parse_fan_out_response

        # FAISS retrieval
        self._stage(self.query_id, "FAISS: Retrieving examples...")
        t0 = time.time()
        faiss_examples = self.pipeline._find_examples(
            self.original_sql, engine=self.engine, k=12
        )
        all_available = self.pipeline._list_gold_examples(self.engine)
        self._stage(self.query_id, f"FAISS: {len(faiss_examples)} examples ({_fmt_elapsed(time.time() - t0)})")

        # Build fan-out prompt
        fan_out_prompt = build_fan_out_prompt(
            query_id=self.query_id,
            sql=self.original_sql,
            dag=dag,
            costs=costs,
            faiss_examples=faiss_examples,
            all_available_examples=all_available,
            dialect=self.dialect,
        )

        # Analyst call
        self._stage(self.query_id, "ANALYST: Distributing strategies to 4 workers...")
        t0 = time.time()
        from ..generate import CandidateGenerator
        generator = CandidateGenerator(
            provider=self.pipeline.provider,
            model=self.pipeline.model,
            analyze_fn=self.pipeline.analyze_fn,
        )

        try:
            analyst_response = generator._analyze(fan_out_prompt)
        except Exception as e:
            self._stage(self.query_id, f"ANALYST: failed ({e}), using fallback")
            analyst_response = self._build_fallback_fan_out(faiss_examples, all_available)

        # Parse worker assignments
        assignments = parse_fan_out_response(analyst_response)
        strategies = [f"W{a.worker_id}={a.strategy}" for a in assignments]
        self._stage(self.query_id, f"ANALYST: {len(assignments)} workers assigned ({_fmt_elapsed(time.time() - t0)})")
        for a in assignments:
            self._stage(self.query_id, f"  W{a.worker_id}: {a.strategy}")

        # Regression warnings (shared across all workers)
        regression_warnings = self.pipeline._find_regression_warnings(
            self.original_sql, engine=self.engine, k=2
        )

        # Global learnings
        global_learnings = self.pipeline.learner.build_learning_summary() or None

        # Step 1: Generate 4 candidates in PARALLEL (LLM calls)
        self._stage(self.query_id, f"GENERATE: 4 workers in parallel... | total {_fmt_elapsed(time.time() - t_session)}")
        t_gen = time.time()

        candidates_by_assignment = {}

        def generate_worker(assignment):
            """Generate a single worker's candidate (LLM call only, no validation)."""
            examples = self.pipeline._load_examples_by_id(
                assignment.examples, self.engine
            )

            prompt = self._build_worker_prompt(
                worker_id=assignment.worker_id,
                strategy=assignment.strategy,
                examples=examples,
                hint=assignment.hint,
                dag=dag,
                costs=costs,
                global_learnings=global_learnings,
                regression_warnings=regression_warnings,
            )

            example_ids = [e.get("id", "?") for e in examples]
            candidate = generator.generate_one(
                sql=self.original_sql,
                prompt=prompt,
                examples_used=example_ids,
                worker_id=assignment.worker_id,
                dialect=self.dialect,
            )

            # Syntax check
            optimized_sql = candidate.optimized_sql
            try:
                import sqlglot
                sqlglot.parse_one(optimized_sql, dialect=self.dialect)
            except Exception:
                optimized_sql = self.original_sql

            return assignment, optimized_sql, candidate.transforms, candidate.prompt, candidate.response

        # Parallel LLM generation
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {
                pool.submit(generate_worker, a): a for a in assignments
            }
            for future in as_completed(futures):
                try:
                    assignment, optimized_sql, transforms, w_prompt, w_response = future.result()
                    candidates_by_assignment[assignment.worker_id] = (
                        assignment, optimized_sql, transforms, w_prompt, w_response
                    )
                    self._stage(
                        self.query_id,
                        f"GENERATE: W{assignment.worker_id} ({assignment.strategy}) ready "
                        f"({_fmt_elapsed(time.time() - t_gen)})"
                    )
                except Exception as e:
                    assignment = futures[future]
                    logger.error(
                        f"[{self.query_id}] W{assignment.worker_id} generation failed: {e}"
                    )
                    candidates_by_assignment[assignment.worker_id] = (
                        assignment, self.original_sql, [], "", str(e)
                    )

        self._stage(self.query_id, f"GENERATE: all 4 workers complete ({_fmt_elapsed(time.time() - t_gen)})")

        # Step 2: Batch validation
        self._stage(self.query_id, f"VALIDATE: Timing original + 4 candidates... | total {_fmt_elapsed(time.time() - t_session)}")
        t_val = time.time()

        sorted_wids = sorted(candidates_by_assignment.keys())
        optimized_sqls = [
            candidates_by_assignment[wid][1] for wid in sorted_wids
        ]

        batch_results = self.pipeline._validate_batch(
            self.original_sql, optimized_sqls
        )

        worker_results = []
        worker_prompts = {}  # wid -> (prompt, response)
        for wid, (status, speedup, error_msgs, error_cat) in zip(sorted_wids, batch_results):
            assignment, optimized_sql, transforms, w_prompt, w_response = candidates_by_assignment[wid]
            worker_prompts[wid] = (w_prompt, w_response)

            wr = WorkerResult(
                worker_id=assignment.worker_id,
                strategy=assignment.strategy,
                examples_used=assignment.examples,
                optimized_sql=optimized_sql,
                speedup=speedup,
                status=status,
                transforms=transforms,
                hint=assignment.hint,
                error_message=" | ".join(error_msgs) if error_msgs else None,
                error_messages=error_msgs or [],
                error_category=error_cat,
            )
            worker_results.append(wr)
            self.all_worker_results.append(wr)

        # Print results table
        self._stage(self.query_id, f"VALIDATE: complete ({_fmt_elapsed(time.time() - t_val)})")
        for wr in sorted(worker_results, key=lambda w: w.worker_id):
            marker = "*" if wr.speedup >= 1.10 else " "
            err = f" — {wr.error_message[:60]}" if wr.error_message else ""
            self._stage(
                self.query_id,
                f" {marker} W{wr.worker_id} ({wr.strategy}): {wr.status} {wr.speedup:.2f}x{err}"
            )

        # Learning records for each worker
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

        # Sort by worker_id for consistency
        worker_results.sort(key=lambda w: w.worker_id)

        return {
            "iteration": 0,
            "phase": "fan_out",
            "analyst_prompt": fan_out_prompt,
            "analyst_response": analyst_response,
            "worker_prompts": worker_prompts,
            "worker_results": [wr.to_dict() for wr in worker_results],
            "best_speedup": max((wr.speedup for wr in worker_results), default=0.0),
        }

    def _snipe_iteration(
        self, dag: Any, costs: Dict[str, Any], snipe_num: int, t_session: float,
    ) -> Dict[str, Any]:
        """Snipe iteration: analyst synthesizes failures into 1 refined worker.

        1. Build snipe prompt with all previous worker results
        2. Parse refined strategy
        3. Generate single refined worker
        4. Validate
        """
        from ..prompts import build_snipe_prompt, parse_snipe_response

        # Full catalog
        all_available = self.pipeline._list_gold_examples(self.engine)

        # Build snipe prompt
        snipe_prompt = build_snipe_prompt(
            query_id=self.query_id,
            original_sql=self.original_sql,
            worker_results=self.all_worker_results,
            target_speedup=self.target_speedup,
            dag=dag,
            costs=costs,
            all_available_examples=all_available,
            dialect=self.dialect,
        )

        # Analyst call
        self._stage(self.query_id, f"ANALYST: Synthesizing snipe strategy... | total {_fmt_elapsed(time.time() - t_session)}")
        t0 = time.time()
        from ..generate import CandidateGenerator
        generator = CandidateGenerator(
            provider=self.pipeline.provider,
            model=self.pipeline.model,
            analyze_fn=self.pipeline.analyze_fn,
        )

        try:
            analyst_response = generator._analyze(snipe_prompt)
        except Exception as e:
            self._stage(self.query_id, f"ANALYST: snipe failed ({e})")
            return {
                "iteration": snipe_num,
                "phase": "snipe",
                "error": str(e),
                "worker_results": [],
                "best_speedup": 0.0,
            }

        # Parse refined strategy
        analysis = parse_snipe_response(analyst_response)
        self._stage(self.query_id, f"ANALYST: done ({_fmt_elapsed(time.time() - t0)})")
        self._stage(self.query_id, f"  Strategy: {analysis.refined_strategy[:80] if analysis.refined_strategy else '?'}")

        # Load examples
        examples = self.pipeline._load_examples_by_id(
            analysis.examples, self.engine
        )

        # Regression warnings
        regression_warnings = self.pipeline._find_regression_warnings(
            self.original_sql, engine=self.engine, k=2
        )

        # Global learnings
        global_learnings = self.pipeline.learner.build_learning_summary() or None

        # Build refined worker prompt
        prompt = self._build_worker_prompt(
            worker_id=1,
            strategy=analysis.refined_strategy[:80] if analysis.refined_strategy else "refined_snipe",
            examples=examples,
            hint=analysis.hint,
            dag=dag,
            costs=costs,
            global_learnings=global_learnings,
            regression_warnings=regression_warnings,
        )

        # Generate single candidate
        self._stage(self.query_id, f"GENERATE: Snipe worker... | total {_fmt_elapsed(time.time() - t_session)}")
        t_gen = time.time()
        example_ids = [e.get("id", "?") for e in examples]
        candidate = generator.generate_one(
            sql=self.original_sql,
            prompt=prompt,
            examples_used=example_ids,
            worker_id=1,
            dialect=self.dialect,
        )
        snipe_worker_prompt = candidate.prompt
        snipe_worker_response = candidate.response
        self._stage(self.query_id, f"GENERATE: done ({_fmt_elapsed(time.time() - t_gen)})")

        # Syntax check
        optimized_sql = candidate.optimized_sql
        try:
            import sqlglot
            sqlglot.parse_one(optimized_sql, dialect=self.dialect)
        except Exception:
            optimized_sql = self.original_sql

        # Validate against original
        self._stage(self.query_id, f"VALIDATE: Timing... | total {_fmt_elapsed(time.time() - t_session)}")
        t_val = time.time()
        status, speedup, val_errors, val_error_cat = self.pipeline._validate(self.original_sql, optimized_sql)

        wr = WorkerResult(
            worker_id=1,
            strategy=f"snipe_{snipe_num}",
            examples_used=analysis.examples,
            optimized_sql=optimized_sql,
            speedup=speedup,
            status=status,
            transforms=candidate.transforms,
            hint=analysis.hint,
            error_message=" | ".join(val_errors) if val_errors else None,
            error_messages=val_errors or [],
            error_category=val_error_cat,
        )
        self.all_worker_results.append(wr)

        err = f" — {val_errors[0][:60]}" if val_errors else ""
        self._stage(self.query_id, f"VALIDATE: {status} {speedup:.2f}x ({_fmt_elapsed(time.time() - t_val)}){err}")

        # Learning record
        try:
            lr = self.pipeline.learner.create_learning_record(
                query_id=self.query_id,
                examples_recommended=analysis.examples,
                transforms_recommended=analysis.examples,
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
            "analyst_prompt": snipe_prompt,
            "analyst_response": analyst_response,
            "worker_prompt": snipe_worker_prompt,
            "worker_response": snipe_worker_response,
            "failure_analysis": analysis.failure_analysis,
            "unexplored": analysis.unexplored,
            "refined_strategy": analysis.refined_strategy,
            "worker_results": [wr.to_dict()],
            "best_speedup": speedup,
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
        - Snipe iteration: 1 analyst + 1 worker generation
        """
        total = 0
        for it_data in self.iterations_data:
            if it_data.get("phase") == "fan_out":
                total += 1  # analyst
                total += len(it_data.get("worker_results", []))  # workers
            elif it_data.get("phase") == "snipe":
                total += 1  # analyst
                total += 1  # worker
        return total

    @staticmethod
    def _build_fallback_fan_out(
        faiss_examples: List[Dict[str, Any]],
        all_available: List[Dict[str, str]],
    ) -> str:
        """Build fallback fan-out response when analyst call fails.

        Distributes FAISS examples evenly across 4 workers with generic strategies.
        """
        # Get example IDs
        ex_ids = [e.get("id", f"ex_{i}") for i, e in enumerate(faiss_examples)]

        # Pad with catalog examples if needed
        catalog_ids = [e.get("id", "") for e in all_available if e.get("id")]
        for cid in catalog_ids:
            if cid not in ex_ids:
                ex_ids.append(cid)
            if len(ex_ids) >= 12:
                break

        # Distribute across 4 workers
        strategies = [
            ("conservative_pushdown", "Apply proven pushdown and early filter patterns."),
            ("date_cte_isolation", "Isolate date dimensions into CTEs for hash join reduction."),
            ("multi_cte_restructure", "Restructure query with multiple CTEs for prefetching."),
            ("structural_transform", "Apply structural transforms like OR-to-UNION or decorrelation."),
        ]

        lines = []
        for i, (strategy, hint) in enumerate(strategies):
            start = i * 3
            worker_examples = ex_ids[start:start + 3]
            if not worker_examples:
                worker_examples = ex_ids[:3]  # reuse if not enough

            lines.append(f"WORKER_{i + 1}:")
            lines.append(f"STRATEGY: {strategy}")
            lines.append(f"EXAMPLES: {', '.join(worker_examples)}")
            lines.append(f"HINT: {hint}")
            lines.append("")

        return "\n".join(lines)

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

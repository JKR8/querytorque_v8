"""Iterative single-query deep-dive optimization (Analyst Mode).

The AnalystSession runs an iterative loop on a single query:
1. Always optimize from the ORIGINAL SQL (no compounding)
2. Generate N candidates via parallel workers
3. Validate the best candidate against the TRUE ORIGINAL
4. If speedup < target → generate LLM failure analysis explaining
   why it failed and what to try next
5. History includes all previous failure analyses so the LLM learns
   and tries different strategies each iteration
6. Stop when: target reached OR max iterations exhausted

Usage:
    from qt_sql.pipeline import Pipeline
    p = Pipeline("ado/benchmarks/duckdb_tpcds")
    session = p.run_analyst_session("query_88", sql, max_iterations=3)
    print(f"Best: {session.best_speedup:.2f}x")
    print(session.best_sql)
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .pipeline import Pipeline

from .session_logging import attach_session_handler, detach_session_handler

logger = logging.getLogger(__name__)


@dataclass
class AnalystIteration:
    """One iteration of the analyst deep-dive loop."""
    iteration: int
    original_sql: str        # TRUE original — never changes
    optimized_sql: str       # This iteration's rewrite
    status: str = ""         # WIN | IMPROVED | NEUTRAL | REGRESSION | ERROR | FAIL
    speedup: float = 0.0    # vs true original
    transforms: List[str] = field(default_factory=list)
    prompt: str = ""         # The prompt used
    analysis: Optional[str] = None  # LLM analyst output
    analysis_prompt: Optional[str] = None  # Prompt sent to analyst LLM
    rewrite_response: Optional[str] = None  # Raw rewrite LLM response
    examples_used: List[str] = field(default_factory=list)
    failure_analysis: Optional[str] = None  # LLM-generated why/what-next
    error_messages: List[str] = field(default_factory=list)  # Raw validation errors
    error_category: Optional[str] = None  # syntax|semantic|timeout|execution|unknown


class AnalystSession:
    """Iterative single-query deep-dive optimization.

    Each iteration:
    1. Always optimizes from ORIGINAL SQL (no compounding)
    2. Builds full history from ALL previous iterations
    3. Runs LLM analyst for structural guidance
    4. Generates N candidates and validates against TRUE original
    5. If speedup < target, generates LLM failure analysis
    6. History includes previous failure analyses so LLM learns

    Stops when:
    - Target speedup is reached
    - Max iterations exhausted
    """

    FINAL_THRESHOLD = 1.05     # 5% minimum for final result

    def __init__(
        self,
        pipeline: "Pipeline",
        query_id: str,
        original_sql: str,
        max_iterations: int = 3,
        target_speedup: float = 2.0,
        n_workers: int = 3,
    ):
        self.pipeline = pipeline
        self.query_id = query_id
        self.original_sql = original_sql       # TRUE original — never changes
        self.max_iterations = max_iterations
        self.target_speedup = target_speedup
        self.n_workers = n_workers
        self.iterations: List[AnalystIteration] = []
        self.best_speedup = 1.0
        self.best_sql = original_sql
        self._run_log_path: Optional[Path] = None
        self._run_log_handler: Optional[logging.Handler] = None

    @staticmethod
    def _stage(query_id: str, msg: str):
        """Print a clear stage banner to console."""
        print(f"  [{query_id}] {msg}", flush=True)
        logger.info(f"[{query_id}] {msg}")

    def _setup_run_logging(self, session_dir: Path) -> Path:
        """Attach a per-run file handler so analyst sessions keep a log file."""
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
        logger.info(f"[{self.query_id}] Expert run log: {log_path}")
        return log_path

    def _teardown_run_logging(self) -> None:
        """Detach and close the per-run file handler."""
        if self._run_log_handler is None:
            return
        detach_session_handler(self._run_log_handler)
        self._run_log_handler.close()
        self._run_log_handler = None

    def run(self) -> Optional[AnalystIteration]:
        """Run the full iterative loop. Returns best iteration (if >=5% speedup)."""
        session_dir = (
            self.pipeline.benchmark_dir / "analyst_sessions" / self.query_id
        )
        self._setup_run_logging(session_dir)
        session_start = time.time()
        try:
            print(f"\n{'='*60}", flush=True)
            print(f"  EXPERT SESSION: {self.query_id}", flush=True)
            print(f"  max_iterations={self.max_iterations}  target={self.target_speedup:.1f}x  workers={self.n_workers}", flush=True)
            print(f"{'='*60}", flush=True)

            for i in range(self.max_iterations):
                print(f"\n--- Iteration {i + 1}/{self.max_iterations} ---", flush=True)

                iteration = self._run_iteration(i)
                self.iterations.append(iteration)

                # Track best result
                if iteration.speedup > self.best_speedup:
                    self.best_speedup = iteration.speedup
                    self.best_sql = iteration.optimized_sql

                # Generate failure analysis if < target
                if iteration.speedup < self.target_speedup:
                    self._stage(self.query_id, f"Phase 7: LLM failure analysis ({iteration.speedup:.2f}x < {self.target_speedup:.1f}x target)...")
                    t_fa = time.time()
                    iteration.failure_analysis = self._generate_failure_analysis(iteration)
                    self._stage(self.query_id, f"  Failure analysis done ({time.time() - t_fa:.0f}s)")

                # Stop if target reached
                if self.best_speedup >= self.target_speedup:
                    self._stage(self.query_id, f"Target {self.target_speedup:.1f}x reached!")
                    break

            best = self._best_iteration()
            total_time = time.time() - session_start
            print(f"\n{'='*60}", flush=True)
            if best:
                print(f"  DONE: {self.query_id} — {best.status} {best.speedup:.2f}x (iter {best.iteration + 1})", flush=True)
            else:
                print(f"  DONE: {self.query_id} — no improvement", flush=True)
            print(f"  elapsed={total_time:.1f}s", flush=True)
            print(f"{'='*60}\n", flush=True)

            return best
        finally:
            if self._run_log_path:
                logger.info(f"[{self.query_id}] Expert log file saved: {self._run_log_path}")
            self._teardown_run_logging()

    def _run_iteration(self, iteration_num: int) -> AnalystIteration:
        """Single iteration: gather context -> analyst briefing -> worker rewrite -> validate.

        Uses the same V2 analyst briefing template as swarm mode (mode="expert"),
        ensuring the expert path gets the same rich context (EXPLAIN, engine
        profiles, constraints, examples, etc.).

        Always optimizes from the original SQL.
        Validates against the TRUE original_sql for consistent speedup measurement.
        """
        from .prompts import (
            build_analyst_briefing_prompt,
            build_worker_prompt,
            parse_briefing_response,
        )

        dialect = (
            self.pipeline.config.engine
            if self.pipeline.config.engine != "postgresql"
            else "postgres"
        )
        engine = (
            "postgres"
            if self.pipeline.config.engine in ("postgresql", "postgres")
            else self.pipeline.config.engine
        )

        input_sql = self.original_sql  # Always from original

        # Phase 1: Parse DAG from original
        self._stage(self.query_id, "Phase 1: Parsing DAG...")
        dag, costs, _explain = self.pipeline._parse_dag(input_sql, dialect=dialect, query_id=self.query_id)
        self._stage(self.query_id, f"  DAG: {len(dag.nodes)} nodes, {len(dag.edges)} edges")

        # Phase 2: Gather all context (same as swarm — EXPLAIN, examples, constraints, etc.)
        self._stage(self.query_id, "Phase 2: Gathering context...")
        t_ctx = time.time()
        ctx = self.pipeline.gather_analyst_context(
            query_id=self.query_id,
            sql=input_sql,
            dialect=dialect,
            engine=engine,
        )
        self._stage(
            self.query_id,
            f"  Context ready — {len(ctx['matched_examples'])} examples, "
            f"{len(ctx['constraints'])} constraints "
            f"({time.time() - t_ctx:.0f}s)"
        )

        # Phase 3: Build analyst briefing (mode="expert" → single worker)
        self._stage(self.query_id, "Phase 3: LLM analyst (structured briefing)...")
        t_analyst = time.time()

        analyst_prompt = build_analyst_briefing_prompt(
            query_id=self.query_id,
            sql=input_sql,
            explain_plan_text=ctx["explain_plan_text"],
            dag=dag,
            costs=costs,
            semantic_intents=ctx["semantic_intents"],
            global_knowledge=ctx["global_knowledge"],
            matched_examples=ctx["matched_examples"],
            all_available_examples=ctx["all_available_examples"],
            constraints=ctx["constraints"],
            regression_warnings=ctx["regression_warnings"],
            dialect=dialect,
            strategy_leaderboard=ctx["strategy_leaderboard"],
            query_archetype=ctx["query_archetype"],
            engine_profile=ctx["engine_profile"],
            resource_envelope=ctx["resource_envelope"],
            exploit_algorithm_text=ctx["exploit_algorithm_text"],
            plan_scanner_text=ctx["plan_scanner_text"],
            iteration_history=self._build_iteration_history(),
            mode="expert",
        )

        from .generate import CandidateGenerator
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
            self._stage(self.query_id, f"  Analyst failed ({e}), using minimal briefing")
            analyst_response = (
                "=== SHARED BRIEFING ===\n"
                "SEMANTIC_CONTRACT: Preserve exact output.\n"
                "BOTTLENECK_DIAGNOSIS: Analysis unavailable.\n"
                "ACTIVE_CONSTRAINTS: Preserve semantic equivalence.\n"
                "REGRESSION_WARNINGS: None applicable.\n\n"
                "=== WORKER 1 BRIEFING ===\n"
                "STRATEGY: conservative_pushdown\n"
                "EXAMPLES: early_filter\n"
                "HAZARD_FLAGS: Analyst unavailable.\n"
            )

        self._stage(self.query_id, f"  Analyst done ({time.time() - t_analyst:.0f}s)")

        # Phase 4: Parse briefing and build worker prompt
        briefing = parse_briefing_response(analyst_response)

        # Load examples for the worker — validate the parsed worker has real
        # content. parse_briefing_response pads empty workers on parse failure
        # (strategy="") and assigns placeholder strategies (strategy="strategy_1")
        # when extraction partially fails. Both must trigger V1 fallback.
        worker = briefing.workers[0] if briefing.workers else None
        if worker:
            import re
            strategy = worker.strategy.strip()
            if not strategy or re.fullmatch(r"strategy_\d+", strategy):
                self._stage(self.query_id, "  Briefing parse returned empty/placeholder worker, falling back to example-based prompt")
                worker = None
        if worker:
            examples = self.pipeline._load_examples_by_id(
                worker.examples, engine
            )
        else:
            examples = self.pipeline._find_examples(input_sql, engine=engine, k=3)

        example_ids = [e.get("id", "?") for e in examples]

        # Extract output columns for column completeness contract
        from .node_prompter import Prompter
        output_columns = Prompter._extract_output_columns(dag)

        # Build worker prompt (same as swarm workers use)
        prompt = build_worker_prompt(
            worker_briefing=worker if worker else briefing.workers[0],
            shared_briefing=briefing.shared,
            examples=examples,
            original_sql=input_sql,
            output_columns=output_columns,
            dialect=dialect,
            engine_version=self.pipeline._engine_version,
            resource_envelope=ctx["resource_envelope"],
        )

        # Phase 5: Generate rewrite
        self._stage(self.query_id, "Phase 5: LLM rewrite (generating candidate)...")
        t_rewrite = time.time()

        candidate = generator.generate_one(
            sql=input_sql,
            prompt=prompt,
            examples_used=example_ids,
            worker_id=1,
            dialect=dialect,
        )

        optimized_sql = candidate.optimized_sql
        transforms = candidate.transforms
        rewrite_response = candidate.response

        self._stage(self.query_id, f"  Rewrite done ({time.time() - t_rewrite:.0f}s), transforms: {transforms or ['none']}")

        # Phase 6: Syntax check
        self._stage(self.query_id, "Phase 6: Syntax check...")
        try:
            import sqlglot
            sqlglot.parse_one(optimized_sql, dialect=dialect)
            self._stage(self.query_id, "  Syntax: OK")
        except Exception as e:
            self._stage(self.query_id, f"  Syntax: FAILED — {e}")
            optimized_sql = input_sql

        # Phase 7: Validate against TRUE ORIGINAL
        self._stage(self.query_id, "Phase 7: Validating (semantic + timing)...")
        t_val = time.time()
        status, speedup, val_errors, val_error_cat = self.pipeline._validate(self.original_sql, optimized_sql)
        self._stage(self.query_id, f"  Result: {status} ({speedup:.2f}x) — {time.time() - t_val:.0f}s")
        if val_errors:
            for err in val_errors[:3]:
                self._stage(self.query_id, f"  Error [{val_error_cat}]: {err[:150]}")

        # Create learning record for this iteration
        try:
            lr = self.pipeline.learner.create_learning_record(
                query_id=self.query_id,
                examples_recommended=example_ids,
                transforms_recommended=example_ids,
                status="pass" if status in ("WIN", "IMPROVED", "NEUTRAL") else "error",
                speedup=speedup,
                transforms_used=transforms,
                worker_id=0,
                attempt_number=iteration_num + 1,
                error_category=val_error_cat,
                error_messages=val_errors,
            )
            self.pipeline.learner.save_learning_record(lr)
        except Exception as e:
            logger.warning(
                f"[{self.query_id}] Learning record failed: {e}"
            )

        return AnalystIteration(
            iteration=iteration_num,
            original_sql=self.original_sql,
            optimized_sql=optimized_sql,
            status=status,
            speedup=speedup,
            transforms=transforms,
            prompt=prompt,
            analysis=analyst_response,
            analysis_prompt=analyst_prompt,
            rewrite_response=rewrite_response,
            examples_used=example_ids,
            error_messages=val_errors,
            error_category=val_error_cat,
        )

    def _generate_failure_analysis(self, iteration: AnalystIteration) -> str:
        """Generate LLM analysis of why attempt failed to reach target.

        Calls the analyst LLM with a specialized prompt asking:
        1. What went wrong?
        2. Why was speedup insufficient?
        3. What should the NEXT attempt try?
        4. What constraints did we learn?
        """
        from .analyst import build_failure_analysis_prompt

        dialect = (
            self.pipeline.config.engine
            if self.pipeline.config.engine != "postgresql"
            else "postgres"
        )

        # Parse DAGs for comparison
        dag_original, costs_original, _ = self.pipeline._parse_dag(
            self.original_sql, dialect=dialect, query_id=self.query_id
        )
        dag_attempted, costs_attempted, _ = self.pipeline._parse_dag(
            iteration.optimized_sql, dialect=dialect, query_id=self.query_id
        )

        # Build failure analysis prompt
        prompt = build_failure_analysis_prompt(
            query_id=self.query_id,
            original_sql=self.original_sql,
            attempted_sql=iteration.optimized_sql,
            target_speedup=self.target_speedup,
            actual_speedup=iteration.speedup,
            status=iteration.status,
            transforms=iteration.transforms,
            dag_original=dag_original,
            costs_original=costs_original,
            dag_attempted=dag_attempted,
            costs_attempted=costs_attempted,
            previous_attempts=self.iterations[:-1],  # All except current
            dialect=dialect,
        )

        # Call LLM
        try:
            from .generate import CandidateGenerator
            generator = CandidateGenerator(
                provider=self.pipeline.provider,
                model=self.pipeline.model,
                analyze_fn=self.pipeline.analyze_fn,
            )
            analysis = generator._analyze(prompt)
            logger.info(
                f"[{self.query_id}] Failure analysis: {len(analysis)} chars"
            )
            return analysis
        except Exception as e:
            logger.warning(
                f"[{self.query_id}] Failure analysis LLM call failed: {e}"
            )
            return (
                f"Failure analysis unavailable (LLM error: {e}). "
                f"Speedup was {iteration.speedup:.2f}x vs target "
                f"{self.target_speedup:.2f}x."
            )

    def _build_iteration_history(self) -> Optional[Dict[str, Any]]:
        """Build history dict from all previous iterations.

        Includes BOTH:
        1. Prior batch pipeline results from state_N/validation/ and leaderboard
        2. Within-session analyst iterations

        This ensures the first analyst iteration sees all prior batch results
        (e.g., state_0 regressions) instead of starting with no history.
        """
        attempts = []

        # Load prior batch results from state_N/validation/ directories
        benchmark_dir = self.pipeline.benchmark_dir
        for state_dir in sorted(benchmark_dir.glob("state_*")):
            if not state_dir.is_dir():
                continue
            state_num = state_dir.name.split("_")[-1]
            val_dir = state_dir / "validation"
            if not val_dir.exists():
                continue
            # Try both naming conventions (q51 vs query_51)
            for variant in [self.query_id, self.query_id.replace("query_", "q"),
                            self.query_id.replace("q", "query_")]:
                val_path = val_dir / f"{variant}.json"
                if val_path.exists():
                    try:
                        data = json.loads(val_path.read_text())
                        attempts.append({
                            "state": int(state_num),
                            "source": f"state_{state_num}",
                            "status": data.get("status", "unknown"),
                            "speedup": data.get("speedup", 0),
                            "transforms": data.get("transforms_applied", []),
                            "original_sql": data.get("original_sql", ""),
                            "optimized_sql": data.get("optimized_sql", ""),
                        })
                    except Exception:
                        pass
                    break

        # Load from leaderboard (may have analyst_mode results from prior sessions)
        lb_path = benchmark_dir / "leaderboard.json"
        if lb_path.exists():
            try:
                lb = json.loads(lb_path.read_text())
                queries = lb.get("queries", lb) if isinstance(lb, dict) else lb
                if isinstance(queries, list):
                    existing_sources = {a.get("source") for a in attempts}
                    for q in queries:
                        qid = q.get("query_id", "")
                        if qid in (self.query_id,
                                   self.query_id.replace("query_", "q"),
                                   self.query_id.replace("q", "query_")):
                            source = q.get("source", "state_0")
                            if source not in existing_sources:
                                attempts.append({
                                    "source": source,
                                    "status": q.get("status", "unknown"),
                                    "speedup": q.get("speedup", 0),
                                    "transforms": q.get("transforms", []),
                                })
            except Exception:
                pass

        # Add within-session analyst iterations
        for it in self.iterations:
            attempts.append({
                "state": it.iteration,
                "source": f"analyst_iter_{it.iteration}",
                "status": it.status,
                "speedup": it.speedup,
                "transforms": it.transforms,
                "original_sql": it.original_sql,
                "optimized_sql": it.optimized_sql,
                "failure_analysis": it.failure_analysis,
                "error_messages": it.error_messages,
                "error_category": it.error_category,
            })

        if not attempts:
            return None

        history = {"attempts": attempts, "promotion": None}

        # If we have a good result, add context for the next iteration
        best = self._best_iteration()
        if best and best.speedup >= 1.05:
            from .schemas import PromotionAnalysis
            history["promotion"] = PromotionAnalysis(
                query_id=self.query_id,
                original_sql=self.original_sql,
                optimized_sql=best.optimized_sql,
                speedup=best.speedup,
                transforms=best.transforms,
                analysis=(
                    f"Best result from analyst iteration {best.iteration + 1}: "
                    f"{best.speedup:.2f}x using {', '.join(best.transforms) or 'unknown transforms'}."
                ),
                suggestions="Try a different structural approach to improve further.",
                state_promoted_from=best.iteration,
            )

        return history

    def _best_iteration(self) -> Optional[AnalystIteration]:
        """Return the iteration with the highest speedup."""
        if not self.iterations:
            return None
        return max(self.iterations, key=lambda it: it.speedup)

    # =========================================================================
    # Session persistence
    # =========================================================================

    def save_session(self, output_dir: Optional[Path] = None) -> Path:
        """Save session state for resumability.

        Saves to benchmark_dir/analyst_sessions/{query_id}/session.json.
        Each iteration's artifacts saved individually.

        Args:
            output_dir: Override output directory. If None, uses
                        benchmark_dir/analyst_sessions/{query_id}/.
        Returns:
            Path to the session directory.
        """
        if output_dir is None:
            output_dir = (
                self.pipeline.benchmark_dir
                / "analyst_sessions"
                / self.query_id
            )
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save session metadata
        session_data = {
            "query_id": self.query_id,
            "max_iterations": self.max_iterations,
            "target_speedup": self.target_speedup,
            "n_workers": self.n_workers,
            "best_speedup": self.best_speedup,
            "best_sql": self.best_sql,
            "n_iterations": len(self.iterations),
            "run_log_path": str(self._run_log_path) if self._run_log_path else None,
        }
        (output_dir / "session.json").write_text(
            json.dumps(session_data, indent=2)
        )

        # Save each iteration
        for it in self.iterations:
            it_dir = output_dir / f"iteration_{it.iteration:02d}"
            it_dir.mkdir(parents=True, exist_ok=True)

            (it_dir / "prompt.txt").write_text(it.prompt)
            (it_dir / "optimized.sql").write_text(it.optimized_sql)
            (it_dir / "validation.json").write_text(json.dumps({
                "status": it.status,
                "speedup": it.speedup,
                "transforms": it.transforms,
                "examples_used": it.examples_used,
                "error_messages": it.error_messages,
                "error_category": it.error_category,
            }, indent=2))
            if it.analysis:
                (it_dir / "analysis.txt").write_text(it.analysis)
            if it.analysis_prompt:
                (it_dir / "analysis_prompt.txt").write_text(it.analysis_prompt)
            if it.rewrite_response:
                (it_dir / "rewrite_response.txt").write_text(it.rewrite_response)
            if it.failure_analysis:
                (it_dir / "failure_analysis.txt").write_text(it.failure_analysis)

        logger.info(
            f"Saved analyst session: {output_dir} "
            f"({len(self.iterations)} iterations)"
        )
        return output_dir

    @classmethod
    def load_session(
        cls,
        pipeline: "Pipeline",
        session_dir: Path,
    ) -> "AnalystSession":
        """Load a previously saved session for resumption.

        Args:
            pipeline: Pipeline instance to use for further iterations.
            session_dir: Path to analyst_sessions/{query_id}/ directory.

        Returns:
            AnalystSession with previous iterations loaded.
        """
        session_dir = Path(session_dir)
        meta = json.loads((session_dir / "session.json").read_text())

        # Load original SQL from first iteration (always the same)
        first_it_dir = session_dir / "iteration_00"
        original_sql = None
        if first_it_dir.exists():
            opt_path = first_it_dir / "optimized.sql"
            val_path = first_it_dir / "validation.json"
            # Original SQL is in every iteration's AnalystIteration
            # but we stored optimized_sql per iteration. We need the original.
            # Load from the query file instead.
            original_sql = pipeline.load_query(meta["query_id"])

        if not original_sql:
            raise FileNotFoundError(
                f"Cannot find original SQL for {meta['query_id']}"
            )

        session = cls(
            pipeline=pipeline,
            query_id=meta["query_id"],
            original_sql=original_sql,
            max_iterations=meta.get("max_iterations", 3),
            target_speedup=meta.get("target_speedup", 2.0),
            n_workers=meta.get("n_workers", 3),
        )
        session.best_speedup = meta.get("best_speedup", 1.0)
        session.best_sql = meta.get("best_sql", original_sql)

        # Load iterations
        n_iterations = meta.get("n_iterations", 0)
        for i in range(n_iterations):
            it_dir = session_dir / f"iteration_{i:02d}"
            if not it_dir.exists():
                continue

            prompt = ""
            prompt_path = it_dir / "prompt.txt"
            if prompt_path.exists():
                prompt = prompt_path.read_text()

            optimized_sql = original_sql
            opt_path = it_dir / "optimized.sql"
            if opt_path.exists():
                optimized_sql = opt_path.read_text()

            analysis = None
            analysis_path = it_dir / "analysis.txt"
            if analysis_path.exists():
                analysis = analysis_path.read_text()

            analysis_prompt = None
            analysis_prompt_path = it_dir / "analysis_prompt.txt"
            if analysis_prompt_path.exists():
                analysis_prompt = analysis_prompt_path.read_text()

            rewrite_response = None
            rewrite_response_path = it_dir / "rewrite_response.txt"
            if rewrite_response_path.exists():
                rewrite_response = rewrite_response_path.read_text()

            failure_analysis = None
            fa_path = it_dir / "failure_analysis.txt"
            if fa_path.exists():
                failure_analysis = fa_path.read_text()

            val_data = {}
            val_path = it_dir / "validation.json"
            if val_path.exists():
                val_data = json.loads(val_path.read_text())

            session.iterations.append(AnalystIteration(
                iteration=i,
                original_sql=original_sql,
                optimized_sql=optimized_sql,
                status=val_data.get("status", "ERROR"),
                speedup=val_data.get("speedup", 0.0),
                transforms=val_data.get("transforms", []),
                prompt=prompt,
                analysis=analysis,
                analysis_prompt=analysis_prompt,
                rewrite_response=rewrite_response,
                examples_used=val_data.get("examples_used", []),
                failure_analysis=failure_analysis,
            ))

        logger.info(
            f"Loaded analyst session: {session.query_id} "
            f"({len(session.iterations)} iterations, "
            f"best {session.best_speedup:.2f}x)"
        )
        return session

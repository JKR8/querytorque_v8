"""Iterative single-query deep-dive optimization (Analyst Mode).

The AnalystSession runs an iterative loop on a single query:
1. Analyze the ORIGINAL query (never intermediate results)
2. Generate N candidates via parallel workers
3. Validate the best candidate
4. Record the iteration's result and transforms
5. Repeat with full history of all previous iterations

Critical invariant: every iteration optimizes from the ORIGINAL SQL,
never from the best intermediate. The full iteration history is passed
to the LLM so it can learn from what worked and what didn't.

Usage:
    from ado.pipeline import Pipeline
    p = Pipeline("ado/benchmarks/duckdb_tpcds")
    session = p.run_analyst_session("query_88", sql, max_iterations=5)
    print(f"Best: {session.best_speedup:.2f}x")
    print(session.best_sql)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .pipeline import Pipeline

logger = logging.getLogger(__name__)


@dataclass
class AnalystIteration:
    """One iteration of the analyst deep-dive loop."""
    iteration: int
    original_sql: str        # ALWAYS the original — never changes
    optimized_sql: str       # This iteration's rewrite
    status: str              # WIN | IMPROVED | NEUTRAL | REGRESSION | ERROR
    speedup: float
    transforms: List[str]
    prompt: str              # The prompt used
    analysis: Optional[str] = None  # LLM analyst output (if analyst enabled)
    examples_used: List[str] = field(default_factory=list)


class AnalystSession:
    """Iterative single-query deep-dive optimization.

    Each iteration:
    1. ALWAYS starts from the original SQL (not from previous best)
    2. Builds full history from ALL previous iterations
    3. Runs LLM analyst for structural guidance
    4. Generates N candidates and validates
    5. Records result

    Stops when:
    - Target speedup is reached
    - Max iterations exhausted
    - Last 2 iterations made no meaningful progress (converged)
    """

    def __init__(
        self,
        pipeline: "Pipeline",
        query_id: str,
        original_sql: str,
        max_iterations: int = 5,
        target_speedup: float = 1.5,
        n_workers: int = 3,
    ):
        self.pipeline = pipeline
        self.query_id = query_id
        self.original_sql = original_sql  # NEVER changes
        self.max_iterations = max_iterations
        self.target_speedup = target_speedup
        self.n_workers = n_workers
        self.iterations: List[AnalystIteration] = []
        self.best_speedup = 1.0
        self.best_sql = original_sql

    def run(self) -> AnalystIteration:
        """Run the full iterative loop. Returns best iteration."""
        for i in range(self.max_iterations):
            logger.info(
                f"[{self.query_id}] Analyst iteration {i + 1}/{self.max_iterations} "
                f"(best so far: {self.best_speedup:.2f}x)"
            )

            iteration = self._run_iteration(i)
            self.iterations.append(iteration)

            # Track best
            if iteration.speedup > self.best_speedup:
                self.best_speedup = iteration.speedup
                self.best_sql = iteration.optimized_sql
                logger.info(
                    f"[{self.query_id}] New best: {self.best_speedup:.2f}x "
                    f"(iteration {i + 1})"
                )

            # Stopping criteria
            if self.best_speedup >= self.target_speedup:
                logger.info(
                    f"[{self.query_id}] Target speedup {self.target_speedup:.2f}x "
                    f"reached at iteration {i + 1}"
                )
                break
            if self._has_converged():
                logger.info(
                    f"[{self.query_id}] Converged after {len(self.iterations)} "
                    f"iterations (no progress in last 2 rounds)"
                )
                break

        best = self._best_iteration()
        logger.info(
            f"[{self.query_id}] Analyst session complete: "
            f"{len(self.iterations)} iterations, "
            f"best {best.speedup:.2f}x (iteration {best.iteration + 1})"
        )
        return best

    def _run_iteration(self, iteration_num: int) -> AnalystIteration:
        """Single iteration: analyze -> rewrite -> validate."""
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

        # Build history from ALL previous iterations
        history = self._build_iteration_history()

        # Phase 1: Parse ORIGINAL SQL (always)
        dag, costs = self.pipeline._parse_dag(self.original_sql, dialect=dialect)

        # Phase 2: FAISS example retrieval (on original SQL)
        examples = self.pipeline._find_examples(
            self.original_sql, engine=engine, k=3,
        )
        example_ids = [e.get("id", "?") for e in examples]

        # Always run analyst in deep-dive mode
        expert_analysis = None
        analysis_raw = None
        expert_analysis, analysis_raw, examples = self.pipeline._run_analyst(
            query_id=self.query_id,
            sql=self.original_sql,
            dag=dag,
            costs=costs,
            history=history,
            faiss_examples=examples,
            engine=engine,
            dialect=dialect,
        )
        example_ids = [e.get("id", "?") for e in examples]

        # Load global learnings
        global_learnings = self.pipeline.learner.build_learning_summary() or None

        # Phase 3: Build prompt with full iteration history
        prompt = self.pipeline.prompter.build_prompt(
            query_id=self.query_id,
            full_sql=self.original_sql,  # ALWAYS original
            dag=dag,
            costs=costs,
            history=history,
            examples=examples,
            expert_analysis=expert_analysis,
            global_learnings=global_learnings,
            dialect=dialect,
        )

        # Generate candidates
        from .generate import CandidateGenerator
        generator = CandidateGenerator(
            provider=self.pipeline.provider,
            model=self.pipeline.model,
            analyze_fn=self.pipeline.analyze_fn,
        )

        candidates = generator.generate(
            sql=self.original_sql,
            prompt=prompt,
            examples_used=example_ids,
            n=self.n_workers,
            dialect=dialect,
        )

        # Pick best candidate
        optimized_sql = self.original_sql
        transforms = []
        if candidates:
            best_cand = None
            for cand in candidates:
                if (
                    not cand.error
                    and cand.optimized_sql
                    and cand.optimized_sql != self.original_sql
                ):
                    best_cand = cand
                    break
            if best_cand:
                optimized_sql = best_cand.optimized_sql
                transforms = best_cand.transforms

        # Phase 4: Syntax check
        try:
            import sqlglot
            sqlglot.parse_one(optimized_sql, dialect=dialect)
        except Exception as e:
            logger.warning(
                f"[{self.query_id}] Iteration {iteration_num + 1} syntax error: {e}"
            )
            optimized_sql = self.original_sql

        # Phase 5: Validate
        status, speedup = self.pipeline._validate(self.original_sql, optimized_sql)
        logger.info(
            f"[{self.query_id}] Iteration {iteration_num + 1}: "
            f"{status} ({speedup:.2f}x)"
        )

        # Create learning record for this iteration
        try:
            error_cat = "execution" if status == "ERROR" else None
            lr = self.pipeline.learner.create_learning_record(
                query_id=self.query_id,
                examples_recommended=example_ids,
                transforms_recommended=example_ids,
                status="pass" if status in ("WIN", "IMPROVED", "NEUTRAL") else "error",
                speedup=speedup,
                transforms_used=transforms,
                worker_id=0,
                attempt_number=iteration_num + 1,
                error_category=error_cat,
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
            analysis=analysis_raw,
            examples_used=example_ids,
        )

    def _build_iteration_history(self) -> Optional[Dict[str, Any]]:
        """Build history dict from all previous analyst iterations.

        Each iteration becomes an 'attempt' with full SQL, status, speedup.
        The best result so far gets special treatment via a PromotionAnalysis.
        """
        if not self.iterations:
            return None

        attempts = []
        for it in self.iterations:
            attempts.append({
                "state": it.iteration,
                "status": it.status,
                "speedup": it.speedup,
                "transforms": it.transforms,
                "original_sql": it.original_sql,
                "optimized_sql": it.optimized_sql,
            })

        history = {"attempts": attempts, "promotion": None}

        # If we have a winner, create a promotion-like context
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

    def _has_converged(self) -> bool:
        """Check if last 2 iterations made no meaningful progress."""
        if len(self.iterations) < 3:
            return False
        recent = self.iterations[-2:]
        return all(it.speedup <= self.best_speedup * 1.02 for it in recent)

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
            }, indent=2))
            if it.analysis:
                (it_dir / "analysis.txt").write_text(it.analysis)

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
            max_iterations=meta.get("max_iterations", 5),
            target_speedup=meta.get("target_speedup", 1.5),
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
                examples_used=val_data.get("examples_used", []),
            ))

        logger.info(
            f"Loaded analyst session: {session.query_id} "
            f"({len(session.iterations)} iterations, "
            f"best {session.best_speedup:.2f}x)"
        )
        return session

"""Standard optimization session — fast, no analyst overhead.

Single iteration: FAISS retrieval → prompt → generate → validate.
No analyst call, no failure analysis, no retry.
Best for queries where analyst guidance doesn't help.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .base_session import OptimizationSession
from ..schemas import SessionResult

if TYPE_CHECKING:
    from ..pipeline import Pipeline

logger = logging.getLogger(__name__)


class StandardSession(OptimizationSession):
    """Fast optimization mode: skip analyst, single iteration."""

    def run(self) -> SessionResult:
        """Run single-iteration optimization without analyst."""
        logger.info(
            f"[{self.query_id}] StandardSession: "
            f"{self.n_workers} workers, no analyst"
        )

        # Phase 1: Parse DAG
        dag, costs, _explain = self.pipeline._parse_dag(
            self.original_sql, dialect=self.dialect, query_id=self.query_id
        )

        # Phase 2: FAISS retrieval (k=3)
        examples = self.pipeline._find_examples(
            self.original_sql, engine=self.engine, k=3
        )
        example_ids = [e.get("id", "?") for e in examples]

        # Regression warnings
        regression_warnings = self.pipeline._find_regression_warnings(
            self.original_sql, engine=self.engine, k=2
        )

        # Global learnings
        global_learnings = self.pipeline.learner.build_learning_summary() or None

        # Phase 3: Build prompt (no analyst, no history)
        prompt = self.pipeline.prompter.build_prompt(
            query_id=self.query_id,
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
        )

        # Generate candidates
        from ..generate import CandidateGenerator
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
            dialect=self.dialect,
        )

        # Pick best candidate
        optimized_sql = self.original_sql
        transforms = []
        for cand in candidates:
            if not cand.error and cand.optimized_sql and cand.optimized_sql != self.original_sql:
                optimized_sql = cand.optimized_sql
                transforms = cand.transforms
                break

        # Phase 4: Syntax check
        try:
            import sqlglot
            sqlglot.parse_one(optimized_sql, dialect=self.dialect)
        except Exception as e:
            logger.warning(f"[{self.query_id}] Syntax error: {e}")
            optimized_sql = self.original_sql

        # Phase 5: Validate
        status, speedup, val_errors, val_error_cat = self.pipeline._validate(self.original_sql, optimized_sql)
        logger.info(f"[{self.query_id}] Standard result: {status} ({speedup:.2f}x)")
        if val_errors:
            logger.info(f"[{self.query_id}] Validation errors: {val_errors}")

        # Learning record
        try:
            lr = self.pipeline.learner.create_learning_record(
                query_id=self.query_id,
                examples_recommended=example_ids,
                transforms_recommended=example_ids,
                status="pass" if status in ("WIN", "IMPROVED", "NEUTRAL") else "error",
                speedup=speedup,
                transforms_used=transforms,
                error_category=val_error_cat,
                error_messages=val_errors,
            )
            self.pipeline.learner.save_learning_record(lr)
        except Exception as e:
            logger.warning(f"[{self.query_id}] Learning record failed: {e}")

        return SessionResult(
            query_id=self.query_id,
            mode="standard",
            best_speedup=speedup,
            best_sql=optimized_sql,
            original_sql=self.original_sql,
            best_transforms=transforms,
            status=status,
            iterations=[{
                "iteration": 0,
                "status": status,
                "speedup": speedup,
                "transforms": transforms,
                "examples_used": example_ids,
            }],
            n_iterations=1,
            n_api_calls=self.n_workers,
        )

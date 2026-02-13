"""Oneshot optimization session — single LLM call produces rewrite directly.

The analyst prompt in oneshot mode asks the LLM to analyze AND rewrite in one
shot (no separate worker step). Output uses the same JSON rewrite_sets
format as worker prompts — parsed by SQLRewriter.apply_response().

Flow per iteration:
1. Parse logical tree + gather context (same as expert/swarm)
2. build_analyst_briefing_prompt(mode="oneshot") → LLM call
3. SQLRewriter.apply_response() → extract per-node SQL + transforms
4. Syntax check → validate → failure analysis if needed → iterate
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from .base_session import OptimizationSession
from ..schemas import SessionResult

if TYPE_CHECKING:
    from ..pipeline import Pipeline

logger = logging.getLogger(__name__)


class OneshotSession(OptimizationSession):
    """Single-LLM-call optimization: analyst produces JSON rewrite directly."""

    def run(self) -> SessionResult:
        """Run oneshot optimization with optional iteration on failure."""
        from ..prompts import build_analyst_briefing_prompt

        logger.info(
            f"[{self.query_id}] OneshotSession: "
            f"max {self.max_iterations} iterations, "
            f"target {self.target_speedup:.1f}x"
        )

        best_speedup = 0.0
        best_sql = self.original_sql
        best_transforms = []
        best_status = "ERROR"
        iterations_data = []
        iteration_history = []
        n_api_calls = 0

        for i in range(self.max_iterations):
            logger.info(f"[{self.query_id}] Oneshot iteration {i + 1}/{self.max_iterations}")

            # Phase 1: Parse logical tree
            dag, costs, _explain = self.pipeline._parse_logical_tree(
                self.original_sql, dialect=self.dialect, query_id=self.query_id,
            )

            # Phase 2: Gather context
            ctx = self.pipeline.gather_analyst_context(
                query_id=self.query_id,
                sql=self.original_sql,
                dialect=self.dialect,
                engine=self.engine,
            )

            # Phase 3: Build oneshot prompt
            prompt = build_analyst_briefing_prompt(
                query_id=self.query_id,
                sql=self.original_sql,
                explain_plan_text=ctx["explain_plan_text"],
                dag=dag,
                costs=costs,
                semantic_intents=ctx["semantic_intents"],
                global_knowledge=ctx["global_knowledge"],
                constraints=ctx["constraints"],
                dialect=self.dialect,
                strategy_leaderboard=ctx["strategy_leaderboard"],
                query_archetype=ctx["query_archetype"],
                engine_profile=ctx["engine_profile"],
                resource_envelope=ctx["resource_envelope"],
                exploit_algorithm_text=ctx["exploit_algorithm_text"],
                plan_scanner_text=ctx["plan_scanner_text"],
                iteration_history={"attempts": iteration_history} if iteration_history else None,
                mode="oneshot",
                qerror_analysis=ctx.get("qerror_analysis"),
            )

            # Phase 4: LLM call + JSON rewrite parsing via generate_one()
            from ..generate import CandidateGenerator
            generator = CandidateGenerator(
                provider=self.pipeline.provider,
                model=self.pipeline.model,
                analyze_fn=self.pipeline.analyze_fn,
            )

            example_ids = [e.get("id", "?") for e in ctx["matched_examples"][:3]]

            try:
                candidate = generator.generate_one(
                    sql=self.original_sql,
                    prompt=prompt,
                    examples_used=example_ids,
                    worker_id=0,  # oneshot = worker 0
                    dialect=self.dialect,
                )
                n_api_calls += 1
            except Exception as e:
                logger.error(f"[{self.query_id}] Oneshot LLM call failed: {e}")
                iterations_data.append({
                    "iteration": i,
                    "status": "ERROR",
                    "speedup": 0.0,
                    "transforms": [],
                    "error": str(e),
                })
                n_api_calls += 1
                continue

            optimized_sql = candidate.optimized_sql
            transforms = candidate.transforms
            strategy = candidate.transforms[0] if candidate.transforms else "unknown"

            logger.info(
                f"[{self.query_id}] Oneshot strategy={strategy}, "
                f"transforms={transforms}"
            )

            # Phase 5: Syntax check
            if candidate.error:
                logger.warning(f"[{self.query_id}] Rewrite error: {candidate.error}")
                optimized_sql = self.original_sql

            try:
                import sqlglot
                sqlglot.parse_one(optimized_sql, dialect=self.dialect)
            except Exception as e:
                logger.warning(f"[{self.query_id}] Syntax error: {e}")
                optimized_sql = self.original_sql

            # Phase 6: Validate
            status, speedup, val_errors, val_error_cat = self.pipeline._validate(
                self.original_sql, optimized_sql,
            )
            logger.info(
                f"[{self.query_id}] Oneshot iter {i + 1}: "
                f"{status} ({speedup:.2f}x)"
            )

            # Track iteration
            iter_record = {
                "iteration": i,
                "status": status,
                "speedup": speedup,
                "transforms": transforms,
                "strategy": strategy,
                "error_messages": val_errors,
                "error_category": val_error_cat,
            }
            iterations_data.append(iter_record)

            # Update iteration history for next round
            iteration_history.append({
                "source": f"oneshot_iter_{i}",
                "status": status,
                "speedup": speedup,
                "transforms": transforms,
                "optimized_sql": optimized_sql,
                "error_messages": val_errors,
                "error_category": val_error_cat,
            })

            # Track best
            if speedup > best_speedup:
                best_speedup = speedup
                best_sql = optimized_sql
                best_transforms = transforms
                best_status = status

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

            # Stop if target reached
            if best_speedup >= self.target_speedup:
                logger.info(
                    f"[{self.query_id}] Target {self.target_speedup:.1f}x reached!"
                )
                break

        # Final status from best result
        if best_speedup <= 0:
            best_status = "ERROR"
        else:
            best_status = self._classify_speedup(best_speedup)

        return SessionResult(
            query_id=self.query_id,
            mode="oneshot",
            best_speedup=best_speedup,
            best_sql=best_sql,
            original_sql=self.original_sql,
            best_transforms=best_transforms,
            status=best_status,
            iterations=iterations_data,
            n_iterations=len(iterations_data),
            n_api_calls=n_api_calls,
        )

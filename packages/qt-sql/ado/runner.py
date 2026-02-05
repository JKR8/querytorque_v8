"""ADO runner (orchestrator).

This module orchestrates the ADO optimization loop:
1. Build context from SQL + database
2. Retrieve relevant examples and constraints
3. Build optimization prompt
4. Generate candidates in parallel
5. Validate candidates
6. Return best result with enriched data for curation
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .context import ContextBuilder, ContextBundle
from .knowledge import KnowledgeRetriever
from .prompt_builder import PromptBuilder
from .generate import CandidateGenerator, Candidate
from .validate import Validator
from .learn import Learner, AttemptHistory, AttemptSummary
from .store import Store
from .schemas import ValidationStatus, ValidationResult

logger = logging.getLogger(__name__)


@dataclass
class ADOConfig:
    """Configuration for ADO runner."""
    sample_db: str
    candidates_per_round: int = 10
    max_rounds: int = 1
    provider: Optional[str] = None
    model: Optional[str] = None
    dialect: str = "duckdb"  # or "postgres"

    # Legacy support for analyze_fn callback
    analyze_fn: Optional[Callable[[str], str]] = None


@dataclass
class ADOResult:
    """Complete result from ADO optimization."""
    query_id: str
    status: str
    speedup: float
    worker_id: Optional[int]
    optimized_sql: str
    original_sql: str
    examples_used: List[str]
    transforms: List[str]
    database: str
    attempts: int = 0
    all_validations: List[Dict[str, Any]] = field(default_factory=list)


class ADORunner:
    """Run the ADO optimization loop for a single query.

    This orchestrator:
    1. Builds context (EXPLAIN plan, table stats)
    2. Retrieves relevant examples and constraints
    3. Builds optimization prompts
    4. Generates candidates in parallel
    5. Validates each candidate
    6. Returns the best result with enriched metadata
    """

    def __init__(
        self,
        config: ADOConfig,
        run_dir: Optional[Path] = None,
    ):
        """Initialize ADO runner.

        Args:
            config: ADO configuration
            run_dir: Directory for storing run artifacts
        """
        self.config = config
        self.run_dir = run_dir or Path("research/ado/runs/default")

        # Determine dialect from database connection
        if config.sample_db.startswith("postgres://") or config.sample_db.startswith("postgresql://"):
            self.dialect = "postgres"
        else:
            self.dialect = config.dialect

        # Initialize components
        self.context_builder = ContextBuilder(engine=self.dialect)
        self.knowledge = KnowledgeRetriever()
        self.prompt_builder = PromptBuilder()

        # Generator - supports both LLM client and legacy analyze_fn
        self.generator = CandidateGenerator(
            provider=config.provider,
            model=config.model,
            analyze_fn=config.analyze_fn,
        )

        self.validator = Validator(sample_db=config.sample_db)
        self.learner = Learner()
        self.store = Store(self.run_dir)

    def run_query(self, query_id: str, sql: str) -> ADOResult:
        """Run optimization on a single query.

        Args:
            query_id: Query identifier (e.g., 'q1', 'query_15')
            sql: The SQL query to optimize

        Returns:
            ADOResult with optimization results and metadata
        """
        history = AttemptHistory()
        all_validations = []
        total_attempts = 0

        for round_num in range(self.config.max_rounds):
            logger.info(f"Starting round {round_num + 1}/{self.config.max_rounds} for {query_id}")

            # 1. Build context
            context = self.context_builder.build(
                query_id=query_id,
                sql=sql,
                sample_db=self.config.sample_db,
            )

            # 2. Retrieve examples and constraints (using DSB query mapping if available)
            retrieval = self.knowledge.retrieve(sql, k_examples=3, query_id=query_id)

            # 3. Build prompt with retrieved examples
            history_text = history.ranked_text() if history.summaries else ""

            # Convert knowledge GoldExamples to prompt_builder format
            from .prompt_builder import GoldExample as PromptExample
            prompt_examples = [
                PromptExample(
                    id=ex.id,
                    name=ex.name,
                    description=ex.description,
                    verified_speedup=ex.verified_speedup,
                    example=ex.example,
                )
                for ex in retrieval.gold_examples
            ]

            prompt_text = self.prompt_builder.build(
                original_sql=sql,
                execution_plan=context.plan_summary,
                history=history_text,
                use_specific_examples=prompt_examples,  # Pass retrieved examples
            )

            # 4. Generate candidates
            examples_used = [e.id for e in retrieval.gold_examples]
            logger.info(f"Using examples for {query_id}: {examples_used}")
            candidates = self.generator.generate(
                sql=sql,
                prompt=prompt_text,
                examples_used=examples_used,
                n=self.config.candidates_per_round,
                dialect=self.dialect,
            )

            # 5. Validate each candidate
            validations = []
            for cand in candidates:
                total_attempts += 1

                result = self.validator.validate(
                    original_sql=sql,
                    candidate_sql=cand.optimized_sql,
                    worker_id=cand.worker_id,
                )
                validations.append(result)

                # Track for learning
                summary = AttemptSummary(
                    worker_id=cand.worker_id,
                    status=result.status.value,
                    speedup=result.speedup,
                    examples_used=cand.examples_used,
                    error_summary=result.error,
                )
                self.learner.update_history(history, summary)

                # Store artifacts
                self.store.save_candidate(
                    query_id=query_id,
                    worker_id=cand.worker_id,
                    prompt=cand.prompt,
                    response=cand.response,
                    optimized_sql=cand.optimized_sql,
                    validation={
                        "status": result.status.value,
                        "speedup": result.speedup,
                        "error": result.error,
                        "errors": result.errors,
                        "error_category": result.error_category,
                        "examples_used": cand.examples_used,
                        "transforms": cand.transforms,
                    },
                )

                # Track all validations for summary
                all_validations.append({
                    "worker_id": cand.worker_id,
                    "status": result.status.value,
                    "speedup": result.speedup,
                    "transforms": cand.transforms,
                })

            # 6. Check for winners
            passing = [v for v in validations if v.status == ValidationStatus.PASS]
            if passing:
                best = max(passing, key=lambda v: v.speedup)

                # Find corresponding candidate
                best_candidate = next(
                    (c for c in candidates if c.worker_id == best.worker_id),
                    None
                )

                # Create learning record for successful optimization
                if best_candidate:
                    learning_record = self.learner.create_learning_record(
                        query_id=query_id,
                        examples_recommended=best_candidate.examples_used,
                        transforms_recommended=examples_used,
                        status="pass",
                        speedup=best.speedup,
                        transforms_used=best_candidate.transforms,
                        worker_id=best.worker_id,
                        attempt_number=round_num + 1,
                        error_category=None,
                    )
                    self.learner.save_learning_record(learning_record)

                result = ADOResult(
                    query_id=query_id,
                    status=best.status.value,
                    speedup=best.speedup,
                    worker_id=best.worker_id,
                    optimized_sql=best.optimized_sql,
                    original_sql=sql,
                    examples_used=best_candidate.examples_used if best_candidate else [],
                    transforms=best_candidate.transforms if best_candidate else [],
                    database=self.config.sample_db,
                    attempts=total_attempts,
                    all_validations=all_validations,
                )

                # Save learning summary after successful optimization
                self.learner.save_learning_summary()

                return result

            # Rotate examples for next round
            self.prompt_builder.rotate_examples()

        # No winners after all rounds - create learning records for failures
        for v in all_validations:
            if v["status"] != "pass":
                learning_record = self.learner.create_learning_record(
                    query_id=query_id,
                    examples_recommended=v.get("examples_used", []),
                    transforms_recommended=[],
                    status=v["status"],
                    speedup=v.get("speedup", 0.0),
                    transforms_used=v.get("transforms", []),
                    worker_id=v.get("worker_id", 0),
                    attempt_number=1,
                    error_category=v.get("error_category"),
                    error_messages=[v.get("error")] if v.get("error") else [],
                )
                self.learner.save_learning_record(learning_record)

        # Save final learning summary
        self.learner.save_learning_summary()

        return ADOResult(
            query_id=query_id,
            status=ValidationStatus.FAIL.value,
            speedup=0.0,
            worker_id=None,
            optimized_sql="",
            original_sql=sql,
            examples_used=[],
            transforms=[],
            database=self.config.sample_db,
            attempts=total_attempts,
            all_validations=all_validations,
        )

    def run_queries(
        self,
        queries: Dict[str, str],
        progress_callback: Optional[Callable[[str, ADOResult], None]] = None,
    ) -> List[ADOResult]:
        """Run optimization on multiple queries.

        Args:
            queries: Dict of {query_id: sql}
            progress_callback: Optional callback(query_id, result) called after each query

        Returns:
            List of ADOResults
        """
        results = []

        for query_id, sql in queries.items():
            logger.info(f"Processing {query_id}")

            result = self.run_query(query_id, sql)
            results.append(result)

            if progress_callback:
                progress_callback(query_id, result)

        return results

    def close(self) -> None:
        """Close resources."""
        self.validator.close()

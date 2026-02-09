"""ADO pipeline orchestrator.

Phases:
1. Parse:     SQL → DAG (deterministic, DagBuilder)
2. Retrieve:  FAISS example matching (engine-specific)
3. Rewrite:   Full-query prompt with DAG topology (N parallel workers)
4. Validate:  Syntax check (deterministic)
5. Validate:  Timing + correctness (3-run or 5-run)

State management:
- State 0: Discovery with N workers
- State 1+: Refinement with 1 worker + full history
- Promote: Best-of-state SQL becomes next state's baseline
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from .learn import Learner
from .node_prompter import Prompter
from .schemas import (
    BenchmarkConfig,
    OptimizationMode,
    PipelineResult,
    PromotionAnalysis,
    SessionResult,
)
from .store import Store

logger = logging.getLogger(__name__)


class Pipeline:
    """ADO pipeline with FAISS example retrieval, LLM rewrite, and validation."""

    def __init__(
        self,
        benchmark_dir: str | Path,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        analyze_fn=None,
        use_analyst: bool = False,
    ):
        """Load config.json from benchmark dir, initialize components.

        Args:
            benchmark_dir: Path to benchmark directory (e.g., ado/benchmarks/duckdb_tpcds)
            provider: LLM provider for generation
            model: LLM model name
            analyze_fn: Optional custom LLM function
            use_analyst: If True, runs LLM analyst before rewrite (costs 1 extra
                         API call per query). Use for stubborn queries only.
        """
        self.benchmark_dir = Path(benchmark_dir)
        self.config = BenchmarkConfig.from_file(
            self.benchmark_dir / "config.json"
        )

        self.provider = provider
        self.model = model
        self.analyze_fn = analyze_fn
        self.use_analyst = use_analyst

        # Initialize pipeline components
        self.prompter = Prompter()
        self.learner = Learner(
            journal_dir=self.benchmark_dir / "learning"
        )

        # Seed dirs: state_0/seed/ for unverified catalog rules
        self._seed_dirs = []
        seed_dir = self.benchmark_dir / "state_0" / "seed"
        if seed_dir.exists():
            self._seed_dirs.append(seed_dir)

        # Resolve engine version (cached, used in prompts)
        self._engine_version = self._resolve_engine_version()

        # Load pre-computed semantic intents (LLM-generated per-query intents)
        self._semantic_intents: Dict[str, Dict[str, Any]] = {}
        intents_path = self.benchmark_dir / "semantic_intents.json"
        if intents_path.exists():
            try:
                data = json.loads(intents_path.read_text())
                for q in data.get("queries", []):
                    qid = q.get("query_id", "")
                    if qid:
                        self._semantic_intents[qid] = q
                logger.info(
                    f"Loaded semantic intents for {len(self._semantic_intents)} queries"
                )
            except Exception as e:
                logger.warning(f"Failed to load semantic_intents.json: {e}")

    def _resolve_engine_version(self) -> Optional[str]:
        """Detect the target engine version for prompt context."""
        engine = self.config.engine
        try:
            if engine == "duckdb":
                import duckdb
                return duckdb.__version__
            elif engine in ("postgresql", "postgres"):
                import subprocess
                result = subprocess.run(
                    ["psql", "--version"], capture_output=True, text=True, timeout=5,
                )
                if result.returncode == 0:
                    # "psql (PostgreSQL) 16.2" → "16.2"
                    parts = result.stdout.strip().split()
                    return parts[-1] if parts else None
        except Exception:
            pass
        return None

    def get_semantic_intents(self, query_id: str) -> Optional[Dict[str, Any]]:
        """Look up pre-computed semantic intents for a query.

        Tries exact match first, then normalized forms (q1, query_1, etc.).
        """
        if not self._semantic_intents:
            return None

        # Exact match
        if query_id in self._semantic_intents:
            return self._semantic_intents[query_id]

        # Normalize: query_75 → q75, q75 → q75
        import re
        m = re.match(r"(?:query_?)?(\d+)", query_id)
        if m:
            num = m.group(1)
            for variant in [f"q{num}", f"query_{num}", f"query{num}"]:
                if variant in self._semantic_intents:
                    return self._semantic_intents[variant]

        return None

    # =========================================================================
    # Phase 1: Parse SQL → DAG
    # =========================================================================

    def _parse_dag(self, sql: str, dialect: str = "duckdb", query_id: str = "unknown"):
        """Phase 1: Parse SQL into DAG structure with real EXPLAIN costs.

        Reads cached EXPLAIN ANALYZE from benchmark_dir/explains/.
        If not cached, runs EXPLAIN ANALYZE and caches the result.
        Falls back to heuristic cost splitting if EXPLAIN fails.

        Returns (dag, costs, explain_result) tuple.
        explain_result is the raw EXPLAIN output dict (or None).
        """
        from .dag import DagBuilder, CostAnalyzer

        builder = DagBuilder(sql, dialect=dialect)
        dag = builder.build()

        # Get real costs from cached EXPLAIN ANALYZE (or run if not cached)
        plan_context = None
        explain_result = None
        try:
            explain_result = self._get_explain(query_id, sql)
            if explain_result and explain_result.get("plan_json"):
                from .plan_analyzer import analyze_plan_for_optimization
                plan_context = analyze_plan_for_optimization(
                    explain_result["plan_json"], sql,
                    engine=self.config.engine,
                )
                logger.info(
                    f"EXPLAIN: {explain_result.get('execution_time_ms', '?')}ms, "
                    f"{len(plan_context.table_scans)} scans, "
                    f"{len(plan_context.bottleneck_operators)} operators"
                )
        except Exception as e:
            logger.warning(f"EXPLAIN failed, using heuristic costs: {e}")

        cost_analyzer = CostAnalyzer(dag, plan_context=plan_context)
        costs = cost_analyzer.analyze()

        return dag, costs, explain_result

    def _get_explain(self, query_id: str, sql: str) -> Optional[Dict[str, Any]]:
        """Get EXPLAIN ANALYZE result — cached first, run if missing.

        Cache location: benchmark_dir/explains/sf10/{query_id}.json
        Falls back to:  benchmark_dir/explains/sf5/{query_id}.json
        """
        cache_dir = self.benchmark_dir / "explains" / "sf10"
        if not cache_dir.exists():
            cache_dir = self.benchmark_dir / "explains" / "sf5"
        cache_path = cache_dir / f"{query_id}.json"

        # Try cache first
        if cache_path.exists():
            try:
                data = json.loads(cache_path.read_text())
                logger.info(f"[{query_id}] EXPLAIN loaded from cache")
                return data
            except Exception:
                pass

        # Run EXPLAIN ANALYZE and cache (with timeout)
        logger.info(f"[{query_id}] Running EXPLAIN ANALYZE (will cache)")
        timeout_ms = int(self.config.extra.get("explain_timeout_ms", 300_000)) if hasattr(self.config, 'extra') else 300_000
        try:
            from .execution.database_utils import run_explain_analyze
            result = run_explain_analyze(self.config.db_path_or_dsn, sql)
            if result:
                cache_dir.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(json.dumps(result, indent=2, default=str))
                logger.info(
                    f"[{query_id}] Cached EXPLAIN: "
                    f"{result.get('execution_time_ms', '?')}ms"
                )
            return result
        except Exception as e:
            logger.warning(f"[{query_id}] EXPLAIN ANALYZE failed: {e}")
            # Fallback: EXPLAIN without ANALYZE (plan structure only, no execution)
            try:
                logger.info(f"[{query_id}] Falling back to EXPLAIN (no ANALYZE)")
                from .execution.factory import create_executor_from_dsn
                with create_executor_from_dsn(self.config.db_path_or_dsn) as executor:
                    plan_result = executor.explain(sql, analyze=False, timeout_ms=30_000)
                    if plan_result and not plan_result.get("error"):
                        result = {
                            "execution_time_ms": None,
                            "plan_text": None,
                            "plan_json": plan_result.get("children", [plan_result.get("Plan", {})]),
                            "actual_rows": plan_result.get("rows_returned", 0),
                            "note": "EXPLAIN only (no ANALYZE) — estimated costs",
                        }
                        cache_dir.mkdir(parents=True, exist_ok=True)
                        cache_path.write_text(json.dumps(result, indent=2, default=str))
                        logger.info(f"[{query_id}] Cached EXPLAIN (plan-only)")
                        return result
            except Exception as e2:
                logger.warning(f"[{query_id}] EXPLAIN fallback also failed: {e2}")
            return None

    # =========================================================================
    # Phase 5: Validate
    # =========================================================================

    def _validate(
        self,
        original_sql: str,
        optimized_sql: str,
    ) -> tuple[str, float, list[str], str | None]:
        """Phase 5: Validate the optimized SQL.

        Returns (status, speedup, error_messages, error_category) tuple.
        Status: WIN | IMPROVED | NEUTRAL | REGRESSION | FAIL | ERROR
        """
        from .validate import Validator

        validator = Validator(sample_db=self.config.db_path_or_dsn)
        try:
            result = validator.validate(
                original_sql=original_sql,
                candidate_sql=optimized_sql,
                worker_id=0,
            )

            speedup = result.speedup
            errors = result.errors or []
            error_cat = result.error_category

            if result.status.value == "error":
                return "ERROR", 0.0, errors, error_cat or "execution"
            elif result.status.value == "fail":
                return "FAIL", 0.0, errors, error_cat or "semantic"
            elif speedup >= 1.10:
                return "WIN", speedup, [], None
            elif speedup >= 1.05:
                return "IMPROVED", speedup, [], None
            elif speedup >= 0.95:
                return "NEUTRAL", speedup, [], None
            else:
                return "REGRESSION", speedup, [], None
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return "ERROR", 0.0, [str(e)], "execution"
        finally:
            validator.close()

    def _validate_batch(
        self,
        original_sql: str,
        optimized_sqls: List[str],
    ) -> List[tuple[str, float, list[str], str | None]]:
        """Validate multiple optimized SQLs against a single original baseline.

        Times the original SQL ONCE, then validates each optimized SQL
        sequentially against the cached baseline.

        Returns:
            List of (status, speedup, error_messages, error_category) tuples
        """
        from .validate import Validator

        validator = Validator(sample_db=self.config.db_path_or_dsn)
        try:
            # Step 1: Benchmark original once
            baseline = validator.benchmark_baseline(original_sql)
            logger.info(
                f"Baseline: {baseline.measured_time_ms:.1f}ms "
                f"({baseline.row_count} rows)"
            )

            # Step 2: Validate each optimized SQL sequentially
            results = []
            for i, opt_sql in enumerate(optimized_sqls):
                result = validator.validate_against_baseline(
                    baseline=baseline,
                    candidate_sql=opt_sql,
                    worker_id=i,
                )

                speedup = result.speedup
                errors = result.errors or []
                error_cat = result.error_category

                if result.status.value == "error":
                    results.append(("ERROR", 0.0, errors, error_cat or "execution"))
                elif result.status.value == "fail":
                    results.append(("FAIL", 0.0, errors, error_cat or "semantic"))
                elif speedup >= 1.10:
                    results.append(("WIN", speedup, [], None))
                elif speedup >= 1.05:
                    results.append(("IMPROVED", speedup, [], None))
                elif speedup >= 0.95:
                    results.append(("NEUTRAL", speedup, [], None))
                else:
                    results.append(("REGRESSION", speedup, [], None))

            return results

        except Exception as e:
            logger.error(f"Batch validation failed: {e}")
            return [("ERROR", 0.0, [str(e)], "execution")] * len(optimized_sqls)
        finally:
            validator.close()

    def _validate_against_baseline(
        self,
        baseline: Any,
        optimized_sql: str,
    ) -> tuple[str, float, list[str], str | None]:
        """Validate optimized SQL against a pre-computed OriginalBaseline.

        Skips re-timing the original query (saves 3 executions per snipe).

        Args:
            baseline: OriginalBaseline from a previous benchmark_baseline() call
            optimized_sql: The optimized SQL to validate

        Returns (status, speedup, error_messages, error_category) tuple.
        """
        from .validate import Validator

        validator = Validator(sample_db=self.config.db_path_or_dsn)
        try:
            result = validator.validate_against_baseline(
                baseline=baseline,
                candidate_sql=optimized_sql,
                worker_id=0,
            )

            speedup = result.speedup
            errors = result.errors or []
            error_cat = result.error_category

            if result.status.value == "error":
                return "ERROR", 0.0, errors, error_cat or "execution"
            elif result.status.value == "fail":
                return "FAIL", 0.0, errors, error_cat or "semantic"
            elif speedup >= 1.10:
                return "WIN", speedup, [], None
            elif speedup >= 1.05:
                return "IMPROVED", speedup, [], None
            elif speedup >= 0.95:
                return "NEUTRAL", speedup, [], None
            else:
                return "REGRESSION", speedup, [], None
        except Exception as e:
            logger.error(f"Validation against baseline failed: {e}")
            return "ERROR", 0.0, [str(e)], "execution"
        finally:
            validator.close()

    def _benchmark_baseline(self, original_sql: str) -> Any:
        """Benchmark original SQL once and return OriginalBaseline.

        Args:
            original_sql: The original SQL query

        Returns:
            OriginalBaseline with timing, rows, and checksum
        """
        from .validate import Validator

        validator = Validator(sample_db=self.config.db_path_or_dsn)
        try:
            return validator.benchmark_baseline(original_sql)
        finally:
            validator.close()

    # =========================================================================
    # Main entry points
    # =========================================================================

    def run_query(
        self,
        query_id: str,
        sql: str,
        n_workers: int = 5,
        history: Optional[Dict[str, Any]] = None,
        use_analyst: Optional[bool] = None,
    ) -> PipelineResult:
        """Run a single query through all 5 phases.

        Args:
            query_id: Query identifier (e.g., 'query_1')
            sql: Original SQL query
            n_workers: Number of parallel workers for Phase 3
            history: Previous attempts and promotion context for this query.
                     Dict with 'attempts' (list of dicts) and 'promotion'
                     (PromotionAnalysis or None).
            use_analyst: Override pipeline-level use_analyst setting.
                         When True, runs LLM analyst before rewrite (1 extra API call).

        Returns:
            PipelineResult with full pipeline outcome
        """
        analyst_enabled = use_analyst if use_analyst is not None else self.use_analyst
        dialect = self.config.engine if self.config.engine != "postgresql" else "postgres"
        engine = (
            "postgres"
            if self.config.engine in ("postgresql", "postgres")
            else self.config.engine
        )

        # Phase 1: Parse
        logger.info(f"[{query_id}] Phase 1: Parsing DAG")
        dag, costs, explain_result = self._parse_dag(sql, dialect=dialect, query_id=query_id)
        logger.info(
            f"[{query_id}] DAG: {len(dag.nodes)} nodes, "
            f"{len(dag.edges)} edges"
        )

        # Phase 2: FAISS example retrieval (gold + regressions)
        logger.info(f"[{query_id}] Phase 2: Finding FAISS examples")
        examples = self._find_examples(sql, engine=engine, k=3)
        example_ids = [e.get('id', '?') for e in examples]
        logger.info(f"[{query_id}] FAISS examples: {example_ids}")

        # Also find regression warnings for structurally similar queries
        regression_warnings = self._find_regression_warnings(sql, engine=engine, k=2)
        if regression_warnings:
            reg_ids = [r.get('id', '?') for r in regression_warnings]
            logger.info(f"[{query_id}] Regression warnings: {reg_ids}")

        # Optional: LLM analyst (stubborn query mode)
        # Analyst sees FAISS picks and can swap them for better ones
        expert_analysis = None
        analysis_raw = None
        analysis_prompt_text = None
        if analyst_enabled:
            expert_analysis, analysis_raw, analysis_prompt_text, examples = self._run_analyst(
                query_id=query_id,
                sql=sql,
                dag=dag,
                costs=costs,
                history=history,
                matched_examples=examples,
                engine=engine,
                dialect=dialect,
            )
            example_ids = [e.get('id', '?') for e in examples]

        # Load global learnings from benchmark runs
        global_learnings = self.learner.build_learning_summary() or None

        # Phase 3: Build full-query prompt + generate rewrites
        logger.info(f"[{query_id}] Phase 3: Generating {n_workers} candidates")

        prompt = self.prompter.build_prompt(
            query_id=query_id,
            full_sql=sql,
            dag=dag,
            costs=costs,
            history=history,
            examples=examples,
            expert_analysis=expert_analysis,
            global_learnings=global_learnings,
            regression_warnings=regression_warnings,
            dialect=dialect,
            semantic_intents=self.get_semantic_intents(query_id),
            engine_version=self._engine_version,
        )

        # Generate candidates via parallel workers
        from .generate import CandidateGenerator
        generator = CandidateGenerator(
            provider=self.provider,
            model=self.model,
            analyze_fn=self.analyze_fn,
        )

        candidates = generator.generate(
            sql=sql,
            prompt=prompt,
            examples_used=example_ids,
            n=n_workers,
            dialect=dialect,
        )

        # Pick best candidate
        optimized_sql = sql
        transforms = []
        best_response = None
        if candidates:
            best = None
            for cand in candidates:
                if not cand.error and cand.optimized_sql and cand.optimized_sql != sql:
                    best = cand
                    break
            if best:
                optimized_sql = best.optimized_sql
                transforms = best.transforms
                best_response = best.response

        # Phase 4: Validate (syntax check)
        logger.info(f"[{query_id}] Phase 4: Syntax check")
        try:
            import sqlglot
            sqlglot.parse_one(optimized_sql, dialect=dialect)
        except Exception as e:
            logger.warning(f"[{query_id}] Syntax error: {e}")
            # Fall back to original
            optimized_sql = sql

        # Phase 5: Validate (timing + correctness)
        logger.info(f"[{query_id}] Phase 5: Validating")
        status, speedup, val_errors, val_error_cat = self._validate(sql, optimized_sql)
        logger.info(f"[{query_id}] Result: {status} ({speedup:.2f}x)")
        if val_errors:
            logger.info(f"[{query_id}] Validation errors: {val_errors}")

        result = PipelineResult(
            query_id=query_id,
            status=status,
            speedup=speedup,
            original_sql=sql,
            optimized_sql=optimized_sql,
            transforms_applied=transforms,
            prompt=prompt,
            response=best_response,
            analysis=analysis_raw,
            analysis_prompt=analysis_prompt_text,
            analysis_formatted=expert_analysis,
        )

        # Create learning record
        try:
            lr = self.learner.create_learning_record(
                query_id=query_id,
                examples_recommended=example_ids,
                transforms_recommended=example_ids,
                status="pass" if status in ("WIN", "IMPROVED", "NEUTRAL") else "error",
                speedup=speedup,
                transforms_used=transforms,
                error_category=val_error_cat,
                error_messages=val_errors,
            )
            self.learner.save_learning_record(lr)
        except Exception as e:
            logger.warning(f"[{query_id}] Learning record failed: {e}")

        return result

    def run_state(
        self,
        state_num: int,
        n_workers: Optional[int] = None,
        query_ids: Optional[List[str]] = None,
        use_analyst: Optional[bool] = None,
    ) -> List[PipelineResult]:
        """Run all queries for a state.

        State 0: n_workers = config.workers_state_0 (discovery)
        State 1+: n_workers = config.workers_state_n (refinement)

        Args:
            state_num: State number (0 = discovery, 1+ = refinement)
            n_workers: Override worker count
            query_ids: Optional subset of queries to run
            use_analyst: Override pipeline-level analyst setting per-state

        Returns:
            List of PipelineResult for each query
        """
        if n_workers is None:
            n_workers = (
                self.config.workers_state_0
                if state_num == 0
                else self.config.workers_state_n
            )

        # Create state directory
        state_dir = self.benchmark_dir / f"state_{state_num}"
        for subdir in ["prompts", "responses", "validation"]:
            (state_dir / subdir).mkdir(parents=True, exist_ok=True)

        # Load queries
        queries = self._load_queries(query_ids, state_num=state_num)

        # Load history from previous states
        history = self._load_history(state_num)

        # Initialize store for this state
        store = Store(run_dir=state_dir)

        # Run each query
        results = []
        for query_id, sql in queries.items():
            logger.info(f"State {state_num}: Processing {query_id}")
            result = self.run_query(
                query_id=query_id,
                sql=sql,
                n_workers=n_workers,
                history=history.get(query_id),
                use_analyst=use_analyst,
            )
            results.append(result)

            # Save validation result
            validation_path = state_dir / "validation" / f"{query_id}.json"
            validation_path.write_text(json.dumps({
                "query_id": result.query_id,
                "status": result.status,
                "speedup": result.speedup,
                "nodes_rewritten": result.nodes_rewritten,
                "transforms_applied": result.transforms_applied,
                "original_sql": result.original_sql,
                "optimized_sql": result.optimized_sql,
            }, indent=2))

            # Save artifacts via store (prompt, response, SQL, validation)
            store.save_candidate(
                query_id=query_id,
                worker_id=0,
                prompt=result.prompt or "",
                response=result.response or "",
                optimized_sql=result.optimized_sql,
                validation={
                    "status": result.status,
                    "speedup": result.speedup,
                    "transforms": result.transforms_applied,
                },
            )
            # Keep top-level state artifacts populated for easy prompt/response review.
            (state_dir / "prompts" / f"{query_id}.txt").write_text(result.prompt or "")
            (state_dir / "responses" / f"{query_id}.txt").write_text(result.response or "")

            # Save analyst artifacts if present (audit trail)
            qdir = state_dir / query_id / "worker_00"
            if result.analysis_prompt:
                (qdir / "analysis_prompt.txt").write_text(result.analysis_prompt)
            if result.analysis:
                (qdir / "analysis_response.txt").write_text(result.analysis)
            if result.analysis_formatted:
                (qdir / "analysis_formatted.txt").write_text(result.analysis_formatted)

        # Save state leaderboard
        leaderboard = {
            r.query_id: {
                "status": r.status,
                "speedup": r.speedup,
                "transforms": r.transforms_applied,
            }
            for r in results
        }
        (state_dir / "leaderboard.json").write_text(
            json.dumps(leaderboard, indent=2)
        )

        # Update top-level benchmark leaderboard
        self._update_benchmark_leaderboard(results, state_num=state_num)

        # Save learning summary + generate history.json for analyst
        self.learner.save_learning_summary()
        self.learner.generate_benchmark_history(self.benchmark_dir)

        return results

    def run_analyst_session(
        self,
        query_id: str,
        sql: str,
        max_iterations: int = 3,
        target_speedup: float = 2.0,
        n_workers: int = 3,
    ) -> "AnalystSession":
        """Run iterative deep-dive optimization on a single query.

        Always optimizes from the ORIGINAL SQL with full history.
        Generates LLM failure analysis when speedup < target.
        Stops when target reached or max iterations exhausted.

        Args:
            query_id: Query identifier (e.g., 'query_88')
            sql: Original SQL query (never modified)
            max_iterations: Max optimization rounds (default 3)
            target_speedup: Stop early when this speedup is reached (default 2.0)
            n_workers: Parallel workers per iteration

        Returns:
            AnalystSession with all iterations and best result
        """
        from .analyst_session import AnalystSession

        session = AnalystSession(
            pipeline=self,
            query_id=query_id,
            original_sql=sql,
            max_iterations=max_iterations,
            target_speedup=target_speedup,
            n_workers=n_workers,
        )
        session.run()
        session.save_session()
        return session

    def run_optimization_session(
        self,
        query_id: str,
        sql: str,
        max_iterations: int = 3,
        target_speedup: float = 2.0,
        n_workers: int = 3,
        mode: OptimizationMode = OptimizationMode.EXPERT,
    ) -> SessionResult:
        """Run optimization session in specified mode.

        Args:
            query_id: Query identifier (e.g., 'query_88')
            sql: Original SQL query
            max_iterations: Max optimization rounds
            target_speedup: Stop early when this speedup is reached
            n_workers: Parallel workers per iteration
            mode: Optimization mode (standard, expert, swarm)

        Returns:
            SessionResult with the best result across all iterations
        """
        if mode == OptimizationMode.STANDARD:
            from .sessions.standard_session import StandardSession
            session = StandardSession(
                pipeline=self,
                query_id=query_id,
                original_sql=sql,
                target_speedup=target_speedup,
                max_iterations=1,
                n_workers=n_workers,
            )
        elif mode == OptimizationMode.EXPERT:
            from .sessions.expert_session import ExpertSession
            session = ExpertSession(
                pipeline=self,
                query_id=query_id,
                original_sql=sql,
                max_iterations=max_iterations,
                target_speedup=target_speedup,
                n_workers=1,  # Expert: always 1 worker, analyst-steered
            )
        elif mode == OptimizationMode.SWARM:
            from .sessions.swarm_session import SwarmSession
            session = SwarmSession(
                pipeline=self,
                query_id=query_id,
                original_sql=sql,
                max_iterations=max_iterations,
                target_speedup=target_speedup,
                n_workers=4,  # Always 4 in swarm mode
            )
        else:
            raise ValueError(f"Unknown optimization mode: {mode}")

        return session.run()

    def promote(self, state_num: int) -> str:
        """Create state_{N+1} from state_N with enriched promotion context.

        Winners (speedup >= promote_threshold):
            - Optimized SQL becomes next state's baseline query
            - LLM generates analysis of what worked and suggestions for next steps
            - Original SQL, optimized SQL, and analysis saved for the next state's prompt
        Non-winners:
            Original query carries forward unchanged

        Returns:
            Path to new state directory
        """
        state_dir = self.benchmark_dir / f"state_{state_num}"
        next_state_dir = self.benchmark_dir / f"state_{state_num + 1}"

        if not state_dir.exists():
            raise FileNotFoundError(f"State {state_num} not found: {state_dir}")

        # Create next state structure
        for subdir in ["prompts", "responses", "validation", "promotion_context"]:
            (next_state_dir / subdir).mkdir(parents=True, exist_ok=True)

        # Copy seed from current state (knowledge carries forward)
        current_seed = state_dir / "seed"
        next_seed = next_state_dir / "seed"
        if current_seed.exists() and not next_seed.exists():
            import shutil
            shutil.copytree(current_seed, next_seed)

        # Load state leaderboard
        lb_path = state_dir / "leaderboard.json"
        if lb_path.exists():
            leaderboard = json.loads(lb_path.read_text())
        else:
            leaderboard = {}

        # Load baseline queries
        queries_dir = self.benchmark_dir / "queries"

        # Promote winners: copy optimized SQL + generate analysis
        promoted = 0
        for query_id, entry in leaderboard.items():
            speedup = entry.get("speedup", 0)
            transforms = entry.get("transforms", [])

            if speedup >= self.config.promote_threshold:
                # Get winning optimized SQL from validation
                val_path = state_dir / "validation" / f"{query_id}.json"
                if not val_path.exists():
                    continue

                val_data = json.loads(val_path.read_text())
                opt_sql = val_data.get("optimized_sql", "")
                orig_sql = val_data.get("original_sql", "")

                if not opt_sql:
                    continue

                # If no original in validation, load from queries/
                if not orig_sql:
                    baseline_path = queries_dir / f"{query_id}.sql"
                    if baseline_path.exists():
                        orig_sql = baseline_path.read_text()

                # Save promoted SQL as next state's baseline
                (next_state_dir / f"{query_id}_promoted.sql").write_text(opt_sql)

                # Generate promotion analysis via LLM
                analysis = self._generate_promotion_analysis(
                    query_id=query_id,
                    original_sql=orig_sql,
                    optimized_sql=opt_sql,
                    speedup=speedup,
                    transforms=transforms,
                    state_num=state_num,
                )

                # Save promotion context for the next state's prompt
                ctx_path = next_state_dir / "promotion_context" / f"{query_id}.json"
                ctx_path.write_text(json.dumps(analysis.to_dict(), indent=2))

                promoted += 1
                logger.info(
                    f"  Promoted {query_id}: {speedup:.2f}x → "
                    f"optimized SQL + analysis → state_{state_num + 1}"
                )
                continue

            # Non-winner: carry original forward (no file needed, queries/ is shared)
            logger.debug(f"  {query_id}: {speedup:.2f}x → original carries forward")

        logger.info(
            f"Promoted {promoted}/{len(leaderboard)} queries from "
            f"state_{state_num} to state_{state_num + 1}"
        )

        return str(next_state_dir)

    def _generate_promotion_analysis(
        self,
        query_id: str,
        original_sql: str,
        optimized_sql: str,
        speedup: float,
        transforms: List[str],
        state_num: int,
    ) -> PromotionAnalysis:
        """Generate LLM analysis of what a winning transform did and what to try next.

        Args:
            query_id: Query identifier
            original_sql: The original SQL before optimization
            optimized_sql: The winning optimized SQL
            speedup: Measured speedup ratio
            transforms: Transforms that were applied
            state_num: State being promoted from

        Returns:
            PromotionAnalysis with LLM-generated reasoning and suggestions
        """
        prompt = self._build_promotion_analysis_prompt(
            query_id=query_id,
            original_sql=original_sql,
            optimized_sql=optimized_sql,
            speedup=speedup,
            transforms=transforms,
        )

        analysis_text = ""
        suggestions_text = ""

        try:
            from .generate import CandidateGenerator
            generator = CandidateGenerator(
                provider=self.provider,
                model=self.model,
                analyze_fn=self.analyze_fn,
            )
            response = generator._analyze(prompt)
            analysis_text, suggestions_text = self._parse_promotion_response(response)

        except Exception as e:
            logger.warning(f"[{query_id}] Promotion analysis LLM call failed: {e}")
            # Fallback: generate deterministic analysis from available data
            t_str = ", ".join(transforms) if transforms else "unknown transforms"
            analysis_text = (
                f"Applied {t_str} to achieve {speedup:.2f}x speedup. "
                f"The optimized query restructured the original to reduce "
                f"redundant computation."
            )
            suggestions_text = (
                f"The current {speedup:.2f}x improvement leaves room for further "
                f"optimization. Consider: additional predicate pushdown, "
                f"CTE materialization boundaries, or join reordering."
            )

        return PromotionAnalysis(
            query_id=query_id,
            original_sql=original_sql,
            optimized_sql=optimized_sql,
            speedup=speedup,
            transforms=transforms,
            analysis=analysis_text,
            suggestions=suggestions_text,
            state_promoted_from=state_num,
        )

    @staticmethod
    def _build_promotion_analysis_prompt(
        query_id: str,
        original_sql: str,
        optimized_sql: str,
        speedup: float,
        transforms: List[str],
    ) -> str:
        """Build prompt asking LLM to analyze a winning optimization."""
        t_str = ", ".join(transforms) if transforms else "unknown"
        return (
            "You are a SQL performance analyst reviewing a successful query optimization.\n"
            "\n"
            f"## Query: {query_id}\n"
            f"## Measured Speedup: {speedup:.2f}x\n"
            f"## Transforms Applied: {t_str}\n"
            "\n"
            "### Original SQL (BEFORE):\n"
            f"```sql\n{original_sql}\n```\n"
            "\n"
            "### Optimized SQL (AFTER):\n"
            f"```sql\n{optimized_sql}\n```\n"
            "\n"
            "## Task\n"
            "\n"
            "Provide two sections:\n"
            "\n"
            "### ANALYSIS\n"
            "Explain specifically what structural changes were made and WHY they\n"
            "improved performance. Reference specific clauses, joins, or subqueries.\n"
            "Be concrete — don't just name the transform, explain the mechanism\n"
            "(e.g., 'moved the date filter into a CTE so the hash join probes a\n"
            "100-row table instead of 73K rows').\n"
            "\n"
            "### SUGGESTIONS\n"
            "Based on what remains in the optimized query, suggest 2-3 specific\n"
            "further optimizations that could yield additional speedup. For each:\n"
            "- Name the technique\n"
            "- Identify the specific clause/join/subquery to target\n"
            "- Estimate the potential impact (minor/moderate/significant)\n"
            "\n"
            "Focus on opportunities the current optimization did NOT address.\n"
            "Do NOT suggest re-applying transforms that were already applied.\n"
        )

    @staticmethod
    def _parse_promotion_response(response: str) -> tuple[str, str]:
        """Parse promotion analysis LLM response into (analysis, suggestions)."""
        analysis = ""
        suggestions = ""

        # Split on section headers
        import re

        # Try to find ANALYSIS section
        analysis_match = re.search(
            r'###?\s*ANALYSIS\s*\n(.*?)(?=###?\s*SUGGESTION|$)',
            response, re.DOTALL | re.IGNORECASE
        )
        if analysis_match:
            analysis = analysis_match.group(1).strip()

        # Try to find SUGGESTIONS section
        suggestions_match = re.search(
            r'###?\s*SUGGESTION[S]?\s*\n(.*?)$',
            response, re.DOTALL | re.IGNORECASE
        )
        if suggestions_match:
            suggestions = suggestions_match.group(1).strip()

        # Fallback: if no sections found, use the whole response as analysis
        if not analysis and not suggestions:
            parts = response.strip().split("\n\n", 1)
            analysis = parts[0]
            suggestions = parts[1] if len(parts) > 1 else ""

        return analysis, suggestions

    # =========================================================================
    # Analyst (opt-in, stubborn query mode)
    # =========================================================================

    def _run_analyst(
        self,
        query_id: str,
        sql: str,
        dag: Any,
        costs: Dict[str, Any],
        history: Optional[Dict[str, Any]],
        matched_examples: List[Dict[str, Any]],
        engine: str,
        dialect: str,
    ) -> tuple[Optional[str], Optional[str], Optional[str], List[Dict[str, Any]]]:
        """Run LLM analyst to generate deep structural analysis.

        The analyst sees the tag-matched examples and the full list of
        available gold examples. It can accept the picks or recommend
        swaps if it thinks different examples would be more relevant.

        Costs 1 extra API call. Only use for stubborn queries.

        Returns:
            (formatted_analysis, raw_response, analysis_prompt, final_examples) —
            final_examples may differ from matched_examples if the analyst
            recommended swaps. Returns (None, None, None, matched_examples) on failure.
        """
        from .analyst import (
            build_analysis_prompt,
            format_analysis_for_prompt,
            parse_analysis_response,
            parse_example_overrides,
        )

        logger.info(f"[{query_id}] Running LLM analyst (stubborn query mode)")

        # Load benchmark learnings for the analyst
        history_path = self.benchmark_dir / "history.json"
        effective_patterns = None
        known_regressions = None
        if history_path.exists():
            try:
                hdata = json.loads(history_path.read_text())
                learnings = hdata.get("cumulative_learnings", {})
                effective_patterns = learnings.get("effective_patterns")
                known_regressions = learnings.get("known_regressions")
            except Exception:
                pass

        # Build catalogue of all available gold examples (id + description)
        available_examples = self._list_gold_examples(engine)

        # Build the analysis prompt
        analysis_prompt = build_analysis_prompt(
            query_id=query_id,
            sql=sql,
            dag=dag,
            costs=costs,
            history=history,
            effective_patterns=effective_patterns,
            known_regressions=known_regressions,
            faiss_picks=[e.get("id", "?") for e in matched_examples],
            available_examples=available_examples,
            dialect=dialect,
        )

        # Send to LLM
        try:
            from .generate import CandidateGenerator
            generator = CandidateGenerator(
                provider=self.provider,
                model=self.model,
                analyze_fn=self.analyze_fn,
            )
            raw_response = generator._analyze(analysis_prompt)
        except Exception as e:
            logger.warning(f"[{query_id}] Analyst LLM call failed: {e}")
            return None, None, analysis_prompt, matched_examples

        # Parse and format for injection into rewrite prompt
        parsed = parse_analysis_response(raw_response)
        formatted = format_analysis_for_prompt(parsed)

        # Check if analyst wants different examples
        overrides = parse_example_overrides(raw_response)
        final_examples = matched_examples
        if overrides:
            logger.info(f"[{query_id}] Analyst overrides matched picks: {overrides}")
            final_examples = self._load_examples_by_id(overrides, engine)
            if not final_examples:
                final_examples = matched_examples  # fallback if load fails

        logger.info(
            f"[{query_id}] Analyst complete: "
            f"{len(raw_response)} chars response, "
            f"{len(parsed) - 1} sections parsed, "
            f"examples: {[e.get('id', '?') for e in final_examples]}"
        )

        return formatted, raw_response, analysis_prompt, final_examples

    def _list_gold_examples(self, engine: str) -> List[Dict[str, str]]:
        """List all available gold examples with id + short description.

        Used to give the analyst a menu it can pick from.
        """
        engine_dir = "postgres" if engine in ("postgresql", "postgres") else engine
        examples_dir = Path(__file__).resolve().parent / "examples" / engine_dir
        result = []

        if not examples_dir.exists():
            return result

        for path in sorted(examples_dir.glob("*.json")):
            try:
                data = json.loads(path.read_text())
                ex_id = data.get("id", path.stem)
                speedup = data.get("verified_speedup", "?")
                desc = data.get("description", "")[:80]
                result.append({"id": ex_id, "speedup": speedup, "description": desc})
            except Exception:
                continue

        return result

    def _load_examples_by_id(
        self,
        example_ids: List[str],
        engine: str,
    ) -> List[Dict[str, Any]]:
        """Load gold examples by their IDs."""
        examples = []
        for ex_id in example_ids:
            ex = self.prompter.load_example_for_pattern(
                ex_id, engine=engine, seed_dirs=self._seed_dirs,
            )
            if ex:
                examples.append(ex)
        return examples

    # =========================================================================
    # Helpers
    # =========================================================================

    def _load_queries(
        self,
        query_ids: Optional[List[str]] = None,
        state_num: int = 0,
    ) -> Dict[str, str]:
        """Load queries from benchmark queries/ directory.

        For state > 0, check if promoted SQL exists from the previous state
        and use that as the baseline instead of the original.
        """
        queries_dir = self.benchmark_dir / "queries"
        if not queries_dir.exists():
            return {}

        queries = {}
        for path in sorted(queries_dir.glob("*.sql")):
            qid = path.stem
            if query_ids and qid not in query_ids:
                continue
            queries[qid] = path.read_text()

        # For state > 0, override with promoted SQL from previous state
        if state_num > 0:
            prev_state_dir = self.benchmark_dir / f"state_{state_num - 1}"
            for qid in list(queries.keys()):
                promoted = prev_state_dir / f"{qid}_promoted.sql"
                if promoted.exists():
                    queries[qid] = promoted.read_text()
                    logger.info(
                        f"Using promoted SQL for {qid} from state_{state_num - 1}"
                    )

        return queries

    def _load_history(
        self,
        state_num: int,
    ) -> Dict[str, Any]:
        """Load cumulative history from all previous states.

        Returns dict keyed by query_id. Each value contains:
        - 'attempts': list of per-state validation results
        - 'promotion': PromotionAnalysis if query was promoted (most recent)
        """
        history: Dict[str, Any] = {}

        for s in range(state_num):
            state_dir = self.benchmark_dir / f"state_{s}"

            # Load validation results
            validation_dir = state_dir / "validation"
            if validation_dir.exists():
                for path in validation_dir.glob("*.json"):
                    try:
                        data = json.loads(path.read_text())
                        qid = data.get("query_id", path.stem)
                        if qid not in history:
                            history[qid] = {"attempts": [], "promotion": None}

                        history[qid]["attempts"].append({
                            "state": s,
                            "status": data.get("status", "unknown"),
                            "speedup": data.get("speedup", 0),
                            "transforms": data.get("transforms_applied", []),
                            "original_sql": data.get("original_sql", ""),
                            "optimized_sql": data.get("optimized_sql", ""),
                        })
                    except Exception:
                        continue

            # Load promotion context from the NEXT state's promotion_context/
            # (promote(s) creates state_{s+1}/promotion_context/)
            next_state_dir = self.benchmark_dir / f"state_{s + 1}"
            promo_dir = next_state_dir / "promotion_context"
            if promo_dir.exists():
                for path in promo_dir.glob("*.json"):
                    try:
                        data = json.loads(path.read_text())
                        qid = data.get("query_id", path.stem)
                        if qid not in history:
                            history[qid] = {"attempts": [], "promotion": None}
                        history[qid]["promotion"] = PromotionAnalysis.from_dict(data)
                    except Exception:
                        continue

        return history

    @staticmethod
    def _parse_speedup(raw) -> float:
        """Parse verified_speedup which may be float or string like '2.92x'."""
        if isinstance(raw, (int, float)):
            return float(raw)
        if isinstance(raw, str):
            try:
                return float(raw.rstrip("xX"))
            except ValueError:
                return 0.0
        return 0.0

    def _find_examples(
        self,
        sql: str,
        engine: str = "duckdb",
        k: int = 3,
    ) -> List[Dict[str, Any]]:
        """Find gold examples via tag-based similarity matching.

        Extracts SQL tags (table names, keywords, structural patterns)
        and ranks examples by tag overlap count.

        Engine-specific: only returns examples for the current engine.
        DuckDB queries get DuckDB examples, PostgreSQL gets PostgreSQL, etc.

        Returns up to k gold examples sorted by similarity.
        Only returns type=gold examples; regressions are handled separately
        by _find_regression_warnings().
        """
        from .knowledge import ADOFAISSRecommender

        engine_dir = "postgres" if engine in ("postgresql", "postgres") else engine
        dialect = "postgres" if engine_dir == "postgres" else engine_dir
        examples = []
        seen_ids = set()

        recommender = ADOFAISSRecommender()
        if recommender._initialized:
            matches = recommender.find_similar_examples(sql, k=k * 5, dialect=dialect)
            for ex_id, score, meta in matches:
                if len(examples) >= k:
                    break
                if ex_id in seen_ids:
                    continue

                seen_ids.add(ex_id)
                ex_data = self.prompter.load_example_for_pattern(
                    ex_id, engine=engine, seed_dirs=self._seed_dirs,
                )
                if ex_data:
                    examples.append(ex_data)

        return examples

    def _find_regression_warnings(
        self,
        sql: str,
        engine: str = "duckdb",
        k: int = 2,
        min_similarity: float = 0.1,
    ) -> List[Dict[str, Any]]:
        """Find regression examples via tag-based similarity matching.

        Returns regression examples where structurally similar queries
        were rewritten and REGRESSED. These serve as anti-patterns in
        the prompt so the LLM avoids repeating the same mistakes.

        Args:
            sql: Query SQL to match against
            engine: Database engine for filtering
            k: Max regression warnings to return
            min_similarity: Minimum tag similarity score (0-1)

        Returns:
            List of regression example dicts with regression_mechanism
        """
        from .knowledge import ADOFAISSRecommender

        engine_dir = "postgres" if engine in ("postgresql", "postgres") else engine
        dialect = "postgres" if engine_dir == "postgres" else engine_dir
        regressions = []
        seen_ids = set()

        recommender = ADOFAISSRecommender()
        if recommender._initialized:
            matches = recommender.find_relevant_regressions(sql, k=k * 5, dialect=dialect)
            for ex_id, score, meta in matches:
                if len(regressions) >= k:
                    break
                if score < min_similarity or ex_id in seen_ids:
                    continue

                seen_ids.add(ex_id)

                # Load regression example from the regressions/ subdir
                reg_data = self._load_regression_example(ex_id, engine_dir)
                if reg_data:
                    reg_data["_tag_score"] = score
                    regressions.append(reg_data)

        return regressions

    @staticmethod
    def _load_regression_example(
        example_id: str, engine_dir: str,
    ) -> Optional[Dict[str, Any]]:
        """Load a regression example JSON file."""
        from .node_prompter import EXAMPLES_DIR

        regressions_dir = EXAMPLES_DIR / engine_dir / "regressions"
        if not regressions_dir.exists():
            return None

        # Try exact filename match
        path = regressions_dir / f"{example_id}.json"
        if path.exists():
            try:
                return json.loads(path.read_text())
            except Exception:
                pass

        # Search by id field
        for p in regressions_dir.glob("*.json"):
            try:
                data = json.loads(p.read_text())
                if data.get("id") == example_id:
                    return data
            except Exception:
                continue

        return None

    def load_global_knowledge(self) -> Optional[Dict[str, Any]]:
        """Load GlobalKnowledge from benchmarks/*/knowledge/*.json.

        GlobalKnowledge is built by build_blackboard.py phase3_global()
        and contains verified optimization principles + anti-patterns.

        Returns:
            Dict with 'principles' and 'anti_patterns' lists, or None
        """
        # Search in knowledge/ subdirectory
        knowledge_dir = self.benchmark_dir / "knowledge"
        if knowledge_dir.exists():
            for path in sorted(knowledge_dir.glob("*.json")):
                try:
                    data = json.loads(path.read_text())
                    if data.get("principles") or data.get("anti_patterns"):
                        logger.info(
                            f"Loaded GlobalKnowledge from {path.name}: "
                            f"{len(data.get('principles', []))} principles, "
                            f"{len(data.get('anti_patterns', []))} anti-patterns"
                        )
                        return data
                except Exception as e:
                    logger.warning(f"Failed to load knowledge {path}: {e}")

        # Fallback: check parent benchmark dirs
        for parent in [self.benchmark_dir.parent]:
            knowledge_dir = parent / "knowledge"
            if knowledge_dir.exists():
                for path in sorted(knowledge_dir.glob("*.json")):
                    try:
                        data = json.loads(path.read_text())
                        if data.get("principles") or data.get("anti_patterns"):
                            return data
                    except Exception:
                        continue

        return None

    def load_query(self, query_id: str) -> Optional[str]:
        """Load a single query by ID."""
        queries_dir = self.benchmark_dir / "queries"
        path = queries_dir / f"{query_id}.sql"
        if path.exists():
            return path.read_text()
        return None

    def _load_benchmark_leaderboard(self) -> dict:
        """Load benchmark leaderboard as dict keyed by query_id.

        Handles all formats: standard wrapper, bare list, legacy dict.
        """
        lb_path = self.benchmark_dir / "leaderboard.json"
        if not lb_path.exists():
            return {}
        try:
            data = json.loads(lb_path.read_text())
            if isinstance(data, dict) and "queries" in data:
                return {e["query_id"]: e for e in data["queries"]}
            elif isinstance(data, list):
                return {e["query_id"]: e for e in data}
            elif isinstance(data, dict):
                return data
        except Exception:
            pass
        return {}

    def _save_benchmark_leaderboard(self, queries_dict: dict) -> None:
        """Save benchmark leaderboard in standard format.

        Standard format: {benchmark, engine, scale_factor, updated_at, summary, queries: []}
        """
        from datetime import datetime

        lb_path = self.benchmark_dir / "leaderboard.json"
        queries = sorted(queries_dict.values(), key=lambda e: -e.get("speedup", 0))

        speedups = [q["speedup"] for q in queries if q.get("speedup", 0) > 0]
        summary = {
            "total": len(queries),
            "wins": sum(1 for q in queries if q.get("status") == "WIN"),
            "improved": sum(1 for q in queries if q.get("status") == "IMPROVED"),
            "neutral": sum(1 for q in queries if q.get("status") == "NEUTRAL"),
            "regression": sum(1 for q in queries if q.get("status") == "REGRESSION"),
            "errors": sum(1 for q in queries if q.get("status") in ("ERROR", "error")),
            "avg_speedup": round(sum(speedups) / len(speedups), 4) if speedups else 0,
        }

        standard = {
            "benchmark": self.config.benchmark,
            "engine": self.config.engine,
            "scale_factor": self.config.scale_factor,
            "updated_at": datetime.now().isoformat(),
            "summary": summary,
            "queries": queries,
        }
        lb_path.write_text(json.dumps(standard, indent=2))
        return queries

    def _update_benchmark_leaderboard(
        self,
        results: List[PipelineResult],
        state_num: int = 0,
    ) -> None:
        """Merge state results into the top-level benchmark leaderboard.

        Updates leaderboard.json with improved results (better speedup wins).
        Tracks all attempts per query for learning from regressions.
        Regenerates leaderboard.md as human-readable view.
        """
        existing = self._load_benchmark_leaderboard()

        # Merge: keep the better result per query + track all attempts
        for r in results:
            entry = existing.get(r.query_id, {})
            prev_speedup = entry.get("speedup", 0) if isinstance(entry, dict) else 0

            # Append this attempt to history
            attempts = entry.get("attempts", []) if isinstance(entry, dict) else []
            attempts.append({
                "state": state_num,
                "status": r.status,
                "speedup": r.speedup,
                "transforms": r.transforms_applied,
            })

            # Update top-level if this is the best result
            if r.speedup > prev_speedup or r.query_id not in existing:
                existing[r.query_id] = {
                    "query_id": r.query_id,
                    "status": r.status,
                    "speedup": r.speedup,
                    "transforms": r.transforms_applied,
                    "source": f"state_{state_num}",
                    "original_sql": r.original_sql,
                    "optimized_sql": r.optimized_sql,
                    "nodes_rewritten": r.nodes_rewritten,
                    "attempts": attempts,
                }
            else:
                # Keep existing best but update attempts list
                entry["attempts"] = attempts
                existing[r.query_id] = entry

        # Save in standard format and regenerate md
        queries = self._save_benchmark_leaderboard(existing)
        self._regenerate_leaderboard_md(queries)

    def _regenerate_leaderboard_md(self, leaderboard: List[Dict]) -> None:
        """Regenerate human-readable leaderboard.md from leaderboard.json."""
        md_path = self.benchmark_dir / "leaderboard.md"

        wins = [e for e in leaderboard if e.get("status") == "WIN"]
        improved = [e for e in leaderboard if e.get("status") == "IMPROVED"]
        neutral = [e for e in leaderboard if e.get("status") == "NEUTRAL"]
        regressions = [e for e in leaderboard if e.get("status") == "REGRESSION"]
        errors = [e for e in leaderboard if e.get("status") == "ERROR"]

        lines = [
            f"# {self.config.benchmark.upper()} {self.config.engine.title()} Leaderboard",
            "",
            f"**Engine:** {self.config.engine} | "
            f"**SF:** {self.config.scale_factor} | "
            f"**Queries:** {len(leaderboard)}",
            "",
            "## Summary",
            "",
            "| Category | Count | % |",
            "|----------|------:|--:|",
            f"| WIN (>=1.1x) | {len(wins)} | {100*len(wins)//max(len(leaderboard),1)}% |",
            f"| IMPROVED (>=1.05x) | {len(improved)} | {100*len(improved)//max(len(leaderboard),1)}% |",
            f"| NEUTRAL | {len(neutral)} | {100*len(neutral)//max(len(leaderboard),1)}% |",
            f"| REGRESSION | {len(regressions)} | {100*len(regressions)//max(len(leaderboard),1)}% |",
            f"| ERROR | {len(errors)} | {100*len(errors)//max(len(leaderboard),1)}% |",
            "",
        ]

        if wins:
            lines.append("## Top Winners")
            lines.append("")
            lines.append("| Query | Speedup | Transforms |")
            lines.append("|-------|--------:|------------|")
            for e in sorted(wins, key=lambda x: -x.get("speedup", 0)):
                t = ", ".join(e.get("transforms", [])[:3]) if isinstance(e.get("transforms"), list) else str(e.get("transforms", ""))
                lines.append(f"| {e['query_id']} | **{e.get('speedup', 0):.2f}x** | {t} |")
            lines.append("")

        if regressions:
            lines.append("## Regressions")
            lines.append("")
            lines.append("| Query | Speedup | Transforms |")
            lines.append("|-------|--------:|------------|")
            for e in sorted(regressions, key=lambda x: x.get("speedup", 0)):
                t = ", ".join(e.get("transforms", [])[:3]) if isinstance(e.get("transforms"), list) else str(e.get("transforms", ""))
                lines.append(f"| {e['query_id']} | {e.get('speedup', 0):.2f}x | {t} |")
            lines.append("")

        lines.append("## All Queries")
        lines.append("")
        lines.append("| Query | Status | Speedup | Transforms | Attempts |")
        lines.append("|-------|--------|--------:|------------|:--------:|")
        for e in leaderboard:
            t = ", ".join(e.get("transforms", [])[:3]) if isinstance(e.get("transforms"), list) else str(e.get("transforms", ""))
            n_attempts = len(e.get("attempts", []))
            attempts_str = str(n_attempts) if n_attempts else "1"
            lines.append(
                f"| {e.get('query_id', '')} | {e.get('status', '')} "
                f"| {e.get('speedup', 0):.2f}x | {t} | {attempts_str} |"
            )

        md_path.write_text("\n".join(lines) + "\n")

"""5-phase DAG pipeline orchestrator.

Phases:
1. Parse:     SQL → DAG (deterministic, DagBuilder)
2. Annotate:  DAG → {node: pattern} (1 LLM call, Annotator)
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

from .annotator import Annotator
from .learn import Learner
from .node_prompter import Prompter
from .schemas import (
    AnnotationResult,
    BenchmarkConfig,
    PipelineResult,
    PromotionAnalysis,
)
from .store import Store

logger = logging.getLogger(__name__)


class Pipeline:
    """5-phase DAG pipeline with contract propagation and state management."""

    def __init__(
        self,
        benchmark_dir: str | Path,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        analyze_fn=None,
        annotate_with_llm: bool = False,
        use_analyst: bool = False,
    ):
        """Load config.json from benchmark dir, initialize components.

        Args:
            benchmark_dir: Path to benchmark directory (e.g., ado/benchmarks/duckdb_tpcds)
            provider: LLM provider for generation
            model: LLM model name
            analyze_fn: Optional custom LLM function
            annotate_with_llm: If True, Phase 2 uses LLM. Default False (heuristic).
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
        self.annotate_with_llm = annotate_with_llm
        self.use_analyst = use_analyst

        # Initialize pipeline components
        self.annotator = Annotator(
            provider=provider, model=model, analyze_fn=analyze_fn
        )
        self.prompter = Prompter()
        self.learner = Learner(
            journal_dir=self.benchmark_dir / "learning"
        )

        # Seed dirs: state_0/seed/ for unverified catalog rules
        self._seed_dirs = []
        seed_dir = self.benchmark_dir / "state_0" / "seed"
        if seed_dir.exists():
            self._seed_dirs.append(seed_dir)

    # =========================================================================
    # Phase 1: Parse SQL → DAG
    # =========================================================================

    @staticmethod
    def _parse_dag(sql: str, dialect: str = "duckdb"):
        """Phase 1: Parse SQL into DAG structure.

        Returns (dag, costs) tuple.
        """
        from qt_sql.optimization.dag_v2 import DagBuilder, CostAnalyzer

        builder = DagBuilder(sql, dialect=dialect)
        dag = builder.build()

        cost_analyzer = CostAnalyzer(dag)
        costs = cost_analyzer.analyze()

        return dag, costs

    # =========================================================================
    # Phase 2: Annotate → {node: pattern}
    # =========================================================================

    def _annotate(self, dag, costs) -> AnnotationResult:
        """Phase 2: Get pattern assignments from annotator.

        Uses heuristic by default. Set annotate_with_llm=True on Pipeline
        to use LLM annotation (costs 1 API call per query).
        """
        available_patterns = self._get_available_patterns()
        return self.annotator.annotate(
            dag, costs, available_patterns, use_llm=self.annotate_with_llm,
        )

    def _get_available_patterns(self) -> List[str]:
        """Get list of available pattern names from gold examples + seed rules.

        Searches:
        1. ado/examples/<engine>/  (gold verified for this DB)
        2. ado/examples/*/         (gold for other DBs)
        3. state_0/seed/           (unverified catalog rules)
        """
        patterns = set()
        base = Path(__file__).resolve().parent
        examples_dir = base / "examples"

        # Map engine to example subdir
        engine_dir = (
            "postgres"
            if self.config.engine in ("postgresql", "postgres")
            else self.config.engine
        )

        # Gold examples for this DB first
        primary = examples_dir / engine_dir
        if primary.exists():
            for p in primary.glob("*.json"):
                patterns.add(p.stem)

        # Gold examples from other DBs
        if examples_dir.exists():
            for subdir in examples_dir.iterdir():
                if subdir.is_dir() and subdir != primary:
                    for p in subdir.glob("*.json"):
                        patterns.add(p.stem)

        # Seed rules from state_0
        for seed_dir in self._seed_dirs:
            for p in seed_dir.glob("*.json"):
                patterns.add(p.stem)

        return sorted(patterns)

    # =========================================================================
    # Phase 5: Validate
    # =========================================================================

    def _validate(
        self,
        original_sql: str,
        optimized_sql: str,
    ) -> tuple[str, float]:
        """Phase 5: Validate the optimized SQL.

        Returns (status, speedup) tuple.
        Status: WIN | IMPROVED | NEUTRAL | REGRESSION | ERROR
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

            if result.status.value == "error":
                return "ERROR", 0.0
            elif result.status.value == "fail":
                return "ERROR", 0.0
            elif speedup >= 1.10:
                return "WIN", speedup
            elif speedup >= 1.05:
                return "IMPROVED", speedup
            elif speedup >= 0.95:
                return "NEUTRAL", speedup
            else:
                return "REGRESSION", speedup
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return "ERROR", 0.0
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
        dag, costs = self._parse_dag(sql, dialect=dialect)
        logger.info(
            f"[{query_id}] DAG: {len(dag.nodes)} nodes, "
            f"{len(dag.edges)} edges"
        )

        # Phase 2: Annotate
        logger.info(f"[{query_id}] Phase 2: Annotating nodes")
        annotation = self._annotate(dag, costs)
        flagged = [a.node_id for a in annotation.rewrites]
        logger.info(f"[{query_id}] Flagged for rewrite: {flagged}")

        if not annotation.rewrites:
            return PipelineResult(
                query_id=query_id,
                status="NEUTRAL",
                speedup=1.0,
                original_sql=sql,
                optimized_sql=sql,
                annotation=annotation,
            )

        # Phase 3: Build full-query prompt + generate rewrites
        logger.info(f"[{query_id}] Phase 3: Generating {n_workers} candidates")

        # FAISS: find structurally similar gold examples
        examples = self._find_examples(sql, engine=engine, k=3)
        logger.info(
            f"[{query_id}] FAISS examples: "
            f"{[e.get('id', '?') for e in examples]}"
        )

        # Optional: LLM analyst (stubborn query mode)
        # Analyst sees FAISS picks and can swap them for better ones
        expert_analysis = None
        analysis_raw = None
        if analyst_enabled:
            expert_analysis, analysis_raw, examples = self._run_analyst(
                query_id=query_id,
                sql=sql,
                dag=dag,
                costs=costs,
                history=history,
                faiss_examples=examples,
                engine=engine,
                dialect=dialect,
            )

        # Build full-query prompt with DAG topology
        prompt = self.prompter.build_prompt(
            query_id=query_id,
            full_sql=sql,
            dag=dag,
            costs=costs,
            annotation=annotation,
            history=history,
            examples=examples,
            expert_analysis=expert_analysis,
            dialect=dialect,
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
            examples_used=[a.pattern for a in annotation.rewrites],
            n=n_workers,
            dialect=dialect,
        )

        # Pick best candidate
        optimized_sql = sql
        transforms = [a.pattern for a in annotation.rewrites]
        if candidates:
            best = None
            for cand in candidates:
                if not cand.error and cand.optimized_sql and cand.optimized_sql != sql:
                    best = cand
                    break
            if best:
                optimized_sql = best.optimized_sql

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
        status, speedup = self._validate(sql, optimized_sql)
        logger.info(f"[{query_id}] Result: {status} ({speedup:.2f}x)")

        result = PipelineResult(
            query_id=query_id,
            status=status,
            speedup=speedup,
            original_sql=sql,
            optimized_sql=optimized_sql,
            nodes_rewritten=flagged,
            transforms_applied=transforms,
            annotation=annotation,
            prompt=prompt,
            analysis=analysis_raw,
        )

        # Create learning record
        try:
            error_cat = "execution" if status == "ERROR" else None
            lr = self.learner.create_learning_record(
                query_id=query_id,
                examples_recommended=[
                    a.pattern for a in annotation.rewrites
                ],
                transforms_recommended=[
                    a.pattern for a in annotation.rewrites
                ],
                status="pass" if status in ("WIN", "IMPROVED", "NEUTRAL") else "error",
                speedup=speedup,
                transforms_used=transforms,
                error_category=error_cat,
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
                response="",
                optimized_sql=result.optimized_sql,
                validation={
                    "status": result.status,
                    "speedup": result.speedup,
                    "transforms": result.transforms_applied,
                },
            )

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
        self._update_benchmark_leaderboard(results)

        # Save learning summary
        self.learner.save_learning_summary()

        return results

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
        faiss_examples: List[Dict[str, Any]],
        engine: str,
        dialect: str,
    ) -> tuple[Optional[str], Optional[str], List[Dict[str, Any]]]:
        """Run LLM analyst to generate deep structural analysis.

        The analyst sees the FAISS-selected examples and the full list of
        available gold examples. It can accept the FAISS picks or recommend
        swaps if it thinks different examples would be more relevant.

        Costs 1 extra API call. Only use for stubborn queries.

        Returns:
            (formatted_analysis, raw_response, final_examples) —
            final_examples may differ from faiss_examples if the analyst
            recommended swaps. Returns (None, None, faiss_examples) on failure.
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
            faiss_picks=[e.get("id", "?") for e in faiss_examples],
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
            return None, None, faiss_examples

        # Parse and format for injection into rewrite prompt
        parsed = parse_analysis_response(raw_response)
        formatted = format_analysis_for_prompt(parsed)

        # Check if analyst wants different examples
        overrides = parse_example_overrides(raw_response)
        final_examples = faiss_examples
        if overrides:
            logger.info(f"[{query_id}] Analyst overrides FAISS picks: {overrides}")
            final_examples = self._load_examples_by_id(overrides, engine)
            if not final_examples:
                final_examples = faiss_examples  # fallback if load fails

        logger.info(
            f"[{query_id}] Analyst complete: "
            f"{len(raw_response)} chars response, "
            f"{len(parsed) - 1} sections parsed, "
            f"examples: {[e.get('id', '?') for e in final_examples]}"
        )

        return formatted, raw_response, final_examples

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
        """Find gold examples via FAISS structural similarity.

        Fingerprints the SQL (literals → placeholders, lowercase identifiers),
        vectorizes the AST into 90 features, and finds the nearest gold
        examples in the FAISS index.

        Engine-specific: only returns examples for the current engine.
        DuckDB queries get DuckDB examples, PostgreSQL gets PostgreSQL, etc.

        Returns up to k examples sorted by similarity.
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
                if score <= 0.0 or ex_id in seen_ids:
                    continue

                # Strict engine filtering: only this engine's examples
                ex_engine = meta.get("engine", "unknown")
                if ex_engine != engine_dir:
                    continue

                seen_ids.add(ex_id)
                ex_data = self.prompter.load_example_for_pattern(
                    ex_id, engine=engine, seed_dirs=self._seed_dirs,
                )
                if ex_data:
                    examples.append(ex_data)

        return examples

    def load_query(self, query_id: str) -> Optional[str]:
        """Load a single query by ID."""
        queries_dir = self.benchmark_dir / "queries"
        path = queries_dir / f"{query_id}.sql"
        if path.exists():
            return path.read_text()
        return None

    def _update_benchmark_leaderboard(
        self,
        results: List[PipelineResult],
    ) -> None:
        """Merge state results into the top-level benchmark leaderboard.

        Updates leaderboard.json with improved results (better speedup wins).
        Regenerates leaderboard.md as human-readable view.
        """
        lb_path = self.benchmark_dir / "leaderboard.json"

        # Load existing leaderboard
        existing = {}
        if lb_path.exists():
            try:
                data = json.loads(lb_path.read_text())
                if isinstance(data, list):
                    existing = {e["query_id"]: e for e in data}
                elif isinstance(data, dict):
                    existing = data
            except Exception:
                pass

        # Merge: keep the better result per query
        for r in results:
            prev = existing.get(r.query_id, {})
            prev_speedup = prev.get("speedup", 0) if isinstance(prev, dict) else 0

            if r.speedup > prev_speedup or r.query_id not in existing:
                existing[r.query_id] = {
                    "query_id": r.query_id,
                    "status": r.status,
                    "speedup": r.speedup,
                    "transforms": r.transforms_applied,
                    "original_sql": r.original_sql,
                    "optimized_sql": r.optimized_sql,
                    "nodes_rewritten": r.nodes_rewritten,
                }

        # Save as list sorted by query_id
        lb_list = sorted(existing.values(), key=lambda e: e.get("query_id", ""))
        lb_path.write_text(json.dumps(lb_list, indent=2))

        # Regenerate leaderboard.md
        self._regenerate_leaderboard_md(lb_list)

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
        lines.append("| Query | Status | Speedup | Transforms |")
        lines.append("|-------|--------|--------:|------------|")
        for e in leaderboard:
            t = ", ".join(e.get("transforms", [])[:3]) if isinstance(e.get("transforms"), list) else str(e.get("transforms", ""))
            lines.append(
                f"| {e.get('query_id', '')} | {e.get('status', '')} "
                f"| {e.get('speedup', 0):.2f}x | {t} |"
            )

        md_path.write_text("\n".join(lines) + "\n")

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
    ):
        """Load config.json from benchmark dir, initialize components.

        Args:
            benchmark_dir: Path to benchmark directory (e.g., ado/benchmarks/duckdb_tpcds)
            provider: LLM provider for generation
            model: LLM model name
            analyze_fn: Optional custom LLM function
            annotate_with_llm: If True, Phase 2 uses LLM. Default False (heuristic).
        """
        self.benchmark_dir = Path(benchmark_dir)
        self.config = BenchmarkConfig.from_file(
            self.benchmark_dir / "config.json"
        )

        self.provider = provider
        self.model = model
        self.analyze_fn = analyze_fn
        self.annotate_with_llm = annotate_with_llm

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
        history: Optional[List[Dict]] = None,
    ) -> PipelineResult:
        """Run a single query through all 5 phases.

        Args:
            query_id: Query identifier (e.g., 'query_1')
            sql: Original SQL query
            n_workers: Number of parallel workers for Phase 3
            history: Previous attempts on this query [{status, transforms, speedup}]

        Returns:
            PipelineResult with full pipeline outcome
        """
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

        # Find examples: gold by annotation first, then FAISS for remaining slots
        examples = self._find_examples(sql, annotation, engine=engine, k=3)
        logger.info(
            f"[{query_id}] FAISS examples: "
            f"{[e.get('id', '?') for e in examples]}"
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
    ) -> List[PipelineResult]:
        """Run all queries for a state.

        State 0: n_workers = config.workers_state_0 (discovery)
        State 1+: n_workers = config.workers_state_n (refinement)

        Args:
            state_num: State number (0 = discovery, 1+ = refinement)
            n_workers: Override worker count
            query_ids: Optional subset of queries to run

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
        """Create state_{N+1} from state_N.

        Winners (speedup >= promote_threshold):
            Optimized SQL becomes next state's baseline query
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
        for subdir in ["prompts", "responses", "validation"]:
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

        # Promote winners: copy optimized SQL as new baseline
        promoted = 0
        for query_id, entry in leaderboard.items():
            speedup = entry.get("speedup", 0)
            baseline_path = queries_dir / f"{query_id}.sql"

            if speedup >= self.config.promote_threshold:
                # Get winning optimized SQL from validation
                val_path = state_dir / "validation" / f"{query_id}.json"
                if val_path.exists():
                    val_data = json.loads(val_path.read_text())
                    opt_sql = val_data.get("optimized_sql", "")
                    if opt_sql:
                        # Save as next state's query
                        (next_state_dir / f"{query_id}_promoted.sql").write_text(opt_sql)
                        promoted += 1
                        logger.info(
                            f"  Promoted {query_id}: {speedup:.2f}x → "
                            f"optimized SQL becomes state_{state_num + 1} baseline"
                        )
                        continue

            # Non-winner: carry original forward (no file needed, queries/ is shared)
            logger.debug(f"  {query_id}: {speedup:.2f}x → original carries forward")

        logger.info(
            f"Promoted {promoted}/{len(leaderboard)} queries from "
            f"state_{state_num} to state_{state_num + 1}"
        )

        return str(next_state_dir)

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
    ) -> Dict[str, Dict[str, List]]:
        """Load cumulative history from all previous states."""
        history: Dict[str, Dict[str, List]] = {}

        for s in range(state_num):
            state_dir = self.benchmark_dir / f"state_{s}"
            validation_dir = state_dir / "validation"
            if not validation_dir.exists():
                continue

            for path in validation_dir.glob("*.json"):
                try:
                    data = json.loads(path.read_text())
                    qid = data.get("query_id", path.stem)
                    if qid not in history:
                        history[qid] = {}

                    # Add per-node results to history
                    for node_id in data.get("nodes_rewritten", []):
                        if node_id not in history[qid]:
                            history[qid][node_id] = []
                        history[qid][node_id].append({
                            "state": s,
                            "status": data.get("status", "unknown"),
                            "speedup": data.get("speedup", 0),
                            "transforms": data.get("transforms_applied", []),
                        })
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
        annotation: AnnotationResult,
        engine: str = "duckdb",
        k: int = 3,
    ) -> List[Dict[str, Any]]:
        """Find examples: gold first (by annotation pattern), then FAISS for the rest.

        Priority:
        1. Gold examples matching annotated patterns (verified speedups)
        2. FAISS similarity on fingerprinted SQL for remaining slots

        Returns up to k examples.
        """
        examples = []
        seen_ids = set()

        # 1. Gold examples matching annotation patterns (sorted by speedup)
        gold_candidates = []
        for ann in annotation.rewrites:
            ex = self.prompter.load_example_for_pattern(
                ann.pattern, engine=engine, seed_dirs=self._seed_dirs,
            )
            if ex:
                ex_id = ex.get("id", ann.pattern)
                if ex_id not in seen_ids:
                    speedup = self._parse_speedup(ex.get("verified_speedup", 0))
                    gold_candidates.append((speedup, ex_id, ex))
                    seen_ids.add(ex_id)

        # Sort gold by speedup descending, take best ones first
        gold_candidates.sort(key=lambda x: -x[0])
        for _, ex_id, ex in gold_candidates:
            if len(examples) >= k:
                break
            examples.append(ex)

        # 2. Fill remaining slots with FAISS similarity matches (same engine + seed only)
        if len(examples) < k:
            from .knowledge import ADOFAISSRecommender

            engine_dir = "postgres" if engine in ("postgresql", "postgres") else engine

            recommender = ADOFAISSRecommender()
            if recommender._initialized:
                matches = recommender.find_similar_examples(sql, k=k * 5)
                for ex_id, score, meta in matches:
                    if len(examples) >= k:
                        break
                    if score <= 0.0 or ex_id in seen_ids:
                        continue

                    # Only allow same engine or seed (generic) examples
                    ex_engine = meta.get("engine", "unknown")
                    if ex_engine not in (engine_dir, "seed", "unknown"):
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

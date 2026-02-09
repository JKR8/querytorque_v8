"""Probe session — orchestrate frontier probes across a benchmark corpus.

Two-phase execution:
  Phase A: LLM calls (parallel via ThreadPoolExecutor, max_workers=20)
  Phase B: Validation (sequential — DB needs exclusive access for clean timing)
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional

from .schemas import AttackResult, DiscoverySummary, ProbeResult
from .probe_prompt import build_probe_prompt, _load_explain_plan
from .probe_parsers import parse_probe_response

logger = logging.getLogger(__name__)


def _fmt_elapsed(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    return f"{m}m{s:02d}s"


def _classify_speedup(speedup: float) -> str:
    """Classify speedup into status categories."""
    if speedup >= 1.50:
        return "WIN"
    elif speedup >= 1.10:
        return "IMPROVED"
    elif speedup >= 0.95:
        return "NEUTRAL"
    else:
        return "REGRESSION"


class ProbeSession:
    """Orchestrate frontier probes across a benchmark corpus."""

    def __init__(
        self,
        benchmark_dir: Path,
        exploit_profile_text: Optional[str] = None,
        round_num: int = 0,
        output_dir: Optional[Path] = None,
        provider: Optional[str] = None,
        model: Optional[str] = None,
    ):
        """Initialize probe session.

        Args:
            benchmark_dir: Benchmark directory with config.json + queries/.
            exploit_profile_text: Current exploit algorithm YAML text (None for round 0).
            round_num: Current probe round number.
            output_dir: Override output directory (default: benchmark_dir/probe/round_N/).
            provider: LLM provider (from .env if None).
            model: LLM model (from .env if None).
        """
        self.benchmark_dir = Path(benchmark_dir)
        self.exploit_profile_text = exploit_profile_text
        self.round_num = round_num
        self.provider = provider
        self.model = model

        # Load config
        from ..schemas import BenchmarkConfig

        self.config = BenchmarkConfig.from_file(self.benchmark_dir / "config.json")
        self.dialect = self.config.engine
        if self.dialect in ("postgres", "pg"):
            self.dialect = "postgresql"

        # Output directory
        if output_dir:
            self.output_dir = Path(output_dir)
        else:
            self.output_dir = self.benchmark_dir / "probe" / f"round_{round_num}"

        self.results: List[ProbeResult] = []

    def _load_queries(
        self, query_ids: Optional[List[str]] = None,
    ) -> Dict[str, str]:
        """Load SQL queries from benchmark_dir/queries/*.sql.

        Args:
            query_ids: Subset of queries to load (None = all).

        Returns:
            Dict of query_id -> SQL text.
        """
        queries_dir = self.benchmark_dir / "queries"
        if not queries_dir.exists():
            raise FileNotFoundError(f"Queries directory not found: {queries_dir}")

        queries = {}
        for sql_path in sorted(queries_dir.glob("*.sql")):
            qid = sql_path.stem
            if query_ids and qid not in query_ids:
                continue
            queries[qid] = sql_path.read_text().strip()

        if query_ids:
            missing = set(query_ids) - set(queries.keys())
            if missing:
                logger.warning(f"Missing queries: {missing}")

        return queries

    def _get_generator(self):
        """Create a CandidateGenerator for LLM calls."""
        from ..generate import CandidateGenerator

        return CandidateGenerator(
            provider=self.provider,
            model=self.model,
        )

    def _get_validator(self):
        """Create a Validator for benchmarking."""
        from ..validate import Validator

        return Validator(sample_db=self.config.db_path_or_dsn)

    def probe_corpus(
        self,
        query_ids: Optional[List[str]] = None,
        max_workers: int = 20,
    ) -> List[ProbeResult]:
        """Probe all benchmark queries: parallel LLM calls, sequential validation.

        Args:
            query_ids: Subset of queries to probe (None = all).
            max_workers: Max concurrent LLM calls.

        Returns:
            List of ProbeResults with validated attacks.
        """
        queries = self._load_queries(query_ids)
        if not queries:
            logger.warning("No queries to probe")
            return []

        print(
            f"\n{'='*60}\n"
            f"  FRONTIER PROBE: Round {self.round_num}\n"
            f"  Queries: {len(queries)} | Workers: {max_workers}\n"
            f"  Exploit profile: {'yes' if self.exploit_profile_text else 'no (round 0)'}\n"
            f"{'='*60}",
            flush=True,
        )

        # Phase A: Parallel LLM calls
        t_start = time.time()
        print(f"\n--- Phase A: LLM probes ({len(queries)} queries) ---", flush=True)
        partial_results = self._probe_llm_batch(queries, max_workers)
        print(
            f"  Phase A complete: {len(partial_results)} probes "
            f"({_fmt_elapsed(time.time() - t_start)})",
            flush=True,
        )

        # Phase B: Sequential validation
        print(f"\n--- Phase B: Validation ---", flush=True)
        t_val = time.time()
        self.results = self._validate_attacks_batch(partial_results)
        print(
            f"  Phase B complete ({_fmt_elapsed(time.time() - t_val)})",
            flush=True,
        )

        # Summary
        total_attacks = sum(len(r.attacks) for r in self.results)
        total_wins = sum(r.n_wins for r in self.results)
        total_improved = sum(r.n_improved for r in self.results)
        print(
            f"\n  Summary: {total_attacks} attacks, "
            f"{total_wins} WIN, {total_improved} IMPROVED "
            f"({_fmt_elapsed(time.time() - t_start)} total)\n",
            flush=True,
        )

        return self.results

    def _probe_llm_batch(
        self,
        queries: Dict[str, str],
        max_workers: int = 20,
    ) -> List[ProbeResult]:
        """Fire all LLM calls in parallel. Returns ProbeResults with parsed attacks (unvalidated)."""
        generator = self._get_generator()
        results: List[ProbeResult] = []

        def probe_one(query_id: str, sql: str) -> ProbeResult:
            """Probe a single query: build prompt -> LLM -> parse."""
            # Load EXPLAIN plan
            explain_text = _load_explain_plan(
                self.benchmark_dir, query_id, self.dialect
            )

            # Build probe prompt
            prompt = build_probe_prompt(
                sql=sql,
                explain_plan_text=explain_text,
                exploit_profile_text=self.exploit_profile_text,
                dialect=self.dialect,
            )

            # Call LLM
            response = generator._analyze_with_max_tokens(prompt, max_tokens=8192)

            # Parse response
            attacks, discovery = parse_probe_response(response)

            # Syntax check each attack
            valid_attacks = []
            for attack in attacks:
                try:
                    import sqlglot

                    sqlglot.parse_one(attack.optimized_sql, dialect=self.dialect)
                    valid_attacks.append(attack)
                except Exception as e:
                    attack.status = "ERROR"
                    attack.speedup = 0.0
                    attack.error_messages = [f"Syntax error: {e}"]
                    valid_attacks.append(attack)

            return ProbeResult(
                query_id=query_id,
                engine=self.dialect,
                original_sql=sql,
                attacks=valid_attacks,
                discovery_summary=discovery,
                probe_response=response,
                round_num=self.round_num,
            )

        # Parallel execution
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(probe_one, qid, sql): qid
                for qid, sql in queries.items()
            }
            for future in as_completed(futures):
                qid = futures[future]
                try:
                    result = future.result()
                    n_attacks = len(result.attacks)
                    n_syntax_ok = sum(
                        1 for a in result.attacks if a.status != "ERROR"
                    )
                    print(
                        f"  [{qid}] {n_attacks} attacks parsed ({n_syntax_ok} valid syntax)",
                        flush=True,
                    )
                    results.append(result)
                except Exception as e:
                    logger.error(f"[{qid}] Probe failed: {e}")
                    # Create empty result for failed probes
                    results.append(
                        ProbeResult(
                            query_id=qid,
                            engine=self.dialect,
                            original_sql=queries[qid],
                            round_num=self.round_num,
                        )
                    )

        return sorted(results, key=lambda r: r.query_id)

    def _validate_attacks_batch(
        self,
        probe_results: List[ProbeResult],
    ) -> List[ProbeResult]:
        """Validate all attacks sequentially (DB needs exclusive access).

        For each query:
        1. Benchmark baseline once
        2. Validate each attack against the baseline
        3. Save ProbeResult JSON immediately
        """
        validator = self._get_validator()

        try:
            for result in probe_results:
                # Skip queries with no valid attacks
                valid_attacks = [
                    a for a in result.attacks if a.status != "ERROR"
                ]
                if not valid_attacks:
                    self._save_single_result(result)
                    continue

                # Benchmark baseline
                try:
                    baseline = validator.benchmark_baseline(result.original_sql)
                    print(
                        f"  [{result.query_id}] Baseline: "
                        f"{baseline.measured_time_ms:.1f}ms ({baseline.row_count} rows)",
                        flush=True,
                    )
                except Exception as e:
                    logger.error(
                        f"[{result.query_id}] Baseline failed: {e}"
                    )
                    for attack in result.attacks:
                        if attack.status != "ERROR":
                            attack.status = "ERROR"
                            attack.error_messages = [f"Baseline failed: {e}"]
                    self._save_single_result(result)
                    continue

                # Validate each attack
                for attack in result.attacks:
                    if attack.status == "ERROR":
                        continue  # Already failed syntax check

                    try:
                        val_result = validator.validate_against_baseline(
                            baseline,
                            attack.optimized_sql,
                            worker_id=attack.attack_id,
                        )

                        if val_result.status.value == "pass":
                            attack.speedup = val_result.speedup
                            attack.status = _classify_speedup(val_result.speedup)
                        elif val_result.status.value == "fail":
                            attack.status = "FAIL"
                            attack.speedup = val_result.speedup
                            attack.error_messages = val_result.errors or []
                        else:
                            attack.status = "ERROR"
                            attack.speedup = 0.0
                            attack.error_messages = val_result.errors or []

                    except Exception as e:
                        attack.status = "ERROR"
                        attack.speedup = 0.0
                        attack.error_messages = [str(e)]

                # Print attack results
                for attack in result.attacks:
                    marker = "*" if attack.status == "WIN" else " "
                    err = ""
                    if attack.error_messages:
                        err = f" — {attack.error_messages[0][:60]}"
                    print(
                        f"  [{result.query_id}] {marker} A{attack.attack_id}: "
                        f"{attack.status} {attack.speedup:.2f}x "
                        f"({attack.gap_hypothesis[:50]}){err}",
                        flush=True,
                    )

                # Save immediately after validating each query
                self._save_single_result(result)
        finally:
            validator.close()

        return probe_results

    def _save_single_result(self, result: ProbeResult) -> None:
        """Save a single ProbeResult as JSON."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        out_path = self.output_dir / f"{result.query_id}.json"
        out_path.write_text(json.dumps(result.to_dict(), indent=2))

    def save_results(self) -> Path:
        """Save all results + summary to output_dir."""
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Save individual results
        for result in self.results:
            self._save_single_result(result)

        # Build and save summary
        total_attacks = sum(len(r.attacks) for r in self.results)
        total_wins = sum(r.n_wins for r in self.results)
        total_improved = sum(r.n_improved for r in self.results)

        # Collect all new gaps
        all_new_gaps: List[str] = []
        all_extended_gaps: List[str] = []
        all_negative_results: List[str] = []
        for r in self.results:
            if r.discovery_summary:
                all_new_gaps.extend(r.discovery_summary.new_gaps)
                all_extended_gaps.extend(r.discovery_summary.extended_gaps)
                all_negative_results.extend(r.discovery_summary.negative_results)

        # Top attacks by speedup
        all_attacks = []
        for r in self.results:
            for a in r.attacks:
                all_attacks.append(
                    {
                        "query_id": r.query_id,
                        "attack_id": a.attack_id,
                        "gap_hypothesis": a.gap_hypothesis,
                        "status": a.status,
                        "speedup": a.speedup,
                    }
                )
        top_attacks = sorted(all_attacks, key=lambda x: -x["speedup"])[:20]

        summary = {
            "round_num": self.round_num,
            "engine": self.dialect,
            "n_queries": len(self.results),
            "n_attacks": total_attacks,
            "n_wins": total_wins,
            "n_improved": total_improved,
            "new_gaps_discovered": list(set(all_new_gaps)),
            "extended_gaps": list(set(all_extended_gaps)),
            "negative_results": list(set(all_negative_results)),
            "top_attacks": top_attacks,
        }

        summary_path = self.output_dir / "summary.json"
        summary_path.write_text(json.dumps(summary, indent=2))

        print(f"\n  Results saved to {self.output_dir}", flush=True)
        return self.output_dir

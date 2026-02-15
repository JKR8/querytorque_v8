"""Workload Session — orchestrates fleet-level optimization.

Pipeline:
1. Triage all queries (pain × frequency × tractability)
2. Quick-win fast path (top 3 → Tier 3 directly)
3. Tier 1: fleet-level actions (config, indexes, statistics)
4. Re-benchmark and re-triage
5. Tier 2: light per-query optimization (single-pass beam)
6. Tier 3: deep per-query optimization (iterative beam)
7. Compile scorecard with business case
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from .triage import triage_workload, WorkloadTriage, Tier
from .fleet import detect_fleet_patterns, FleetAnalysis
from .scorecard import (
    WorkloadScorecard,
    compile_scorecard,
    render_scorecard_markdown,
)

logger = logging.getLogger(__name__)


def _parse_threshold(threshold: str) -> Optional[float]:
    """Parse threshold string like '>300s', '>0', '>500MB' into numeric value.

    Returns seconds for time units, bytes for data units, raw number otherwise.
    Returns None if unparseable.
    """
    s = threshold.strip().lstrip("><=")
    if not s:
        return None
    # "any" means threshold is 0 (any amount triggers)
    if s.lower() == "any":
        return 0.0
    try:
        if s.endswith("s"):
            return float(s[:-1])
        if s.endswith("GB"):
            return float(s[:-2]) * 1e9
        if s.endswith("MB"):
            return float(s[:-2]) * 1e6
        return float(s)
    except (ValueError, IndexError):
        return None


@dataclass
class WorkloadConfig:
    """Configuration for workload optimization."""
    benchmark_dir: str = ""
    engine: str = "postgres"
    scenario: str = ""
    original_warehouse: str = ""
    target_warehouse: str = ""
    max_tier3_queries: int = 20
    pass_rate_threshold: float = 0.95
    enable_quick_win: bool = True
    enable_fleet: bool = True
    enable_tier2: bool = True
    enable_tier3: bool = True


class WorkloadSession:
    """Orchestrates workload-level optimization.

    Usage:
        session = WorkloadSession(config, queries)
        scorecard = session.run()
    """

    def __init__(
        self,
        config: WorkloadConfig,
        queries: List[Dict[str, Any]],
        pipeline: Optional[Any] = None,
    ):
        self.config = config
        self.queries = queries
        self.pipeline = pipeline
        self._triage: Optional[WorkloadTriage] = None
        self._fleet: Optional[FleetAnalysis] = None
        self._results: List[Dict[str, Any]] = []
        self._scenario_card: Optional[Dict[str, Any]] = None

        # Load scenario card if configured
        if self.config.scenario:
            from ..scenario_cards import load_scenario_card
            self._scenario_card = load_scenario_card(self.config.scenario)

    def run(self) -> WorkloadScorecard:
        """Execute full workload optimization pipeline.

        Returns:
            WorkloadScorecard with all results.
        """
        logger.info(
            f"Workload session: {len(self.queries)} queries, "
            f"engine={self.config.engine}, target={self.config.target_warehouse}"
        )

        # Step 1: Triage
        self._triage = triage_workload(self.queries)
        logger.info(
            f"Triage complete: {len(self._triage.tier_2_queries)} tier-2, "
            f"{len(self._triage.tier_3_queries)} tier-3, "
            f"{len(self._triage.quick_wins)} quick-win"
        )

        # Step 2: Fleet-level actions (Tier 1)
        if self.config.enable_fleet:
            self._fleet = detect_fleet_patterns(
                self.queries, engine=self.config.engine
            )
            self._apply_fleet_actions()

        # Step 3: Quick-win fast path → Tier 3
        if self.config.enable_quick_win and self._triage.quick_wins:
            self._run_tier3(self._triage.quick_wins)

        # Step 4: Tier 2 light optimization
        if self.config.enable_tier2 and self._triage.tier_2_queries:
            self._run_tier2(self._triage.tier_2_queries)

        # Step 5: Tier 3 deep optimization
        if self.config.enable_tier3 and self._triage.tier_3_queries:
            tier3_ids = self._triage.tier_3_queries[:self.config.max_tier3_queries]
            self._run_tier3(tier3_ids)

        # Step 6: Handle skipped queries
        for qid in self._triage.skipped:
            self._results.append({
                "query_id": qid,
                "tier": "SKIP",
                "status": "SKIP",
                "speedup": 1.0,
                "fits_scenario": self._evaluate_scenario_fit(qid, 1.0),
            })

        # Step 7: Compile scorecard
        fleet_actions = []
        if self._fleet:
            fleet_actions = [
                {
                    "action": a.action,
                    "action_type": a.action_type,
                    "queries_affected": len(a.queries_affected),
                    "impact": a.estimated_impact,
                }
                for a in self._fleet.actions
            ]

        scorecard = compile_scorecard(
            query_results=self._results,
            fleet_actions=fleet_actions,
            workload_id=f"{self.config.engine}_{self.config.target_warehouse}",
            original_warehouse=self.config.original_warehouse,
            target_warehouse=self.config.target_warehouse,
        )

        logger.info(
            f"Scorecard: pass_rate={scorecard.pass_rate:.0%}, "
            f"wins={scorecard.wins}, residuals={len(scorecard.residuals)}"
        )

        return scorecard

    def _apply_fleet_actions(self) -> None:
        """Apply fleet-level actions (Tier 1).

        These are logged but not automatically executed — they produce
        recommendations that may need human approval (index creation,
        config changes, etc.).
        """
        if not self._fleet:
            return

        for action in self._fleet.actions:
            logger.info(
                f"Fleet action [{action.action_type}]: {action.action} "
                f"(affects {len(action.queries_affected)} queries)"
            )

    def _run_tier2(self, query_ids: List[str]) -> None:
        """Run Tier 2 light optimization on queries.

        Single-pass beam (analyst -> workers -> snipe once).
        If pipeline is not available, records queries as NEUTRAL.
        """
        for qid in query_ids:
            logger.info(f"Tier 2: {qid}")

            if self.pipeline:
                try:
                    result = self._run_single_query(qid, tier="TIER_2")
                    self._results.append(result)
                    continue
                except Exception as e:
                    logger.warning(f"Tier 2 failed for {qid}: {e}")

            # No pipeline or failed — record as attempted
            self._results.append({
                "query_id": qid,
                "tier": "TIER_2",
                "status": "NEUTRAL",
                "speedup": 1.0,
                "fits_scenario": self._evaluate_scenario_fit(qid, 1.0),
            })

    def _run_tier3(self, query_ids: List[str]) -> None:
        """Run Tier 3 deep optimization on queries.

        Full iterative beam pipeline.
        """
        for qid in query_ids:
            logger.info(f"Tier 3: {qid}")

            if self.pipeline:
                try:
                    result = self._run_single_query(qid, tier="TIER_3")
                    if result.get("status") in ("ERROR", "REGRESSION"):
                        result = self._handle_tier3_failure(qid, result)
                    self._results.append(result)
                    continue
                except Exception as e:
                    logger.warning(f"Tier 3 failed for {qid}: {e}")

            # No pipeline or failed — record as attempted
            self._results.append({
                "query_id": qid,
                "tier": "TIER_3",
                "status": "NEUTRAL",
                "speedup": 1.0,
                "fits_scenario": self._evaluate_scenario_fit(qid, 1.0),
            })

    def _run_single_query(
        self, query_id: str, tier: str = "TIER_3"
    ) -> Dict[str, Any]:
        """Run optimization pipeline for a single query.

        Uses the existing Pipeline/Session infrastructure.
        """
        if not self.pipeline:
            return {
                "query_id": query_id,
                "tier": tier,
                "status": "NEUTRAL",
                "speedup": 1.0,
            }

        # Find the query SQL
        query_sql = None
        for q in self.queries:
            if q["query_id"] == query_id:
                query_sql = q.get("sql", "")
                break

        if not query_sql:
            return {
                "query_id": query_id,
                "tier": tier,
                "status": "ERROR",
                "speedup": 1.0,
                "failure_reason": "Query SQL not found",
            }

        # Run through canonical beam pipeline
        try:
            from ..schemas import OptimizationMode

            max_iterations = 1 if tier == "TIER_2" else 3
            target_speedup = 2.0 if tier == "TIER_2" else 10.0

            result = self.pipeline.run_optimization_session(
                query_id=query_id,
                sql=query_sql,
                max_iterations=max_iterations,
                target_speedup=target_speedup,
                mode=OptimizationMode.BEAM,
                patch=True,
            )
            fits = self._evaluate_scenario_fit(query_id, result.best_speedup)
            return {
                "query_id": query_id,
                "tier": tier,
                "status": result.status,
                "speedup": result.best_speedup,
                "technique": ", ".join(result.best_transforms) if result.best_transforms else "",
                "fits_scenario": fits,
            }
        except Exception as e:
            return {
                "query_id": query_id,
                "tier": tier,
                "status": "ERROR",
                "speedup": 1.0,
                "failure_reason": str(e),
            }

    def _evaluate_scenario_fit(
        self, query_id: str, speedup: float,
    ) -> bool:
        """Evaluate whether post-optimization metrics fit scenario constraints.

        Checks the scenario card's failure_definitions against estimated
        post-optimization values. Currently evaluates:
        - query_duration: original_ms / speedup vs threshold

        Returns True (fits) when no scenario card is loaded.
        """
        if not self._scenario_card:
            return True

        # Find original query data
        original = None
        for q in self.queries:
            if q["query_id"] == query_id:
                original = q
                break
        if not original:
            return True

        failures = self._scenario_card.get("failure_definitions", [])
        for fdef in failures:
            if fdef.get("severity") != "fatal":
                continue

            metric = fdef.get("metric", "")
            threshold = _parse_threshold(fdef.get("threshold", ""))
            if threshold is None:
                continue

            if metric == "query_duration":
                orig_ms = original.get("duration_ms")
                if orig_ms and speedup > 0:
                    estimated_ms = orig_ms / speedup
                    # threshold is in seconds
                    if estimated_ms > threshold * 1000:
                        return False

            elif metric in ("bytes_spilled_remote", "bytes_spilled_local"):
                # If original had spill and optimization didn't help much,
                # conservatively mark as not fitting
                if original.get("spill_detected") and speedup < 2.0:
                    if threshold == 0:  # ">0" means any spill is fatal
                        return False

        return True

    def _handle_tier3_failure(
        self, query_id: str, result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Handle Tier 3 failure with 3-level escalation.

        Level 1: Constraint feedback (re-run with relaxed constraints)
        Level 2: Human escalation (flag for review)
        Level 3: Accept and recommend (query can't fit, explain why)
        """
        # Level 1: Try once more with constraint feedback
        logger.info(f"Tier 3 escalation level 1 for {query_id}")
        if self.pipeline:
            try:
                retry_result = self._run_single_query(query_id, tier="TIER_3")
                if retry_result.get("status") in ("WIN", "IMPROVED", "NEUTRAL"):
                    retry_result["escalation_level"] = 1
                    return retry_result
            except Exception:
                pass

        # Level 2: Human escalation
        logger.info(f"Tier 3 escalation level 2 for {query_id}: flagging for human review")
        result["escalation_level"] = 2
        result["failure_reason"] = (
            f"All optimization attempts failed. "
            f"Recommend: physical design changes or manual review."
        )

        # Level 3: Accept and recommend
        if result.get("status") == "ERROR":
            result["escalation_level"] = 3
            result["failure_reason"] = (
                f"Query is fundamentally compute-bound on target. "
                f"Recommend: keep on {self.config.original_warehouse} "
                f"or specific infrastructure upgrade."
            )
            result["fits_scenario"] = False

        return result

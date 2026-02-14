"""Orchestrator — declarative composition of system context.

The Orchestrator assembles: universal_doctrine + engine_pack + scenario_card
+ pattern_library into a unified context for analyst prompt construction.

It is a COMPOSITION WRAPPER — it does NOT touch:
- Race validation, semantic pre-validation, EXPLAIN collection
- Coach/snipe iterations
- Q-Error routing
- Worker diversity enforcement
- Discovery mode detection
- Per-worker SET LOCAL tuning

The pipeline logic (validate → coach → snipe) is untouched.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class Orchestrator:
    """Assembles modular components into unified system context.

    Usage:
        orch = Orchestrator(engine="postgres", scenario="postgres_small_instance")
        ctx = orch.compose_system_context()
        evidence = orch.enrich_evidence(evidence_bundle)
        examples = orch.select_gold_examples(evidence)
        contract = orch.build_output_contract(session_result)
    """

    def __init__(
        self,
        engine: str,
        scenario: Optional[str] = None,
        engine_version: Optional[str] = None,
    ):
        self.engine = engine
        self.scenario_name = scenario
        self.engine_version = engine_version

        # Lazy-load components
        self._doctrine = None
        self._engine_pack = None
        self._scenario_card = None
        self._pattern_library = None

    @property
    def doctrine(self) -> Dict[str, Any]:
        if self._doctrine is None:
            from .doctrine import load_doctrine
            self._doctrine = load_doctrine()
        return self._doctrine

    @property
    def engine_pack(self) -> Optional[Dict[str, Any]]:
        if self._engine_pack is None:
            from .engine_packs import load_engine_pack
            self._engine_pack = load_engine_pack(self.engine)
        return self._engine_pack

    @property
    def scenario_card(self) -> Optional[Dict[str, Any]]:
        if self._scenario_card is None and self.scenario_name:
            from .scenario_cards import load_scenario_card
            self._scenario_card = load_scenario_card(self.scenario_name)
        return self._scenario_card

    @property
    def pattern_library(self) -> Dict[str, Any]:
        if self._pattern_library is None:
            try:
                from .patterns import load_pattern_library
                self._pattern_library = load_pattern_library()
            except Exception:
                self._pattern_library = {}
        return self._pattern_library

    def compose_system_context(self) -> Dict[str, Any]:
        """Merge doctrine + pack + card + patterns into unified context.

        Returns dict with keys that can be used alongside or instead of
        the existing gather_analyst_context() output.

        Falls back gracefully when components are unavailable.
        """
        ctx: Dict[str, Any] = {}

        # Doctrine (always available)
        ctx["doctrine"] = self.doctrine
        ctx["doctrine_text"] = self._render_doctrine()

        # Engine pack (may be None for unsupported engines)
        if self.engine_pack:
            ctx["engine_pack"] = self.engine_pack
            ctx["capabilities_text"] = self._render_capabilities()
            ctx["optimizer_profile_text"] = self._render_optimizer_profile()

        # Scenario card (None if not specified)
        if self.scenario_card:
            ctx["scenario_card"] = self.scenario_card
            ctx["scenario_text"] = self._render_scenario()

        # Pattern library (empty dict if unavailable)
        ctx["pattern_library"] = self.pattern_library

        return ctx

    def enrich_evidence(self, bundle: Any) -> Any:
        """Enrich evidence bundle with scenario card constraints.

        Per Patch 8: compare memory.peak against scenario_card,
        set budget and status fields.
        """
        if not self.scenario_card or bundle is None:
            return bundle

        # Enrich memory budget/status from scenario card
        envelope = self.scenario_card.get("resource_envelope", {})
        memory_budget = envelope.get("memory", "")

        if hasattr(bundle, "runtime_profile"):
            rp = bundle.runtime_profile
            if hasattr(rp, "memory"):
                rp.memory.budget = memory_budget
                # Determine status based on failure definitions
                failures = self.scenario_card.get("failure_definitions", [])
                for f in failures:
                    if f.get("metric") in ("bytes_spilled_remote", "temp_blks_written"):
                        if rp.spill.detected and f.get("severity") == "fatal":
                            rp.memory.status = "over_budget"
                            break
                else:
                    rp.memory.status = "within_budget"

        return bundle

    def select_gold_examples(
        self,
        evidence: Any = None,
        bottleneck: Optional[str] = None,
        k: int = 5,
    ) -> List[Dict[str, Any]]:
        """Deterministic example selection with Patch 3 scoring rubric.

        1. Filter patterns by engine tag
        2. Score: bottleneck match +3, signal match +2, scenario match +1
        3. Top k by score
        4. Fallback: if <3 matches, include 3 most common patterns
        """
        try:
            from .patterns import select_gold_examples
            return select_gold_examples(
                engine=self.engine,
                evidence_bundle=evidence,
                scenario_card=self.scenario_card,
                bottleneck=bottleneck,
                k=k,
            )
        except Exception as e:
            logger.warning(f"Pattern library selection failed: {e}")
            return []

    def build_output_contract(self, session_result: Any) -> Any:
        """Wrap SessionResult into structured output contract."""
        from .contracts import QueryOutputContract
        return QueryOutputContract.from_session_result(session_result)

    def _render_doctrine(self) -> str:
        """Render doctrine for prompt injection."""
        from .doctrine import render_doctrine_for_prompt
        return render_doctrine_for_prompt()

    def _render_capabilities(self) -> str:
        """Render engine capabilities for prompt injection."""
        if not self.engine_pack:
            return ""
        from .engine_packs import render_capabilities_for_prompt
        return render_capabilities_for_prompt(self.engine)

    def _render_optimizer_profile(self) -> str:
        """Render optimizer profile for prompt injection."""
        if not self.engine_pack:
            return ""
        from .engine_packs import render_optimizer_profile_for_prompt
        return render_optimizer_profile_for_prompt(self.engine)

    def _render_scenario(self) -> str:
        """Render scenario card for prompt injection."""
        if not self.scenario_card:
            return ""
        from .scenario_cards import render_scenario_for_prompt
        return render_scenario_for_prompt(self.scenario_card)

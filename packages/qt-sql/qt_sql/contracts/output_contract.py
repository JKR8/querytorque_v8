"""QueryOutputContract — structured per-query optimization output.

Wraps the existing SessionResult (which is preserved unchanged) with
additional fields for diagnosis, expected impact, and validation summary.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class Diagnosis:
    """What's wrong with the query and why."""
    bottleneck: str = ""         # Universal taxonomy label (spill, bad_pruning, etc.)
    evidence: str = ""           # Specific plan node / counter / cost that supports claim
    engine_feature: str = ""     # Engine-specific feature involved (optional)


@dataclass
class ExpectedImpact:
    """Expected improvement from the optimization."""
    metric: str = "latency"      # What we're improving (latency, memory, spill)
    before: str = ""             # Original value
    after: str = ""              # Expected optimized value
    confidence: str = "medium"   # high | medium | low


@dataclass
class ValidationSummary:
    """Summary of validation results."""
    equivalence: str = "needs_manual_check"  # verified | needs_manual_check
    regression_check: str = ""    # pass | fail | not_run
    benchmark_result: Optional[Dict[str, Any]] = None  # latency/spill before/after
    fits_scenario: Optional[bool] = None  # Does rewrite fit target scenario card?


@dataclass
class QueryOutputContract:
    """Full per-query output contract, wrapping SessionResult.

    This is the structured deliverable per query. It adds diagnosis,
    expected impact, and validation summary on top of the raw SessionResult.
    """
    query_id: str
    original_sql: str
    optimized_sql: str
    config_changes: List[str] = field(default_factory=list)
    diagnosis: Diagnosis = field(default_factory=Diagnosis)
    optimization_technique: str = ""
    optimization_description: str = ""
    expected_impact: ExpectedImpact = field(default_factory=ExpectedImpact)
    validation: ValidationSummary = field(default_factory=ValidationSummary)
    transforms_used: List[str] = field(default_factory=list)
    speedup: float = 1.0
    status: str = ""  # WIN | IMPROVED | NEUTRAL | REGRESSION | ERROR

    @classmethod
    def from_session_result(cls, result: Any) -> "QueryOutputContract":
        """Create from an existing SessionResult.

        Args:
            result: A SessionResult dataclass instance.

        Returns:
            QueryOutputContract with fields populated from SessionResult.
        """
        contract = cls(
            query_id=result.query_id,
            original_sql=result.original_sql,
            optimized_sql=result.best_sql,
            transforms_used=getattr(result, "best_transforms", []),
            speedup=result.best_speedup,
            status=result.status,
            config_changes=getattr(result, "config_changes", []),
        )

        # Populate expected impact from speedup
        if result.best_speedup > 1.0:
            contract.expected_impact = ExpectedImpact(
                metric="latency",
                before="baseline",
                after=f"{result.best_speedup:.2f}x faster",
                confidence="high" if result.best_speedup >= 1.2 else "medium",
            )

        # Populate validation from status
        if result.status in ("WIN", "IMPROVED"):
            contract.validation = ValidationSummary(
                equivalence="verified",
                regression_check="pass",
            )
        elif result.status == "REGRESSION":
            contract.validation = ValidationSummary(
                equivalence="verified",
                regression_check="fail",
            )

        return contract

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_markdown(self) -> str:
        """Render as human-readable markdown."""
        lines = [
            f"## Query: {self.query_id}",
            "",
            f"**Status:** {self.status}",
            f"**Speedup:** {self.speedup:.2f}x",
            "",
        ]

        if self.diagnosis.bottleneck:
            lines.extend([
                "### Diagnosis",
                f"- **Bottleneck:** {self.diagnosis.bottleneck}",
                f"- **Evidence:** {self.diagnosis.evidence}",
            ])
            if self.diagnosis.engine_feature:
                lines.append(f"- **Engine Feature:** {self.diagnosis.engine_feature}")
            lines.append("")

        if self.optimization_technique:
            lines.extend([
                "### Optimization",
                f"- **Technique:** {self.optimization_technique}",
                f"- **Description:** {self.optimization_description}",
                "",
            ])

        if self.transforms_used:
            lines.append(f"**Transforms:** {', '.join(self.transforms_used)}")
            lines.append("")

        if self.config_changes:
            lines.append("### Config Changes")
            for c in self.config_changes:
                lines.append(f"- `{c}`")
            lines.append("")

        lines.extend([
            "### Validation",
            f"- **Equivalence:** {self.validation.equivalence}",
            f"- **Regression Check:** {self.validation.regression_check}",
        ])
        if self.validation.fits_scenario is not None:
            lines.append(f"- **Fits Scenario:** {'Yes' if self.validation.fits_scenario else 'No'}")

        return "\n".join(lines)

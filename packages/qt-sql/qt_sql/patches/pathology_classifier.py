"""Pathology classifier: AST detection + LLM classification → intelligence brief.

Provides:
- PathologyMatch / ClassificationResult dataclasses
- PathologyClassifier: LLM-based pathology classification for a single query
- build_intelligence_brief(): merge AST detection + LLM classification into
  analyst-readable text for prompt injection
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


# ── Data Classes ──────────────────────────────────────────────────────────

@dataclass
class PathologyMatch:
    """A pathology match from LLM classification."""

    pathology_id: str       # "P3", "P4"
    name: str               # Human-readable name
    confidence: float       # 0.0-1.0
    evidence: str           # Why this pathology matches
    recommended_transform: str  # e.g. "sf_inline_decorrelate"


@dataclass
class ClassificationResult:
    """Result of classifying a single query against known pathologies."""

    query_id: str
    matches: List[PathologyMatch] = field(default_factory=list)
    reasoning: str = ""


# ── PathologyClassifier (LLM-based) ──────────────────────────────────────

class PathologyClassifier:
    """Classify SQL queries against known pathologies using an LLM.

    Args:
        classify_fn: Callable that takes a prompt string, returns LLM response.
        dialect: SQL dialect (snowflake, postgres, duckdb).
    """

    def __init__(self, classify_fn: Callable[[str], str], dialect: str):
        self.classify_fn = classify_fn
        self.dialect = dialect
        self._pathology_text = self._load_pathology_defs()

    def classify(
        self,
        query_id: str,
        sql: str,
        explain_text: str = "",
    ) -> ClassificationResult:
        """Classify a query against known pathologies.

        Returns ClassificationResult with matches sorted by confidence.
        """
        prompt = self._build_prompt(sql, explain_text)

        try:
            response = self.classify_fn(prompt)
            return self._parse_response(query_id, response)
        except Exception as e:
            logger.warning(f"[{query_id}] Classification failed: {e}")
            return ClassificationResult(query_id=query_id, reasoning=str(e))

    def _load_pathology_defs(self) -> str:
        """Load DOCUMENTED CASES section from knowledge/{dialect}.md."""
        knowledge_dir = Path(__file__).resolve().parent.parent / "knowledge"

        # Normalize dialect name
        dialect_map = {"postgres": "postgresql", "postgresql": "postgresql"}
        normalized = dialect_map.get(self.dialect, self.dialect)
        knowledge_path = knowledge_dir / f"{normalized}.md"

        if not knowledge_path.exists():
            logger.warning(f"No knowledge file for dialect {self.dialect}")
            return ""

        text = knowledge_path.read_text()

        # Extract DOCUMENTED CASES section
        marker = "## DOCUMENTED CASES"
        idx = text.find(marker)
        if idx < 0:
            return text

        return text[idx:]

    def _build_prompt(self, sql: str, explain_text: str) -> str:
        """Build classification prompt (~2.5K tokens)."""
        explain_section = ""
        if explain_text:
            explain_section = f"""
## Execution Plan
```
{explain_text.strip()}
```
"""

        return f"""You are a SQL optimization classifier. Given a query and known pathology patterns,
identify which pathologies apply to this query.

## Query
```sql
{sql}
```
{explain_section}
## Known Pathologies ({self.dialect.upper()})

{self._pathology_text}

## Task

For each pathology that matches this query, output a JSON object with:
- pathology_id: The pathology ID (e.g. "P3")
- name: Short name
- confidence: 0.0-1.0 (how confident the match is)
- evidence: 1-2 sentences explaining why this pathology matches
- transform: Recommended transform ID from the catalog

Output a JSON array. If no pathologies match, output an empty array [].

```json
[
  {{"pathology_id": "P3", "name": "Correlated Scalar Subquery", "confidence": 0.95, "evidence": "WHERE clause contains correlated scalar subquery with AVG aggregation", "transform": "sf_inline_decorrelate"}}
]
```
"""

    def _parse_response(
        self, query_id: str, response: str
    ) -> ClassificationResult:
        """Parse LLM response into ClassificationResult."""
        # Extract JSON array from response
        import re

        matches = []
        reasoning = ""

        # Try to find JSON array
        json_match = re.search(r"\[.*\]", response, re.DOTALL)
        if json_match:
            try:
                data = json.loads(json_match.group())
                for item in data:
                    if isinstance(item, dict):
                        matches.append(PathologyMatch(
                            pathology_id=item.get("pathology_id", "?"),
                            name=item.get("name", ""),
                            confidence=float(item.get("confidence", 0.0)),
                            evidence=item.get("evidence", ""),
                            recommended_transform=item.get("transform", ""),
                        ))
            except (json.JSONDecodeError, ValueError) as e:
                reasoning = f"JSON parse error: {e}"

        # Sort by confidence descending
        matches.sort(key=lambda m: -m.confidence)

        # Extract any reasoning text outside JSON
        non_json = response.replace(json_match.group(), "").strip() if json_match else response
        if non_json and len(non_json) > 10:
            reasoning = non_json

        return ClassificationResult(
            query_id=query_id,
            matches=matches,
            reasoning=reasoning,
        )


# ── Intelligence Brief Builder ───────────────────────────────────────────

def build_intelligence_brief(
    detected_transforms: list,
    classification: Optional[ClassificationResult] = None,
) -> str:
    """Merge AST detection + LLM classification into analyst-readable brief.

    Args:
        detected_transforms: List of TransformMatch from detect_transforms().
        classification: Optional ClassificationResult from PathologyClassifier.

    Returns:
        Formatted string for injection into analyst prompt.
        Empty string if no useful signals.
    """
    lines: List[str] = []

    # AST detection results (top 5 with >30% overlap)
    top_ast = [m for m in detected_transforms if m.overlap_ratio >= 0.30][:5]
    if top_ast:
        lines.append("### AST Feature Detection")
        lines.append("")
        for m in top_ast:
            gap_str = f" (gap: {m.gap})" if m.gap else ""
            contra_str = ""
            if m.contraindications:
                contra_names = [c.get("id", "?") for c in m.contraindications[:2]]
                contra_str = f" [CAUTION: {', '.join(contra_names)}]"
            lines.append(
                f"- **{m.id}**: {m.overlap_ratio:.0%} match "
                f"({', '.join(m.matched_features[:4])}){gap_str}{contra_str}"
            )
            if m.missing_features:
                lines.append(
                    f"  Missing: {', '.join(m.missing_features[:3])}"
                )
        lines.append("")

    # LLM classification results
    if classification and classification.matches:
        lines.append("### Pathology Classification (pre-computed)")
        lines.append("")
        for m in classification.matches:
            lines.append(
                f"- **{m.pathology_id} {m.name}**: "
                f"{m.confidence:.0%} confidence"
            )
            if m.evidence:
                lines.append(f"  Evidence: {m.evidence}")
            if m.recommended_transform:
                lines.append(
                    f"  Recommended transform: `{m.recommended_transform}`"
                )
        lines.append("")

    return "\n".join(lines)

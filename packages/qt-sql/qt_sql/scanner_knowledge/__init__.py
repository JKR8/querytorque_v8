"""Scanner knowledge pipeline: Blackboard → Findings.

Two-layer knowledge extraction from plan-space scanner data:

  Layer 1 (Blackboard): Raw JSONL observations — machine-generated,
      one record per (query, flags) combo. No interpretation.

  Layer 2 (Findings): LLM-extracted claims about engine behavior with
      evidence, boundaries, confidence, mechanism. Human-reviewed.
"""

from .blackboard import populate_blackboard
from .findings import extract_findings, load_findings, build_findings_prompt

__all__ = [
    "populate_blackboard",
    "extract_findings",
    "load_findings",
    "build_findings_prompt",
]

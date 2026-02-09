"""Layer 2: Extract scanner findings from blackboard via two-pass LLM.

Pass 1 (reasoner): Free-form analysis of blackboard observations.
Pass 2 (chat model): Structure the analysis into ScannerFinding JSON.

The two-pass approach works around deepseek-reasoner's poor structured
output reliability — the reasoner thinks freely, then a chat model
formats the output.

Usage:
  python -m qt_sql.scanner_knowledge.findings benchmarks/postgres_dsb_76/
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from .schemas import ScannerFinding, ScannerObservation

logger = logging.getLogger(__name__)


# ── Prompt builders ─────────────────────────────────────────────────────

def _fmt_combo_name(flags: Dict[str, str]) -> str:
    """Short human name for a flag combo: enable_nestloop=off → 'no_nestloop'."""
    _SHORT = {
        ("enable_nestloop", "off"): "no_nestloop",
        ("enable_hashjoin", "off"): "no_hashjoin",
        ("enable_mergejoin", "off"): "no_mergejoin",
        ("enable_seqscan", "off"): "no_seqscan",
        ("enable_indexscan", "off"): "no_indexscan",
        ("enable_bitmapscan", "off"): "no_bitmapscan",
        ("jit", "off"): "jit_off",
        ("max_parallel_workers_per_gather", "0"): "no_parallel",
        ("max_parallel_workers_per_gather", "8"): "max_parallel",
        ("join_collapse_limit", "1"): "no_reorder",
    }
    parts = []
    for k, v in sorted(flags.items()):
        short = _SHORT.get((k, v))
        if short:
            parts.append(short)
        else:
            parts.append(f"{k}={v}")
    return "+".join(parts)


def build_findings_prompt(blackboard_path: Path) -> str:
    """Build the Pass 1 analysis prompt from blackboard JSONL.

    Returns the full prompt text for the reasoner model.
    Compact TSV format — no redundant summary text.
    """
    observations = _load_blackboard(blackboard_path)

    # Group by category for structured analysis
    by_category: Dict[str, List[ScannerObservation]] = {}
    for obs in observations:
        by_category.setdefault(obs.category, []).append(obs)

    prompt_parts = [
        "# PostgreSQL Plan-Space Scanner Analysis",
        "",
        "Below are observations from toggling planner flags (SET LOCAL) across 76",
        "DSB benchmark queries on PostgreSQL 14.3 (SF10). Each row is one (query, flag-combo)",
        "that produced a DIFFERENT plan than baseline — neutral combos are excluded.",
        "",
        "Column key:",
        "- **query**: query ID",
        "- **combo**: flag(s) toggled",
        "- **cost_ratio**: baseline_cost / combo_cost (>1 = combo cheaper, <1 = combo worse)",
        "- **wall_speedup**: baseline_ms / combo_ms (>1 = faster, <1 = regression). Only for 10 queries with wall-clock data.",
        "- **vulns**: vulnerability types detected (JOIN_TYPE_TRAP, JOIN_ORDER_TRAP, SCAN_TYPE_TRAP, MEMORY_SENSITIVITY, PARALLELISM_GAP)",
        "- **plans**: number of distinct plans discovered for this query",
        "",
        "## Your Task",
        "",
        "Extract 10-30 **findings** — generalizable claims about how this engine",
        "behaves on star-schema analytics. Focus on join sensitivity, memory/spill,",
        "JIT, parallelism, cost model accuracy, join reorder, and flag interactions.",
        "",
        "## Output Format",
        "",
        "Return ONLY a JSON array. No markdown fences, no explanation. Example:",
        "",
        "```json",
        "[",
        "  {",
        '    "id": "SF-001",                          // sequential SF-001, SF-002, ...',
        '    "claim": "Disabling nested loops causes >4x regression on dim-heavy star queries",',
        '    "category": "join_sensitivity",           // join_sensitivity|memory|parallelism|jit|cost_model|join_order|interaction|scan_method',
        '    "supporting_queries": ["query001_multi_i1", "query065_multi_i1", "query080_multi_i1"],',
        '    "evidence_summary": "8/10 queries with nested loop baseline regress >4x",',
        '    "evidence_count": 8,',
        '    "contradicting_count": 2,',
        '    "boundaries": ["Applies when baseline uses nested loops for dimension PK lookups"],',
        '    "mechanism": "Nested loops exploit dim PK indexes; hash join must full-scan dimension tables",',
        '    "confidence": "high",                     // high (>5 queries, consistent) | medium (3-5 or contradictions) | low (<3)',
        '    "confidence_rationale": "Consistent across 8 queries with cost + wall-clock evidence",',
        '    "implication": "Do NOT restructure joins that eliminate nested loop index lookups on dimension tables"',
        "  }",
        "]",
        "```",
        "",
        "---",
        "",
    ]

    # Add observations as compact tables per category
    for category in sorted(by_category.keys()):
        obs_list = by_category[category]
        prompt_parts.append(f"## {category} ({len(obs_list)} observations)")
        prompt_parts.append("")

        for obs in obs_list:
            combo = _fmt_combo_name(obs.flags)
            parts = [obs.query_id, combo]

            if obs.cost_ratio is not None:
                parts.append(f"cost={obs.cost_ratio:.2f}")
            if obs.wall_speedup is not None:
                parts.append(f"wall={obs.wall_speedup:.2f}x")
            if obs.baseline_ms is not None and obs.combo_ms is not None:
                parts.append(f"{obs.baseline_ms:.0f}→{obs.combo_ms:.0f}ms")
            if obs.rows_match is not None and not obs.rows_match:
                parts.append("ROWS_MISMATCH")
            if obs.vulnerability_types:
                parts.append(",".join(obs.vulnerability_types))
            if obs.n_distinct_plans:
                parts.append(f"{obs.n_distinct_plans}plans")

            prompt_parts.append("  ".join(parts))

        prompt_parts.append("")

    # Stats footer
    n_with_wallclock = sum(1 for o in observations if o.wall_speedup is not None)
    prompt_parts.append(
        f"---\n{len(observations)} observations across {len(by_category)} categories. "
        f"{n_with_wallclock} have wall-clock validation."
    )

    return "\n".join(prompt_parts)


def _build_structuring_prompt(analysis_text: str) -> str:
    """Build the Pass 2 structuring prompt from Pass 1 analysis."""
    return f"""Extract the findings from the analysis below into a JSON array.

Each finding should be a JSON object with these exact fields:
- "id": string, format "SF-001", "SF-002", etc.
- "claim": string, the main claim
- "category": string, one of: "join_sensitivity", "memory", "parallelism", "jit", "cost_model", "join_order", "interaction", "scan_method"
- "supporting_queries": array of strings, query IDs
- "evidence_summary": string, summary of supporting evidence
- "evidence_count": integer, number of supporting observations
- "contradicting_count": integer, number of contradicting observations
- "boundaries": array of strings, when this finding applies
- "mechanism": string, why this happens
- "confidence": string, one of "high", "medium", "low"
- "confidence_rationale": string, why this confidence level
- "implication": string, what SQL rewriting workers should do

Return ONLY a JSON array of finding objects. No markdown, no explanation.

## Analysis to Structure

{analysis_text}"""


# ── JSON repair ─────────────────────────────────────────────────────────

def _repair_json(text: str) -> str:
    """Robust JSON repair for LLM output.

    Handles: markdown fences, reasoning traces, trailing commas,
    unquoted keys, truncated output.
    """
    # Strip markdown code fences
    text = re.sub(r'^```(?:json)?\s*', '', text, flags=re.MULTILINE)
    text = re.sub(r'```\s*$', '', text, flags=re.MULTILINE)
    text = text.strip()

    # If it starts with reasoning/thinking, find the JSON part
    if not text.startswith('[') and not text.startswith('{'):
        # Find first [ or {
        bracket_pos = -1
        for i, ch in enumerate(text):
            if ch in '[{':
                bracket_pos = i
                break
        if bracket_pos >= 0:
            text = text[bracket_pos:]

    # Strip trailing content after the JSON
    if text.startswith('['):
        depth = 0
        end = -1
        for i, ch in enumerate(text):
            if ch == '[':
                depth += 1
            elif ch == ']':
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break
        if end > 0:
            text = text[:end]

    # Fix trailing commas before ] or }
    text = re.sub(r',\s*([}\]])', r'\1', text)

    return text


def parse_findings_response(text: str) -> List[ScannerFinding]:
    """Parse LLM response into ScannerFinding objects with JSON repair."""
    repaired = _repair_json(text)

    try:
        data = json.loads(repaired)
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse failed after repair: {e}")
        logger.error(f"Repaired text (first 500 chars): {repaired[:500]}")
        return []

    if isinstance(data, dict):
        # Maybe wrapped in {"findings": [...]}
        if "findings" in data:
            data = data["findings"]
        else:
            data = [data]

    if not isinstance(data, list):
        logger.error(f"Expected list, got {type(data)}")
        return []

    findings = []
    for i, item in enumerate(data):
        if not isinstance(item, dict):
            continue
        # Ensure ID exists
        if "id" not in item:
            item["id"] = f"SF-{i+1:03d}"
        try:
            findings.append(ScannerFinding.from_dict(item))
        except Exception as e:
            logger.warning(f"Failed to parse finding {item.get('id', i)}: {e}")

    return findings


# ── Blackboard loading ──────────────────────────────────────────────────

def _load_blackboard(path: Path) -> List[ScannerObservation]:
    """Load observations from scanner_blackboard.jsonl."""
    observations = []
    with open(path) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                observations.append(ScannerObservation.from_dict(data))
            except Exception as e:
                logger.warning(f"Line {line_num}: parse error: {e}")
    return observations


def _sha256_file(path: Path) -> str:
    """Compute SHA-256 of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ── Main extraction ─────────────────────────────────────────────────────

def extract_findings(
    blackboard_path: Path,
    output_path: Path,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    structuring_model: Optional[str] = None,
) -> List[ScannerFinding]:
    """Two-pass LLM extraction: reasoner → chat model.

    Pass 1: Send blackboard to reasoner for free-form analysis.
    Pass 2: Send analysis to chat model for JSON structuring.

    Args:
        blackboard_path: Path to scanner_blackboard.jsonl
        output_path: Path to write scanner_findings.json
        provider: LLM provider (default: from settings)
        model: Pass 1 model (default: deepseek-reasoner)
        structuring_model: Pass 2 model (default: deepseek-chat)

    Returns:
        List of extracted ScannerFinding objects.
    """
    from qt_shared.llm import create_llm_client

    # Keep previous findings for diffing
    if output_path.exists():
        prev_path = output_path.with_suffix(".prev.json")
        shutil.copy2(output_path, prev_path)
        logger.info(f"Previous findings saved to {prev_path}")

    # Build Pass 1 prompt
    prompt = build_findings_prompt(blackboard_path)
    print(f"  Pass 1 prompt: {len(prompt)} chars ({len(prompt)//4} est. tokens)")

    # Pass 1: Reasoner analysis
    reasoner = create_llm_client(
        provider=provider,
        model=model,
    )
    if reasoner is None:
        raise RuntimeError("No LLM client available for Pass 1 (reasoner)")

    print("  Pass 1: Sending to reasoner for analysis...")
    analysis_text = reasoner.analyze(prompt)
    print(f"  Pass 1 response: {len(analysis_text)} chars")

    # Pass 2: Structure into JSON
    structuring_prompt = _build_structuring_prompt(analysis_text)

    chat_client = create_llm_client(
        provider=provider,
        model=structuring_model or "deepseek-chat",
    )
    if chat_client is None:
        # Fallback: try parsing reasoner output directly
        logger.warning("No chat model for Pass 2, trying direct parse")
        findings = parse_findings_response(analysis_text)
    else:
        print("  Pass 2: Structuring into JSON...")
        structured_text = chat_client.analyze(structuring_prompt)
        print(f"  Pass 2 response: {len(structured_text)} chars")
        findings = parse_findings_response(structured_text)

    if not findings:
        logger.error("No findings extracted! Check LLM output.")
        return []

    # Write output with metadata
    output_data = {
        "metadata": {
            "blackboard_sha256": _sha256_file(blackboard_path),
            "extracted_at": datetime.now().isoformat(),
            "pass1_model": model or "default",
            "pass2_model": structuring_model or "deepseek-chat",
            "n_findings": len(findings),
        },
        "findings": [f.to_dict() for f in findings],
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output_data, indent=2))

    print(f"  Findings written: {output_path}")
    print(f"  Total findings: {len(findings)}")

    # Summary
    by_confidence = {}
    for f in findings:
        by_confidence[f.confidence] = by_confidence.get(f.confidence, 0) + 1
    print(f"  Confidence: {by_confidence}")

    by_category = {}
    for f in findings:
        by_category[f.category] = by_category.get(f.category, 0) + 1
    print(f"  Categories: {by_category}")

    return findings


def load_findings(path: Path) -> List[ScannerFinding]:
    """Load findings from scanner_findings.json."""
    data = json.loads(path.read_text())
    findings_data = data.get("findings", data if isinstance(data, list) else [])
    return [ScannerFinding.from_dict(f) for f in findings_data]


# ── CLI entry point ─────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Extract scanner findings from blackboard via LLM"
    )
    parser.add_argument(
        "benchmark_dir",
        type=Path,
        help="Path to benchmark directory",
    )
    parser.add_argument(
        "--prompt-only",
        action="store_true",
        help="Print the findings prompt without calling LLM",
    )
    parser.add_argument(
        "--provider",
        type=str,
        default=None,
        help="LLM provider override",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=None,
        help="Pass 1 model override",
    )
    parser.add_argument(
        "--structuring-model",
        type=str,
        default=None,
        help="Pass 2 model override (default: deepseek-chat)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    blackboard_path = args.benchmark_dir / "scanner_blackboard.jsonl"
    if not blackboard_path.exists():
        print(f"  ERROR: No blackboard at {blackboard_path}")
        print(f"  Run: python -m qt_sql.scanner_knowledge.blackboard {args.benchmark_dir}")
        exit(1)

    if args.prompt_only:
        prompt = build_findings_prompt(blackboard_path)
        print(prompt)
    else:
        findings_path = args.benchmark_dir / "scanner_findings.json"
        extract_findings(
            blackboard_path,
            findings_path,
            provider=args.provider,
            model=args.model,
            structuring_model=args.structuring_model,
        )

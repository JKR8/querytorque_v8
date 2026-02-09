"""Compress probe results into an exploit algorithm YAML file.

Flow:
1. Build compression prompt
2. Call LLM
3. Extract YAML from response
4. Validate via yaml.safe_load()
5. Save to constraints/ and versioned copy to benchmark_dir/probe/
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import List, Optional

from .schemas import ProbeResult
from .compress_prompt import build_compression_prompt

logger = logging.getLogger(__name__)

# constraints/ directory next to this module's parent
CONSTRAINTS_DIR = Path(__file__).resolve().parent.parent / "constraints"


def _extract_yaml_from_response(response: str) -> str:
    """Extract YAML from LLM response.

    Strips <reasoning> wrapper and extracts ```yaml ... ``` block.
    """
    # Strip <reasoning> blocks
    stripped = re.sub(
        r"<reasoning>.*?</reasoning>", "", response, flags=re.DOTALL
    ).strip()

    # Extract ```yaml ... ``` block
    yaml_match = re.search(
        r"```ya?ml\s*\n(.*?)```", stripped, re.DOTALL | re.IGNORECASE
    )
    if yaml_match:
        return yaml_match.group(1).strip()

    # Fallback: try ``` ... ``` block
    code_match = re.search(r"```\s*\n(.*?)```", stripped, re.DOTALL)
    if code_match:
        return code_match.group(1).strip()

    # Final fallback: return stripped response
    logger.warning("No YAML code block found, using raw response")
    return stripped


def _validate_yaml(yaml_text: str) -> bool:
    """Validate YAML via yaml.safe_load(). Returns True if valid."""
    try:
        import yaml

        data = yaml.safe_load(yaml_text)
        if not isinstance(data, dict):
            logger.warning(f"YAML parsed but not a dict: {type(data)}")
            return False
        return True
    except Exception as e:
        logger.warning(f"YAML validation failed: {e}")
        return False


def compress_probe_results(
    probe_results: List[ProbeResult],
    previous_algorithm_text: Optional[str],
    benchmark_dir: Path,
    round_num: int = 0,
    provider: Optional[str] = None,
    model: Optional[str] = None,
) -> str:
    """Compress probe results into exploit algorithm YAML text.

    Args:
        probe_results: All probe results from this round.
        previous_algorithm_text: Previous round's algorithm text (or None).
        benchmark_dir: Benchmark directory for versioned copies.
        round_num: Current round number.
        provider: LLM provider (from .env if None).
        model: LLM model (from .env if None).

    Returns:
        The exploit algorithm YAML text (also saved to disk).
    """
    from ..generate import CandidateGenerator

    benchmark_dir = Path(benchmark_dir)

    # Detect engine from first result
    engine = "duckdb"
    if probe_results:
        engine = probe_results[0].engine

    # Normalize engine name for file paths
    engine_norm = engine.lower()
    if engine_norm in ("postgres", "pg"):
        engine_norm = "postgresql"

    # Build compression prompt
    prompt = build_compression_prompt(
        probe_results=probe_results,
        previous_algorithm_text=previous_algorithm_text,
        engine=engine,
    )

    # Call LLM
    print(f"  COMPRESS: Calling LLM to generate exploit algorithm...", flush=True)
    generator = CandidateGenerator(provider=provider, model=model)
    response = generator._analyze_with_max_tokens(prompt, max_tokens=8192)

    # Extract YAML
    yaml_text = _extract_yaml_from_response(response)

    # Validate YAML
    if not _validate_yaml(yaml_text):
        logger.warning(
            "YAML validation failed on first attempt, retrying with fix prompt..."
        )
        # Retry: ask LLM to fix the YAML
        fix_prompt = (
            "The following YAML has syntax errors. Fix the YAML syntax and output "
            "ONLY the corrected YAML inside ```yaml ... ``` markers.\n\n"
            f"```yaml\n{yaml_text}\n```"
        )
        fix_response = generator._analyze_with_max_tokens(fix_prompt, max_tokens=8192)
        yaml_text = _extract_yaml_from_response(fix_response)

        if not _validate_yaml(yaml_text):
            logger.error("YAML validation failed after retry, saving raw output")

    # Save to constraints/ (production location)
    CONSTRAINTS_DIR.mkdir(parents=True, exist_ok=True)
    algo_path = CONSTRAINTS_DIR / f"exploit_algorithm_{engine_norm}.yaml"
    algo_path.write_text(yaml_text)
    print(f"  COMPRESS: Saved to {algo_path}", flush=True)

    # Save versioned copy
    probe_dir = benchmark_dir / "probe"
    probe_dir.mkdir(parents=True, exist_ok=True)
    versioned_path = probe_dir / f"exploit_algorithm_v{round_num}.yaml"
    versioned_path.write_text(yaml_text)
    print(f"  COMPRESS: Versioned copy at {versioned_path}", flush=True)

    return yaml_text

"""Result aggregation, summary generation, and gold example curation.

This module provides utilities for:
- Generating YAML summaries of ADO runs
- Auto-curating validated wins into gold examples
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from .runner import ADOResult

logger = logging.getLogger(__name__)

# Default directory for curated examples
DEFAULT_EXAMPLES_DIR = Path(__file__).parent / "examples"


def generate_yaml_summary(
    results: List[ADOResult],
    output_file: Path,
    run_metadata: Optional[Dict[str, Any]] = None,
) -> None:
    """Generate YAML summary of ADO run.

    Args:
        results: List of ADOResult objects
        output_file: Path to write summary YAML
        run_metadata: Optional additional metadata to include
    """
    # Calculate statistics
    total = len(results)
    wins = [r for r in results if r.status == "pass" and r.speedup >= 1.1]
    failures = [r for r in results if r.status in ("fail", "error")]

    win_rate = len(wins) / total if total > 0 else 0.0
    avg_speedup = sum(r.speedup for r in wins) / len(wins) if wins else 0.0

    # Build summary
    summary = {
        "run_metadata": {
            "total_queries": total,
            "wins": len(wins),
            "failures": len(failures),
            "win_rate": round(win_rate, 3),
            "avg_speedup": round(avg_speedup, 2),
            "generated_at": datetime.now().isoformat(),
            **(run_metadata or {}),
        },
        "top_speedups": [
            {
                "query_id": r.query_id,
                "speedup": round(r.speedup, 2),
                "worker_id": r.worker_id,
                "transforms": r.transforms,
            }
            for r in sorted(wins, key=lambda x: x.speedup, reverse=True)[:10]
        ],
        "wins": [
            {
                "query_id": r.query_id,
                "speedup": round(r.speedup, 2),
                "worker_id": r.worker_id,
                "transforms": r.transforms,
                "examples_used": r.examples_used,
            }
            for r in sorted(wins, key=lambda x: x.speedup, reverse=True)
        ],
        "failures": [
            {
                "query_id": r.query_id,
                "status": r.status,
                "attempts": r.attempts,
            }
            for r in failures
        ],
    }

    # Write YAML
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w") as f:
        yaml.dump(summary, f, default_flow_style=False, sort_keys=False)

    logger.info(f"Summary written to {output_file}")


def curate_gold_example(
    result: ADOResult,
    examples_dir: Path = DEFAULT_EXAMPLES_DIR,
) -> Optional[str]:
    """Curate a single validated win into a gold example.

    Args:
        result: ADOResult with a validated win
        examples_dir: Directory to save examples

    Returns:
        Example ID if created, None otherwise
    """
    if result.status != "pass" or not result.optimized_sql:
        return None

    # Generate example ID
    speedup_str = f"{result.speedup:.2f}x".replace(".", "_")
    worker_str = f"w{result.worker_id}" if result.worker_id else "w0"
    example_id = f"{result.query_id}_{worker_str}_{speedup_str}"

    # Determine transform name for the example
    transform_name = result.transforms[0] if result.transforms else "optimization"

    # Build example structure
    example = {
        "id": example_id,
        "name": f"DSB {result.query_id.upper()} Optimization",
        "description": f"Validated {result.speedup:.2f}x speedup on DSB {result.query_id} using {transform_name}",
        "verified_speedup": f"{result.speedup:.2f}x",
        "benchmark": "dsb",
        "benchmark_queries": [result.query_id],
        "example_class": "auto_curated",
        "example": {
            "before_sql": result.original_sql,
            "after_sql": result.optimized_sql,
            "transforms": result.transforms,
            "input_slice": _truncate_sql(result.original_sql, 500),
            "output": {
                "rewrite_sets": [
                    {
                        "id": "rs_01",
                        "transform": result.transforms[0] if result.transforms else "semantic_rewrite",
                        "nodes": {"main_query": result.optimized_sql},
                        "expected_speedup": f"{result.speedup:.2f}x",
                        "risk": "low",
                    }
                ]
            },
            "key_insight": f"Auto-curated from ADO run with {result.speedup:.2f}x validated speedup",
        },
        "metadata": {
            "worker_id": result.worker_id,
            "examples_used": result.examples_used,
            "database": result.database,
            "created_at": datetime.now().isoformat(),
            "auto_curated": True,
        },
    }

    # Save example
    examples_dir.mkdir(parents=True, exist_ok=True)
    example_file = examples_dir / f"{example_id}.json"

    with open(example_file, "w") as f:
        json.dump(example, f, indent=2)

    logger.info(f"Curated gold example: {example_id}")
    return example_id


def curate_gold_examples(
    results: List[ADOResult],
    min_speedup: float = 1.5,
    examples_dir: Path = DEFAULT_EXAMPLES_DIR,
    max_examples_per_query: int = 1,
) -> List[str]:
    """Auto-curate validated wins into gold examples.

    Args:
        results: List of ADOResult objects
        min_speedup: Minimum speedup threshold for curation
        examples_dir: Directory to save examples
        max_examples_per_query: Max examples to curate per query

    Returns:
        List of example IDs that were created
    """
    # Filter candidates
    candidates = [
        r for r in results
        if r.status == "pass" and r.speedup >= min_speedup and r.optimized_sql
    ]

    # Sort by speedup (best first)
    candidates.sort(key=lambda x: x.speedup, reverse=True)

    # Track examples per query
    query_counts: Dict[str, int] = {}
    added = []

    for result in candidates:
        qid = result.query_id
        if query_counts.get(qid, 0) >= max_examples_per_query:
            continue

        example_id = curate_gold_example(result, examples_dir)
        if example_id:
            added.append(example_id)
            query_counts[qid] = query_counts.get(qid, 0) + 1

    logger.info(f"Curated {len(added)} gold examples (min speedup: {min_speedup}x)")
    return added


def _truncate_sql(sql: str, max_chars: int = 500) -> str:
    """Truncate SQL for example input_slice."""
    if len(sql) <= max_chars:
        return sql
    return sql[:max_chars] + "\n... (truncated)"


def aggregate_results(
    results: List[ADOResult],
    output_dir: Path,
    min_speedup_for_curation: float = 1.5,
    curate: bool = True,
) -> Dict[str, Any]:
    """Aggregate results, generate summary, and optionally curate examples.

    Args:
        results: List of ADOResult objects
        output_dir: Directory for output files
        min_speedup_for_curation: Minimum speedup for gold example curation
        curate: Whether to auto-curate gold examples

    Returns:
        Summary dict with statistics
    """
    output_dir = Path(output_dir)

    # Generate YAML summary
    summary_file = output_dir / "summary.yaml"
    generate_yaml_summary(results, summary_file)

    # Curate gold examples if enabled
    curated = []
    if curate:
        curated = curate_gold_examples(
            results,
            min_speedup=min_speedup_for_curation,
        )

    return {
        "summary_file": str(summary_file),
        "total_queries": len(results),
        "wins": sum(1 for r in results if r.status == "pass" and r.speedup >= 1.1),
        "failures": sum(1 for r in results if r.status in ("fail", "error")),
        "curated_examples": curated,
    }

"""Blackboard system — per-run knowledge accumulation.

After every worker validates, a BlackboardEntry captures what was learned.
Entries are saved to runs/<name>/blackboard/raw/<query_id>/worker_<N>.json.

Three main components:
- BlackboardWriter: Save entries after validation
- BlackboardReader: Load entries for collation or prompt serving
- Knowledge extraction helpers: Parse LLM responses for insights
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .schemas import BlackboardEntry

logger = logging.getLogger(__name__)

# Known transform → principle mapping (from gold examples)
TRANSFORM_PRINCIPLES = {
    "decorrelate": "Correlated subqueries re-execute per outer row; converting to JOIN eliminates per-row overhead",
    "early_filter": "Applying selective filters early reduces intermediate row counts before expensive operations",
    "pushdown": "Pushing predicates closer to table scans reduces data volume in upper operators",
    "date_cte_isolate": "Pre-filtering date dimension into CTE reduces hash join probe table from 73K to ~365 rows",
    "dimension_cte_isolate": "Pre-filtering all dimension tables into CTEs avoids repeated full-table scans",
    "prefetch_fact_join": "Pre-joining filtered dimensions with fact table before aggregation reduces join input",
    "multi_dimension_prefetch": "Pre-filtering multiple dimension tables in parallel reduces join fan-out",
    "multi_date_range_cte": "Separate CTEs for each date alias avoids ambiguous multi-way date joins",
    "single_pass_aggregation": "Consolidating repeated scans into CASE aggregates reduces I/O from N scans to 1",
    "or_to_union": "Converting OR to UNION ALL lets optimizer choose independent index paths per branch",
    "intersect_to_exists": "Replacing INTERSECT with EXISTS avoids materializing full intermediate sets",
    "materialize_cte": "Materializing a CTE used multiple times prevents redundant re-computation",
    "union_cte_split": "Splitting complex UNION into separate CTEs enables per-branch optimization",
}


class BlackboardWriter:
    """Write blackboard entries to the run directory."""

    def __init__(self, run_dir: Path):
        self.run_dir = Path(run_dir)
        self.raw_dir = self.run_dir / "blackboard" / "raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    def write_entry(self, entry: BlackboardEntry) -> Path:
        """Save a blackboard entry to disk.

        Args:
            entry: The BlackboardEntry to save.

        Returns:
            Path to the saved entry file.
        """
        query_dir = self.raw_dir / entry.query_id
        query_dir.mkdir(parents=True, exist_ok=True)

        path = query_dir / f"worker_{entry.worker_id:02d}.json"
        path.write_text(json.dumps(entry.to_dict(), indent=2))

        logger.debug(
            f"Blackboard: {entry.query_id}/W{entry.worker_id} "
            f"→ {entry.status} {entry.speedup:.2f}x"
        )
        return path

    def create_entry(
        self,
        query_id: str,
        worker_id: int,
        run_name: str,
        status: str,
        speedup: float,
        transforms: List[str],
        examples_used: List[str],
        strategy: str = "",
        error_category: Optional[str] = None,
        error_messages: Optional[List[str]] = None,
        llm_response: str = "",
        query_intent: str = "",
        failure_analysis: str = "",
    ) -> BlackboardEntry:
        """Create a BlackboardEntry with knowledge extraction.

        Extracts what_worked/why_it_worked from the LLM response for wins,
        and what_failed/why_it_failed for failures.

        Args:
            query_id: Query identifier
            worker_id: Worker number
            run_name: Name of the current run
            status: Outcome (WIN|IMPROVED|NEUTRAL|REGRESSION|ERROR)
            speedup: Measured speedup ratio
            transforms: Transforms applied
            examples_used: Example IDs used in prompt
            strategy: Worker strategy name
            error_category: Error category if failed
            error_messages: Error messages if failed
            llm_response: Raw LLM response (for knowledge extraction)
            query_intent: Semantic intent of the query
            failure_analysis: Analyst failure analysis text

        Returns:
            BlackboardEntry ready to be written.
        """
        entry = BlackboardEntry(
            query_id=query_id,
            worker_id=worker_id,
            run_name=run_name,
            timestamp=datetime.now().isoformat(),
            query_intent=query_intent,
            examples_used=examples_used,
            strategy=strategy,
            status=status,
            speedup=speedup,
            transforms_applied=transforms,
            error_category=error_category,
            error_messages=error_messages or [],
        )

        # Extract knowledge based on outcome
        if status in ("WIN", "IMPROVED"):
            entry.what_worked, entry.why_it_worked = _extract_success_knowledge(
                llm_response, transforms, speedup,
            )
            entry.principle = _map_transforms_to_principle(transforms)

        elif status in ("REGRESSION", "ERROR", "FAIL"):
            entry.what_failed, entry.why_it_failed = _extract_failure_knowledge(
                error_messages or [], error_category, failure_analysis,
            )

        return entry


class BlackboardReader:
    """Read blackboard entries from a run directory."""

    def __init__(self, run_dir: Path):
        self.run_dir = Path(run_dir)
        self.raw_dir = self.run_dir / "blackboard" / "raw"

    def load_all(self) -> List[BlackboardEntry]:
        """Load all blackboard entries from the run."""
        entries = []
        if not self.raw_dir.exists():
            return entries

        for query_dir in sorted(self.raw_dir.iterdir()):
            if not query_dir.is_dir():
                continue
            for path in sorted(query_dir.glob("worker_*.json")):
                try:
                    data = json.loads(path.read_text())
                    entries.append(BlackboardEntry.from_dict(data))
                except Exception as e:
                    logger.warning(f"Failed to load {path}: {e}")

        return entries

    def load_for_query(self, query_id: str) -> List[BlackboardEntry]:
        """Load blackboard entries for a specific query."""
        query_dir = self.raw_dir / query_id
        if not query_dir.exists():
            return []

        entries = []
        for path in sorted(query_dir.glob("worker_*.json")):
            try:
                data = json.loads(path.read_text())
                entries.append(BlackboardEntry.from_dict(data))
            except Exception:
                continue
        return entries

    def load_collated(self) -> Dict[str, Any]:
        """Load the collated blackboard summary."""
        path = self.run_dir / "blackboard" / "collated.json"
        if path.exists():
            return json.loads(path.read_text())
        return {}

    def count_entries(self) -> int:
        """Count total blackboard entries."""
        if not self.raw_dir.exists():
            return 0
        return sum(
            1
            for qdir in self.raw_dir.iterdir()
            if qdir.is_dir()
            for _ in qdir.glob("worker_*.json")
        )


# =============================================================================
# Knowledge extraction helpers
# =============================================================================


def _extract_success_knowledge(
    llm_response: str,
    transforms: List[str],
    speedup: float,
) -> tuple[Optional[str], Optional[str]]:
    """Extract what_worked and why_it_worked from a successful optimization.

    Parses the LLM response for "Changes:" and "Expected speedup:" sections,
    and supplements with deterministic transform+speedup data.
    """
    what_worked = None
    why_it_worked = None

    if llm_response:
        # Try to find "Changes:" section
        changes_match = re.search(
            r'Changes?:\s*(.+?)(?:\n|Expected|```|$)',
            llm_response, re.IGNORECASE | re.DOTALL,
        )
        if changes_match:
            what_worked = changes_match.group(1).strip()[:500]

    # Supplement with deterministic data
    if transforms:
        transform_str = ", ".join(transforms)
        if what_worked:
            what_worked = f"Applied {transform_str}: {what_worked}"
        else:
            what_worked = f"Applied {transform_str} achieving {speedup:.2f}x speedup"

        # Look up known principles for the why
        principles = [
            TRANSFORM_PRINCIPLES.get(t, "")
            for t in transforms
            if t in TRANSFORM_PRINCIPLES
        ]
        if principles:
            why_it_worked = "; ".join(p for p in principles if p)

    return what_worked, why_it_worked


def _extract_failure_knowledge(
    error_messages: List[str],
    error_category: Optional[str],
    failure_analysis: str = "",
) -> tuple[Optional[str], Optional[str]]:
    """Extract what_failed and why_it_failed from a failed optimization."""
    what_failed = None
    why_it_failed = None

    if error_messages:
        what_failed = " | ".join(msg[:200] for msg in error_messages[:3])

    if failure_analysis:
        why_it_failed = failure_analysis[:500]
    elif error_category:
        category_explanations = {
            "syntax": "SQL syntax error in the rewritten query",
            "semantic": "Query results differ from original (wrong rows/columns)",
            "timeout": "Rewritten query timed out (likely regression)",
            "execution": "Runtime error during query execution",
        }
        why_it_failed = category_explanations.get(
            error_category, f"Error category: {error_category}"
        )

    return what_failed, why_it_failed


def _map_transforms_to_principle(transforms: List[str]) -> Optional[str]:
    """Map a list of transforms to the best matching principle name."""
    if not transforms:
        return None

    # Use the first transform that has a known principle
    for t in transforms:
        if t in TRANSFORM_PRINCIPLES:
            return t

    # Fall back to first transform
    return transforms[0]


# =============================================================================
# CLI entry point: python3 -m ado.blackboard
# =============================================================================

def main():
    """CLI entry point for blackboard operations."""
    import sys

    if len(sys.argv) < 3:
        print("Usage:")
        print("  python3 -m ado.blackboard list <run_dir>")
        print("  python3 -m ado.blackboard collate <run_dir>")
        print("  python3 -m ado.blackboard cleanup <run_dir>")
        sys.exit(1)

    command = sys.argv[1]
    run_dir = Path(sys.argv[2])

    if command == "list":
        reader = BlackboardReader(run_dir)
        entries = reader.load_all()
        print(f"\nBlackboard entries: {len(entries)}")
        for e in entries:
            print(f"  {e.query_id}/W{e.worker_id}: {e.status} {e.speedup:.2f}x "
                  f"[{','.join(e.transforms_applied[:3])}]")
            if e.what_worked:
                print(f"    + {e.what_worked[:80]}")
            if e.what_failed:
                print(f"    - {e.what_failed[:80]}")

    elif command == "collate":
        from .blackboard_collator import BlackboardCollator
        collator = BlackboardCollator(run_dir)
        result = collator.auto_collate()
        print(f"\nCollated {len(result.get('entries', []))} entries")
        print(f"  Wins: {result.get('summary', {}).get('wins', 0)}")
        print(f"  Regressions: {result.get('summary', {}).get('regressions', 0)}")

    elif command == "cleanup":
        from .blackboard_collator import BlackboardCollator
        collator = BlackboardCollator(run_dir)
        collator.cleanup()

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()

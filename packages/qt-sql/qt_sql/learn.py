"""Learning extraction and feedback system for ADO.

This module extracts structured learning records from optimization attempts
to enable the system to improve:
- Which examples are effective for which query patterns
- Which transforms have high success rates
- Which error patterns are recoverable
- Overall optimization effectiveness
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class AttemptSummary:
    """Summary of a single optimization attempt."""
    worker_id: int
    status: str  # "pass" | "fail" | "error"
    speedup: float
    examples_used: List[str]
    transforms: List[str] = field(default_factory=list)
    error_summary: Optional[str] = None


@dataclass
class AttemptHistory:
    """Aggregated attempt history to feed into the next round."""
    summaries: List[AttemptSummary] = field(default_factory=list)

    def add(self, summary: AttemptSummary) -> None:
        self.summaries.append(summary)

    def ranked_text(self) -> str:
        """Render relevance-ordered summaries (placeholder: speedup desc)."""
        ordered = sorted(self.summaries, key=lambda s: (s.status != "pass", -s.speedup))
        lines = ["## Previous Attempts (ordered by relevance)"]
        for s in ordered[:8]:
            base = f"- worker {s.worker_id}: {s.status}, {s.speedup:.2f}x, examples={s.examples_used}"
            if s.error_summary:
                base += f" | error: {s.error_summary}"
            lines.append(base)
        return "\n".join(lines)


@dataclass
class LearningRecord:
    """Structured learning record for training and analysis."""
    timestamp: str
    query_id: str
    query_pattern: Optional[str]  # Inferred pattern: subquery | join | aggregate | etc

    # Inputs
    examples_recommended: List[str]
    transforms_recommended: List[str]

    # Outputs
    transform_used: Optional[str]  # Primary transform that worked
    transforms_used: List[str]  # All transforms applied
    status: str  # "pass" | "fail" | "error"
    speedup: float

    # Learning outcomes
    examples_effective: bool  # Did recommended examples help?
    transform_effectiveness: float  # 0-1 score (speedup if pass, 0 if fail)
    error_category: Optional[str]  # "syntax" | "semantic" | "timeout" | "execution"
    error_messages: List[str] = field(default_factory=list)

    # Metadata
    worker_id: int = 0
    attempt_number: int = 1
    notes: str = ""

    def success_key(self) -> str:
        """Generate a key for grouping successful patterns."""
        return f"{self.query_pattern}:{','.join(sorted(self.transforms_used))}"

    def failure_key(self) -> str:
        """Generate a key for grouping failure patterns."""
        return f"{self.query_pattern}:{self.error_category}"


class Learner:
    """Extract wins/failures and maintain learning journal for training."""

    def __init__(self, journal_dir: Optional[Path] = None):
        """Initialize learner.

        Args:
            journal_dir: Directory to store learning records (journal)
        """
        self.journal_dir = journal_dir or Path("research/qt_sql/learning")
        if self.journal_dir:
            self.journal_dir.mkdir(parents=True, exist_ok=True)

    def update_history(self, history: AttemptHistory, summary: AttemptSummary) -> None:
        """Update attempt history with a new summary.

        This is for in-memory history during multi-round optimization.
        """
        history.add(summary)

    def create_learning_record(
        self,
        query_id: str,
        examples_recommended: List[str],
        transforms_recommended: List[str],
        status: str,
        speedup: float,
        transforms_used: List[str],
        worker_id: int = 0,
        attempt_number: int = 1,
        error_category: Optional[str] = None,
        error_messages: Optional[List[str]] = None,
        query_pattern: Optional[str] = None,
    ) -> LearningRecord:
        """Create a structured learning record from optimization result.

        Args:
            query_id: Query identifier
            examples_recommended: Examples provided to LLM
            transforms_recommended: Transforms suggested in prompt
            status: Optimization result status ("pass" | "fail" | "error")
            speedup: Achieved speedup (or 0 if failed)
            transforms_used: Transforms extracted from LLM response
            worker_id: Worker that produced result
            attempt_number: Which attempt (for multi-round optimization)
            error_category: Type of error if failed
            error_messages: Detailed error messages (list of strings)
            query_pattern: Inferred query pattern (for correlation analysis)

        Returns:
            LearningRecord with complete information
        """
        # Determine effectiveness metrics
        examples_effective = status == "pass" and speedup >= 1.0
        transform_effectiveness = speedup if status == "pass" else 0.0
        primary_transform = transforms_used[0] if transforms_used else None

        record = LearningRecord(
            timestamp=datetime.utcnow().isoformat(),
            query_id=query_id,
            query_pattern=query_pattern,
            examples_recommended=examples_recommended,
            transforms_recommended=transforms_recommended,
            transform_used=primary_transform,
            transforms_used=transforms_used,
            status=status,
            speedup=speedup,
            examples_effective=examples_effective,
            transform_effectiveness=transform_effectiveness,
            error_category=error_category,
            error_messages=error_messages or [],
            worker_id=worker_id,
            attempt_number=attempt_number,
        )

        return record

    def save_learning_record(self, record: LearningRecord) -> Path:
        """Save a learning record to the journal for future analysis.

        Args:
            record: The learning record to save

        Returns:
            Path to saved record
        """
        if not self.journal_dir:
            return None

        # Create query-specific directory
        query_dir = self.journal_dir / record.query_id
        query_dir.mkdir(parents=True, exist_ok=True)

        # Save with attempt number
        filename = f"attempt_{record.attempt_number:02d}.json"
        filepath = query_dir / filename

        with open(filepath, 'w') as f:
            json.dump(asdict(record), f, indent=2)

        logger.info(f"Saved learning record: {filepath}")
        return filepath

    def build_learning_summary(self) -> Dict[str, Any]:
        """Build summary statistics from all learning records in journal.

        Analyzes:
        - Overall success rate
        - Transform effectiveness
        - Example recommendation effectiveness
        - Error patterns and categories

        Returns:
            Summary dict with statistics
        """
        if not self.journal_dir or not self.journal_dir.exists():
            return {}

        all_records = []

        # Load all learning records
        for query_dir in self.journal_dir.iterdir():
            if not query_dir.is_dir():
                continue

            for attempt_file in query_dir.glob("attempt_*.json"):
                try:
                    data = json.loads(attempt_file.read_text())
                    all_records.append(data)
                except Exception as e:
                    logger.warning(f"Failed to load {attempt_file}: {e}")

        if not all_records:
            return {}

        # Compute basic statistics
        total_attempts = len(all_records)
        pass_count = sum(1 for r in all_records if r["status"] == "pass")
        fail_count = sum(1 for r in all_records if r["status"] == "fail")
        error_count = sum(1 for r in all_records if r["status"] == "error")

        success_rate = pass_count / total_attempts if total_attempts > 0 else 0
        fail_rate = fail_count / total_attempts if total_attempts > 0 else 0
        error_rate = error_count / total_attempts if total_attempts > 0 else 0
        avg_speedup = sum(r["speedup"] for r in all_records) / total_attempts if total_attempts > 0 else 0

        # Transform effectiveness (pass rate and average speedup per transform)
        transform_stats: Dict[str, Dict[str, Any]] = {}
        for record in all_records:
            for transform in record.get("transforms_used", []):
                if transform not in transform_stats:
                    transform_stats[transform] = {"pass": 0, "total": 0, "speedups": []}

                transform_stats[transform]["total"] += 1
                if record["status"] == "pass":
                    transform_stats[transform]["pass"] += 1
                    transform_stats[transform]["speedups"].append(record["speedup"])

        transform_effectiveness = {}
        for transform, stats in transform_stats.items():
            success_rate_t = stats["pass"] / stats["total"] if stats["total"] > 0 else 0
            avg_spd = sum(stats["speedups"]) / len(stats["speedups"]) if stats["speedups"] else 0
            transform_effectiveness[transform] = {
                "success_rate": success_rate_t,
                "avg_speedup": avg_spd,
                "attempts": stats["total"],
                "successful_attempts": stats["pass"],
            }

        # Example recommendation effectiveness
        example_stats: Dict[str, Dict[str, int]] = {}
        for record in all_records:
            for example in record.get("examples_recommended", []):
                if example not in example_stats:
                    example_stats[example] = {"recommended": 0, "led_to_pass": 0}

                example_stats[example]["recommended"] += 1
                if record["status"] == "pass":
                    example_stats[example]["led_to_pass"] += 1

        example_effectiveness = {}
        for example, stats in example_stats.items():
            effectiveness = (
                stats["led_to_pass"] / stats["recommended"]
                if stats["recommended"] > 0
                else 0
            )
            example_effectiveness[example] = {
                "effectiveness": effectiveness,
                "times_recommended": stats["recommended"],
                "led_to_success": stats["led_to_pass"],
            }

        # Error pattern analysis (for recovery/retry strategies)
        error_patterns: Dict[str, Dict[str, Any]] = {}
        for record in all_records:
            if record["status"] == "error":
                category = record.get("error_category", "unknown")
                if category not in error_patterns:
                    error_patterns[category] = {"count": 0, "messages": []}
                error_patterns[category]["count"] += 1
                if len(error_patterns[category]["messages"]) < 5:
                    error_patterns[category]["messages"].extend(
                        record.get("error_messages", [])[:2]
                    )

        return {
            "total_attempts": total_attempts,
            "pass_rate": success_rate,
            "fail_rate": fail_rate,
            "error_rate": error_rate,
            "avg_speedup": avg_speedup,
            "transform_effectiveness": transform_effectiveness,
            "example_effectiveness": example_effectiveness,
            "error_patterns": error_patterns,
        }

    def save_learning_summary(self) -> Path:
        """Save learning summary to journal for analysis.

        Returns:
            Path to summary file
        """
        if not self.journal_dir:
            return None

        summary = self.build_learning_summary()
        filepath = self.journal_dir / "summary.json"

        with open(filepath, 'w') as f:
            json.dump(summary, f, indent=2)

        logger.info(f"Saved learning summary: {filepath}")
        return filepath

    def generate_benchmark_history(self, benchmark_dir: Path) -> Path:
        """Generate history.json with cumulative_learnings for the analyst.

        Creates the structure the analyst expects:
        {
            "cumulative_learnings": {
                "effective_patterns": { ... },
                "known_regressions": { ... }
            }
        }

        Also loads constraint files to extract known regression examples.

        Args:
            benchmark_dir: Benchmark directory to write history.json into.

        Returns:
            Path to the generated history.json file.
        """
        summary = self.build_learning_summary()
        if not summary:
            return None

        # Build effective_patterns from transform effectiveness
        effective_patterns = {}
        transform_eff = summary.get("transform_effectiveness", {})
        for name, stats in transform_eff.items():
            if stats.get("success_rate", 0) >= 0.3 and stats.get("avg_speedup", 0) >= 1.0:
                effective_patterns[name] = {
                    "wins": stats.get("successful_attempts", 0),
                    "avg_speedup": round(stats.get("avg_speedup", 0), 2),
                    "success_rate": round(stats.get("success_rate", 0), 2),
                    "attempts": stats.get("attempts", 0),
                }

        # Build known_regressions from:
        # 1. Low success rate transforms
        # 2. Constraint files with observed_failures
        known_regressions = {}

        # From learning records: transforms that mostly fail
        for name, stats in transform_eff.items():
            if (
                stats.get("attempts", 0) >= 2
                and stats.get("success_rate", 1) < 0.3
            ):
                known_regressions[name] = (
                    f"{stats['success_rate']:.0%} success rate over "
                    f"{stats['attempts']} attempts"
                )

        # From constraint files: extract observed failures
        constraints_dir = Path(__file__).resolve().parent / "constraints"
        if constraints_dir.exists():
            for path in constraints_dir.glob("*.json"):
                try:
                    data = json.loads(path.read_text())
                    for failure in data.get("observed_failures", []):
                        query = failure.get("query", "")
                        regression = failure.get("regression", "")
                        problem = failure.get("problem", "")
                        if regression and problem:
                            key = f"{data['id']}_{query}" if query else data["id"]
                            known_regressions[key] = (
                                f"{regression}. {problem}" if regression else problem
                            )
                except Exception:
                    continue

        history = {
            "cumulative_learnings": {
                "effective_patterns": effective_patterns,
                "known_regressions": known_regressions,
            },
            "summary": {
                "total_attempts": summary.get("total_attempts", 0),
                "pass_rate": round(summary.get("pass_rate", 0), 3),
                "avg_speedup": round(summary.get("avg_speedup", 0), 3),
            },
        }

        filepath = Path(benchmark_dir) / "history.json"
        with open(filepath, 'w') as f:
            json.dump(history, f, indent=2)

        logger.info(f"Generated benchmark history: {filepath}")
        return filepath

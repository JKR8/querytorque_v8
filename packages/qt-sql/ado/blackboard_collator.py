"""Blackboard collation — synthesize raw entries into knowledge.

Two-phase process:
1. Auto-collate: Deterministic grouping, runs at end of each run
2. Manual cleanup: LLM-assisted dedup and synthesis (user-triggered)

Output: collated.json with principles[] and anti_patterns[]
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .blackboard import BlackboardReader
from .schemas import (
    BlackboardEntry,
    GlobalKnowledge,
    KnowledgeAntiPattern,
    KnowledgePrinciple,
)

logger = logging.getLogger(__name__)


class BlackboardCollator:
    """Collate raw blackboard entries into structured knowledge."""

    def __init__(self, run_dir: Path):
        self.run_dir = Path(run_dir)
        self.reader = BlackboardReader(run_dir)
        self.collated_path = self.run_dir / "blackboard" / "collated.json"

    def auto_collate(self) -> Dict[str, Any]:
        """Phase 1: Auto-populate collated.json from raw entries.

        Groups entries by outcome, captures all entries including duplicates.
        Each entry gets reviewed=False. Fast, deterministic, no LLM calls.

        Returns:
            Collated data dict.
        """
        entries = self.reader.load_all()
        if not entries:
            logger.info("No blackboard entries to collate")
            return {}

        # Group by outcome
        wins = [e for e in entries if e.status == "WIN"]
        improved = [e for e in entries if e.status == "IMPROVED"]
        neutral = [e for e in entries if e.status == "NEUTRAL"]
        regressions = [e for e in entries if e.status == "REGRESSION"]
        errors = [e for e in entries if e.status in ("ERROR", "FAIL")]

        # Extract preliminary principles from wins
        principles = self._extract_principles(wins + improved)

        # Extract anti-patterns from regressions
        anti_patterns = self._extract_anti_patterns(regressions + errors)

        collated = {
            "run_name": entries[0].run_name if entries else "",
            "collated_at": datetime.now().isoformat(),
            "summary": {
                "total_entries": len(entries),
                "wins": len(wins),
                "improved": len(improved),
                "neutral": len(neutral),
                "regressions": len(regressions),
                "errors": len(errors),
            },
            "entries": [e.to_dict() for e in entries],
            "principles": principles,
            "anti_patterns": anti_patterns,
        }

        # Write collated file
        self.collated_path.parent.mkdir(parents=True, exist_ok=True)
        self.collated_path.write_text(json.dumps(collated, indent=2))
        logger.info(
            f"Auto-collated {len(entries)} entries → {self.collated_path}"
        )

        return collated

    def _extract_principles(
        self, entries: List[BlackboardEntry],
    ) -> List[Dict[str, Any]]:
        """Extract preliminary principles from winning entries.

        Groups by principle/transform, picks strongest evidence.
        """
        # Group by principle name
        by_principle: Dict[str, List[BlackboardEntry]] = defaultdict(list)
        for e in entries:
            key = e.principle or (e.transforms_applied[0] if e.transforms_applied else "unknown")
            by_principle[key].append(e)

        principles = []
        for name, group in sorted(by_principle.items()):
            speedups = [e.speedup for e in group if e.speedup > 0]
            queries = sorted(set(e.query_id for e in group))

            # Pick the best what_worked/why_it_worked from the group
            best_entry = max(group, key=lambda e: e.speedup)

            all_transforms = set()
            for e in group:
                all_transforms.update(e.transforms_applied)

            principles.append({
                "id": name,
                "name": name.replace("_", " ").title(),
                "what": best_entry.what_worked or f"Applied {name}",
                "why": best_entry.why_it_worked or "",
                "when": "",  # Filled in during manual cleanup
                "when_not": "",
                "verified_speedups": sorted(speedups, reverse=True),
                "avg_speedup": round(sum(speedups) / len(speedups), 2) if speedups else 0,
                "queries": queries,
                "transforms": sorted(all_transforms),
                "reviewed": False,
            })

        # Sort by average speedup
        principles.sort(key=lambda p: -p.get("avg_speedup", 0))
        return principles

    def _extract_anti_patterns(
        self, entries: List[BlackboardEntry],
    ) -> List[Dict[str, Any]]:
        """Extract anti-patterns from regression/error entries."""
        # Group by error category or transform
        by_pattern: Dict[str, List[BlackboardEntry]] = defaultdict(list)
        for e in entries:
            if e.transforms_applied:
                key = e.transforms_applied[0]
            elif e.error_category:
                key = f"error_{e.error_category}"
            else:
                key = "unknown"
            by_pattern[key].append(e)

        anti_patterns = []
        for name, group in sorted(by_pattern.items()):
            speedups = [e.speedup for e in group if e.speedup > 0]
            queries = sorted(set(e.query_id for e in group))

            # Pick best failure explanation
            best_entry = min(group, key=lambda e: e.speedup if e.speedup > 0 else 999)

            anti_patterns.append({
                "id": name,
                "name": name.replace("_", " ").title(),
                "mechanism": best_entry.why_it_failed or best_entry.what_failed or "",
                "observed_regressions": sorted(speedups),
                "queries": queries,
                "avoid_when": "",  # Filled in during manual cleanup
                "reviewed": False,
            })

        return anti_patterns

    def cleanup(self) -> Dict[str, Any]:
        """Phase 2: Manual cleanup — LLM-assisted dedup and synthesis.

        Loads collated.json, calls LLM to deduplicate and merge similar
        entries, then writes cleaned version back.

        For now, provides a deterministic cleanup (merge duplicates by
        principle name). LLM synthesis can be added later.
        """
        if not self.collated_path.exists():
            logger.info("No collated.json found, running auto_collate first")
            self.auto_collate()

        collated = json.loads(self.collated_path.read_text())
        principles = collated.get("principles", [])
        anti_patterns = collated.get("anti_patterns", [])

        # Deterministic dedup: merge principles with same id
        merged_principles = self._merge_principles(principles)
        merged_anti_patterns = self._merge_anti_patterns(anti_patterns)

        # Mark all as reviewed
        for p in merged_principles:
            p["reviewed"] = True
        for a in merged_anti_patterns:
            a["reviewed"] = True

        collated["principles"] = merged_principles
        collated["anti_patterns"] = merged_anti_patterns
        collated["cleaned_at"] = datetime.now().isoformat()

        self.collated_path.write_text(json.dumps(collated, indent=2))
        logger.info(
            f"Cleaned: {len(merged_principles)} principles, "
            f"{len(merged_anti_patterns)} anti-patterns"
        )

        return collated

    @staticmethod
    def _merge_principles(principles: List[Dict]) -> List[Dict]:
        """Merge duplicate principles by id."""
        merged: Dict[str, Dict] = {}
        for p in principles:
            pid = p.get("id", "")
            if pid in merged:
                existing = merged[pid]
                existing["verified_speedups"].extend(p.get("verified_speedups", []))
                existing["queries"] = sorted(
                    set(existing["queries"]) | set(p.get("queries", []))
                )
                existing["transforms"] = sorted(
                    set(existing["transforms"]) | set(p.get("transforms", []))
                )
                # Recalculate average
                speedups = existing["verified_speedups"]
                existing["avg_speedup"] = (
                    round(sum(speedups) / len(speedups), 2) if speedups else 0
                )
                # Keep best what/why
                if p.get("avg_speedup", 0) > existing.get("avg_speedup", 0):
                    if p.get("what"):
                        existing["what"] = p["what"]
                    if p.get("why"):
                        existing["why"] = p["why"]
            else:
                merged[pid] = dict(p)

        return sorted(merged.values(), key=lambda p: -p.get("avg_speedup", 0))

    @staticmethod
    def _merge_anti_patterns(anti_patterns: List[Dict]) -> List[Dict]:
        """Merge duplicate anti-patterns by id."""
        merged: Dict[str, Dict] = {}
        for a in anti_patterns:
            aid = a.get("id", "")
            if aid in merged:
                existing = merged[aid]
                existing["observed_regressions"].extend(
                    a.get("observed_regressions", [])
                )
                existing["queries"] = sorted(
                    set(existing["queries"]) | set(a.get("queries", []))
                )
                if not existing.get("mechanism") and a.get("mechanism"):
                    existing["mechanism"] = a["mechanism"]
            else:
                merged[aid] = dict(a)

        return list(merged.values())

    def merge_to_global_knowledge(
        self,
        knowledge_dir: Path,
        dataset_name: str,
    ) -> GlobalKnowledge:
        """Merge collated knowledge into global knowledge file.

        Args:
            knowledge_dir: Directory for global knowledge files
            dataset_name: Dataset identifier (e.g., "duckdb_tpcds")

        Returns:
            Updated GlobalKnowledge.
        """
        knowledge_dir = Path(knowledge_dir)
        knowledge_dir.mkdir(parents=True, exist_ok=True)
        knowledge_path = knowledge_dir / f"{dataset_name}.json"

        # Load existing global knowledge
        existing = GlobalKnowledge(dataset=dataset_name)
        if knowledge_path.exists():
            try:
                data = json.loads(knowledge_path.read_text())
                existing = GlobalKnowledge.from_dict(data)
            except Exception:
                pass

        # Load collated data
        if not self.collated_path.exists():
            return existing

        collated = json.loads(self.collated_path.read_text())
        run_name = collated.get("run_name", "")

        # Add run to sources
        if run_name and run_name not in existing.source_runs:
            existing.source_runs.append(run_name)

        # Merge principles
        existing_ids = {p.id for p in existing.principles}
        for p_data in collated.get("principles", []):
            if not p_data.get("reviewed"):
                continue
            pid = p_data.get("id", "")
            if pid in existing_ids:
                # Update existing principle
                for ep in existing.principles:
                    if ep.id == pid:
                        ep.verified_speedups.extend(
                            p_data.get("verified_speedups", [])
                        )
                        ep.queries = sorted(
                            set(ep.queries) | set(p_data.get("queries", []))
                        )
                        if ep.verified_speedups:
                            ep.avg_speedup = round(
                                sum(ep.verified_speedups) / len(ep.verified_speedups), 2
                            )
                        break
            else:
                existing.principles.append(KnowledgePrinciple(
                    id=pid,
                    name=p_data.get("name", ""),
                    what=p_data.get("what", ""),
                    why=p_data.get("why", ""),
                    when=p_data.get("when", ""),
                    when_not=p_data.get("when_not", ""),
                    verified_speedups=p_data.get("verified_speedups", []),
                    avg_speedup=p_data.get("avg_speedup", 0),
                    queries=p_data.get("queries", []),
                    transforms=p_data.get("transforms", []),
                ))

        # Merge anti-patterns
        existing_ap_ids = {a.id for a in existing.anti_patterns}
        for a_data in collated.get("anti_patterns", []):
            if not a_data.get("reviewed"):
                continue
            aid = a_data.get("id", "")
            if aid not in existing_ap_ids:
                existing.anti_patterns.append(KnowledgeAntiPattern(
                    id=aid,
                    name=a_data.get("name", ""),
                    mechanism=a_data.get("mechanism", ""),
                    observed_regressions=a_data.get("observed_regressions", []),
                    queries=a_data.get("queries", []),
                    avoid_when=a_data.get("avoid_when", ""),
                ))

        existing.last_updated = datetime.now().isoformat()

        # Save
        knowledge_path.write_text(json.dumps(existing.to_dict(), indent=2))
        logger.info(
            f"Merged to global knowledge: {knowledge_path} "
            f"({len(existing.principles)} principles, "
            f"{len(existing.anti_patterns)} anti-patterns)"
        )

        return existing

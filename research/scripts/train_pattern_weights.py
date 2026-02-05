#!/usr/bin/env python3
"""Train pattern weight matrix for gold detection → transform recommendations.

Output: research/ml_pipeline/models/pattern_weights.json

Structure:
{
  "single_patterns": {
    "GLD-003": {
      "early_filter": {"count": 4, "avg_speedup": 1.75, "confidence": 0.85},
      "projection_prune": {"count": 1, "avg_speedup": 1.21, "confidence": 0.15}
    }
  },
  "pattern_combinations": {
    "GLD-001+GLD-003": {
      "decorrelate": {"count": 1, "avg_speedup": 2.81, "confidence": 1.0}
    }
  }
}
"""

import json
import sys
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List
import csv

# Check virtual environment
if not (hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)):
    print("❌ ERROR: Not running in virtual environment!")
    print("Please run: source .venv/bin/activate")
    sys.exit(1)

BASE = Path(__file__).parent.parent
TRAINING_DATA = BASE / "research" / "ml_pipeline" / "data" / "ml_training_data.csv"
OUTPUT_DIR = BASE / "research" / "ml_pipeline" / "models"
OUTPUT_FILE = OUTPUT_DIR / "pattern_weights.json"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


class PatternWeightTrainer:
    """Train pattern → transform weight matrix."""

    def __init__(self, training_data_path: Path, win_threshold: float = 1.2):
        self.data = self._load_data(training_data_path)
        self.win_threshold = win_threshold

    def _load_data(self, path: Path) -> List[Dict]:
        """Load training CSV."""
        with open(path) as f:
            reader = csv.DictReader(f)
            return list(reader)

    def train(self) -> Dict:
        """Train pattern weight matrix."""
        print("=" * 80)
        print("TRAINING PATTERN WEIGHT MATRIX")
        print("=" * 80)
        print(f"Dataset: {len(self.data)} queries")
        print(f"Win threshold: {self.win_threshold}x speedup")
        print()

        # Track pattern → transform mappings
        single_pattern_stats = defaultdict(lambda: defaultdict(lambda: {
            "speedups": [],
            "count": 0,
        }))

        combo_pattern_stats = defaultdict(lambda: defaultdict(lambda: {
            "speedups": [],
            "count": 0,
        }))

        # Build statistics from winning queries
        wins_processed = 0
        for row in self.data:
            speedup = float(row["speedup"])
            if speedup < self.win_threshold:
                continue

            winning_transform = row["winning_transform"]
            if not winning_transform:
                continue

            gold_detections = row["gold_detections"]
            if not gold_detections:
                continue

            gold_list = gold_detections.split("|")
            wins_processed += 1

            # Single pattern statistics
            for gold_id in gold_list:
                stats = single_pattern_stats[gold_id][winning_transform]
                stats["speedups"].append(speedup)
                stats["count"] += 1

            # Combination pattern statistics (if multiple golds detected)
            if len(gold_list) > 1:
                combo_key = "+".join(sorted(gold_list))
                stats = combo_pattern_stats[combo_key][winning_transform]
                stats["speedups"].append(speedup)
                stats["count"] += 1

        print(f"Processed {wins_processed} winning queries")
        print(f"Unique single patterns: {len(single_pattern_stats)}")
        print(f"Unique combo patterns: {len(combo_pattern_stats)}")

        # Calculate weights and confidence scores
        weights = {
            "metadata": {
                "win_threshold": self.win_threshold,
                "total_queries": len(self.data),
                "winning_queries": wins_processed,
                "training_date": "2026-02-04",
            },
            "single_patterns": {},
            "pattern_combinations": {},
        }

        # Process single patterns
        for pattern_id, transforms in single_pattern_stats.items():
            weights["single_patterns"][pattern_id] = {}

            total_occurrences = sum(t["count"] for t in transforms.values())

            for transform_name, stats in transforms.items():
                avg_speedup = sum(stats["speedups"]) / len(stats["speedups"])
                max_speedup = max(stats["speedups"])
                confidence = stats["count"] / total_occurrences

                weights["single_patterns"][pattern_id][transform_name] = {
                    "count": stats["count"],
                    "avg_speedup": round(avg_speedup, 3),
                    "max_speedup": round(max_speedup, 3),
                    "confidence": round(confidence, 3),
                }

        # Process combination patterns
        for combo_id, transforms in combo_pattern_stats.items():
            weights["pattern_combinations"][combo_id] = {}

            total_occurrences = sum(t["count"] for t in transforms.values())

            for transform_name, stats in transforms.items():
                avg_speedup = sum(stats["speedups"]) / len(stats["speedups"])
                max_speedup = max(stats["speedups"])
                confidence = stats["count"] / total_occurrences

                weights["pattern_combinations"][combo_id][transform_name] = {
                    "count": stats["count"],
                    "avg_speedup": round(avg_speedup, 3),
                    "max_speedup": round(max_speedup, 3),
                    "confidence": round(confidence, 3),
                }

        self._print_summary(weights)
        return weights

    def _print_summary(self, weights: Dict):
        """Print training summary."""

        print("\n" + "=" * 80)
        print("SINGLE PATTERN WEIGHTS")
        print("=" * 80)
        print(f"{'Pattern':<15} {'Transform':<20} {'Conf':>6} {'Count':>6} {'AvgSpd':>8} {'MaxSpd':>8}")
        print("-" * 80)

        for pattern_id, transforms in sorted(weights["single_patterns"].items()):
            for transform_name, stats in sorted(transforms.items(), key=lambda x: -x[1]["confidence"]):
                print(f"{pattern_id:<15} {transform_name:<20} "
                      f"{stats['confidence']:>5.0%} {stats['count']:>6} "
                      f"{stats['avg_speedup']:>7.2f}x {stats['max_speedup']:>7.2f}x")

        print("\n" + "=" * 80)
        print("PATTERN COMBINATION WEIGHTS")
        print("=" * 80)
        print(f"{'Pattern Combo':<35} {'Transform':<20} {'Conf':>6} {'Count':>6} {'AvgSpd':>8}")
        print("-" * 80)

        for combo_id, transforms in sorted(weights["pattern_combinations"].items(),
                                           key=lambda x: -sum(t["count"] for t in x[1].values())):
            for transform_name, stats in sorted(transforms.items(), key=lambda x: -x[1]["confidence"]):
                print(f"{combo_id:<35} {transform_name:<20} "
                      f"{stats['confidence']:>5.0%} {stats['count']:>6} "
                      f"{stats['avg_speedup']:>7.2f}x")

        # Summary statistics
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        meta = weights["metadata"]
        print(f"Training queries:      {meta['total_queries']}")
        print(f"Winning queries used:  {meta['winning_queries']}")
        print(f"Single patterns:       {len(weights['single_patterns'])}")
        print(f"Combination patterns:  {len(weights['pattern_combinations'])}")


def main():
    """Train and save pattern weights."""

    if not TRAINING_DATA.exists():
        print(f"Error: Training data not found at {TRAINING_DATA}")
        print("Run: python scripts/generate_ml_training_data.py")
        return

    trainer = PatternWeightTrainer(TRAINING_DATA)
    weights = trainer.train()

    # Save weights
    print(f"\n✓ Saving pattern weights to: {OUTPUT_FILE}")
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(weights, f, indent=2)

    print(f"\n{'=' * 80}")
    print("✅ Pattern weight matrix saved!")
    print(f"{'=' * 80}")
    print(f"\nOutput: {OUTPUT_FILE}")
    print("\nUsage in recommender:")
    print("  weights = load_pattern_weights()")
    print("  recommendations = weights['single_patterns']['GLD-003']")
    print("  # → {'early_filter': {'confidence': 0.85, 'avg_speedup': 1.75}}")


if __name__ == "__main__":
    main()

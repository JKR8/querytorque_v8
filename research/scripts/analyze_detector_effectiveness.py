#!/usr/bin/env python3
"""Analyze which AST detections actually matter for optimization.

Outputs:
- Detector effectiveness scores (correlation with speedups)
- Useful vs. noisy detections
- Recommendations for archiving/deleting detectors
"""

import json
import sys
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Tuple
import csv

# Check virtual environment
if not (hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)):
    print("❌ ERROR: Not running in virtual environment!")
    print("Please run: source .venv/bin/activate")
    sys.exit(1)

BASE = Path(__file__).parent.parent
TRAINING_DATA = BASE / "research" / "ml_pipeline" / "data" / "ml_training_data.csv"
OUTPUT_DIR = BASE / "research" / "ml_pipeline" / "analysis"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


class DetectorAnalyzer:
    """Analyze AST detector effectiveness."""

    def __init__(self, training_data_path: Path):
        self.data = self._load_data(training_data_path)
        self.win_threshold = 1.2  # Speedup >= 1.2x is a "win"

    def _load_data(self, path: Path) -> List[Dict]:
        """Load training CSV into list of dicts."""
        with open(path) as f:
            reader = csv.DictReader(f)
            return list(reader)

    def analyze_all(self) -> Dict:
        """Run all analyses."""
        print("=" * 80)
        print("AST DETECTOR EFFECTIVENESS ANALYSIS")
        print("=" * 80)
        print(f"Dataset: {len(self.data)} queries")
        print(f"Win threshold: {self.win_threshold}x speedup")
        print()

        results = {
            "basic_stats": self._basic_stats(),
            "detector_frequency": self._detector_frequency(),
            "detector_correlation": self._detector_correlation(),
            "gold_effectiveness": self._gold_effectiveness(),
            "noise_detectors": self._identify_noise(),
            "combination_patterns": self._analyze_combinations(),
            "recommendations": self._generate_recommendations(),
        }

        self._print_results(results)
        return results

    def _basic_stats(self) -> Dict:
        """Basic dataset statistics."""
        total = len(self.data)
        wins = sum(1 for row in self.data if float(row["speedup"]) >= self.win_threshold)
        with_gold = sum(1 for row in self.data if row["gold_detections"])

        return {
            "total_queries": total,
            "queries_with_wins": wins,
            "win_rate": wins / total,
            "queries_with_gold": with_gold,
            "gold_rate": with_gold / total,
        }

    def _detector_frequency(self) -> Dict:
        """Count how often each detector fires."""
        all_detections = Counter()
        gold_detections = Counter()

        for row in self.data:
            # All detections
            if row["all_detections"]:
                for det in row["all_detections"].split("|"):
                    all_detections[det] += 1

            # Gold detections
            if row["gold_detections"]:
                for det in row["gold_detections"].split("|"):
                    gold_detections[det] += 1

        return {
            "all": dict(all_detections.most_common()),
            "gold": dict(gold_detections.most_common()),
        }

    def _detector_correlation(self) -> List[Dict]:
        """Correlate detectors with speedups."""
        detector_stats = defaultdict(lambda: {
            "total_occurrences": 0,
            "in_wins": 0,
            "in_losses": 0,
            "avg_speedup": 0.0,
            "max_speedup": 0.0,
            "win_rate": 0.0,
            "speedups": [],
        })

        for row in self.data:
            speedup = float(row["speedup"])
            is_win = speedup >= self.win_threshold

            if row["all_detections"]:
                detectors = row["all_detections"].split("|")
                for det in detectors:
                    stats = detector_stats[det]
                    stats["total_occurrences"] += 1
                    stats["speedups"].append(speedup)
                    stats["max_speedup"] = max(stats["max_speedup"], speedup)

                    if is_win:
                        stats["in_wins"] += 1
                    else:
                        stats["in_losses"] += 1

        # Calculate averages and win rates
        results = []
        for detector, stats in detector_stats.items():
            if stats["total_occurrences"] > 0:
                stats["avg_speedup"] = sum(stats["speedups"]) / len(stats["speedups"])
                stats["win_rate"] = stats["in_wins"] / stats["total_occurrences"]

                # Calculate effectiveness score
                # Score = win_rate * avg_speedup * log(occurrences)
                # High score = frequently appears in winning queries with high speedup
                import math
                stats["effectiveness_score"] = (
                    stats["win_rate"] *
                    stats["avg_speedup"] *
                    math.log1p(stats["total_occurrences"])
                )

                results.append({
                    "detector": detector,
                    **stats
                })

        # Sort by effectiveness score
        results.sort(key=lambda x: -x["effectiveness_score"])
        return results

    def _gold_effectiveness(self) -> Dict:
        """Analyze gold detector effectiveness."""
        gold_stats = {}

        for row in self.data:
            if not row["gold_detections"]:
                continue

            speedup = float(row["speedup"])
            is_win = speedup >= self.win_threshold
            transform = row["winning_transform"]

            for gold in row["gold_detections"].split("|"):
                if gold not in gold_stats:
                    gold_stats[gold] = {
                        "occurrences": 0,
                        "wins": 0,
                        "speedups": [],
                        "transforms": Counter(),
                    }

                stats = gold_stats[gold]
                stats["occurrences"] += 1
                stats["speedups"].append(speedup)

                if is_win:
                    stats["wins"] += 1
                    if transform:
                        stats["transforms"][transform] += 1

        # Calculate metrics
        for gold, stats in gold_stats.items():
            stats["win_rate"] = stats["wins"] / stats["occurrences"] if stats["occurrences"] > 0 else 0
            stats["avg_speedup"] = sum(stats["speedups"]) / len(stats["speedups"]) if stats["speedups"] else 0
            stats["max_speedup"] = max(stats["speedups"]) if stats["speedups"] else 0
            stats["top_transform"] = stats["transforms"].most_common(1)[0] if stats["transforms"] else (None, 0)

        return gold_stats

    def _identify_noise(self) -> Dict:
        """Identify noisy detectors (high frequency, low correlation with wins)."""
        correlation = self._detector_correlation()

        # Noise criteria:
        # 1. High frequency (> 50% of queries) but low win rate (< 15%)
        # 2. Never appears in winning queries
        # 3. Average speedup < 1.05x

        total_queries = len(self.data)

        high_noise = []  # High frequency, low value
        zero_value = []  # Never in wins
        low_value = []   # Low speedup correlation

        for det in correlation:
            freq_pct = det["total_occurrences"] / total_queries

            # High frequency but low win rate
            if freq_pct > 0.5 and det["win_rate"] < 0.15:
                high_noise.append(det["detector"])

            # Never in wins
            if det["in_wins"] == 0:
                zero_value.append(det["detector"])

            # Low speedup correlation
            if det["avg_speedup"] < 1.05 and det["total_occurrences"] > 5:
                low_value.append(det["detector"])

        return {
            "high_frequency_low_value": high_noise,
            "never_in_wins": zero_value,
            "low_speedup_correlation": low_value,
        }

    def _analyze_combinations(self) -> List[Dict]:
        """Find detection combinations that predict specific transforms."""
        combo_stats = defaultdict(lambda: {
            "count": 0,
            "transforms": Counter(),
            "speedups": [],
        })

        for row in self.data:
            if float(row["speedup"]) < self.win_threshold:
                continue  # Only analyze winners

            if not row["gold_detections"] or not row["winning_transform"]:
                continue

            gold_set = frozenset(row["gold_detections"].split("|"))
            transform = row["winning_transform"]
            speedup = float(row["speedup"])

            stats = combo_stats[gold_set]
            stats["count"] += 1
            stats["transforms"][transform] += 1
            stats["speedups"].append(speedup)

        # Convert to list and calculate metrics
        results = []
        for gold_set, stats in combo_stats.items():
            if stats["count"] < 2:  # Need at least 2 occurrences
                continue

            top_transform, transform_count = stats["transforms"].most_common(1)[0]
            confidence = transform_count / stats["count"]

            results.append({
                "pattern": sorted(gold_set),
                "occurrences": stats["count"],
                "recommended_transform": top_transform,
                "confidence": confidence,
                "avg_speedup": sum(stats["speedups"]) / len(stats["speedups"]),
                "max_speedup": max(stats["speedups"]),
            })

        results.sort(key=lambda x: (-x["confidence"], -x["occurrences"]))
        return results

    def _generate_recommendations(self) -> Dict:
        """Generate actionable recommendations."""
        correlation = self._detector_correlation()
        noise = self._identify_noise()

        # Categorize detectors
        keep = []      # High value, keep active
        archive = []   # Low value for DuckDB, test on other DBs
        delete = []    # No value, consider removal

        for det in correlation:
            detector_id = det["detector"]

            # Skip gold detectors (never archive/delete)
            if detector_id.startswith("GLD-"):
                keep.append({
                    "detector": detector_id,
                    "reason": "Gold standard detector",
                    "win_rate": det["win_rate"],
                    "avg_speedup": det["avg_speedup"],
                })
                continue

            # High effectiveness - keep
            if det["effectiveness_score"] > 0.5:
                keep.append({
                    "detector": detector_id,
                    "reason": f"High effectiveness (score={det['effectiveness_score']:.2f})",
                    "win_rate": det["win_rate"],
                    "avg_speedup": det["avg_speedup"],
                })

            # Never in wins - consider deletion
            elif det["in_wins"] == 0 and det["total_occurrences"] > 10:
                delete.append({
                    "detector": detector_id,
                    "reason": f"Never appears in winning queries ({det['total_occurrences']} occurrences)",
                    "win_rate": 0.0,
                    "avg_speedup": det["avg_speedup"],
                })

            # Low value - archive for other DB testing
            elif det["win_rate"] < 0.15 and det["avg_speedup"] < 1.1:
                archive.append({
                    "detector": detector_id,
                    "reason": f"Low value for DuckDB (win_rate={det['win_rate']:.1%}, avg_speedup={det['avg_speedup']:.2f}x)",
                    "win_rate": det["win_rate"],
                    "avg_speedup": det["avg_speedup"],
                })

        return {
            "keep": keep,
            "archive": archive,
            "delete": delete,
        }

    def _print_results(self, results: Dict):
        """Print formatted results."""

        # Basic stats
        print("\n" + "=" * 80)
        print("BASIC STATISTICS")
        print("=" * 80)
        stats = results["basic_stats"]
        print(f"Total queries:       {stats['total_queries']}")
        print(f"Queries with wins:   {stats['queries_with_wins']} ({stats['win_rate']:.1%})")
        print(f"Queries with gold:   {stats['queries_with_gold']} ({stats['gold_rate']:.1%})")

        # Top effective detectors
        print("\n" + "=" * 80)
        print("TOP 15 MOST EFFECTIVE DETECTORS")
        print("=" * 80)
        print(f"{'Detector':<20} {'Score':>8} {'Win%':>8} {'AvgSpd':>8} {'MaxSpd':>8} {'Count':>6}")
        print("-" * 80)

        for det in results["detector_correlation"][:15]:
            print(f"{det['detector']:<20} {det['effectiveness_score']:>8.2f} "
                  f"{det['win_rate']:>7.1%} {det['avg_speedup']:>8.2f}x "
                  f"{det['max_speedup']:>8.2f}x {det['total_occurrences']:>6}")

        # Gold detector analysis
        print("\n" + "=" * 80)
        print("GOLD DETECTOR EFFECTIVENESS")
        print("=" * 80)
        print(f"{'Gold':<15} {'Win%':>8} {'AvgSpd':>8} {'MaxSpd':>8} {'Count':>6} {'Top Transform':<25}")
        print("-" * 80)

        for gold_id, stats in sorted(results["gold_effectiveness"].items()):
            top_xform, count = stats["top_transform"]
            xform_str = f"{top_xform}({count})" if top_xform else "none"
            print(f"{gold_id:<15} {stats['win_rate']:>7.1%} "
                  f"{stats['avg_speedup']:>8.2f}x {stats['max_speedup']:>8.2f}x "
                  f"{stats['occurrences']:>6} {xform_str:<25}")

        # Pattern combinations
        print("\n" + "=" * 80)
        print("PATTERN COMBINATIONS → TRANSFORMS")
        print("=" * 80)
        print(f"{'Pattern':<40} {'Transform':<20} {'Conf':>6} {'Count':>6} {'Speedup':>8}")
        print("-" * 80)

        for combo in results["combination_patterns"][:10]:
            pattern_str = "+".join(combo["pattern"][:3])
            if len(combo["pattern"]) > 3:
                pattern_str += f"+{len(combo['pattern'])-3}more"
            print(f"{pattern_str:<40} {combo['recommended_transform']:<20} "
                  f"{combo['confidence']:>5.0%} {combo['occurrences']:>6} "
                  f"{combo['avg_speedup']:>7.2f}x")

        # Noise detectors
        print("\n" + "=" * 80)
        print("NOISE DETECTORS (Low Value)")
        print("=" * 80)
        noise = results["noise_detectors"]
        print(f"\nHigh frequency, low value: {len(noise['high_frequency_low_value'])}")
        for det in noise['high_frequency_low_value'][:10]:
            print(f"  - {det}")

        print(f"\nNever in winning queries: {len(noise['never_in_wins'])}")
        for det in noise['never_in_wins'][:10]:
            print(f"  - {det}")

        # Recommendations
        print("\n" + "=" * 80)
        print("RECOMMENDATIONS")
        print("=" * 80)
        recs = results["recommendations"]

        print(f"\n✓ KEEP ({len(recs['keep'])} detectors)")
        for item in recs['keep'][:10]:
            print(f"  {item['detector']:<20} - {item['reason']}")
        if len(recs['keep']) > 10:
            print(f"  ... and {len(recs['keep']) - 10} more")

        print(f"\n📦 ARCHIVE for other DB testing ({len(recs['archive'])} detectors)")
        for item in recs['archive'][:10]:
            print(f"  {item['detector']:<20} - {item['reason']}")
        if len(recs['archive']) > 10:
            print(f"  ... and {len(recs['archive']) - 10} more")

        print(f"\n🗑️  CONSIDER DELETION ({len(recs['delete'])} detectors)")
        for item in recs['delete']:
            print(f"  {item['detector']:<20} - {item['reason']}")

        print("\n" + "=" * 80)


def main():
    """Run detector effectiveness analysis."""

    if not TRAINING_DATA.exists():
        print(f"Error: Training data not found at {TRAINING_DATA}")
        print("Run: python scripts/generate_ml_training_data.py")
        return

    analyzer = DetectorAnalyzer(TRAINING_DATA)
    results = analyzer.analyze_all()

    # Save results
    output_file = OUTPUT_DIR / "detector_effectiveness.json"
    with open(output_file, 'w') as f:
        # Convert any remaining frozensets to lists for JSON serialization
        def convert_for_json(obj):
            if isinstance(obj, frozenset):
                return sorted(list(obj))
            elif isinstance(obj, Counter):
                return dict(obj)
            elif isinstance(obj, dict):
                return {k: convert_for_json(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_for_json(item) for item in obj]
            return obj

        json.dump(convert_for_json(results), f, indent=2)

    print(f"\n✓ Full results saved to: {output_file}")


if __name__ == "__main__":
    main()

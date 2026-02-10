#!/usr/bin/env python3
"""Generate comprehensive recommendation report for all TPC-DS queries.

Outputs:
- Top 3 transform recommendations per query (with confidence, speedup, methodology)
- Gold example matches (similar queries that succeeded)
- AST pattern detections that triggered recommendations
"""

import json
import sys
import csv
from pathlib import Path
from typing import List, Dict
from collections import defaultdict

# Check virtual environment
if not (hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)):
    print("❌ ERROR: Not running in virtual environment!")
    print("Please run: source .venv/bin/activate")
    sys.exit(1)

# Add packages to path
sys.path.insert(0, "packages/qt-sql")

from qt_sql.analyzers.ast_detector import detect_antipatterns
from qt_sql.optimization.ml_recommender import load_recommender

BASE = Path(__file__).parent.parent
BENCHMARK_DIR = BASE / "research" / "experiments" / "benchmarks" / "kimi_benchmark_20260202_221828"
TRAINING_DATA = BASE / "research" / "ml_pipeline" / "data" / "ml_training_data.csv"
OUTPUT_DIR = BASE / "research" / "ml_pipeline" / "recommendations"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_REPORT = OUTPUT_DIR / "query_recommendations_report.md"
OUTPUT_JSON = OUTPUT_DIR / "query_recommendations.json"


class RecommendationReporter:
    """Generate comprehensive recommendation report."""

    def __init__(self, benchmark_dir: Path, training_data: Path):
        self.benchmark_dir = benchmark_dir
        self.recommender = load_recommender()

        # Load actual results
        self.actual_results = self._load_actual_results(training_data)

    def _load_actual_results(self, training_data: Path) -> Dict:
        """Load actual speedups and transforms from training data."""
        results = {}
        with open(training_data) as f:
            reader = csv.DictReader(f)
            for row in reader:
                qid = row["query_id"]
                results[qid] = {
                    "speedup": float(row["speedup"]),
                    "winning_transform": row["winning_transform"],
                    "has_win": row["has_win"] == "1",
                    "gold_detections": row["gold_detections"].split("|") if row["gold_detections"] else [],
                    "all_detections": row["all_detections"].split("|") if row["all_detections"] else [],
                }
        return results

    def generate_report(self) -> Dict:
        """Generate recommendations for all queries."""
        print("=" * 80)
        print("GENERATING RECOMMENDATION REPORT")
        print("=" * 80)
        print()

        if not self.recommender:
            print("❌ Error: ML models not loaded")
            print("Run: bash scripts/run_ml_training.sh")
            return {}

        all_recommendations = {}
        stats = {
            "total_queries": 0,
            "queries_with_recommendations": 0,
            "top1_correct": 0,
            "top3_correct": 0,
            "queries_with_gold": 0,
            "queries_with_wins": 0,
        }

        for qnum in range(1, 100):
            qid = f"q{qnum}"
            query_dir = self.benchmark_dir / qid
            original_sql = query_dir / "original.sql"

            if not original_sql.exists():
                continue

            stats["total_queries"] += 1

            # Load SQL
            sql = original_sql.read_text()

            # Get actual results
            actual = self.actual_results.get(qid, {})
            if actual.get("has_win"):
                stats["queries_with_wins"] += 1

            # Detect patterns
            issues = detect_antipatterns(sql, dialect="duckdb")
            gold_detections = [i.rule_id for i in issues if i.rule_id.startswith("GLD-")]
            all_detections = [i.rule_id for i in issues]

            if gold_detections:
                stats["queries_with_gold"] += 1

            # Get recommendations
            recommendations = None
            if self.recommender and gold_detections:
                recommendations = self.recommender.recommend(
                    sql=sql,
                    gold_detections=gold_detections,
                    top_k=5
                )
                stats["queries_with_recommendations"] += 1

            # Check accuracy
            if recommendations and recommendations["combined_recommendations"]:
                top1_rec = recommendations["combined_recommendations"][0]["transform"]
                top3_recs = [r["transform"] for r in recommendations["combined_recommendations"][:3]]

                if actual.get("winning_transform"):
                    if top1_rec == actual["winning_transform"]:
                        stats["top1_correct"] += 1
                    if actual["winning_transform"] in top3_recs:
                        stats["top3_correct"] += 1

            # Store results (convert similar_queries dataclass to dict)
            similar_queries = []
            if recommendations and recommendations.get("similar_queries"):
                for sq in recommendations["similar_queries"][:3]:
                    similar_queries.append({
                        "query_id": sq.query_id,
                        "distance": sq.distance,
                        "speedup": sq.speedup,
                        "winning_transform": sq.winning_transform,
                        "similarity_score": sq.similarity_score,
                    })

            all_recommendations[qid] = {
                "query_num": qnum,
                "gold_detections": gold_detections,
                "all_detections": all_detections,
                "recommendations": recommendations["combined_recommendations"][:3] if recommendations else [],
                "similar_queries": similar_queries,
                "actual_speedup": actual.get("speedup", 1.0),
                "actual_transform": actual.get("winning_transform", ""),
                "has_win": actual.get("has_win", False),
            }

            if qnum % 10 == 0:
                print(f"  Processed {qnum} queries...")

        # Calculate hit rates
        if stats["queries_with_wins"] > 0:
            stats["top1_hit_rate"] = stats["top1_correct"] / stats["queries_with_wins"]
            stats["top3_hit_rate"] = stats["top3_correct"] / stats["queries_with_wins"]
        else:
            stats["top1_hit_rate"] = 0.0
            stats["top3_hit_rate"] = 0.0

        print(f"\n✓ Processed {stats['total_queries']} queries")
        print(f"  - Queries with gold detections: {stats['queries_with_gold']}")
        print(f"  - Queries with recommendations: {stats['queries_with_recommendations']}")
        print(f"  - Queries with actual wins: {stats['queries_with_wins']}")
        print(f"  - Top-1 hit rate: {stats['top1_hit_rate']:.1%}")
        print(f"  - Top-3 hit rate: {stats['top3_hit_rate']:.1%}")

        return {
            "statistics": stats,
            "recommendations": all_recommendations,
        }

    def write_markdown_report(self, report_data: Dict):
        """Write human-readable markdown report."""
        print(f"\n✓ Writing markdown report: {OUTPUT_REPORT}")

        stats = report_data["statistics"]
        recs = report_data["recommendations"]

        with open(OUTPUT_REPORT, 'w') as f:
            # Header
            f.write("# Query Optimization Recommendations Report\n\n")
            f.write("**Generated**: 2026-02-04\n")
            f.write("**Dataset**: TPC-DS SF100 (99 queries)\n")
            f.write("**Model**: Hybrid ML (Pattern Weights + FAISS Similarity)\n\n")

            # Summary statistics
            f.write("## Executive Summary\n\n")
            f.write(f"- **Total queries analyzed**: {stats['total_queries']}\n")
            f.write(f"- **Queries with gold patterns**: {stats['queries_with_gold']} ({stats['queries_with_gold']/stats['total_queries']:.1%})\n")
            f.write(f"- **Queries with recommendations**: {stats['queries_with_recommendations']}\n")
            f.write(f"- **Queries with actual wins**: {stats['queries_with_wins']} ({stats['queries_with_wins']/stats['total_queries']:.1%})\n")
            f.write(f"- **Top-1 hit rate**: {stats['top1_hit_rate']:.1%} (ML's #1 recommendation matches actual best)\n")
            f.write(f"- **Top-3 hit rate**: {stats['top3_hit_rate']:.1%} (actual best in ML's top 3)\n\n")

            # Methodology
            f.write("## Methodology\n\n")
            f.write("For each query, recommendations are generated using:\n\n")
            f.write("1. **Pattern Detection**: AST analysis identifies gold patterns (GLD-001 to GLD-007)\n")
            f.write("2. **Pattern Weights**: Historical pattern→transform mappings with confidence scores\n")
            f.write("3. **Similarity Search**: FAISS finds structurally similar queries with speedups\n")
            f.write("4. **Combined Ranking**: Weighted combination of pattern confidence (70%) and similarity evidence (30%)\n\n")
            f.write("**Ranking Formula**:\n")
            f.write("```\n")
            f.write("combined_confidence = 0.7 × pattern_confidence + 0.3 × (similar_count / 5)\n")
            f.write("estimated_speedup = 0.7 × pattern_avg_speedup + 0.3 × similar_avg_speedup\n")
            f.write("final_score = combined_confidence × estimated_speedup\n")
            f.write("```\n\n")

            # Per-query recommendations
            f.write("---\n\n")
            f.write("## Per-Query Recommendations\n\n")

            # Group by: has recommendations, has gold, neither
            with_recs = [(qid, data) for qid, data in sorted(recs.items()) if data["recommendations"]]
            with_gold_no_recs = [(qid, data) for qid, data in sorted(recs.items()) if data["gold_detections"] and not data["recommendations"]]
            no_gold = [(qid, data) for qid, data in sorted(recs.items()) if not data["gold_detections"]]

            # Queries with recommendations
            f.write(f"### Queries with Recommendations ({len(with_recs)})\n\n")

            for qid, data in with_recs:
                self._write_query_section(f, qid, data)

            # Queries with gold but no recs
            if with_gold_no_recs:
                f.write(f"\n### Queries with Gold Patterns but No Recommendations ({len(with_gold_no_recs)})\n\n")
                for qid, data in with_gold_no_recs:
                    f.write(f"**{qid}**: Gold patterns: {', '.join(data['gold_detections'])} (no transform mappings yet)\n\n")

            # Queries with no gold patterns
            if no_gold:
                f.write(f"\n### Queries with No Gold Patterns ({len(no_gold)})\n\n")
                f.write("These queries do not match any verified optimization patterns.\n\n")
                f.write(f"Query IDs: {', '.join([qid for qid, _ in no_gold])}\n\n")

    def _write_query_section(self, f, qid: str, data: Dict):
        """Write detailed section for a single query."""
        f.write(f"#### {qid.upper()}\n\n")

        # Actual results
        if data["has_win"]:
            f.write(f"**✓ Actual Result**: {data['actual_speedup']:.2f}x speedup with `{data['actual_transform']}`\n\n")
        else:
            f.write(f"**Actual Result**: {data['actual_speedup']:.2f}x (no significant speedup)\n\n")

        # Gold patterns detected
        if data["gold_detections"]:
            f.write(f"**Gold Patterns Detected**: {', '.join(data['gold_detections'])}\n\n")

        # Top 3 recommendations
        if data["recommendations"]:
            f.write("**Top 3 Recommendations**:\n\n")

            for i, rec in enumerate(data["recommendations"], 1):
                # Highlight if matches actual
                match_marker = ""
                if rec["transform"] == data["actual_transform"]:
                    match_marker = " ✓ **MATCH**"

                f.write(f"{i}. **{rec['transform']}**{match_marker}\n")
                f.write(f"   - Combined confidence: {rec['combined_confidence']:.0%}\n")
                f.write(f"   - Estimated speedup: {rec['estimated_speedup']:.2f}x\n")

                # Methodology breakdown
                f.write(f"   - **Methodology**:\n")

                # Pattern evidence
                if rec["pattern_confidence"] > 0:
                    pattern_info = rec.get("pattern_evidence", {})
                    if isinstance(pattern_info.get("patterns"), list):
                        patterns_str = " + ".join(pattern_info["patterns"])
                    else:
                        patterns_str = pattern_info.get("pattern", "N/A")

                    f.write(f"     - Pattern-based: {rec['pattern_confidence']:.0%} confidence\n")
                    f.write(f"       - Detected: {patterns_str}\n")
                    f.write(f"       - Historical: {rec['pattern_avg_speedup']:.2f}x avg, {rec['pattern_max_speedup']:.2f}x max ({pattern_info.get('count', 0)} cases)\n")

                # Similarity evidence
                if rec["similar_query_count"] > 0:
                    f.write(f"     - Similarity-based: {rec['similar_query_count']} similar quer{'y' if rec['similar_query_count']==1 else 'ies'}\n")
                    f.write(f"       - Average speedup: {rec['similar_query_avg_speedup']:.2f}x\n")

                    # List similar queries
                    if rec["similar_queries"]:
                        f.write(f"       - Examples:\n")
                        for sim in rec["similar_queries"][:2]:
                            f.write(f"         - {sim['query_id']}: {sim['speedup']:.2f}x speedup (similarity: {sim['similarity']:.0%})\n")

                f.write("\n")

        # Similar queries (gold examples)
        if data["similar_queries"]:
            f.write("**Gold Example Matches** (structurally similar winning queries):\n\n")
            for i, sim in enumerate(data["similar_queries"], 1):
                f.write(f"{i}. **{sim['query_id']}**: {sim['speedup']:.2f}x speedup with `{sim['winning_transform']}`\n")
                f.write(f"   - Similarity: {sim['similarity_score']:.0%}\n")
                f.write(f"   - Distance: {sim['distance']:.4f}\n")

        f.write("\n---\n\n")

    def write_json_report(self, report_data: Dict):
        """Write machine-readable JSON report."""
        print(f"✓ Writing JSON report: {OUTPUT_JSON}")

        # Convert numpy types to native Python types
        def convert_types(obj):
            import numpy as np
            if isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, dict):
                return {k: convert_types(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_types(item) for item in obj]
            return obj

        clean_data = convert_types(report_data)

        with open(OUTPUT_JSON, 'w') as f:
            json.dump(clean_data, f, indent=2)


def main():
    """Generate recommendation report."""

    if not TRAINING_DATA.exists():
        print(f"Error: Training data not found at {TRAINING_DATA}")
        print("Run: python scripts/generate_ml_training_data.py")
        return

    if not BENCHMARK_DIR.exists():
        print(f"Error: Benchmark data not found at {BENCHMARK_DIR}")
        return

    reporter = RecommendationReporter(BENCHMARK_DIR, TRAINING_DATA)
    report_data = reporter.generate_report()

    if not report_data:
        return

    reporter.write_markdown_report(report_data)
    reporter.write_json_report(report_data)

    print("\n" + "=" * 80)
    print("✅ RECOMMENDATION REPORT COMPLETE")
    print("=" * 80)
    print()
    print(f"Reports generated:")
    print(f"  - Markdown: {OUTPUT_REPORT}")
    print(f"  - JSON: {OUTPUT_JSON}")
    print()
    print("View report:")
    print(f"  cat {OUTPUT_REPORT}")


if __name__ == "__main__":
    main()

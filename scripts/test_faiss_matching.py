#!/usr/bin/env python3
"""Test FAISS matching across all 99 TPC-DS queries.

This verifies that z-score normalization distributes matches more evenly
across the 7 verified transforms, rather than having total_nodes dominate.
"""

import sys
from pathlib import Path
from collections import defaultdict

BASE = Path(__file__).parent.parent

# Add packages to path
sys.path.insert(0, str(BASE / "packages" / "qt-sql"))

from qt_sql.optimization.query_recommender import get_recommendations_for_sql, get_similar_queries_for_sql

# Query files
QUERY_DIR = BASE / "packages" / "qt-sql" / "tests" / "fixtures" / "tpcds"

# The 8 gold queries
GOLD_QUERIES = {"q1", "q6", "q9", "q14", "q15", "q74", "q93", "q95"}


def test_faiss_matching():
    """Test FAISS matching on all 99 TPC-DS queries."""
    print("=" * 80)
    print("FAISS Matching Test (with Z-Score Normalization)")
    print("=" * 80)

    # Track which transforms get recommended
    transform_counts = defaultdict(int)
    transform_queries = defaultdict(list)
    gold_self_matches = {}

    # Track by query
    results = []

    for i in range(1, 100):
        query_id = f"q{i}"
        query_file = QUERY_DIR / f"query_{i:02d}.sql"

        if not query_file.exists():
            print(f"  Skipping {query_id}: file not found")
            continue

        sql = query_file.read_text()
        similar = get_similar_queries_for_sql(sql, k=3)

        if not similar:
            print(f"  {query_id}: No matches (FAISS unavailable?)")
            continue

        top_match = similar[0]
        recs = get_recommendations_for_sql(sql, top_n=3)

        # Record results
        results.append({
            "query_id": query_id,
            "is_gold": query_id in GOLD_QUERIES,
            "top_match_id": top_match.query_id,
            "top_match_transform": top_match.winning_transform,
            "top_match_distance": top_match.distance,
            "recommendations": recs,
        })

        # Check gold self-matching
        if query_id in GOLD_QUERIES:
            gold_self_matches[query_id] = {
                "self_matched": top_match.query_id == query_id,
                "top_match_id": top_match.query_id,
                "distance": top_match.distance,
            }

        # Count transform recommendations
        if recs:
            transform_counts[recs[0]] += 1
            transform_queries[recs[0]].append(query_id)

    # Print results summary
    print("\n" + "=" * 80)
    print("GOLD QUERY SELF-MATCHING (most critical test)")
    print("=" * 80)

    all_gold_passed = True
    for qid in sorted(GOLD_QUERIES, key=lambda x: int(x[1:])):
        if qid in gold_self_matches:
            match = gold_self_matches[qid]
            status = "✓ PASS" if match["self_matched"] else "✗ FAIL"
            if not match["self_matched"]:
                all_gold_passed = False
            print(f"  {qid}: {status} - top match: {match['top_match_id']} (d={match['distance']:.4f})")
        else:
            print(f"  {qid}: NOT TESTED")
            all_gold_passed = False

    if all_gold_passed:
        print("\n  ✓ ALL GOLD QUERIES CORRECTLY SELF-IDENTIFY!")
    else:
        print("\n  ✗ SOME GOLD QUERIES FAILED SELF-MATCHING!")

    # Print transform distribution
    print("\n" + "=" * 80)
    print("TRANSFORM RECOMMENDATION DISTRIBUTION")
    print("=" * 80)

    total_queries = sum(transform_counts.values())
    for transform in sorted(transform_counts.keys(), key=lambda t: transform_counts[t], reverse=True):
        count = transform_counts[transform]
        pct = count / total_queries * 100 if total_queries > 0 else 0
        print(f"  {transform:20s}: {count:3d} queries ({pct:5.1f}%)")

    print(f"\n  Total queries with recommendations: {total_queries}")

    # Show which queries match each gold
    print("\n" + "=" * 80)
    print("QUERIES MATCHED TO EACH TRANSFORM (sample)")
    print("=" * 80)

    for transform, queries in sorted(transform_queries.items(), key=lambda x: len(x[1]), reverse=True):
        sample = queries[:10]
        extra = f" (+{len(queries)-10} more)" if len(queries) > 10 else ""
        print(f"  {transform}: {', '.join(sample)}{extra}")

    # Save full results to file
    output_file = BASE / "research" / "ml_pipeline" / "faiss_matching_results.json"
    import json
    with open(output_file, 'w') as f:
        json.dump({
            "results": results,
            "transform_counts": dict(transform_counts),
            "transform_queries": dict(transform_queries),
            "gold_self_matches": gold_self_matches,
        }, f, indent=2)
    print(f"\n  Full results saved to: {output_file}")

    print("\n" + "=" * 80)
    print("DONE")
    print("=" * 80)


if __name__ == "__main__":
    test_faiss_matching()

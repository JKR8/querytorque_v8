#!/usr/bin/env python3
"""Generate summary CSV of recommendations."""

import json
import csv
from pathlib import Path

BASE = Path(__file__).parent.parent
INPUT_JSON = BASE / "research" / "ml_pipeline" / "recommendations" / "query_recommendations.json"
OUTPUT_CSV = BASE / "research" / "ml_pipeline" / "recommendations" / "recommendations_summary.csv"

def main():
    print("Generating summary CSV...")

    with open(INPUT_JSON) as f:
        data = json.load(f)

    recs = data["recommendations"]

    rows = []
    for qid, query_data in sorted(recs.items(), key=lambda x: int(x[0][1:])):
        # Top 3 recommendations
        top_recs = query_data.get("recommendations", [])[:3]

        row = {
            "query": qid,
            "actual_speedup": f"{query_data['actual_speedup']:.2f}x",
            "actual_transform": query_data["actual_transform"] or "none",
            "is_win": "✓" if query_data["has_win"] else "",
            "gold_patterns": "+".join(query_data["gold_detections"]) if query_data["gold_detections"] else "none",
            "rec1_transform": top_recs[0]["transform"] if len(top_recs) > 0 else "",
            "rec1_conf": f"{top_recs[0]['combined_confidence']:.0%}" if len(top_recs) > 0 else "",
            "rec1_speedup": f"{top_recs[0]['estimated_speedup']:.2f}x" if len(top_recs) > 0 else "",
            "rec1_match": "✓" if len(top_recs) > 0 and top_recs[0]["transform"] == query_data["actual_transform"] else "",
            "rec2_transform": top_recs[1]["transform"] if len(top_recs) > 1 else "",
            "rec2_conf": f"{top_recs[1]['combined_confidence']:.0%}" if len(top_recs) > 1 else "",
            "rec3_transform": top_recs[2]["transform"] if len(top_recs) > 2 else "",
            "rec3_conf": f"{top_recs[2]['combined_confidence']:.0%}" if len(top_recs) > 2 else "",
        }
        rows.append(row)

    # Write CSV
    fieldnames = [
        "query", "actual_speedup", "actual_transform", "is_win", "gold_patterns",
        "rec1_transform", "rec1_conf", "rec1_speedup", "rec1_match",
        "rec2_transform", "rec2_conf",
        "rec3_transform", "rec3_conf"
    ]

    with open(OUTPUT_CSV, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"✓ Summary CSV written: {OUTPUT_CSV}")
    print(f"  - {len(rows)} queries")

if __name__ == "__main__":
    main()

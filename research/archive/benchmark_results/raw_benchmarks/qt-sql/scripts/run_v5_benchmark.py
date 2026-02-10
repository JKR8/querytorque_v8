#!/usr/bin/env python3
"""
Benchmark v5 parallel adaptive rewriter across TPC-DS queries.

Writes a detailed CSV with per-query sample/full results.
"""

import argparse
import csv
import time
from pathlib import Path

from qt_sql.optimization import optimize_v5_json_queue


SAMPLE_DB_DEFAULT = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
FULL_DB_DEFAULT = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"
QUERIES_DIR_DEFAULT = "/mnt/d/TPC-DS/queries_duckdb_converted"


def load_query(query_num: int, queries_dir: Path) -> str:
    patterns = [
        f"query_{query_num}.sql",
        f"query{query_num:02d}.sql",
        f"query{query_num}.sql",
    ]
    for pattern in patterns:
        path = queries_dir / pattern
        if path.exists():
            return path.read_text()
    raise FileNotFoundError(f"Query {query_num} not found in {queries_dir}")


def prefilled_rows() -> dict[int, dict]:
    # Prefill Q2 and Q9 from the last v5 run
    return {
        2: {
            "prefilled": True,
            "valid_sample_count": 2,
            "sample_workers": "4,5",
            "sample_speedups": "0.99;1.09",
            "sample_best_speedup": 1.09,
            "full_workers": "4,5",
            "full_speedups": "0.98;1.01",
            "winner_worker": "",
            "winner_full_speedup": "",
            "winner_sample_speedup": "",
            "winner_found": False,
        },
        9: {
            "prefilled": True,
            "valid_sample_count": 5,
            "sample_workers": "1,2,3,4,5",
            "sample_speedups": "0.28;0.28;1.87;0.38;0.45",
            "sample_best_speedup": 1.87,
            "full_workers": "2,3",
            "full_speedups": "0.37;2.05",
            "winner_worker": 3,
            "winner_full_speedup": 2.05,
            "winner_sample_speedup": 1.87,
            "winner_found": True,
        },
    }


def main():
    parser = argparse.ArgumentParser(description="Run v5 benchmark on TPC-DS")
    parser.add_argument("--sample-db", default=SAMPLE_DB_DEFAULT)
    parser.add_argument("--full-db", default=FULL_DB_DEFAULT)
    parser.add_argument("--queries-dir", default=QUERIES_DIR_DEFAULT)
    parser.add_argument("--output-csv", required=True)
    parser.add_argument("--max-workers", type=int, default=5)
    parser.add_argument("--exclude", default="2,9", help="Comma-separated query numbers to skip")

    args = parser.parse_args()

    queries_dir = Path(args.queries_dir)
    output_csv = Path(args.output_csv)
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    exclude = {int(q.strip()) for q in args.exclude.split(",") if q.strip()}
    prefills = prefilled_rows()

    fieldnames = [
        "query",
        "prefilled",
        "valid_sample_count",
        "sample_workers",
        "sample_speedups",
        "sample_best_speedup",
        "full_workers",
        "full_speedups",
        "winner_found",
        "winner_worker",
        "winner_full_speedup",
        "winner_sample_speedup",
        "elapsed_s",
    ]

    with output_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for q in range(1, 100):
            if q in prefills:
                row = {
                    "query": q,
                    "elapsed_s": "",
                    **prefills[q],
                }
                writer.writerow(row)
                continue
            if q in exclude:
                # Shouldn't happen due to prefills, but skip anyway
                continue

            start = time.time()
            sql = load_query(q, queries_dir)
            valid, full_results, winner = optimize_v5_json_queue(
                sql,
                sample_db=args.sample_db,
                full_db=args.full_db,
                max_workers=args.max_workers,
                target_speedup=2.0,
            )
            elapsed_s = round(time.time() - start, 2)

            sample_workers = ",".join(str(v.worker_id) for v in valid)
            sample_speedups = ";".join(f"{v.speedup:.2f}" for v in valid)
            sample_best_speedup = max([v.speedup for v in valid], default=0.0)

            full_workers = ",".join(str(fr.sample.worker_id) for fr in full_results)
            full_speedups = ";".join(f"{fr.full_speedup:.2f}" for fr in full_results)

            row = {
                "query": q,
                "prefilled": False,
                "valid_sample_count": len(valid),
                "sample_workers": sample_workers,
                "sample_speedups": sample_speedups,
                "sample_best_speedup": f"{sample_best_speedup:.2f}",
                "full_workers": full_workers,
                "full_speedups": full_speedups,
                "winner_found": bool(winner),
                "winner_worker": winner.sample.worker_id if winner else "",
                "winner_full_speedup": f"{winner.full_speedup:.2f}" if winner else "",
                "winner_sample_speedup": f"{winner.sample.speedup:.2f}" if winner else "",
                "elapsed_s": elapsed_s,
            }
            writer.writerow(row)

            print(f"Q{q}: valid={len(valid)} winner={'yes' if winner else 'no'} elapsed={elapsed_s}s")

    print(f"Saved CSV to: {output_csv}")


if __name__ == "__main__":
    main()

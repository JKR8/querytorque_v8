#!/usr/bin/env python3
"""Collect DuckDB SF10 EXPLAIN ANALYZE plans for benchmark query sets.

Default targets:
- TPC-DS: /mnt/d/TPC-DS/tpcds_sf10.duckdb + /mnt/d/TPC-DS/queries_duckdb_converted
- DSB:    /mnt/d/TPC-DS/dsb_sf10.duckdb   + /mnt/d/dsb_sf10/queries
"""

from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path
from typing import Iterable

import duckdb


DEFAULTS = {
    "tpcds": {
        "db_path": "/mnt/d/TPC-DS/tpcds_sf10.duckdb",
        "queries_dir": "/mnt/d/TPC-DS/queries_duckdb_converted",
        "glob": "query_*.sql",
    },
    "dsb": {
        "db_path": "/mnt/d/TPC-DS/dsb_sf10.duckdb",
        "queries_dir": "/mnt/d/dsb_sf10/queries",
        "glob": "**/*.sql",
    },
}


def clean_sql(sql_text: str) -> str:
    """Remove comment-only lines and trailing semicolons."""
    lines = []
    for line in sql_text.splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    clean = "\n".join(lines).strip()
    while clean.endswith(";"):
        clean = clean[:-1].rstrip()
    return clean


def first_statement(sql_text: str, con: duckdb.DuckDBPyConnection) -> tuple[str, int]:
    """Return first statement text and total statement count."""
    stmts = con.extract_statements(sql_text)
    if not stmts:
        return sql_text, 0
    # Some source files include duplicate queries; explain only the first statement.
    return stmts[0].query, len(stmts)


def query_sort_key(path: Path) -> tuple[int, str]:
    """Sort by numeric query id when present, then by stem."""
    stem = path.stem.lower()
    match = re.search(r"query[_]?(\d+)", stem)
    if not match:
        return (10**9, stem)
    return (int(match.group(1)), stem)


def flatten_rows(rows: Iterable[tuple]) -> str:
    """Convert DB row tuples into plain text."""
    chunks = []
    for row in rows:
        for col in row:
            if col is not None:
                chunks.append(str(col))
    return "\n".join(chunks)


def collect_one_benchmark(
    benchmark: str,
    db_path: str,
    queries_dir: Path,
    query_glob: str,
    output_root: Path,
    max_queries: int | None,
) -> dict:
    bench_out = output_root / f"duckdb_{benchmark}_sf10"
    txt_out = bench_out / "plans_text"
    json_out = bench_out / "plans_json"
    txt_out.mkdir(parents=True, exist_ok=True)
    json_out.mkdir(parents=True, exist_ok=True)

    query_files = sorted(queries_dir.glob(query_glob), key=query_sort_key)
    if max_queries is not None:
        query_files = query_files[:max_queries]

    if not query_files:
        raise FileNotFoundError(
            f"No query files found for {benchmark} in {queries_dir} with glob {query_glob}"
        )

    manifest = {
        "benchmark": benchmark,
        "engine": "duckdb",
        "scale_factor": 10,
        "database": db_path,
        "queries_dir": str(queries_dir),
        "query_glob": query_glob,
        "started_at_epoch_s": time.time(),
        "results": [],
    }

    con = duckdb.connect(db_path, read_only=True)
    ok = 0
    errors = 0

    for idx, query_path in enumerate(query_files, start=1):
        query_id = query_path.stem
        sql_raw = query_path.read_text()
        sql = clean_sql(sql_raw)

        record = {
            "query_id": query_id,
            "query_file": str(query_path),
            "status": "ok",
            "error": None,
            "statement_count": None,
            "client_elapsed_ms": None,
            "execution_time_ms": None,
            "text_plan_file": str(txt_out / f"{query_id}_explain.txt"),
            "json_plan_file": str(json_out / f"{query_id}_explain.json"),
        }

        try:
            sql_single, statement_count = first_statement(sql, con)
            record["statement_count"] = statement_count

            t0 = time.perf_counter()
            text_rows = con.execute(f"EXPLAIN ANALYZE {sql_single}").fetchall()
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            plan_text = flatten_rows(text_rows)

            json_rows = con.execute(
                f"EXPLAIN (ANALYZE, FORMAT JSON) {sql_single}"
            ).fetchall()
            plan_json = None
            for row in json_rows:
                if len(row) >= 2 and row[0] == "analyzed_plan":
                    payload = row[1]
                    if isinstance(payload, str):
                        parsed = json.loads(payload)
                        if isinstance(parsed, dict):
                            plan_json = parsed
                            break
                elif len(row) == 1 and isinstance(row[0], str):
                    candidate = row[0].strip()
                    if candidate.startswith("{") and candidate.endswith("}"):
                        parsed = json.loads(candidate)
                        if isinstance(parsed, dict):
                            plan_json = parsed
                            break

            execution_time_ms = None
            if isinstance(plan_json, dict):
                latency_s = plan_json.get("latency")
                if isinstance(latency_s, (int, float)):
                    execution_time_ms = latency_s * 1000.0

            (txt_out / f"{query_id}_explain.txt").write_text(
                "\n".join(
                    [
                        f"-- benchmark: {benchmark}",
                        f"-- query_id: {query_id}",
                        f"-- source_sql: {query_path}",
                        f"-- database: {db_path}",
                        f"-- statement_count: {statement_count}",
                        f"-- client_elapsed_ms: {elapsed_ms:.3f}",
                        (
                            f"-- execution_time_ms: {execution_time_ms:.3f}"
                            if execution_time_ms is not None
                            else "-- execution_time_ms: null"
                        ),
                        "",
                        plan_text,
                        "",
                    ]
                )
            )

            (json_out / f"{query_id}_explain.json").write_text(
                json.dumps(
                    {
                        "benchmark": benchmark,
                        "query_id": query_id,
                        "source_sql": str(query_path),
                        "database": db_path,
                        "statement_count": statement_count,
                        "client_elapsed_ms": elapsed_ms,
                        "execution_time_ms": execution_time_ms,
                        "plan_json": plan_json,
                    },
                    indent=2,
                    default=str,
                )
            )

            record["client_elapsed_ms"] = round(elapsed_ms, 3)
            record["execution_time_ms"] = (
                round(execution_time_ms, 3)
                if execution_time_ms is not None
                else None
            )
            ok += 1
            print(
                f"[{benchmark}] {idx}/{len(query_files)} {query_id}: OK "
                f"({record['client_elapsed_ms']}ms)"
            )
        except Exception as exc:
            record["status"] = "error"
            record["error"] = str(exc)
            errors += 1
            print(
                f"[{benchmark}] {idx}/{len(query_files)} {query_id}: ERROR {exc}"
            )

        manifest["results"].append(record)

    con.close()

    manifest["finished_at_epoch_s"] = time.time()
    manifest["summary"] = {
        "total_queries": len(query_files),
        "ok": ok,
        "errors": errors,
        "output_dir": str(bench_out),
    }

    (bench_out / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect DuckDB SF10 EXPLAIN ANALYZE plans for TPC-DS and/or DSB."
    )
    parser.add_argument(
        "--benchmark",
        choices=["tpcds", "dsb", "all"],
        default="tpcds",
        help="Benchmark to process (default: tpcds).",
    )
    parser.add_argument(
        "--output-root",
        default="research/ado/explain_plans",
        help="Output root directory.",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Override DB path when running a single benchmark.",
    )
    parser.add_argument(
        "--queries-dir",
        default=None,
        help="Override queries dir when running a single benchmark.",
    )
    parser.add_argument(
        "--max-queries",
        type=int,
        default=None,
        help="Optional cap for debugging/partial runs.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    if args.benchmark == "all":
        benchmarks = ["tpcds", "dsb"]
    else:
        benchmarks = [args.benchmark]

    manifests = []
    for benchmark in benchmarks:
        cfg = DEFAULTS[benchmark]
        db_path = args.db_path or cfg["db_path"]
        queries_dir = Path(args.queries_dir or cfg["queries_dir"])
        query_glob = cfg["glob"]

        manifest = collect_one_benchmark(
            benchmark=benchmark,
            db_path=db_path,
            queries_dir=queries_dir,
            query_glob=query_glob,
            output_root=output_root,
            max_queries=args.max_queries,
        )
        manifests.append(manifest)

    print("\nCollection complete:")
    for manifest in manifests:
        summary = manifest["summary"]
        print(
            f"- {manifest['benchmark']}: {summary['ok']}/{summary['total_queries']} OK, "
            f"{summary['errors']} errors, output={summary['output_dir']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

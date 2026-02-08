#!/usr/bin/env python3
"""Collect PostgreSQL DSB EXPLAIN ANALYZE plans for all 52 benchmark queries.

Single JSON EXPLAIN ANALYZE per query — no redundant text run.

Outputs:
- packages/qt-sql/ado/benchmarks/postgres_dsb/explain_plans/  (JSON plans + manifest)
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path

import psycopg2

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
QUERIES_DIR = REPO_ROOT / "packages" / "qt-sql" / "ado" / "benchmarks" / "postgres_dsb" / "queries"
OUTPUT_ROOT = REPO_ROOT / "packages" / "qt-sql" / "ado" / "benchmarks" / "postgres_dsb" / "explain_plans"
ADO_EXPLAINS = OUTPUT_ROOT

TIMEOUT_MS = 300_000  # 5 minutes per query

SF_CONFIGS = {
    5:  {"dbname": "dsb_sf5",  "scale_factor": 5},
    10: {"dbname": "dsb_sf10", "scale_factor": 10},
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


def query_sort_key(path: Path) -> tuple[int, str]:
    """Sort by numeric query id when present, then by stem."""
    stem = path.stem.lower()
    match = re.search(r"query[_]?(\d+)", stem)
    if not match:
        return (10**9, stem)
    return (int(match.group(1)), stem)


def collect_all(sf: int = 5):
    cfg = SF_CONFIGS[sf]
    dsn = f"host=127.0.0.1 port=5433 dbname={cfg['dbname']} user=jakc9 password=jakc9"
    scale_factor = cfg["scale_factor"]

    json_out = OUTPUT_ROOT / "plans_json"
    json_out.mkdir(parents=True, exist_ok=True)
    ADO_EXPLAINS.mkdir(parents=True, exist_ok=True)

    query_files = sorted(QUERIES_DIR.glob("*.sql"), key=query_sort_key)
    if not query_files:
        raise FileNotFoundError(f"No query files found in {QUERIES_DIR}")

    print(f"Found {len(query_files)} queries in {QUERIES_DIR}")
    print(f"Output: {OUTPUT_ROOT}")
    print(f"Scale factor: SF{scale_factor}")
    print()

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()

    manifest = {
        "benchmark": "dsb",
        "engine": "postgresql",
        "scale_factor": scale_factor,
        "dsn": f"postgres://jakc9:jakc9@127.0.0.1:5433/{cfg['dbname']}",
        "queries_dir": str(QUERIES_DIR),
        "started_at_epoch_s": time.time(),
        "results": [],
    }

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
            "client_elapsed_ms": None,
            "execution_time_ms": None,
            "planning_time_ms": None,
            "json_plan_file": str(json_out / f"{query_id}_explain.json"),
        }

        try:
            cur.execute(f"SET statement_timeout = {TIMEOUT_MS}")

            # Single JSON EXPLAIN ANALYZE — no redundant text run
            t0 = time.perf_counter()
            cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, COSTS, VERBOSE, FORMAT JSON) {sql}")
            json_rows = cur.fetchall()
            client_elapsed_ms = (time.perf_counter() - t0) * 1000.0
            plan_json = json_rows[0][0]
            if isinstance(plan_json, str):
                plan_json = json.loads(plan_json)

            execution_time_ms = None
            planning_time_ms = None
            if isinstance(plan_json, list) and len(plan_json) > 0:
                top = plan_json[0]
                if "Execution Time" in top:
                    execution_time_ms = top["Execution Time"]
                if "Planning Time" in top:
                    planning_time_ms = top["Planning Time"]

            # Save JSON plan
            json_payload = {
                "benchmark": "dsb",
                "engine": "postgresql",
                "scale_factor": scale_factor,
                "query_id": query_id,
                "source_sql": str(query_path),
                "client_elapsed_ms": round(client_elapsed_ms, 3),
                "execution_time_ms": execution_time_ms,
                "planning_time_ms": planning_time_ms,
                "plan_json": plan_json,
            }
            (json_out / f"{query_id}_explain.json").write_text(
                json.dumps(json_payload, indent=2, default=str)
            )

            # Save ADO baseline
            ado_payload = {
                "query_id": query_id,
                "engine": "postgresql",
                "benchmark": "dsb",
                "scale_factor": scale_factor,
                "execution_time_ms": execution_time_ms,
                "planning_time_ms": planning_time_ms,
                "plan_json": plan_json,
                "original_sql": sql,
            }
            (ADO_EXPLAINS / f"{query_id}.json").write_text(
                json.dumps(ado_payload, indent=2, default=str)
            )

            record["client_elapsed_ms"] = round(client_elapsed_ms, 3)
            record["execution_time_ms"] = execution_time_ms
            record["planning_time_ms"] = planning_time_ms
            ok += 1
            print(
                f"[dsb-pg-sf{scale_factor}] {idx}/{len(query_files)} {query_id}: OK "
                f"(exec={execution_time_ms}ms, plan={planning_time_ms}ms, client={client_elapsed_ms:.1f}ms)"
            )

        except Exception as exc:
            record["status"] = "error"
            record["error"] = str(exc)
            errors += 1
            print(f"[dsb-pg-sf{scale_factor}] {idx}/{len(query_files)} {query_id}: ERROR {exc}")
            conn.rollback()

        manifest["results"].append(record)

    cur.close()
    conn.close()

    manifest["finished_at_epoch_s"] = time.time()
    manifest["summary"] = {
        "total_queries": len(query_files),
        "ok": ok,
        "errors": errors,
        "output_dir": str(OUTPUT_ROOT),
        "ado_explains_dir": str(ADO_EXPLAINS),
    }

    (OUTPUT_ROOT / "manifest.json").write_text(json.dumps(manifest, indent=2))

    print(f"\nCollection complete:")
    print(f"  Total: {len(query_files)}, OK: {ok}, Errors: {errors}")
    print(f"  Output: {OUTPUT_ROOT}")
    print(f"  Manifest: {OUTPUT_ROOT / 'manifest.json'}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Collect PG DSB EXPLAIN plans")
    parser.add_argument("--sf", type=int, choices=[5, 10], default=5,
                        help="Scale factor: 5 or 10 (default: 5)")
    args = parser.parse_args()
    collect_all(sf=args.sf)

#!/usr/bin/env python3
"""
Build ALL_OPTIMIZATIONS: every optimization attempt we ever made, organized by
database/benchmark/query with each attempt in its own subfolder.

Output: research/ALL_OPTIMIZATIONS/
  ├── duckdb_tpcds/
  │   ├── index.json
  │   ├── q1/
  │   │   ├── original.sql
  │   │   ├── kimi/optimized.sql, meta.json
  │   │   ├── kimi_extended/optimized.sql, meta.json
  │   │   ├── v1_standard/optimized.sql, meta.json
  │   │   ├── v2_standard/optimized.sql, meta.json
  │   │   ├── swarm_w1/optimized.sql, meta.json
  │   │   ├── swarm_w2/ ...
  │   │   ├── swarm_w3/ ...
  │   │   ├── swarm_w4/ ...
  │   │   ├── swarm_final/ ...
  │   │   ├── swarm_snipe/ ...
  │   │   ├── retry_neutrals_w1/ ...
  │   │   ├── retry_collect_w1/ ...
  │   │   └── attempts.json       (index of all attempts)
  │   ├── q2/ ...
  ├── postgres_dsb/
  │   └── (same structure)
  └── README.md
"""

import csv
import json
import os
import shutil
from pathlib import Path
from datetime import datetime

ROOT = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
OUT = ROOT / "research" / "ALL_OPTIMIZATIONS"
BENCH = ROOT / "packages" / "qt-sql" / "qt_sql" / "benchmarks"
ARCHIVE = ROOT / "research" / "archive"
CONSOL = ARCHIVE / "benchmark_results" / "CONSOLIDATED_BENCHMARKS"
RETRIES = ARCHIVE / "benchmark_results" / "retry_runs"

# Track all attempts globally
all_attempts = {"duckdb_tpcds": {}, "postgres_dsb": {}}


def add_attempt(engine, query_id, source, opt_sql_path, meta_path=None, extra_meta=None):
    """Register an attempt and copy files."""
    bucket = all_attempts[engine]
    if query_id not in bucket:
        bucket[query_id] = []

    # Read optimized SQL
    if not opt_sql_path.exists():
        return

    attempt = {
        "source": source,
        "opt_sql_path": str(opt_sql_path),
        "has_sql": True,
    }
    if extra_meta:
        attempt.update(extra_meta)

    # Read validation/result metadata if available
    if meta_path and meta_path.exists():
        try:
            with open(meta_path) as f:
                meta_data = json.load(f)
            attempt["validation"] = meta_data
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

    bucket[query_id].append(attempt)


def normalize_duck_qid(name):
    """Normalize DuckDB query ID: query_1 -> q1, query_23a -> q23a, q88 -> q88."""
    name = name.strip()
    if name.startswith("query_"):
        return "q" + name[6:]
    if name.startswith("q"):
        return name
    return "q" + name


# ─── DuckDB sources ──────────────────────────────────────────────────────────

def collect_kimi_full():
    """Kimi full benchmark: 99 queries with original.sql + optimized.sql + validation.json."""
    src = ARCHIVE / "experiment_history" / "experiments" / "benchmarks" / "kimi_benchmark_20260202_221828"
    if not src.exists():
        print(f"  SKIP kimi_full: {src} not found")
        return 0
    count = 0
    for qdir in sorted(src.glob("q*")):
        if not qdir.is_dir():
            continue
        qid = qdir.name  # q1, q2, ...
        opt = qdir / "optimized.sql"
        val = qdir / "validation.json"
        if opt.exists():
            add_attempt("duckdb_tpcds", qid, "kimi", opt, val)
            count += 1
    print(f"  kimi_full: {count} optimizations")
    return count


def collect_kimi_extended():
    """Kimi Q1-Q30 and Q31-Q99 extended runs with prompts/DAGs."""
    total = 0
    for dirname in ["kimi_q1-q30_optimization", "kimi_q31-q99_optimization"]:
        src = CONSOL / dirname
        if not src.exists():
            print(f"  SKIP {dirname}: not found")
            continue
        count = 0
        for qdir in sorted(src.glob("q*")):
            if not qdir.is_dir():
                continue
            qid = qdir.name
            opt = qdir / "output_optimized.sql"
            meta = qdir / "result.json"
            if opt.exists():
                add_attempt("duckdb_tpcds", qid, "kimi_extended", opt, meta)
                count += 1
        print(f"  {dirname}: {count} optimizations")
        total += count
    return total


def collect_v1_standard():
    """V1 standard benchmark output (17 queries)."""
    src = CONSOL / "benchmark_output_v1_standard"
    if not src.exists():
        print(f"  SKIP v1_standard: not found")
        return 0
    count = 0
    for qdir in sorted(src.glob("q*")):
        if not qdir.is_dir():
            continue
        qid = qdir.name
        opt = qdir / "iteration_1_optimized.sql"
        meta = qdir / "iterations_history.json"
        if opt.exists():
            add_attempt("duckdb_tpcds", qid, "v1_standard", opt, meta)
            count += 1
    print(f"  v1_standard: {count} optimizations")
    return count


def collect_v2_standard():
    """V2 standard benchmark output (88 queries)."""
    src = CONSOL / "benchmark_output_v2"
    if not src.exists():
        print(f"  SKIP v2_standard: not found")
        return 0
    count = 0
    for qdir in sorted(src.glob("q*")):
        if not qdir.is_dir():
            continue
        qid = qdir.name
        opt = qdir / "final_optimized.sql"
        meta = qdir / "status.json"
        if opt.exists():
            add_attempt("duckdb_tpcds", qid, "v2_standard", opt, meta)
            count += 1
    print(f"  v2_standard: {count} optimizations")
    return count


def collect_swarm_batch(engine, bench_subdir, batch_name, source_label):
    """Collect from a V2 swarm batch (4 workers + final + snipe per query)."""
    src = BENCH / bench_subdir / batch_name
    if not src.exists():
        print(f"  SKIP {source_label}: not found")
        return 0

    count = 0
    for qdir in sorted(src.glob("query*")):
        if not qdir.is_dir():
            continue

        # Normalize query ID
        if engine == "duckdb_tpcds":
            qid = normalize_duck_qid(qdir.name)
        else:
            qid = qdir.name  # keep PG names as-is

        # Collect each worker's SQL
        worker_files = {}
        # Try both naming conventions: worker_N_sql.sql and worker_N_extracted.sql
        for w in range(1, 5):
            for suffix in ["_sql.sql", "_extracted.sql"]:
                f = qdir / f"worker_{w}{suffix}"
                if f.exists():
                    worker_files[f"w{w}"] = f
                    break
        for name in ["final_worker_sql.sql", "snipe_worker_sql.sql"]:
            f = qdir / name
            if f.exists():
                key = name.replace("_worker_sql.sql", "").replace("_sql.sql", "")
                worker_files[key] = f

        # Try to load benchmark results for speedup data
        bench_results = {}
        for bfile in sorted(qdir.glob("benchmark_iter*.json")):
            try:
                with open(bfile) as f:
                    bench_results[bfile.stem] = json.load(f)
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

        # Also try result.json (PG format)
        result_json = qdir / "result.json"

        for worker_key, sql_path in worker_files.items():
            if sql_path.exists():
                source = f"{source_label}_{worker_key}"
                extra = {}
                if bench_results:
                    extra["benchmark_iters"] = bench_results
                add_attempt(engine, qid, source, sql_path,
                            result_json if result_json.exists() else None, extra)
                count += 1

    print(f"  {source_label}: {count} optimizations")
    return count


def collect_retry_run(run_name, source_prefix):
    """Collect from a retry run (4 workers per query)."""
    src = RETRIES / run_name
    if not src.exists():
        print(f"  SKIP {run_name}: not found")
        return 0
    count = 0
    for qdir in sorted(src.glob("q*")):
        if not qdir.is_dir():
            continue
        qid = qdir.name
        for w in range(1, 5):
            opt = qdir / f"w{w}_optimized.sql"
            if opt.exists():
                add_attempt("duckdb_tpcds", qid, f"{source_prefix}_w{w}", opt)
                count += 1
    print(f"  {run_name}: {count} optimizations")
    return count


def collect_analyst_queries():
    """Collect from analyst_query_* directories (V1 analyst format)."""
    src = BENCH / "duckdb_tpcds"
    count = 0
    for adir in sorted(src.glob("analyst_query_*")):
        if not adir.is_dir():
            continue
        # Extract query ID: analyst_query_4 -> q4, analyst_query_23a -> q23a
        qid = "q" + adir.name.replace("analyst_query_", "")
        opt = adir / "07_optimized.sql"
        val = adir / "08_validation.json"
        if opt.exists():
            add_attempt("duckdb_tpcds", qid, "analyst_v1", opt, val)
            count += 1
    print(f"  analyst_queries: {count} optimizations")
    return count


def collect_analyst_winners():
    """Collect from analyst_winners/ directory."""
    src = BENCH / "duckdb_tpcds" / "analyst_winners"
    if not src.exists():
        print(f"  SKIP analyst_winners: not found")
        return 0
    count = 0
    for opt in sorted(src.glob("*_optimized.sql")):
        # query_4_optimized.sql -> q4
        qid = "q" + opt.stem.replace("query_", "").replace("_optimized", "")
        val = opt.parent / opt.name.replace("_optimized.sql", "_validation.json")
        add_attempt("duckdb_tpcds", qid, "analyst_winner", opt, val if val.exists() else None)
        count += 1
    print(f"  analyst_winners: {count} optimizations")
    return count


def collect_analyst_sessions():
    """Collect from analyst_sessions/ with iteration subdirs."""
    src = BENCH / "duckdb_tpcds" / "analyst_sessions"
    if not src.exists():
        print(f"  SKIP analyst_sessions: not found")
        return 0
    count = 0
    for qdir in sorted(src.iterdir()):
        if not qdir.is_dir():
            continue
        qid = normalize_duck_qid(qdir.name)
        for idir in sorted(qdir.glob("iteration_*")):
            if not idir.is_dir():
                continue
            opt = idir / "optimized.sql"
            val = idir / "validation.json"
            if opt.exists():
                iter_num = idir.name.split("_")[1] if "_" in idir.name else "0"
                add_attempt("duckdb_tpcds", qid, f"analyst_session_iter{iter_num}", opt,
                            val if val.exists() else None)
                count += 1
    print(f"  analyst_sessions: {count} optimizations")
    return count


def collect_swarm_sessions(engine, bench_subdir):
    """Collect from swarm_sessions/ with iteration/worker subdirs."""
    src = BENCH / bench_subdir / "swarm_sessions"
    if not src.exists():
        print(f"  SKIP swarm_sessions ({bench_subdir}): not found")
        return 0
    count = 0
    for qdir in sorted(src.iterdir()):
        if not qdir.is_dir():
            continue
        if engine == "duckdb_tpcds":
            qid = normalize_duck_qid(qdir.name)
        else:
            qid = qdir.name

        for idir in sorted(qdir.glob("iteration_*")):
            if not idir.is_dir():
                continue
            iter_name = idir.name  # e.g. iteration_00_fan_out
            # Check for worker subdirs
            for wdir in sorted(idir.glob("worker_*")):
                if not wdir.is_dir():
                    continue
                opt = wdir / "optimized.sql"
                result = wdir / "result.json"
                if opt.exists():
                    source = f"session_{iter_name}_{wdir.name}"
                    add_attempt(engine, qid, source, opt, result if result.exists() else None)
                    count += 1
    print(f"  swarm_sessions ({bench_subdir}): {count} optimizations")
    return count


# ─── Evolutionary experiments ─────────────────────────────────────────────────

def collect_evo_experiments():
    """Collect from evolutionary experiment directories in CONSOLIDATED_BENCHMARKS."""
    total = 0
    for dirname in ["evolutionary_v1", "evolutionary_v2", "evo_experiment"]:
        src = CONSOL / dirname
        if not src.exists():
            continue
        count = 0
        for qdir in sorted(src.glob("q*")):
            if not qdir.is_dir():
                continue
            qid = qdir.name
            # Try various optimized SQL filenames
            for opt_name in ["optimized.sql", "final_optimized.sql", "output_optimized.sql",
                             "best_optimized.sql"]:
                opt = qdir / opt_name
                if opt.exists():
                    # Try various metadata filenames
                    meta = None
                    for meta_name in ["validation.json", "result.json", "status.json"]:
                        m = qdir / meta_name
                        if m.exists():
                            meta = m
                            break
                    add_attempt("duckdb_tpcds", qid, f"evo_{dirname}", opt, meta)
                    count += 1
                    break
        if count:
            print(f"  {dirname}: {count} optimizations")
            total += count
    return total


# ─── Write output ─────────────────────────────────────────────────────────────

def write_output():
    """Write all collected attempts to disk."""
    grand_total = 0

    for engine in ["duckdb_tpcds", "postgres_dsb"]:
        bucket = all_attempts[engine]
        if not bucket:
            continue

        out_dir = OUT / engine

        # Determine original query directory
        if engine == "duckdb_tpcds":
            orig_dir = BENCH / "duckdb_tpcds" / "queries"
        else:
            orig_dir = BENCH / "postgres_dsb" / "queries"

        index_entries = []
        total_attempts = 0

        for qid in sorted(bucket.keys(), key=lambda x: (len(x), x)):
            attempts = bucket[qid]
            qdir = out_dir / qid
            qdir.mkdir(parents=True, exist_ok=True)

            # Copy original SQL
            if engine == "duckdb_tpcds":
                num = qid[1:]  # q88 -> 88
                orig_file = orig_dir / f"query_{num}.sql"
            else:
                orig_file = orig_dir / f"{qid}.sql"

            if orig_file.exists():
                shutil.copy2(orig_file, qdir / "original.sql")

            # Write each attempt
            attempt_index = []
            for i, attempt in enumerate(attempts):
                source = attempt["source"]
                # Make source name filesystem-safe
                safe_source = source.replace("/", "_").replace("\\", "_").replace(" ", "_")

                att_dir = qdir / safe_source
                att_dir.mkdir(parents=True, exist_ok=True)

                # Copy optimized SQL
                opt_path = Path(attempt["opt_sql_path"])
                if opt_path.exists():
                    shutil.copy2(opt_path, att_dir / "optimized.sql")

                # Write metadata
                meta = {
                    "source": source,
                    "original_path": str(orig_file) if orig_file.exists() else None,
                    "optimized_path": str(opt_path),
                }
                if "validation" in attempt:
                    meta["validation"] = attempt["validation"]
                if "benchmark_iters" in attempt:
                    meta["benchmark_iters"] = attempt["benchmark_iters"]

                with open(att_dir / "meta.json", "w") as f:
                    json.dump(meta, f, indent=2)

                attempt_index.append({
                    "source": source,
                    "dir": safe_source,
                    "has_validation": "validation" in attempt,
                })
                total_attempts += 1

            # Write attempts.json for this query
            with open(qdir / "attempts.json", "w") as f:
                json.dump({
                    "query_id": qid,
                    "engine": engine,
                    "num_attempts": len(attempt_index),
                    "attempts": attempt_index,
                }, f, indent=2)

            index_entries.append({
                "query_id": qid,
                "num_attempts": len(attempt_index),
                "sources": [a["source"] for a in attempt_index],
            })

        # Write engine-level index
        index = {
            "engine": engine,
            "total_queries": len(index_entries),
            "total_attempts": total_attempts,
            "built_at": datetime.now().isoformat(),
            "queries": index_entries,
        }
        with open(out_dir / "index.json", "w") as f:
            json.dump(index, f, indent=2)

        print(f"\n{engine}: {len(index_entries)} queries, {total_attempts} total attempts")
        grand_total += total_attempts

    return grand_total


def write_readme(grand_total):
    lines = [
        "# ALL_OPTIMIZATIONS: Complete History of Every Optimization Attempt",
        "",
        f"> Built: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"> Total: {grand_total} optimization attempts",
        "",
        "## Purpose",
        "",
        "This folder contains EVERY optimization attempt we ever made, not just the best.",
        "For the best-per-query gold collection, see `research/GOLD/`.",
        "",
        "## Structure",
        "",
        "```",
        "ALL_OPTIMIZATIONS/",
        "├── duckdb_tpcds/",
        "│   ├── index.json",
        "│   ├── q1/",
        "│   │   ├── original.sql          # Base query",
        "│   │   ├── attempts.json         # Index of all attempts",
        "│   │   ├── kimi/",
        "│   │   │   ├── optimized.sql",
        "│   │   │   └── meta.json",
        "│   │   ├── kimi_extended/",
        "│   │   ├── v2_standard/",
        "│   │   ├── swarm_w1/",
        "│   │   ├── swarm_w2/",
        "│   │   ├── retry_neutrals_w1/",
        "│   │   └── ...",
        "├── postgres_dsb/",
        "│   └── (same structure)",
        "└── README.md",
        "```",
        "",
        "## Sources Collected",
        "",
        "### DuckDB TPC-DS",
        "| Source | Description | Queries |",
        "|--------|------------|---------|",
        "| kimi | Kimi K2.5 full benchmark | 99 |",
        "| kimi_extended | Kimi with DAG prompts | 99 |",
        "| v1_standard | V1 pipeline standard | 17 |",
        "| v2_standard | V2 pipeline standard | 88 |",
        "| swarm_w[1-4] | Swarm batch workers | ~101 each |",
        "| swarm_final | Swarm best-of selection | ~101 |",
        "| swarm_snipe | Swarm targeted improvement | ~101 |",
        "| retry_neutrals_w[1-4] | 4-worker retry on neutrals | ~43 each |",
        "| retry_collect_w[1-4] | 3-worker retry on regressions | ~25 each |",
        "| retry_under1_3x_w[1-4] | Retry on <1.3x queries | ~44 each |",
        "| retry_sf10_winners_w[1-4] | Retry SF10 validated wins | ~17 each |",
        "| analyst_v1 | V1 analyst mode | 5 |",
        "| analyst_winner | Analyst validated winners | 2 |",
        "| analyst_session_iter* | Multi-iteration analyst | varies |",
        "| session_* | Swarm session iterations | varies |",
        "",
        "### PostgreSQL DSB",
        "| Source | Description | Queries |",
        "|--------|------------|---------|",
        "| swarm1_w[1-4] | Swarm batch 1 workers | ~52 each |",
        "| swarm2_w[1-4] | Swarm batch 2 workers | ~52 each |",
        "| session_* | Swarm session iterations | 7 |",
        "",
    ]

    with open(OUT / "README.md", "w") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True, exist_ok=True)

    print("Collecting DuckDB TPC-DS optimizations...")
    collect_kimi_full()
    collect_kimi_extended()
    collect_v1_standard()
    collect_v2_standard()

    # Swarm batches (latest first, then earlier)
    collect_swarm_batch("duckdb_tpcds", "duckdb_tpcds",
                        "swarm_batch_20260208_102033", "swarm")
    collect_swarm_batch("duckdb_tpcds", "duckdb_tpcds",
                        "swarm_batch_20260208_030342", "swarm_early")
    collect_swarm_batch("duckdb_tpcds", "duckdb_tpcds",
                        "swarm_batch_20260208_101242", "swarm_mid")

    # Retry runs
    collect_retry_run("retry_neutrals", "retry_neutrals")
    collect_retry_run("retry_collect", "retry_collect")
    collect_retry_run("retry_under_1_3x", "retry_under1_3x")
    collect_retry_run("retry_neutrals_sf10_winners", "retry_sf10_winners")

    # Analyst
    collect_analyst_queries()
    collect_analyst_winners()
    collect_analyst_sessions()
    collect_swarm_sessions("duckdb_tpcds", "duckdb_tpcds")

    # Evolutionary
    collect_evo_experiments()

    print("\nCollecting PostgreSQL DSB optimizations...")
    collect_swarm_batch("postgres_dsb", "postgres_dsb",
                        "swarm_batch_20260208_142643", "swarm2")
    collect_swarm_batch("postgres_dsb", "postgres_dsb",
                        "swarm_batch_20260208_124333", "swarm1")
    collect_swarm_sessions("postgres_dsb", "postgres_dsb")

    print("\nWriting output...")
    grand_total = write_output()
    write_readme(grand_total)

    print(f"\n{'='*60}")
    print(f"  ALL_OPTIMIZATIONS built: {grand_total} total attempts")
    print(f"  Location: {OUT}")
    print(f"{'='*60}")

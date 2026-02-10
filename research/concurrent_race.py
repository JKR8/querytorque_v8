"""
Concurrent race: original + 4 workers fire simultaneously on PG.
Single query per run — pass query_id as CLI arg.
"""
import threading
import time
import sys
import psycopg2
from pathlib import Path

DSN = "postgres://jakc9:jakc9@127.0.0.1:5434/dsb_sf10"
BASE = Path("packages/qt-sql/qt_sql")
SWARM = BASE / "_benchmarks_phantom/postgres_dsb/swarm_batch_20260208_142643"
QUERIES_DIR = BASE / "benchmarks/postgres_dsb/queries"


def load_queries(query_id):
    original = (QUERIES_DIR / f"{query_id}.sql").read_text()
    workers = {}
    for i in range(1, 5):
        f = SWARM / query_id / f"worker_{i}_sql.sql"
        if f.exists():
            workers[f"W{i}"] = f.read_text()
    return {"original": original, **workers}


def run_query(label, sql, results, barrier):
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()
    barrier.wait()
    t0 = time.perf_counter()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        elapsed = time.perf_counter() - t0
        results[label] = {"elapsed_ms": round(elapsed * 1000, 2), "rows": len(rows), "error": None}
    except Exception as e:
        elapsed = time.perf_counter() - t0
        results[label] = {"elapsed_ms": round(elapsed * 1000, 2), "rows": 0, "error": str(e).split('\n')[0][:80]}
    finally:
        cur.close()
        conn.close()


def race(query_id):
    queries = load_queries(query_id)
    results = {}
    n = len(queries)
    barrier = threading.Barrier(n)

    print(f"{query_id}: racing {n} variants concurrently\n")

    threads = []
    for label, sql in queries.items():
        t = threading.Thread(target=run_query, args=(label, sql, results, barrier))
        threads.append(t)

    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall = time.perf_counter() - start

    ranked = sorted(results.items(), key=lambda x: x[1]["elapsed_ms"] if not x[1]["error"] else 999999)

    for i, (label, r) in enumerate(ranked):
        if r["error"]:
            print(f"  {i+1}. {label:10s}  ERROR: {r['error']}")
        else:
            marker = " <-- FASTEST" if i == 0 else ""
            print(f"  {i+1}. {label:10s}  {r['elapsed_ms']:>10.2f} ms  ({r['rows']} rows){marker}")

    orig_ms = results["original"]["elapsed_ms"]
    best_label, best_data = ranked[0]
    if best_label != "original" and not best_data["error"]:
        print(f"\n  => {best_label} wins ({orig_ms/best_data['elapsed_ms']:.2f}x vs original)")
    else:
        print(f"\n  => Original was fastest")
    print(f"  Wall: {wall*1000:.0f} ms")


if __name__ == "__main__":
    race(sys.argv[1])

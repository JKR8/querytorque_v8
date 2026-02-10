"""
Proper sequential validation: Q059 original vs W3
3-run method: discard 1st (warmup), average last 2
"""
import time
import psycopg2
from pathlib import Path

DSN = "postgres://jakc9:jakc9@127.0.0.1:5434/dsb_sf10"
BASE = Path("packages/qt-sql/qt_sql")

ORIGINAL = (BASE / "benchmarks/postgres_dsb/queries/query059_multi.sql").read_text()
W3 = (BASE / "_benchmarks_phantom/postgres_dsb/swarm_batch_20260208_142643/query059_multi/worker_3_sql.sql").read_text()


def run_timed(sql, label, runs=3):
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()
    times = []
    for i in range(runs):
        t0 = time.perf_counter()
        cur.execute(sql)
        rows = cur.fetchall()
        elapsed = (time.perf_counter() - t0) * 1000
        times.append(elapsed)
        tag = "(warmup)" if i == 0 else ""
        print(f"  {label} run {i+1}: {elapsed:>10.2f} ms  ({len(rows)} rows) {tag}")
    cur.close()
    conn.close()
    avg = sum(times[1:]) / len(times[1:])
    print(f"  {label} avg (last {runs-1}): {avg:.2f} ms\n")
    return avg, len(rows)


print("=== Q059 Sequential Validation: Original vs W3 ===\n")
orig_avg, orig_rows = run_timed(ORIGINAL, "original")
w3_avg, w3_rows = run_timed(W3, "W3      ")

print(f"Original: {orig_avg:.2f} ms ({orig_rows} rows)")
print(f"W3:       {w3_avg:.2f} ms ({w3_rows} rows)")
print(f"Rows match: {orig_rows == w3_rows}")
print(f"Speedup: {orig_avg / w3_avg:.2f}x")

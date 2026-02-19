"""Manually create witness data for the 4 queries the LLM couldn't solve."""

import json
import duckdb
import sqlglot
from sqlglot.optimizer.qualify import qualify
from pathlib import Path
from witness_llm import SCHEMA_DDL, clean_query

QUERIES_DIR = Path(__file__).resolve().parents[2] / \
    "packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/queries"

def _build_schema():
    import re
    schema = {}
    for stmt in SCHEMA_DDL.strip().split(';'):
        stmt = stmt.strip()
        if not stmt.upper().startswith('CREATE TABLE'):
            continue
        m = re.match(r'CREATE TABLE (\w+)\s*\((.+)\)', stmt, re.DOTALL | re.IGNORECASE)
        if not m:
            continue
        table = m.group(1).lower()
        cols = {}
        for col_def in m.group(2).split(','):
            parts = col_def.strip().split(None, 2)
            if len(parts) >= 2:
                cols[parts[0].lower()] = parts[1].upper()
        schema[table] = cols
    return schema

SCHEMA = _build_schema()

def qualify_sql(sql):
    try:
        tree = sqlglot.parse_one(sql, read='duckdb')
        return qualify(tree, schema=SCHEMA, validate_qualify_columns=False).sql(dialect='duckdb')
    except:
        return sql

def test_witness(query_file, witness_json, label=""):
    """Insert witness data and run query, return row count."""
    query_sql = (QUERIES_DIR / query_file).read_text()

    # Handle multi-statement queries (Q039 has 2 statements separated by ;)
    stmts = [s.strip() for s in query_sql.strip().rstrip(';').split(';') if s.strip()]

    con = duckdb.connect(":memory:")
    for stmt in SCHEMA_DDL.split(';\n'):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)

    # Insert witness data
    tables = witness_json.get("tables", {})
    total_inserts = 0
    for tname, tdata in tables.items():
        cols = tdata["columns"]
        for row in tdata["rows"]:
            vals = []
            for v in row:
                if v is None: vals.append("NULL")
                elif isinstance(v, str): vals.append(f"'{v}'")
                else: vals.append(str(v))
            sql = f"INSERT INTO {tname} ({', '.join(cols)}) VALUES ({', '.join(vals)})"
            con.execute(sql)
            total_inserts += 1

    # Run each statement
    total_rows = 0
    for i, stmt in enumerate(stmts):
        stmt = clean_query(stmt)
        stmt = qualify_sql(stmt)
        try:
            rows = con.execute(stmt).fetchall()
            print(f"  [{label}] statement {i+1}: {len(rows)} rows")
            total_rows += len(rows)
        except Exception as e:
            print(f"  [{label}] statement {i+1} ERROR: {e}")

    con.close()
    return total_rows


# ============================================================================
# Q065_multi_i1: sc.revenue <= 0.1 * sb.ave
# Need items at same store, one with high revenue (anchor), others with low.
# Constraint: ss_sales_price / ss_list_price BETWEEN 0.38 AND 0.48
# d_month_seq between 1195 and 1206, i_manager_id BETWEEN 80 AND 84
# s_state in ('IA','IL','NC')
# ============================================================================

q065_i1 = {
    "tables": {
        "date_dim": {
            "columns": ["d_date_sk", "d_month_seq"],
            "rows": [[1000, 1200]]
        },
        "store": {
            "columns": ["s_store_sk", "s_store_name", "s_state"],
            "rows": [[1000, "Store1", "IA"]]
        },
        "item": {
            "columns": ["i_item_sk", "i_item_desc", "i_current_price", "i_wholesale_cost", "i_brand", "i_manager_id"],
            "rows": [
                [1000, "Anchor item high rev", 10.00, 5.00, "BrandA", 82],
                [1001, "Low rev item 1", 10.00, 5.00, "BrandB", 80],
                [1002, "Low rev item 2", 10.00, 5.00, "BrandC", 83],
            ]
        },
        "store_sales": {
            "columns": ["ss_sold_date_sk", "ss_store_sk", "ss_item_sk", "ss_sales_price", "ss_list_price"],
            "rows": [
                # Anchor item: 25 rows at 40.00 each -> revenue = 1000
                # ratio = 40/100 = 0.40 (within 0.38-0.48)
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                [1000, 1000, 1000, 40.00, 100.00],
                # Low-rev item 1: 1 row, revenue = 4.00
                # ratio = 4/10 = 0.40
                [1000, 1000, 1001, 4.00, 10.00],
                # Low-rev item 2: 1 row, revenue = 4.00
                [1000, 1000, 1002, 4.00, 10.00],
            ]
        }
    }
}
# sb.ave = avg(1000, 4, 4) = 336.0
# 0.1 * 336 = 33.6
# item 1001 rev=4 <= 33.6 YES
# item 1002 rev=4 <= 33.6 YES -> 2 result rows

# ============================================================================
# Q065_multi_i2: Same structure, different filters
# ss_sales_price / ss_list_price BETWEEN 0.79 AND 0.89
# d_month_seq between 1215 and 1226, i_manager_id BETWEEN 10 AND 14
# s_state in ('KS','OH','SD')
# ============================================================================

q065_i2 = {
    "tables": {
        "date_dim": {
            "columns": ["d_date_sk", "d_month_seq"],
            "rows": [[2000, 1220]]
        },
        "store": {
            "columns": ["s_store_sk", "s_store_name", "s_state"],
            "rows": [[2000, "StoreOH", "OH"]]
        },
        "item": {
            "columns": ["i_item_sk", "i_item_desc", "i_current_price", "i_wholesale_cost", "i_brand", "i_manager_id"],
            "rows": [
                [2000, "Anchor hi", 50.00, 25.00, "BrandX", 12],
                [2001, "Lowrev 1", 50.00, 25.00, "BrandY", 10],
                [2002, "Lowrev 2", 50.00, 25.00, "BrandZ", 14],
            ]
        },
        "store_sales": {
            "columns": ["ss_sold_date_sk", "ss_store_sk", "ss_item_sk", "ss_sales_price", "ss_list_price"],
            "rows": [
                # Anchor: 25 rows, price=80, list=100 -> ratio=0.80 -> revenue=2000
                *[[2000, 2000, 2000, 80.00, 100.00]] * 25,
                # Low 1: 1 row, price=8.00, list=10.00 -> ratio=0.80 -> revenue=8
                [2000, 2000, 2001, 8.00, 10.00],
                # Low 2: 1 row
                [2000, 2000, 2002, 8.00, 10.00],
            ]
        }
    }
}
# sb.ave = avg(2000, 8, 8) = 672.0
# 0.1 * 672 = 67.2
# items 2001,2002 rev=8 <= 67.2 YES


# ============================================================================
# Q039_multi_i2: UNSATISFIABLE
# stddev_samp(inv_quantity_on_hand)/avg(inv_quantity_on_hand) > 1
# BUT inv_quantity_on_hand BETWEEN 791 AND 991
# Max stddev_samp with 2 values (791,991) = 141.42, mean=891
# cov = 141.42/891 = 0.159 — NEVER > 1
# ============================================================================
# Let's verify this is truly impossible:

def check_q039_satisfiability():
    """Prove Q039_i2 cov>1 constraint is impossible given the range filter."""
    import math
    # Best case: half values at 791, half at 991 (maximizes variance)
    vals = [791, 991]
    mean = sum(vals) / len(vals)  # 891
    var = sum((x - mean)**2 for x in vals) / (len(vals) - 1)  # sample variance
    std = math.sqrt(var)  # 141.42
    cov = std / mean  # 0.159
    print(f"\nQ039 satisfiability check:")
    print(f"  Best case vals={vals}, mean={mean}, std={std:.2f}, cov={cov:.4f}")
    print(f"  cov > 1? {cov > 1} -> UNSATISFIABLE (range 791-991 is too narrow)")
    return False


# ============================================================================
# Q064_multi_i2: UNSATISFIABLE
# cd1.cd_marital_status IN ('S','S','S') AND cd2.cd_marital_status IN ('S','S','S')
# AND cd1.cd_marital_status <> cd2.cd_marital_status
# 'S' <> 'S' is always FALSE
# ============================================================================

def check_q064_satisfiability():
    print(f"\nQ064_i2 satisfiability check:")
    print(f"  cd1.cd_marital_status IN ('S','S','S') -> must be 'S'")
    print(f"  cd2.cd_marital_status IN ('S','S','S') -> must be 'S'")
    print(f"  cd1.cd_marital_status <> cd2.cd_marital_status -> 'S' <> 'S' -> FALSE")
    print(f"  -> UNSATISFIABLE (contradictory predicates)")
    return False


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    out_dir = Path(__file__).parent / "witness_runs" / "20260219_142815"

    print("=" * 60)
    print("Manual witness generation for 4 failed queries")
    print("=" * 60)

    # Test Q065_i1
    print("\n--- query065_multi_i1 ---")
    rows = test_witness("query065_multi_i1.sql", q065_i1, "Q065_i1")
    if rows > 0:
        print(f"  PASS: {rows} rows")
        qdir = out_dir / "query065_multi_i1"
        qdir.mkdir(parents=True, exist_ok=True)
        (qdir / "manual_witness.json").write_text(json.dumps(q065_i1, indent=2))
        (qdir / "result.json").write_text(json.dumps({
            "query": "query065_multi_i1", "success": True, "row_count": rows,
            "attempts": 0, "inserts_ok": sum(len(t["rows"]) for t in q065_i1["tables"].values()),
            "inserts_failed": 0, "error": None, "note": "manual witness"
        }, indent=2))
    else:
        print("  FAIL — adjusting...")

    # Test Q065_i2
    print("\n--- query065_multi_i2 ---")
    rows = test_witness("query065_multi_i2.sql", q065_i2, "Q065_i2")
    if rows > 0:
        print(f"  PASS: {rows} rows")
        qdir = out_dir / "query065_multi_i2"
        qdir.mkdir(parents=True, exist_ok=True)
        (qdir / "manual_witness.json").write_text(json.dumps(q065_i2, indent=2))
        (qdir / "result.json").write_text(json.dumps({
            "query": "query065_multi_i2", "success": True, "row_count": rows,
            "attempts": 0, "inserts_ok": sum(len(t["rows"]) for t in q065_i2["tables"].values()),
            "inserts_failed": 0, "error": None, "note": "manual witness"
        }, indent=2))
    else:
        print("  FAIL — adjusting...")

    # Check unsatisfiable queries
    q039_sat = check_q039_satisfiability()
    q064_sat = check_q064_satisfiability()

    # Mark unsatisfiable queries
    for qname, sat in [("query039_multi_i2", q039_sat), ("query064_multi_i2", q064_sat)]:
        if not sat:
            qdir = out_dir / qname
            qdir.mkdir(parents=True, exist_ok=True)
            (qdir / "result.json").write_text(json.dumps({
                "query": qname, "success": False, "row_count": 0,
                "attempts": 0, "inserts_ok": 0, "inserts_failed": 0,
                "error": None, "note": "UNSATISFIABLE - contradictory predicates"
            }, indent=2))

    # Update the main results.json
    results_file = out_dir / "results.json"
    if results_file.exists():
        results = json.loads(results_file.read_text())
        # Update Q065 entries
        for r in results:
            if r["query"] == "query065_multi_i1" and not r["success"]:
                r2 = json.loads((out_dir / "query065_multi_i1" / "result.json").read_text())
                r.update(r2)
            elif r["query"] == "query065_multi_i2" and not r["success"]:
                r2 = json.loads((out_dir / "query065_multi_i2" / "result.json").read_text())
                r.update(r2)
            elif r["query"] in ("query039_multi_i2", "query064_multi_i2"):
                r["note"] = "UNSATISFIABLE"
        results_file.write_text(json.dumps(results, indent=2))

        passes = sum(1 for r in results if r["success"])
        unsat = sum(1 for r in results if r.get("note") == "UNSATISFIABLE")
        total = len(results)
        print(f"\n{'='*60}")
        print(f"FINAL: {passes}/{total} PASS, {unsat} UNSATISFIABLE, "
              f"{total - passes - unsat} FAIL")
        print(f"{'='*60}")

"""One-time clustering setup for Snowflake TPC-DS tables.

WARNING: Clustering costs credits (background reclustering process).
Run once, then wait for clustering to complete before benchmarking.

Usage:
    # Check clustering status (no changes)
    python research/snowflake/setup_clustering.py --check

    # Apply clustering (costs credits!)
    python research/snowflake/setup_clustering.py --apply

    # Setup QAS warehouse
    python research/snowflake/setup_clustering.py --setup-warehouse
"""

import argparse
import json
import os
import sys
from urllib.parse import urlparse, parse_qs, unquote

import snowflake.connector

DSN = os.environ.get(
    "QT_SNOWFLAKE_DSN",
    "snowflake://jkdl:QGdg%2A%24WC%25O62xS71@CVRYJTF-AW47074"
    "/SNOWFLAKE_SAMPLE_DATA/TPCDS_SF10TCL?warehouse=COMPUTE_WH&role=ACCOUNTADMIN",
)

# Clustering targets: table -> (date_col, secondary_col)
CLUSTER_TARGETS = {
    "STORE_SALES":   ("SS_SOLD_DATE_SK", "SS_ITEM_SK"),
    "WEB_SALES":     ("WS_SOLD_DATE_SK", "WS_ITEM_SK"),
    "CATALOG_SALES": ("CS_SOLD_DATE_SK", "CS_ITEM_SK"),
    "INVENTORY":     ("INV_DATE_SK",     "INV_ITEM_SK"),
}


def parse_dsn(dsn: str) -> dict:
    parsed = urlparse(dsn)
    params = parse_qs(parsed.query)
    path_parts = parsed.path.strip("/").split("/")
    return {
        "account": parsed.hostname,
        "user": parsed.username,
        "password": unquote(parsed.password),
        "database": path_parts[0] if len(path_parts) > 0 else "",
        "schema": path_parts[1] if len(path_parts) > 1 else "PUBLIC",
        "warehouse": params.get("warehouse", ["COMPUTE_WH"])[0],
        "role": params.get("role", [""])[0],
    }


def get_connection():
    p = parse_dsn(DSN)
    return snowflake.connector.connect(
        account=p["account"], user=p["user"], password=p["password"],
        database=p["database"], schema=p["schema"],
        warehouse=p["warehouse"], role=p["role"],
    )


def check_clustering(conn):
    """Check current clustering status for all target tables."""
    cur = conn.cursor()
    print("\n=== CLUSTERING STATUS ===\n")

    for table, (date_col, item_col) in CLUSTER_TARGETS.items():
        cluster_cols = f"({date_col}, {item_col})"
        print(f"--- {table} {cluster_cols} ---")
        try:
            cur.execute(f"SELECT SYSTEM$CLUSTERING_INFORMATION('{table}', '{cluster_cols}')")
            info = json.loads(cur.fetchone()[0])
            depth = info.get("average_overlap_depth", "N/A")
            total = info.get("total_partition_count", "N/A")
            const = info.get("total_constant_partition_count", "N/A")
            ratio = info.get("average_depth", "N/A")

            # Check if clustering is good enough (depth < 5 is our target)
            status = "GOOD" if isinstance(depth, (int, float)) and depth < 5 else "NEEDS WORK"

            print(f"  overlap_depth: {depth}  (target: < 5) [{status}]")
            print(f"  total_partitions: {total}")
            print(f"  constant_partitions: {const}")
            print(f"  average_depth: {ratio}")
            print()
        except Exception as e:
            print(f"  ERROR: {e}\n")

    cur.close()


def apply_clustering(conn):
    """Apply clustering to fact tables. COSTS CREDITS."""
    cur = conn.cursor()
    print("\n=== APPLYING CLUSTERING (costs credits!) ===\n")

    for table, (date_col, item_col) in CLUSTER_TARGETS.items():
        cluster_expr = f"({date_col}, {item_col})"
        print(f"  ALTER TABLE {table} CLUSTER BY {cluster_expr}")
        try:
            cur.execute(f"ALTER TABLE {table} CLUSTER BY {cluster_expr}")
            print(f"    -> OK (background reclustering started)")
        except Exception as e:
            print(f"    -> FAILED: {e}")
        print()

    print("Clustering started. Use --check to monitor progress.")
    print("Wait for average_overlap_depth < 5 before benchmarking.")
    cur.close()


def setup_warehouse(conn, name: str = "COMPUTE_WH"):
    """Configure warehouse with QAS enabled."""
    cur = conn.cursor()
    print(f"\n=== WAREHOUSE SETUP: {name} ===\n")

    commands = [
        f"ALTER WAREHOUSE {name} SET WAREHOUSE_SIZE = 'XSMALL'",
        f"ALTER WAREHOUSE {name} SET ENABLE_QUERY_ACCELERATION = TRUE "
        f"QUERY_ACCELERATION_MAX_SCALE_FACTOR = 8",
        f"ALTER WAREHOUSE {name} SET AUTO_SUSPEND = 60",
    ]
    for cmd in commands:
        print(f"  {cmd}")
        try:
            cur.execute(cmd)
            print(f"    -> OK")
        except Exception as e:
            print(f"    -> FAILED: {e}")

    cur.close()


def main():
    parser = argparse.ArgumentParser(description="Snowflake clustering setup")
    parser.add_argument("--check", action="store_true", help="Check clustering status")
    parser.add_argument("--apply", action="store_true", help="Apply clustering (costs credits!)")
    parser.add_argument("--setup-warehouse", action="store_true", help="Setup QAS warehouse")

    args = parser.parse_args()
    if not (args.check or args.apply or args.setup_warehouse):
        parser.print_help()
        sys.exit(1)

    conn = get_connection()
    try:
        if args.check:
            check_clustering(conn)
        if args.apply:
            confirm = input("This will start background reclustering (costs credits). Continue? [y/N] ")
            if confirm.lower() == "y":
                apply_clustering(conn)
            else:
                print("Aborted.")
        if args.setup_warehouse:
            setup_warehouse(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

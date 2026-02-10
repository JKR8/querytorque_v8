#!/usr/bin/env python3
"""Generate TPC-DS database using DuckDB's built-in extension.

Usage:
    python generate_tpcds_duckdb.py --sf 5 --output /mnt/d/TPC-DS/tpcds_sf5.duckdb
"""

import argparse
import duckdb
from pathlib import Path
import time


def generate_tpcds(output_path: str, scale_factor: int = 5) -> None:
    """Generate TPC-DS data at specified scale factor."""

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    # Remove existing file if present
    if output.exists():
        print(f"Removing existing database: {output}")
        output.unlink()

    print(f"Creating TPC-DS SF{scale_factor} database at: {output}")
    print(f"Expected size: ~{scale_factor}GB")
    print()

    # Connect to new database file
    conn = duckdb.connect(str(output))

    # Install and load TPC-DS extension
    print("Installing TPC-DS extension...")
    conn.execute("INSTALL tpcds")
    conn.execute("LOAD tpcds")

    # Generate data
    print(f"Generating TPC-DS SF{scale_factor} data (this may take a while)...")
    start = time.time()
    conn.execute(f"CALL dsdgen(sf={scale_factor})")
    elapsed = time.time() - start
    print(f"Data generation complete in {elapsed:.1f}s")

    # Verify tables
    print("\nVerifying tables...")
    tables = conn.execute("""
        SELECT table_name,
               (SELECT COUNT(*) FROM pragma_table_info(table_name)) as columns
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
    """).fetchall()

    print(f"\nCreated {len(tables)} tables:")
    for table, cols in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count:,} rows ({cols} columns)")

    # Get file size
    conn.close()
    size_mb = output.stat().st_size / (1024 * 1024)
    print(f"\nDatabase file size: {size_mb:.1f} MB")
    print(f"Output: {output}")


def main():
    parser = argparse.ArgumentParser(description="Generate TPC-DS DuckDB database")
    parser.add_argument(
        "--sf",
        type=int,
        default=5,
        help="Scale factor (default: 5, meaning ~5GB)"
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default="/mnt/d/TPC-DS/tpcds_sf5.duckdb",
        help="Output database path"
    )

    args = parser.parse_args()
    generate_tpcds(args.output, args.sf)


if __name__ == "__main__":
    main()

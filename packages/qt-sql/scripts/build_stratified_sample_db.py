#!/usr/bin/env python3
"""Build a stratified 1% sample DuckDB database.

Strategy (query-agnostic):
- Copy all dimension tables fully.
- For large fact tables, stratify by date key (*_sold_date_sk or *_date_sk).
- Ensure minimum rows per date key to preserve coverage.
"""

from __future__ import annotations

import argparse
import pathlib

import duckdb


def build_sample(
    *,
    full_db: str,
    out_db: str,
    sample_pct: float,
    min_rows_per_stratum: int,
    fact_row_threshold: int,
    tables_allowlist: list[str] | None,
    mode: str,
) -> list[str]:
    out_path = pathlib.Path(out_db)
    if out_path.exists():
        out_path.unlink()

    con = duckdb.connect(out_db)
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=1")
    con.execute(f"ATTACH '{full_db}' AS full_db")

    tables = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog = 'full_db' AND table_schema = 'main' AND table_type = 'BASE TABLE'"
    ).fetchall()
    tables = [t[0] for t in tables]
    if tables_allowlist:
        allow = set(tables_allowlist)
        tables = [t for t in tables if t in allow]

    cols = con.execute(
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_catalog = 'full_db' AND table_schema = 'main'"
    ).fetchall()

    cols_by_table: dict[str, list[str]] = {}
    for table, col in cols:
        cols_by_table.setdefault(table, []).append(col)

    def find_date_key(table: str) -> str | None:
        candidates = [c for c in cols_by_table.get(table, []) if c.endswith("_sold_date_sk")]
        if candidates:
            return candidates[0]
        candidates = [c for c in cols_by_table.get(table, []) if c.endswith("_date_sk")]
        if candidates:
            return candidates[0]
        return None

    rowcounts: dict[str, int] = {}
    for t in tables:
        rowcounts[t] = con.execute(f"SELECT COUNT(*) FROM full_db.{t}").fetchone()[0]

    fact_tables: list[str] = []
    for t in tables:
        if rowcounts[t] > fact_row_threshold and find_date_key(t):
            fact_tables.append(t)

    fact_set = set(fact_tables)

    # Copy dimensions fully
    for t in tables:
        if t in fact_set:
            continue
        con.execute(f"CREATE TABLE {t} AS SELECT * FROM full_db.{t}")

    # Precompute sampled date keys for date-key mode
    sampled_date_keys: dict[str, list[int]] = {}
    if mode == "date-key":
        # Sample date_dim by year to preserve temporal coverage
        con.execute("CREATE TEMP TABLE sampled_dates AS\n"
                    "SELECT d_date_sk FROM (\n"
                    "  SELECT d_date_sk,\n"
                    "         ROW_NUMBER() OVER (PARTITION BY d_year ORDER BY random()) AS rn,\n"
                    "         COUNT(*) OVER (PARTITION BY d_year) AS cnt\n"
                    "  FROM full_db.date_dim\n"
                    ") WHERE rn <= GREATEST(CAST(CEIL(cnt * %s) AS BIGINT), %s)" % (sample_pct, min_rows_per_stratum))
        sampled_dates = [row[0] for row in con.execute("SELECT d_date_sk FROM sampled_dates").fetchall()]
        sampled_date_keys["d_date_sk"] = sampled_dates

    # Stratified sampling for fact tables
    for t in fact_tables:
        date_key = find_date_key(t)
        # Create empty table with same schema
        con.execute(f"CREATE TABLE {t} AS SELECT * FROM full_db.{t} LIMIT 0")

        column_list = ", ".join(cols_by_table.get(t, []))

        if mode == "date-key":
            # Use sampled date_dim keys (fast, query-agnostic)
            query = f"""
            INSERT INTO {t}
            SELECT {column_list}
            FROM full_db.{t}
            WHERE {date_key} IN (SELECT d_date_sk FROM sampled_dates)
            """
            con.execute(query)
        else:
            # Full stratified sampling (slower, per-date key)
            date_keys = [
                row[0]
                for row in con.execute(
                    f"SELECT DISTINCT {date_key} FROM full_db.{t} WHERE {date_key} IS NOT NULL"
                ).fetchall()
            ]
            chunk_size = 200

            for i in range(0, len(date_keys), chunk_size):
                chunk = date_keys[i : i + chunk_size]
                in_list = ",".join(str(v) for v in chunk)
                query = f"""
                INSERT INTO {t}
                SELECT {column_list} FROM (
                    SELECT
                        {column_list},
                        ROW_NUMBER() OVER (PARTITION BY {date_key} ORDER BY random()) AS rn,
                        COUNT(*) OVER (PARTITION BY {date_key}) AS cnt
                    FROM full_db.{t}
                    WHERE {date_key} IN ({in_list})
                )
                WHERE rn <= GREATEST(CAST(CEIL(cnt * {sample_pct}) AS BIGINT), {min_rows_per_stratum})
                """
                con.execute(query)

    con.execute("CHECKPOINT")
    con.close()

    return fact_tables


def main() -> None:
    parser = argparse.ArgumentParser(description="Build stratified 1% sample DuckDB database")
    parser.add_argument("--full-db", required=True, help="Path to full DuckDB database")
    parser.add_argument("--out-db", required=True, help="Output path for sampled DuckDB database")
    parser.add_argument("--sample-pct", type=float, default=0.01, help="Sampling percentage per stratum")
    parser.add_argument("--min-rows", type=int, default=10, help="Minimum rows per stratum")
    parser.add_argument("--fact-threshold", type=int, default=1_000_000, help="Rowcount threshold for fact tables")
    parser.add_argument(
        "--tables",
        type=str,
        default="",
        help="Optional comma-separated list of tables to include (others skipped)",
    )
    parser.add_argument(
        "--mode",
        type=str,
        default="date-key",
        choices=["date-key", "stratified"],
        help="Sampling mode: date-key (fast) or stratified (slow, per-date key).",
    )
    args = parser.parse_args()

    tables_allowlist = [t.strip() for t in args.tables.split(",") if t.strip()] or None
    fact_tables = build_sample(
        full_db=args.full_db,
        out_db=args.out_db,
        sample_pct=args.sample_pct,
        min_rows_per_stratum=args.min_rows,
        fact_row_threshold=args.fact_threshold,
        tables_allowlist=tables_allowlist,
        mode=args.mode,
    )

    print("Wrote stratified sample:", args.out_db)
    print("Fact tables:", fact_tables)


if __name__ == "__main__":
    main()

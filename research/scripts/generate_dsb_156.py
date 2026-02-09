#!/usr/bin/env python3
"""Generate 156 DSB queries (52 templates × 3 parameter streams) for paper comparison.

Uses dsqgen with 3 different RNG seeds to produce different parameter instantiations
(counties, dates, categories, etc.) from the same SQL templates.

Usage:
    python3 research/scripts/generate_dsb_156.py [--dry-run]
"""

import argparse
import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DSQGEN = Path("/mnt/d/dsb/code/tools/dsqgen")
TPL_ROOT = Path("/mnt/d/dsb/query_templates_pg")
TPCDS_IDX = Path("/mnt/d/dsb/code/tools/tpcds.idx")

OUTPUT_DIR = Path(
    "/mnt/c/Users/jakc9/Documents/QueryTorque_V8"
    "/packages/qt-sql/ado/benchmarks/postgres_dsb_156/queries"
)

# 3 seeds for 3 parameter streams (arbitrary, documented for reproducibility)
SEEDS = [1001, 1002, 1003]

# Template directories and their category suffixes
CATEGORIES = [
    ("multi_block_queries", "multi"),
    ("agg_queries", "agg"),
    ("spj_queries", "spj_spj"),
]


def discover_templates() -> list[tuple[str, str, str]]:
    """Return list of (template_dir, template_file, category_suffix) tuples."""
    templates = []
    for subdir, suffix in CATEGORIES:
        tpl_dir = TPL_ROOT / subdir
        for tpl in sorted(tpl_dir.glob("query*.tpl")):
            # SPJ templates already have _spj in the name (e.g., query013_spj.tpl)
            # We want the output to match our naming: query013_spj_spj
            templates.append((str(tpl_dir), tpl.name, suffix))
    return templates


def generate_query(
    tpl_dir: str, tpl_name: str, seed: int
) -> str | None:
    """Run dsqgen and return the SQL string (or None on failure)."""
    cmd = [
        str(DSQGEN),
        "-DIRECTORY", tpl_dir,
        "-TEMPLATE", tpl_name,
        "-DIALECT", "postgres",
        "-SCALE", "10",
        "-RNGSEED", str(seed),
        "-FILTER", "Y",
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            cwd=str(DSQGEN.parent),
        )
    except subprocess.TimeoutExpired:
        print(f"  TIMEOUT: {tpl_name} seed={seed}", file=sys.stderr)
        return None

    if result.returncode != 0:
        print(
            f"  ERROR: {tpl_name} seed={seed}: {result.stderr.strip()}",
            file=sys.stderr,
        )
        return None

    # dsqgen outputs "name DIRECTORY, param ..." lines to stdout — filter them
    lines = result.stdout.splitlines()
    sql_lines = [l for l in lines if not l.startswith("name ")]
    sql = "\n".join(sql_lines).strip()
    if not sql:
        print(f"  EMPTY: {tpl_name} seed={seed}", file=sys.stderr)
        return None
    return sql


def query_id_from_template(tpl_name: str, suffix: str) -> str:
    """Convert template name to query ID matching our convention.

    Examples:
        query010.tpl + multi  -> query010_multi
        query013_spj.tpl + spj_spj -> query013_spj_spj
    """
    stem = tpl_name.replace(".tpl", "")
    # For SPJ templates, stem already has _spj (e.g., query013_spj)
    if suffix == "spj_spj" and stem.endswith("_spj"):
        return f"{stem}_spj"  # query013_spj -> query013_spj_spj
    return f"{stem}_{suffix}"


def main():
    parser = argparse.ArgumentParser(description="Generate 156 DSB queries")
    parser.add_argument("--dry-run", action="store_true", help="List what would be generated")
    args = parser.parse_args()

    templates = discover_templates()
    print(f"Discovered {len(templates)} templates across {len(CATEGORIES)} categories")

    if len(templates) != 52:
        print(f"WARNING: Expected 52 templates, found {len(templates)}", file=sys.stderr)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    total = 0
    errors = 0
    skipped = 0

    for stream_idx, seed in enumerate(SEEDS, start=1):
        print(f"\n--- Stream {stream_idx} (seed={seed}) ---")
        for tpl_dir, tpl_name, suffix in templates:
            qid = query_id_from_template(tpl_name, suffix)
            out_name = f"{qid}_s{stream_idx}.sql"
            out_path = OUTPUT_DIR / out_name

            if args.dry_run:
                print(f"  Would generate: {out_name}")
                total += 1
                continue

            if out_path.exists():
                skipped += 1
                continue

            sql = generate_query(tpl_dir, tpl_name, seed)
            if sql is None:
                errors += 1
                continue

            out_path.write_text(sql + "\n")
            total += 1

    print(f"\nDone: {total} generated, {skipped} skipped (already exist), {errors} errors")
    print(f"Output: {OUTPUT_DIR}")

    # Verify
    actual = len(list(OUTPUT_DIR.glob("query*_s*.sql")))
    print(f"Total files in output: {actual}")
    if actual == 156:
        print("SUCCESS: 156 queries generated")
    else:
        print(f"WARNING: Expected 156, got {actual}")


if __name__ == "__main__":
    main()

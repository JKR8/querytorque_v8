#!/usr/bin/env python3
"""Generate 76 DSB queries matching R-Bot's evaluation configuration.

R-Bot (VLDB 2025) evaluates on 76 DSB queries. DSB has 37 unique template
numbers. R-Bot's TPC-H setup uses 22 templates × 2 instances = 44 queries.
The same pattern for DSB: 37 multi+agg templates × 2 instances = 74,
plus 1 SPJ template × 2 = 76.

This script uses dsqgen to generate parameterized query instances from
the DSB template files, producing exactly 76 queries for each of 2 seeds.

Usage (from project root):
    python3 research/scripts/generate_dsb_76.py

Output:
    packages/qt-sql/ado/benchmarks/postgres_dsb_76/queries/*.sql
    packages/qt-sql/ado/benchmarks/postgres_dsb_76/config.json
"""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

# DSB installation paths
DSB_ROOT = Path("/mnt/d/dsb")
DSQGEN = DSB_ROOT / "code" / "tools" / "dsqgen"
TEMPLATE_ROOT = DSB_ROOT / "query_templates_pg"

# Output
PROJECT_ROOT = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
OUTPUT_DIR = PROJECT_ROOT / "packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76"
QUERIES_DIR = OUTPUT_DIR / "queries"

# Generation config
SCALE_FACTOR = 10
SEED_A = 1001  # First instance seed
SEED_B = 1002  # Second instance seed
DIALECT = "postgres"

# Template categories
MULTI_TEMPLATES = [
    "query001", "query010", "query014", "query023", "query030", "query031",
    "query032", "query038", "query039", "query054", "query058", "query059",
    "query064", "query065", "query069", "query075", "query080", "query081",
    "query083", "query087", "query092", "query094",
]  # 22 templates

AGG_TEMPLATES = [
    "query013", "query018", "query019", "query025", "query027", "query040",
    "query050", "query072", "query084", "query085", "query091", "query099",
    "query100", "query101", "query102",
]  # 15 templates

# 1 SPJ template to reach 76 (query013_spj is the most complex SPJ)
SPJ_TEMPLATES = [
    "query013_spj",
]  # 1 template

# Total: (22 + 15 + 1) × 2 instances = 76 queries


def generate_query(template_name: str, template_dir: Path, seed: int,
                   tmp_dir: Path) -> str:
    """Run dsqgen for a single template with a specific seed.

    Returns the generated SQL text.
    """
    cmd = [
        str(DSQGEN),
        "-DIRECTORY", str(template_dir),
        "-TEMPLATE", f"{template_name}.tpl",
        "-DIALECT", DIALECT,
        "-SCALE", str(SCALE_FACTOR),
        "-RNGSEED", str(seed),
        "-OUTPUT_DIR", str(tmp_dir),
        "-STREAMS", "1",
        "-QUIET", "Y",
    ]
    subprocess.run(cmd, cwd=str(DSQGEN.parent), capture_output=True, timeout=30)

    # dsqgen outputs query_0.sql
    out_file = tmp_dir / "query_0.sql"
    if out_file.exists():
        sql = out_file.read_text().strip()
        out_file.unlink()
        return sql
    return ""


def main():
    if not DSQGEN.exists():
        print(f"ERROR: dsqgen not found at {DSQGEN}")
        sys.exit(1)

    QUERIES_DIR.mkdir(parents=True, exist_ok=True)
    tmp_dir = OUTPUT_DIR / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # Template dirs
    multi_dir = TEMPLATE_ROOT / "multi_block_queries"
    agg_dir = TEMPLATE_ROOT / "agg_queries"
    spj_dir = TEMPLATE_ROOT / "spj_queries"

    generated = []
    errors = []

    print(f"Generating 76 DSB queries (PG dialect, SF{SCALE_FACTOR})")
    print(f"  Seeds: {SEED_A}, {SEED_B}")
    print(f"  Output: {QUERIES_DIR}")
    print()

    # Generate 2 instances per template
    templates = (
        [(t, multi_dir, "multi") for t in MULTI_TEMPLATES] +
        [(t, agg_dir, "agg") for t in AGG_TEMPLATES] +
        [(t, spj_dir, "spj") for t in SPJ_TEMPLATES]
    )

    for template_name, template_dir, category in templates:
        for i, seed in enumerate([SEED_A, SEED_B], 1):
            # Naming: query013_agg_i1.sql, query013_agg_i2.sql
            suffix = category if category != "multi" else "multi"
            if category == "spj":
                # SPJ template names already have _spj suffix
                base_name = template_name.replace("_spj", "")
                out_name = f"{base_name}_spj_i{i}"
            else:
                out_name = f"{template_name}_{suffix}_i{i}"

            sql = generate_query(template_name, template_dir, seed, tmp_dir)
            if not sql:
                print(f"  ERROR: {out_name} — dsqgen produced no output")
                errors.append(out_name)
                continue

            out_path = QUERIES_DIR / f"{out_name}.sql"
            out_path.write_text(sql + "\n")
            generated.append(out_name)
            print(f"  OK: {out_name} ({len(sql)} chars)")

    # Clean up
    shutil.rmtree(tmp_dir, ignore_errors=True)

    # Summary
    print(f"\nGenerated: {len(generated)}/76 queries")
    if errors:
        print(f"Errors: {len(errors)} — {errors}")

    # Also copy existing queries from postgres_dsb for comparison
    # (single-instance versions for baseline timing reference)
    existing_dir = PROJECT_ROOT / "packages/qt-sql/qt_sql/benchmarks/postgres_dsb/queries"
    baseline_count = 0
    baseline_dir = OUTPUT_DIR / "baseline_queries"
    baseline_dir.mkdir(parents=True, exist_ok=True)
    for sql_file in sorted(existing_dir.glob("*.sql")):
        shutil.copy2(sql_file, baseline_dir / sql_file.name)
        baseline_count += 1
    print(f"Copied {baseline_count} baseline queries to {baseline_dir}")

    # Generate config.json
    config = {
        "engine": "postgresql",
        "benchmark": "dsb",
        "dsn": "postgres://jakc9:jakc9@127.0.0.1:5434/dsb_sf10",
        "benchmark_dsn": "postgres://jakc9:jakc9@127.0.0.1:5434/dsb_sf10",
        "scale_factor": SCALE_FACTOR,
        "timeout_seconds": 300,
        "validation_method": "5x-trimmed-mean",
        "n_queries": len(generated),
        "workers_state_0": 4,
        "workers_state_n": 1,
        "promote_threshold": 1.05,
        "generation": {
            "dsqgen_version": "2.11.0",
            "seeds": [SEED_A, SEED_B],
            "instances_per_template": 2,
            "multi_templates": len(MULTI_TEMPLATES),
            "agg_templates": len(AGG_TEMPLATES),
            "spj_templates": len(SPJ_TEMPLATES),
            "total_templates": len(MULTI_TEMPLATES) + len(AGG_TEMPLATES) + len(SPJ_TEMPLATES),
            "rbot_comparison_note": (
                "R-Bot (VLDB 2025) evaluates on 76 DSB queries. "
                "TPC-H pattern: 22 templates × 2 instances = 44. "
                "DSB: 38 templates × 2 instances = 76. "
                "We generate (22 multi + 15 agg + 1 spj) × 2 seeds = 76."
            ),
        },
    }
    config_path = OUTPUT_DIR / "config.json"
    config_path.write_text(json.dumps(config, indent=2))
    print(f"Config saved: {config_path}")

    # Generate manifest
    manifest = {
        "total": len(generated),
        "target": 76,
        "templates": {
            "multi_block": MULTI_TEMPLATES,
            "agg": AGG_TEMPLATES,
            "spj": SPJ_TEMPLATES,
        },
        "queries": generated,
    }
    manifest_path = OUTPUT_DIR / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"Manifest saved: {manifest_path}")


if __name__ == "__main__":
    main()

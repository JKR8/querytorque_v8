#!/usr/bin/env python3
"""Snapshot the prompt chain (knowledge, constraints, examples) for a benchmark run.

Usage:
    python snapshot_prompts.py <dialect> <run_name>

Examples:
    python snapshot_prompts.py duckdb 20260209_duckdb_tpcds_v3_swarm
    python snapshot_prompts.py postgresql 20260212_pg_dsb_v2_combined
"""
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent.parent  # QueryTorque_V8/
QT_SQL = ROOT / "packages" / "qt-sql" / "qt_sql"
PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"

# Dialect → example subdirectory name
EXAMPLE_DIRS = {
    "duckdb": "duckdb",
    "postgresql": "postgres",
}


def get_git_hash() -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"],
            text=True,
        ).strip()
    except Exception:
        return "unknown"


def snapshot(dialect: str, run_name: str) -> None:
    dest = PROMPTS_DIR / run_name
    if dest.exists():
        print(f"  WARNING: {dest} already exists, overwriting...")
        shutil.rmtree(dest)
    dest.mkdir(parents=True)

    # 1. Knowledge file
    knowledge_src = QT_SQL / "knowledge" / f"{dialect}.md"
    if knowledge_src.exists():
        kd = dest / "knowledge"
        kd.mkdir()
        shutil.copy2(knowledge_src, kd / knowledge_src.name)
        print(f"  Copied knowledge/{knowledge_src.name}")

    # 2. Constraints (engine profile + all constraint JSONs)
    constraints_src = QT_SQL / "constraints"
    if constraints_src.exists():
        cd = dest / "constraints"
        cd.mkdir()
        for f in sorted(constraints_src.glob("*.json")):
            # Copy engine profile for this dialect + all shared constraints
            is_engine_profile = f.name.startswith("engine_profile_")
            is_this_dialect = dialect in f.name
            is_shared = not f.name.startswith("engine_profile_")
            if (is_engine_profile and is_this_dialect) or is_shared:
                shutil.copy2(f, cd / f.name)
        copied = len(list(cd.glob("*.json")))
        print(f"  Copied {copied} constraint files")

    # 3. Examples
    example_subdir = EXAMPLE_DIRS.get(dialect, dialect)
    examples_src = QT_SQL / "examples" / example_subdir
    if examples_src.exists():
        ed = dest / "examples" / example_subdir
        ed.mkdir(parents=True)
        for f in sorted(examples_src.glob("*.json")):
            shutil.copy2(f, ed / f.name)
        copied = len(list(ed.glob("*.json")))
        print(f"  Copied {copied} example files")

    # 4. MANIFEST.md
    git_hash = get_git_hash()
    manifest = f"""# Prompt Snapshot: {run_name}

- **Dialect**: {dialect}
- **Git hash**: {git_hash}
- **Snapshot date**: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
- **Source**: `packages/qt-sql/qt_sql/`

## Contents

- `knowledge/{dialect}.md` — Exploit algorithm (pathology-based decision tree)
- `constraints/*.json` — Engine profile + correctness constraints
- `examples/{example_subdir}/*.json` — Gold examples with verified speedups
"""
    (dest / "MANIFEST.md").write_text(manifest)
    print(f"  Written MANIFEST.md")
    print(f"  Snapshot complete: {dest}")


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    dialect = sys.argv[1].lower()
    run_name = sys.argv[2]

    if dialect not in EXAMPLE_DIRS:
        print(f"Unknown dialect: {dialect}. Expected: {list(EXAMPLE_DIRS.keys())}")
        sys.exit(1)

    print(f"Snapshotting prompt chain for {dialect} → {run_name}")
    snapshot(dialect, run_name)


if __name__ == "__main__":
    main()

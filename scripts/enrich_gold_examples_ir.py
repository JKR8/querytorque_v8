"""Enrich gold examples with IR node maps.

For each gold example JSON that has original_sql and optimized_sql,
generate ir_node_map_before and ir_node_map_target fields using the
IR builder + renderer. Write back to the same file.

Usage:
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 scripts/enrich_gold_examples_ir.py
"""

import json
import sys
from pathlib import Path

# Add packages to path if needed
sys.path.insert(0, str(Path(__file__).parent.parent / "packages" / "qt-shared"))
sys.path.insert(0, str(Path(__file__).parent.parent / "packages" / "qt-sql"))

from qt_sql.ir import build_script_ir, render_ir_node_map, Dialect

EXAMPLES_ROOT = Path(__file__).parent.parent / "packages" / "qt-sql" / "qt_sql" / "examples"

DIALECT_MAP = {
    "duckdb": Dialect.DUCKDB,
    "postgres": Dialect.POSTGRES,
    "snowflake": Dialect.SNOWFLAKE,
}


def enrich_example(path: Path, dialect_enum: Dialect) -> bool:
    """Enrich a single gold example with IR node maps. Returns True if updated."""
    with open(path) as f:
        data = json.load(f)

    original_sql = data.get("original_sql", "")
    optimized_sql = data.get("optimized_sql", "")

    if not original_sql or not optimized_sql:
        print(f"  SKIP {path.name}: missing SQL")
        return False

    updated = False

    # Generate ir_node_map_before from original_sql
    try:
        ir_before = build_script_ir(original_sql, dialect_enum)
        data["ir_node_map_before"] = render_ir_node_map(ir_before)
        updated = True
    except Exception as e:
        print(f"  WARN {path.name}: failed to build IR for original_sql: {e}")

    # Generate ir_node_map_target from optimized_sql
    try:
        ir_target = build_script_ir(optimized_sql, dialect_enum)
        data["ir_node_map_target"] = render_ir_node_map(ir_target)
        updated = True
    except Exception as e:
        print(f"  WARN {path.name}: failed to build IR for optimized_sql: {e}")

    if updated:
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
            f.write("\n")
        print(f"  OK   {path.name}")

    return updated


def main():
    total = 0
    enriched = 0

    for dialect_dir in sorted(EXAMPLES_ROOT.iterdir()):
        if not dialect_dir.is_dir():
            continue

        dialect_name = dialect_dir.name
        dialect_enum = DIALECT_MAP.get(dialect_name)
        if dialect_enum is None:
            print(f"Skipping unknown dialect dir: {dialect_name}")
            continue

        print(f"\n=== {dialect_name.upper()} ===")

        for example_path in sorted(dialect_dir.glob("*.json")):
            total += 1
            if enrich_example(example_path, dialect_enum):
                enriched += 1

    print(f"\nDone: {enriched}/{total} examples enriched")


if __name__ == "__main__":
    main()

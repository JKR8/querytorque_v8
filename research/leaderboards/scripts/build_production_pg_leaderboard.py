#!/usr/bin/env python3
"""Build production PG DSB leaderboard files from combined results.

Reads:
  - research/config_tuning_results/combined_pg_dsb_leaderboard.json

Writes:
  - packages/qt-sql/qt_sql/benchmarks/postgres_dsb/leaderboard.json (production)
  - research/GOLD/GOLD_LEADERBOARD_PG_DSB.csv (GOLD CSV)
  - research/GOLD/pg_dsb/index.json (GOLD index)
  - Updates PG DSB section in research/GOLD/README.md
"""
import json
import csv
import io
import re
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent.parent
COMBINED = ROOT / "research" / "config_tuning_results" / "combined_pg_dsb_leaderboard.json"
PROD_LB = ROOT / "packages" / "qt-sql" / "qt_sql" / "benchmarks" / "postgres_dsb" / "leaderboard.json"
GOLD_CSV = ROOT / "research" / "GOLD" / "GOLD_LEADERBOARD_PG_DSB.csv"
GOLD_INDEX = ROOT / "research" / "GOLD" / "pg_dsb" / "index.json"
GOLD_README = ROOT / "research" / "GOLD" / "README.md"
# For original/optimized ms from the rewrite-only GOLD index
OLD_GOLD_INDEX = GOLD_INDEX


def load_combined():
    with open(COMBINED) as f:
        return json.load(f)


def load_old_gold_index():
    """Load existing GOLD index for original_ms/optimized_ms data."""
    try:
        with open(OLD_GOLD_INDEX) as f:
            data = json.load(f)
        return {q["query_id"]: q for q in data.get("queries", [])}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def classify_verdict(entry):
    """Classify combined verdict into standard buckets."""
    v = entry["verdict"]
    if v in ("WIN", "RECOVERED"):
        return "WIN" if entry["best_speedup"] and entry["best_speedup"] >= 1.10 else "IMPROVED"
    return v


def build_production_leaderboard(combined):
    """Build production leaderboard.json from combined data."""
    board = combined["board"]
    summary = combined["summary"]

    queries = []
    for entry in board:
        status = entry["verdict"]
        # Map RECOVERED → WIN/IMPROVED based on speedup
        if status == "RECOVERED":
            status = "WIN" if entry["best_speedup"] >= 1.10 else "IMPROVED"

        q = {
            "query_id": entry["query_id"],
            "status": status,
            "speedup": entry["best_speedup"],
            "best_source": entry["best_source"],
            "rewrite_speedup": entry["rewrite_speedup"],
            "config_speedup": entry["config_speedup"],
            "config_type": entry["config_type"],
            "config_gap_pct": entry["config_gap_pct"],
            "both_help": entry["both_help"],
        }
        queries.append(q)

    # Recount statuses from final mapped values
    status_counts = {}
    for q in queries:
        s = q["status"]
        status_counts[s] = status_counts.get(s, 0) + 1

    production = {
        "benchmark": "dsb",
        "engine": "postgresql",
        "scale_factor": 10,
        "updated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "source": "V2 Swarm (76 sessions) + Config Tuning (52 queries, 3-race validated)",
        "summary": {
            "total": len(queries),
            "wins": status_counts.get("WIN", 0),
            "improved": status_counts.get("IMPROVED", 0),
            "neutral": status_counts.get("NEUTRAL", 0),
            "regression": status_counts.get("REGRESSION", 0),
        },
        "queries": queries,
    }
    return production


def build_gold_csv(combined, old_index):
    """Build GOLD CSV from combined data."""
    board = combined["board"]

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "Query", "Best_Speedup", "Best_Source", "Status",
        "Rewrite_Speedup", "Config_Speedup", "Config_Type",
        "Orig_ms", "Opt_ms", "Notes",
    ])

    for entry in board:
        qid = entry["query_id"]
        status = entry["verdict"]
        if status == "RECOVERED":
            status = "WIN" if entry["best_speedup"] >= 1.10 else "IMPROVED"

        # Get timing data from old GOLD index if available
        old = old_index.get(qid, {})
        orig_ms = old.get("original_ms", "")
        opt_ms = old.get("optimized_ms", "")

        # Build notes
        notes_parts = []
        if entry["both_help"]:
            notes_parts.append("rewrite+config both help")
        if entry["verdict"] == "RECOVERED":
            notes_parts.append(f"Recovered from {entry['rewrite_speedup']:.2f}x rewrite regression via config")
        if entry["config_type"] and entry["config_type"] != "null":
            notes_parts.append(f"Config: {entry['config_type']}")

        writer.writerow([
            qid,
            f"{entry['best_speedup']:.2f}" if entry["best_speedup"] else "1.00",
            entry["best_source"],
            status,
            f"{entry['rewrite_speedup']:.2f}" if entry["rewrite_speedup"] else "",
            f"{entry['config_speedup']:.2f}" if entry["config_speedup"] else "",
            entry["config_type"] or "",
            f"{orig_ms:.1f}" if isinstance(orig_ms, (int, float)) and orig_ms else "",
            f"{opt_ms:.1f}" if isinstance(opt_ms, (int, float)) and opt_ms else "",
            "; ".join(notes_parts),
        ])

    return buf.getvalue()


def build_gold_index(combined, old_index):
    """Build GOLD pg_dsb/index.json from combined data."""
    board = combined["board"]

    # Count statuses
    status_counts = {}
    for entry in board:
        s = entry["verdict"]
        if s == "RECOVERED":
            s = "WIN" if entry["best_speedup"] >= 1.10 else "IMPROVED"
        status_counts[s] = status_counts.get(s, 0) + 1

    queries = []
    for entry in board:
        qid = entry["query_id"]
        status = entry["verdict"]
        if status == "RECOVERED":
            status = "WIN" if entry["best_speedup"] >= 1.10 else "IMPROVED"

        old = old_index.get(qid, {})

        q = {
            "query_id": qid,
            "engine": "postgresql",
            "benchmark": "DSB",
            "scale_factor": 10,
            "status": status,
            "speedup": entry["best_speedup"] or 1.0,
            "source": entry["best_source"],
            "best_source": entry["best_source"],
            "rewrite_speedup": entry["rewrite_speedup"],
            "config_speedup": entry["config_speedup"],
            "config_type": entry["config_type"],
            "both_help": entry["both_help"],
            "transforms": old.get("transforms", []),
            "original_ms": old.get("original_ms"),
            "optimized_ms": old.get("optimized_ms"),
            "notes": old.get("notes", ""),
        }

        # Add config notes for recovered/config-only entries
        if entry["verdict"] == "RECOVERED":
            q["notes"] = f"Recovered from {entry['rewrite_speedup']:.2f}x rewrite regression via {entry['config_type']}"
        elif entry["best_source"] == "config" and not q["notes"]:
            q["notes"] = f"Config-only win via {entry['config_type']}"

        queries.append(q)

    index = {
        "engine": "postgresql",
        "benchmark": "DSB SF10",
        "total": len(queries),
        "summary": status_counts,
        "source": "V2 Swarm (76 sessions) + Config Tuning (52 queries, 3-race validated)",
        "queries": queries,
    }
    return index


def update_readme(combined):
    """Update PG DSB section in README.md."""
    readme_path = GOLD_README
    text = readme_path.read_text()

    board = combined["board"]
    # Count final statuses
    status_counts = {}
    for entry in board:
        s = entry["verdict"]
        if s == "RECOVERED":
            s = "WIN" if entry["best_speedup"] >= 1.10 else "IMPROVED"
        status_counts[s] = status_counts.get(s, 0) + 1

    # Build new PG section
    new_pg_section = f"""### PostgreSQL DSB (SF10)
- **{len(board)} queries** total (combined: SQL rewrites + config tuning)
- Status: {status_counts}
- Sources: V2 Swarm (76 sessions), Config Tuning (52 queries, 3-race validated), pg_hint_plan"""

    # Replace existing PG section
    pattern = r"### PostgreSQL DSB \(SF10\)\n.*?(?=\n##|\n### DuckDB|\Z)"
    new_text = re.sub(pattern, new_pg_section + "\n", text, flags=re.DOTALL)

    # Also build the PG leaderboard table
    pg_table_header = """## PostgreSQL DSB Full Leaderboard

| # | Query | Speedup | Status | Best Source | Config Type | Notes |
|---|-------|---------|--------|-------------|-------------|-------|"""

    rows = []
    for i, entry in enumerate(board, 1):
        status = entry["verdict"]
        if status == "RECOVERED":
            status = "WIN" if entry["best_speedup"] >= 1.10 else "IMPROVED"

        speedup = f"{entry['best_speedup']:.2f}x" if entry["best_speedup"] else "1.00x"
        config_type = entry["config_type"] or ""
        notes_parts = []
        if entry["both_help"]:
            notes_parts.append("both help")
        if entry["verdict"] == "RECOVERED":
            notes_parts.append(f"recovered from {entry['rewrite_speedup']:.2f}x")
        notes = ", ".join(notes_parts)

        rows.append(f"| {i} | {entry['query_id']} | {speedup} | {status} | {entry['best_source']} | {config_type} | {notes} |")

    pg_table = pg_table_header + "\n" + "\n".join(rows)

    # Replace old PG leaderboard table
    pg_lb_pattern = r"## PostgreSQL DSB Full Leaderboard\n\n\|.*?\n(?:\|.*\n)*"
    new_text = re.sub(pg_lb_pattern, pg_table + "\n\n", new_text, flags=re.DOTALL)

    return new_text


def main():
    print("Loading combined leaderboard...")
    combined = load_combined()
    print(f"  {combined['total_queries']} queries, {combined['summary']}")

    print("Loading old GOLD index for timing data...")
    old_index = load_old_gold_index()
    print(f"  {len(old_index)} existing entries")

    # 1. Production leaderboard
    print("\nBuilding production leaderboard...")
    prod = build_production_leaderboard(combined)
    PROD_LB.parent.mkdir(parents=True, exist_ok=True)
    with open(PROD_LB, "w") as f:
        json.dump(prod, f, indent=2)
    print(f"  Written: {PROD_LB}")
    print(f"  Summary: {prod['summary']}")

    # 2. GOLD CSV
    print("\nBuilding GOLD CSV...")
    csv_text = build_gold_csv(combined, old_index)
    with open(GOLD_CSV, "w") as f:
        f.write(csv_text)
    line_count = csv_text.count("\n")
    print(f"  Written: {GOLD_CSV} ({line_count} lines)")

    # 3. GOLD index.json
    print("\nBuilding GOLD index.json...")
    index = build_gold_index(combined, old_index)
    with open(GOLD_INDEX, "w") as f:
        json.dump(index, f, indent=2)
    print(f"  Written: {GOLD_INDEX}")
    print(f"  Summary: {index['summary']}")

    # 4. README update
    print("\nUpdating README.md PG section...")
    readme_text = update_readme(combined)
    with open(GOLD_README, "w") as f:
        f.write(readme_text)
    print(f"  Written: {GOLD_README}")

    # Verification
    print("\n=== VERIFICATION ===")
    assert prod["scale_factor"] == 10, f"Wrong scale: {prod['scale_factor']}"
    assert prod["summary"]["total"] == 52, f"Wrong total: {prod['summary']['total']}"
    assert prod["summary"]["wins"] >= 24, f"Too few wins: {prod['summary']['wins']}"
    assert prod["summary"]["regression"] == 0, f"Unexpected regressions: {prod['summary']['regression']}"
    print("  All assertions passed!")
    print(f"  Production: {prod['summary']['total']} queries, {prod['summary']['wins']} wins, 0 regressions")


if __name__ == "__main__":
    main()

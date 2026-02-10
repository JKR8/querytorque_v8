#!/usr/bin/env python3
"""Merge config validation results into leaderboard_sf10.json.

Adds per-query fields: config_params, config_speedup, config_additive,
config_ms, config_status, best_variant.

Adds summary section: config_summary with counts and averages.

Input:  pg_config_validation_results.json + leaderboard_sf10.json
Output: Updated leaderboard_sf10.json
"""

from __future__ import annotations

import json
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
VALIDATION_FILE = ROOT / "research/pg_config_validation_results.json"
LEADERBOARD_FILE = ROOT / "packages/qt-sql/ado/benchmarks/postgres_dsb/leaderboard_sf10.json"
CONFIGS_FILE = ROOT / "research/pg_tuning_configs.json"


def main():
    print("=" * 70)
    print("PostgreSQL Config Leaderboard Merger")
    print("=" * 70)

    # Load inputs
    validation = json.loads(VALIDATION_FILE.read_text())
    leaderboard = json.loads(LEADERBOARD_FILE.read_text())
    configs = json.loads(CONFIGS_FILE.read_text())

    val_results = validation.get("results", {})
    print(f"\nValidation results: {len(val_results)} queries")
    print(f"Leaderboard queries: {len(leaderboard['queries'])}")

    # Merge into leaderboard
    config_wins = 0
    config_improved = 0
    config_neutral = 0
    config_regression = 0
    config_errors = 0
    total_configured = 0
    additives = []

    for entry in leaderboard["queries"]:
        query_id = entry["query_id"]
        val = val_results.get(query_id)
        cfg = configs.get(query_id, {})

        if val and val.get("status") != "ERROR":
            # Has benchmark results — all timings from same PG14 run
            entry["config_params"] = val.get("config_params", {})
            entry["config_speedup"] = val.get("config_speedup")
            entry["config_additive"] = val.get("config_additive")
            entry["config_ms"] = val.get("config_ms")
            entry["config_rewrite_ms"] = val.get("rewrite_ms")
            entry["config_original_ms"] = val.get("original_ms")
            entry["config_rewrite_speedup"] = val.get("rewrite_speedup")
            entry["config_status"] = val.get("status")
            entry["config_rules"] = val.get("rules", [])

            # Determine best variant from same-run data
            cs = val.get("config_speedup", 1.0)
            rs = val.get("rewrite_speedup", 1.0)
            if cs > rs and cs >= 1.05:
                entry["config_best_variant"] = "rewrite+config"
            elif rs >= 1.05:
                entry["config_best_variant"] = "rewrite"
            else:
                entry["config_best_variant"] = "original"

            status = val["status"]
            if status == "CONFIG_WIN":
                config_wins += 1
            elif status == "CONFIG_IMPROVED":
                config_improved += 1
            elif status == "CONFIG_NEUTRAL":
                config_neutral += 1
            elif status == "CONFIG_REGRESSION":
                config_regression += 1

            if val.get("config_additive") is not None:
                additives.append(val["config_additive"])
            total_configured += 1

        elif val and val.get("status") == "ERROR":
            entry["config_params"] = val.get("config_params", {})
            entry["config_status"] = "ERROR"
            entry["config_rules"] = val.get("rules", [])
            config_errors += 1
            total_configured += 1

        elif cfg.get("params"):
            # Has config but wasn't benchmarked (shouldn't happen normally)
            entry["config_params"] = cfg["params"]
            entry["config_status"] = "PENDING"
            entry["config_rules"] = cfg.get("rules_triggered", [])
            total_configured += 1

        else:
            # No config needed
            entry["config_params"] = {}
            entry["config_status"] = "NO_BOTTLENECK"
            entry["config_rules"] = []

    # Add config_summary
    avg_additive = round(sum(additives) / len(additives), 3) if additives else 0
    leaderboard["config_summary"] = {
        "total_configured": total_configured,
        "config_wins": config_wins,
        "config_improved": config_improved,
        "config_neutral": config_neutral,
        "config_regression": config_regression,
        "config_errors": config_errors,
        "avg_config_additive": avg_additive,
    }

    # Update timestamp
    leaderboard["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    leaderboard["source"] = (
        leaderboard.get("source", "") + " + config_tuning"
    )

    # Print summary
    print(f"\nConfig Summary:")
    print(f"  Total configured:   {total_configured}")
    print(f"  CONFIG_WIN:         {config_wins}")
    print(f"  CONFIG_IMPROVED:    {config_improved}")
    print(f"  CONFIG_NEUTRAL:     {config_neutral}")
    print(f"  CONFIG_REGRESSION:  {config_regression}")
    print(f"  CONFIG_ERROR:       {config_errors}")
    print(f"  Avg additive lift:  {avg_additive:.3f}x")

    # Print top config wins
    config_entries = [
        e for e in leaderboard["queries"]
        if e.get("config_additive") is not None
    ]
    config_entries.sort(key=lambda x: x.get("config_additive", 0), reverse=True)
    if config_entries:
        print(f"\nTop config winners:")
        for e in config_entries[:10]:
            orig_status = e.get("status", "?")
            print(f"  {e['query_id']:30s} additive={e['config_additive']:.3f}x "
                  f"config_speedup={e.get('config_speedup', 0):.3f}x "
                  f"orig_status={orig_status} "
                  f"config_status={e.get('config_status', '?')}")

    # Write updated leaderboard
    LEADERBOARD_FILE.write_text(json.dumps(leaderboard, indent=2) + "\n")
    print(f"\nUpdated {LEADERBOARD_FILE}")


if __name__ == "__main__":
    main()

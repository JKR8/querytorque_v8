"""CLI entry point for multi-round frontier probing.

Usage:
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. \
    python -m qt_sql.probe.run_probe benchmarks/duckdb_tpcds \
        --max-rounds 3 \
        --query-ids query042 query088

Multi-round loop:
    Round 0: No exploit profile -> probe all queries -> compress to exploit algorithm
    Round 1: Exploit profile from round 0 -> probe pushes into unexplored territory
    Round 2+: Diminishing returns -> stop when no new gaps discovered
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import List, Optional

from .probe_session import ProbeSession
from .compress import compress_probe_results


CONSTRAINTS_DIR = Path(__file__).resolve().parent.parent / "constraints"


def _load_existing_results(probe_dir: Path, round_num: int) -> list:
    """Load already-computed probe results for a given round."""
    from .schemas import ProbeResult, AttackResult, DiscoverySummary

    round_dir = probe_dir / f"round_{round_num}"
    if not round_dir.exists():
        return []

    results = []
    for json_path in sorted(round_dir.glob("*.json")):
        if json_path.name == "summary.json":
            continue
        try:
            data = json.loads(json_path.read_text())
            attacks = []
            for a_data in data.get("attacks", []):
                attacks.append(AttackResult(
                    attack_id=a_data.get("attack_id", 0),
                    target_node=a_data.get("target_node", ""),
                    gap_hypothesis=a_data.get("gap_hypothesis", ""),
                    structural_preconditions=a_data.get("structural_preconditions", ""),
                    mechanism=a_data.get("mechanism", ""),
                    expected_plan_change=a_data.get("expected_plan_change", ""),
                    semantic_risk=a_data.get("semantic_risk", ""),
                    optimized_sql=a_data.get("optimized_sql", ""),
                    status=a_data.get("status", ""),
                    speedup=a_data.get("speedup", 0.0),
                    error_messages=a_data.get("error_messages", []),
                ))

            ds_data = data.get("discovery_summary")
            discovery = None
            if ds_data:
                discovery = DiscoverySummary(
                    new_gaps=ds_data.get("new_gaps", []),
                    extended_gaps=ds_data.get("extended_gaps", []),
                    negative_results=ds_data.get("negative_results", []),
                )

            results.append(ProbeResult(
                query_id=data.get("query_id", json_path.stem),
                engine=data.get("engine", "duckdb"),
                original_sql="",  # not stored in output JSON
                attacks=attacks,
                discovery_summary=discovery,
                round_num=data.get("round_num", round_num),
            ))
        except Exception as e:
            print(f"  Warning: Failed to load {json_path}: {e}", flush=True)

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Frontier probe: discover engine optimizer gaps"
    )
    parser.add_argument(
        "benchmark_dir",
        help="Path to benchmark directory (with config.json + queries/)",
    )
    parser.add_argument(
        "--max-rounds",
        type=int,
        default=3,
        help="Maximum probe rounds (default: 3)",
    )
    parser.add_argument(
        "--query-ids",
        nargs="*",
        help="Subset of queries to probe (default: all)",
    )
    parser.add_argument(
        "--round",
        type=int,
        help="Resume at specific round (skips earlier rounds)",
    )
    parser.add_argument(
        "--compress-only",
        action="store_true",
        help="Skip probing, just compress existing results for the given round",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=20,
        help="Max concurrent LLM calls (default: 20)",
    )
    parser.add_argument(
        "--provider",
        help="LLM provider override (default: from .env)",
    )
    parser.add_argument(
        "--model",
        help="LLM model override (default: from .env)",
    )

    args = parser.parse_args()
    benchmark_dir = Path(args.benchmark_dir)

    if not (benchmark_dir / "config.json").exists():
        print(f"Error: config.json not found in {benchmark_dir}", file=sys.stderr)
        sys.exit(1)

    # Determine engine from config
    config_data = json.loads((benchmark_dir / "config.json").read_text())
    engine = config_data.get("engine", "duckdb")
    engine_norm = engine.lower()
    if engine_norm in ("postgres", "pg"):
        engine_norm = "postgresql"

    start_round = args.round if args.round is not None else 0
    t_total = time.time()

    for round_num in range(start_round, args.max_rounds):
        print(
            f"\n{'='*60}\n"
            f"  ROUND {round_num}\n"
            f"{'='*60}",
            flush=True,
        )

        # Load current exploit algorithm (None for round 0)
        algo_path = CONSTRAINTS_DIR / f"exploit_algorithm_{engine_norm}.yaml"
        exploit_text: Optional[str] = None
        if algo_path.exists():
            exploit_text = algo_path.read_text()
            print(f"  Loaded exploit algorithm: {algo_path} ({len(exploit_text)} chars)", flush=True)
        else:
            print(f"  No exploit algorithm found (round 0)", flush=True)

        if args.compress_only:
            # Just compress existing results
            results = _load_existing_results(benchmark_dir / "probe", round_num)
            if not results:
                print(f"  No existing results for round {round_num}", flush=True)
                break
            print(f"  Loaded {len(results)} existing results", flush=True)
        else:
            # Probe
            session = ProbeSession(
                benchmark_dir=benchmark_dir,
                exploit_profile_text=exploit_text,
                round_num=round_num,
                provider=args.provider,
                model=args.model,
            )
            results = session.probe_corpus(
                query_ids=args.query_ids,
                max_workers=args.max_workers,
            )
            session.save_results()

        # Compress
        algo_text = compress_probe_results(
            probe_results=results,
            previous_algorithm_text=exploit_text,
            benchmark_dir=benchmark_dir,
            round_num=round_num,
            provider=args.provider,
            model=args.model,
        )

        # Convergence check
        total_new = sum(
            len(r.discovery_summary.new_gaps)
            for r in results
            if r.discovery_summary
        )
        if total_new == 0 and round_num >= 1:
            print(
                f"\n  CONVERGED at round {round_num}: "
                f"no new gaps discovered",
                flush=True,
            )
            break

        print(
            f"\n  Round {round_num} complete: "
            f"{total_new} new gaps discovered",
            flush=True,
        )

    print(
        f"\n{'='*60}\n"
        f"  ALL ROUNDS COMPLETE ({time.time() - t_total:.0f}s total)\n"
        f"  Exploit algorithm: {CONSTRAINTS_DIR / f'exploit_algorithm_{engine_norm}.yaml'}\n"
        f"{'='*60}",
        flush=True,
    )


if __name__ == "__main__":
    main()

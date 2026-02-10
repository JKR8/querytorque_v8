#!/usr/bin/env python3
"""
Comprehensive DSB validation script - Tests ALL 52 DSB queries.

Timing Methodology (Default: 3x Runs):
  1. Run query 3 times
  2. Discard 1st run (warmup)
  3. Average last 2 runs

Discovers all DSB query templates, runs optimizations, validates results,
and produces consolidated leaderboard.

Usage:
    python validate_all_dsb.py                          # Full run (52 queries, 3x)
    python validate_all_dsb.py --runs 5                 # 5x trimmed mean (more robust)
    python validate_all_dsb.py --queries query001 query010  # Specific queries
    python validate_all_dsb.py --agg-only              # Only aggregation queries
    python validate_all_dsb.py --dry-run               # Show what would run
    python validate_all_dsb.py --output results.json   # Custom output file
"""

import argparse
import json
import logging
import sys
import subprocess
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
from dataclasses import dataclass, asdict, field

# Setup paths
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
DSB_TEMPLATE_DIR = Path("/mnt/d/dsb/query_templates_pg")
ROUNDS_DIR = SCRIPT_DIR / "rounds"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


@dataclass
class DSBQuery:
    """DSB Query metadata."""
    query_id: str
    query_type: str  # "agg", "spj", "multi_block"
    path: Path

    def __hash__(self):
        return hash(self.query_id)

    def __eq__(self, other):
        return self.query_id == other.query_id


@dataclass
class BatchResult:
    """Consolidated batch results."""
    timestamp: str
    total_queries: int
    discovered: int
    validated: int
    wins: int
    passes: int
    regressions: int
    errors: int
    average_speedup: float
    best_speedup: Dict[str, Any] = field(default_factory=dict)
    worst_regression: Dict[str, Any] = field(default_factory=dict)
    results: List[Dict[str, Any]] = field(default_factory=list)


def discover_dsb_queries() -> List[DSBQuery]:
    """Discover all 52 DSB queries from template directory."""
    queries = []

    if not DSB_TEMPLATE_DIR.exists():
        logger.error(f"DSB template directory not found: {DSB_TEMPLATE_DIR}")
        return queries

    # Aggregation queries
    agg_dir = DSB_TEMPLATE_DIR / "agg_queries"
    if agg_dir.exists():
        for tpl_file in sorted(agg_dir.glob("query*.tpl")):
            query_id = tpl_file.stem  # e.g., "query072"
            queries.append(DSBQuery(
                query_id=query_id,
                query_type="agg",
                path=tpl_file
            ))

    # Multi-block queries
    multi_dir = DSB_TEMPLATE_DIR / "multi_block_queries"
    if multi_dir.exists():
        for tpl_file in sorted(multi_dir.glob("query*.tpl")):
            query_id = tpl_file.stem
            queries.append(DSBQuery(
                query_id=query_id,
                query_type="multi_block",
                path=tpl_file
            ))

    # SPJ queries
    spj_dir = DSB_TEMPLATE_DIR / "spj_queries"
    if spj_dir.exists():
        for tpl_file in sorted(spj_dir.glob("query*_spj.tpl")):
            query_id = tpl_file.stem  # e.g., "query001_spj"
            queries.append(DSBQuery(
                query_id=query_id,
                query_type="spj",
                path=tpl_file
            ))

    return queries


def validate_single_query(query_id: str, runs: int = 5, dry_run: bool = False) -> Dict[str, Any]:
    """Validate a single query using existing validate_dsb_pg.py script."""

    validate_script = SCRIPT_DIR / "validate_dsb_pg.py"
    if not validate_script.exists():
        logger.error(f"Validation script not found: {validate_script}")
        return {
            "query_id": query_id,
            "status": "ERROR",
            "error": "Validation script not found"
        }

    if dry_run:
        logger.info(f"[DRY RUN] Would validate: {query_id}")
        return {"query_id": query_id, "status": "DRY_RUN", "speedup": 0.0}

    cmd = [
        "python", str(validate_script),
        "--round", "round_01",
        "--query", query_id,
        "--runs", str(runs),
        "--json"
    ]

    try:
        logger.info(f"Validating {query_id}...")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout
        )

        if result.returncode == 0 and result.stdout:
            try:
                return json.loads(result.stdout)
            except json.JSONDecodeError:
                return {
                    "query_id": query_id,
                    "status": "ERROR",
                    "error": f"Invalid JSON from validator"
                }
        else:
            return {
                "query_id": query_id,
                "status": "ERROR",
                "error": result.stderr or "Validator failed"
            }

    except subprocess.TimeoutExpired:
        return {
            "query_id": query_id,
            "status": "ERROR",
            "error": "Validation timeout (>10m)"
        }
    except Exception as e:
        return {
            "query_id": query_id,
            "status": "ERROR",
            "error": str(e)
        }


def run_batch_validation(
    queries: List[DSBQuery],
    runs: int = 3,
    limit: int = None,
    query_filter: List[str] = None,
    type_filter: str = None,
    dry_run: bool = False
) -> BatchResult:
    """Run validation on multiple queries.

    Timing methodology:
    - runs=3: Discard 1st (warmup), average last 2
    - runs=5: Run 5x, discard min/max outliers, average middle 3
    """

    # Filter queries
    if type_filter:
        queries = [q for q in queries if q.query_type == type_filter]

    if query_filter:
        query_ids = set(query_filter)
        queries = [q for q in queries if q.query_id in query_ids]

    if limit:
        queries = queries[:limit]

    method = "3x (discard warmup, avg last 2)" if runs == 3 else f"{runs}x trimmed mean"
    logger.info(f"Running batch validation on {len(queries)} queries (method={method}, dry_run={dry_run})")

    results = []
    wins = 0
    passes = 0
    regressions = 0
    errors = 0
    speedups = []
    best = None
    worst = None

    for i, query in enumerate(queries, 1):
        logger.info(f"[{i}/{len(queries)}] Processing {query.query_id}...")

        result = validate_single_query(query.query_id, runs=runs, dry_run=dry_run)
        results.append(result)

        if dry_run:
            continue

        status = result.get("status", "ERROR")
        speedup = result.get("speedup", 0.0)

        if status == "PASS":
            if speedup >= 1.1:
                wins += 1
            elif speedup >= 0.95:
                passes += 1
            else:
                regressions += 1

            speedups.append(speedup)

            # Track best and worst
            if best is None or speedup > best.get("speedup", 0):
                best = {"query_id": query.query_id, "speedup": speedup}
            if worst is None or speedup < worst.get("speedup", float("inf")):
                worst = {"query_id": query.query_id, "speedup": speedup}

        elif status == "ERROR":
            errors += 1

    avg_speedup = sum(speedups) / len(speedups) if speedups else 0.0

    batch_result = BatchResult(
        timestamp=datetime.utcnow().isoformat(),
        total_queries=52,  # Total in catalog
        discovered=len(queries),
        validated=len([r for r in results if r.get("status") in ["PASS", "FAIL"]]),
        wins=wins,
        passes=passes,
        regressions=regressions,
        errors=errors,
        average_speedup=avg_speedup,
        best_speedup=best or {},
        worst_regression=worst or {},
        results=results
    )

    return batch_result


def format_summary(batch_result: BatchResult) -> str:
    """Format results summary."""
    summary = f"""
╔════════════════════════════════════════════════════════════╗
║         DSB PostgreSQL Batch Validation Results           ║
╚════════════════════════════════════════════════════════════╝

Timestamp:     {batch_result.timestamp}

Coverage:
  Total DSB Queries:      {batch_result.total_queries}
  Discovered:             {batch_result.discovered}
  Validated:              {batch_result.validated}

Results:
  Wins (≥1.1x):           {batch_result.wins}
  Passes (0.95-1.1x):     {batch_result.passes}
  Regressions (<0.95x):   {batch_result.regressions}
  Errors:                 {batch_result.errors}

Performance:
  Average Speedup:        {batch_result.average_speedup:.2f}x
  Best:                   {batch_result.best_speedup.get('query_id', 'N/A')} ({batch_result.best_speedup.get('speedup', 0):.2f}x)
  Worst:                  {batch_result.worst_regression.get('query_id', 'N/A')} ({batch_result.worst_regression.get('speedup', 0):.2f}x)

Success Rate:
  {(batch_result.validated / batch_result.discovered * 100):.1f}% validated
  {(batch_result.wins / max(batch_result.validated, 1) * 100):.1f}% of validated are wins
"""
    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Comprehensive DSB validation - tests all 52 queries",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="Number of validation runs: 3=3x (discard 1st, avg last 2), 5=5x trimmed mean (discard min/max) (default: 3)"
    )
    parser.add_argument(
        "--queries",
        nargs="+",
        help="Specific queries to validate (e.g., query001 query010)"
    )
    parser.add_argument(
        "--agg-only",
        action="store_true",
        help="Only validate aggregation queries"
    )
    parser.add_argument(
        "--spj-only",
        action="store_true",
        help="Only validate select-project-join queries"
    )
    parser.add_argument(
        "--multi-only",
        action="store_true",
        help="Only validate multi-block queries"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of queries to validate"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would run without executing"
    )
    parser.add_argument(
        "--output",
        default="dsb_batch_results.json",
        help="Output JSON file for results (default: dsb_batch_results.json)"
    )

    args = parser.parse_args()

    # Discover queries
    logger.info("Discovering DSB queries...")
    queries = discover_dsb_queries()
    logger.info(f"Discovered {len(queries)} queries")

    if not queries:
        logger.error("No queries discovered!")
        return 1

    # Run validation
    type_filter = None
    if args.agg_only:
        type_filter = "agg"
    elif args.spj_only:
        type_filter = "spj"
    elif args.multi_only:
        type_filter = "multi_block"

    batch_result = run_batch_validation(
        queries,
        runs=args.runs,
        limit=args.limit,
        query_filter=args.queries,
        type_filter=type_filter,
        dry_run=args.dry_run
    )

    # Print summary
    print(format_summary(batch_result))

    # Save results
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(asdict(batch_result), f, indent=2)

    logger.info(f"Results saved to {output_path}")

    return 0 if batch_result.errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

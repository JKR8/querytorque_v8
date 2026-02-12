"""Config Boost: post-rewrite SET LOCAL tuning from EXPLAIN analysis.

After the swarm fan-out picks a winning rewrite, this module proposes
SET LOCAL config changes based on the winner's EXPLAIN ANALYZE plan.
Rules are derived from V1 manual tuning evidence + PG internals.

Two modes:
  1. Rule-based (default): Pure regex EXPLAIN parsing, no LLM calls.
  2. LLM-driven (--use-llm): Uses pg_tuner prompt, falls back to rules.

Two input paths:
  1. boost_session() / boost_benchmark(): Reads from swarm_sessions/
  2. boost_from_leaderboard(): Reads from leaderboard.json (preferred)

Usage:
    from qt_sql.config_boost import boost_from_leaderboard

    # From leaderboard (preferred)
    results = boost_from_leaderboard(benchmark_dir, dsn)

    # Legacy: from swarm_sessions
    from qt_sql.config_boost import boost_session, boost_benchmark
    result = boost_session(session_dir, dsn)
"""

from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# TPC-DS / DSB fact tables (large, benefit from parallel + SSD tuning)
_FACT_TABLES = frozenset([
    "store_sales", "catalog_sales", "web_sales",
    "store_returns", "catalog_returns", "web_returns",
    "inventory",
])


# =========================================================================
# Rule-Based EXPLAIN → Config Mapping
# =========================================================================

def propose_config_from_explain(
    explain_text: str,
    current_work_mem_mb: int = 4,
) -> Dict[str, Dict[str, Any]]:
    """Parse EXPLAIN ANALYZE text and propose SET LOCAL params.

    Each rule returns at most 1 param change. Multiple rules can fire,
    producing a combined config.

    Args:
        explain_text: Full EXPLAIN ANALYZE output text
        current_work_mem_mb: Current work_mem setting in MB (default 4MB)

    Returns:
        Dict of param_name -> {"value": str, "rule": str, "reason": str}
    """
    if not explain_text:
        return {}

    proposals: Dict[str, Dict[str, Any]] = {}

    # Rule 1: Hash operations with high peak memory → increase work_mem
    _rule_work_mem(explain_text, current_work_mem_mb, proposals)

    # Rule 2: Nested Loop with high row estimates → disable nestloop
    _rule_nestloop(explain_text, proposals)

    # Rule 3: No parallel nodes despite seq scans → enable parallelism
    _rule_parallelism(explain_text, proposals)

    # Rule 4: JIT on short query → disable JIT
    _rule_jit(explain_text, proposals)

    # Rule 5: Seq scans on fact tables → lower random_page_cost
    _rule_random_page_cost(explain_text, proposals)

    # Rule 6: Many joins → increase join_collapse_limit
    _rule_join_collapse(explain_text, proposals)

    return proposals


def _rule_work_mem(
    explain: str, current_mb: int, proposals: Dict[str, Dict[str, Any]]
) -> None:
    """Rule 1: Hash ops with peak memory near/exceeding work_mem."""
    # Match patterns like: "Memory: 12345kB" or "Peak Memory: 12345kB"
    # Also match "Memory Usage: 12345kB" and "Batches: N" (indicates spill)
    memory_matches = re.findall(
        r'(?:Peak\s+)?Memory(?:\s+Usage)?:\s*(\d+)kB', explain, re.IGNORECASE
    )
    if not memory_matches:
        return

    peak_kb = max(int(m) for m in memory_matches)
    peak_mb = peak_kb / 1024

    # Check for disk spill indicators (find ALL batch counts, not just first)
    batch_matches = re.findall(r'Batches:\s*(\d+)', explain)
    max_batches = max((int(b) for b in batch_matches), default=1)
    has_spill = max_batches > 1

    # Fire if peak memory is >= 50% of current work_mem or if spilling
    threshold_mb = current_mb * 0.5
    if peak_mb >= threshold_mb or has_spill:
        # Propose 4x the peak memory, capped at 2048MB
        proposed_mb = min(2048, max(256, int(peak_mb * 4)))
        # Round up to nearest power of 2 for clean values
        proposed_mb = _round_to_power_of_2(proposed_mb)
        proposals["work_mem"] = {
            "value": f"{proposed_mb}MB",
            "rule": "increase_work_mem",
            "reason": (
                f"Peak hash memory {peak_mb:.0f}MB "
                f"{'+ disk spill (' + str(max_batches) + ' batches)' if has_spill else ''} "
                f"vs current {current_mb}MB work_mem"
            ),
        }


def _rule_nestloop(explain: str, proposals: Dict[str, Dict[str, Any]]) -> None:
    """Rule 2: Nested Loop with high row estimates → disable."""
    # Find Nested Loop nodes with high actual rows
    nl_patterns = re.findall(
        r'Nested Loop.*?(?:actual\s+)?rows=(\d+)',
        explain, re.IGNORECASE | re.DOTALL,
    )
    if not nl_patterns:
        return

    max_rows = max(int(r) for r in nl_patterns)
    if max_rows > 10000:
        proposals["enable_nestloop"] = {
            "value": "off",
            "rule": "disable_nestloop",
            "reason": f"Nested Loop scanning {max_rows:,} rows",
        }


def _rule_parallelism(explain: str, proposals: Dict[str, Dict[str, Any]]) -> None:
    """Rule 3: No parallel nodes despite sequential scans on large tables."""
    has_parallel = bool(re.search(r'Gather|Parallel', explain, re.IGNORECASE))
    if has_parallel:
        return

    # Check for seq scans with high row counts
    seq_scan_rows = re.findall(
        r'Seq Scan on (\w+).*?rows=(\d+)',
        explain, re.IGNORECASE | re.DOTALL,
    )
    large_scans = [
        (table, int(rows))
        for table, rows in seq_scan_rows
        if int(rows) > 100_000
    ]
    if large_scans:
        biggest = max(large_scans, key=lambda x: x[1])
        proposals["max_parallel_workers_per_gather"] = {
            "value": "4",
            "rule": "enable_parallelism",
            "reason": (
                f"No parallel nodes but Seq Scan on {biggest[0]} "
                f"({biggest[1]:,} rows)"
            ),
        }


def _rule_jit(explain: str, proposals: Dict[str, Dict[str, Any]]) -> None:
    """Rule 4: JIT compilation on short queries → disable."""
    has_jit = bool(re.search(r'JIT:', explain, re.IGNORECASE))
    if not has_jit:
        return

    # Extract total execution time
    time_match = re.search(
        r'Execution\s+Time:\s*([\d.]+)\s*ms', explain, re.IGNORECASE
    )
    if not time_match:
        # Try Planning + Execution
        time_match = re.search(
            r'Total\s+runtime:\s*([\d.]+)\s*ms', explain, re.IGNORECASE
        )
    if not time_match:
        return

    total_ms = float(time_match.group(1))
    if total_ms < 500:
        # Also check JIT overhead
        jit_time_match = re.search(
            r'JIT:.*?Time:\s*([\d.]+)\s*ms', explain, re.IGNORECASE | re.DOTALL
        )
        jit_ms = float(jit_time_match.group(1)) if jit_time_match else 0

        proposals["jit"] = {
            "value": "off",
            "rule": "jit_off_short_query",
            "reason": (
                f"JIT active on {total_ms:.0f}ms query "
                f"(JIT overhead ~{jit_ms:.0f}ms)"
            ),
        }


def _rule_random_page_cost(
    explain: str, proposals: Dict[str, Dict[str, Any]]
) -> None:
    """Rule 5: Seq scans on fact tables → favor index scans."""
    seq_scan_matches = re.findall(
        r'Seq Scan on (\w+).*?rows=(\d+)',
        explain, re.IGNORECASE | re.DOTALL,
    )

    fact_seq_scans = [
        (table, int(rows))
        for table, rows in seq_scan_matches
        if table.lower() in _FACT_TABLES and int(rows) > 100_000
    ]

    if fact_seq_scans:
        biggest = max(fact_seq_scans, key=lambda x: x[1])
        proposals["random_page_cost"] = {
            "value": "1.1",
            "rule": "favor_index_scans",
            "reason": (
                f"Seq Scan on fact table {biggest[0]} "
                f"({biggest[1]:,} rows) — favor index scans"
            ),
        }


def _rule_join_collapse(
    explain: str, proposals: Dict[str, Dict[str, Any]]
) -> None:
    """Rule 6: Many joins → increase join_collapse_limit."""
    # Count distinct join nodes in the plan
    join_count = len(re.findall(
        r'(?:Hash|Merge|Nested Loop)\s+(?:Left |Right |Full |Semi |Anti )?Join',
        explain, re.IGNORECASE,
    ))

    if join_count > 6:
        proposals["join_collapse_limit"] = {
            "value": "12",
            "rule": "increase_join_collapse",
            "reason": f"{join_count} join nodes in plan",
        }


def _round_to_power_of_2(n: int) -> int:
    """Round up to nearest power of 2."""
    if n <= 0:
        return 1
    p = 1
    while p < n:
        p <<= 1
    return p


# =========================================================================
# Session + Benchmark Runners
# =========================================================================

def boost_session(
    session_dir: Path,
    dsn: str,
    min_speedup: float = 1.05,
    dry_run: bool = False,
) -> Optional[Dict[str, Any]]:
    """Run config boost on a completed swarm session.

    1. Load session.json, find best worker
    2. Skip if best_speedup < min_speedup
    3. Get EXPLAIN ANALYZE for best worker's SQL (re-run if needed)
    4. Call propose_config_from_explain()
    5. If config proposed and not dry_run, run benchmark_three_variants()
    6. Write config_boost.json
    7. Return result dict

    Args:
        session_dir: Path to swarm_sessions/<query_id>/
        dsn: PostgreSQL connection DSN
        min_speedup: Minimum rewrite speedup to attempt boost
        dry_run: If True, propose configs without benchmarking

    Returns:
        Result dict or None if session doesn't qualify
    """
    session_dir = Path(session_dir)
    session_json = session_dir / "session.json"
    if not session_json.exists():
        return None

    try:
        session_data = json.loads(session_json.read_text())
    except (json.JSONDecodeError, OSError):
        return None

    query_id = session_data.get("query_id", session_dir.name)
    best_speedup = session_data.get("best_speedup", 0.0)
    best_worker_id = session_data.get("best_worker_id")

    # Skip if not a winner
    if best_speedup < min_speedup:
        return {
            "query_id": query_id,
            "status": "SKIPPED",
            "reason": f"speedup {best_speedup:.2f}x < {min_speedup}x threshold",
        }

    # Find best worker's SQL
    optimized_sql = _load_best_worker_sql(session_dir, best_worker_id)
    if not optimized_sql:
        return {
            "query_id": query_id,
            "status": "SKIPPED",
            "reason": "Could not find best worker's optimized SQL",
        }

    # Load original SQL
    original_sql = _load_original_sql(session_dir)
    if not original_sql:
        return {
            "query_id": query_id,
            "status": "SKIPPED",
            "reason": "Could not find original SQL",
        }

    # Get EXPLAIN ANALYZE for the optimized SQL
    explain_text = _get_or_run_explain(session_dir, dsn, optimized_sql, best_worker_id)
    if not explain_text:
        return {
            "query_id": query_id,
            "status": "SKIPPED",
            "reason": "Could not obtain EXPLAIN ANALYZE",
        }

    # Propose config
    proposals = propose_config_from_explain(explain_text)
    if not proposals:
        result = {
            "query_id": query_id,
            "rewrite_speedup": best_speedup,
            "config_proposed": {},
            "config_commands": [],
            "rules_fired": [],
            "status": "NO_RULES",
        }
        _write_config_boost_json(session_dir, result)
        return result

    # Build config dict and SET LOCAL commands
    from .pg_tuning import validate_tuning_config, build_set_local_sql

    raw_config = {k: v["value"] for k, v in proposals.items()}
    validated_config = validate_tuning_config(raw_config)
    if not validated_config:
        result = {
            "query_id": query_id,
            "rewrite_speedup": best_speedup,
            "config_proposed": raw_config,
            "config_commands": [],
            "rules_fired": [v["rule"] for v in proposals.values()],
            "status": "NO_VALID_CONFIG",
        }
        _write_config_boost_json(session_dir, result)
        return result

    config_commands = build_set_local_sql(validated_config)
    rules_fired = [v["rule"] for v in proposals.values()]
    reasons = {k: v["reason"] for k, v in proposals.items()}

    if dry_run:
        result = {
            "query_id": query_id,
            "rewrite_speedup": best_speedup,
            "config_proposed": validated_config,
            "config_commands": config_commands,
            "rules_fired": rules_fired,
            "reasons": reasons,
            "status": "DRY_RUN",
        }
        return result

    # Benchmark: original vs rewrite vs rewrite+config
    benchmark_result = _run_benchmark(dsn, original_sql, optimized_sql, config_commands)
    if "error" in benchmark_result:
        result = {
            "query_id": query_id,
            "rewrite_speedup": best_speedup,
            "config_proposed": validated_config,
            "config_commands": config_commands,
            "rules_fired": rules_fired,
            "reasons": reasons,
            "benchmark_error": benchmark_result["error"],
            "status": "BENCHMARK_ERROR",
        }
        _write_config_boost_json(session_dir, result)
        return result

    # Determine if config actually helped
    config_speedup = benchmark_result.get("config_speedup", 0)
    rewrite_speedup = benchmark_result.get("rewrite_speedup", 0)
    config_additive = benchmark_result.get("config_additive", 1.0)

    if benchmark_result.get("best_variant") == "rewrite+config" and config_additive > 1.02:
        status = "BOOSTED"
    else:
        status = "NO_GAIN"

    result = {
        "query_id": query_id,
        "rewrite_speedup": best_speedup,
        "config_proposed": validated_config,
        "config_commands": config_commands,
        "benchmark": benchmark_result,
        "rules_fired": rules_fired,
        "reasons": reasons,
        "status": status,
    }
    _write_config_boost_json(session_dir, result)
    return result


def boost_benchmark(
    benchmark_dir: Path,
    dsn: str,
    min_speedup: float = 1.05,
    dry_run: bool = False,
    query_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Run config boost on qualifying sessions in a benchmark.

    Args:
        benchmark_dir: Path to the benchmark directory
        dsn: PostgreSQL connection DSN
        min_speedup: Minimum rewrite speedup to attempt boost
        dry_run: If True, propose configs without benchmarking
        query_ids: If provided, only boost these query IDs (respects -q filter)

    Returns:
        List of result dicts, one per session attempted
    """
    benchmark_dir = Path(benchmark_dir)
    sessions_dir = benchmark_dir / "swarm_sessions"

    if not sessions_dir.exists():
        logger.warning(f"No swarm_sessions directory in {benchmark_dir}")
        return []

    results: List[Dict[str, Any]] = []
    session_dirs = sorted(
        d for d in sessions_dir.iterdir()
        if d.is_dir() and (d / "session.json").exists()
    )

    # Filter to requested query IDs if provided
    if query_ids is not None:
        query_id_set = set(query_ids)
        session_dirs = [d for d in session_dirs if d.name in query_id_set]

    for session_dir in session_dirs:
        try:
            result = boost_session(session_dir, dsn, min_speedup, dry_run)
            if result:
                results.append(result)
        except Exception as e:
            logger.error(f"Config boost failed for {session_dir.name}: {e}")
            results.append({
                "query_id": session_dir.name,
                "status": "ERROR",
                "error": str(e),
            })

    return results


# =========================================================================
# Internal Helpers
# =========================================================================

def _load_best_worker_sql(session_dir: Path, best_worker_id: Optional[int]) -> Optional[str]:
    """Find and load the best worker's optimized SQL."""
    if best_worker_id is None:
        return None

    # Check fan-out iteration first
    iter_dir = session_dir / "iteration_00_fan_out"
    if best_worker_id <= 4:
        sql_file = iter_dir / f"worker_{best_worker_id:02d}" / "optimized.sql"
    else:
        # Snipe worker — take the last (most recent) match
        sql_file = None
        for snipe_dir in sorted(session_dir.glob("iteration_*_snipe")):
            candidate = snipe_dir / f"worker_{best_worker_id:02d}" / "optimized.sql"
            if candidate.exists():
                sql_file = candidate

    if sql_file and sql_file.exists():
        return sql_file.read_text(errors="replace").strip()
    return None


def _load_original_sql(session_dir: Path) -> Optional[str]:
    """Load original SQL from the benchmark queries directory."""
    query_id = session_dir.name
    # Walk up to benchmark dir: swarm_sessions/<query_id> → benchmark_dir
    benchmark_dir = session_dir.parent.parent
    sql_path = benchmark_dir / "queries" / f"{query_id}.sql"
    if sql_path.exists():
        return sql_path.read_text(errors="replace").strip()
    return None


def _get_or_run_explain(
    session_dir: Path,
    dsn: str,
    optimized_sql: str,
    best_worker_id: Optional[int],
) -> Optional[str]:
    """Get EXPLAIN ANALYZE for optimized SQL — cached or fresh."""
    # Try to load from existing candidate explains on disk
    if best_worker_id is not None:
        iter_dir = session_dir / "iteration_00_fan_out"
        explain_file = iter_dir / f"worker_{best_worker_id:02d}" / "explain.txt"
        if explain_file.exists():
            text = explain_file.read_text(errors="replace").strip()
            if text:
                return text

    # Run fresh EXPLAIN ANALYZE
    try:
        from .execution.database_utils import run_explain_analyze
        from .prompts.analyst_briefing import format_pg_explain_tree

        result = run_explain_analyze(dsn, optimized_sql)
        if not result:
            return None

        plan_text = result.get("plan_text")
        if not plan_text:
            plan_json = result.get("plan_json")
            if plan_json:
                plan_text = format_pg_explain_tree(plan_json)

        return plan_text
    except Exception as e:
        logger.warning(f"EXPLAIN ANALYZE failed: {e}")
        return None


def _run_benchmark(
    dsn: str,
    original_sql: str,
    rewrite_sql: str,
    config_commands: List[str],
) -> Dict[str, Any]:
    """Run interleaved 3-variant benchmark using existing validator."""
    from .validate import Validator

    validator = Validator(sample_db=dsn)
    try:
        return validator.benchmark_three_variants(
            original_sql, rewrite_sql, config_commands
        )
    finally:
        validator.close()


def _write_config_boost_json(session_dir: Path, result: Dict[str, Any]) -> None:
    """Write config_boost.json to session directory."""
    out_path = session_dir / "config_boost.json"
    out_path.write_text(json.dumps(result, indent=2, default=str))
    logger.info(f"Wrote {out_path}")


# =========================================================================
# LLM-Driven Config Proposal
# =========================================================================

def propose_config_from_llm(
    query_sql: str,
    explain_text: str,
    dsn: str,
    current_work_mem_mb: int = 4,
) -> Dict[str, Dict[str, Any]]:
    """LLM-driven config proposal using pg_tuner prompt.

    Falls back to rule-based if LLM unavailable or returns empty config.

    Args:
        query_sql: The SQL query being optimized
        explain_text: EXPLAIN ANALYZE output text
        dsn: PostgreSQL connection DSN (for system profile)
        current_work_mem_mb: Current work_mem in MB

    Returns:
        Dict of param_name -> {"value": str, "rule": str, "reason": str}
    """
    try:
        from qt_shared.llm import create_llm_client
        from .prompts.pg_tuner import build_pg_tuner_prompt
        from .pg_tuning import load_or_collect_profile, PG_TUNABLE_PARAMS

        # Load system profile
        cache_dir = Path.cwd() / ".cache"
        profile = load_or_collect_profile(dsn, cache_dir=cache_dir)
        settings = {s["name"]: s["setting"] for s in profile.settings}

        # Load engine profile if available
        engine_profile_path = (
            Path(__file__).parent / "constraints" / "engine_profile_postgresql.json"
        )
        engine_profile = None
        if engine_profile_path.exists():
            engine_profile = json.loads(engine_profile_path.read_text())

        # Build prompt
        prompt = build_pg_tuner_prompt(
            query_sql=query_sql,
            explain_plan=explain_text,
            current_settings=settings,
            engine_profile=engine_profile,
            baseline_ms=None,
        )

        # Call LLM
        llm = create_llm_client()
        if llm is None:
            logger.warning("No LLM client configured, falling back to rules")
            return propose_config_from_explain(explain_text, current_work_mem_mb)

        response = llm.analyze(prompt)

        # Parse response (expects JSON {"params": {...}, "reasoning": "..."})
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', response, re.DOTALL)
        if not json_match:
            json_match = re.search(r'(\{[^{}]*"params"[^{}]*\{[^}]*\}[^}]*\})', response, re.DOTALL)

        if json_match:
            config_data = json.loads(json_match.group(1))
            raw_params = config_data.get("params", {})
            reasoning = config_data.get("reasoning", "")

            # Convert to proposals format, validate against whitelist
            proposals: Dict[str, Dict[str, Any]] = {}
            for param, value in raw_params.items():
                if param in PG_TUNABLE_PARAMS:
                    proposals[param] = {
                        "value": str(value),
                        "rule": "llm_analysis",
                        "reason": reasoning[:200],
                    }

            if proposals:
                return proposals

        logger.info("LLM returned no usable config, falling back to rules")

    except Exception as e:
        logger.warning(f"LLM config proposal failed: {e}, falling back to rules")

    # Fallback to rule-based
    return propose_config_from_explain(explain_text, current_work_mem_mb)


# =========================================================================
# Leaderboard-Based Boost
# =========================================================================

def boost_from_leaderboard(
    benchmark_dir: Path,
    dsn: str,
    min_speedup: float = 1.05,
    dry_run: bool = False,
    use_llm: bool = False,
    query_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Run config boost on queries from leaderboard.json.

    Reads optimized SQL + EXPLAIN from leaderboard, proposes SET LOCAL
    configs, and benchmarks the 3-variant comparison.

    Args:
        benchmark_dir: Path to the benchmark directory
        dsn: PostgreSQL connection DSN
        min_speedup: Minimum rewrite speedup to attempt boost
        dry_run: If True, propose configs without benchmarking
        use_llm: If True, use LLM for config analysis instead of rules
        query_ids: If provided, only boost these query IDs

    Returns:
        List of result dicts, one per query attempted
    """
    from .pg_tuning import validate_tuning_config, build_set_local_sql

    benchmark_dir = Path(benchmark_dir)
    lb_path = benchmark_dir / "leaderboard.json"
    if not lb_path.exists():
        logger.error(f"No leaderboard.json in {benchmark_dir}")
        return []

    data = json.loads(lb_path.read_text())
    queries = data.get("queries", [])

    # Filter to requested query IDs
    if query_ids:
        query_id_set = set(query_ids)
        queries = [q for q in queries if q.get("query_id") in query_id_set]

    # Filter by speedup threshold
    queries = [q for q in queries if q.get("speedup", 0) >= min_speedup]

    results: List[Dict[str, Any]] = []
    for q in queries:
        query_id = q["query_id"]

        try:
            result = _boost_leaderboard_entry(
                q, benchmark_dir, dsn, min_speedup, dry_run, use_llm,
            )
            results.append(result)
        except Exception as e:
            logger.error(f"Config boost failed for {query_id}: {e}")
            results.append({
                "query_id": query_id,
                "status": "ERROR",
                "error": str(e),
            })

    return results


def _boost_leaderboard_entry(
    entry: Dict[str, Any],
    benchmark_dir: Path,
    dsn: str,
    min_speedup: float,
    dry_run: bool,
    use_llm: bool,
) -> Dict[str, Any]:
    """Process a single leaderboard entry for config boost."""
    from .pg_tuning import validate_tuning_config, build_set_local_sql

    query_id = entry["query_id"]

    # Load optimized SQL from leaderboard
    optimized_sql = entry.get("optimized_sql")
    if not optimized_sql:
        # Fallback: read from swarm_sessions
        sessions_dir = benchmark_dir / "swarm_sessions" / query_id
        worker_id = entry.get("best_worker_id")
        optimized_sql = _load_best_worker_sql(sessions_dir, worker_id) if sessions_dir.exists() else None

    if not optimized_sql:
        return {
            "query_id": query_id,
            "status": "SKIPPED",
            "reason": "No optimized SQL in leaderboard",
        }

    # Load original SQL
    original_sql = entry.get("original_sql")
    if not original_sql:
        sql_path = benchmark_dir / "queries" / f"{query_id}.sql"
        if sql_path.exists():
            original_sql = sql_path.read_text(errors="replace").strip()

    if not original_sql:
        return {
            "query_id": query_id,
            "status": "SKIPPED",
            "reason": "Could not find original SQL",
        }

    # Get EXPLAIN text: from leaderboard entry, session files, or run fresh
    explain_text = entry.get("explain_text") or entry.get("explain")
    if not explain_text:
        session_dir = benchmark_dir / "swarm_sessions" / query_id
        worker_id = entry.get("best_worker_id")
        if session_dir.exists():
            explain_text = _get_or_run_explain(session_dir, dsn, optimized_sql, worker_id)
        else:
            # Run fresh EXPLAIN
            try:
                from .execution.database_utils import run_explain_analyze
                from .prompts.analyst_briefing import format_pg_explain_tree
                result = run_explain_analyze(dsn, optimized_sql)
                if result:
                    explain_text = result.get("plan_text")
                    if not explain_text:
                        plan_json = result.get("plan_json")
                        if plan_json:
                            explain_text = format_pg_explain_tree(plan_json)
            except Exception as e:
                logger.warning(f"EXPLAIN failed for {query_id}: {e}")

    # Propose config
    if use_llm and explain_text:
        proposals = propose_config_from_llm(optimized_sql, explain_text, dsn)
    elif explain_text:
        proposals = propose_config_from_explain(explain_text)
    else:
        proposals = {}

    if not proposals:
        return {
            "query_id": query_id,
            "rewrite_speedup": entry.get("speedup"),
            "config_proposed": {},
            "status": "NO_RULES",
        }

    # Validate and build SET LOCAL commands
    raw_config = {k: v["value"] for k, v in proposals.items()}
    validated_config = validate_tuning_config(raw_config)
    config_commands = build_set_local_sql(validated_config)
    rules_fired = [v["rule"] for v in proposals.values()]
    reasons = {k: v["reason"] for k, v in proposals.items()}

    if dry_run:
        return {
            "query_id": query_id,
            "rewrite_speedup": entry.get("speedup"),
            "config_proposed": validated_config,
            "config_commands": config_commands,
            "rules_fired": rules_fired,
            "reasons": reasons,
            "status": "DRY_RUN",
        }

    # Benchmark 3 variants
    benchmark_result = _run_benchmark(dsn, original_sql, optimized_sql, config_commands)

    if "error" in benchmark_result:
        return {
            "query_id": query_id,
            "rewrite_speedup": entry.get("speedup"),
            "config_proposed": validated_config,
            "config_commands": config_commands,
            "rules_fired": rules_fired,
            "reasons": reasons,
            "benchmark_error": benchmark_result["error"],
            "status": "BENCHMARK_ERROR",
        }

    # Determine status
    config_additive = benchmark_result.get("config_additive", 1.0)
    if benchmark_result.get("best_variant") == "rewrite+config" and config_additive > 1.02:
        status = "BOOSTED"
    else:
        status = "NO_GAIN"

    return {
        "query_id": query_id,
        "rewrite_speedup": entry.get("speedup"),
        "config_proposed": validated_config,
        "config_commands": config_commands,
        "benchmark": benchmark_result,
        "rules_fired": rules_fired,
        "reasons": reasons,
        "status": status,
    }

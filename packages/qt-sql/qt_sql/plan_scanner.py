"""Plan-space scanner for PostgreSQL queries.

Toggles planner flags via SET LOCAL to discover the performance ceiling for
each query without any SQL rewriting. If disabling nested loops gives 2.75x
speedup, the LLM knows the optimizer is picking poorly and should rewrite SQL
to guide it toward hash joins.

Offline tool: run ahead of time, cache results to benchmark_dir/plan_scanner/.
During swarm: load cached results, inject into analyst prompt.

Usage:
  cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
  PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m qt_sql.plan_scanner \\
      packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76 [--query-ids q001 q002]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ── Flag combos (~17) ───────────────────────────────────────────────────

PLAN_SPACE_COMBOS: Dict[str, Dict[str, str]] = {
    # Single planner flag toggles
    "no_nestloop":     {"enable_nestloop": "off"},
    "no_hashjoin":     {"enable_hashjoin": "off"},
    "no_mergejoin":    {"enable_mergejoin": "off"},
    "no_seqscan":      {"enable_seqscan": "off"},

    # Force specific join type (disable other two)
    "force_hash":      {"enable_nestloop": "off", "enable_mergejoin": "off"},
    "force_merge":     {"enable_nestloop": "off", "enable_hashjoin": "off"},
    "force_nestloop":  {"enable_hashjoin": "off", "enable_mergejoin": "off"},

    # Memory variants
    "work_mem_256mb":  {"work_mem": "256MB"},
    "work_mem_1gb":    {"work_mem": "1GB"},
    "work_mem_2gb":    {"work_mem": "2GB"},

    # JIT toggle
    "no_jit":          {"jit": "off"},

    # Parallelism
    "no_parallel":     {"max_parallel_workers_per_gather": "0"},
    "max_parallel":    {"max_parallel_workers_per_gather": "8"},

    # Join reordering
    "no_reorder":      {"join_collapse_limit": "1"},
    "max_reorder":     {"join_collapse_limit": "20", "from_collapse_limit": "20"},

    # Cost model (SSD-tuned)
    "ssd_costs":       {"random_page_cost": "1.1", "effective_cache_size": "24GB"},

    # Kitchen sink: SSD + memory + hash multiplier
    "ssd_plus_mem":    {"random_page_cost": "1.1", "effective_cache_size": "24GB",
                        "work_mem": "256MB", "hash_mem_multiplier": "4"},

    # Compound combos (interaction effects — flags that individually help may
    # compound or cancel; test the interaction space)
    "jit_off_mem_256mb":    {"jit": "off", "work_mem": "256MB"},
    "jit_off_no_parallel":  {"jit": "off", "max_parallel_workers_per_gather": "0"},
    "mem_256mb_max_par":    {"work_mem": "256MB", "max_parallel_workers_per_gather": "8"},
    "no_reorder_mem_256mb": {"join_collapse_limit": "1", "work_mem": "256MB"},
    "ssd_no_jit":           {"random_page_cost": "1.1", "effective_cache_size": "24GB",
                             "jit": "off"},
}


# ── Schemas ─────────────────────────────────────────────────────────────

@dataclass
class ComboResult:
    combo_name: str
    config: Dict[str, str]
    set_local_commands: List[str]
    time_ms: float
    speedup: float
    top_plan_node: str
    row_count: int
    rows_match: bool
    error: Optional[str] = None


@dataclass
class ScanResult:
    query_id: str
    baseline_ms: float
    baseline_plan_node: str
    baseline_rows: int
    combos: List[ComboResult]
    ceiling_speedup: float
    ceiling_combo: str
    scanned_at: str

    def to_dict(self) -> dict:
        return {
            "query_id": self.query_id,
            "baseline_ms": round(self.baseline_ms, 1),
            "baseline_plan_node": self.baseline_plan_node,
            "baseline_rows": self.baseline_rows,
            "combos": [
                {
                    "combo_name": c.combo_name,
                    "config": c.config,
                    "set_local_commands": c.set_local_commands,
                    "time_ms": round(c.time_ms, 1),
                    "speedup": round(c.speedup, 3),
                    "top_plan_node": c.top_plan_node,
                    "row_count": c.row_count,
                    "rows_match": c.rows_match,
                    "error": c.error,
                }
                for c in self.combos
            ],
            "ceiling_speedup": round(self.ceiling_speedup, 3),
            "ceiling_combo": self.ceiling_combo,
            "scanned_at": self.scanned_at,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ScanResult":
        combos = [
            ComboResult(
                combo_name=c["combo_name"],
                config=c["config"],
                set_local_commands=c["set_local_commands"],
                time_ms=c["time_ms"],
                speedup=c["speedup"],
                top_plan_node=c["top_plan_node"],
                row_count=c["row_count"],
                rows_match=c["rows_match"],
                error=c.get("error"),
            )
            for c in d["combos"]
        ]
        return cls(
            query_id=d["query_id"],
            baseline_ms=d["baseline_ms"],
            baseline_plan_node=d["baseline_plan_node"],
            baseline_rows=d["baseline_rows"],
            combos=combos,
            ceiling_speedup=d["ceiling_speedup"],
            ceiling_combo=d["ceiling_combo"],
            scanned_at=d.get("scanned_at") or d.get("swept_at", ""),
        )


# ── Helpers ─────────────────────────────────────────────────────────────

def _build_set_local_cmds(config: Dict[str, str]) -> List[str]:
    """Build SET LOCAL commands from a config dict."""
    return [f"SET LOCAL {k} = '{v}'" for k, v in config.items()]


def _explain_plan(executor, sql: str, set_local_cmds: List[str]) -> tuple[str, float]:
    """Run EXPLAIN (FORMAT JSON, COSTS) with SET LOCAL.

    Returns (root_node_type, total_cost).
    """
    try:
        explain_sql = f"EXPLAIN (FORMAT JSON, COSTS) {sql}"
        rows = executor.execute_with_config(
            explain_sql, set_local_cmds, timeout_ms=30_000
        )
        if rows:
            plan_json = rows[0].get("QUERY PLAN", rows[0].get("query plan"))
            root_plan = {}
            if isinstance(plan_json, list) and plan_json:
                root_plan = plan_json[0].get("Plan", {})
            elif isinstance(plan_json, dict):
                root_plan = plan_json.get("Plan", {})
            node_type = root_plan.get("Node Type", "Unknown")
            total_cost = root_plan.get("Total Cost", 0.0)
            return node_type, total_cost
    except Exception as e:
        logger.debug(f"EXPLAIN failed: {e}")
    return "Unknown", 0.0


def _extract_top_plan_node(executor, sql: str, set_local_cmds: List[str]) -> str:
    """Run EXPLAIN (FORMAT JSON, COSTS) with SET LOCAL and return root node type."""
    node_type, _ = _explain_plan(executor, sql, set_local_cmds)
    return node_type


def _triage_measure(
    executor,
    sql: str,
    config_cmds: List[str],
    timeout_ms: int,
) -> tuple[float, float, int, int]:
    """4x triage: warmup orig, warmup config, measure orig, measure config.

    Returns (orig_ms, config_ms, orig_rows, config_rows).
    """
    # Warmup original
    rows_o = executor.execute(sql, timeout_ms=timeout_ms)

    # Warmup config
    rows_c = executor.execute_with_config(sql, config_cmds, timeout_ms=timeout_ms)

    # Measure original
    t0 = time.perf_counter()
    rows_o = executor.execute(sql, timeout_ms=timeout_ms)
    t_orig = (time.perf_counter() - t0) * 1000

    # Measure config
    t0 = time.perf_counter()
    rows_c = executor.execute_with_config(sql, config_cmds, timeout_ms=timeout_ms)
    t_config = (time.perf_counter() - t0) * 1000

    return t_orig, t_config, len(rows_o), len(rows_c)


def _baseline_measure(executor, sql: str, timeout_ms: int) -> tuple[float, int]:
    """3-run baseline: warmup + average of 2 measures."""
    # Warmup
    rows = executor.execute(sql, timeout_ms=timeout_ms)

    # Measure 1
    t0 = time.perf_counter()
    rows = executor.execute(sql, timeout_ms=timeout_ms)
    t1 = (time.perf_counter() - t0) * 1000

    # Measure 2
    t0 = time.perf_counter()
    rows = executor.execute(sql, timeout_ms=timeout_ms)
    t2 = (time.perf_counter() - t0) * 1000

    return (t1 + t2) / 2, len(rows)


# ── Core functions ──────────────────────────────────────────────────────

def scan_query(
    executor,
    sql: str,
    query_id: str = "unknown",
    timeout_ms: int = 120_000,
) -> ScanResult:
    """Scan one query across all PLAN_SPACE_COMBOS.

    For each combo:
      1. EXPLAIN (FORMAT JSON) with SET LOCAL -> capture plan shape (instant)
      2. 4x triage timing -> capture speedup

    Returns ScanResult with all combo results + ceiling.
    """
    # Baseline timing (3-run)
    baseline_ms, baseline_rows = _baseline_measure(executor, sql, timeout_ms)

    # Baseline plan node
    baseline_plan_node = _extract_top_plan_node(executor, sql, [])

    combo_results: List[ComboResult] = []

    for combo_name, config in PLAN_SPACE_COMBOS.items():
        set_local_cmds = _build_set_local_cmds(config)

        try:
            # Plan shape (instant)
            top_node = _extract_top_plan_node(executor, sql, set_local_cmds)

            # 4x triage timing
            t_orig, t_config, rc_o, rc_c = _triage_measure(
                executor, sql, set_local_cmds, timeout_ms
            )
            speedup = t_orig / t_config if t_config > 0 else 1.0
            rows_match = rc_o == rc_c

            combo_results.append(ComboResult(
                combo_name=combo_name,
                config=config,
                set_local_commands=set_local_cmds,
                time_ms=round(t_config, 1),
                speedup=round(speedup, 3),
                top_plan_node=top_node,
                row_count=rc_c,
                rows_match=rows_match,
            ))

        except Exception as e:
            err_str = str(e)[:200]
            logger.warning(f"[{query_id}] Combo {combo_name} failed: {err_str}")
            combo_results.append(ComboResult(
                combo_name=combo_name,
                config=config,
                set_local_commands=set_local_cmds,
                time_ms=0.0,
                speedup=0.0,
                top_plan_node="Error",
                row_count=0,
                rows_match=False,
                error=err_str,
            ))
            # Recover connection
            try:
                executor.rollback()
            except Exception:
                try:
                    executor.close()
                    executor.connect()
                except Exception:
                    pass

    # Find ceiling
    valid_combos = [c for c in combo_results if c.error is None and c.rows_match]
    if valid_combos:
        best = max(valid_combos, key=lambda c: c.speedup)
        ceiling_speedup = best.speedup
        ceiling_combo = best.combo_name
    else:
        ceiling_speedup = 1.0
        ceiling_combo = "baseline"

    return ScanResult(
        query_id=query_id,
        baseline_ms=round(baseline_ms, 1),
        baseline_plan_node=baseline_plan_node,
        baseline_rows=baseline_rows,
        combos=combo_results,
        ceiling_speedup=round(ceiling_speedup, 3),
        ceiling_combo=ceiling_combo,
        scanned_at=datetime.now().isoformat(),
    )


def scan_query_explain_only(
    executor,
    sql: str,
    query_id: str = "unknown",
) -> ScanResult:
    """Scan one query using EXPLAIN costs only (no execution).

    Near-instant: ~1-10ms per EXPLAIN × 17 combos ≈ <1 second per query.
    Uses planner cost estimates as proxy for wall-clock time.
    The 'time_ms' field stores estimated total cost (not ms).
    The 'speedup' field stores baseline_cost / combo_cost.
    """
    # Baseline plan
    baseline_node, baseline_cost = _explain_plan(executor, sql, [])

    combo_results: List[ComboResult] = []

    for combo_name, config in PLAN_SPACE_COMBOS.items():
        set_local_cmds = _build_set_local_cmds(config)

        try:
            top_node, combo_cost = _explain_plan(executor, sql, set_local_cmds)
            cost_speedup = baseline_cost / combo_cost if combo_cost > 0 else 1.0

            combo_results.append(ComboResult(
                combo_name=combo_name,
                config=config,
                set_local_commands=set_local_cmds,
                time_ms=round(combo_cost, 1),  # cost units, not ms
                speedup=round(cost_speedup, 3),
                top_plan_node=top_node,
                row_count=0,  # unknown without execution
                rows_match=True,  # planner flags don't change results
            ))
        except Exception as e:
            err_str = str(e)[:200]
            combo_results.append(ComboResult(
                combo_name=combo_name,
                config=config,
                set_local_commands=set_local_cmds,
                time_ms=0.0,
                speedup=0.0,
                top_plan_node="Error",
                row_count=0,
                rows_match=False,
                error=err_str,
            ))
            try:
                executor.rollback()
            except Exception:
                pass

    # Find ceiling
    valid = [c for c in combo_results if c.error is None and c.speedup > 0]
    if valid:
        best = max(valid, key=lambda c: c.speedup)
        ceiling_speedup = best.speedup
        ceiling_combo = best.combo_name
    else:
        ceiling_speedup = 1.0
        ceiling_combo = "baseline"

    return ScanResult(
        query_id=query_id,
        baseline_ms=round(baseline_cost, 1),  # cost units, not ms
        baseline_plan_node=baseline_node,
        baseline_rows=0,
        combos=combo_results,
        ceiling_speedup=round(ceiling_speedup, 3),
        ceiling_combo=ceiling_combo,
        scanned_at=datetime.now().isoformat(),
    )


def scan_corpus_explain_only(
    benchmark_dir: Path,
    query_ids: Optional[List[str]] = None,
) -> List[ScanResult]:
    """Scan all queries using EXPLAIN costs only. ~30s for 76 queries."""
    from .schemas import BenchmarkConfig
    from .execution.factory import create_executor_from_dsn

    benchmark_dir = Path(benchmark_dir)
    config = BenchmarkConfig.from_file(benchmark_dir / "config.json")

    if config.engine not in ("postgresql", "postgres"):
        print(f"  ERROR: Plan scanner is PostgreSQL-only (got engine={config.engine})")
        return []

    executor = create_executor_from_dsn(config.db_path_or_dsn)
    executor.connect()

    # Fresh statistics
    print("  ANALYZE: Refreshing table statistics...", flush=True)
    t0 = time.time()
    try:
        executor.execute("ANALYZE")
    except Exception as e:
        print(f"  ANALYZE failed (non-fatal): {e}")
    print(f"  ANALYZE: done ({time.time() - t0:.1f}s)", flush=True)

    query_dir = benchmark_dir / "queries"
    if not query_dir.exists():
        print(f"  ERROR: No queries/ directory in {benchmark_dir}")
        executor.close()
        return []

    query_files = sorted(query_dir.glob("*.sql"))
    if query_ids:
        query_set = set(query_ids)
        query_files = [f for f in query_files if f.stem in query_set]

    scan_dir = benchmark_dir / "plan_scanner"
    scan_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*70}")
    print(f"  PLAN-SPACE SWEEP (EXPLAIN-only, cost-based)")
    print(f"  Queries: {len(query_files)}, Combos: {len(PLAN_SPACE_COMBOS)}")
    print(f"{'='*70}\n")

    print(f"  {'Query':20s}  {'Base Cost':>12s}  {'Ceiling':>8s}  {'Best Combo':25s}  {'Plan':15s}")
    print(f"  {'-'*20}  {'-'*12}  {'-'*8}  {'-'*25}  {'-'*15}")

    results: List[ScanResult] = []
    t_start = time.time()

    for qf in query_files:
        qid = qf.stem
        sql = qf.read_text().strip()

        try:
            result = scan_query_explain_only(executor, sql, query_id=qid)
            results.append(result)

            # Save immediately
            out_path = scan_dir / f"{qid}.json"
            out_path.write_text(json.dumps(result.to_dict(), indent=2))

            tag = ""
            if result.ceiling_speedup >= 1.50:
                tag = " ***"
            elif result.ceiling_speedup >= 1.10:
                tag = " *"
            print(
                f"  {qid:20s}  {result.baseline_ms:12.0f}  "
                f"{result.ceiling_speedup:7.2f}x  {result.ceiling_combo:25s}  "
                f"{result.baseline_plan_node:15s}{tag}",
                flush=True,
            )
        except Exception as e:
            print(f"  {qid:20s}  ERROR: {str(e)[:60]}", flush=True)
            try:
                executor.rollback()
            except Exception:
                try:
                    executor.close()
                    executor.connect()
                except Exception:
                    pass

    elapsed = time.time() - t_start
    print(f"\n  Swept {len(results)} queries in {elapsed:.1f}s")

    if results:
        _save_summary(scan_dir, results)
        _print_summary(results)

    executor.close()
    return results


def validate_correlation(benchmark_dir: Path) -> None:
    """Compare EXPLAIN-only cost speedups vs real timing speedups.

    Loads existing timed results from plan_scanner/, runs EXPLAIN-only on the
    same queries, then computes Pearson r across all combo speedups.
    """
    from .schemas import BenchmarkConfig
    from .execution.factory import create_executor_from_dsn

    benchmark_dir = Path(benchmark_dir)
    scan_dir = benchmark_dir / "plan_scanner"

    # Load timed results
    timed_files = sorted(scan_dir.glob("*.json"))
    timed_results = {}
    for f in timed_files:
        if f.name == "summary.json":
            continue
        try:
            sr = ScanResult.from_dict(json.loads(f.read_text()))
            # Skip if baseline_ms looks like cost units (no timing data)
            # Real timing: baseline_ms typically 100-30000
            # Cost units: typically 10000-1000000
            if sr.baseline_rows == 0 and sr.baseline_ms > 50000:
                continue  # likely explain-only result
            timed_results[sr.query_id] = sr
        except Exception:
            pass

    if len(timed_results) < 3:
        print(f"  Need at least 3 timed results for correlation. Found: {len(timed_results)}")
        return

    print(f"  Found {len(timed_results)} timed results for correlation check")

    # Run EXPLAIN-only on same queries
    config = BenchmarkConfig.from_file(benchmark_dir / "config.json")
    executor = create_executor_from_dsn(config.db_path_or_dsn)
    executor.connect()

    query_dir = benchmark_dir / "queries"
    cost_speedups: List[float] = []
    time_speedups: List[float] = []
    labels: List[str] = []

    for qid, timed_sr in sorted(timed_results.items()):
        sql_path = query_dir / f"{qid}.sql"
        if not sql_path.exists():
            continue

        sql = sql_path.read_text().strip()
        explain_sr = scan_query_explain_only(executor, sql, query_id=qid)

        # Match each combo between timed and explain results
        timed_by_name = {c.combo_name: c for c in timed_sr.combos if c.error is None}
        explain_by_name = {c.combo_name: c for c in explain_sr.combos if c.error is None}

        for combo_name in timed_by_name:
            if combo_name not in explain_by_name:
                continue
            ts = timed_by_name[combo_name].speedup
            cs = explain_by_name[combo_name].speedup
            if ts > 0 and cs > 0:
                time_speedups.append(ts)
                cost_speedups.append(cs)
                labels.append(f"{qid}/{combo_name}")

    executor.close()

    if len(time_speedups) < 5:
        print(f"  Not enough matched combos ({len(time_speedups)}). Need at least 5.")
        return

    # Pearson correlation (no numpy needed)
    n = len(time_speedups)
    mean_t = sum(time_speedups) / n
    mean_c = sum(cost_speedups) / n
    cov = sum((t - mean_t) * (c - mean_c) for t, c in zip(time_speedups, cost_speedups)) / n
    std_t = (sum((t - mean_t) ** 2 for t in time_speedups) / n) ** 0.5
    std_c = (sum((c - mean_c) ** 2 for c in cost_speedups) / n) ** 0.5
    r = cov / (std_t * std_c) if std_t > 0 and std_c > 0 else 0.0

    print(f"\n{'='*70}")
    print(f"  CORRELATION: EXPLAIN cost speedup vs wall-clock speedup")
    print(f"  Matched data points: {n} (across {len(timed_results)} queries × 17 combos)")
    print(f"  Pearson r = {r:.4f}")
    if r >= 0.80:
        print(f"  PASS: r >= 0.80 — EXPLAIN costs are a reliable proxy")
    else:
        print(f"  FAIL: r < 0.80 — EXPLAIN costs may not be reliable")
    print(f"{'='*70}\n")

    # Show outliers (biggest disagreements)
    diffs = [(abs(t - c), t, c, lbl) for t, c, lbl in zip(time_speedups, cost_speedups, labels)]
    diffs.sort(reverse=True)
    if diffs:
        print(f"  Top disagreements (|time_speedup - cost_speedup|):")
        print(f"  {'Label':40s}  {'Time':>8s}  {'Cost':>8s}  {'Delta':>8s}")
        for diff, ts, cs, lbl in diffs[:10]:
            print(f"  {lbl:40s}  {ts:7.3f}x  {cs:7.3f}x  {diff:7.3f}")
    print()


# ── Plan space exploration (EXPLAIN-only intelligence gathering) ─────

def _get_full_plan(executor, sql: str, set_local_cmds: List[str]) -> Optional[dict]:
    """Run EXPLAIN (FORMAT JSON, COSTS) and return the full plan JSON."""
    try:
        explain_sql = f"EXPLAIN (FORMAT JSON, COSTS) {sql}"
        rows = executor.execute_with_config(
            explain_sql, set_local_cmds, timeout_ms=30_000
        )
        if rows:
            plan_json = rows[0].get("QUERY PLAN", rows[0].get("query plan"))
            if isinstance(plan_json, list) and plan_json:
                return plan_json[0].get("Plan")
            if isinstance(plan_json, dict):
                return plan_json.get("Plan")
    except Exception as e:
        logger.debug(f"EXPLAIN failed: {e}")
    return None


def _plan_fingerprint(node: dict) -> str:
    """Structural fingerprint of a plan tree. Captures operator types,
    tables, join types, index usage — everything that defines the plan shape.
    Order-preserving (left/right child order matters for join sides)."""
    parts = [node.get("Node Type", "?")]
    rel = node.get("Relation Name")
    if rel:
        parts.append(rel)
    jt = node.get("Join Type")
    if jt:
        parts.append(jt)
    idx = node.get("Index Name")
    if idx:
        parts.append(idx)
    wp = node.get("Workers Planned", 0)
    if wp:
        parts.append(f"par{wp}")
    child_fps = [_plan_fingerprint(c) for c in node.get("Plans", [])]
    return "(" + "|".join(parts) + ")" + "".join(child_fps)


def _extract_plan_intel(node: dict, depth: int = 0) -> dict:
    """Walk plan tree and extract all structural intelligence."""
    intel = {
        "node_type": node.get("Node Type", ""),
        "depth": depth,
        "relation": node.get("Relation Name"),
        "alias": node.get("Alias"),
        "join_type": node.get("Join Type"),
        "index_name": node.get("Index Name"),
        "scan_direction": node.get("Scan Direction"),
        "hash_cond": node.get("Hash Cond"),
        "merge_cond": node.get("Merge Cond"),
        "join_filter": node.get("Join Filter"),
        "filter": node.get("Filter"),
        "index_cond": node.get("Index Cond"),
        "sort_key": node.get("Sort Key"),
        "workers_planned": node.get("Workers Planned", 0),
        "plan_rows": node.get("Plan Rows", 0),
        "total_cost": node.get("Total Cost", 0),
        "startup_cost": node.get("Startup Cost", 0),
    }
    intel["children"] = [
        _extract_plan_intel(c, depth + 1) for c in node.get("Plans", [])
    ]
    return intel


def _collect_join_types(node: dict) -> List[str]:
    """Collect all join types in DFS order."""
    joins = []
    nt = node.get("Node Type", "")
    if "Join" in nt or "Loop" in nt:
        jt = node.get("Join Type", "")
        joins.append(f"{nt}({jt})" if jt else nt)
    for c in node.get("Plans", []):
        joins.extend(_collect_join_types(c))
    return joins


def _collect_scan_types(node: dict) -> List[str]:
    """Collect all scan types with table names in DFS order."""
    scans = []
    nt = node.get("Node Type", "")
    if "Scan" in nt:
        rel = node.get("Relation Name", "?")
        idx = node.get("Index Name")
        if idx:
            scans.append(f"{nt}({rel},{idx})")
        else:
            scans.append(f"{nt}({rel})")
    for c in node.get("Plans", []):
        scans.extend(_collect_scan_types(c))
    return scans


def _collect_table_access_order(node: dict) -> List[str]:
    """DFS order of base table accesses (join order signal)."""
    tables = []
    rel = node.get("Relation Name")
    if rel:
        tables.append(rel)
    for c in node.get("Plans", []):
        tables.extend(_collect_table_access_order(c))
    return tables


def _collect_scan_counts(node: dict) -> Dict[str, int]:
    """Count how many times each base table is scanned in the plan tree.

    Multiple scans of the same table (self-joins, UNION branches) indicate
    consolidation opportunities. Returns {table_name: scan_count}.
    """
    counts: Dict[str, int] = {}
    nt = node.get("Node Type", "")
    rel = node.get("Relation Name")
    if "Scan" in nt and rel:
        counts[rel] = counts.get(rel, 0) + 1
    for child in node.get("Plans", []):
        for table, count in _collect_scan_counts(child).items():
            counts[table] = counts.get(table, 0) + count
    return counts


def _collect_predicate_placement(node: dict) -> List[dict]:
    """Walk plan tree and classify each predicate as EARLY or LATE.

    EARLY:     Index Cond — applied at scan time, uses index (good)
    LATE:      Filter — applied post-scan or post-join (pushdown opportunity)
    JOIN_EQUI: Hash Cond / Merge Cond — equi-join condition
    JOIN_LATE: Join Filter — non-equi join predicate, evaluated per-tuple (expensive)
    """
    placements: List[dict] = []
    nt = node.get("Node Type", "")
    rel = node.get("Relation Name", "")
    alias = node.get("Alias", rel)

    # Index conditions (EARLY — good)
    idx_cond = node.get("Index Cond")
    if idx_cond:
        placements.append({
            "predicate": idx_cond,
            "table": alias or rel,
            "placement": "EARLY",
            "node_type": nt,
        })

    # Filters (LATE — potential pushdown opportunity)
    filt = node.get("Filter")
    if filt:
        placements.append({
            "predicate": filt,
            "table": alias or rel,
            "placement": "LATE",
            "node_type": nt,
        })

    # Join conditions
    for cond_key, placement in [
        ("Hash Cond", "JOIN_EQUI"),
        ("Merge Cond", "JOIN_EQUI"),
        ("Join Filter", "JOIN_LATE"),
    ]:
        cond = node.get(cond_key)
        if cond:
            placements.append({
                "predicate": cond,
                "table": "",
                "placement": placement,
                "node_type": nt,
            })

    for child in node.get("Plans", []):
        placements.extend(_collect_predicate_placement(child))

    return placements


def _find_bottleneck_joins(node: dict, depth: int = 0) -> List[dict]:
    """Find join nodes with their input characteristics.

    Returns list sorted by total_cost descending. Each entry has:
    join_type, condition, input tables/rows, whether non-equi.
    The highest-cost join is the bottleneck the LLM should target.
    """
    joins: List[dict] = []
    nt = node.get("Node Type", "")

    if "Join" in nt or "Loop" in nt:
        children = node.get("Plans", [])
        left_rows = children[0].get("Plan Rows", 0) if len(children) > 0 else 0
        right_rows = children[1].get("Plan Rows", 0) if len(children) > 1 else 0

        # Tables on each side of the join
        left_tables = _collect_table_access_order(children[0]) if children else []
        right_tables = _collect_table_access_order(children[1]) if len(children) > 1 else []

        # Join condition
        condition = (
            node.get("Hash Cond") or
            node.get("Merge Cond") or
            node.get("Join Filter") or
            node.get("Index Cond") or
            ""
        )

        # Non-equi: has Join Filter but no Hash/Merge Cond
        is_non_equi = (
            bool(node.get("Join Filter"))
            and not node.get("Hash Cond")
            and not node.get("Merge Cond")
        )

        joins.append({
            "join_type": nt,
            "join_subtype": node.get("Join Type", ""),
            "condition": condition,
            "is_non_equi": is_non_equi,
            "total_cost": node.get("Total Cost", 0),
            "plan_rows": node.get("Plan Rows", 0),
            "left_rows": left_rows,
            "right_rows": right_rows,
            "left_tables": left_tables,
            "right_tables": right_tables,
            "depth": depth,
        })

    for child in node.get("Plans", []):
        joins.extend(_find_bottleneck_joins(child, depth + 1))

    joins.sort(key=lambda j: j["total_cost"], reverse=True)
    return joins


def _assess_confidence(baseline_ms: float, ceiling_speedup: float) -> tuple:
    """Assess measurement confidence based on baseline speed.

    Fast queries have high noise-to-signal ratios. Returns (level, explanation).
    """
    NOISE_FLOOR_MS = 15.0  # PG overhead: connection + buffer cache variance

    if baseline_ms <= 0:
        return "UNKNOWN", "no baseline measurement"

    # Absolute improvement in ms
    improvement_ms = (
        baseline_ms - (baseline_ms / ceiling_speedup)
        if ceiling_speedup > 1.0 else 0
    )

    if baseline_ms < 50:
        return "LOW", (
            f"baseline {baseline_ms:.0f}ms < 50ms, noise floor ~{NOISE_FLOOR_MS:.0f}ms "
            f"— measurement unreliable"
        )
    elif baseline_ms < 200 and improvement_ms < NOISE_FLOOR_MS * 2:
        return "LOW", (
            f"baseline {baseline_ms:.0f}ms, ceiling improvement ~{improvement_ms:.0f}ms "
            f"≈ noise floor"
        )
    elif baseline_ms < 500:
        return "MEDIUM", f"baseline {baseline_ms:.0f}ms — moderate confidence"
    else:
        return "HIGH", f"baseline {baseline_ms:.0f}ms — high confidence"


def _format_bottleneck_joins(bottleneck_joins: List[dict]) -> str:
    """Format bottleneck join sub-signals for prompt injection."""
    if not bottleneck_joins:
        return ""

    lines: List[str] = []
    # Show top bottleneck join
    top = bottleneck_joins[0]
    left_str = " × ".join(top["left_tables"][:3]) or "?"
    right_str = " × ".join(top["right_tables"][:3]) or "?"

    jtype = top["join_type"]
    if top["join_subtype"]:
        jtype += f"({top['join_subtype']})"
    equi_tag = ", non-equi" if top["is_non_equi"] else ""

    lines.append(
        f"  BOTTLENECK_JOIN: {left_str} × {right_str} "
        f"({jtype}{equi_tag})"
    )

    # Format row estimates with K/M suffixes
    def _fmt_rows(n: int) -> str:
        if n >= 1_000_000:
            return f"{n/1_000_000:.1f}M"
        elif n >= 1_000:
            return f"{n/1_000:.0f}K"
        return str(n)

    lines.append(
        f"  INPUT_SIZES: {_fmt_rows(top['left_rows'])} × {_fmt_rows(top['right_rows'])}"
    )

    # Reduction guidance based on join type
    if "Nested" in top["join_type"] or "Loop" in top["join_type"]:
        inner_side = right_str
        lines.append(
            f"  REDUCTION_OPPORTUNITY: pre-filter {inner_side} to shrink "
            f"nested-loop input"
        )
    elif "Hash" in top["join_type"]:
        build_side = right_str
        lines.append(
            f"  REDUCTION_OPPORTUNITY: reduce {build_side} (hash build side) "
            f"cardinality"
        )

    return "\n".join(lines)


def _format_scan_counts(scan_counts: Dict[str, int]) -> str:
    """Format scan counts with redundancy detection."""
    if not scan_counts:
        return ""

    lines = ["SCAN_COUNTS:"]
    for table, count in sorted(scan_counts.items(), key=lambda x: -x[1]):
        suffix = " scan" if count == 1 else " scans"
        lines.append(f"  {table}: {count}{suffix}")

    # Detect redundant scan opportunities
    redundant = {t: c for t, c in scan_counts.items() if c > 1}
    if redundant:
        targets = ", ".join(
            f"{t} ({c}x)" for t, c in
            sorted(redundant.items(), key=lambda x: -x[1])
        )
        lines.append(f"REDUNDANT_SCAN_OPPORTUNITY: {targets} — consolidate into single CTE")
    else:
        lines.append("REDUNDANT_SCAN_OPPORTUNITY: none")

    return "\n".join(lines)


def _format_predicate_placement(placements: List[dict]) -> str:
    """Format predicate placement audit with pushdown opportunities."""
    if not placements:
        return ""

    late_preds = [p for p in placements if p["placement"] == "LATE"]
    early_preds = [p for p in placements if p["placement"] == "EARLY"]
    join_late = [p for p in placements if p["placement"] == "JOIN_LATE"]

    if not late_preds and not join_late:
        return ""

    lines = ["PREDICATE_PLACEMENT:"]
    for p in early_preds[:5]:
        lines.append(f"  {p['predicate']}: Index Cond on {p['table']} (EARLY)")
    for p in late_preds[:8]:
        lines.append(f"  {p['predicate']}: Filter on {p['table']} (LATE)")
    for p in join_late[:3]:
        lines.append(f"  {p['predicate']}: Join Filter (LATE — per-tuple eval)")

    # Pushdown opportunities
    if late_preds:
        lines.append("")
        lines.append("PUSHDOWN_OPPORTUNITIES:")
        for p in late_preds[:5]:
            lines.append(
                f"  {p['table']}: {p['predicate']} applied post-join. "
                f"Pre-filter via CTE to reduce join input."
            )

    return "\n".join(lines)


def _compose_strategy(
    join_class: str,
    memory_class: str,
    join_detail: str,
    bottleneck_joins: List[dict],
) -> str:
    """Compose a decision-tree strategy from multi-dimensional signals.

    Instead of independent per-dimension rules that can contradict, this
    produces a single coherent recommendation.
    """
    lines = ["STRATEGY:"]

    has_nestloop_bottleneck = any(
        "Nested" in j.get("join_type", "") or "Loop" in j.get("join_type", "")
        for j in bottleneck_joins[:1]
    ) if bottleneck_joins else ("nested" in join_detail.lower())

    has_hash_bottleneck = any(
        "Hash" in j.get("join_type", "")
        for j in bottleneck_joins[:1]
    ) if bottleneck_joins else ("hash" in join_detail.lower())

    if "LOCKED" in join_class:
        if has_nestloop_bottleneck:
            lines.append("  JOINS=LOCKED (nested loop) →")
            if "HIGH" in memory_class:
                lines.append(
                    "    Reduce nested-loop inner side via pre-filtering CTE."
                )
                lines.append(
                    "    work_mem helps downstream sorts/aggs, NOT the nested loop."
                )
            else:
                lines.append(
                    "    Reduce nested-loop inner side via pre-filtering CTE."
                )
                lines.append(
                    "    Memory is fine — focus purely on cardinality reduction."
                )
        elif has_hash_bottleneck:
            lines.append("  JOINS=LOCKED (hash) →")
            if "HIGH" in memory_class:
                lines.append(
                    "    CONFIG work_mem handles spill. Focus SQL rewrite on "
                    "reducing hash build side cardinality."
                )
            else:
                lines.append(
                    "    Reduce hash build side cardinality (smaller inner relation)."
                )
        else:
            lines.append("  JOINS=LOCKED →")
            lines.append(
                "    Do NOT restructure join types. Reduce intermediate cardinality."
            )

    elif "SENSITIVE" in join_class:
        if has_hash_bottleneck or "hash" in join_class.lower():
            lines.append("  JOINS=SENSITIVE (toward hash join) →")
            lines.append(
                "    Enlarge inner side or add equi-join conditions to "
                "guide optimizer toward hash join."
            )
            if "HIGH" in memory_class:
                lines.append("    CONFIG work_mem already handles spill.")
        else:
            lines.append("  JOINS=SENSITIVE →")
            lines.append(
                "    Rewrite SQL to guide optimizer toward the faster join strategy."
            )
            lines.append(
                "    Add selective predicates or restructure join conditions."
            )
    else:
        # Stable joins
        lines.append("  JOINS=STABLE →")
        lines.append(
            "    Join methods are optimal. Focus on cardinality reduction, "
            "redundant scan elimination, or predicate pushdown."
        )

    return "\n".join(lines)


def _diff_plans(baseline: dict, combo: dict) -> dict:
    """Compare two plan trees and identify structural differences."""
    b_joins = _collect_join_types(baseline)
    c_joins = _collect_join_types(combo)
    b_scans = _collect_scan_types(baseline)
    c_scans = _collect_scan_types(combo)
    b_tables = _collect_table_access_order(baseline)
    c_tables = _collect_table_access_order(combo)
    b_par = baseline.get("Workers Planned", 0) or 0
    c_par = combo.get("Workers Planned", 0) or 0

    return {
        "join_types_changed": b_joins != c_joins,
        "baseline_joins": b_joins,
        "combo_joins": c_joins,
        "scan_types_changed": b_scans != c_scans,
        "baseline_scans": b_scans,
        "combo_scans": c_scans,
        "join_order_changed": b_tables != c_tables,
        "baseline_table_order": b_tables,
        "combo_table_order": c_tables,
        "parallelism_changed": b_par != c_par,
        "baseline_parallel": b_par,
        "combo_parallel": c_par,
    }


def _classify_vulnerabilities(baseline_plan: dict, combo_plans: Dict[str, dict]) -> List[dict]:
    """Classify optimizer vulnerabilities from plan differences."""
    vulns = []
    baseline_fp = _plan_fingerprint(baseline_plan)
    baseline_joins = _collect_join_types(baseline_plan)
    baseline_scans = _collect_scan_types(baseline_plan)
    baseline_tables = _collect_table_access_order(baseline_plan)
    baseline_cost = baseline_plan.get("Total Cost", 0)

    # Group combos by what they change
    join_changers = []
    scan_changers = []
    order_changers = []
    parallel_changers = []
    cost_reducers = []

    for combo_name, plan in combo_plans.items():
        if plan is None:
            continue
        fp = _plan_fingerprint(plan)
        if fp == baseline_fp:
            # Plan identical — check cost difference only
            cost = plan.get("Total Cost", 0)
            if baseline_cost > 0 and cost > 0 and baseline_cost / cost > 1.2:
                cost_reducers.append((combo_name, baseline_cost / cost))
            continue

        diff = _diff_plans(baseline_plan, plan)
        if diff["join_types_changed"]:
            join_changers.append((combo_name, diff))
        if diff["scan_types_changed"]:
            scan_changers.append((combo_name, diff))
        if diff["join_order_changed"]:
            order_changers.append((combo_name, diff))
        if diff["parallelism_changed"]:
            parallel_changers.append((combo_name, diff))

    # Classify
    if join_changers:
        # Find what join types are being swapped
        examples = []
        for name, diff in join_changers[:3]:
            removed = set(diff["baseline_joins"]) - set(diff["combo_joins"])
            added = set(diff["combo_joins"]) - set(diff["baseline_joins"])
            examples.append(f"{name}: {list(removed)} → {list(added)}")
        vulns.append({
            "type": "JOIN_TYPE_TRAP",
            "combos": [c[0] for c in join_changers],
            "description": f"Optimizer join type choice is vulnerable. "
                           f"{len(join_changers)} combos change join types.",
            "detail": examples,
        })

    if order_changers:
        examples = []
        for name, diff in order_changers[:2]:
            examples.append(
                f"{name}: [{', '.join(diff['baseline_table_order'][:6])}] → "
                f"[{', '.join(diff['combo_table_order'][:6])}]"
            )
        vulns.append({
            "type": "JOIN_ORDER_TRAP",
            "combos": [c[0] for c in order_changers],
            "description": f"Join enumeration is unstable. "
                           f"{len(order_changers)} combos change table access order.",
            "detail": examples,
        })

    if scan_changers:
        examples = []
        for name, diff in scan_changers[:3]:
            removed = set(diff["baseline_scans"]) - set(diff["combo_scans"])
            added = set(diff["combo_scans"]) - set(diff["baseline_scans"])
            examples.append(f"{name}: {list(removed)} → {list(added)}")
        vulns.append({
            "type": "SCAN_TYPE_TRAP",
            "combos": [c[0] for c in scan_changers],
            "description": f"Scan type choice is vulnerable. "
                           f"{len(scan_changers)} combos change scan types.",
            "detail": examples,
        })

    if parallel_changers:
        vulns.append({
            "type": "PARALLELISM_GAP",
            "combos": [c[0] for c in parallel_changers],
            "description": f"Parallelism changes with {len(parallel_changers)} combos.",
            "detail": [
                f"{name}: parallel {diff['baseline_parallel']} → {diff['combo_parallel']}"
                for name, diff in parallel_changers[:3]
            ],
        })

    # Memory sensitivity: work_mem combos that change plan structure
    mem_combos = {"work_mem_256mb", "work_mem_1gb", "work_mem_2gb", "ssd_plus_mem"}
    mem_changers = [
        (name, diff) for name, diff in join_changers + scan_changers
        if name in mem_combos
    ]
    if mem_changers:
        vulns.append({
            "type": "MEMORY_SENSITIVITY",
            "combos": [c[0] for c in mem_changers],
            "description": "Plan structure changes with memory settings — "
                           "likely hash/sort spill in baseline.",
            "detail": [c[0] for c in mem_changers],
        })

    # No structural changes at all
    if not vulns:
        if cost_reducers:
            vulns.append({
                "type": "COST_MODEL_ONLY",
                "combos": [c[0] for c in cost_reducers],
                "description": f"No plan structure changes. {len(cost_reducers)} combos "
                               f"only change cost estimates (need wall-clock to verify).",
                "detail": [f"{name}: {ratio:.2f}x cost reduction" for name, ratio in cost_reducers[:5]],
            })
        else:
            vulns.append({
                "type": "PLAN_LOCKED",
                "combos": [],
                "description": "Optimizer produces the same plan regardless of flags. "
                               "Plan is robust — speedup must come from SQL rewriting.",
                "detail": [],
            })

    return vulns


def explore_plan_space(
    executor,
    sql: str,
    query_id: str = "unknown",
) -> dict:
    """Full plan space exploration for one query.

    Phase 1: Single flag scan (17 EXPLAINs)
    Phase 2: Pairwise combos of plan-changing flags
    Phase 3: Classify vulnerabilities

    Returns rich intelligence dict.
    """
    from itertools import combinations

    # Baseline
    baseline_plan = _get_full_plan(executor, sql, [])
    if baseline_plan is None:
        return {"query_id": query_id, "error": "baseline EXPLAIN failed"}

    baseline_fp = _plan_fingerprint(baseline_plan)
    baseline_cost = baseline_plan.get("Total Cost", 0)

    # Phase 1: Single flags
    single_results = {}
    plan_changers = []  # flags that produce a different plan
    fingerprints = {baseline_fp: {"combos": ["baseline"], "cost": baseline_cost}}

    for combo_name, config in PLAN_SPACE_COMBOS.items():
        cmds = _build_set_local_cmds(config)
        plan = _get_full_plan(executor, sql, cmds)
        if plan is None:
            single_results[combo_name] = {"error": "EXPLAIN failed"}
            continue

        fp = _plan_fingerprint(plan)
        cost = plan.get("Total Cost", 0)
        single_results[combo_name] = {
            "plan": plan,
            "fingerprint": fp,
            "cost": cost,
            "plan_changed": fp != baseline_fp,
        }

        if fp not in fingerprints:
            fingerprints[fp] = {"combos": [combo_name], "cost": cost}
            plan_changers.append(combo_name)
        else:
            fingerprints[fp]["combos"].append(combo_name)

    # Phase 2: Pairwise combos of plan-changing flags
    pair_results = {}
    if len(plan_changers) >= 2:
        for a, b in combinations(plan_changers, 2):
            pair_name = f"{a}+{b}"
            merged_config = {**PLAN_SPACE_COMBOS[a], **PLAN_SPACE_COMBOS[b]}
            cmds = _build_set_local_cmds(merged_config)

            try:
                plan = _get_full_plan(executor, sql, cmds)
                if plan is None:
                    continue
                fp = _plan_fingerprint(plan)
                cost = plan.get("Total Cost", 0)
                pair_results[pair_name] = {
                    "plan": plan,
                    "fingerprint": fp,
                    "cost": cost,
                    "novel": fp not in fingerprints,
                }
                if fp not in fingerprints:
                    fingerprints[fp] = {"combos": [pair_name], "cost": cost}
                else:
                    fingerprints[fp]["combos"].append(pair_name)
            except Exception:
                try:
                    executor.rollback()
                except Exception:
                    pass

    # Collect all combo plans for vulnerability classification
    combo_plans = {}
    for name, data in single_results.items():
        if "plan" in data:
            combo_plans[name] = data["plan"]
    for name, data in pair_results.items():
        if "plan" in data:
            combo_plans[name] = data["plan"]

    # Classify vulnerabilities
    vulns = _classify_vulnerabilities(baseline_plan, combo_plans)

    # Build result
    distinct_plans = []
    for fp, info in fingerprints.items():
        is_baseline = "baseline" in info["combos"]
        distinct_plans.append({
            "fingerprint_hash": hash(fp) & 0xFFFFFFFF,  # short hash for display
            "combos": info["combos"],
            "cost": info["cost"],
            "is_baseline": is_baseline,
            "cost_ratio": baseline_cost / info["cost"] if info["cost"] > 0 else 1.0,
        })

    # Extract enriched plan intelligence from baseline plan
    scan_counts = _collect_scan_counts(baseline_plan)
    predicate_placement = _collect_predicate_placement(baseline_plan)
    bottleneck_joins_raw = _find_bottleneck_joins(baseline_plan)

    return {
        "query_id": query_id,
        "baseline_cost": baseline_cost,
        "baseline_plan_node": baseline_plan.get("Node Type", "?"),
        "baseline_joins": _collect_join_types(baseline_plan),
        "baseline_scans": _collect_scan_types(baseline_plan),
        "baseline_table_order": _collect_table_access_order(baseline_plan),
        "n_explains": len(single_results) + len(pair_results) + 1,
        "n_distinct_plans": len(distinct_plans),
        "n_plan_changers": len(plan_changers),
        "plan_changers": plan_changers,
        "distinct_plans": distinct_plans,
        "vulnerabilities": vulns,
        "pair_results_novel": [
            name for name, data in pair_results.items() if data.get("novel")
        ],
        # Enriched plan intelligence (Issue 2, 3 from scanner review)
        "scan_counts": scan_counts,
        "predicate_placement": predicate_placement,
        "bottleneck_joins": bottleneck_joins_raw[:5],
        "explored_at": datetime.now().isoformat(),
    }


def explore_corpus(
    benchmark_dir: Path,
    query_ids: Optional[List[str]] = None,
) -> List[dict]:
    """Explore plan space for all queries. ANALYZE first, then ~1s per query."""
    from .schemas import BenchmarkConfig
    from .execution.factory import create_executor_from_dsn

    benchmark_dir = Path(benchmark_dir)
    config = BenchmarkConfig.from_file(benchmark_dir / "config.json")

    if config.engine not in ("postgresql", "postgres"):
        print(f"  ERROR: Plan scanner is PostgreSQL-only (got engine={config.engine})")
        return []

    executor = create_executor_from_dsn(config.db_path_or_dsn)
    executor.connect()

    # Fresh statistics
    print("  ANALYZE: Refreshing table statistics...", flush=True)
    t0 = time.time()
    try:
        executor.execute("ANALYZE")
    except Exception as e:
        print(f"  ANALYZE failed (non-fatal): {e}")
    print(f"  ANALYZE: done ({time.time() - t0:.1f}s)", flush=True)

    query_dir = benchmark_dir / "queries"
    query_files = sorted(query_dir.glob("*.sql"))
    if query_ids:
        query_set = set(query_ids)
        query_files = [f for f in query_files if f.stem in query_set]

    explore_dir = benchmark_dir / "plan_explore"
    explore_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*70}")
    print(f"  PLAN SPACE EXPLORATION (EXPLAIN-only)")
    print(f"  Queries: {len(query_files)}, Base combos: {len(PLAN_SPACE_COMBOS)}")
    print(f"{'='*70}\n")

    print(f"  {'Query':20s}  {'Plans':>6s}  {'Changers':>9s}  {'Vulns':>6s}  {'Types':30s}")
    print(f"  {'-'*20}  {'-'*6}  {'-'*9}  {'-'*6}  {'-'*30}")

    results = []
    t_start = time.time()

    for qf in query_files:
        qid = qf.stem
        sql = qf.read_text().strip()

        try:
            result = explore_plan_space(executor, sql, query_id=qid)
            results.append(result)

            # Save immediately
            out_path = explore_dir / f"{qid}.json"
            out_path.write_text(json.dumps(result, indent=2, default=str))

            vuln_types = [v["type"] for v in result.get("vulnerabilities", [])]
            n_plans = result.get("n_distinct_plans", 0)
            n_changers = result.get("n_plan_changers", 0)

            tag = " ***" if n_plans >= 4 else " *" if n_plans >= 2 else ""
            print(
                f"  {qid:20s}  {n_plans:6d}  {n_changers:9d}  "
                f"{len(vuln_types):6d}  {', '.join(vuln_types):30s}{tag}",
                flush=True,
            )
        except Exception as e:
            print(f"  {qid:20s}  ERROR: {str(e)[:50]}", flush=True)
            try:
                executor.rollback()
            except Exception:
                try:
                    executor.close()
                    executor.connect()
                except Exception:
                    pass

    elapsed = time.time() - t_start
    print(f"\n  Explored {len(results)} queries in {elapsed:.1f}s")

    # Save corpus summary
    if results:
        vuln_counts: Dict[str, int] = {}
        for r in results:
            for v in r.get("vulnerabilities", []):
                vt = v["type"]
                vuln_counts[vt] = vuln_counts.get(vt, 0) + 1

        multi_plan = [r for r in results if r.get("n_distinct_plans", 0) >= 2]
        novel_pairs = [r for r in results if r.get("pair_results_novel")]

        summary = {
            "n_queries": len(results),
            "n_multi_plan": len(multi_plan),
            "n_novel_pairs": len(novel_pairs),
            "vulnerability_counts": vuln_counts,
            "explored_at": datetime.now().isoformat(),
            "elapsed_seconds": round(elapsed, 1),
        }
        (explore_dir / "summary.json").write_text(json.dumps(summary, indent=2))

        print(f"\n{'='*70}")
        print(f"  SUMMARY")
        print(f"  Queries with multiple plans: {len(multi_plan)}/{len(results)}")
        print(f"  Queries with novel pair combos: {len(novel_pairs)}")
        print(f"  Vulnerability breakdown:")
        for vt, count in sorted(vuln_counts.items(), key=lambda x: -x[1]):
            print(f"    {vt:25s}  {count}")
        print(f"{'='*70}\n")

    executor.close()
    return results


def scan_corpus(
    benchmark_dir: Path,
    query_ids: Optional[List[str]] = None,
    timeout_ms: int = 120_000,
) -> List[ScanResult]:
    """Scan all queries in a benchmark directory.

    Loads config.json for DSN, iterates queries/, saves results to
    benchmark_dir/plan_scanner/{query_id}.json.
    """
    from .schemas import BenchmarkConfig
    from .execution.factory import create_executor_from_dsn

    benchmark_dir = Path(benchmark_dir)
    config = BenchmarkConfig.from_file(benchmark_dir / "config.json")

    if config.engine not in ("postgresql", "postgres"):
        print(f"  ERROR: Plan scanner is PostgreSQL-only (got engine={config.engine})")
        return []

    # Create executor
    executor = create_executor_from_dsn(config.db_path_or_dsn)
    executor.connect()

    # Find queries
    query_dir = benchmark_dir / "queries"
    if not query_dir.exists():
        print(f"  ERROR: No queries/ directory in {benchmark_dir}")
        executor.close()
        return []

    query_files = sorted(query_dir.glob("*.sql"))
    if query_ids:
        query_set = set(query_ids)
        query_files = [f for f in query_files if f.stem in query_set]

    print(f"\n{'='*70}")
    print(f"  PLAN-SPACE SWEEP: {len(query_files)} queries")
    print(f"  Benchmark: {benchmark_dir.name}")
    print(f"  DSN: {config.db_path_or_dsn}")
    print(f"  Combos: {len(PLAN_SPACE_COMBOS)}")
    print(f"  Timeout: {timeout_ms}ms per execution")
    print(f"{'='*70}\n")

    # Output directory
    scan_dir = benchmark_dir / "plan_scanner"
    scan_dir.mkdir(parents=True, exist_ok=True)

    results: List[ScanResult] = []

    # Print header
    print(f"  {'Query':20s}  {'Baseline(ms)':>12s}  {'Ceiling':>8s}  {'Best Combo':25s}  {'Plan':15s}")
    print(f"  {'-'*20}  {'-'*12}  {'-'*8}  {'-'*25}  {'-'*15}")

    skipped = 0
    for i, qf in enumerate(query_files):
        qid = qf.stem
        out_path = scan_dir / f"{qid}.json"

        # Resume support: skip already-swept queries
        if out_path.exists():
            try:
                cached = ScanResult.from_dict(json.loads(out_path.read_text()))
                results.append(cached)
                skipped += 1
                print(
                    f"  {qid:20s}  {'(cached)':>12s}  "
                    f"{cached.ceiling_speedup:7.2f}x  {cached.ceiling_combo:25s}  "
                    f"{cached.baseline_plan_node:15s}  [skip]"
                )
                continue
            except Exception:
                pass  # corrupt file — re-scan

        sql = qf.read_text().strip()

        try:
            result = scan_query(
                executor, sql, query_id=qid, timeout_ms=timeout_ms
            )
            results.append(result)

            # Save immediately (resumable)
            out_path.write_text(json.dumps(result.to_dict(), indent=2))

            # Print progress
            tag = ""
            if result.ceiling_speedup >= 1.50:
                tag = " ***"
            elif result.ceiling_speedup >= 1.10:
                tag = " *"
            remaining = len(query_files) - i - 1
            print(
                f"  {qid:20s}  {result.baseline_ms:12.1f}  "
                f"{result.ceiling_speedup:7.2f}x  {result.ceiling_combo:25s}  "
                f"{result.baseline_plan_node:15s}{tag}  [{remaining} left]",
                flush=True,
            )

        except Exception as e:
            err_str = str(e)[:80]
            print(f"  {qid:20s}  ERROR: {err_str}", flush=True)
            # Reconnect
            try:
                executor.close()
            except Exception:
                pass
            executor = create_executor_from_dsn(config.db_path_or_dsn)
            executor.connect()

    if skipped:
        print(f"\n  ({skipped} queries loaded from cache, {len(query_files) - skipped} swept fresh)")

    # Summary
    if results:
        _save_summary(scan_dir, results)
        _print_summary(results)

    executor.close()
    return results


def _save_summary(scan_dir: Path, results: List[ScanResult]) -> None:
    """Save corpus-level summary statistics."""
    ceilings = [r.ceiling_speedup for r in results]
    wins = [r for r in results if r.ceiling_speedup >= 1.10]
    big_wins = [r for r in results if r.ceiling_speedup >= 1.50]

    summary = {
        "n_queries": len(results),
        "n_wins": len(wins),
        "n_big_wins": len(big_wins),
        "avg_ceiling": round(sum(ceilings) / len(ceilings), 3),
        "max_ceiling": round(max(ceilings), 3),
        "max_ceiling_query": max(results, key=lambda r: r.ceiling_speedup).query_id,
        "scanned_at": datetime.now().isoformat(),
        "top_queries": [
            {
                "query_id": r.query_id,
                "ceiling_speedup": round(r.ceiling_speedup, 3),
                "ceiling_combo": r.ceiling_combo,
                "baseline_ms": round(r.baseline_ms, 1),
            }
            for r in sorted(results, key=lambda r: r.ceiling_speedup, reverse=True)[:20]
        ],
    }
    (scan_dir / "summary.json").write_text(json.dumps(summary, indent=2))


def _print_summary(results: List[ScanResult]) -> None:
    """Print corpus-level summary."""
    ceilings = [r.ceiling_speedup for r in results]
    wins = [r for r in results if r.ceiling_speedup >= 1.10]
    big_wins = [r for r in results if r.ceiling_speedup >= 1.50]

    print(f"\n{'='*70}")
    print(f"  SUMMARY: {len(results)} queries swept")
    print(f"  WIN (>=1.10x):       {len(wins)}")
    print(f"  BIG WIN (>=1.50x):   {len(big_wins)}")
    print(f"  Avg ceiling:         {sum(ceilings)/len(ceilings):.3f}x")
    print(f"  Max ceiling:         {max(ceilings):.3f}x")
    print(f"{'='*70}\n")


# ── Load / format functions (used during swarm) ────────────────────────

def load_scan_result(benchmark_dir: Path, query_id: str) -> Optional[ScanResult]:
    """Load cached wall-clock scan result from plan_scanner/."""
    scan_path = Path(benchmark_dir) / "plan_scanner" / f"{query_id}.json"
    if not scan_path.exists():
        return None
    try:
        data = json.loads(scan_path.read_text())
        return ScanResult.from_dict(data)
    except Exception as e:
        logger.warning(f"Failed to load scan result {scan_path}: {e}")
        return None


def load_explore_result(benchmark_dir: Path, query_id: str) -> Optional[dict]:
    """Load cached EXPLAIN-only explore result from plan_explore/."""
    explore_path = Path(benchmark_dir) / "plan_explore" / f"{query_id}.json"
    if not explore_path.exists():
        return None
    try:
        return json.loads(explore_path.read_text())
    except Exception as e:
        logger.warning(f"Failed to load explore result {explore_path}: {e}")
        return None


def format_explore_for_prompt(data: dict) -> str:
    """Render EXPLAIN-only explore results as concise prompt text.

    Used standalone when no wall-clock scan data exists. When scan data IS
    available, use format_scan_for_prompt(scan, explore=data) instead.

    Includes: plan diversity, vulnerability types, baseline plan structure,
    scan counts, predicate placement, bottleneck joins.
    """
    lines: List[str] = []

    # Plan diversity headline
    n_plans = data.get("n_distinct_plans", 0)
    n_changers = data.get("n_plan_changers", 0)
    if n_plans <= 2:
        diversity = "RIGID"
    elif n_plans <= 8:
        diversity = "MODERATE"
    else:
        diversity = "HIGH"
    lines.append(
        f"Plan diversity: {n_plans} distinct plans, "
        f"{n_changers} plan changers | {diversity}"
    )

    # Baseline plan structure (compact)
    joins = data.get("baseline_joins", [])
    if joins:
        from collections import Counter
        join_counts = Counter(j.split("(")[0] for j in joins)
        join_str = ", ".join(f"{v}x {k}" for k, v in join_counts.most_common())
        lines.append(f"Baseline joins: {join_str}")

    # Vulnerability summary — one line per type
    vulns = data.get("vulnerabilities", [])
    for v in vulns:
        vtype = v["type"]
        n_combos = len(v.get("combos", []))
        details = v.get("detail", [])
        if vtype == "JOIN_TYPE_TRAP" and details:
            first = details[0]
            lines.append(f"  JOIN_TYPE_TRAP ({n_combos} combos): {first}")
        elif vtype == "JOIN_ORDER_TRAP":
            lines.append(f"  JOIN_ORDER_TRAP ({n_combos} combos): table order unstable")
        elif vtype == "SCAN_TYPE_TRAP":
            lines.append(f"  SCAN_TYPE_TRAP ({n_combos} combos): scan methods fragile")
        elif vtype == "MEMORY_SENSITIVITY":
            lines.append(f"  MEMORY_SENSITIVITY: plan shape changes with more memory")

    # Plan changers (which flags move the needle)
    changers = data.get("plan_changers", [])
    if changers:
        lines.append(f"Plan changers: {', '.join(changers)}")

    # Bottleneck joins (from enriched explore data)
    bottleneck_joins = data.get("bottleneck_joins", [])
    if bottleneck_joins:
        lines.append("")
        lines.append(_format_bottleneck_joins(bottleneck_joins))

    # Scan counts + redundant scan detection
    scan_counts = data.get("scan_counts", {})
    if scan_counts:
        lines.append("")
        lines.append(_format_scan_counts(scan_counts))

    # Predicate placement audit
    placements = data.get("predicate_placement", [])
    if placements:
        pred_text = _format_predicate_placement(placements)
        if pred_text:
            lines.append("")
            lines.append(pred_text)

    return "\n".join(lines)


def _get_combo(result: ScanResult, name: str) -> Optional[ComboResult]:
    """Look up a combo by name, return None if missing or errored."""
    for c in result.combos:
        if c.combo_name == name and c.error is None:
            return c
    return None


def _classify_speedup(s: float) -> str:
    if s >= 1.50:
        return "BIG_WIN"
    if s >= 1.10:
        return "WIN"
    if s >= 0.95:
        return "NEUTRAL"
    if s >= 0.50:
        return "REGRESSION"
    return "CATASTROPHIC"


def _analyze_joins(result: ScanResult) -> str:
    """Classify join sensitivity from sweep combos."""
    force_hash = _get_combo(result, "force_hash")
    force_merge = _get_combo(result, "force_merge")
    force_nl = _get_combo(result, "force_nestloop")
    no_nl = _get_combo(result, "no_nestloop")
    no_hj = _get_combo(result, "no_hashjoin")
    no_mj = _get_combo(result, "no_mergejoin")

    # Check for catastrophic regressions — tells us which join type is critical
    catastrophic = []
    if force_hash and force_hash.speedup < 0.10:
        catastrophic.append(("hash", force_hash.speedup))
    if force_merge and force_merge.speedup < 0.10:
        catastrophic.append(("merge", force_merge.speedup))
    if force_nl and force_nl.speedup < 0.10:
        catastrophic.append(("nestloop", force_nl.speedup))
    if no_nl and no_nl.speedup < 0.10:
        catastrophic.append(("no_nestloop", no_nl.speedup))
    if no_hj and no_hj.speedup < 0.10:
        catastrophic.append(("no_hashjoin", no_hj.speedup))
    if no_mj and no_mj.speedup < 0.10:
        catastrophic.append(("no_mergejoin", no_mj.speedup))

    # Determine which join type the plan is locked on
    nl_critical = (no_nl and no_nl.speedup < 0.10) or (force_hash and force_hash.speedup < 0.10)
    hj_critical = (no_hj and no_hj.speedup < 0.10) or (force_nl and force_nl.speedup < 0.10)

    # Check for wins from changing join type
    best_join = max(
        [c for c in [force_hash, force_merge, force_nl, no_nl, no_hj, no_mj] if c],
        key=lambda c: c.speedup,
        default=None,
    )
    join_win = best_join and best_join.speedup >= 1.10

    if nl_critical and not join_win:
        worst = min(
            [c.speedup for c in [force_hash, force_merge, no_nl] if c and c.speedup < 0.50],
            default=0.5,
        )
        return (
            f"JOINS: LOCKED on nested loops. Alternatives = catastrophic "
            f"({worst:.0%} baseline).\n"
            f"  -> Do NOT change join methods. Reduce what nested loops process."
        )
    elif hj_critical and not join_win:
        worst = min(
            [c.speedup for c in [force_nl, force_merge, no_hj] if c and c.speedup < 0.50],
            default=0.5,
        )
        return (
            f"JOINS: LOCKED on hash joins. Alternatives = catastrophic "
            f"({worst:.0%} baseline).\n"
            f"  -> Do NOT change join methods. Reduce hash build side cardinality."
        )
    elif join_win:
        return (
            f"JOINS: SENSITIVE — {best_join.combo_name} gives {best_join.speedup:.2f}x.\n"
            f"  -> Rewrite SQL to guide optimizer toward this join strategy."
        )
    else:
        return "JOINS: Stable. Join method changes have minimal impact."


def _analyze_memory(result: ScanResult) -> str:
    """Classify memory sensitivity."""
    mem256 = _get_combo(result, "work_mem_256mb")
    mem1g = _get_combo(result, "work_mem_1gb")
    mem2g = _get_combo(result, "work_mem_2gb")
    ssd_mem = _get_combo(result, "ssd_plus_mem")

    best = max(
        [c for c in [mem256, mem1g, mem2g, ssd_mem] if c],
        key=lambda c: c.speedup,
        default=None,
    )
    if not best:
        return "MEMORY: No data."

    if best.speedup >= 1.50:
        return (
            f"MEMORY: HIGH impact — {best.combo_name} gives {best.speedup:.2f}x.\n"
            f"  -> Likely spilling to disk. Recommend SET LOCAL work_mem = '256MB'."
        )
    elif best.speedup >= 1.10:
        return (
            f"MEMORY: MODERATE — {best.combo_name} gives {best.speedup:.2f}x.\n"
            f"  -> Some spill benefit. Consider SET LOCAL work_mem = '256MB'."
        )
    else:
        return f"MEMORY: Minor ({best.combo_name} -> {best.speedup:.2f}x). No significant spill."


def _analyze_jit(result: ScanResult) -> Optional[str]:
    """Classify JIT sensitivity. Returns None if neutral."""
    no_jit = _get_combo(result, "no_jit")
    if not no_jit:
        return None
    if no_jit.speedup >= 1.10:
        return (
            f"JIT: Overhead ({no_jit.speedup:.2f}x from disabling). "
            f"Recommend SET LOCAL jit = off."
        )
    elif no_jit.speedup < 0.90:
        return f"JIT: Beneficial ({no_jit.speedup:.2f}x when disabled = worse). Keep JIT on."
    return None


def _analyze_parallelism(result: ScanResult) -> Optional[str]:
    """Classify parallelism sensitivity. Returns None if neutral."""
    no_par = _get_combo(result, "no_parallel")
    max_par = _get_combo(result, "max_parallel")

    if max_par and max_par.speedup >= 1.20:
        return (
            f"PARALLELISM: Beneficial — max_parallel gives {max_par.speedup:.2f}x.\n"
            f"  -> Recommend SET LOCAL max_parallel_workers_per_gather = '8'."
        )
    if no_par and no_par.speedup >= 1.10:
        return (
            f"PARALLELISM: Overhead — disabling gives {no_par.speedup:.2f}x.\n"
            f"  -> Workers hurt. Consider SET LOCAL max_parallel_workers_per_gather = '0'."
        )
    if max_par and max_par.speedup < 0.85:
        return "PARALLELISM: Overhead. More workers = slower. Query too fast for parallel."
    return None


def _analyze_reorder(result: ScanResult) -> Optional[str]:
    """Classify join reorder sensitivity. Returns None if neutral."""
    no_reorder = _get_combo(result, "no_reorder")
    max_reorder = _get_combo(result, "max_reorder")

    if no_reorder and no_reorder.speedup >= 1.20:
        return (
            f"JOIN ORDER: Fragile — disabling reorder gives {no_reorder.speedup:.2f}x.\n"
            f"  -> Written join order is better than optimizer's choice. "
            f"Use explicit JOIN syntax."
        )
    if max_reorder and max_reorder.speedup >= 1.20:
        return (
            f"JOIN ORDER: Fragile — max reorder gives {max_reorder.speedup:.2f}x.\n"
            f"  -> Optimizer needs more search space. Restructure to simplify join graph."
        )
    return None


def load_known_sql_ceiling(
    benchmark_dir: Path, query_id: str,
) -> Optional[tuple]:
    """Load known SQL rewrite ceiling from known_ceilings.json.

    Returns (speedup, technique) or None. The known_ceilings.json file maps
    query_id -> {"speedup": float, "technique": str} and is curated from
    engine profiles and prior benchmark runs.
    """
    ceiling_path = Path(benchmark_dir) / "known_ceilings.json"
    if not ceiling_path.exists():
        return None
    try:
        data = json.loads(ceiling_path.read_text())
        if query_id in data:
            entry = data[query_id]
            return entry.get("speedup", 1.0), entry.get("technique", "")
    except Exception:
        pass
    return None


def format_scan_for_prompt(
    result: ScanResult,
    explore: Optional[dict] = None,
    known_sql_ceiling: Optional[float] = None,
    known_sql_technique: Optional[str] = None,
) -> str:
    """Render scan results as classified intelligence for probe/analyst prompt.

    Combines wall-clock scan data (ScanResult) with optional EXPLAIN-only
    explore data and known SQL ceiling from prior runs. Produces:
    - Dual ceilings: CONFIG_CEILING + KNOWN_SQL_CEILING
    - Confidence assessment (noise floor vs signal)
    - Enriched JOINS with bottleneck sub-signals
    - Scan counts + redundant scan opportunities
    - Predicate placement + pushdown opportunities
    - Composed decision-tree strategy
    - SET LOCAL config recommendations
    """
    lines: List[str] = []

    # ── Confidence assessment (Issue 5) ────────────────────────────────
    conf_level, conf_detail = _assess_confidence(
        result.baseline_ms, result.ceiling_speedup
    )

    # ── Dual ceiling (Issue 1) ─────────────────────────────────────────
    if result.ceiling_speedup >= 1.50:
        ceiling_tag = "HIGH"
    elif result.ceiling_speedup >= 1.10:
        ceiling_tag = "LOW"
    else:
        ceiling_tag = "NONE"

    ceiling_combo_obj = _get_combo(result, result.ceiling_combo)
    config_str = ""
    if ceiling_combo_obj:
        config_str = ", ".join(
            f"{k}={v}" for k, v in ceiling_combo_obj.config.items()
        )

    lines.append(f"Baseline: {result.baseline_ms:.0f}ms | CONFIDENCE: {conf_level}")
    if conf_level == "LOW":
        lines.append(f"  ({conf_detail})")

    if config_str:
        lines.append(f"CONFIG_CEILING: {result.ceiling_speedup:.2f}x ({config_str}) — {ceiling_tag}")
    else:
        lines.append(f"CONFIG_CEILING: {result.ceiling_speedup:.2f}x — {ceiling_tag}")

    if known_sql_ceiling and known_sql_ceiling > 1.0:
        tech_str = f" ({known_sql_technique})" if known_sql_technique else ""
        lines.append(f"KNOWN_SQL_CEILING: {known_sql_ceiling:.2f}x{tech_str}")
        total = max(result.ceiling_speedup, known_sql_ceiling)
        headroom = "HIGH" if total >= 1.50 else "LOW"
        lines.append(f"TOTAL_HEADROOM: {headroom} (best proven: {total:.2f}x)")
    elif ceiling_tag == "NONE":
        lines.append("Planner flags have no effect. All improvement must come from SQL rewrite.")
    elif ceiling_tag == "LOW":
        lines.append("Config alone insufficient. SQL restructuring required.")
    else:
        lines.append("Config alone can deliver significant speedup. Include winning config.")

    lines.append("")

    # ── Dimensional analysis ───────────────────────────────────────────
    join_text = _analyze_joins(result)
    memory_text = _analyze_memory(result)

    # Enriched JOINS with bottleneck sub-signals (Issue 2)
    lines.append(join_text)
    bottleneck_joins = explore.get("bottleneck_joins", []) if explore else []
    if bottleneck_joins and "LOCKED" in join_text:
        lines.append(_format_bottleneck_joins(bottleneck_joins))

    lines.append(memory_text)

    # Optional dimensions — only include if non-neutral
    jit = _analyze_jit(result)
    if jit:
        lines.append(jit)

    par = _analyze_parallelism(result)
    if par:
        lines.append(par)

    reorder = _analyze_reorder(result)
    if reorder:
        lines.append(reorder)

    # ── Enriched signals from explore data (Issue 3 + Additions) ───────
    if explore:
        # Scan counts + redundant scan detection
        scan_counts = explore.get("scan_counts", {})
        if scan_counts:
            lines.append("")
            lines.append(_format_scan_counts(scan_counts))

        # Predicate placement audit
        placements = explore.get("predicate_placement", [])
        if placements:
            pred_text = _format_predicate_placement(placements)
            if pred_text:
                lines.append("")
                lines.append(pred_text)

        # Plan diversity headline from explore
        n_plans = explore.get("n_distinct_plans", 0)
        n_changers = explore.get("n_plan_changers", 0)
        if n_plans > 0:
            if n_plans <= 2:
                diversity = "RIGID"
            elif n_plans <= 8:
                diversity = "MODERATE"
            else:
                diversity = "HIGH"
            lines.append("")
            lines.append(
                f"Plan diversity: {n_plans} distinct plans, "
                f"{n_changers} plan changers | {diversity}"
            )

    # ── Composed decision-tree strategy (Issue 4) ──────────────────────
    # Extract memory class from memory_text
    if "HIGH" in memory_text:
        mem_class = "HIGH"
    elif "MODERATE" in memory_text:
        mem_class = "MODERATE"
    else:
        mem_class = "MINOR"

    lines.append("")
    lines.append(_compose_strategy(
        join_class=join_text,
        memory_class=mem_class,
        join_detail=join_text,
        bottleneck_joins=bottleneck_joins,
    ))

    # ── SET LOCAL recommendations ──────────────────────────────────────
    recs: List[str] = []
    no_jit = _get_combo(result, "no_jit")
    if no_jit and no_jit.speedup >= 1.10:
        recs.append("SET LOCAL jit = 'off'")
    mem_combos = [
        (_get_combo(result, "work_mem_256mb"), "256MB"),
        (_get_combo(result, "work_mem_1gb"), "1GB"),
    ]
    best_mem = max(
        [(c, v) for c, v in mem_combos if c],
        key=lambda x: x[0].speedup,
        default=(None, None),
    )
    if best_mem[0] and best_mem[0].speedup >= 1.10:
        recs.append(f"SET LOCAL work_mem = '{best_mem[1]}'")
    max_par = _get_combo(result, "max_parallel")
    no_par = _get_combo(result, "no_parallel")
    if max_par and max_par.speedup >= 1.20:
        recs.append("SET LOCAL max_parallel_workers_per_gather = '8'")
    elif no_par and no_par.speedup >= 1.10:
        recs.append("SET LOCAL max_parallel_workers_per_gather = '0'")

    # Check compound combos for interaction effects
    jit_mem = _get_combo(result, "jit_off_mem_256mb")
    if jit_mem and jit_mem.speedup > result.ceiling_speedup:
        recs = ["SET LOCAL jit = 'off'", "SET LOCAL work_mem = '256MB'"]

    if recs:
        lines.append("")
        lines.append("CONFIG: " + " + ".join(recs))

    return "\n".join(lines)


# ── CLI entry point ─────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Plan-space scanner: toggle PG planner flags to find performance ceiling"
    )
    parser.add_argument(
        "benchmark_dir",
        type=Path,
        help="Path to benchmark directory (must contain config.json and queries/)",
    )
    parser.add_argument(
        "--query-ids",
        nargs="*",
        default=None,
        help="Specific query IDs to scan (default: all)",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=120_000,
        help="Timeout per execution in ms (default: 120000)",
    )
    parser.add_argument(
        "--explain-only",
        action="store_true",
        help="Use EXPLAIN costs only (no execution). ~30s for 76 queries.",
    )
    parser.add_argument(
        "--explore",
        action="store_true",
        help="Plan space exploration: find distinct plans + vulnerabilities (EXPLAIN-only).",
    )
    parser.add_argument(
        "--correlate",
        action="store_true",
        help="Validate EXPLAIN cost vs wall-clock timing correlation.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.explore:
        explore_corpus(
            benchmark_dir=args.benchmark_dir,
            query_ids=args.query_ids,
        )
    elif args.correlate:
        validate_correlation(benchmark_dir=args.benchmark_dir)
    elif args.explain_only:
        scan_corpus_explain_only(
            benchmark_dir=args.benchmark_dir,
            query_ids=args.query_ids,
        )
    else:
        scan_corpus(
            benchmark_dir=args.benchmark_dir,
            query_ids=args.query_ids,
            timeout_ms=args.timeout_ms,
        )


if __name__ == "__main__":
    main()

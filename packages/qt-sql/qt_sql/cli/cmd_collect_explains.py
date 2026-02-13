"""Collect EXPLAIN plans, baselines, and operator stats for all queries in a benchmark.

Usage:
    qt collect-explains <benchmark> [--query query_1] [--timeout 300]

Connects to the database, runs each query with result caching disabled,
and saves structured EXPLAIN data to benchmark_dir/explains/.
"""

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import click

logger = logging.getLogger(__name__)


def _strip_comments(sql: str) -> str:
    """Remove SQL comment lines (-- ...)."""
    lines = [l for l in sql.split("\n") if not l.strip().startswith("--")]
    return "\n".join(lines).strip()


def _render_explain_text(explain_rows: List[Dict[str, Any]]) -> str:
    """Render Snowflake EXPLAIN rows into readable plan text."""
    lines = []
    for r in explain_rows:
        step = r.get("step", "")
        oid = r.get("id", "")
        op = r.get("operation", "")
        objects = r.get("objects", "")
        parent = r.get("parentoperators", "")
        ptotal = r.get("partitionstotal", "")
        passigned = r.get("partitionsassigned", "")
        bytesassigned = r.get("bytesassigned", "")
        expr = r.get("expressions", "")

        if step is None and oid is None:
            # GlobalStats row
            line = f"GlobalStats Parts={passigned}/{ptotal} Bytes={bytesassigned}"
        else:
            indent = "  " if parent else ""
            line = f"{indent}[{step}.{oid}] {op}"
            if objects:
                # Shorten fully-qualified table names
                short = str(objects).split(".")[-1] if "." in str(objects) else objects
                line += f" ({short})"
            if ptotal:
                line += f" parts={passigned}/{ptotal}"
            if bytesassigned:
                line += f" bytes={bytesassigned}"
            if expr:
                line += f" expr={expr}"
        lines.append(line)
    return "\n".join(lines)


def _render_operator_stats_text(stats: List[Dict[str, Any]]) -> str:
    """Render operator stats into compact text for prompts."""
    lines = []
    for s in stats:
        op_type = s.get("operator_type", "?")
        step_id = s.get("step_id", "?")
        op_id = s.get("operator_id", "?")

        # Parse JSON stats
        raw_stats = s.get("operator_statistics", "{}")
        if isinstance(raw_stats, str):
            try:
                op_stats = json.loads(raw_stats)
            except json.JSONDecodeError:
                op_stats = {}
        else:
            op_stats = raw_stats if isinstance(raw_stats, dict) else {}

        raw_timing = s.get("execution_time_breakdown", "{}")
        if isinstance(raw_timing, str):
            try:
                timing = json.loads(raw_timing)
            except json.JSONDecodeError:
                timing = {}
        else:
            timing = raw_timing if isinstance(raw_timing, dict) else {}

        # Extract key metrics
        input_rows = op_stats.get("input_rows", "")
        output_rows = op_stats.get("output_rows", "")
        pct = timing.get("overall_percentage", 0)
        pct_str = f"{pct * 100:.1f}%" if pct else ""

        io = op_stats.get("io", {})
        pruning = op_stats.get("pruning", {})

        line = f"[{step_id}.{op_id}] {op_type}"
        if input_rows:
            line += f" in={input_rows:,}" if isinstance(input_rows, int) else f" in={input_rows}"
        if output_rows:
            line += f" out={output_rows:,}" if isinstance(output_rows, int) else f" out={output_rows}"
        if pct_str:
            line += f" ({pct_str})"
        if pruning:
            ps = pruning.get("partitions_scanned", "?")
            pt = pruning.get("partitions_total", "?")
            line += f" parts={ps}/{pt}"
        if io:
            scanned = io.get("bytes_scanned", 0)
            if scanned:
                mb = scanned / (1024 * 1024)
                line += f" scanned={mb:.1f}MB"
            cache_pct = io.get("percentage_scanned_from_cache", 0)
            if cache_pct:
                line += f" cache={cache_pct:.0%}"

        lines.append(line)

    return "\n".join(lines)


def collect_single_query(
    executor,
    query_id: str,
    sql: str,
    timeout_ms: int = 300_000,
) -> Dict[str, Any]:
    """Collect EXPLAIN, baseline timing, and operator stats for one query.

    Returns dict matching the explains/ JSON format.
    """
    clean_sql = _strip_comments(sql)
    result: Dict[str, Any] = {
        "execution_time_ms": None,
        "row_count": None,
        "plan_text": None,
        "plan_json": None,
        "actual_rows": None,
        "operator_stats": None,
        "operator_stats_text": None,
        "error": None,
    }

    # 1. EXPLAIN (estimated plan)
    try:
        explain_rows = executor.execute(f"EXPLAIN {clean_sql}")
        result["plan_text"] = _render_explain_text(explain_rows)
    except Exception as e:
        logger.warning(f"[{query_id}] EXPLAIN failed: {e}")
        result["error"] = f"EXPLAIN failed: {e}"
        return result

    # 2. SYSTEM$EXPLAIN_PLAN_JSON (structured plan)
    try:
        sql_escaped = clean_sql.replace("'", "''")
        json_rows = executor.execute(
            f"SELECT SYSTEM$EXPLAIN_PLAN_JSON('{sql_escaped}')"
        )
        if json_rows:
            plan_json_str = list(json_rows[0].values())[0]
            if plan_json_str:
                result["plan_json"] = json.loads(plan_json_str)
    except Exception as e:
        logger.warning(f"[{query_id}] SYSTEM$EXPLAIN_PLAN_JSON failed: {e}")

    # 3. Execute: warmup + 2 measures
    try:
        # Warmup
        executor.execute(clean_sql, timeout_ms=timeout_ms)

        # Measure 1 (capture rows for count)
        t0 = time.perf_counter()
        rows = executor.execute(clean_sql, timeout_ms=timeout_ms)
        t1 = (time.perf_counter() - t0) * 1000

        # Measure 2
        t0 = time.perf_counter()
        executor.execute(clean_sql, timeout_ms=timeout_ms)
        t2 = (time.perf_counter() - t0) * 1000

        avg_ms = (t1 + t2) / 2
        result["execution_time_ms"] = round(avg_ms, 1)
        result["row_count"] = len(rows) if rows else 0
        result["actual_rows"] = len(rows) if rows else 0
    except Exception as e:
        error_str = str(e).lower()
        if "timeout" in error_str or "cancel" in error_str:
            logger.warning(f"[{query_id}] Query TIMEOUT at {timeout_ms}ms")
            result["execution_time_ms"] = float(timeout_ms)
            result["error"] = "TIMEOUT"
        else:
            logger.warning(f"[{query_id}] Query FAILED: {e}")
            result["error"] = str(e)[:500]
        return result

    # 4. GET_QUERY_OPERATOR_STATS (actual execution stats)
    try:
        stats = executor.execute(
            "SELECT * FROM TABLE(GET_QUERY_OPERATOR_STATS(LAST_QUERY_ID()))"
        )
        if stats:
            # Parse JSON fields
            parsed_stats = []
            for s in stats:
                parsed = dict(s)
                for json_field in (
                    "operator_statistics",
                    "execution_time_breakdown",
                    "operator_attributes",
                ):
                    raw = parsed.get(json_field, "{}")
                    if isinstance(raw, str):
                        try:
                            parsed[json_field] = json.loads(raw)
                        except json.JSONDecodeError:
                            pass
                parsed_stats.append(parsed)

            result["operator_stats"] = parsed_stats
            result["operator_stats_text"] = _render_operator_stats_text(stats)
    except Exception as e:
        logger.warning(f"[{query_id}] GET_QUERY_OPERATOR_STATS failed: {e}")

    return result


@click.command("collect-explains")
@click.argument("benchmark")
@click.option("-q", "--query", multiple=True, help="Specific query IDs (default: all)")
@click.option("--timeout", default=300, help="Query timeout in seconds (default: 300)")
@click.option("--parallel", default=10, help="Concurrent connections (default: 10)")
@click.option("--force", is_flag=True, help="Overwrite existing non-stub explains")
def cmd_collect_explains(benchmark: str, query: tuple, timeout: int, parallel: int, force: bool):
    """Collect EXPLAIN plans, baselines, and operator stats from the database."""
    from ._common import resolve_benchmark

    benchmark_dir = resolve_benchmark(benchmark)

    config_path = benchmark_dir / "config.json"
    if not config_path.exists():
        click.echo(f"No config.json in {benchmark_dir}")
        raise SystemExit(1)

    config = json.loads(config_path.read_text())
    dsn = config.get("dsn", "")
    if not dsn:
        click.echo("No DSN in config.json")
        raise SystemExit(1)

    # Find queries
    queries_dir = benchmark_dir / "queries"
    explains_dir = benchmark_dir / "explains"
    explains_dir.mkdir(exist_ok=True)

    if query:
        query_ids = list(query)
    else:
        query_ids = sorted(
            [p.stem for p in queries_dir.glob("*.sql")],
            key=lambda x: (
                int("".join(c for c in x.split("_")[-1] if c.isdigit()) or "0"),
                x,
            ),
        )

    # Filter out already-collected (unless --force)
    if not force:
        pending = []
        for qid in query_ids:
            explain_path = explains_dir / f"{qid}.json"
            if explain_path.exists():
                data = json.loads(explain_path.read_text())
                if data.get("execution_time_ms") is not None:
                    continue  # Already collected
            pending.append(qid)
        skipped = len(query_ids) - len(pending)
        if skipped:
            click.echo(f"Skipping {skipped} already-collected queries (use --force to overwrite)")
        query_ids = pending

    if not query_ids:
        click.echo("No queries to collect. Done.")
        return

    click.echo(f"Collecting EXPLAIN + baselines for {len(query_ids)} queries")
    click.echo(f"  Benchmark: {benchmark_dir.name}")
    click.echo(f"  DSN: {dsn[:40]}...")
    click.echo(f"  Timeout: {timeout}s")
    click.echo(f"  Parallelism: {parallel} concurrent connections")
    click.echo()

    from concurrent.futures import ThreadPoolExecutor, as_completed
    from ..execution.factory import create_executor_from_dsn

    timeout_ms = timeout * 1000
    results_summary = []
    t_total = time.perf_counter()

    def _collect_one(qid: str) -> tuple:
        """Collect a single query on its own connection."""
        sql_path = queries_dir / f"{qid}.sql"
        if not sql_path.exists():
            return (qid, {"error": "no .sql file"}, 0.0)

        sql = sql_path.read_text()
        t0 = time.perf_counter()
        try:
            executor = create_executor_from_dsn(dsn)
            executor.connect()
            executor.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
            result = collect_single_query(executor, qid, sql, timeout_ms)
            executor.close()
        except Exception as e:
            result = {"error": str(e)[:500]}
        elapsed = time.perf_counter() - t0

        # Save immediately (thread-safe — different files)
        explain_path = explains_dir / f"{qid}.json"
        explain_path.write_text(json.dumps(result, indent=2, default=str))

        return (qid, result, elapsed)

    # Process in batches for readable output
    batch_size = parallel
    completed_count = 0

    for batch_start in range(0, len(query_ids), batch_size):
        batch = query_ids[batch_start : batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        total_batches = (len(query_ids) + batch_size - 1) // batch_size
        click.echo(
            f"--- Batch {batch_num}/{total_batches}: "
            f"{', '.join(batch)} ---"
        )

        with ThreadPoolExecutor(max_workers=batch_size) as pool:
            futures = {pool.submit(_collect_one, qid): qid for qid in batch}
            for future in as_completed(futures):
                qid, result, elapsed = future.result()
                completed_count += 1

                ms = result.get("execution_time_ms")
                rows = result.get("row_count")
                err = result.get("error")
                n_ops = len(result.get("operator_stats", []) or [])

                if err and err != "TIMEOUT":
                    status = f"ERROR ({err[:60]})"
                elif err == "TIMEOUT":
                    status = f"TIMEOUT ({timeout}s)"
                else:
                    status = f"{ms:.0f}ms, {rows} rows, {n_ops} ops"

                click.echo(
                    f"  [{completed_count}/{len(query_ids)}] {qid}: "
                    f"{status} ({elapsed:.1f}s)"
                )
                results_summary.append((qid, ms, rows, err))

    total_elapsed = time.perf_counter() - t_total

    # Print summary
    click.echo()
    click.echo(f"=== Collection complete ({total_elapsed:.0f}s) ===")
    collected = [r for r in results_summary if r[1] is not None and r[3] is None]
    timeouts = [r for r in results_summary if r[3] == "TIMEOUT"]
    errors = [r for r in results_summary if r[3] and r[3] != "TIMEOUT"]
    click.echo(f"  Collected: {len(collected)}")
    click.echo(f"  Timeouts:  {len(timeouts)}")
    click.echo(f"  Errors:    {len(errors)}")

    if collected:
        times = sorted(r[1] for r in collected)
        click.echo(f"  Timing range: {times[0]:.0f}ms – {times[-1]:.0f}ms")
        slow = [(qid, ms) for qid, ms, _, _ in collected if ms > 5000]
        if slow:
            click.echo(f"  Slow queries (>5s): {', '.join(f'{qid}={ms:.0f}ms' for qid, ms in slow)}")

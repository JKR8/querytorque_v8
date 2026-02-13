"""Vital signs extractor — condenses EXPLAIN plans into actionable signals.

Extracts 5 key signals from a full EXPLAIN plan:
1. Wall-clock time (ms)
2. Buffer efficiency (hit/read ratio for PG, scan selectivity for DuckDB)
3. Spill indicator (temp written blocks / out-of-core)
4. Liar node (biggest planned-vs-actual rows delta)
5. Bottleneck node (single highest-cost operator)

Output is compact (5-10 lines) for LLM consumption in coach prompts.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)


def extract_vital_signs(
    explain_text: str,
    plan_json: Optional[Any] = None,
    dialect: str = "duckdb",
) -> str:
    """Condense a full EXPLAIN into 5-10 lines of actionable signals.

    Args:
        explain_text: Formatted EXPLAIN text (from format_*_explain_tree or raw)
        plan_json: Raw plan JSON (optional, for deeper extraction)
        dialect: SQL dialect (duckdb, postgres, postgresql, snowflake)

    Returns:
        Compact vital signs string (5-10 lines)
    """
    if not explain_text and not plan_json:
        return "No EXPLAIN data available."

    # Handle error/estimate EXPLAIN context from candidate collection
    if explain_text and explain_text.startswith("[EXPLAIN failed"):
        return explain_text.strip()
    if explain_text and explain_text.startswith("[EXPLAIN estimate"):
        return explain_text.strip()

    dialect_lower = dialect.lower()

    if dialect_lower in ("postgres", "postgresql"):
        if plan_json:
            return _extract_pg_vital_signs_from_json(plan_json)
        return _extract_pg_vital_signs_from_text(explain_text)
    elif dialect_lower == "duckdb":
        return _extract_duckdb_vital_signs(explain_text, plan_json)
    elif dialect_lower == "snowflake":
        return _extract_snowflake_vital_signs(explain_text)
    else:
        return _extract_generic_vital_signs(explain_text)


def _extract_pg_vital_signs_from_json(plan_json: Any) -> str:
    """Extract vital signs from PostgreSQL EXPLAIN JSON."""
    if isinstance(plan_json, list) and plan_json:
        top = plan_json[0]
    elif isinstance(plan_json, dict):
        top = plan_json
    else:
        return "Invalid PG plan JSON."

    root = top.get("Plan", {})
    if not root:
        return "Empty PG plan."

    lines = []

    # 1. Wall-clock time
    exec_ms = top.get("Execution Time", 0) or root.get("Actual Total Time", 0)
    planning_ms = top.get("Planning Time", 0)
    if exec_ms:
        lines.append(f"Time: {exec_ms:.0f}ms (planning: {planning_ms:.0f}ms)")

    # 2. Buffer efficiency — aggregate across all nodes
    total_hit, total_read, total_temp_w = _pg_aggregate_buffers(root)
    if total_hit + total_read > 0:
        hit_pct = (total_hit / (total_hit + total_read)) * 100
        lines.append(
            f"Buffers: {_fmt_k(total_hit)} hit / {_fmt_k(total_read)} read "
            f"({hit_pct:.0f}% cache)"
        )

    # 3. Spill indicator
    if total_temp_w > 0:
        temp_mb = total_temp_w * 8 / 1024  # 8KB blocks -> MB
        lines.append(f"Spill: {temp_mb:.0f}MB temp written to disk")

    # 4. Liar node (biggest planned-vs-actual rows delta)
    liar = _pg_find_liar_node(root)
    if liar:
        lines.append(liar)

    # 5. Bottleneck node (highest time operator)
    bottleneck = _pg_find_bottleneck(root, exec_ms)
    if bottleneck:
        lines.append(bottleneck)

    # JIT overhead
    jit = top.get("JIT") or root.get("JIT")
    if jit:
        gen_ms = jit.get("Generation", {}).get("Timing", 0)
        opt_ms = jit.get("Optimization", {}).get("Timing", 0)
        emit_ms = jit.get("Emission", {}).get("Timing", 0)
        total_jit = gen_ms + opt_ms + emit_ms
        if total_jit > 10:  # Only report if significant
            lines.append(f"JIT: {total_jit:.0f}ms overhead")

    return "\n".join(lines) if lines else "PG plan: no actionable signals extracted."


def _extract_pg_vital_signs_from_text(explain_text: str) -> str:
    """Extract vital signs from formatted PG EXPLAIN text."""
    lines = []

    # 1. Wall-clock time
    m = re.search(r'Total execution time:\s*([\d.]+)ms', explain_text)
    if m:
        lines.append(f"Time: {float(m.group(1)):.0f}ms")

    # 2. Buffer summary — look for aggregated buffer lines
    hit_total, read_total = 0, 0
    for m in re.finditer(r'hit=(\d+[KkMm]?)', explain_text):
        hit_total += _parse_fmt_count(m.group(1))
    for m in re.finditer(r'read=(\d+[KkMm]?)', explain_text):
        read_total += _parse_fmt_count(m.group(1))
    if hit_total + read_total > 0:
        hit_pct = (hit_total / (hit_total + read_total)) * 100
        lines.append(f"Buffers: ~{hit_pct:.0f}% cache hit")

    # 3. Spill
    temp_writes = re.findall(r'temp_w=(\d+[KkMm]?)', explain_text)
    if temp_writes:
        lines.append(f"Spill: temp writes detected ({len(temp_writes)} operators)")

    # 4/5. Bottleneck — find the highest time operator
    bottleneck_m = re.findall(r'->\s+(\S+(?:\s+\S+)?)\s.*?time=([\d.]+)ms', explain_text)
    if bottleneck_m:
        sorted_ops = sorted(bottleneck_m, key=lambda x: float(x[1]), reverse=True)
        top_op = sorted_ops[0]
        lines.append(f"Bottleneck: {top_op[0]} ({top_op[1]}ms)")

    # Liar — look for large row misestimates
    liar_m = re.findall(r'->\s+(\S+(?:\s+\S+)?).*?est_rows?=(\d+).*?rows?=(\d+)', explain_text)
    if not liar_m:
        liar_m = re.findall(r'->\s+(\S+(?:\s+\S+)?).*?\(rows?=(\d+).*?est.*?(\d+)', explain_text)
    if liar_m:
        for op, est_s, act_s in liar_m:
            est_val = int(est_s)
            act_val = int(act_s)
            if est_val > 0 and act_val > 0:
                ratio = max(act_val / est_val, est_val / act_val)
                if ratio > 10 and act_val > 1000:
                    lines.append(
                        f"Liar: {op} (est {_fmt_k(est_val)} vs actual {_fmt_k(act_val)}, "
                        f"{ratio:.0f}x off)"
                    )
                    break  # Report only the worst

    return "\n".join(lines) if lines else _extract_generic_vital_signs(explain_text)


def _extract_duckdb_vital_signs(explain_text: str, plan_json: Optional[Any] = None) -> str:
    """Extract vital signs from DuckDB EXPLAIN."""
    lines = []

    # Try JSON first
    if plan_json:
        return _extract_duckdb_from_json(plan_json)

    # Text parsing fallback
    # 1. Wall-clock time
    m = re.search(r'Total execution time:\s*([\d.]+)ms', explain_text)
    if m:
        lines.append(f"Time: {float(m.group(1)):.0f}ms")

    # Look for bottleneck operators (highest time %)
    # DuckDB format: "  HASH_GROUP_BY  rows=5000  time=120.5ms (45%)"
    ops = re.findall(r'(\w+(?:_\w+)*)\s+.*?time=([\d.]+)ms\s*\((\d+)%\)', explain_text)
    if ops:
        sorted_ops = sorted(ops, key=lambda x: float(x[2]), reverse=True)
        top = sorted_ops[0]
        lines.append(f"Bottleneck: {top[0]} ({top[1]}ms, {top[2]}% of total)")
        if len(sorted_ops) > 1:
            second = sorted_ops[1]
            lines.append(f"Second: {second[0]} ({second[1]}ms, {second[2]}%)")

    # Cardinality misestimates
    liars = re.findall(r'(\w+(?:_\w+)*)\s+.*?est=([\d.]+[KkMm]?)\s+actual=([\d.]+[KkMm]?)', explain_text)
    if liars:
        for op, est, actual in liars:
            est_val = _parse_fmt_count(est)
            act_val = _parse_fmt_count(actual)
            if act_val > 0 and est_val > 0:
                ratio = max(act_val / est_val, est_val / act_val)
                if ratio > 10 and act_val > 10000:
                    lines.append(
                        f"Liar: {op} (est {est} vs actual {actual}, {ratio:.0f}x off)"
                    )

    return "\n".join(lines) if lines else _extract_generic_vital_signs(explain_text)


def _extract_duckdb_from_json(plan_json: Any) -> str:
    """Extract DuckDB vital signs from JSON plan."""
    if isinstance(plan_json, str):
        try:
            plan_json = json.loads(plan_json)
        except (json.JSONDecodeError, ValueError):
            return "Cannot parse DuckDB plan JSON."

    lines = []

    # Wall-clock time from top-level latency (seconds → ms)
    latency_s = plan_json.get("latency", 0) or 0
    if latency_s > 0:
        lines.append(f"Time: {latency_s * 1000:.0f}ms")

    # Walk the tree collecting operator stats
    nodes = []
    _collect_duckdb_nodes(plan_json, nodes)

    # Total operator time (fallback if latency not available)
    total_time_s = sum(n["timing"] for n in nodes)
    if not latency_s and total_time_s > 0:
        lines.append(f"Time: {total_time_s * 1000:.0f}ms (sum of operators)")

    # Use latency for percentage calculation, fall back to sum
    ref_time_s = latency_s if latency_s > 0 else total_time_s

    # Bottleneck — highest-time operator
    if nodes:
        sorted_nodes = sorted(nodes, key=lambda n: n["timing"], reverse=True)
        top = sorted_nodes[0]
        if top["timing"] > 0:
            pct = (top["timing"] / ref_time_s * 100) if ref_time_s > 0 else 0
            lines.append(
                f"Bottleneck: {top['name']} ({top['timing']*1000:.0f}ms, {pct:.0f}%)"
            )
            if len(sorted_nodes) > 1 and sorted_nodes[1]["timing"] > 0:
                second = sorted_nodes[1]
                pct2 = (second["timing"] / ref_time_s * 100) if ref_time_s > 0 else 0
                lines.append(
                    f"Second: {second['name']} ({second['timing']*1000:.0f}ms, {pct2:.0f}%)"
                )

    # Spill indicator
    temp_size = plan_json.get("system_peak_temp_dir_size", 0) or 0
    if temp_size > 0:
        lines.append(f"Spill: {temp_size / (1024**2):.0f}MB temp dir")

    # Cardinality liars — report worst 2 only (keeps coach prompt compact)
    liars = []
    for n in nodes:
        est = n["estimated_cardinality"]
        actual = n["cardinality"]
        if est > 0 and actual > 0:
            ratio = max(actual / est, est / actual)
            if ratio > 10 and actual > 10000:
                liars.append((ratio, n["name"], est, actual))
    liars.sort(reverse=True)
    for ratio, name, est, actual in liars[:2]:
        lines.append(
            f"Liar: {name} (est {_fmt_k(est)} vs actual {_fmt_k(actual)}, "
            f"{ratio:.0f}x off)"
        )

    return "\n".join(lines) if lines else "DuckDB plan: no actionable signals."


def _extract_snowflake_vital_signs(explain_text: str) -> str:
    """Extract vital signs from Snowflake EXPLAIN (text-based)."""
    lines = []
    # Snowflake EXPLAIN is limited — extract what we can
    m = re.search(r'partitionsTotal=(\d+)', explain_text)
    if m:
        lines.append(f"Partitions: {m.group(1)} total scanned")
    m = re.search(r'bytesAssigned=(\d+)', explain_text)
    if m:
        bytes_val = int(m.group(1))
        lines.append(f"Bytes scanned: {bytes_val / (1024**2):.0f}MB")
    return "\n".join(lines) if lines else _extract_generic_vital_signs(explain_text)


def _extract_generic_vital_signs(explain_text: str) -> str:
    """Fallback: extract first 8 non-empty lines as-is."""
    non_empty = [l.strip() for l in explain_text.strip().split("\n") if l.strip()]
    return "\n".join(non_empty[:8])


# ─── Helper functions ──────────────────────────────────────────────────

def _pg_aggregate_buffers(node: dict) -> tuple:
    """Recursively aggregate buffer stats across all PG plan nodes."""
    hit = node.get("Shared Hit Blocks", 0) or 0
    read = node.get("Shared Read Blocks", 0) or 0
    temp_w = node.get("Temp Written Blocks", 0) or 0

    for child in node.get("Plans", []):
        c_hit, c_read, c_temp = _pg_aggregate_buffers(child)
        hit += c_hit
        read += c_read
        temp_w += c_temp

    return hit, read, temp_w


def _pg_find_liar_node(node: dict, depth: int = 0) -> Optional[str]:
    """Find the node with the largest planned-vs-actual rows delta."""
    best_ratio = 10  # Minimum 10x off to report
    best_msg = None

    planned = node.get("Plan Rows", 0) or 0
    actual = node.get("Actual Rows", 0) or 0
    loops = node.get("Actual Loops", 1) or 1
    total_actual = actual * loops

    if planned > 0 and total_actual > 0:
        ratio = max(total_actual / planned, planned / total_actual)
        if ratio > best_ratio and total_actual > 1000:
            ntype = node.get("Node Type", "???")
            rel = node.get("Relation Name", "")
            rel_str = f" on {rel}" if rel else ""
            best_ratio = ratio
            best_msg = (
                f"Liar: {ntype}{rel_str} "
                f"(planned {_fmt_k(planned)} rows, actual {_fmt_k(total_actual)}, "
                f"{ratio:.0f}x off)"
            )

    for child in node.get("Plans", []):
        child_msg = _pg_find_liar_node(child, depth + 1)
        if child_msg:
            # Parse ratio from child message
            m = re.search(r'(\d+)x off', child_msg)
            if m and int(m.group(1)) > best_ratio:
                best_msg = child_msg
                best_ratio = int(m.group(1))

    return best_msg


def _pg_find_bottleneck(node: dict, total_ms: float, depth: int = 0) -> Optional[str]:
    """Find the single highest-cost operator in PG plan."""
    best_time = 0.0
    best_msg = None

    time_ms = node.get("Actual Total Time", 0) or 0
    ntype = node.get("Node Type", "???")
    rows = node.get("Actual Rows", 0) or 0
    loops = node.get("Actual Loops", 1) or 1

    # Exclusive time approximation (subtract children)
    child_time = sum(
        (c.get("Actual Total Time", 0) or 0) * (c.get("Actual Loops", 1) or 1)
        for c in node.get("Plans", [])
    )
    exclusive_ms = max(0, time_ms * loops - child_time)

    if exclusive_ms > best_time:
        best_time = exclusive_ms
        pct = (exclusive_ms / total_ms * 100) if total_ms > 0 else 0
        rel = node.get("Relation Name", "")
        rel_str = f" on {rel}" if rel else ""
        best_msg = f"Bottleneck: {ntype}{rel_str} ({exclusive_ms:.0f}ms, {pct:.0f}% of total, {_fmt_k(rows * loops)} rows)"

    for child in node.get("Plans", []):
        child_msg = _pg_find_bottleneck(child, total_ms, depth + 1)
        if child_msg:
            # Parse time from child message
            m = re.search(r'\((\d+)ms', child_msg)
            if m and float(m.group(1)) > best_time:
                best_msg = child_msg
                best_time = float(m.group(1))

    return best_msg


def _collect_duckdb_nodes(node: dict, result: list) -> None:
    """Recursively collect DuckDB plan nodes for analysis.

    DuckDB EXPLAIN (ANALYZE, FORMAT JSON) uses these field names:
      - operator_name: e.g. "HASH_JOIN", "SEQ_SCAN"
      - operator_timing: seconds (float)
      - operator_cardinality: actual rows produced
      - extra_info["Estimated Cardinality"]: planned rows (string, may have ~ prefix)
    """
    name = node.get("operator_name") or node.get("name")

    # Skip envelope nodes (top-level plan root has no operator_name)
    # and skip wrapper/noise operators
    skip = {"EXPLAIN_ANALYZE", "RESULT_COLLECTOR", "PROJECTION"}
    if name and name not in skip:
        # Extract estimated cardinality from extra_info
        extra = node.get("extra_info", {})
        est_card = 0
        if isinstance(extra, dict):
            est_str = extra.get("Estimated Cardinality", "")
            if est_str:
                try:
                    est_card = int(str(est_str).lstrip("~"))
                except (ValueError, TypeError):
                    est_card = 0

        result.append({
            "name": name,
            "timing": node.get("operator_timing", 0) or node.get("timing", 0) or 0,
            "cardinality": node.get("operator_cardinality", 0) or node.get("cardinality", 0) or 0,
            "estimated_cardinality": est_card,
        })

    for child in node.get("children", []):
        _collect_duckdb_nodes(child, result)


def _fmt_k(n: int) -> str:
    """Format a count with K/M suffix."""
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)


def _parse_fmt_count(s: str) -> int:
    """Parse a formatted count (e.g., '5.2K', '1.3M', '500')."""
    s = s.strip()
    if s.upper().endswith("M"):
        return int(float(s[:-1]) * 1_000_000)
    if s.upper().endswith("K"):
        return int(float(s[:-1]) * 1_000)
    try:
        return int(float(s))
    except ValueError:
        return 0

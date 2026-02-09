#!/usr/bin/env python3
"""Programmatic EXPLAIN plan analyzer → per-query SET LOCAL configs.

Walks PostgreSQL EXPLAIN JSON plans, detects performance bottlenecks
(sort spills, hash spills, JIT overhead, missing parallelism, etc.),
and generates per-query SET LOCAL config recommendations.

Input:  EXPLAIN plans from packages/qt-sql/ado/benchmarks/postgres_dsb/explains/sf10/
Output: research/pg_tuning_configs.json
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Project root
ROOT = Path(__file__).resolve().parents[2]
EXPLAIN_DIR = ROOT / "packages/qt-sql/ado/benchmarks/postgres_dsb/explains/sf10"
LLM_RESULTS = ROOT / "research/pg_tuner_llm_results.json"
OUTPUT_FILE = ROOT / "research/pg_tuning_configs.json"

# Add to path for pg_tuning imports
sys.path.insert(0, str(ROOT / "packages/qt-sql"))
sys.path.insert(0, str(ROOT / "packages/qt-shared"))

from ado.pg_tuning import validate_tuning_config, build_set_local_sql


@dataclass
class PlanAnalysis:
    """Collected metrics from recursive EXPLAIN plan walk."""
    sort_spills: list[dict] = field(default_factory=list)   # nodes with Sort Space Type == Disk
    hash_spills: list[dict] = field(default_factory=list)   # nodes with Hash Batches > 1
    sort_nodes: int = 0
    hash_nodes: int = 0
    gather_nodes: list[dict] = field(default_factory=list)  # Workers Planned vs Launched
    large_seqscans: list[dict] = field(default_factory=list)  # >500K rows, not Parallel Aware
    temp_read_blocks: int = 0     # from root node
    temp_written_blocks: int = 0  # from root node
    shared_read_blocks: int = 0   # from root node
    shared_hit_blocks: int = 0    # from root node
    jit_functions: int = 0
    jit_total_ms: float = 0.0
    execution_time_ms: float = 0.0
    has_gather_above: bool = False  # tracks if we've seen a Gather node as parent


def walk_plan(node: dict, analysis: PlanAnalysis, under_gather: bool = False) -> None:
    """Recursively walk EXPLAIN plan tree collecting metrics."""
    node_type = node.get("Node Type", "")

    # Track if this node is a Gather/Gather Merge
    is_gather = node_type in ("Gather", "Gather Merge")
    if is_gather:
        planned = node.get("Workers Planned", 0)
        launched = node.get("Workers Launched", 0)
        analysis.gather_nodes.append({
            "planned": planned,
            "launched": launched,
            "node_type": node_type,
        })

    # Sort spill detection
    if node_type == "Sort":
        analysis.sort_nodes += 1
        space_type = node.get("Sort Space Type", "")
        if space_type == "Disk":
            analysis.sort_spills.append({
                "space_used_kb": node.get("Sort Space Used", 0),
                "rows": node.get("Actual Rows", 0),
            })
        # Also check worker-level sort spills
        for worker in node.get("Workers", []):
            if worker.get("Sort Space Type") == "Disk":
                analysis.sort_spills.append({
                    "space_used_kb": worker.get("Sort Space Used", 0),
                    "rows": worker.get("Actual Rows", 0),
                    "worker": worker.get("Worker Number"),
                })

    # Hash spill detection
    if node_type == "Hash":
        analysis.hash_nodes += 1
        batches = node.get("Hash Batches", 1)
        if batches > 1:
            analysis.hash_spills.append({
                "batches": batches,
                "original_batches": node.get("Original Hash Batches", 1),
                "rows": node.get("Actual Rows", 0),
            })

    # Large sequential scans without parallelism
    if node_type == "Seq Scan":
        actual_rows = node.get("Actual Rows", 0)
        parallel_aware = node.get("Parallel Aware", False)
        if actual_rows > 500_000 and not parallel_aware and not under_gather:
            analysis.large_seqscans.append({
                "table": node.get("Relation Name", "unknown"),
                "rows": actual_rows,
                "parallel_aware": parallel_aware,
            })

    # Worker-level JIT
    for worker in node.get("Workers", []):
        jit = worker.get("JIT", {})
        if jit:
            timing = jit.get("Timing", {})
            # Worker JIT is already included in top-level JIT, don't double-count

    # Recurse into children
    for child in node.get("Plans", []):
        walk_plan(child, analysis, under_gather=under_gather or is_gather)


def analyze_plan(plan_data: dict) -> PlanAnalysis:
    """Analyze a complete EXPLAIN plan JSON file."""
    analysis = PlanAnalysis()

    plan_json = plan_data.get("plan_json")
    if not plan_json or not isinstance(plan_json, list) or len(plan_json) == 0:
        return analysis

    top = plan_json[0]
    root_plan = top.get("Plan", {})

    # Root-level buffer stats
    analysis.temp_read_blocks = root_plan.get("Temp Read Blocks", 0)
    analysis.temp_written_blocks = root_plan.get("Temp Written Blocks", 0)
    analysis.shared_read_blocks = root_plan.get("Shared Read Blocks", 0)
    analysis.shared_hit_blocks = root_plan.get("Shared Hit Blocks", 0)

    # Top-level JIT
    jit = top.get("JIT", {})
    if jit:
        analysis.jit_functions = jit.get("Functions", 0)
        timing = jit.get("Timing", {})
        analysis.jit_total_ms = timing.get("Total", 0.0)

    # Execution time
    analysis.execution_time_ms = top.get("Execution Time", 0.0)

    # Walk the plan tree
    walk_plan(root_plan, analysis)

    return analysis


def generate_config(query_id: str, analysis: PlanAnalysis) -> dict:
    """Generate SET LOCAL config based on 8 rules.

    Returns dict with params, rules_triggered, and reasoning.
    """
    params: dict[str, str] = {}
    rules: list[str] = []
    reasoning_parts: list[str] = []

    total_sort_hash_ops = analysis.sort_nodes + analysis.hash_nodes

    # Rule 1: Sort spill → work_mem sized by op count
    if analysis.sort_spills:
        if total_sort_hash_ops <= 2:
            wm = "1GB"
        elif total_sort_hash_ops <= 5:
            wm = "512MB"
        elif total_sort_hash_ops <= 10:
            wm = "256MB"
        else:
            wm = "128MB"
        params["work_mem"] = wm
        total_spill_kb = sum(s["space_used_kb"] for s in analysis.sort_spills)
        rules.append("sort_spill")
        reasoning_parts.append(
            f"Sort spill to disk: {len(analysis.sort_spills)} nodes, "
            f"~{total_spill_kb}kB total. {total_sort_hash_ops} sort/hash ops → work_mem={wm}"
        )

    # Rule 2: Hash spill → hash_mem_multiplier
    if analysis.hash_spills:
        max_batches = max(h["batches"] for h in analysis.hash_spills)
        hmm = min(8, max(2, max_batches // 2))
        params["hash_mem_multiplier"] = str(float(hmm))
        rules.append("hash_spill")
        reasoning_parts.append(
            f"Hash spill: {len(analysis.hash_spills)} nodes, max {max_batches} batches → "
            f"hash_mem_multiplier={hmm}"
        )
        # If batches > 16, also bump work_mem if not already set by sort spill
        if max_batches > 16 and "work_mem" not in params:
            params["work_mem"] = "256MB"
            reasoning_parts.append(f"  + work_mem=256MB (high hash batches={max_batches})")

    # Rule 3: Workers not launching (Launched < Planned)
    workers_not_launching = [
        g for g in analysis.gather_nodes if g["launched"] < g["planned"]
    ]
    if workers_not_launching:
        params["parallel_setup_cost"] = "100.0"
        params["parallel_tuple_cost"] = "0.001"
        rules.append("workers_not_launching")
        reasoning_parts.append(
            f"Workers not launching: {len(workers_not_launching)} gather nodes with "
            f"launched < planned → parallel costs reduced"
        )

    # Rule 4: Missing parallelism — large SeqScan without Gather above
    if analysis.large_seqscans:
        params["max_parallel_workers_per_gather"] = "4"
        rules.append("missing_parallelism")
        tables = [s["table"] for s in analysis.large_seqscans]
        reasoning_parts.append(
            f"Large SeqScan without parallelism: {tables} → "
            f"max_parallel_workers_per_gather=4"
        )

    # Rule 5: JIT overhead — disable JIT if overhead is significant
    # or if absolute JIT time > 500ms (even if small %)
    if analysis.execution_time_ms > 0 and analysis.jit_total_ms > 0:
        jit_pct = analysis.jit_total_ms / analysis.execution_time_ms
        jit_flag = (
            jit_pct > 0.05
            or (jit_pct > 0.02 and analysis.execution_time_ms < 10_000)
            or analysis.jit_total_ms > 500
        )
        if jit_flag:
            params["jit"] = "off"
            rules.append("jit_overhead")
            reasoning_parts.append(
                f"JIT overhead: {analysis.jit_total_ms:.0f}ms = "
                f"{jit_pct*100:.1f}% of {analysis.execution_time_ms:.0f}ms exec → jit=off"
            )

    # Rule 6: Temp I/O heavy
    if analysis.temp_read_blocks > 1000:
        if "work_mem" not in params:
            # Set work_mem based on op count
            if total_sort_hash_ops <= 2:
                params["work_mem"] = "1GB"
            elif total_sort_hash_ops <= 5:
                params["work_mem"] = "512MB"
            else:
                params["work_mem"] = "256MB"
        params["effective_cache_size"] = "48GB"
        rules.append("temp_io_heavy")
        reasoning_parts.append(
            f"Temp I/O: {analysis.temp_read_blocks} read blocks, "
            f"{analysis.temp_written_blocks} written blocks → "
            f"work_mem bump + effective_cache_size=48GB"
        )

    # Rule 7: Cold buffer ratio (Shared Read > 10x Shared Hit)
    if (analysis.shared_read_blocks > 0 and analysis.shared_hit_blocks > 0 and
            analysis.shared_read_blocks > 10 * analysis.shared_hit_blocks):
        if "effective_cache_size" not in params:
            params["effective_cache_size"] = "48GB"
            rules.append("cold_buffers")
            reasoning_parts.append(
                f"Cold buffers: {analysis.shared_read_blocks} reads vs "
                f"{analysis.shared_hit_blocks} hits (ratio "
                f"{analysis.shared_read_blocks/analysis.shared_hit_blocks:.0f}x) → "
                f"effective_cache_size=48GB"
            )

    return {
        "params": params,
        "rules_triggered": rules,
        "reasoning": "; ".join(reasoning_parts) if reasoning_parts else "No bottlenecks detected",
    }


def generate_template_config(query_id: str) -> dict:
    """Template config for queries without usable EXPLAIN plans (timeouts).

    Does NOT force max_parallel_workers_per_gather — reducing parallel costs
    already encourages parallelism where the planner thinks it helps, without
    forcing overhead on fast rewrites (Q039: 187ms → 1782ms with forced 4 workers).
    """
    params = {
        "work_mem": "256MB",
        "jit": "off",
        "parallel_setup_cost": "100.0",
        "parallel_tuple_cost": "0.001",
    }
    return {
        "params": params,
        "rules_triggered": ["template_no_explain"],
        "reasoning": "No EXPLAIN plan available (timeout/error). Applying template config.",
    }


def main():
    print("=" * 70)
    print("PostgreSQL Config Tuner — EXPLAIN Plan Analyzer")
    print("=" * 70)

    # Load all EXPLAIN plans
    explain_files = sorted(EXPLAIN_DIR.glob("*.json"))
    print(f"\nFound {len(explain_files)} EXPLAIN plans in {EXPLAIN_DIR}")

    configs = {}
    rule_counts: dict[str, int] = {}
    no_config_count = 0
    template_count = 0

    for ef in explain_files:
        query_id = ef.stem
        plan_data = json.loads(ef.read_text())

        # Check if this is a stub/error plan (no usable EXPLAIN data)
        plan_json = plan_data.get("plan_json")
        is_stub = (
            plan_json is None
            or (isinstance(plan_json, list) and len(plan_json) == 0)
            or plan_data.get("execution_time_ms", 0) == 0
        )

        if is_stub:
            config = generate_template_config(query_id)
            template_count += 1
        else:
            analysis = analyze_plan(plan_data)
            config = generate_config(query_id, analysis)

        # Validate params through whitelist
        validated = validate_tuning_config(config["params"])
        config["config_cmds"] = build_set_local_sql(validated)
        config["params"] = validated

        if validated:
            configs[query_id] = config
        else:
            configs[query_id] = {
                "params": {},
                "config_cmds": [],
                "rules_triggered": [],
                "reasoning": "No bottlenecks detected",
            }
            no_config_count += 1

        for rule in config.get("rules_triggered", []):
            rule_counts[rule] = rule_counts.get(rule, 0) + 1

    # Summary
    configured = sum(1 for c in configs.values() if c["params"])
    print(f"\nResults:")
    print(f"  Queries with config:    {configured}")
    print(f"  Queries without config: {no_config_count}")
    print(f"  Template configs:       {template_count}")
    print(f"\nRules triggered:")
    for rule, count in sorted(rule_counts.items(), key=lambda x: -x[1]):
        print(f"  {rule:30s} {count:3d}")

    # Cross-check against LLM results
    if LLM_RESULTS.exists():
        print(f"\n{'='*70}")
        print("Cross-check vs LLM-generated configs:")
        print("=" * 70)
        llm_data = json.loads(LLM_RESULTS.read_text())
        for qid, llm_cfg in llm_data.items():
            our_cfg = configs.get(qid, {})
            our_params = our_cfg.get("params", {})
            llm_params = llm_cfg.get("params", {})

            # Compare
            match_keys = set(our_params.keys()) & set(llm_params.keys())
            our_only = set(our_params.keys()) - set(llm_params.keys())
            llm_only = set(llm_params.keys()) - set(our_params.keys())

            print(f"\n  {qid}:")
            print(f"    LLM verdict: {llm_cfg.get('verdict', '?')}, "
                  f"speedup: {llm_cfg.get('config_speedup', '?')}")
            print(f"    Rules:    {our_cfg.get('rules_triggered', [])}")
            if match_keys:
                for k in sorted(match_keys):
                    match = "✓" if our_params[k] == llm_params[k] else "≠"
                    print(f"    {match} {k}: ours={our_params[k]}, llm={llm_params[k]}")
            if our_only:
                print(f"    + Our extras: {dict((k, our_params[k]) for k in our_only)}")
            if llm_only:
                print(f"    - LLM extras: {dict((k, llm_params[k]) for k in llm_only)}")
            if not llm_params and not our_params:
                print(f"    Both: no config")

    # Write output
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(configs, indent=2) + "\n")
    print(f"\nWrote {len(configs)} configs to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()

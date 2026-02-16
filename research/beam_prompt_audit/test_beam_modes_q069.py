"""Test both beam modes on Q069_multi_i1.

Swarm got 17.48x, beam got 1.08x. Baseline 14,517ms.
This script generates prompts for BOTH modes — no LLM calls.
"""

import json
import sys
import os
from pathlib import Path

sys.path.insert(0, "packages/qt-shared")
sys.path.insert(0, "packages/qt-sql")
sys.path.insert(0, ".")

OUT_DIR = Path("research/beam_prompt_audit")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Load Q069 data ──────────────────────────────────────────────────────────

QUERY_PATH = "packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/queries/query069_multi_i1.sql"
EXPLAIN_PATH = "packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/explains/query069_multi_i1.json"

with open(QUERY_PATH) as f:
    original_sql = f.read().strip()

with open(EXPLAIN_PATH) as f:
    explain_data = json.load(f)

plan_json = explain_data["plan_json"]

# Format EXPLAIN to readable text
from qt_sql.prompts.analyst_briefing import format_pg_explain_tree
explain_text = format_pg_explain_tree(plan_json)

print("=" * 80)
print("Q069_multi_i1 — Test Both Beam Modes")
print("=" * 80)
print(f"\nSQL length: {len(original_sql)} chars")
print(f"EXPLAIN length: {len(explain_text)} chars")
print(f"Execution time: {explain_data['plan_json'][0].get('Execution Time', '?')}ms")

# ── Generate real IR node map ────────────────────────────────────────────────

print("\n" + "=" * 80)
print("IR Generation")
print("=" * 80)

from qt_sql.ir import build_script_ir, render_ir_node_map, Dialect

script_ir = build_script_ir(original_sql, Dialect.POSTGRES)
ir_node_map = render_ir_node_map(script_ir)

print(f"\nIR node map: {len(ir_node_map)} chars, {len(ir_node_map.split(chr(10)))} lines")
print(f"\n{ir_node_map}")

# ── STEP 1: AST Front Gate ──────────────────────────────────────────────────

print("\n" + "=" * 80)
print("STEP 1: AST Front Gate (filter_applicable_transforms)")
print("=" * 80)

from qt_sql.patches.beam_wide_prompts import (
    filter_applicable_transforms,
    build_wide_analyst_prompt,
    build_wide_strike_prompt,
    ProbeSpec,
)

applicable = filter_applicable_transforms(
    sql=original_sql,
    engine="postgresql",
    dialect="postgres",
    min_overlap=0.4,  # lower threshold to see more candidates
)

print(f"\n{len(applicable)} transforms survived AST front gate (>= 40% overlap):\n")
for t in applicable:
    contras = [c.get("instruction", "")[:60] for c in t.contraindications]
    contra_str = f" ! {contras[0]}" if contras else ""
    print(f"  {t.overlap_ratio:5.0%}  {t.id:<40s} Family {t.family}{contra_str}")

# Show which would survive at 50% (default threshold)
n_50 = sum(1 for t in applicable if t.overlap_ratio >= 0.50)
print(f"\n  -> {n_50} survive at 50% threshold (default)")

# ── STEP 2: Beam WIDE — Analyst Prompt ───────────────────────────────────────

print("\n" + "=" * 80)
print("STEP 2: Beam WIDE — Analyst Prompt")
print("=" * 80)

# Use 50% threshold for the actual prompt
applicable_50 = [t for t in applicable if t.overlap_ratio >= 0.50]

# Load gold examples for wide analyst
from qt_sql.patches.beam_wide_prompts import _load_gold_example_for_family as _load_wide_ex
wide_gold_examples = {}
for fam in ["A", "B", "C", "D", "E", "F"]:
    ex = _load_wide_ex(fam, "postgres")
    if ex:
        wide_gold_examples[fam] = ex

wide_analyst_prompt = build_wide_analyst_prompt(
    query_id="query069_multi_i1",
    original_sql=original_sql,
    explain_text=explain_text,
    ir_node_map=ir_node_map,
    applicable_transforms=applicable_50,
    gold_examples=wide_gold_examples,
    dialect="postgres",
    # intelligence_brief NOT passed — _build_prompt_body loads it internally
    # via _load_engine_intelligence(). Passing it here would double-inject.
)

print(f"\nWide analyst prompt: {len(wide_analyst_prompt)} chars, {len(wide_analyst_prompt.split(chr(10)))} lines")
print(f"Transforms in prompt: {len(applicable_50)}")
print(f"Gold examples loaded: {list(wide_gold_examples.keys())}")

# Quick duplication check
playbook_count = wide_analyst_prompt.count("Engine Playbook")
print(f"'Engine Playbook' appears: {playbook_count}x {'(OK)' if playbook_count == 1 else '(DUPLICATE!)'}")

with open(OUT_DIR / "wide_analyst.txt", "w") as f:
    f.write(wide_analyst_prompt)
print(f"-> Saved to {OUT_DIR}/wide_analyst.txt")

# ── STEP 3: Beam WIDE — Example Strike Prompt ──────────────────────────────

print("\n" + "=" * 80)
print("STEP 3: Beam WIDE — Example Strike Worker Prompt")
print("=" * 80)

# Create a sample probe (as if analyst returned it)
if applicable_50:
    sample_t = applicable_50[0]
    sample_probe = ProbeSpec(
        probe_id="p01",
        transform_id=sample_t.id,
        family=sample_t.family,
        target=f"Convert the NOT EXISTS (web_sales, date_dim) correlated subquery into a MATERIALIZED CTE with DISTINCT ws_bill_customer_sk, then replace NOT EXISTS with LEFT JOIN ... IS NULL anti-pattern",
        confidence=0.9,
    )

    # Load gold patch plan for this family
    from qt_sql.patches.beam_wide_prompts import _load_gold_example_for_family
    gold_ex = _load_gold_example_for_family(sample_t.family, "postgres")
    gold_patch_plan = gold_ex.get("patch_plan") if gold_ex else None

    strike_prompt = build_wide_strike_prompt(
        original_sql=original_sql,
        ir_node_map=ir_node_map,
        hypothesis="The bottleneck is the Nested Loop Anti joins scanning 343M + 94M rows for NOT EXISTS checks against web_sales and catalog_sales. Each correlated subquery does a full materialized scan per customer row.",
        probe=sample_probe,
        gold_patch_plan=gold_patch_plan,
        dialect="postgres",
    )

    print(f"\nStrike prompt: {len(strike_prompt)} chars, {len(strike_prompt.split(chr(10)))} lines")
    print(f"Transform: {sample_probe.transform_id} (Family {sample_probe.family})")
    print(f"Gold patch plan loaded: {'yes' if gold_patch_plan else 'no'}")

    with open(OUT_DIR / "wide_strike.txt", "w") as f:
        f.write(strike_prompt)
    print(f"-> Saved to {OUT_DIR}/wide_strike.txt")

# ── STEP 4: Beam FOCUSED — Analyst Prompt ───────────────────────────────────

print("\n" + "=" * 80)
print("STEP 4: Beam FOCUSED — Analyst Prompt (tiered)")
print("=" * 80)

from qt_sql.patches.beam_focused_prompts import (
    build_focused_analyst_prompt,
    build_focused_strike_prompt,
    build_focused_sniper_prompt,
    FocusedTarget,
    SortieResult,
)

# Load gold examples for focused mode (needs dict format)
from qt_sql.patches.beam_wide_prompts import _load_gold_example_for_family
gold_examples = {}
for fam in ["A", "B", "C", "D", "E", "F"]:
    ex = _load_gold_example_for_family(fam, "postgres")
    if ex:
        gold_examples[fam] = ex

focused_analyst_prompt = build_focused_analyst_prompt(
    query_id="query069_multi_i1",
    original_sql=original_sql,
    explain_text=explain_text,
    ir_node_map=ir_node_map,
    gold_examples=gold_examples,
    dialect="postgres",
    # intelligence_brief NOT passed — same reason as wide
)

print(f"\nFocused analyst prompt: {len(focused_analyst_prompt)} chars, {len(focused_analyst_prompt.split(chr(10)))} lines")
print(f"Gold examples loaded: {list(gold_examples.keys())}")

# Quick duplication check
playbook_count_f = focused_analyst_prompt.count("Engine Playbook")
print(f"'Engine Playbook' appears: {playbook_count_f}x {'(OK)' if playbook_count_f == 1 else '(DUPLICATE!)'}")

with open(OUT_DIR / "focused_analyst.txt", "w") as f:
    f.write(focused_analyst_prompt)
print(f"-> Saved to {OUT_DIR}/focused_analyst.txt")

# ── STEP 5: Beam FOCUSED — Strike Prompt ───────────────────────────────────

print("\n" + "=" * 80)
print("STEP 5: Beam FOCUSED — Example R1 Strike Prompt")
print("=" * 80)

# Simulate a target from the analyst
sample_target = FocusedTarget(
    target_id="t1",
    family="B",
    transform="shared_scan_decorrelate",
    relevance_score=0.95,
    hypothesis=(
        "The Nested Loop Anti joins (lines 3-4) scan 343M + 94M rows for "
        "NOT EXISTS checks. The web_sales and catalog_sales Materialize nodes "
        "re-read 83K and 332K rows per outer loop. Converting correlated "
        "subqueries into pre-materialized CTEs with semi-join eliminates "
        "the per-row re-scan overhead."
    ),
    target_ir=(
        "S0 [SELECT cd_gender, cd_marital_status, cd_education_status, ...]\n"
        "  CTE: store_buyers = SELECT DISTINCT ss_customer_sk FROM store_sales JOIN date_dim ...\n"
        "  CTE: web_buyers = SELECT DISTINCT ws_bill_customer_sk FROM web_sales JOIN date_dim ...\n"
        "  CTE: catalog_buyers = SELECT DISTINCT cs_ship_customer_sk FROM catalog_sales JOIN date_dim ...\n"
        "  MAIN: customer JOIN customer_address JOIN customer_demographics\n"
        "    WHERE c_customer_sk IN store_buyers\n"
        "      AND c_customer_sk NOT IN web_buyers\n"
        "      AND c_customer_sk NOT IN catalog_buyers\n"
        "    GROUP BY ... ORDER BY ... LIMIT 100"
    ),
    recommended_examples=["shared_scan_decorrelate", "inline_decorrelate"],
)

# Load gold examples for this target
focused_gold = []
for ex_id in sample_target.recommended_examples:
    from qt_sql.patches.beam_wide_prompts import _load_gold_example_by_id
    ex = _load_gold_example_by_id(ex_id, "postgres")
    if ex:
        focused_gold.append(ex)

focused_strike_prompt = build_focused_strike_prompt(
    original_sql=original_sql,
    explain_text=explain_text,
    target=sample_target,
    gold_examples=focused_gold,
    dialect="postgres",
    engine_version="14.3",
)

print(f"\nFocused strike prompt: {len(focused_strike_prompt)} chars, {len(focused_strike_prompt.split(chr(10)))} lines")
print(f"Gold examples loaded: {len(focused_gold)} ({[e.get('id','?') for e in focused_gold]})")

with open(OUT_DIR / "focused_strike.txt", "w") as f:
    f.write(focused_strike_prompt)
print(f"-> Saved to {OUT_DIR}/focused_strike.txt")

# ── STEP 6: Beam FOCUSED — Sniper Prompt ───────────────────────────────────

print("\n" + "=" * 80)
print("STEP 6: Beam FOCUSED — Sniper Prompt (V4 protocol)")
print("=" * 80)

# Simulate sortie 0 results
sortie0 = SortieResult(
    sortie=0,
    strikes=[
        {"strike_id": "t1", "family": "B", "transform": "shared_scan_decorrelate",
         "speedup": 1.35, "status": "WIN"},
        {"strike_id": "t2", "family": "A", "transform": "early_filter_decorrelate",
         "speedup": 1.12, "status": "IMPROVED"},
        {"strike_id": "t3", "family": "E", "transform": "materialize_cte",
         "speedup": 0.95, "status": "REGRESSION"},
        {"strike_id": "t4", "family": "C", "transform": "single_pass_aggregation",
         "speedup": None, "status": "FAIL", "error": "column cd_purchase_estimate not found"},
    ],
    explains={
        "t1": "Limit (actual rows=80)\n  GroupAggregate (actual rows=80)\n    Sort (actual rows=964)\n      Hash Semi Join (actual rows=964)\n        Hash Anti Join (actual rows=1105)\n          Hash Anti Join (actual rows=1128)\n            ...",
        "t2": "Limit (actual rows=80)\n  GroupAggregate (actual rows=80)\n    Sort (actual rows=964)\n      Nested Loop Anti (rows removed=210M)\n        ...",
    },
)

focused_sniper_prompt = build_focused_sniper_prompt(
    query_id="query069_multi_i1",
    original_sql=original_sql,
    original_explain=explain_text,
    sortie_history=[sortie0],
    gold_examples=gold_examples,
    dialect="postgres",
    # intelligence_brief NOT passed
)

print(f"\nFocused sniper prompt: {len(focused_sniper_prompt)} chars, {len(focused_sniper_prompt.split(chr(10)))} lines")

with open(OUT_DIR / "focused_sniper.txt", "w") as f:
    f.write(focused_sniper_prompt)
print(f"-> Saved to {OUT_DIR}/focused_sniper.txt")

# ── STEP 7: Workload Router ────────────────────────────────────────────────

print("\n" + "=" * 80)
print("STEP 7: Workload Router")
print("=" * 80)

from qt_sql.patches.beam_router import classify_workload, BeamMode

# Simulate a batch with Q069 as heavy
baselines = {
    "query069_multi_i1": 14517.0,  # heavy
    "query032_i1": 45000.0,        # heaviest
    "query001_i1": 8500.0,         # medium
    "query092_i1": 3200.0,         # medium
    "query050_i1": 200.0,          # light
    "query010_i1": 150.0,          # light
    "query020_i1": 80.0,           # light
    "query030_i1": 50.0,           # light
}

assignments = classify_workload(baselines, mode="auto")

print(f"\nTotal workload: {sum(baselines.values()):.0f}ms")
print(f"\nAssignments:")
for qid, a in sorted(assignments.items(), key=lambda x: -x[1].baseline_ms):
    mode_icon = ">" if a.mode == BeamMode.FOCUSED else " "
    print(f"  {mode_icon} {a.mode.value:8s} {qid:<25s} {a.baseline_ms:>8.0f}ms ({a.workload_pct:>5.1f}%)")

n_focused = sum(1 for a in assignments.values() if a.mode == BeamMode.FOCUSED)
n_wide = sum(1 for a in assignments.values() if a.mode == BeamMode.WIDE)
focused_ms = sum(a.baseline_ms for a in assignments.values() if a.mode == BeamMode.FOCUSED)
wide_ms = sum(a.baseline_ms for a in assignments.values() if a.mode == BeamMode.WIDE)
total = sum(baselines.values())

print(f"\n  FOCUSED: {n_focused} queries ({focused_ms:.0f}ms = {focused_ms/total*100:.0f}% of workload)")
print(f"  WIDE:    {n_wide} queries ({wide_ms:.0f}ms = {wide_ms/total*100:.0f}% of workload)")

# ── Summary ─────────────────────────────────────────────────────────────────

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"""
Generated prompts for Q069_multi_i1 (baseline 14,517ms):

  BEAM WIDE:
    Analyst prompt:  {len(wide_analyst_prompt):>6,} chars  -> {OUT_DIR}/wide_analyst.txt
    Strike prompt:   {len(strike_prompt):>6,} chars  -> {OUT_DIR}/wide_strike.txt

  BEAM FOCUSED:
    Analyst prompt:  {len(focused_analyst_prompt):>6,} chars  -> {OUT_DIR}/focused_analyst.txt
    Strike prompt:   {len(focused_strike_prompt):>6,} chars  -> {OUT_DIR}/focused_strike.txt
    Sniper prompt:   {len(focused_sniper_prompt):>6,} chars  -> {OUT_DIR}/focused_sniper.txt

  IR node map: {len(ir_node_map)} chars
  AST Front Gate: {len(applicable_50)} transforms at 50%+ overlap
  Router: Q069 -> {'FOCUSED' if assignments.get('query069_multi_i1', None) and assignments['query069_multi_i1'].mode == BeamMode.FOCUSED else 'WIDE'}
""")

"""Test full beam flow on Q069: BEAM + REASONING modes.

Renders all 4 prompt types without LLM calls:
  1. beam_dispatcher.txt — qwen designs 8-16 probes
  2. beam_worker.txt    — qwen executes one probe → PatchPlan
  3. beam_sniper.txt    — R1 sees BDA + intelligence → 2 PatchPlans
  4. reasoning.txt      — R1 full intelligence → 2 PatchPlans

Pass --live to actually call the LLM (requires QT_DEEPSEEK_API_KEY).
"""

import json
import sys
import functools
from pathlib import Path

print = functools.partial(print, flush=True)

sys.path.insert(0, "packages/qt-shared")
sys.path.insert(0, "packages/qt-sql")
sys.path.insert(0, ".")

RENDERED_DIR = Path("packages/qt-sql/qt_sql/patches/rendered_prompts")
RENDERED_DIR.mkdir(parents=True, exist_ok=True)

# ── Load Q069 data ──────────────────────────────────────────────────────────

QUERY_PATH = "packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/queries/query069_multi_i1.sql"
EXPLAIN_PATH = "packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/explains/query069_multi_i1.json"

with open(QUERY_PATH) as f:
    original_sql = f.read().strip()

with open(EXPLAIN_PATH) as f:
    explain_data = json.load(f)

from qt_sql.prompts.analyst_briefing import format_pg_explain_tree
explain_text = format_pg_explain_tree(explain_data["plan_json"])

# ── Generate IR ─────────────────────────────────────────────────────────────

from qt_sql.ir import build_script_ir, render_ir_node_map, Dialect
script_ir = build_script_ir(original_sql, Dialect.POSTGRES)
ir_node_map = render_ir_node_map(script_ir)

# ── Load gold examples ──────────────────────────────────────────────────────

from qt_sql.patches.beam_wide_prompts import (
    build_beam_dispatcher_prompt,
    build_beam_worker_prompt,
    parse_scout_response,
    _load_gold_example_for_family,
    ProbeSpec,
)
from qt_sql.patches.beam_prompt_builder import (
    build_reasoning_prompt,
    build_beam_sniper_prompt,
)

gold_examples = {}
for fam in ["A", "B", "C", "D", "E", "F"]:
    ex = _load_gold_example_for_family(fam, "postgres")
    if ex:
        gold_examples[fam] = ex

print(f"Loaded {len(gold_examples)} gold example families: {list(gold_examples.keys())}")

# ══════════════════════════════════════════════════════════════════════════════
# PROMPT 1: BEAM Dispatcher (R1 designs 8-16 probes)
# ══════════════════════════════════════════════════════════════════════════════

print("\n[1/4] Rendering BEAM dispatcher prompt...")

dispatcher_prompt = build_beam_dispatcher_prompt(
    query_id="query069_multi_i1",
    original_sql=original_sql,
    explain_text=explain_text,
    ir_node_map=ir_node_map,
    gold_examples=gold_examples,
    dialect="postgres",
)

out_path = RENDERED_DIR / "beam_dispatcher.txt"
out_path.write_text(dispatcher_prompt)
print(f"  -> {out_path} ({len(dispatcher_prompt)} chars)")

# ══════════════════════════════════════════════════════════════════════════════
# PROMPT 2: BEAM Worker (qwen executes one probe → PatchPlan)
# ══════════════════════════════════════════════════════════════════════════════

print("\n[2/4] Rendering BEAM worker prompt...")

# Mock probe for rendering
mock_probe = ProbeSpec(
    probe_id="p01",
    transform_id="decorrelate_not_exists_to_cte",
    family="B",
    target=(
        "Convert the NOT EXISTS correlated subquery into a MATERIALIZED CTE: "
        "SELECT DISTINCT customer_sk FROM web_sales JOIN date_dim WHERE d_year = 2002. "
        "Then replace NOT EXISTS with LEFT JOIN cte ... IS NULL anti-pattern."
    ),
    confidence=0.90,
)

gold_ex = gold_examples.get("B")
gold_patch_plan = gold_ex.get("patch_plan") if gold_ex else None

worker_prompt = build_beam_worker_prompt(
    original_sql=original_sql,
    ir_node_map=ir_node_map,
    hypothesis="The bottleneck is repeated correlated subqueries scanning store_sales per row.",
    probe=mock_probe,
    gold_patch_plan=gold_patch_plan,
    dialect="postgres",
)

out_path = RENDERED_DIR / "beam_worker.txt"
out_path.write_text(worker_prompt)
print(f"  -> {out_path} ({len(worker_prompt)} chars)")

# ══════════════════════════════════════════════════════════════════════════════
# PROMPT 3: BEAM Sniper (R1 sees BDA → 2 PatchPlans)
# ══════════════════════════════════════════════════════════════════════════════

print("\n[3/4] Rendering BEAM sniper prompt...")

# Mock strike results (BDA)
mock_strike_results = [
    {
        "probe_id": "p01",
        "transform_id": "decorrelate_not_exists_to_cte",
        "family": "B",
        "status": "WIN",
        "speedup": 1.45,
        "error": None,
        "explain_text": "HashJoin (actual rows=150, loops=1)\n  -> SeqScan store_sales (actual rows=28000)",
        "sql": "WITH cte AS (SELECT DISTINCT ...) SELECT ... FROM ... LEFT JOIN cte ...",
    },
    {
        "probe_id": "p02",
        "transform_id": "date_cte_isolate",
        "family": "A",
        "status": "PASS",
        "speedup": 1.02,
        "error": None,
        "explain_text": None,
        "sql": "WITH date_filter AS (...) SELECT ...",
    },
    {
        "probe_id": "p03",
        "transform_id": "comma_join_to_explicit",
        "family": "F",
        "status": "FAIL",
        "speedup": None,
        "error": "Syntax error near line 12",
        "explain_text": None,
        "sql": None,
    },
    {
        "probe_id": "p04",
        "transform_id": "or_to_union",
        "family": "D",
        "status": "WIN",
        "speedup": 1.30,
        "error": None,
        "explain_text": "Append (actual rows=200)\n  -> Index Scan (actual rows=100)\n  -> Index Scan (actual rows=100)",
        "sql": "SELECT ... UNION ALL SELECT ...",
    },
]

sniper_prompt = build_beam_sniper_prompt(
    query_id="query069_multi_i1",
    original_sql=original_sql,
    explain_text=explain_text,
    ir_node_map=ir_node_map,
    all_5_examples=gold_examples,
    dialect="postgres",
    strike_results=mock_strike_results,
)

out_path = RENDERED_DIR / "beam_sniper.txt"
out_path.write_text(sniper_prompt)
print(f"  -> {out_path} ({len(sniper_prompt)} chars)")

# ══════════════════════════════════════════════════════════════════════════════
# PROMPT 4: REASONING (R1 full intelligence → 2 PatchPlans)
# ══════════════════════════════════════════════════════════════════════════════

print("\n[4/4] Rendering REASONING prompt...")

reasoning_prompt = build_reasoning_prompt(
    query_id="query069_multi_i1",
    original_sql=original_sql,
    explain_text=explain_text,
    ir_node_map=ir_node_map,
    all_5_examples=gold_examples,
    dialect="postgres",
)

out_path = RENDERED_DIR / "reasoning.txt"
out_path.write_text(reasoning_prompt)
print(f"  -> {out_path} ({len(reasoning_prompt)} chars)")

# ══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("RENDERED PROMPTS:")
print("=" * 60)
for p in sorted(RENDERED_DIR.glob("*.txt")):
    size = p.stat().st_size
    print(f"  {p.name:30s} {size:>6,} chars")
print(f"\nAll outputs saved to {RENDERED_DIR}/")

"""Fire both beam analyst prompts at the LLM on Q069_multi_i1.

Sends wide analyst + focused analyst prompts, saves raw responses.
No validation, no workers — just analyst R1 comparison.
"""

import json
import sys
import time
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

from qt_sql.prompts.analyst_briefing import format_pg_explain_tree
explain_text = format_pg_explain_tree(explain_data["plan_json"])

# ── Generate IR ─────────────────────────────────────────────────────────────

from qt_sql.ir import build_script_ir, render_ir_node_map, Dialect
script_ir = build_script_ir(original_sql, Dialect.POSTGRES)
ir_node_map = render_ir_node_map(script_ir)

# ── Load gold examples ──────────────────────────────────────────────────────

from qt_sql.patches.beam_wide_prompts import (
    filter_applicable_transforms,
    build_wide_analyst_prompt,
    _load_gold_example_for_family,
)
from qt_sql.patches.beam_focused_prompts import build_focused_analyst_prompt

gold_examples = {}
for fam in ["A", "B", "C", "D", "E", "F"]:
    ex = _load_gold_example_for_family(fam, "postgres")
    if ex:
        gold_examples[fam] = ex

# ── AST front gate (for wide mode) ─────────────────────────────────────────

applicable = filter_applicable_transforms(
    sql=original_sql, engine="postgresql", dialect="postgres", min_overlap=0.5,
)

# ── Build both prompts ──────────────────────────────────────────────────────

wide_prompt = build_wide_analyst_prompt(
    query_id="query069_multi_i1",
    original_sql=original_sql,
    explain_text=explain_text,
    ir_node_map=ir_node_map,
    applicable_transforms=applicable,
    gold_examples=gold_examples,
    dialect="postgres",
)

focused_prompt = build_focused_analyst_prompt(
    query_id="query069_multi_i1",
    original_sql=original_sql,
    explain_text=explain_text,
    ir_node_map=ir_node_map,
    gold_examples=gold_examples,
    dialect="postgres",
)

print(f"Wide analyst:    {len(wide_prompt):>6,} chars")
print(f"Focused analyst: {len(focused_prompt):>6,} chars")

# ── LLM setup ──────────────────────────────────────────────────────────────

from qt_shared.config import get_settings
settings = get_settings()

from qt_sql.generate import CandidateGenerator
gen = CandidateGenerator(provider=settings.llm_provider, model=settings.llm_model)

print(f"\nProvider: {settings.llm_provider}")
print(f"Model:    {settings.llm_model}")

# ── Fire wide analyst ──────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("FIRING: Wide Analyst (8-16 probes)")
print("=" * 60)

t0 = time.time()
wide_response = gen._analyze(wide_prompt)
wide_elapsed = time.time() - t0

print(f"Response: {len(wide_response)} chars in {wide_elapsed:.1f}s")

with open(OUT_DIR / "wide_analyst_response.txt", "w") as f:
    f.write(wide_response)
print(f"-> {OUT_DIR}/wide_analyst_response.txt")

# ── Fire focused analyst ───────────────────────────────────────────────────

print("\n" + "=" * 60)
print("FIRING: Focused Analyst (1-4 deep targets)")
print("=" * 60)

t0 = time.time()
focused_response = gen._analyze(focused_prompt)
focused_elapsed = time.time() - t0

print(f"Response: {len(focused_response)} chars in {focused_elapsed:.1f}s")

with open(OUT_DIR / "focused_analyst_response.txt", "w") as f:
    f.write(focused_response)
print(f"-> {OUT_DIR}/focused_analyst_response.txt")

# ── Quick comparison ───────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("COMPARISON")
print("=" * 60)

# Try to parse probe/target counts
import re

def count_json_items(text, key):
    """Count items in a JSON array field."""
    match = re.search(r'"' + key + r'"\s*:\s*\[', text)
    if not match:
        return 0
    # Count objects in array
    start = match.end()
    depth = 1
    items = 0
    for c in text[start:]:
        if c == '{':
            if depth == 1:
                items += 1
            depth += 1
        elif c == '}':
            depth -= 1
        elif c == ']' and depth == 1:
            break
    return items

wide_probes = count_json_items(wide_response, "probes")
focused_targets = count_json_items(focused_response, "targets")

print(f"""
  Wide:     {wide_probes} probes,   {len(wide_response):>6,} chars, {wide_elapsed:.1f}s
  Focused:  {focused_targets} targets, {len(focused_response):>6,} chars, {focused_elapsed:.1f}s

  Responses saved to {OUT_DIR}/
""")

"""Test full beam flow on Q069: analyst → workers → sniper.

Tests BOTH modes end-to-end without validation/benchmarking.
Just checks if the LLM calls produce reasonable SQL.
"""

import json
import sys
import time
import functools
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Force unbuffered output so we can monitor progress
print = functools.partial(print, flush=True)

sys.path.insert(0, "packages/qt-shared")
sys.path.insert(0, "packages/qt-sql")
sys.path.insert(0, ".")

OUT_DIR = Path("research/beam_prompt_audit/full_flow")
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
    build_wide_strike_prompt,
    build_wide_sniper_prompt,
    parse_scout_response,
    _load_gold_example_for_family,
    StrikeBDA,
)
from qt_sql.patches.beam_focused_prompts import (
    build_focused_analyst_prompt,
    build_focused_strike_prompt,
    build_focused_sniper_prompt,
)

gold_examples = {}
for fam in ["A", "B", "C", "D", "E", "F"]:
    ex = _load_gold_example_for_family(fam, "postgres")
    if ex:
        gold_examples[fam] = ex

# ── LLM setup ──────────────────────────────────────────────────────────────

from qt_shared.config import get_settings
settings = get_settings()

from qt_sql.generate import CandidateGenerator
analyst_gen = CandidateGenerator(provider=settings.llm_provider, model="deepseek/deepseek-r1")
worker_gen = CandidateGenerator(provider=settings.llm_provider, model="qwen/qwen-2.5-coder-32b-instruct")

print(f"Provider: {settings.llm_provider}")
print(f"Analyst:  deepseek-r1")
print(f"Worker:   qwen-2.5-coder-32b")

# ══════════════════════════════════════════════════════════════════════════════
# WIDE MODE: analyst → qwen workers → sniper
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("WIDE MODE: Analyst → Workers → Sniper")
print("=" * 80)

# ── Phase 1: Wide Analyst ───────────────────────────────────────────────────

print("\n[WIDE ANALYST] Designing 8-16 probes...")

applicable = filter_applicable_transforms(
    sql=original_sql, engine="postgresql", dialect="postgres", min_overlap=0.5,
)

wide_analyst_prompt = build_wide_analyst_prompt(
    query_id="query069_multi_i1",
    original_sql=original_sql,
    explain_text=explain_text,
    ir_node_map=ir_node_map,
    applicable_transforms=applicable,
    gold_examples=gold_examples,
    dialect="postgres",
)

t0 = time.time()
wide_analyst_response = analyst_gen._analyze(wide_analyst_prompt)
wide_analyst_time = time.time() - t0

with open(OUT_DIR / "wide_analyst.txt", "w") as f:
    f.write(wide_analyst_response)

scout_result = parse_scout_response(wide_analyst_response)
if not scout_result or not scout_result.probes:
    print("ERROR: No probes returned")
    sys.exit(1)

probes = scout_result.probes[:12]  # limit to 12
print(f"  → {len(probes)} probes in {wide_analyst_time:.1f}s")
for p in probes:
    print(f"     {p.probe_id}: {p.transform_id} (Family {p.family}, conf {p.confidence:.0%})")

# ── Phase 2: Wide Workers (parallel qwen) ───────────────────────────────────

print(f"\n[WIDE WORKERS] Firing {len(probes)} qwen workers in parallel...")

wide_worker_results = []

with ThreadPoolExecutor(max_workers=8) as pool:
    futures = {}
    for probe in probes:
        gold_ex = gold_examples.get(probe.family)
        gold_patch_plan = gold_ex.get("patch_plan") if gold_ex else None

        worker_prompt = build_wide_strike_prompt(
            original_sql=original_sql,
            ir_node_map=ir_node_map,
            hypothesis=scout_result.hypothesis,
            probe=probe,
            gold_patch_plan=gold_patch_plan,
            dialect="postgres",
        )

        future = pool.submit(worker_gen._analyze, worker_prompt)
        futures[future] = probe

    for future in as_completed(futures):
        probe = futures[future]
        try:
            response = future.result(timeout=300)
            wide_worker_results.append({
                "probe_id": probe.probe_id,
                "transform_id": probe.transform_id,
                "family": probe.family,
                "response": response,
                "status": "RESPONSE",
            })
            # Save individual worker response
            with open(OUT_DIR / f"wide_worker_{probe.probe_id}.txt", "w") as f:
                f.write(response)
            print(f"  ✓ {probe.probe_id}: {len(response)} chars")
        except Exception as e:
            print(f"  ✗ {probe.probe_id}: {e}")
            wide_worker_results.append({
                "probe_id": probe.probe_id,
                "transform_id": probe.transform_id,
                "family": probe.family,
                "error": str(e),
                "status": "ERROR",
            })

# ── Phase 3: Wide Sniper (synthesize) ───────────────────────────────────────

print(f"\n[WIDE SNIPER] Synthesizing compound rewrites from {len(wide_worker_results)} results...")

# Mock BDA results (we don't have real benchmarks)
strike_bdas = []
for r in wide_worker_results:
    bda = StrikeBDA(
        probe_id=r["probe_id"],
        transform_id=r["transform_id"],
        family=r["family"],
        status="PASS" if r["status"] == "RESPONSE" else "FAIL",
        speedup=1.0 if r["status"] == "RESPONSE" else None,
        error=r.get("error"),
        explain_text=None,  # no EXPLAIN in quick test
        sql=r.get("response"),
    )
    strike_bdas.append(bda)

sniper_prompt = build_wide_sniper_prompt(
    query_id="query069_multi_i1",
    original_sql=original_sql,
    original_explain=explain_text,
    hypothesis=scout_result.hypothesis,
    strike_results=strike_bdas,
    dialect="postgres",
)

t0 = time.time()
sniper_response = analyst_gen._analyze(sniper_prompt)
sniper_time = time.time() - t0

with open(OUT_DIR / "wide_sniper.txt", "w") as f:
    f.write(sniper_response)

print(f"  → Sniper response: {len(sniper_response)} chars in {sniper_time:.1f}s")

# ══════════════════════════════════════════════════════════════════════════════
# FOCUSED MODE: analyst → R1 workers → sniper
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("FOCUSED MODE: Analyst → Workers → Sniper")
print("=" * 80)

# ── Phase 1: Focused Analyst ────────────────────────────────────────────────

print("\n[FOCUSED ANALYST] Designing 1-4 deep targets...")

focused_analyst_prompt = build_focused_analyst_prompt(
    query_id="query069_multi_i1",
    original_sql=original_sql,
    explain_text=explain_text,
    ir_node_map=ir_node_map,
    gold_examples=gold_examples,
    dialect="postgres",
)

t0 = time.time()
focused_analyst_response = analyst_gen._analyze(focused_analyst_prompt)
focused_analyst_time = time.time() - t0

with open(OUT_DIR / "focused_analyst.txt", "w") as f:
    f.write(focused_analyst_response)

# Parse targets (JSON array)
import re
targets_match = re.search(r'\[[\s\S]*\]', focused_analyst_response)
if not targets_match:
    print("ERROR: No targets JSON found")
    sys.exit(1)

try:
    targets = json.loads(targets_match.group(0))
except:
    print("ERROR: Failed to parse targets JSON")
    sys.exit(1)

print(f"  → {len(targets)} targets in {focused_analyst_time:.1f}s")
for t in targets:
    print(f"     {t.get('target_id')}: {t.get('transform')} (Family {t.get('family')}, score {t.get('relevance_score', 0):.2f})")

# ── Phase 2: Focused Workers (parallel R1) ──────────────────────────────────

print(f"\n[FOCUSED WORKERS] Firing {len(targets)} R1 workers in parallel...")

from qt_sql.patches.beam_focused_prompts import FocusedTarget

focused_worker_results = []

with ThreadPoolExecutor(max_workers=4) as pool:
    futures = {}
    for t_dict in targets:
        target = FocusedTarget(
            target_id=t_dict["target_id"],
            family=t_dict["family"],
            transform=t_dict["transform"],
            relevance_score=t_dict["relevance_score"],
            hypothesis=t_dict["hypothesis"],
            target_ir=t_dict["target_ir"],
            recommended_examples=t_dict.get("recommended_examples", []),
        )

        # Load recommended gold examples
        target_gold = []
        for ex_id in target.recommended_examples:
            from qt_sql.patches.beam_wide_prompts import _load_gold_example_by_id
            ex = _load_gold_example_by_id(ex_id, "postgres")
            if ex:
                target_gold.append(ex)

        worker_prompt = build_focused_strike_prompt(
            original_sql=original_sql,
            explain_text=explain_text,
            target=target,
            gold_examples=target_gold,
            dialect="postgres",
            engine_version="14.3",
        )

        future = pool.submit(analyst_gen._analyze, worker_prompt)  # R1 for focused
        futures[future] = target

    for future in as_completed(futures):
        target = futures[future]
        try:
            response = future.result(timeout=300)
            focused_worker_results.append({
                "target_id": target.target_id,
                "transform": target.transform,
                "family": target.family,
                "response": response,
                "status": "RESPONSE",
            })
            with open(OUT_DIR / f"focused_worker_{target.target_id}.txt", "w") as f:
                f.write(response)
            print(f"  ✓ {target.target_id}: {len(response)} chars")
        except Exception as e:
            print(f"  ✗ {target.target_id}: {e}")
            focused_worker_results.append({
                "target_id": target.target_id,
                "transform": target.transform,
                "family": target.family,
                "error": str(e),
                "status": "ERROR",
            })

# ── Phase 3: Focused Sniper ─────────────────────────────────────────────────

print(f"\n[FOCUSED SNIPER] Compounding from {len(focused_worker_results)} results...")

from qt_sql.patches.beam_focused_prompts import SortieResult

# Mock sortie result
sortie = SortieResult(
    sortie=0,
    strikes=[
        {
            "strike_id": r["target_id"],
            "family": r["family"],
            "transform": r["transform"],
            "speedup": 1.0 if r["status"] == "RESPONSE" else None,
            "status": "PASS" if r["status"] == "RESPONSE" else "FAIL",
            "error": r.get("error"),
        }
        for r in focused_worker_results
    ],
    explains={},  # no EXPLAINs in quick test
)

sniper_prompt_focused = build_focused_sniper_prompt(
    query_id="query069_multi_i1",
    original_sql=original_sql,
    original_explain=explain_text,
    sortie_history=[sortie],
    gold_examples=gold_examples,
    dialect="postgres",
)

t0 = time.time()
sniper_response_focused = analyst_gen._analyze(sniper_prompt_focused)
sniper_time_focused = time.time() - t0

with open(OUT_DIR / "focused_sniper.txt", "w") as f:
    f.write(sniper_response_focused)

print(f"  → Sniper response: {len(sniper_response_focused)} chars in {sniper_time_focused:.1f}s")

# ══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

n_wide_success = sum(1 for r in wide_worker_results if r["status"] == "RESPONSE")
n_focused_success = sum(1 for r in focused_worker_results if r["status"] == "RESPONSE")

print(f"""
WIDE MODE:
  Analyst:  {len(probes)} probes designed
  Workers:  {n_wide_success}/{len(probes)} succeeded
  Sniper:   {len(sniper_response)} chars

FOCUSED MODE:
  Analyst:  {len(targets)} targets designed
  Workers:  {n_focused_success}/{len(targets)} succeeded
  Sniper:   {len(sniper_response_focused)} chars

All outputs saved to {OUT_DIR}/
""")

"""Fire workers + sniper using cached analyst responses.

Uses cached analyst responses from fire_both_modes_q069.py run.
Skips the 3-5 min analyst calls, goes straight to workers + sniper.
"""

import json
import sys
import time
import functools
import traceback
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

print = functools.partial(print, flush=True)

sys.path.insert(0, "packages/qt-shared")
sys.path.insert(0, "packages/qt-sql")
sys.path.insert(0, ".")

OUT_DIR = Path("research/beam_prompt_audit/full_flow")
OUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR = Path("research/beam_prompt_audit")

# ── Load Q069 data ──────────────────────────────────────────────────────────

QUERY_PATH = "packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/queries/query069_multi_i1.sql"
EXPLAIN_PATH = "packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/explains/query069_multi_i1.json"

with open(QUERY_PATH) as f:
    original_sql = f.read().strip()

with open(EXPLAIN_PATH) as f:
    explain_data = json.load(f)

from qt_sql.prompts.analyst_briefing import format_pg_explain_tree
explain_text = format_pg_explain_tree(explain_data["plan_json"])

from qt_sql.ir import build_script_ir, render_ir_node_map, Dialect
script_ir = build_script_ir(original_sql, Dialect.POSTGRES)
ir_node_map = render_ir_node_map(script_ir)

# ── Load gold examples ──────────────────────────────────────────────────────

from qt_sql.patches.beam_wide_prompts import (
    build_wide_strike_prompt,
    build_wide_sniper_prompt,
    parse_scout_response,
    _load_gold_example_for_family,
    ProbeSpec,
    StrikeBDA,
)
from qt_sql.patches.beam_focused_prompts import (
    build_focused_strike_prompt,
    build_focused_sniper_prompt,
    FocusedTarget,
    SortieResult,
)
from qt_sql.patches.beam_wide_prompts import _load_gold_example_by_id

gold_examples = {}
for fam in ["A", "B", "C", "D", "E", "F"]:
    ex = _load_gold_example_for_family(fam, "postgres")
    if ex:
        gold_examples[fam] = ex

# ── LLM setup ──────────────────────────────────────────────────────────────

from qt_shared.config import get_settings
from qt_sql.generate import CandidateGenerator

settings = get_settings()
ANALYST_MODEL = "deepseek/deepseek-r1"
WORKER_MODEL = "qwen/qwen3-coder"

analyst_gen = CandidateGenerator(provider=settings.llm_provider, model=ANALYST_MODEL)
worker_gen = CandidateGenerator(provider=settings.llm_provider, model=WORKER_MODEL)

print(f"Worker model: {WORKER_MODEL}")

# ── Load cached analyst responses ───────────────────────────────────────────

wide_analyst_response = (CACHE_DIR / "wide_analyst_response.txt").read_text()
focused_analyst_response = (CACHE_DIR / "focused_analyst_response.txt").read_text()

print(f"Wide analyst:    {len(wide_analyst_response)} chars (cached)")
print(f"Focused analyst: {len(focused_analyst_response)} chars (cached)")

# ══════════════════════════════════════════════════════════════════════════════
# WIDE MODE: workers → sniper
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("WIDE MODE: Workers → Sniper")
print("=" * 80)

scout_result = parse_scout_response(wide_analyst_response)
probes = scout_result.probes[:12]

print(f"\n{len(probes)} probes from cached analyst:")
for p in probes:
    print(f"  {p.probe_id}: {p.transform_id} (Family {p.family})")

print(f"\n[WIDE WORKERS] Firing {len(probes)} qwen workers in parallel...")

wide_worker_results = []
t0 = time.time()

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
        futures[future] = (probe, worker_prompt)

    for future in as_completed(futures):
        probe, prompt = futures[future]
        try:
            response = future.result(timeout=120)
            wide_worker_results.append({
                "probe_id": probe.probe_id,
                "transform_id": probe.transform_id,
                "family": probe.family,
                "sql": response,
                "status": "OK",
            })
            (OUT_DIR / f"wide_worker_{probe.probe_id}.txt").write_text(response)
            # Show first line of SQL
            first_line = response.strip().split("\n")[0][:80]
            print(f"  OK {probe.probe_id} ({probe.transform_id}): {first_line}")
        except Exception as e:
            traceback.print_exc()
            print(f"  FAIL {probe.probe_id}: {e}")
            wide_worker_results.append({
                "probe_id": probe.probe_id,
                "transform_id": probe.transform_id,
                "family": probe.family,
                "error": str(e),
                "status": "ERROR",
            })

wide_workers_time = time.time() - t0
n_ok = sum(1 for r in wide_worker_results if r["status"] == "OK")
print(f"\n  {n_ok}/{len(probes)} succeeded in {wide_workers_time:.1f}s")

# ── Wide Sniper ─────────────────────────────────────────────────────────────

print(f"\n[WIDE SNIPER] Synthesizing from {n_ok} worker results...")

strike_bdas = []
for r in wide_worker_results:
    strike_bdas.append(StrikeBDA(
        probe_id=r["probe_id"],
        transform_id=r["transform_id"],
        family=r["family"],
        status="PASS" if r["status"] == "OK" else "FAIL",
        speedup=None,  # no benchmark
        error=r.get("error"),
        explain_text=None,
        sql=r.get("sql"),
    ))

sniper_prompt = build_wide_sniper_prompt(
    query_id="query069_multi_i1",
    original_sql=original_sql,
    original_explain=explain_text,
    hypothesis=scout_result.hypothesis,
    strike_results=strike_bdas,
    dialect="postgres",
)

print(f"  Sniper prompt: {len(sniper_prompt)} chars")

t0 = time.time()
wide_sniper_response = analyst_gen._analyze(sniper_prompt)
wide_sniper_time = time.time() - t0

(OUT_DIR / "wide_sniper.txt").write_text(wide_sniper_response)
print(f"  Sniper response: {len(wide_sniper_response)} chars in {wide_sniper_time:.1f}s")

# ══════════════════════════════════════════════════════════════════════════════
# FOCUSED MODE: workers → sniper
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("FOCUSED MODE: Workers → Sniper")
print("=" * 80)

# Parse focused targets
import re
targets_match = re.search(r'\[[\s\S]*\]', focused_analyst_response)
targets = json.loads(targets_match.group(0))

print(f"\n{len(targets)} targets from cached analyst:")
for t in targets:
    print(f"  {t.get('target_id')}: {t.get('transform')} (Family {t.get('family')})")

print(f"\n[FOCUSED WORKERS] Firing {len(targets)} qwen workers in parallel...")

focused_worker_results = []
t0 = time.time()

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

        target_gold = []
        for ex_id in target.recommended_examples:
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

        future = pool.submit(worker_gen._analyze, worker_prompt)
        futures[future] = (target, worker_prompt)

    for future in as_completed(futures):
        target, prompt = futures[future]
        try:
            response = future.result(timeout=120)
            focused_worker_results.append({
                "target_id": target.target_id,
                "transform": target.transform,
                "family": target.family,
                "sql": response,
                "status": "OK",
            })
            (OUT_DIR / f"focused_worker_{target.target_id}.txt").write_text(response)
            first_line = response.strip().split("\n")[0][:80]
            print(f"  OK {target.target_id} ({target.transform}): {first_line}")
        except Exception as e:
            traceback.print_exc()
            print(f"  FAIL {target.target_id}: {e}")
            focused_worker_results.append({
                "target_id": target.target_id,
                "transform": target.transform,
                "family": target.family,
                "error": str(e),
                "status": "ERROR",
            })

focused_workers_time = time.time() - t0
n_ok_f = sum(1 for r in focused_worker_results if r["status"] == "OK")
print(f"\n  {n_ok_f}/{len(targets)} succeeded in {focused_workers_time:.1f}s")

# ── Focused Sniper ──────────────────────────────────────────────────────────

print(f"\n[FOCUSED SNIPER] Compounding from {n_ok_f} results...")

sortie = SortieResult(
    sortie=0,
    strikes=[
        {
            "strike_id": r["target_id"],
            "family": r["family"],
            "transform": r["transform"],
            "speedup": None,
            "status": "PASS" if r["status"] == "OK" else "FAIL",
            "error": r.get("error"),
        }
        for r in focused_worker_results
    ],
    explains={},
)

sniper_prompt_f = build_focused_sniper_prompt(
    query_id="query069_multi_i1",
    original_sql=original_sql,
    original_explain=explain_text,
    sortie_history=[sortie],
    gold_examples=gold_examples,
    dialect="postgres",
)

print(f"  Sniper prompt: {len(sniper_prompt_f)} chars")

t0 = time.time()
focused_sniper_response = analyst_gen._analyze(sniper_prompt_f)
focused_sniper_time = time.time() - t0

(OUT_DIR / "focused_sniper.txt").write_text(focused_sniper_response)
print(f"  Sniper response: {len(focused_sniper_response)} chars in {focused_sniper_time:.1f}s")

# ══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

print(f"""
WIDE:    {n_ok}/{len(probes)} workers OK, workers {wide_workers_time:.0f}s, sniper {wide_sniper_time:.0f}s
FOCUSED: {n_ok_f}/{len(targets)} workers OK, workers {focused_workers_time:.0f}s, sniper {focused_sniper_time:.0f}s

All outputs in {OUT_DIR}/
""")

#!/usr/bin/env python3
"""Render analyst + worker V2 prompts for review — NO API calls.

Loads a real PG DSB query, explain plan, engine profile, and gold examples,
then builds the full analyst briefing and a sample worker prompt using
mock data for DAG/costs and a mock PGSystemProfile for the resource envelope.

Usage:
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 research/scripts/render_prompts_review.py
"""

import json
import sys
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent.parent

# Ensure correct import paths — remove script dir (shadows 'ado' package)
script_dir = str(Path(__file__).resolve().parent)
sys.path = [p for p in sys.path if p != script_dir]
for p in [str(ROOT / "packages" / "qt-sql"), str(ROOT / "packages" / "qt-shared"), str(ROOT)]:
    if p not in sys.path:
        sys.path.insert(0, p)
BENCH = ROOT / "packages" / "qt-sql" / "ado" / "benchmarks" / "postgres_dsb_156"
QUERY_FILE = BENCH / "queries" / "query072_agg_s1.sql"
EXPLAIN_FILE = BENCH / "explains" / "sf10" / "query072_agg_s1.json"
ENGINE_PROFILE = ROOT / "packages" / "qt-sql" / "ado" / "constraints" / "engine_profile_postgresql.json"
EXAMPLES_DIR = ROOT / "packages" / "qt-sql" / "ado" / "examples" / "postgres"
CONSTRAINTS_DIR = ROOT / "packages" / "qt-sql" / "ado" / "constraints"

# ── Load real data ─────────────────────────────────────────────────────
sql = QUERY_FILE.read_text().strip()
explain_data = json.loads(EXPLAIN_FILE.read_text())
plan_json = explain_data.get("plan_json", [])
engine_profile = json.loads(ENGINE_PROFILE.read_text())

# Load PG gold examples
pg_examples = []
for f in sorted(EXAMPLES_DIR.glob("*.json")):
    pg_examples.append(json.loads(f.read_text()))

# Load correctness constraints
correctness_ids = [
    "LITERAL_PRESERVATION", "SEMANTIC_EQUIVALENCE",
    "COMPLETE_OUTPUT", "CTE_COLUMN_COMPLETENESS",
]
constraints = []
for cid in correctness_ids:
    # These are synthetic — the real ones live embedded in the analyst code
    constraints.append({
        "id": cid,
        "severity": "CRITICAL",
        "overridable": False,
        "prompt_instruction": f"Correctness gate: {cid.replace('_', ' ').title()}",
    })

# ── Build resource envelope from mock PGSystemProfile ─────────────────
from ado.pg_tuning import PGSystemProfile, build_resource_envelope

mock_profile = PGSystemProfile(
    settings=[
        {"name": "shared_buffers", "setting": "524288", "unit": "8kB", "context": "postmaster",
         "min_val": "16", "max_val": "1073741823", "boot_val": "16384", "reset_val": "524288"},
        {"name": "effective_cache_size", "setting": "12582912", "unit": "kB", "context": "user",
         "min_val": "1", "max_val": "2147483647", "boot_val": "524288", "reset_val": "12582912"},
        {"name": "work_mem", "setting": "131072", "unit": "kB", "context": "user",
         "min_val": "64", "max_val": "2147483647", "boot_val": "4096", "reset_val": "131072"},
        {"name": "random_page_cost", "setting": "1.1", "unit": None, "context": "user",
         "min_val": "0", "max_val": "1.79769e+308", "boot_val": "4", "reset_val": "1.1"},
        {"name": "seq_page_cost", "setting": "1", "unit": None, "context": "user",
         "min_val": "0", "max_val": "1.79769e+308", "boot_val": "1", "reset_val": "1"},
        {"name": "max_parallel_workers", "setting": "8", "unit": None, "context": "user",
         "min_val": "0", "max_val": "1024", "boot_val": "8", "reset_val": "8"},
        {"name": "max_parallel_workers_per_gather", "setting": "4", "unit": None, "context": "user",
         "min_val": "0", "max_val": "1024", "boot_val": "2", "reset_val": "4"},
        {"name": "max_worker_processes", "setting": "8", "unit": None, "context": "postmaster",
         "min_val": "0", "max_val": "262143", "boot_val": "8", "reset_val": "8"},
        {"name": "max_connections", "setting": "100", "unit": None, "context": "postmaster",
         "min_val": "1", "max_val": "262143", "boot_val": "100", "reset_val": "100"},
        {"name": "jit", "setting": "on", "unit": None, "context": "user",
         "min_val": None, "max_val": None, "boot_val": "on", "reset_val": "on"},
        {"name": "jit_above_cost", "setting": "100000", "unit": None, "context": "user",
         "min_val": "-1", "max_val": "1.79769e+308", "boot_val": "100000", "reset_val": "100000"},
        {"name": "hash_mem_multiplier", "setting": "2", "unit": None, "context": "user",
         "min_val": "1", "max_val": "1000", "boot_val": "2", "reset_val": "2"},
        {"name": "join_collapse_limit", "setting": "8", "unit": None, "context": "user",
         "min_val": "1", "max_val": "2147483647", "boot_val": "8", "reset_val": "8"},
        {"name": "from_collapse_limit", "setting": "8", "unit": None, "context": "user",
         "min_val": "1", "max_val": "2147483647", "boot_val": "8", "reset_val": "8"},
        {"name": "geqo_threshold", "setting": "12", "unit": None, "context": "user",
         "min_val": "2", "max_val": "2147483647", "boot_val": "12", "reset_val": "12"},
        {"name": "default_statistics_target", "setting": "100", "unit": None, "context": "user",
         "min_val": "1", "max_val": "10000", "boot_val": "100", "reset_val": "100"},
        {"name": "enable_hashjoin", "setting": "on", "unit": None, "context": "user",
         "min_val": None, "max_val": None, "boot_val": "on", "reset_val": "on"},
        {"name": "enable_mergejoin", "setting": "on", "unit": None, "context": "user",
         "min_val": None, "max_val": None, "boot_val": "on", "reset_val": "on"},
        {"name": "enable_nestloop", "setting": "on", "unit": None, "context": "user",
         "min_val": None, "max_val": None, "boot_val": "on", "reset_val": "on"},
        {"name": "enable_seqscan", "setting": "on", "unit": None, "context": "user",
         "min_val": None, "max_val": None, "boot_val": "on", "reset_val": "on"},
        {"name": "parallel_setup_cost", "setting": "1000", "unit": None, "context": "user",
         "min_val": "0", "max_val": "1.79769e+308", "boot_val": "1000", "reset_val": "1000"},
        {"name": "parallel_tuple_cost", "setting": "0.1", "unit": None, "context": "user",
         "min_val": "0", "max_val": "1.79769e+308", "boot_val": "0.01", "reset_val": "0.1"},
    ],
    active_connections=10,
    collected_at="2026-02-09T12:00:00",
)

resource_envelope = build_resource_envelope(mock_profile)

# ── Render PG explain tree ─────────────────────────────────────────────
from ado.prompts.analyst_briefing import format_pg_explain_tree

explain_tree_text = format_pg_explain_tree(plan_json)

# ── Build minimal mock DAG for query072 (flat query, no CTEs) ──────────
from qt_sql.optimization.dag_v2 import QueryDag, DagNode

mock_main_node = DagNode(
    node_id="main_query",
    node_type="main",
    sql=sql,
    tables=["catalog_sales", "inventory", "warehouse", "item",
            "customer_demographics", "household_demographics",
            "date_dim", "promotion", "catalog_returns"],
    refs=[],
    flags=["multi_join", "non_equi_join", "left_outer_join"],
)
mock_dag = QueryDag(
    nodes={"main_query": mock_main_node},
    edges=[],
    original_sql=sql,
)

# ── Build compact example catalog ──────────────────────────────────────
example_catalog = []
for ex in pg_examples:
    example_catalog.append({
        "id": ex.get("id", "?"),
        "speedup": ex.get("verified_speedup", "?").rstrip("x"),
        "description": ex.get("description", ""),
    })

# ── Build analyst prompt ───────────────────────────────────────────────
from ado.prompts.analyst_briefing import build_analyst_briefing_prompt

analyst_prompt = build_analyst_briefing_prompt(
    query_id="query072_agg_s1",
    sql=sql,
    explain_plan_text=explain_tree_text,  # pre-rendered PG explain tree
    dag=mock_dag,
    costs={},
    semantic_intents=None,
    global_knowledge=None,
    matched_examples=pg_examples[:6],  # All PG gold examples
    all_available_examples=example_catalog,
    constraints=constraints,
    regression_warnings=None,
    dialect="postgresql",
    dialect_version="14.3",
    strategy_leaderboard=None,
    query_archetype=None,
    engine_profile=engine_profile,
    resource_envelope=resource_envelope,
)

# ── Build mock worker briefing ─────────────────────────────────────────
from ado.prompts.swarm_parsers import BriefingShared, BriefingWorker
from ado.prompts.worker_v2 import build_worker_v2_prompt

mock_shared = BriefingShared(
    semantic_contract=(
        "Query finds (item, warehouse, week) triples where catalog_sales inventory was "
        "insufficient (inv_quantity_on_hand < cs_quantity), filtered by demographic criteria "
        "(unmarried, dep_count 9-11, buy_potential >10000), item category, wholesale cost range, "
        "and shipping delay (d3.d_date > d1.d_date + 3 days) for year 1998. "
        "JOIN types: 8 INNER + 1 LEFT (promotion) + 1 LEFT (catalog_returns). "
        "The LEFT JOINs preserve all matching catalog_sales rows. "
        "Aggregation: COUNT and conditional SUM on promotion NULL status — safe for restructuring."
    ),
    bottleneck_diagnosis=(
        "Dominant cost: inventory × catalog_sales non-equi join (inv_quantity_on_hand < cs_quantity) "
        "via nested-loop. Fact table catalog_sales is the largest input (~14M rows at SF10). "
        "The optimizer pre-filters catalog_sales by date (d1.d_year=1998) and wholesale_cost range "
        "via index scan, but the residual non-equi join with inventory is still O(N×M) nested-loop. "
        "Dimension joins (demographics, item, warehouse) are cheap index lookups. "
        "The LEFT JOIN to catalog_returns is unused in output — pure overhead."
    ),
    active_constraints=(
        "- LITERAL_PRESERVATION: All numeric literals (1998, 9, 11, 35, 55, '>10000', 3 days) must be preserved exactly.\n"
        "- SEMANTIC_EQUIVALENCE: The non-equi condition inv_quantity_on_hand < cs_quantity must remain row-level, not aggregated.\n"
        "- COMPLETE_OUTPUT: 6 output columns (i_item_desc, w_warehouse_name, d_week_seq, no_promo, promo, total_cnt).\n"
        "- CTE_COLUMN_COMPLETENESS: Any intermediate CTEs must carry all columns needed downstream.\n"
        "- NON_EQUI_JOIN_INPUT_BLINDNESS: Active — nested-loop on inventory×catalog_sales is the bottleneck."
    ),
    regression_warnings="None applicable.",
    resource_envelope=resource_envelope,
)

mock_worker1 = BriefingWorker(
    worker_id=1,
    strategy="star_join_prefetch + early_filter",
    target_dag=(
        "TARGET_DAG:\n"
        "  filtered_dates -> filtered_cs -> cs_inv_join -> dim_lookups -> final_agg\n\n"
        "NODE_CONTRACTS:\n"
        "  filtered_dates:\n"
        "    FROM: date_dim d1\n"
        "    WHERE: d1.d_year = 1998\n"
        "    OUTPUT: d_date_sk, d_week_seq, d_date\n"
        "    EXPECTED_ROWS: ~365\n"
        "    CONSUMERS: filtered_cs\n\n"
        "  filtered_cs:\n"
        "    FROM: catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk\n"
        "    WHERE: cs_wholesale_cost BETWEEN 35 AND 55\n"
        "    OUTPUT: cs_item_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_ship_date_sk, cs_promo_sk, cs_order_number, cs_quantity, d_week_seq, d_date\n"
        "    EXPECTED_ROWS: ~170K (pre-filtered from 14M)\n"
        "    CONSUMERS: cs_inv_join\n\n"
        "  cs_inv_join:\n"
        "    FROM: filtered_cs JOIN date_dim d2 ON d2.d_week_seq = filtered_cs.d_week_seq\n"
        "          JOIN inventory ON inv_date_sk = d2.d_date_sk AND inv_item_sk = cs_item_sk\n"
        "    WHERE: inv_quantity_on_hand < cs_quantity\n"
        "    OUTPUT: cs_item_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_ship_date_sk, cs_promo_sk, cs_order_number, cs_quantity, d_week_seq, d_date, inv_warehouse_sk\n"
        "    EXPECTED_ROWS: ~5K\n"
        "    CONSUMERS: dim_lookups\n\n"
        "  dim_lookups:\n"
        "    FROM: cs_inv_join JOIN warehouse ON inv_warehouse_sk = w_warehouse_sk\n"
        "          JOIN item ON cs_item_sk = i_item_sk\n"
        "          JOIN customer_demographics ON cs_bill_cdemo_sk = cd_demo_sk\n"
        "          JOIN household_demographics ON cs_bill_hdemo_sk = hd_demo_sk\n"
        "          JOIN date_dim d3 ON cs_ship_date_sk = d3.d_date_sk\n"
        "          LEFT JOIN promotion ON cs_promo_sk = p_promo_sk\n"
        "          LEFT JOIN catalog_returns ON cr_item_sk = cs_item_sk AND cr_order_number = cs_order_number\n"
        "    WHERE: d3.d_date > d_date + interval '3 day' AND hd_buy_potential = '>10000' AND cd_marital_status = 'U' AND cd_dep_count BETWEEN 9 AND 11 AND i_category IN ('Children', 'Jewelry', 'Men')\n"
        "    OUTPUT: i_item_desc, w_warehouse_name, d_week_seq, p_promo_sk\n"
        "    EXPECTED_ROWS: ~2K\n"
        "    CONSUMERS: final_agg\n\n"
        "  final_agg:\n"
        "    FROM: dim_lookups\n"
        "    GROUP BY: i_item_desc, w_warehouse_name, d_week_seq\n"
        "    AGGREGATE: SUM(CASE WHEN p_promo_sk IS NULL THEN 1 ELSE 0 END) AS no_promo, SUM(CASE WHEN p_promo_sk IS NOT NULL THEN 1 ELSE 0 END) AS promo, COUNT(*) AS total_cnt\n"
        "    OUTPUT: i_item_desc, w_warehouse_name, d_week_seq, no_promo, promo, total_cnt\n"
        "    EXPECTED_ROWS: ~100\n"
        "    CONSUMERS: result"
    ),
    examples=["pg_date_cte_explicit_join", "pg_dimension_prefetch_star"],
    example_reasoning=(
        "pg_date_cte_explicit_join: Q072 has d1.d_year=1998 filter on date_dim — isolating this "
        "into a CTE reduces the catalog_sales join input. Same pattern as Q099 (2.28x).\n"
        "pg_dimension_prefetch_star: Q072 has 8 dimension joins — pre-filtering date+cost "
        "before the expensive inventory non-equi join reduces nested-loop input."
    ),
    hazard_flags=(
        "- CTE materialization fence: filtered_cs CTE must carry ALL columns needed downstream "
        "(cs_promo_sk for LEFT JOIN promotion, cs_order_number for LEFT JOIN catalog_returns).\n"
        "- Non-equi join (inv_quantity_on_hand < cs_quantity) MUST remain as nested-loop — "
        "do not attempt to convert to hash join.\n"
        "- LEFT JOIN to catalog_returns is in the original query — preserve it even though "
        "cr_* columns are not in the output (may affect row count via duplication)."
    ),
)

output_columns = [
    "i_item_desc", "w_warehouse_name", "d_week_seq",
    "no_promo", "promo", "total_cnt",
]

worker_prompt = build_worker_v2_prompt(
    worker_briefing=mock_worker1,
    shared_briefing=mock_shared,
    examples=pg_examples[:2],  # First 2 PG examples
    original_sql=sql,
    output_columns=output_columns,
    dialect="postgresql",
    engine_version="14.3",
    resource_envelope=resource_envelope,
)

# ── Build V2 snipe prompts (analyst2 + sniper + sniper retry) ─────────
from ado.prompts.swarm_snipe import build_snipe_analyst_prompt, build_sniper_prompt
from ado.prompts.swarm_parsers import SnipeAnalysisV2
from ado.schemas import WorkerResult

# Mock worker results from fan-out
mock_worker_results = [
    WorkerResult(
        worker_id=1, strategy="star_join_prefetch",
        examples_used=["pg_date_cte_explicit_join", "pg_dimension_prefetch_star"],
        optimized_sql="WITH filtered_dates AS (\n  SELECT d_date_sk, d_week_seq, d_date\n  FROM date_dim\n  WHERE d_year = 1998\n)\nSELECT i_item_desc, w_warehouse_name, d_week_seq\nFROM catalog_sales cs\nJOIN filtered_dates fd ON cs.cs_sold_date_sk = fd.d_date_sk\n-- ... rest of query",
        speedup=1.15, status="NEUTRAL", transforms=["pushdown", "date_cte_isolate"],
        hint="Pre-filter date dim into CTE, then join star schema",
    ),
    WorkerResult(
        worker_id=2, strategy="decorrelate_subquery",
        examples_used=["pg_decorrelate_exists"],
        optimized_sql="SELECT i_item_desc, w_warehouse_name, d_week_seq\nFROM catalog_sales cs\nJOIN inventory inv ON inv.inv_item_sk = cs.cs_item_sk\n-- ... decorrelated version",
        speedup=0.85, status="REGRESSION", transforms=["decorrelate"],
        error_message="Row count mismatch: expected 142, got 198",
    ),
    WorkerResult(
        worker_id=3, strategy="scan_consolidation",
        examples_used=["pg_single_pass_agg"],
        optimized_sql="-- attempted scan consolidation\nSELECT 1",
        speedup=0.0, status="ERROR", transforms=[],
        error_message="syntax error at or near 'FILTER' (line 12)",
    ),
    WorkerResult(
        worker_id=4, strategy="exploration_novel",
        examples_used=[],
        optimized_sql="SELECT i_item_desc, w_warehouse_name, d_week_seq\nFROM catalog_sales cs\nJOIN date_dim d1 ON cs.cs_sold_date_sk = d1.d_date_sk\n-- ... novel approach",
        speedup=0.92, status="REGRESSION", transforms=["novel"],
        exploratory=True,
    ),
]

snipe_analyst_prompt = build_snipe_analyst_prompt(
    query_id="query072_agg_s1",
    original_sql=sql,
    worker_results=mock_worker_results,
    target_speedup=2.0,
    dag=mock_dag,
    costs={},
    explain_plan_text=explain_tree_text,
    engine_profile=engine_profile,
    constraints=constraints,
    matched_examples=pg_examples[:6],
    regression_warnings=None,
    resource_envelope=resource_envelope,
    dialect="postgresql",
    dialect_version="14.3",
)

# Mock analysis from analyst2
mock_snipe_analysis = SnipeAnalysisV2(
    failure_synthesis=(
        "W1 achieved 1.15x via date CTE isolation — the only passing approach. The bottleneck "
        "is the nested-loop join between filtered catalog_sales and inventory (non-equi condition "
        "inv_quantity_on_hand < cs_quantity). W2 broke semantic equivalence by decorrelating a "
        "non-correlated subquery path. W3 used PG-unsupported FILTER syntax. W4's novel approach "
        "didn't reduce the critical join input cardinality."
    ),
    best_foundation=(
        "Build on W1's date CTE isolation (1.15x). The date pre-filtering is sound — it reduces "
        "catalog_sales from 14M to ~170K rows. But the real bottleneck is AFTER this filter: "
        "the inventory non-equi join processes 170K × 11M = ~1.9T comparisons."
    ),
    unexplored_angles=(
        "No worker attempted to pre-aggregate or pre-filter inventory. The EXPLAIN shows inventory "
        "is scanned fully (11M rows) for each probe. Pre-filtering inventory by the d_week_seq "
        "range (only ~7 weeks in 1998) would reduce the join input from 11M to ~1.5M rows."
    ),
    strategy_guidance=(
        "1. Keep W1's date CTE isolation (proven 1.15x). "
        "2. Add inventory pre-filtering: join inventory with the same date CTE to restrict to "
        "relevant weeks, reducing from 11M to ~1.5M rows. "
        "3. Then join filtered_cs with filtered_inventory on (item_sk, quantity < condition). "
        "This reduces the nested-loop from 170K×11M to 170K×1.5M — a ~7x reduction in join work."
    ),
    examples=["pg_date_cte_explicit_join", "pg_dimension_prefetch_star"],
    example_adaptation=(
        "pg_date_cte_explicit_join: APPLY the date CTE isolation pattern (proven by W1). "
        "IGNORE the explicit join conversion — PG already uses explicit joins.\n"
        "pg_dimension_prefetch_star: APPLY the multi-dimension prefetch concept, adapted for "
        "INVENTORY pre-filtering. IGNORE the star-join aspect — Q072 is not a star join."
    ),
    hazard_flags=(
        "- Do NOT attempt FILTER clause syntax — PG 14.3 supports it but W3's error suggests "
        "a syntax context issue. Use CASE WHEN instead.\n"
        "- The non-equi join (inv_quantity_on_hand < cs_quantity) CANNOT be converted to hash join. "
        "Keep as nested-loop but reduce input cardinality.\n"
        "- Preserve LEFT JOINs to promotion and catalog_returns."
    ),
    retry_worthiness=(
        "high — W1's 1.15x with just date isolation leaves clear headroom. Adding inventory "
        "pre-filtering reduces the dominant join by ~7x, giving a path to 2.0x target."
    ),
    retry_digest=(
        "The nested-loop between catalog_sales and inventory is the bottleneck (170K × 11M probes). "
        "W1's date CTE gives 1.15x but doesn't touch this join. Pre-filter inventory by d_week_seq "
        "range to reduce from 11M to ~1.5M rows. Keep W1's date isolation. Use CASE WHEN, not FILTER. "
        "Preserve LEFT JOINs. Do NOT attempt decorrelation (W2 broke equivalence)."
    ),
)

sniper_prompt = build_sniper_prompt(
    snipe_analysis=mock_snipe_analysis,
    original_sql=sql,
    worker_results=mock_worker_results,
    best_worker_sql=mock_worker_results[0].optimized_sql,
    examples=pg_examples[:2],
    output_columns=output_columns,
    dag=mock_dag,
    costs={},
    engine_profile=engine_profile,
    constraints=constraints,
    dialect="postgresql",
    engine_version="14.3",
    resource_envelope=resource_envelope,
    target_speedup=2.0,
)

# Sniper retry prompt
sniper_retry_prompt = build_sniper_prompt(
    snipe_analysis=mock_snipe_analysis,
    original_sql=sql,
    worker_results=mock_worker_results,
    best_worker_sql=mock_worker_results[0].optimized_sql,
    examples=pg_examples[:2],
    output_columns=output_columns,
    dag=mock_dag,
    costs={},
    engine_profile=engine_profile,
    constraints=constraints,
    dialect="postgresql",
    engine_version="14.3",
    resource_envelope=resource_envelope,
    target_speedup=2.0,
    previous_sniper_result=WorkerResult(
        worker_id=5, strategy="sniper_1",
        examples_used=["pg_date_cte_explicit_join"],
        optimized_sql="WITH date_cte AS (...)\nSELECT ...",
        speedup=1.45, status="NEUTRAL", transforms=["date_cte_isolate", "inventory_prefetch"],
    ),
)

# ── Save to files ──────────────────────────────────────────────────────
OUT_DIR = ROOT / "research" / "prompt_samples" / "pg_q072_v2"
OUT_DIR.mkdir(parents=True, exist_ok=True)

files = {
    "00_resource_envelope.txt": resource_envelope,
    "01_analyst_prompt.txt": analyst_prompt,
    "02_worker1_prompt.txt": worker_prompt,
    "03_snipe_analyst_prompt.txt": snipe_analyst_prompt,
    "04_sniper_prompt.txt": sniper_prompt,
    "05_sniper_retry_prompt.txt": sniper_retry_prompt,
}

for fname, content in files.items():
    out_path = OUT_DIR / fname
    out_path.write_text(content)
    words = len(content.split())
    chars = len(content)
    print(f"  {fname}: {chars:,} chars, ~{words:,} words")

print(f"\nSaved to: {OUT_DIR}")

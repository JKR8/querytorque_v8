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
        "    OUTPUT: all filtered_cs columns + inv_warehouse_sk\n"
        "    EXPECTED_ROWS: ~5K\n"
        "    CONSUMERS: dim_lookups"
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

# ── Output ─────────────────────────────────────────────────────────────
separator = "=" * 80

print(separator)
print("RESOURCE ENVELOPE (passed to both analyst + workers)")
print(separator)
print(resource_envelope)
print()

print(separator)
print(f"ANALYST PROMPT ({len(analyst_prompt)} chars, ~{len(analyst_prompt.split())}) words)")
print(separator)
print(analyst_prompt)
print()

print(separator)
print(f"WORKER 1 PROMPT ({len(worker_prompt)} chars, ~{len(worker_prompt.split())} words)")
print(separator)
print(worker_prompt)

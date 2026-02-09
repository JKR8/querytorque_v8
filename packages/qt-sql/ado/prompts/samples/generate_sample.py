#!/usr/bin/env python3
"""Generate real V2 prompt samples from benchmark data.

Writes rendered analyst and worker prompts to this samples/ directory
so they can be reviewed without running the full pipeline.

Usage:
    cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.prompts.samples.generate_sample query_74
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# ── Paths ───────────────────────────────────────────────────────────
SAMPLES_DIR = Path(__file__).resolve().parent
ADO_DIR = SAMPLES_DIR.parent.parent          # ado/
BENCHMARK_DIR = ADO_DIR / "benchmarks" / "duckdb_tpcds"
CONSTRAINTS_DIR = ADO_DIR / "constraints"
BATCH_DIR = BENCHMARK_DIR / "swarm_batch_20260208_102033"

QUERY_ID = sys.argv[1] if len(sys.argv) > 1 else "query_74"


def load_original_sql() -> str:
    sql_path = BATCH_DIR / QUERY_ID / "original.sql"
    if not sql_path.exists():
        sql_path = ADO_DIR.parent.parent.parent / "research" / "tpcds_queries" / f"{QUERY_ID}.sql"
    if not sql_path.exists():
        print(f"ERROR: Cannot find SQL for {QUERY_ID}")
        sys.exit(1)
    sql = sql_path.read_text().strip()
    print(f"Loaded SQL: {len(sql)} chars from {sql_path}")
    return sql


def load_explain_plan() -> str | None:
    for subdir in ["sf10", "sf5", ""]:
        d = BENCHMARK_DIR / "explains" / subdir if subdir else BENCHMARK_DIR / "explains"
        p = d / f"{QUERY_ID}.json"
        if p.exists():
            data = json.loads(p.read_text())
            plan_text = data.get("plan_text")
            if plan_text:
                print(f"Loaded EXPLAIN: {len(plan_text)} chars from {p}")
                return plan_text
    print("No EXPLAIN plan found (will show fallback note)")
    return None


def parse_dag(sql: str, explain_plan_text: str | None = None):
    try:
        from qt_sql.optimization.dag_v2 import DagBuilder, CostAnalyzer
        builder = DagBuilder(sql, dialect="duckdb")
        dag = builder.build()

        # Try to get real cost attribution from EXPLAIN plan
        plan_context = None
        if explain_plan_text:
            try:
                plan_json = json.loads(explain_plan_text) if explain_plan_text.strip().startswith("{") else None
                if plan_json:
                    from qt_sql.optimization.plan_analyzer import analyze_plan_for_optimization
                    plan_context = analyze_plan_for_optimization(plan_json, sql, "duckdb")
                    print(f"EXPLAIN → OptimizationContext: {len(plan_context.table_scans)} scans, "
                          f"{len(plan_context.bottleneck_operators)} operators")
            except Exception as e:
                print(f"EXPLAIN analysis failed ({e}), using heuristic costs")

        analyzer = CostAnalyzer(dag, plan_context)
        costs = analyzer.analyze()
        nonzero = sum(1 for c in costs.values() if c.cost_pct > 0)
        print(f"Parsed DAG: {len(dag.nodes)} nodes, {nonzero} with cost > 0%")
        return dag, costs
    except Exception as e:
        print(f"DAG parsing failed ({e}), using stub")
        from types import SimpleNamespace
        return SimpleNamespace(nodes={}, edges=[]), {}


def load_semantic_intents():
    intents_path = BENCHMARK_DIR / "semantic_intents.json"
    if not intents_path.exists():
        print("No semantic intents found")
        return None

    import re
    data = json.loads(intents_path.read_text())
    m = re.match(r"(?:query_?)(\d+\w?)", QUERY_ID)
    qnum = m.group(1) if m else QUERY_ID
    for q in data.get("queries", []):
        qid = q.get("query_id", "")
        if qid == QUERY_ID or qid == f"q{qnum}":
            print(f"Loaded semantic intents for {qid}")
            return q
    print("No semantic intents found")
    return None


def load_global_knowledge():
    knowledge_dir = BENCHMARK_DIR / "knowledge"
    if knowledge_dir.exists():
        for p in sorted(knowledge_dir.glob("*.json")):
            data = json.loads(p.read_text())
            if data.get("principles") or data.get("anti_patterns"):
                print(f"Loaded GlobalKnowledge: {len(data.get('principles', []))} principles, "
                      f"{len(data.get('anti_patterns', []))} anti-patterns")
                return data
    print("No GlobalKnowledge found")
    return None


def load_matched_examples(sql: str, recommender):
    matched = []
    if recommender._initialized:
        matches = recommender.find_similar_examples(sql, k=16, dialect="duckdb")
        examples_dir = ADO_DIR / "examples" / "duckdb"
        for ex_id, score, meta in matches:
            ex_path = examples_dir / f"{ex_id}.json"
            if ex_path.exists():
                matched.append(json.loads(ex_path.read_text()))
        print(f"Tag-matched: {len(matched)} examples")
    else:
        print("Recommender not initialized")
    return matched


def load_full_catalog():
    catalog = []
    for p in sorted((ADO_DIR / "examples" / "duckdb").glob("*.json")):
        try:
            d = json.loads(p.read_text())
            catalog.append({
                "id": d.get("id", p.stem),
                "speedup": d.get("verified_speedup", "?"),
                "description": d.get("description", "")[:80],
            })
        except Exception:
            pass
    print(f"Full catalog: {len(catalog)} examples")
    return catalog


def load_constraints():
    result = []
    if CONSTRAINTS_DIR.exists():
        severity_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2}
        for p in sorted(CONSTRAINTS_DIR.glob("*.json")):
            try:
                d = json.loads(p.read_text())
                if "id" not in d or "prompt_instruction" not in d:
                    continue
                engine = d.get("engine")
                if engine and engine.lower() != "duckdb":
                    continue
                result.append(d)
            except Exception:
                pass
        result.sort(key=lambda c: severity_order.get(c.get("severity", "MEDIUM"), 2))
    print(f"Constraints: {len(result)}")
    return result


def load_regression_warnings(sql: str, recommender):
    warnings = []
    if recommender._initialized:
        reg_matches = recommender.find_relevant_regressions(sql, k=3, dialect="duckdb")
        reg_dir = ADO_DIR / "examples" / "duckdb" / "regressions"
        for ex_id, score, meta in reg_matches:
            reg_path = reg_dir / f"{ex_id}.json"
            if reg_path.exists():
                warnings.append(json.loads(reg_path.read_text()))
    print(f"Regression warnings: {len(warnings)}")
    return warnings


def build_mock_worker_briefing():
    """Create a realistic mock analyst briefing for Q74 Worker 2.

    Demonstrates late_attribute_binding: customer join deferred to final
    resolve_names CTE. Each channel×year CTE aggregates on c_customer_sk
    (the FK already in the fact table), avoiding 3 of 4 customer scans.
    """
    from ado.prompts.swarm_parsers import BriefingShared, BriefingWorker

    shared = BriefingShared(
        semantic_contract=(
            "This query finds customers whose web-channel payment variability grew "
            "faster year-over-year than their store-channel variability. "
            "Intersection semantics: customers must have sales in BOTH channels "
            "(store and web) in BOTH years (1999 and 2000). This is enforced by "
            "the 4-way inner join — any rewrite must preserve this intersection. "
            "STDDEV_SAMP returns NULL for single-row groups. The year_total > 0 "
            "filter excludes NULL and zero-stddev customers. "
            "Output: 3 columns, ordered by first_name, customer_id, last_name. LIMIT 100."
        ),
        bottleneck_diagnosis=(
            "EXPLAIN shows DuckDB already unrolled year_total into 4 branches "
            "(store×1999, store×2000, web×1999, web×2000). The dominant cost is "
            "the store_sales × customer hash join at 598ms × 2 = 1.2s (34% of "
            "3.5s total). Customer is scanned 4× (500K rows each) purely for "
            "name resolution (c_customer_id, c_first_name, c_last_name) — zero "
            "selectivity. Deferring customer join to after aggregation and "
            "self-join resolution eliminates 3 of 4 customer scans and joins "
            "~4K qualifying rows instead of 5.4M."
        ),
        active_constraints=(
            "- REMOVE_REPLACED_CTES: Your rewrite must NOT include a year_total "
            "CTE. The 4 channel×year CTEs REPLACE it entirely. Keeping both "
            "caused 0.68x regression on Q74.\n"
            "- CTE_COLUMN_COMPLETENESS: Each CTE's SELECT must include ALL "
            "columns referenced by downstream consumers. Check: ss_customer_sk "
            "flows through store_agg → compare_ratios → resolve_names; "
            "ws_bill_customer_sk flows through web_agg → compare_ratios.\n"
            "- LITERAL_PRESERVATION: d_year IN (1999, 1999+1) must be preserved "
            "exactly. Do not substitute computed values."
        ),
        regression_warnings=(
            "1. regression_q74_pushdown (0.68x):\n"
            "   CAUSE: Created year-specific CTEs but KEPT the original year_total "
            "UNION CTE. DuckDB materialized both, causing redundant computation.\n"
            "   RULE: Your rewrite must NOT include a year_total CTE. The 4 "
            "channel×year CTEs REPLACE it entirely."
        ),
    )

    worker = BriefingWorker(
        worker_id=2,
        strategy="date_cte_isolate + late_attribute_binding",
        target_dag=(
            "TARGET_DAG:\n"
            "  filtered_dates -> store_agg -+\n"
            "  filtered_dates -> web_agg   -+-> compare_ratios -> resolve_names\n\n"
            "NODE_CONTRACTS:\n"
            "  filtered_dates:\n"
            "    FROM: date_dim\n"
            "    WHERE: d_year IN (1999, 1999 + 1)\n"
            "    OUTPUT: d_date_sk, d_year\n"
            "    EXPECTED_ROWS: ~730\n"
            "    CONSUMERS: store_agg, web_agg\n"
            "  store_agg:\n"
            "    FROM: store_sales JOIN filtered_dates\n"
            "    JOIN: ss_sold_date_sk = d_date_sk\n"
            "    GROUP BY: ss_customer_sk, d_year\n"
            "    AGGREGATE: STDDEV_SAMP(ss_net_paid) AS year_total\n"
            "    OUTPUT: ss_customer_sk, d_year, year_total\n"
            "    EXPECTED_ROWS: ~600K (300K per year)\n"
            "    CONSUMERS: compare_ratios\n"
            "  web_agg:\n"
            "    FROM: web_sales JOIN filtered_dates\n"
            "    JOIN: ws_sold_date_sk = d_date_sk\n"
            "    GROUP BY: ws_bill_customer_sk, d_year\n"
            "    AGGREGATE: STDDEV_SAMP(ws_net_paid) AS year_total\n"
            "    OUTPUT: ws_bill_customer_sk, d_year, year_total\n"
            "    EXPECTED_ROWS: ~200K (100K per year)\n"
            "    CONSUMERS: compare_ratios\n"
            "  compare_ratios:\n"
            "    FROM: store_agg s1\n"
            "      JOIN store_agg s2 ON s1.ss_customer_sk = s2.ss_customer_sk\n"
            "      JOIN web_agg w1  ON s1.ss_customer_sk = w1.ws_bill_customer_sk\n"
            "      JOIN web_agg w2  ON s1.ss_customer_sk = w2.ws_bill_customer_sk\n"
            "    WHERE: s1.d_year = 1999 AND s2.d_year = 1999 + 1\n"
            "           AND w1.d_year = 1999 AND w2.d_year = 1999 + 1\n"
            "           AND s1.year_total > 0 AND w1.year_total > 0\n"
            "           AND (w2.year_total / w1.year_total) > (s2.year_total / s1.year_total)\n"
            "    NOTE: The original uses CASE WHEN ... > 0 guards around divisions.\n"
            "          Preserve them — even though WHERE > 0 makes zero unreachable,\n"
            "          the guards prevent silent breakage if upstream filters change.\n"
            "    NOTE: ss_customer_sk (store) and ws_bill_customer_sk (web) are both FKs\n"
            "          to customer.c_customer_sk. They are equi-joined here.\n"
            "    OUTPUT: ss_customer_sk\n"
            "    EXPECTED_ROWS: ~4K\n"
            "    CONSUMERS: resolve_names\n"
            "  resolve_names:\n"
            "    FROM: compare_ratios JOIN customer\n"
            "    JOIN: ss_customer_sk = c_customer_sk\n"
            "    OUTPUT: c_customer_id AS customer_id, c_first_name AS customer_first_name, "
            "c_last_name AS customer_last_name\n"
            "    EXPECTED_ROWS: ~4K -> 100 after ORDER BY + LIMIT\n"
            "    ORDER BY: customer_first_name, customer_id, customer_last_name\n"
            "    LIMIT: 100"
        ),
        examples=["date_cte_isolate", "shared_dimension_multi_channel"],
        example_reasoning=(
            "date_cte_isolate (4.00x on Q6):\n"
            "  APPLY: Pre-filter date_dim into CTE. Q74 joins date_dim 4 times with "
            "d_year filters — pre-filtering from 73K to ~730 rows eliminates 4 full "
            "hash-join probes. Same pattern: date join -> date CTE -> probe reduction.\n"
            "  IGNORE: The decorrelated scalar subquery pattern from Q6 (month_seq "
            "lookup + category_avg CTE). Q74 has no correlated subquery.\n\n"
            "shared_dimension_multi_channel (1.30x on Q56):\n"
            "  APPLY: Shared dimension CTE across channels. Q74 has store_sales + "
            "web_sales both joining date_dim with the same d_year filter — a single "
            "filtered_dates CTE shared by both avoids redundant scans.\n"
            "  IGNORE: The BETWEEN/INTERVAL date filtering from Q56. Q74 uses "
            "d_year IN (1999, 2000), not date ranges."
        ),
        hazard_flags=(
            "- STDDEV_SAMP(ss_net_paid) FILTER (WHERE d_year = 1999) computed over "
            "a combined 1999+2000 group IS NOT EQUIVALENT to STDDEV_SAMP computed "
            "over only 1999 rows. The group membership changes the variance. "
            "The target DAG avoids this: store_agg and web_agg GROUP BY d_year, so "
            "each group is naturally partitioned by year. STDDEV_SAMP is computed "
            "correctly per-partition. The compare_ratios CTE then filters to the "
            "specific year via WHERE d_year = 1999.\n"
            "- Do NOT include a year_total CTE. The original UNION ALL CTE is fully "
            "replaced by store_agg + web_agg. Including both causes 0.68x regression.\n"
            "- The customer join is deferred to resolve_names (joins ~4K rows, not 5.4M). "
            "Do NOT join customer in store_agg or web_agg — use ss_customer_sk/ws_bill_customer_sk "
            "as the join key throughout."
        ),
    )

    return shared, worker


def main():
    from ado.knowledge import ADOFAISSRecommender

    original_sql = load_original_sql()
    explain_plan_text = load_explain_plan()
    dag, costs = parse_dag(original_sql, explain_plan_text)
    semantic_intents = load_semantic_intents()
    global_knowledge = load_global_knowledge()

    recommender = ADOFAISSRecommender()
    matched_examples = load_matched_examples(original_sql, recommender)
    all_available = load_full_catalog()
    constraints = load_constraints()
    regression_warnings = load_regression_warnings(original_sql, recommender)

    # ── Strategy leaderboard + archetype ──────────────────────────
    strategy_leaderboard = None
    query_archetype = None
    leaderboard_path = BENCHMARK_DIR / "strategy_leaderboard.json"
    if leaderboard_path.exists():
        strategy_leaderboard = json.loads(leaderboard_path.read_text())
        from ado.faiss_builder import extract_tags, classify_category
        query_archetype = classify_category(extract_tags(original_sql, dialect="duckdb"))
        print(f"Strategy leaderboard: {strategy_leaderboard['total_attempts']} attempts, "
              f"archetype={query_archetype}")
    else:
        print("No strategy leaderboard found (run build_strategy_leaderboard.py first)")

    # ── Engine profile ───────────────────────────────────────────────
    engine_profile = None
    engine_profile_path = CONSTRAINTS_DIR / "engine_profile_duckdb.json"
    if engine_profile_path.exists():
        engine_profile = json.loads(engine_profile_path.read_text())
        print(f"Engine profile: {engine_profile.get('engine', '?')} v{engine_profile.get('version_tested', '?')}")

    # ── Analyst prompt ──────────────────────────────────────────────
    from ado.prompts.analyst_briefing import build_analyst_briefing_prompt

    # Get engine version
    try:
        import duckdb
        dialect_version = duckdb.__version__
    except ImportError:
        dialect_version = None

    analyst_prompt = build_analyst_briefing_prompt(
        query_id=QUERY_ID,
        sql=original_sql,
        explain_plan_text=explain_plan_text,
        dag=dag,
        costs=costs,
        semantic_intents=semantic_intents,
        global_knowledge=global_knowledge,
        matched_examples=matched_examples,
        all_available_examples=all_available,
        constraints=constraints,
        regression_warnings=regression_warnings,
        dialect="duckdb",
        dialect_version=dialect_version,
        strategy_leaderboard=strategy_leaderboard,
        query_archetype=query_archetype,
        engine_profile=engine_profile,
    )

    analyst_path = SAMPLES_DIR / f"analyst_v2_{QUERY_ID}.md"
    analyst_path.write_text(analyst_prompt)
    print(f"\n{'='*70}")
    print(f"ANALYST PROMPT: {analyst_path}")
    print(f"  Length: {len(analyst_prompt)} chars, ~{len(analyst_prompt)//4} tokens")

    # ── Worker V2 prompt ────────────────────────────────────────────
    from ado.prompts.worker_v2 import build_worker_v2_prompt

    mock_shared, mock_worker = build_mock_worker_briefing()

    # Load the specific examples the mock analyst assigned to this worker
    worker_example_ids = set(mock_worker.examples)
    worker_examples = [ex for ex in matched_examples if ex.get("id") in worker_example_ids]
    # Fallback: if assigned examples not in tag-matched set, try full catalog
    if len(worker_examples) < len(worker_example_ids):
        examples_dir = ADO_DIR / "examples" / "duckdb"
        for eid in worker_example_ids:
            if not any(ex.get("id") == eid for ex in worker_examples):
                p = examples_dir / f"{eid}.json"
                if p.exists():
                    worker_examples.append(json.loads(p.read_text()))

    output_columns = []
    try:
        from ado.node_prompter import Prompter
        output_columns = Prompter._extract_output_columns(dag)
    except Exception:
        pass
    if not output_columns:
        output_columns = ["customer_id", "customer_first_name", "customer_last_name"]

    worker_prompt = build_worker_v2_prompt(
        worker_briefing=mock_worker,
        shared_briefing=mock_shared,
        examples=worker_examples,
        original_sql=original_sql,
        output_columns=output_columns,
        dialect="duckdb",
    )

    worker_path = SAMPLES_DIR / f"worker_v2_{QUERY_ID}.md"
    worker_path.write_text(worker_prompt)
    print(f"WORKER V2 PROMPT: {worker_path}")
    print(f"  Length: {len(worker_prompt)} chars, ~{len(worker_prompt)//4} tokens")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()

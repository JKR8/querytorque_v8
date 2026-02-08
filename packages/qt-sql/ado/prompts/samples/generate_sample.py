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
    """Create a realistic mock analyst briefing for Q74 Worker 2."""
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
            "Four separate full scans of store_sales (28M rows) and web_sales (7M rows), "
            "each joined with date_dim (73K rows) and customer (100K rows). "
            "The bottleneck is scan-bound: 4x fact table scans dominate at ~80% total cost. "
            "The optimizer already handles the INNER JOIN ordering well — do not restructure joins. "
            "DAG cost% is misleading: the 'customer' node shows 2% but it's probed 4 times."
        ),
        active_constraints=(
            "- union_cte_split_must_replace: If splitting a UNION into CTEs, the UNION "
            "in the main query MUST be replaced with references to the new CTEs\n"
            "- or_to_union_limit: Limit OR-to-UNION to 3 or fewer branches to avoid "
            "multiplying fact table scans\n"
            "- decorrelate_must_filter_first: When decorrelating, apply dimension filters "
            "before the main join to reduce probe-side cardinality"
        ),
        regression_warnings=(
            "1. union_cte_split on Q74 (0.73x regression):\n"
            "   CAUSE: Splitting the 4-way structure into separate CTEs forced the optimizer "
            "to materialize intermediate results that it previously streamed.\n"
            "   RULE: For Q74, keep the 4-way join structure intact. Optimize the INPUTS "
            "to the join (date filtering), not the join topology itself."
        ),
    )

    worker = BriefingWorker(
        worker_id=2,
        strategy="date_cte_isolate",
        target_dag=(
            "TARGET_DAG:\n"
            "  filtered_dates -> store_1999 -+\n"
            "  filtered_dates -> store_2000 -+\n"
            "  filtered_dates -> web_1999   -+-> main_query\n"
            "  filtered_dates -> web_2000   -+\n\n"
            "NODE_CONTRACTS:\n"
            "  filtered_dates:\n"
            "    FROM: date_dim\n"
            "    WHERE: d_year IN (1999, 2000)\n"
            "    OUTPUT: d_date_sk, d_year\n"
            "    CONSUMERS: store_1999, store_2000, web_1999, web_2000\n"
            "  store_1999:\n"
            "    FROM: store_sales JOIN customer JOIN filtered_dates\n"
            "    JOIN: c_customer_sk = ss_customer_sk, ss_sold_date_sk = d_date_sk\n"
            "    WHERE: d_year = 1999\n"
            "    GROUP BY: c_customer_id, c_first_name, c_last_name\n"
            "    AGGREGATE: STDDEV_SAMP(ss_net_paid) AS year_total\n"
            "    OUTPUT: customer_id, customer_first_name, customer_last_name, year_total\n"
            "    CONSUMERS: main_query\n"
            "  [store_2000, web_1999, web_2000: same pattern, different table/year]\n"
            "  main_query:\n"
            "    FROM: store_1999 JOIN store_2000 JOIN web_1999 JOIN web_2000\n"
            "    JOIN: all on customer_id (INNER — preserves intersection semantics)\n"
            "    WHERE: store_1999.year_total > 0 AND web_1999.year_total > 0\n"
            "           AND (web_2000/web_1999) > (store_2000/store_1999)\n"
            "    OUTPUT: customer_id, customer_first_name, customer_last_name\n"
            "    ORDER BY: customer_first_name, customer_id, customer_last_name\n"
            "    LIMIT: 100"
        ),
        examples=["date_cte_isolate", "dimension_cte_isolate"],
        example_reasoning=(
            "date_cte_isolate (4.00x on Q6): Q74 joins date_dim 4 times with d_year "
            "filters. Pre-filtering date_dim from 73K to ~730 rows eliminates 4 full "
            "hash-join probes. Same pattern: date join -> date CTE -> probe reduction.\n\n"
            "dimension_cte_isolate (1.93x on Q26): Q74 also joins customer 4 times. "
            "While customer can't be pre-filtered (no WHERE on customer), the date CTE "
            "pattern reduces probe-side cardinality which shrinks the customer join input."
        ),
        hazard_flags=(
            "- STDDEV_SAMP(ss_net_paid) FILTER (WHERE d_year = 1999) computed over "
            "a combined 1999+2000 group IS NOT EQUIVALENT to STDDEV_SAMP computed "
            "over only 1999 rows. The group membership changes the variance. "
            "You MUST either: (a) GROUP BY d_year first then pivot, or (b) pre-filter "
            "fact rows to the target year before aggregation.\n"
            "- Do NOT merge the 4 channel-year CTEs into fewer CTEs. Each must filter "
            "to its specific year BEFORE aggregation to preserve STDDEV_SAMP semantics."
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
    )

    analyst_path = SAMPLES_DIR / f"analyst_v2_{QUERY_ID}.md"
    analyst_path.write_text(analyst_prompt)
    print(f"\n{'='*70}")
    print(f"ANALYST PROMPT: {analyst_path}")
    print(f"  Length: {len(analyst_prompt)} chars, ~{len(analyst_prompt)//4} tokens")

    # ── Worker V2 prompt ────────────────────────────────────────────
    from ado.prompts.worker_v2 import build_worker_v2_prompt

    mock_shared, mock_worker = build_mock_worker_briefing()

    worker_examples = matched_examples[:2] if len(matched_examples) >= 2 else matched_examples

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

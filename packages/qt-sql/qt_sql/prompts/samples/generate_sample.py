#!/usr/bin/env python3
"""Generate rendered prompt samples for every prompt type.

Produces a versioned Prompt Pack (V0/, V1/, ...) with one .md file per
prompt builder. Uses real benchmark data from Q88 and the everyhousehold
enterprise pipeline.

Usage:
    cd <repo-root>
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m qt_sql.prompts.samples.generate_sample query_88 --version V0 --script paper/sql/everyhousehold_deidentified.sql

    # Generate just one prompt type:
    PYTHONPATH=... python3 -m qt_sql.prompts.samples.generate_sample query_88 --version V0 --only beam_script
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ── Paths ───────────────────────────────────────────────────────────
SAMPLES_DIR = Path(__file__).resolve().parent
ARCHIVE_SAMPLES_DIR = SAMPLES_DIR.parent / "archive" / "samples"
QT_SQL_DIR = SAMPLES_DIR.parent.parent           # qt_sql/
BENCHMARK_DIR = QT_SQL_DIR / "benchmarks" / "duckdb_tpcds"
CONSTRAINTS_DIR = QT_SQL_DIR / "constraints"
EXAMPLES_DIR = QT_SQL_DIR / "examples" / "duckdb"
BATCH_DIR = BENCHMARK_DIR / "swarm_batch_20260208_102033"
PROJECT_ROOT = QT_SQL_DIR.parent.parent.parent    # QueryTorque_V8/


def get_output_dir(version: str) -> Path:
    """Return versioned output directory, creating it if needed."""
    if str(version).strip().upper() == "V3":
        d = SAMPLES_DIR / "V3"
    else:
        d = ARCHIVE_SAMPLES_DIR / version
    d.mkdir(parents=True, exist_ok=True)
    return d


# ═══════════════════════════════════════════════════════════════════════
# Data loaders (parameterized by query_id)
# ═══════════════════════════════════════════════════════════════════════

def load_original_sql(query_id: str) -> str:
    sql_path = BATCH_DIR / query_id / "original.sql"
    if not sql_path.exists():
        sql_path = PROJECT_ROOT / "research" / "tpcds_queries" / f"{query_id}.sql"
    if not sql_path.exists():
        print(f"ERROR: Cannot find SQL for {query_id}")
        sys.exit(1)
    sql = sql_path.read_text().strip()
    print(f"Loaded SQL: {len(sql)} chars from {sql_path}")
    return sql


def load_explain_plan(query_id: str) -> Optional[str]:
    for subdir in ["sf10", "sf5", ""]:
        d = BENCHMARK_DIR / "explains" / subdir if subdir else BENCHMARK_DIR / "explains"
        p = d / f"{query_id}.json"
        if p.exists():
            data = json.loads(p.read_text())
            plan_text = data.get("plan_text")
            if plan_text:
                print(f"Loaded EXPLAIN: {len(plan_text)} chars from {p}")
                return plan_text
    print("No EXPLAIN plan found (will show fallback note)")
    return None


def parse_dag(sql: str, explain_plan_text: Optional[str] = None):
    try:
        from qt_sql.dag import LogicalTreeBuilder, CostAnalyzer
        builder = LogicalTreeBuilder(sql, dialect="duckdb")
        dag = builder.build()

        plan_context = None
        if explain_plan_text:
            try:
                plan_json = json.loads(explain_plan_text) if explain_plan_text.strip().startswith("{") else None
                if plan_json:
                    from qt_sql.plan_analyzer import analyze_plan_for_optimization
                    plan_context = analyze_plan_for_optimization(plan_json, sql, "duckdb")
                    print(f"EXPLAIN -> OptimizationContext: {len(plan_context.table_scans)} scans, "
                          f"{len(plan_context.bottleneck_operators)} operators")
            except Exception as e:
                print(f"EXPLAIN analysis failed ({e}), using heuristic costs")

        analyzer = CostAnalyzer(dag, plan_context)
        costs = analyzer.analyze()
        nonzero = sum(1 for c in costs.values() if c.cost_pct > 0)
        print(f"Parsed logical tree: {len(dag.nodes)} nodes, {nonzero} with cost > 0%")
        return dag, costs
    except Exception as e:
        print(f"Logical-tree parsing failed ({e}), using stub")
        from types import SimpleNamespace
        return SimpleNamespace(nodes={}, edges=[]), {}


def load_semantic_intents(query_id: str):
    intents_path = BENCHMARK_DIR / "semantic_intents.json"
    if not intents_path.exists():
        print("No semantic intents found")
        return None

    import re
    data = json.loads(intents_path.read_text())
    m = re.match(r"(?:query_?)(\d+\w?)", query_id)
    qnum = m.group(1) if m else query_id
    for q in data.get("queries", []):
        qid = q.get("query_id", "")
        if qid == query_id or qid == f"q{qnum}":
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


def load_matched_examples(sql: str, recommender) -> List[Dict]:
    matched = []
    if recommender._initialized:
        matches = recommender.find_similar_examples(sql, k=16, dialect="duckdb")
        examples_dir = EXAMPLES_DIR
        for ex_id, score, meta in matches:
            ex_path = examples_dir / f"{ex_id}.json"
            if ex_path.exists():
                matched.append(json.loads(ex_path.read_text()))
        print(f"Tag-matched: {len(matched)} examples")
    else:
        print("Recommender not initialized")
    return matched


def load_full_catalog() -> List[Dict]:
    catalog = []
    examples_dir = EXAMPLES_DIR
    if examples_dir.exists():
        for p in sorted(examples_dir.glob("*.json")):
            try:
                d = json.loads(p.read_text())
                catalog.append({
                    "id": d.get("id", p.stem),
                    "speedup": d.get("verified_speedup", "?"),
                    "description": d.get("description", "")[:80],
                })
            except Exception as e:
                print(f"  WARNING: Failed to load example {p.name}: {e}")
    print(f"Full catalog: {len(catalog)} examples")
    return catalog


def load_constraints() -> List[Dict]:
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
            except Exception as e:
                print(f"  WARNING: Failed to load constraint {p.name}: {e}")
        result.sort(key=lambda c: severity_order.get(c.get("severity", "MEDIUM"), 2))
    print(f"Constraints: {len(result)}")
    return result


def load_regression_warnings(sql: str, recommender) -> List[Dict]:
    warnings = []
    if recommender._initialized:
        reg_matches = recommender.find_relevant_regressions(sql, k=3, dialect="duckdb")
        reg_dir = EXAMPLES_DIR / "regressions"
        for ex_id, score, meta in reg_matches:
            reg_path = reg_dir / f"{ex_id}.json"
            if reg_path.exists():
                warnings.append(json.loads(reg_path.read_text()))
    print(f"Regression warnings: {len(warnings)}")
    return warnings


def load_strategy_leaderboard(original_sql: str):
    leaderboard_path = BENCHMARK_DIR / "strategy_leaderboard.json"
    if not leaderboard_path.exists():
        print("No strategy leaderboard found")
        return None, None
    strategy_leaderboard = json.loads(leaderboard_path.read_text())
    from qt_sql.tag_index import extract_tags, classify_category
    query_archetype = classify_category(extract_tags(original_sql, dialect="duckdb"))
    print(f"Strategy leaderboard: {strategy_leaderboard['total_attempts']} attempts, "
          f"archetype={query_archetype}")
    return strategy_leaderboard, query_archetype


def load_engine_profile(dialect: str = "duckdb") -> Optional[Dict]:
    engine_profile_path = CONSTRAINTS_DIR / f"engine_profile_{dialect}.json"
    if engine_profile_path.exists():
        engine_profile = json.loads(engine_profile_path.read_text())
        print(f"Engine profile: {engine_profile.get('engine', '?')} "
              f"v{engine_profile.get('version_tested', '?')}")
        return engine_profile
    return None


def get_dialect_version() -> Optional[str]:
    try:
        import duckdb
        return duckdb.__version__
    except ImportError:
        return None


def load_exploit_algorithm(dialect: str = "duckdb") -> Optional[str]:
    """Load the distilled knowledge playbook from knowledge/{dialect}.md."""
    try:
        from qt_sql.prompter import load_exploit_algorithm as _load
        text = _load(dialect)
        if text:
            print(f"Exploit algorithm: {len(text)} chars from knowledge/{dialect}.md")
        return text
    except Exception as e:
        print(f"Failed to load exploit algorithm: {e}")
        return None


def load_qerror_analysis(query_id: str):
    """Load plan_json from explain cache and compute Q-Error analysis."""
    for subdir in ["", "sf10", "sf5"]:
        d = BENCHMARK_DIR / "explains" / subdir if subdir else BENCHMARK_DIR / "explains"
        p = d / f"{query_id}.json"
        if p.exists():
            try:
                data = json.loads(p.read_text())
                plan_json = data.get("plan_json")
                if plan_json and plan_json != {} and plan_json != []:
                    from qt_sql.qerror import analyze_plan_qerror
                    analysis = analyze_plan_qerror(plan_json)
                    if analysis.signals or analysis.structural_flags:
                        print(f"Q-Error: {analysis.severity} max_q={analysis.max_q_error:.0f} "
                              f"dir={analysis.direction} locus={analysis.locus} "
                              f"routing={analysis.pathology_candidates}")
                    return analysis
            except Exception as e:
                print(f"Q-Error analysis failed: {e}")
    print("No Q-Error data (plan_json empty or missing)")
    return None


def load_examples_by_ids(
    example_ids: List[str],
    matched_examples: List[Dict],
) -> List[Dict]:
    """Load gold examples by ID, falling back to disk."""
    result = []
    found_ids = set()
    for ex in matched_examples:
        if ex.get("id") in example_ids:
            result.append(ex)
            found_ids.add(ex.get("id"))
    if len(found_ids) < len(example_ids):
        examples_dir = EXAMPLES_DIR
        for eid in example_ids:
            if eid not in found_ids:
                p = examples_dir / f"{eid}.json"
                if p.exists():
                    result.append(json.loads(p.read_text()))
    return result


def extract_output_columns(dag) -> List[str]:
    try:
        from qt_sql.prompter import Prompter
        return Prompter._extract_output_columns(dag)
    except Exception:
        return []


def load_worker_sql(query_id: str, worker_id: int) -> str:
    """Load actual worker SQL from batch data."""
    p = BATCH_DIR / query_id / f"worker_{worker_id}_sql.sql"
    if p.exists():
        return p.read_text().strip()
    return f"-- Worker {worker_id} SQL not found"


def load_assignments(query_id: str) -> List[Dict]:
    """Load worker assignments from batch data."""
    p = BATCH_DIR / query_id / "assignments.json"
    if p.exists():
        return json.loads(p.read_text())
    return []


def load_benchmark_results(query_id: str) -> Optional[Dict]:
    """Load benchmark results from batch data."""
    p = BATCH_DIR / query_id / "benchmark_iter0.json"
    if p.exists():
        return json.loads(p.read_text())
    return None


# ═══════════════════════════════════════════════════════════════════════
# Shared context holder
# ═══════════════════════════════════════════════════════════════════════

class PromptContext:
    """Holds all loaded data needed by generators."""

    def __init__(self, query_id: str):
        self.query_id = query_id
        self.original_sql = load_original_sql(query_id)
        self.explain_plan_text = load_explain_plan(query_id)
        self.dag, self.costs = parse_dag(self.original_sql, self.explain_plan_text)
        self.semantic_intents = load_semantic_intents(query_id)
        self.global_knowledge = load_global_knowledge()

        from qt_sql.knowledge import TagRecommender
        self.recommender = TagRecommender()

        self.matched_examples = load_matched_examples(self.original_sql, self.recommender)
        self.all_available = load_full_catalog()
        self.constraints = load_constraints()
        self.regression_warnings = load_regression_warnings(self.original_sql, self.recommender)
        self.strategy_leaderboard, self.query_archetype = load_strategy_leaderboard(self.original_sql)
        self.engine_profile = load_engine_profile("duckdb")
        self.dialect_version = get_dialect_version()
        self.exploit_algorithm_text = load_exploit_algorithm("duckdb")
        self.qerror_analysis = load_qerror_analysis(query_id)

        # Q88-specific output columns
        self.output_columns = extract_output_columns(self.dag)
        if not self.output_columns:
            self.output_columns = [
                "h8_30_to_9", "h9_to_9_30", "h9_30_to_10", "h10_to_10_30",
                "h10_30_to_11", "h11_to_11_30", "h11_30_to_12", "h12_to_12_30",
            ]

        # Load real batch data for mock worker results
        self.assignments = load_assignments(query_id)
        self.benchmark_results = load_benchmark_results(query_id)


# ═══════════════════════════════════════════════════════════════════════
# Mock data builders (Q88 — using real batch data)
# ═══════════════════════════════════════════════════════════════════════

def build_q88_worker_results(ctx: PromptContext) -> list:
    """Build WorkerResult objects from real Q88 batch data."""
    from qt_sql.schemas import WorkerResult

    results = []
    bench = ctx.benchmark_results or {}
    worker_benchmarks_list = bench.get("workers", [])
    # Index by worker_id for easy lookup
    worker_benchmarks = {w["worker_id"]: w for w in worker_benchmarks_list if isinstance(w, dict)}

    for assignment in ctx.assignments:
        wid = assignment["worker_id"]
        w_bench = worker_benchmarks.get(wid, {})
        w_sql = load_worker_sql(ctx.query_id, wid)
        speedup = w_bench.get("speedup", 0.0)
        status = w_bench.get("status", "PASS")

        results.append(WorkerResult(
            worker_id=wid,
            strategy=assignment["strategy"],
            examples_used=assignment["examples"],
            optimized_sql=w_sql,
            speedup=speedup,
            status=status,
            transforms=assignment["examples"][:2],
            hint=assignment.get("hint", ""),
            exploratory=(wid == 4),
        ))

    if not results:
        # Fallback with synthetic data if no batch data
        results = _build_fallback_worker_results()
    return results


def _build_fallback_worker_results() -> list:
    """Fallback synthetic WorkerResult objects."""
    from qt_sql.schemas import WorkerResult
    return [
        WorkerResult(
            worker_id=1,
            strategy="conservative_early_reduction",
            examples_used=["early_filter", "pushdown", "materialize_cte"],
            optimized_sql="-- W1: dimension CTE isolation\nWITH ...",
            speedup=5.27,
            status="PASS",
            transforms=["early_filter", "pushdown"],
            hint="Apply early filtering to dimension tables before joining",
        ),
        WorkerResult(
            worker_id=2,
            strategy="moderate_dimension_isolation",
            examples_used=["dimension_cte_isolate", "date_cte_isolate", "shared_dimension_multi_channel"],
            optimized_sql="-- W2: single-pass time window CASE (BEST)\nWITH ...",
            speedup=6.24,
            status="PASS",
            transforms=["single_pass_aggregation", "multi_date_range_cte"],
            hint="Consolidate 8 subqueries into single-pass with CASE time windows",
        ),
        WorkerResult(
            worker_id=3,
            strategy="aggressive_single_pass_restructure",
            examples_used=["single_pass_aggregation", "prefetch_fact_join", "multi_date_range_cte"],
            optimized_sql="-- W3: prefetch + time slices\nWITH ...",
            speedup=5.85,
            status="PASS",
            transforms=["prefetch_fact_join", "early_filter"],
            hint="Prefetch filtered dimensions, single-pass with relaxed time filter",
        ),
        WorkerResult(
            worker_id=4,
            strategy="novel_structural_transform",
            examples_used=["or_to_union", "union_cte_split", "composite_decorrelate_union"],
            optimized_sql="-- W4: time slices + qualified_sales CTE\nWITH ...",
            speedup=6.10,
            status="PASS",
            transforms=["single_pass_aggregation"],
            hint="Time slices with CASE labels, qualified_sales CTE for single scan",
            exploratory=True,
        ),
    ]


def build_q88_mock_briefing(ctx: PromptContext):
    """Build realistic mock analyst briefing for Q88 Worker 2 (best: 6.24x)."""
    from qt_sql.prompts.parsers import BriefingShared, BriefingWorker

    shared = BriefingShared(
        semantic_contract=(
            "This query counts store sales by 8 consecutive half-hour time windows "
            "(8:30-12:30) at store 'ese' for households matching 3 specific "
            "(hd_dep_count, hd_vehicle_count) conditions. Returns a single row with "
            "8 COUNT columns. The 8 subqueries are independent — same fact table, same "
            "dim filters, different time_dim conditions. Cross-join semantics: each "
            "count is independent."
        ),
        bottleneck_diagnosis=(
            "EXPLAIN shows 8 independent SEQ_SCAN pipelines on store_sales, each "
            "joining time_dim, household_demographics, and store with identical filters "
            "except the time window. Total: 8x store_sales scan, 8x household_demographics "
            "scan, 8x store scan, 8x time_dim scan. The dominant cost is the repeated "
            "store_sales scans (8x ~2.5M rows each = 20M rows total). A single-pass "
            "scan with CASE-based time window classification consolidates to 1x scan."
        ),
        active_constraints=(
            "- LITERAL_PRESERVATION: hd_dep_count = -1, hd_dep_count = 4, "
            "hd_dep_count = 3 and corresponding hd_vehicle_count bounds must be "
            "preserved exactly.\n"
            "- SEMANTIC_EQUIVALENCE: Each COUNT must produce identical results — "
            "no row duplication from joins.\n"
            "- COMPLETE_OUTPUT: All 8 output columns with exact aliases.\n"
            "- CTE_COLUMN_COMPLETENESS: Every CTE must pass through all columns "
            "needed by downstream consumers."
        ),
        regression_warnings=(
            "None applicable — Q88's 8 independent subqueries have no "
            "cross-CTE dependencies that could cause the known regression patterns."
        ),
    )

    worker = BriefingWorker(
        worker_id=2,
        strategy="moderate_dimension_isolation + single_pass_aggregation",
        target_logical_tree=(
            "TARGET_LOGICAL_TREE:\n"
            "  filtered_store -> sales_with_time\n"
            "  filtered_hd -> sales_with_time\n"
            "  time_ranges -> sales_with_time -> final_counts\n\n"
            "NODE_CONTRACTS:\n"
            "  filtered_store:\n"
            "    FROM: store\n"
            "    WHERE: s_store_name = 'ese'\n"
            "    OUTPUT: s_store_sk\n"
            "    EXPECTED_ROWS: ~1-5\n"
            "    CONSUMERS: sales_with_time\n"
            "  filtered_hd:\n"
            "    FROM: household_demographics\n"
            "    WHERE: (hd_dep_count = -1 AND hd_vehicle_count <= 1)\n"
            "       OR (hd_dep_count = 4 AND hd_vehicle_count <= 6)\n"
            "       OR (hd_dep_count = 3 AND hd_vehicle_count <= 5)\n"
            "    OUTPUT: hd_demo_sk\n"
            "    EXPECTED_ROWS: ~1200\n"
            "    CONSUMERS: sales_with_time\n"
            "  time_ranges:\n"
            "    FROM: time_dim\n"
            "    WHERE: (t_hour BETWEEN 8 AND 12)\n"
            "    SELECT: t_time_sk, CASE WHEN t_hour=8 AND t_minute>=30 THEN 1 "
            "WHEN t_hour=9 AND t_minute<30 THEN 2 ... WHEN t_hour=12 AND t_minute<30 "
            "THEN 8 END AS time_window\n"
            "    OUTPUT: t_time_sk, time_window\n"
            "    EXPECTED_ROWS: ~240 (8 half-hour windows x ~30 per window)\n"
            "    CONSUMERS: sales_with_time\n"
            "  sales_with_time:\n"
            "    FROM: store_sales JOIN filtered_store JOIN filtered_hd JOIN time_ranges\n"
            "    JOIN: ss_store_sk = s_store_sk, ss_hdemo_sk = hd_demo_sk, "
            "ss_sold_time_sk = t_time_sk\n"
            "    OUTPUT: time_window\n"
            "    EXPECTED_ROWS: ~10K-50K\n"
            "    CONSUMERS: final_counts\n"
            "  final_counts:\n"
            "    FROM: sales_with_time\n"
            "    AGGREGATE: COUNT(CASE WHEN time_window = N THEN 1 END) for N=1..8\n"
            "    OUTPUT: h8_30_to_9, h9_to_9_30, h9_30_to_10, h10_to_10_30, "
            "h10_30_to_11, h11_to_11_30, h11_30_to_12, h12_to_12_30\n"
            "    EXPECTED_ROWS: 1"
        ),
        examples=["dimension_cte_isolate", "date_cte_isolate", "shared_dimension_multi_channel"],
        example_adaptation=(
            "dimension_cte_isolate:\n"
            "  APPLY: Pre-filter household_demographics and store into CTEs. "
            "Q88 joins these 8 times each — pre-filtering from 7.2K/1K rows "
            "to ~1200/5 rows eliminates repeated hash probes.\n"
            "  IGNORE: The date_dim isolation from the original example — Q88 "
            "uses time_dim, not date_dim.\n\n"
            "date_cte_isolate:\n"
            "  APPLY: Same pattern but for time_dim. Add CASE-based time window "
            "classification during the CTE so downstream only needs the window ID.\n"
            "  IGNORE: The d_year filter pattern — Q88 uses t_hour/t_minute.\n\n"
            "shared_dimension_multi_channel:\n"
            "  APPLY: Shared dimension CTEs across all 8 time windows. "
            "Each window shares the same store + household filters.\n"
            "  IGNORE: The multi-channel (store/web/catalog) structure — Q88 "
            "is single-channel (store_sales only)."
        ),
        hazard_flags=(
            "- The 8 subqueries use different OR conditions on household_demographics "
            "for different time windows. VERIFY: all 8 subqueries actually share the "
            "same hd filters — they do (all use the same 3 OR branches). The time "
            "window is the only differentiator.\n"
            "- Do NOT use FILTER clause with COUNT — use COUNT(CASE WHEN ... THEN 1 END) "
            "for maximum DuckDB compatibility.\n"
            "- Preserve exact literal values: hd_dep_count = -1, hd_dep_count = 4, "
            "hd_dep_count = 3, hd_vehicle_count <= -1+2, <= 4+2, <= 3+2."
        ),
    )

    return shared, worker


def build_q88_snipe_analysis():
    """Build mock SnipeAnalysis from Q88 batch results."""
    from qt_sql.prompts.parsers import SnipeAnalysis

    return SnipeAnalysis(
        failure_synthesis=(
            "All 4 workers achieved strong wins (5.27x–6.24x) by consolidating "
            "8 independent store_sales scans into a single pass. W2 (6.24x) was "
            "best because it classified time windows in the CTE itself using CASE, "
            "avoiding any post-join filtering. W1 (5.27x) was slowest because it "
            "kept 8 correlated subqueries against a single qualified_sales CTE — "
            "still 8 scans of the CTE. W3/W4 used similar single-pass strategies "
            "with minor structural differences."
        ),
        best_foundation=(
            "Worker 2 (6.24x): time_ranges CTE with CASE window classification, "
            "filtered_store + filtered_hd dimension CTEs, single sales_with_time "
            "CTE joining all dimensions, final COUNT(CASE) aggregation. Clean, "
            "minimal structure."
        ),
        unexplored_angles=(
            "1. Aggregate pushdown: compute counts directly in the sales_with_time "
            "CTE instead of materializing individual rows then aggregating\n"
            "2. Hash join order optimization: join the smallest dimension first "
            "(store ~5 rows) to reduce intermediate cardinality earliest\n"
            "3. Bit manipulation: encode time window as a bitmap for the 8 windows "
            "and use a single GROUP BY with bit operations"
        ),
        strategy_guidance=(
            "W2's approach is nearly optimal — 6.24x from a single-pass scan. "
            "The remaining headroom is in join ordering (smallest dim first) and "
            "potentially pushing the aggregation into a single SELECT without "
            "materializing sales_with_time as a separate CTE."
        ),
        examples=["single_pass_aggregation", "dimension_cte_isolate"],
        example_adaptation=(
            "single_pass_aggregation: W2 already applied this. The sniper should "
            "focus on whether inlining the final aggregation into the CTE body "
            "(no separate final_counts step) improves performance.\n"
            "dimension_cte_isolate: Already applied by all workers. Verify that "
            "join order matches dimension cardinality (store first, then hd, then time)."
        ),
        hazard_flags=(
            "- All workers passed — no semantic errors to avoid\n"
            "- The 8 COUNT(CASE) pattern is proven correct\n"
            "- Avoid re-introducing 8 separate scans (the original bottleneck)"
        ),
        retry_worthiness="LOW",
        retry_digest=(
            "All 4 workers achieved 5.27x–6.24x. W2's single-pass approach is "
            "near-optimal. Limited headroom for further improvement. A retry "
            "would focus on micro-optimizations (join order, CTE inlining)."
        ),
        raw="",
    )


# ═══════════════════════════════════════════════════════════════════════
# Generator functions — one per prompt type
# ═══════════════════════════════════════════════════════════════════════

def _write_and_report(path: Path, content: str, label: str) -> None:
    path.write_text(content)
    print(f"  {label}: {path.name} ({len(content)} chars, ~{len(content)//4} tokens)")


def generate_beam_script(out_dir: Path, script_path: str) -> None:
    """01 — Script beam using ScriptParser + build_script_beam_prompt()."""
    from qt_sql.prompts.analyst_briefing import build_script_beam_prompt
    from qt_sql.script_parser import ScriptParser

    script_file = Path(script_path)
    if not script_file.is_absolute():
        script_file = PROJECT_ROOT / script_file
    if not script_file.exists():
        print(f"  SKIP beam_script: {script_file} not found")
        return

    sql_script = script_file.read_text()
    print(f"Loaded script: {len(sql_script)} chars, {sql_script.count(chr(10))+1} lines from {script_file}")

    parser = ScriptParser(sql_script, dialect="duckdb")
    script_dag = parser.parse()
    print(f"Script dependency graph: {len(script_dag.statements)} statements, "
          f"{len(script_dag.optimization_targets())} optimization targets")

    engine_profile = load_engine_profile("duckdb")
    constraints = load_constraints()

    prompt = build_script_beam_prompt(
        sql_script=sql_script,
        script_dag=script_dag,
        dialect="duckdb",
        engine_profile=engine_profile,
        constraints=constraints,
    )

    _write_and_report(
        out_dir / "01_beam_script_everyhousehold.md",
        prompt, "BEAM SCRIPT",
    )


def generate_beam_query(ctx: PromptContext, out_dir: Path) -> None:
    """02 — Query beam using build_analyst_briefing_prompt(mode='beam')."""
    from qt_sql.prompts.analyst_briefing import build_analyst_briefing_prompt

    prompt = build_analyst_briefing_prompt(
        query_id=ctx.query_id,
        sql=ctx.original_sql,
        explain_plan_text=ctx.explain_plan_text,
        dag=ctx.dag,
        costs=ctx.costs,
        semantic_intents=ctx.semantic_intents,
        global_knowledge=ctx.global_knowledge,
        constraints=ctx.constraints,
        dialect="duckdb",
        dialect_version=ctx.dialect_version,
        strategy_leaderboard=ctx.strategy_leaderboard,
        query_archetype=ctx.query_archetype,
        engine_profile=ctx.engine_profile,
        exploit_algorithm_text=ctx.exploit_algorithm_text,
        qerror_analysis=ctx.qerror_analysis,
        mode="beam",
    )

    _write_and_report(
        out_dir / f"02_beam_{ctx.query_id}.md",
        prompt, "ONESHOT QUERY",
    )


def generate_expert(ctx: PromptContext, out_dir: Path) -> None:
    """03+04 — Expert analyst + expert worker."""
    from qt_sql.prompts.analyst_briefing import build_analyst_briefing_prompt
    from qt_sql.prompts.worker import build_worker_prompt

    # 03: Expert analyst
    analyst_prompt = build_analyst_briefing_prompt(
        query_id=ctx.query_id,
        sql=ctx.original_sql,
        explain_plan_text=ctx.explain_plan_text,
        dag=ctx.dag,
        costs=ctx.costs,
        semantic_intents=ctx.semantic_intents,
        global_knowledge=ctx.global_knowledge,
        constraints=ctx.constraints,
        dialect="duckdb",
        dialect_version=ctx.dialect_version,
        strategy_leaderboard=ctx.strategy_leaderboard,
        query_archetype=ctx.query_archetype,
        engine_profile=ctx.engine_profile,
        exploit_algorithm_text=ctx.exploit_algorithm_text,
        qerror_analysis=ctx.qerror_analysis,
        mode="expert",
    )

    _write_and_report(
        out_dir / f"03_expert_analyst_{ctx.query_id}.md",
        analyst_prompt, "EXPERT ANALYST",
    )

    # 04: Expert worker (uses same mock briefing as beam worker)
    mock_shared, mock_worker = build_q88_mock_briefing(ctx)
    worker_examples = load_examples_by_ids(mock_worker.examples, ctx.matched_examples)

    worker_prompt = build_worker_prompt(
        worker_briefing=mock_worker,
        shared_briefing=mock_shared,
        examples=worker_examples,
        original_sql=ctx.original_sql,
        output_columns=ctx.output_columns,
        dialect="duckdb",
        engine_version=ctx.dialect_version,
    )

    _write_and_report(
        out_dir / f"04_expert_worker_{ctx.query_id}.md",
        worker_prompt, "EXPERT WORKER",
    )


def generate_beam_analyst(ctx: PromptContext, out_dir: Path) -> None:
    """05 — Beam analyst briefing."""
    from qt_sql.prompts.analyst_briefing import build_analyst_briefing_prompt

    prompt = build_analyst_briefing_prompt(
        query_id=ctx.query_id,
        sql=ctx.original_sql,
        explain_plan_text=ctx.explain_plan_text,
        dag=ctx.dag,
        costs=ctx.costs,
        semantic_intents=ctx.semantic_intents,
        global_knowledge=ctx.global_knowledge,
        constraints=ctx.constraints,
        dialect="duckdb",
        dialect_version=ctx.dialect_version,
        strategy_leaderboard=ctx.strategy_leaderboard,
        query_archetype=ctx.query_archetype,
        engine_profile=ctx.engine_profile,
        exploit_algorithm_text=ctx.exploit_algorithm_text,
        qerror_analysis=ctx.qerror_analysis,
        mode="beam",
    )

    _write_and_report(
        out_dir / f"05_beam_analyst_{ctx.query_id}.md",
        prompt, "BEAM ANALYST",
    )


def generate_fan_out(ctx: PromptContext, out_dir: Path) -> None:
    """06 — Fan-out prompt."""
    from qt_sql.prompts.fan_out import build_fan_out_prompt

    prompt = build_fan_out_prompt(
        query_id=ctx.query_id,
        sql=ctx.original_sql,
        dag=ctx.dag,
        costs=ctx.costs,
        matched_examples=ctx.matched_examples,
        all_available_examples=ctx.all_available,
        regression_warnings=ctx.regression_warnings,
        dialect="duckdb",
    )

    _write_and_report(
        out_dir / f"06_fan_out_{ctx.query_id}.md",
        prompt, "FAN-OUT",
    )


def generate_beam_worker(ctx: PromptContext, out_dir: Path) -> None:
    """07 — Beam worker prompt (Worker 2, best worker)."""
    from qt_sql.prompts.worker import build_worker_prompt

    mock_shared, mock_worker = build_q88_mock_briefing(ctx)
    worker_examples = load_examples_by_ids(mock_worker.examples, ctx.matched_examples)

    prompt = build_worker_prompt(
        worker_briefing=mock_worker,
        shared_briefing=mock_shared,
        examples=worker_examples,
        original_sql=ctx.original_sql,
        output_columns=ctx.output_columns,
        dialect="duckdb",
        engine_version=ctx.dialect_version,
    )

    _write_and_report(
        out_dir / f"07_worker_{ctx.query_id}.md",
        prompt, "SWARM WORKER",
    )


def generate_snipe(ctx: PromptContext, out_dir: Path) -> None:
    """08+09+10 — Snipe analyst + sniper iter1 + sniper iter2."""
    from qt_sql.prompts.snipe import build_snipe_analyst_prompt, build_sniper_prompt
    from qt_sql.schemas import WorkerResult

    worker_results = build_q88_worker_results(ctx)
    mock_snipe_analysis = build_q88_snipe_analysis()

    # 08: Snipe analyst
    snipe_analyst_prompt = build_snipe_analyst_prompt(
        query_id=ctx.query_id,
        original_sql=ctx.original_sql,
        worker_results=worker_results,
        target_speedup=2.0,
        dag=ctx.dag,
        costs=ctx.costs,
        explain_plan_text=ctx.explain_plan_text,
        engine_profile=ctx.engine_profile,
        constraints=ctx.constraints,
        matched_examples=ctx.matched_examples,
        all_available_examples=ctx.all_available,
        semantic_intents=ctx.semantic_intents,
        regression_warnings=ctx.regression_warnings,
        dialect="duckdb",
        dialect_version=ctx.dialect_version,
    )

    _write_and_report(
        out_dir / f"08_snipe_analyst_{ctx.query_id}.md",
        snipe_analyst_prompt, "SNIPE ANALYST",
    )

    # Get best worker SQL for sniper
    best_worker = max(worker_results, key=lambda w: w.speedup)
    best_sql = best_worker.optimized_sql

    # Sniper examples
    sniper_example_ids = mock_snipe_analysis.examples
    sniper_examples = load_examples_by_ids(sniper_example_ids, ctx.matched_examples)

    # 09: Sniper iteration 1
    sniper_prompt_1 = build_sniper_prompt(
        snipe_analysis=mock_snipe_analysis,
        original_sql=ctx.original_sql,
        worker_results=worker_results,
        best_worker_sql=best_sql,
        examples=sniper_examples,
        output_columns=ctx.output_columns,
        dag=ctx.dag,
        costs=ctx.costs,
        engine_profile=ctx.engine_profile,
        constraints=ctx.constraints,
        semantic_intents=ctx.semantic_intents,
        regression_warnings=ctx.regression_warnings,
        dialect="duckdb",
        engine_version=ctx.dialect_version,
        target_speedup=2.0,
        previous_sniper_result=None,
    )

    _write_and_report(
        out_dir / f"09_sniper_iter1_{ctx.query_id}.md",
        sniper_prompt_1, "SNIPER ITER1",
    )

    # 10: Sniper iteration 2 (with previous result)
    mock_sniper1_result = WorkerResult(
        worker_id=5,
        strategy="single_pass_aggregation + join_order_optimization",
        examples_used=["single_pass_aggregation", "dimension_cte_isolate"],
        optimized_sql=best_sql,
        speedup=5.80,
        status="PASS",
        transforms=["single_pass_aggregation", "dimension_cte_isolate"],
        hint="Single-pass with optimized join order — close to W2 but not better",
    )

    sniper_prompt_2 = build_sniper_prompt(
        snipe_analysis=mock_snipe_analysis,
        original_sql=ctx.original_sql,
        worker_results=worker_results + [mock_sniper1_result],
        best_worker_sql=best_sql,
        examples=sniper_examples,
        output_columns=ctx.output_columns,
        dag=ctx.dag,
        costs=ctx.costs,
        engine_profile=ctx.engine_profile,
        constraints=ctx.constraints,
        semantic_intents=ctx.semantic_intents,
        regression_warnings=ctx.regression_warnings,
        dialect="duckdb",
        engine_version=ctx.dialect_version,
        target_speedup=2.0,
        previous_sniper_result=mock_sniper1_result,
    )

    _write_and_report(
        out_dir / f"10_sniper_iter2_{ctx.query_id}.md",
        sniper_prompt_2, "SNIPER ITER2",
    )


def generate_pg_tuner(ctx: PromptContext, out_dir: Path) -> None:
    """11 — PG tuner prompt with mock PG settings."""
    from qt_sql.prompts.pg_tuner import build_pg_tuner_prompt

    # Mock PG settings (realistic PG14.3 config)
    mock_settings = {
        "shared_buffers": "2GB",
        "effective_cache_size": "6GB",
        "work_mem": "64MB",
        "maintenance_work_mem": "512MB",
        "max_parallel_workers": "4",
        "max_parallel_workers_per_gather": "2",
        "max_connections": "100",
        "random_page_cost": "4.0",
        "effective_io_concurrency": "200",
        "jit": "on",
    }

    pg_engine_profile = load_engine_profile("postgresql")

    prompt = build_pg_tuner_prompt(
        query_sql=ctx.original_sql,
        explain_plan=None,  # No PG EXPLAIN for DuckDB query — mock scenario
        current_settings=mock_settings,
        engine_profile=pg_engine_profile,
        baseline_ms=1415.6,
    )

    _write_and_report(
        out_dir / f"11_pg_tuner_{ctx.query_id}.md",
        prompt, "PG TUNER",
    )


# ═══════════════════════════════════════════════════════════════════════
# Generator registry
# ═══════════════════════════════════════════════════════════════════════

GENERATORS = {
    "beam_script": "01_beam_script",
    "beam_query": "02_beam_query",
    "expert": "03+04_expert",
    "beam_analyst": "05_beam_analyst",
    "fan_out": "06_fan_out",
    "beam_worker": "07_worker",
    "snipe": "08+09+10_snipe",
    "pg_tuner": "11_pg_tuner",
}


def run_generator(
    name: str,
    ctx: Optional[PromptContext],
    out_dir: Path,
    script_path: Optional[str],
) -> None:
    """Run a single generator by name."""
    if name == "beam_script":
        if not script_path:
            print(f"  SKIP {name}: no --script provided")
            return
        generate_beam_script(out_dir, script_path)
    elif name == "beam_query":
        generate_beam_query(ctx, out_dir)
    elif name == "expert":
        generate_expert(ctx, out_dir)
    elif name == "beam_analyst":
        generate_beam_analyst(ctx, out_dir)
    elif name == "fan_out":
        generate_fan_out(ctx, out_dir)
    elif name == "beam_worker":
        generate_beam_worker(ctx, out_dir)
    elif name == "snipe":
        generate_snipe(ctx, out_dir)
    elif name == "pg_tuner":
        generate_pg_tuner(ctx, out_dir)
    else:
        print(f"  ERROR: Unknown generator '{name}'")


# ═══════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Generate V0 Prompt Pack — rendered samples of every prompt type.",
    )
    parser.add_argument(
        "query_id",
        nargs="?",
        default="query_88",
        help="Query ID for non-script prompts (default: query_88)",
    )
    parser.add_argument(
        "--version", "-v",
        default="V0",
        help="Version folder name (default: V0)",
    )
    parser.add_argument(
        "--script", "-s",
        default=None,
        help="Path to SQL script for beam_script prompt (e.g., paper/sql/everyhousehold_deidentified.sql)",
    )
    parser.add_argument(
        "--only",
        default=None,
        choices=list(GENERATORS.keys()),
        help="Generate only this prompt type",
    )
    args = parser.parse_args()

    out_dir = get_output_dir(args.version)
    print(f"Output directory: {out_dir}")
    print(f"Query: {args.query_id}")
    print(f"Version: {args.version}")
    print(f"{'='*70}")

    # Build context (skip if only generating beam_script)
    ctx = None
    if args.only != "beam_script":
        print("\nLoading data...")
        ctx = PromptContext(args.query_id)

    print(f"\n{'='*70}")
    print("Generating prompts...")
    print(f"{'='*70}")

    if args.only:
        run_generator(args.only, ctx, out_dir, args.script)
    else:
        for name in GENERATORS:
            try:
                run_generator(name, ctx, out_dir, args.script)
            except Exception as e:
                print(f"  ERROR generating {name}: {e}")
                import traceback
                traceback.print_exc()

    # Summary
    generated = sorted(out_dir.glob("*.md"))
    print(f"\n{'='*70}")
    print(f"Generated {len(generated)} files in {out_dir}/:")
    for f in generated:
        size = f.stat().st_size
        print(f"  {f.name} ({size:,} bytes)")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()

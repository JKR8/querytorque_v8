"""Generate the Phase 3 expert analyst prompt for Q080 — save to prompt_review, don't call LLM."""
import sys, json
sys.path.insert(0, "packages/qt-shared")
sys.path.insert(0, "packages/qt-sql")

from pathlib import Path
from qt_sql.prompts.analyst_briefing import build_analyst_briefing_prompt
from qt_sql.dag import DagBuilder, CostAnalyzer
from qt_sql.knowledge import TagRecommender
from qt_sql.node_prompter import _load_constraint_files, _load_engine_profile

QUERY_ID = "query080_multi_i1"
BENCHMARK_DIR = Path("packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76")
SQL_PATH = BENCHMARK_DIR / "queries" / f"{QUERY_ID}.sql"

original_sql = SQL_PATH.read_text().strip()

# Parse DAG
builder = DagBuilder(original_sql, dialect="postgres")
dag = builder.build()
cost_analyzer = CostAnalyzer(dag)
costs = cost_analyzer.analyze()
print(f"DAG: {len(dag.nodes)} nodes, {len(dag.edges)} edges")

# Load gold examples for PG
examples_dir = Path("packages/qt-sql/qt_sql/examples/postgres")
available_examples = []
for d in [examples_dir, Path("packages/qt-sql/qt_sql/optimization/examples/postgres")]:
    if not d.exists():
        continue
    for p in sorted(d.glob("*.json")):
        try:
            data = json.loads(p.read_text())
            ex_id = data.get("id", p.stem)
            if any(e["id"] == ex_id for e in available_examples):
                continue
            available_examples.append({
                "id": ex_id,
                "speedup": data.get("verified_speedup", "?"),
                "description": data.get("description", "")[:80],
            })
        except Exception:
            continue
print(f"Available PG examples: {len(available_examples)}")

# Tag-match top 20 (analyst selects per worker)
matched_examples = []
try:
    from qt_sql.pipeline import Pipeline
    pipeline = Pipeline(BENCHMARK_DIR)
    matched_examples = pipeline._find_examples(original_sql, engine="postgres", k=20)
except Exception as e:
    print(f"Tag matching failed: {e}")
print(f"Tag-matched: {len(matched_examples)} examples")

# Load constraints and engine profile
constraints = _load_constraint_files("postgres")
engine_profile = _load_engine_profile("postgres")

# Build analyst prompt (expert mode)
prompt = build_analyst_briefing_prompt(
    query_id=QUERY_ID,
    sql=original_sql,
    explain_plan_text=None,
    dag=dag,
    costs=costs,
    semantic_intents=None,
    global_knowledge=None,
    matched_examples=matched_examples,
    all_available_examples=available_examples,
    constraints=constraints,
    regression_warnings=None,
    dialect="postgres",
    engine_profile=engine_profile,
    mode="expert",
)

# Save
out_path = Path("research/prompt_review/08_expert_analyst_prompt_q080.txt")
out_path.write_text(prompt)
print(f"\nSaved: {out_path}")
print(f"Length: {len(prompt)} chars, ~{len(prompt)//4} tokens")

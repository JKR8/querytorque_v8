import pytest

from qt_sql.optimization.dag_v2 import DagBuilder, CostAnalyzer, DagV2Pipeline
from qt_sql.optimization.plan_analyzer import OptimizationContext, TableScan, JoinInfo
from qt_sql.optimization.adaptive_rewriter_v5 import _format_plan_summary
from qt_sql.optimization.dag_v3 import GoldExample, build_prompt_with_examples


def test_compute_usage_uses_full_output_columns():
    cols = [f"col{i} AS c{i}" for i in range(1, 26)]
    sql = "WITH base AS (SELECT " + ", ".join(cols) + " FROM t1) SELECT c25 FROM base"

    dag = DagBuilder(sql).build()
    base = dag.nodes["base"]

    assert base.usage is not None
    assert "c25" in base.usage.downstream_refs


def test_cost_analyzer_attributes_scan_costs():
    sql = "WITH base AS (SELECT * FROM t1) SELECT * FROM base"
    dag = DagBuilder(sql).build()

    ctx = OptimizationContext(
        table_scans=[
            TableScan(
                table="t1",
                rows_scanned=1000,
                rows_out=1000,
                cost_pct=72.5,
                has_filter=False,
            )
        ]
    )

    costs = CostAnalyzer(dag, ctx).analyze()

    assert costs["base"].cost_pct == pytest.approx(72.5)
    assert "SEQ_SCAN[t1]" in costs["base"].operators


def test_prompt_includes_examples_and_plan_summary():
    ctx = OptimizationContext(
        table_scans=[
            TableScan(
                table="t1",
                rows_scanned=2000,
                rows_out=1500,
                cost_pct=60.0,
                has_filter=True,
                filter_expr="c1 > 10",
            )
        ],
        bottleneck_operators=[
            {"operator": "SEQ_SCAN", "cost_pct": 60.0, "rows": 2000}
        ],
        joins=[
            JoinInfo(
                join_type="HASH_JOIN",
                left_table="t1",
                right_table="t2",
                left_rows=2000,
                right_rows=500,
                output_rows=250,
                cost_pct=20.0,
                is_late=False,
            )
        ],
    )
    plan_summary = _format_plan_summary(ctx)

    example = GoldExample(
        id="ex1",
        name="Test Example",
        description="",
        benchmark_queries=[],
        verified_speedup="2x",
        example={
            "input_slice": "SELECT * FROM t1",
            "output": {"rewrite_sets": []},
            "key_insight": "test",
        },
    )

    base_prompt = "BASE PROMPT"
    full_prompt = build_prompt_with_examples(base_prompt, [example], plan_summary)

    assert "## Example: Test Example (EX1)" in full_prompt
    assert "## Execution Plan" in full_prompt
    assert "Operators by cost:" in full_prompt
    assert "Scans:" in full_prompt
    assert "Joins:" in full_prompt
    assert base_prompt in full_prompt


def test_dag_v2_prompt_uses_plan_context_costs():
    sql = "WITH base AS (SELECT * FROM t1) SELECT * FROM base"

    ctx = OptimizationContext(
        table_scans=[
            TableScan(
                table="t1",
                rows_scanned=1000,
                rows_out=1000,
                cost_pct=72.5,
                has_filter=False,
            )
        ]
    )

    prompt = DagV2Pipeline(sql, plan_context=ctx).get_prompt()

    assert "Cost Attribution" in prompt
    assert "SEQ_SCAN[t1]" in prompt
    assert "72.5% cost" in prompt

from __future__ import annotations

from pathlib import Path
import json
from types import SimpleNamespace

from qt_sql.patches.beam_router import assign_importance_stars
from qt_sql.patches.beam_prompts import (
    ProbeSpec,
    build_beam_editor_strike_prompt,
    build_beam_analyst_prompt,
    build_beam_worker_retry_prompt,
    build_beam_worker_prompt,
    parse_analyst_response,
)
from qt_sql.patches.beam_prompt_builder import (
    append_shot_results,
    build_beam_compiler_prompt,
)
from qt_sql.patches.pathology_classifier import build_intelligence_brief


PROMPTS_ROOT = Path(__file__).resolve().parent.parent / "qt_sql" / "prompts"
TEMPLATE_DIR = PROMPTS_ROOT / "templates" / "V3"
SAMPLES_DIR = PROMPTS_ROOT / "samples" / "V3"
VERSIONS_TEMPLATE_DIR = PROMPTS_ROOT / "versions" / "V3" / "templates"
VERSIONS_EXAMPLES_DIR = PROMPTS_ROOT / "versions" / "V3" / "examples"
GOLD_EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "qt_sql" / "examples"


def test_assign_importance_stars_uses_80_10_10_workload_split() -> None:
    baselines = {
        "q1": 80.0,
        "q2": 10.0,
        "q3": 10.0,
    }
    stars = assign_importance_stars(baselines)
    assert stars["q1"] == 3
    assert stars["q2"] == 2
    assert stars["q3"] == 1


def test_parse_analyst_response_supports_dispatch_wrapper_and_probe_count() -> None:
    response = """
    {
      "dispatch": {
        "hypothesis": "Nested loop hotspot",
        "probe_count": 1,
        "equivalence_tier": "unordered",
        "reasoning_trace": ["Cost spine dominated by nested loop"],
        "do_not_do": ["avoid_or_to_union"]
      },
      "probes": [
        {
          "probe_id": "p01",
          "transform_id": "decorrelate",
          "family": "B",
          "target": "rewrite correlated subquery",
          "confidence": 0.9,
          "expected_explain_delta": "Nested loop removed",
          "recommended_patch_ops": ["insert_cte", "replace_from"]
        },
        {
          "probe_id": "p02",
          "transform_id": "early_filter",
          "family": "A",
          "target": "push filter",
          "confidence": 0.7
        }
      ],
      "dropped": []
    }
    """
    result = parse_analyst_response(response)
    assert result is not None
    assert result.hypothesis == "Nested loop hotspot"
    assert len(result.probes) == 1
    assert result.probes[0].probe_id == "p01"
    assert result.probes[0].expected_explain_delta == "Nested loop removed"
    assert result.probes[0].recommended_patch_ops == ["insert_cte", "replace_from"]
    assert result.equivalence_tier == "unordered"
    assert result.reasoning_trace == ["Cost spine dominated by nested loop"]
    assert result.do_not_do == ["avoid_or_to_union"]


def test_dispatcher_prompt_includes_v3_dynamic_sections() -> None:
    prompt = build_beam_analyst_prompt(
        query_id="query_001",
        original_sql="SELECT 1",
        explain_text="SEQ_SCAN t [100ms]",
        ir_node_map="S0: SELECT 1",
        dialect="postgres",
        importance_stars=3,
        budget_hint="high-priority",
        schema_context="| t | 1000 | id | idx_t_id |",
        engine_knowledge="## Dialect Profile (POSTGRES)\n- sample",
    )
    assert "## Cache Boundary" in prompt
    assert "## Runtime Dialect Contract" in prompt
    assert "target_dialect: postgres" in prompt
    assert "## Query Importance" in prompt
    assert "importance_stars: 3" in prompt
    assert "## Transform Catalog (full list; not pre-filtered)" in prompt
    assert "selection_policy: prioritize native/universal transforms first." in prompt
    assert "support `portability_candidate`" in prompt
    assert "## Schema / Index / Stats Context" in prompt
    assert "## Engine-Specific Knowledge" in prompt


def test_sniper_prompt_contains_bda_table_and_full_sql_patches() -> None:
    sql_patch = "SELECT 1 AS a\nUNION ALL\nSELECT 2 AS a"
    prompt = build_beam_compiler_prompt(
        query_id="query_001",
        original_sql="SELECT a FROM t",
        explain_text="SEQ_SCAN t [200ms]",
        ir_node_map="S0: SELECT a FROM t",
        all_5_examples={},
        dialect="postgres",
        importance_stars=2,
        strike_results=[
            {
                "probe_id": "p01",
                "transform_id": "early_filter",
                "family": "A",
                "status": "WIN",
                "speedup": 1.4,
                "error": "",
                "explain_text": "HASH_JOIN [120ms]\nSEQ_SCAN x [90ms]",
                "sql": sql_patch,
                "description": "Push selective filter into source CTE",
            }
        ],
        schema_context="| t | 1000 | id | idx_t_id |",
        dispatch_hypothesis="Nested loop dominates due to late filtering",
        dispatch_reasoning_trace=["Primary hotspot on NL anti join"],
        equivalence_tier="unordered",
        engine_knowledge="## Dialect Profile (POSTGRES)\n- sample",
    )
    assert "## BDA Table (all probes)" in prompt
    assert "## Runtime Dialect Contract" in prompt
    assert "## Engine-Specific Knowledge" in prompt
    assert "## Worker SQL Patches" in prompt
    assert sql_patch in prompt
    assert "## Schema / Index / Stats Context" in prompt
    assert "## Analyst Hypothesis" in prompt
    assert "## Analyst Reasoning Trace" in prompt
    assert "## Equivalence Tier" in prompt


def test_worker_prompt_contains_transform_recipe_section() -> None:
    probe = ProbeSpec(
        probe_id="p01",
        transform_id="decorrelate",
        family="B",
        target="Rewrite correlated aggregate into CTE+JOIN",
        confidence=0.9,
        expected_explain_delta="Nested loop removed",
        recommended_patch_ops=["insert_cte", "replace_from"],
    )
    prompt = build_beam_worker_prompt(
        original_sql="SELECT * FROM t WHERE x IN (SELECT y FROM u)",
        ir_node_map="S0: SELECT ...",
        current_tree_map="node: final_select\n  parent_node_id: null\n  sources: []\n  outputs: [x]\nroot_node_id: final_select",
        hypothesis="Nested loop decorrelation",
        probe=probe,
        dialect="postgres",
        equivalence_tier="unordered",
        reasoning_trace=["Primary hotspot: nested loop"],
        engine_knowledge="## Dialect Profile (POSTGRES)\n- sample",
        do_not_do=["avoid_or_to_union"],
    )
    assert "### Transform Recipe" in prompt
    assert "## Runtime Dialect Contract" in prompt
    assert "`transform_id`: `decorrelate`" in prompt
    assert "recommended_patch_ops" in prompt
    assert "expected_explain_delta: Nested loop removed" in prompt
    assert "equivalence_tier: unordered" in prompt
    assert "### Analyst Reasoning Trace" in prompt
    assert "### Analyst Do-Not-Do" in prompt
    assert "avoid_or_to_union" in prompt
    assert "existing_ctes" in prompt
    assert "### Engine-Specific Knowledge" in prompt
    assert "### Current TREE Node Map" in prompt
    assert "node: final_select" in prompt
    assert "## Tree Output Contract (MUST follow)" in prompt
    assert "\"verification\"" in prompt
    assert "one or more changed nodes are allowed" in prompt


def test_worker_prompt_always_uses_worker_v3_template() -> None:
    probe = ProbeSpec(
        probe_id="p11",
        transform_id="decorrelate",
        family="B",
        target="Rewrite correlated aggregate into set form",
        confidence=0.9,
    )
    prompt = build_beam_worker_prompt(
        original_sql="SELECT * FROM t",
        ir_node_map="S0: SELECT * FROM t",
        hypothesis="Nested loop correlation hotspot",
        probe=probe,
        dialect="duckdb",
        worker_lane="scout",
    )
    assert "Senior SQL Rewrite Engineer" in prompt
    assert "## Tree Output Contract (MUST follow)" in prompt
    assert "- worker_lane: scout" in prompt


def test_worker_output_schema_includes_status_and_failure_fields() -> None:
    text = (TEMPLATE_DIR / "beam_worker_v3.txt").read_text(encoding="utf-8")
    assert "| `status` | string | yes |" in text
    assert "| `failure_reason` | string | conditional |" in text
    assert "| `partial_work` | object | conditional |" in text
    assert "Partial-work schema" in text
    assert "attempted_approach" in text
    assert "blocking_issue" in text
    assert "hypothesis_still_valid" in text


def test_worker_retry_prompt_includes_structured_gate_failure_feedback() -> None:
    retry_prompt = build_beam_worker_retry_prompt(
        "BASE PROMPT",
        probe_id="p07",
        transform_id="decorrelate",
        gate_name="tier1_structural",
        gate_error="Tier-1: missing alias x in FROM",
        failed_sql="SELECT * FROM t",
        previous_response='{"steps":[]}',
    )
    assert "## RETRY — Gate failure feedback" in retry_prompt
    assert '"gate": "tier1_structural"' in retry_prompt
    assert "Tier-1: missing alias x in FROM" in retry_prompt
    assert "Output ONLY valid TREE JSON." in retry_prompt


def test_editor_strike_prompt_injects_selected_transform() -> None:
    prompt = build_beam_editor_strike_prompt(
        query_id="query_001",
        original_sql="SELECT * FROM t",
        explain_text="SEQ_SCAN t [100ms]",
        ir_node_map="S0: SELECT * FROM t",
        transform_id="decorrelate",
        dialect="postgres",
        schema_context="| t | 1000 | id | idx_t_id |",
    )
    assert "mode: editor_strike" in prompt
    assert "transform_id: decorrelate" in prompt
    assert "`transform_id`: `decorrelate`" in prompt
    assert "## Schema / Index / Stats Context" in prompt
    assert "Editor strike is a fast, single-call pathway" in prompt
    assert "Worked Strike Example" in prompt


def test_compiler_template_matches_runtime_evidence_contract() -> None:
    text = (TEMPLATE_DIR / "beam_compiler_v3.txt").read_text(encoding="utf-8")
    assert "Principal SQL Optimization Reviewer" in text
    assert "Tree Output Contract (MUST follow)" in text
    assert "top-level value may be:" in text
    assert "changed nodes MUST include full executable SQL in `sql`" in text
    assert "## Terminology (normative)" in text
    assert "## Input Contract" in text
    assert "## Decision Priority Ladder" in text
    assert "## Distinct Pathway Decision Matrix" in text
    assert "Per-attempt schema:" in text
    assert "## Worked Valid Example (two-attempt array)" in text
    assert "## Worked Invalid Example (do not produce)" in text
    assert "## Safe No-Change Fallback (required capability)" in text
    assert "PatchPlan" not in text


def test_analyst_template_has_strict_schema_and_invalid_boundary_examples() -> None:
    text = (TEMPLATE_DIR / "beam_analyst_v3.txt").read_text(encoding="utf-8")
    assert "## Terminology (normative)" in text
    assert "## Input Contract" in text
    assert "## Decision Priority Ladder" in text
    assert "## Probe-count Policy" in text
    assert "## Gold Example Routing Policy" in text
    assert "set `gold_example_id` to the best-fit gold id" in text
    assert "Diversity rule for non-gold probes" in text
    assert "stars=2" in text  # probe-count policy references star levels
    assert "Top-level schema:" in text
    assert "Dispatch schema:" in text
    assert "Probe item schema:" in text
    assert "Dropped item schema:" in text
    assert "## Worked Invalid Example (do not produce)" in text
    assert "Corrective action:" in text


def test_worker_templates_have_strict_schema_and_invalid_boundary_examples() -> None:
    worker_text = (TEMPLATE_DIR / "beam_worker_v3.txt").read_text(encoding="utf-8")

    assert "## Terminology (normative)" in worker_text
    assert "## Input Contract" in worker_text
    assert "## Decision Priority Ladder" in worker_text
    assert "Top-level schema:" in worker_text
    assert "Verification schema:" in worker_text
    assert "Tree schema:" in worker_text
    assert "Node schema:" in worker_text
    assert "## Worked Invalid Example (do not produce)" in worker_text
    assert "Corrective action:" in worker_text
    assert "## Worker Procedure (reasoning checklist)" in worker_text
    assert "## Worked Failure Example" in worker_text


def test_v3_templates_avoid_sql_ellipsis_placeholders() -> None:
    for name in (
        "beam_analyst_v3.txt",
        "beam_worker_v3.txt",
        "beam_compiler_v3.txt",
    ):
        text = (TEMPLATE_DIR / name).read_text(encoding="utf-8")
        assert "SELECT ..." not in text
        assert "..." not in text


def test_append_shot_results_for_compiler_allows_one_or_two_plans() -> None:
    base_prompt = "You are a Principal SQL Optimization Reviewer.\nYour task: produce one or two optimization attempts."
    patches = [
        SimpleNamespace(
            patch_id="s1",
            family="A",
            transform="early_filter",
            speedup=1.2,
            status="WIN",
            apply_error="",
        )
    ]
    shot2 = append_shot_results(base_prompt=base_prompt, patches=patches, explains={})
    assert "Output policy:" in shot2
    assert "Output one tree object, or a JSON array with one to four tree objects." in shot2


def test_intelligence_brief_labels_portability_candidates() -> None:
    detected = [
        SimpleNamespace(
            id="channel_bitmap_aggregation",
            overlap_ratio=0.78,
            matched_features=["AGG_COUNT", "TABLE_REPEAT_8+"],
            missing_features=[],
            gap="REDUNDANT_SCAN_ELIMINATION",
            contraindications=[],
            engines=["duckdb"],
        )
    ]
    brief = build_intelligence_brief(
        detected_transforms=detected,
        classification=None,
        runtime_dialect="postgres",
    )
    assert "SUPPORT: portability_candidate" in brief


def test_v3_prompt_files_are_synced_across_template_sample_and_versions() -> None:
    files = (
        "beam_analyst_v3.txt",
        "beam_worker_v3.txt",
        "beam_compiler_v3.txt",
        "beam_strike_worker_v1.txt",
    )
    for name in files:
        template_text = (TEMPLATE_DIR / name).read_text(encoding="utf-8")
        sample_text = (SAMPLES_DIR / name).read_text(encoding="utf-8")
        versions_template_text = (VERSIONS_TEMPLATE_DIR / name).read_text(
            encoding="utf-8"
        )
        versions_example_text = (VERSIONS_EXAMPLES_DIR / name).read_text(
            encoding="utf-8"
        )
        assert sample_text == template_text
        assert versions_template_text == template_text
        assert versions_example_text == template_text


def test_gold_examples_use_tree_example_not_patch_plan() -> None:
    for path in sorted(GOLD_EXAMPLES_DIR.rglob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            continue
        if "optimized_sql" not in data:
            continue
        assert "tree_example" in data
        assert "patch_plan" not in data

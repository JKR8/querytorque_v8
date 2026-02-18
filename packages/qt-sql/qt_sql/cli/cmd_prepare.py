"""qt prepare — generate beam-runtime analyst prompts + context (no LLM calls)."""

from __future__ import annotations

import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import click


def _parse_speedup_value(value: Any) -> Optional[float]:
    """Parse numeric speedup from values like '2.13x'."""
    if isinstance(value, (int, float)):
        val = float(value)
        return val if val > 0 else None
    if value is None:
        return None
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)", str(value))
    if not m:
        return None
    try:
        parsed = float(m.group(1))
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _load_cached_classification(bench_dir: Path, query_id: str):
    """Load optional pre-computed pathology classification."""
    path = bench_dir / "classifications.json"
    if not path.exists():
        return None
    try:
        from ..patches.pathology_classifier import (
            ClassificationResult,
            PathologyMatch,
        )

        data = json.loads(path.read_text())
        entry = data.get(query_id)
        if not isinstance(entry, dict):
            return None
        matches = [
            PathologyMatch(
                pathology_id=str(m.get("pathology_id", "?")),
                name=str(m.get("name", "")),
                confidence=float(m.get("confidence", 0.0)),
                evidence=str(m.get("evidence", "")),
                recommended_transform=str(m.get("transform", "")),
            )
            for m in (entry.get("llm_matches") or [])
            if isinstance(m, dict)
        ]
        return ClassificationResult(
            query_id=query_id,
            matches=matches,
            reasoning=str(entry.get("reasoning", "")),
        )
    except Exception:
        return None


def _build_sample_probe(
    detected_transforms: list[Any],
    transform_by_id: Dict[str, Dict[str, Any]],
    dialect: str,
):
    """Build one realistic sample probe for prepare-time worker/compiler prompts."""
    from ..patches.beam_prompts import ProbeSpec, _load_gold_example_for_family

    detected = detected_transforms or []
    chosen = next((m for m in detected if getattr(m, "overlap_ratio", 0.0) >= 0.30), None)
    if chosen is None and detected:
        chosen = detected[0]

    transform_id = str(getattr(chosen, "id", "") or "date_cte_isolate")
    tmeta = transform_by_id.get(transform_id, {})
    family = str(tmeta.get("family", "A"))
    principle = str(tmeta.get("principle") or "Apply a targeted single-transform rewrite")
    overlap = float(getattr(chosen, "overlap_ratio", 0.5) if chosen else 0.5)
    confidence = max(0.4, min(0.9, overlap))

    gates_checked = []
    if chosen is not None:
        gates_checked.append(f"feature_overlap={overlap:.0%}")
        matched = list(getattr(chosen, "matched_features", []) or [])
        if matched:
            gates_checked.append("matched_features=" + ", ".join(str(x) for x in matched[:4]))
        missing = list(getattr(chosen, "missing_features", []) or [])
        if missing:
            gates_checked.append("missing_features=" + ", ".join(str(x) for x in missing[:3]))
    else:
        gates_checked.append("no_detected_transform; using fallback seed")

    gold_example = _load_gold_example_for_family(family, dialect) if family else None
    rec_examples = []
    if isinstance(gold_example, dict) and gold_example.get("id"):
        rec_examples.append(str(gold_example["id"]))

    probe = ProbeSpec(
        probe_id="sample_p01",
        transform_id=transform_id,
        family=family,
        target=principle,
        confidence=confidence,
        gold_example_id=str(gold_example.get("id")) if isinstance(gold_example, dict) else None,
        recommended_examples=rec_examples,
        node_contract={
            "from": ["final_select"],
            "where": "dominant runtime hotspot",
            "output_preservation": "exact rowset and aggregates",
        },
        gates_checked=gates_checked,
        expected_explain_delta=f"Lower dominant operator cost via {transform_id}.",
        recommended_patch_ops=[],
        phase=1,
        exploration=False,
        exploration_hypothesis="",
    )
    return probe, gold_example, tmeta


@click.command()
@click.argument("benchmark")
@click.option("-q", "--query", multiple=True, help="Query filter (repeatable, prefix match).")
@click.option("--mode", type=click.Choice(["beam"]), default="beam", hidden=True,
              help="Deprecated. Prepare always uses beam prompt format.")
@click.option("--force", is_flag=True, help="Regenerate even if prompts exist.")
@click.option("--bootstrap", is_flag=True,
              help="Allow first-run mode: skip intelligence gates (no gold examples/global knowledge required).")
@click.option("-o", "--output-dir", type=click.Path(), default=None,
              help="Custom output directory (default: benchmark/prepared/<timestamp>).")
@click.option("--scenario", default="",
              help="Scenario card name (e.g., 'postgres_small_instance').")
@click.option("--evidence", is_flag=True,
              help="Include evidence bundle in prepared output.")
@click.option("--patch", "patch_mode", is_flag=True,
              help="Include IR node map in prepared output for patch-mode workers.")
@click.pass_context
def prepare(
    ctx: click.Context,
    benchmark: str,
    query: tuple,
    mode: str,
    force: bool,
    bootstrap: bool,
    output_dir: str | None,
    scenario: str,
    evidence: bool,
    patch_mode: bool,
) -> None:
    """Generate beam analyst prompts deterministically (no LLM calls).

    Runs Phases 1-3: parse logical tree, gather analyst context, build runtime prompt.
    Output is saved under benchmark/prepared/<timestamp>/.
    """
    from ._common import (
        console,
        resolve_benchmark,
        load_benchmark_config,
        parse_query_filter,
        print_header,
        print_error,
        print_success,
        dialect_from_engine,
    )

    bench_dir = resolve_benchmark(benchmark)
    cfg = load_benchmark_config(bench_dir)
    engine = cfg["engine"]
    dialect = dialect_from_engine(engine)
    query_ids = parse_query_filter(query, bench_dir)

    if not query_ids:
        print_error("No queries found.")
        raise SystemExit(1)

    print_header(f"Preparing {len(query_ids)} queries [{bench_dir.name}] mode={mode}")

    # Output directory
    if output_dir:
        out = Path(output_dir)
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = bench_dir / "prepared" / ts
    out.mkdir(parents=True, exist_ok=True)
    (out / "prompts").mkdir(exist_ok=True)
    (out / "context").mkdir(exist_ok=True)
    (out / "metadata").mkdir(exist_ok=True)
    (out / "original").mkdir(exist_ok=True)

    # Lazy imports — keep `qt --help` fast
    from ..pipeline import Pipeline
    from ..patches.beam_prompt_builder import (
        load_gold_examples,
        build_beam_compiler_prompt,
        _load_engine_intelligence,
    )
    from ..patches.beam_prompts import (
        build_base_tree_prompt,
        build_beam_analyst_prompt,
        build_beam_worker_prompt,
    )
    from ..detection import detect_transforms, load_transforms
    from ..patches.pathology_classifier import build_intelligence_brief

    if bootstrap:
        import os
        os.environ["QT_ALLOW_INTELLIGENCE_BOOTSTRAP"] = "1"

    pipeline = Pipeline(bench_dir)
    # Prepare is the offline prep step — collect EXPLAINs for missing cache entries.
    pipeline.config.explain_policy = "explain"

    # Load scenario card if specified
    scenario_card = None
    if scenario:
        from ..scenario_cards import load_scenario_card
        scenario_card = load_scenario_card(scenario)
        if scenario_card:
            console.print(f"  Scenario card: {scenario}")
        else:
            console.print(f"  [yellow]Warning: scenario '{scenario}' not found[/yellow]")

    results = []
    errors = []
    t0 = time.time()

    for i, qid in enumerate(query_ids, 1):
        sql_path = bench_dir / "queries" / f"{qid}.sql"
        if not sql_path.exists():
            errors.append((qid, f"SQL file not found: {sql_path}"))
            continue

        sql = sql_path.read_text().strip()
        status_prefix = f"[{i}/{len(query_ids)}] {qid}"

        try:
            # Phase 1: Parse logical tree (caches EXPLAIN if missing)
            dag, costs, explain_result = pipeline._parse_logical_tree(
                sql, dialect=dialect, query_id=qid,
            )

            # Phase 2-3: Gather context + build prompt
            ctx_data = pipeline.gather_analyst_context(
                query_id=qid, sql=sql, dialect=dialect, engine=engine,
            )

            # Build IR node map for runtime beam analyst prompt contract.
            from ..ir import build_script_ir, render_ir_node_map, Dialect
            dialect_map = {
                "duckdb": Dialect.DUCKDB,
                "postgres": Dialect.POSTGRES,
                "snowflake": Dialect.SNOWFLAKE,
            }
            ir_dialect = dialect_map.get(dialect, Dialect.DUCKDB)
            script_ir = build_script_ir(sql, ir_dialect)
            ir_node_map_text = render_ir_node_map(script_ir)
            base_tree_prompt = build_base_tree_prompt(sql, dialect=dialect)

            # Keep prepare aligned with beam runtime templates.
            gold_examples = load_gold_examples(dialect)
            engine_knowledge = _load_engine_intelligence(dialect) or ""
            explain_text = ctx_data.get("explain_plan_text") or ""
            qerror_analysis = ctx_data.get("qerror_analysis")

            # Runtime-aligned intelligence brief (AST detection + cached classification).
            transforms_catalog = load_transforms()
            transform_by_id = {
                str(t.get("id")): t for t in transforms_catalog
                if isinstance(t, dict) and t.get("id")
            }
            detected_transforms = detect_transforms(
                sql, transforms_catalog, dialect=dialect,
            )
            classification = _load_cached_classification(bench_dir, qid)
            intelligence_brief = build_intelligence_brief(
                detected_transforms,
                classification,
                runtime_dialect=dialect,
            ) or (ctx_data.get("plan_scanner_text") or "")

            prompt = build_beam_analyst_prompt(
                query_id=qid,
                original_sql=sql,
                explain_text=explain_text,
                ir_node_map=ir_node_map_text,
                current_tree_map=base_tree_prompt,
                gold_examples=gold_examples,
                dialect=dialect,
                intelligence_brief=intelligence_brief,
                importance_stars=2,
                schema_context="",
                engine_knowledge=engine_knowledge,
                qerror_analysis=qerror_analysis,
            )

            # Emit one realistic worker+compiler sample prompt per query so
            # prepare output can validate all runtime prompt render paths.
            sample_probe, sample_gold_example, sample_tmeta = _build_sample_probe(
                detected_transforms, transform_by_id, dialect,
            )
            sample_hypothesis = (
                "Primary hotspot likely responds to a single-transform rewrite "
                f"({sample_probe.transform_id}) based on detected features."
            )
            sample_do_not_do = [
                "do not change projection semantics",
                "do not alter grouping cardinality",
                "do not introduce unfiltered wide CTE materialization",
            ]
            sample_gold_tree = None
            if isinstance(sample_gold_example, dict):
                sample_gold_tree = sample_gold_example.get("tree_example")

            worker_qwen_prompt = build_beam_worker_prompt(
                original_sql=sql,
                ir_node_map=ir_node_map_text,
                current_tree_map=base_tree_prompt,
                hypothesis=sample_hypothesis,
                probe=sample_probe,
                gold_tree_example=sample_gold_tree,
                dialect=dialect,
                schema_context="",
                equivalence_tier="exact",
                reasoning_trace=[],
                qerror_analysis=qerror_analysis,
                engine_knowledge=engine_knowledge,
                do_not_do=sample_do_not_do,
                worker_lane="qwen",
            )
            worker_reasoner_prompt = build_beam_worker_prompt(
                original_sql=sql,
                ir_node_map=ir_node_map_text,
                current_tree_map=base_tree_prompt,
                hypothesis=sample_hypothesis,
                probe=sample_probe,
                gold_tree_example=sample_gold_tree,
                dialect=dialect,
                schema_context="",
                equivalence_tier="exact",
                reasoning_trace=[],
                qerror_analysis=qerror_analysis,
                engine_knowledge=engine_knowledge,
                do_not_do=sample_do_not_do,
                worker_lane="reasoner",
            )

            sample_sql = ""
            sample_speedup = None
            if isinstance(sample_gold_example, dict):
                sample_sql = str(sample_gold_example.get("optimized_sql") or "").strip()
                sample_speedup = _parse_speedup_value(sample_gold_example.get("verified_speedup"))
            if not sample_sql:
                sample_sql = sql

            sample_strike_results = [
                {
                    "probe_id": sample_probe.probe_id,
                    "transform_id": sample_probe.transform_id,
                    "family": sample_probe.family,
                    "status": "WIN" if sample_speedup and sample_speedup >= 1.05 else "PASS",
                    "failure_category": "none",
                    "speedup": sample_speedup,
                    "error": "",
                    "explain_text": explain_text,
                    "sql": sample_sql,
                    "description": str(sample_tmeta.get("principle") or sample_probe.target),
                }
            ]
            compiler_sample_prompt = build_beam_compiler_prompt(
                query_id=qid,
                original_sql=sql,
                explain_text=explain_text,
                ir_node_map=ir_node_map_text,
                all_5_examples=gold_examples,
                dialect=dialect,
                strike_results=sample_strike_results,
                intelligence_brief=intelligence_brief,
                importance_stars=2,
                current_tree_map=base_tree_prompt,
                schema_context="",
                engine_knowledge=engine_knowledge,
                dispatch_hypothesis=sample_hypothesis,
                dispatch_reasoning_trace=[
                    f"{sample_probe.transform_id} selected as top detected sample transform.",
                ],
                equivalence_tier="exact",
                qerror_analysis=qerror_analysis,
            )

            # Keep optional IR artifact behavior for downstream consumers.
            if patch_mode:
                (out / "context" / f"{qid}_ir_node_map.txt").write_text(ir_node_map_text)
            # Save outputs
            (out / "prompts" / f"{qid}.txt").write_text(prompt)
            (out / "prompts" / f"{qid}_worker_qwen_sample.txt").write_text(worker_qwen_prompt)
            (out / "prompts" / f"{qid}_worker_reasoner_sample.txt").write_text(worker_reasoner_prompt)
            (out / "prompts" / f"{qid}_compiler_sample.txt").write_text(compiler_sample_prompt)
            (out / "original" / f"{qid}.sql").write_text(sql)

            # Evidence bundle (optional)
            if evidence:
                from ..evidence import extract_evidence_bundle, render_evidence_for_prompt
                # explain_result is the wrapper dict; extract nested plan_json
                plan_json = explain_result.get("plan_json") if explain_result else None
                bundle = extract_evidence_bundle(
                    query_id=qid,
                    query_sql=sql,
                    explain_text=ctx_data.get("explain_plan_text"),
                    plan_json=plan_json,
                    qerror_analysis=ctx_data.get("qerror_analysis"),
                    resource_envelope=ctx_data.get("resource_envelope"),
                    dialect=dialect,
                )
                evidence_text = render_evidence_for_prompt(bundle)
                (out / "context" / f"{qid}_evidence.txt").write_text(evidence_text)

            # Scenario card (optional)
            if scenario_card:
                from ..scenario_cards import render_scenario_for_prompt
                scenario_text = render_scenario_for_prompt(scenario_card)
                (out / "context" / f"{qid}_scenario.txt").write_text(scenario_text)

            # Context JSON (non-serializable items removed)
            ctx_serializable = {}
            for k, v in ctx_data.items():
                try:
                    json.dumps(v)
                    ctx_serializable[k] = v
                except (TypeError, ValueError):
                    ctx_serializable[k] = str(v)
            (out / "context" / f"{qid}.json").write_text(
                json.dumps(ctx_serializable, indent=2)
            )

            # Metadata
            meta = {
                "query_id": qid,
                "mode": mode,
                "engine": engine,
                "dialect": dialect,
                "prompt_tokens": len(prompt.split()),
                "worker_qwen_sample_tokens": len(worker_qwen_prompt.split()),
                "worker_reasoner_sample_tokens": len(worker_reasoner_prompt.split()),
                "compiler_sample_tokens": len(compiler_sample_prompt.split()),
                "has_explain": ctx_data.get("explain_plan_text") is not None,
                "has_plan_scanner": ctx_data.get("plan_scanner_text") is not None,
                "has_worker_samples": True,
                "has_compiler_sample": True,
                "n_matched_examples": len(ctx_data.get("matched_examples", [])),
                "n_constraints": len(ctx_data.get("constraints", [])),
                "query_archetype": ctx_data.get("query_archetype"),
                "scenario": scenario or None,
                "has_evidence": evidence,
                "patch_mode": patch_mode,
                "has_ir_node_map": ir_node_map_text is not None,
            }
            (out / "metadata" / f"{qid}.json").write_text(
                json.dumps(meta, indent=2)
            )

            results.append(meta)
            console.print(f"  {status_prefix} [green]OK[/green]  ~{meta['prompt_tokens']} tokens")

        except Exception as e:
            errors.append((qid, str(e)))
            console.print(f"  {status_prefix} [red]ERROR[/red] {e}")

    elapsed = time.time() - t0

    # Summary
    summary = {
        "benchmark": bench_dir.name,
        "mode": mode,
        "total_queries": len(query_ids),
        "prepared": len(results),
        "errors": len(errors),
        "elapsed_seconds": round(elapsed, 1),
        "output_dir": str(out),
        "error_details": [{"query_id": qid, "error": msg} for qid, msg in errors],
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2))

    console.print()
    print_success(
        f"Prepared {len(results)}/{len(query_ids)} queries in {elapsed:.1f}s → {out}"
    )
    if errors:
        print_error(f"{len(errors)} errors (see summary.json)")
        raise SystemExit(2)

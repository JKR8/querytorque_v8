"""qt prepare — generate analyst prompts + context (no LLM calls)."""

from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path

import click


@click.command()
@click.argument("benchmark")
@click.option("-q", "--query", multiple=True, help="Query filter (repeatable, prefix match).")
@click.option(
    "--mode",
    type=click.Choice(["swarm", "oneshot"]),
    default="swarm",
    show_default=True,
    help="Prompt mode: swarm (multi-worker) or oneshot (single call).",
)
@click.option("--force", is_flag=True, help="Regenerate even if prompts exist.")
@click.option("--bootstrap", is_flag=True,
              help="Allow first-run mode: skip intelligence gates (no gold examples/global knowledge required).")
@click.option("-o", "--output-dir", type=click.Path(), default=None,
              help="Custom output directory (default: benchmark/prepared/<timestamp>).")
@click.option("--scenario", default="",
              help="Scenario card name (e.g., 'postgres_small_instance').")
@click.option("--evidence", is_flag=True,
              help="Include evidence bundle in prepared output.")
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
) -> None:
    """Generate analyst briefing prompts deterministically (no LLM calls).

    Runs Phases 1-3: parse logical tree, gather analyst context, build prompt.
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
    from ..prompts import build_analyst_briefing_prompt

    if bootstrap:
        import os
        os.environ["QT_ALLOW_INTELLIGENCE_BOOTSTRAP"] = "1"

    pipeline = Pipeline(bench_dir)

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

            prompt = build_analyst_briefing_prompt(
                query_id=qid,
                sql=sql,
                explain_plan_text=ctx_data.get("explain_plan_text"),
                dag=dag,
                costs=costs,
                semantic_intents=ctx_data.get("semantic_intents"),
                constraints=ctx_data.get("constraints", []),
                dialect=dialect,
                engine_profile=ctx_data.get("engine_profile"),
                resource_envelope=ctx_data.get("resource_envelope"),
                exploit_algorithm_text=ctx_data.get("exploit_algorithm_text"),
                plan_scanner_text=ctx_data.get("plan_scanner_text"),
                mode=mode,
                detected_transforms=ctx_data.get("detected_transforms"),
                qerror_analysis=ctx_data.get("qerror_analysis"),
                matched_examples=ctx_data.get("matched_examples"),
            )

            # Save outputs
            (out / "prompts" / f"{qid}.txt").write_text(prompt)
            (out / "original" / f"{qid}.sql").write_text(sql)

            # Evidence bundle (optional)
            if evidence:
                from ..evidence import extract_evidence_bundle, render_evidence_for_prompt
                bundle = extract_evidence_bundle(
                    query_id=qid,
                    query_sql=sql,
                    explain_result=explain_result,
                    dag=dag,
                    costs=costs,
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
                "has_explain": ctx_data.get("explain_plan_text") is not None,
                "has_plan_scanner": ctx_data.get("plan_scanner_text") is not None,
                "n_matched_examples": len(ctx_data.get("matched_examples", [])),
                "n_constraints": len(ctx_data.get("constraints", [])),
                "query_archetype": ctx_data.get("query_archetype"),
                "scenario": scenario or None,
                "has_evidence": evidence,
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

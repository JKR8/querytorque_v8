"""qt classify — batch pathology classification for benchmark queries."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import click


@click.command()
@click.argument("benchmark")
@click.option("-q", "--query", multiple=True, help="Query filter (repeatable, prefix match).")
@click.option("--model", default=None, help="LLM model override for classification.")
@click.option("--force", is_flag=True, help="Re-classify even if classifications.json exists.")
@click.pass_context
def classify(
    ctx: click.Context,
    benchmark: str,
    query: tuple,
    model: str | None,
    force: bool,
) -> None:
    """Batch classify queries against known pathologies.

    Runs AST detection + LLM classification for each query and saves
    results to benchmark/classifications.json. Fleet and oneshot modes
    load these at runtime to guide the analyst.
    """
    from ._common import (
        console,
        resolve_benchmark,
        load_benchmark_config,
        parse_query_filter,
        dialect_from_engine,
        print_header,
        print_error,
        print_success,
    )
    from ..detection import detect_transforms, load_transforms
    from ..patches.pathology_classifier import (
        PathologyClassifier,
        ClassificationResult,
    )

    bench_dir = resolve_benchmark(benchmark)
    cfg = load_benchmark_config(bench_dir)
    engine = cfg["engine"]
    dialect = dialect_from_engine(engine)
    query_ids = parse_query_filter(query, bench_dir)

    if not query_ids:
        print_error("No queries found.")
        raise SystemExit(1)

    out_path = bench_dir / "classifications.json"

    # Load existing classifications (merge mode)
    existing: dict = {}
    if out_path.exists() and not force:
        existing = json.loads(out_path.read_text())

    # Filter to unclassified queries
    if existing and not force:
        remaining = [q for q in query_ids if q not in existing]
        if not remaining:
            console.print(
                f"All {len(query_ids)} queries already classified. "
                f"Use --force to re-classify."
            )
            return
        console.print(
            f"Skipping {len(query_ids) - len(remaining)} already classified. "
            f"Classifying {len(remaining)} remaining."
        )
        query_ids = remaining

    print_header(
        f"Classifying {len(query_ids)} queries [{bench_dir.name}] dialect={dialect}"
    )

    # Load transforms catalog
    transforms_catalog = load_transforms()

    # Build LLM classify function
    classifier = None
    try:
        from qt_shared.config import get_settings

        settings = get_settings()
        from ..generate import CandidateGenerator

        gen = CandidateGenerator(
            provider=settings.llm_provider,
            model=model or settings.llm_model,
        )
        classify_fn = gen._analyze  # reuse existing LLM call path
        classifier = PathologyClassifier(classify_fn, dialect)
        console.print(f"LLM classifier: {model or settings.llm_model}")
    except Exception as e:
        console.print(
            f"[yellow]LLM classifier unavailable ({e}). "
            f"AST-only mode.[/yellow]"
        )

    # Process queries
    queries_dir = bench_dir / "queries"
    results = dict(existing)  # start from existing
    t0 = time.time()

    for i, qid in enumerate(query_ids, 1):
        sql_path = queries_dir / f"{qid}.sql"
        if not sql_path.exists():
            console.print(f"  [yellow]Skip {qid}: no SQL file[/yellow]")
            continue

        sql = sql_path.read_text()

        # AST detection (instant)
        detected = detect_transforms(
            sql, transforms_catalog, engine=engine, dialect=dialect
        )
        ast_matches = [
            {
                "id": m.id,
                "overlap": round(m.overlap_ratio, 3),
                "matched": m.matched_features,
                "missing": m.missing_features,
                "gap": m.gap,
            }
            for m in detected
            if m.overlap_ratio >= 0.25
        ]

        # LLM classification (if available)
        llm_matches = []
        reasoning = ""
        if classifier:
            # Load cached EXPLAIN if available
            explain_text = ""
            for explain_path in [
                bench_dir / "explains" / f"{qid}.json",
                bench_dir / "explains" / "sf10" / f"{qid}.json",
            ]:
                if explain_path.exists():
                    explain_data = json.loads(explain_path.read_text())
                    explain_text = explain_data.get("plan_text", "")
                    break

            cr = classifier.classify(qid, sql, explain_text)
            llm_matches = [
                {
                    "pathology_id": m.pathology_id,
                    "name": m.name,
                    "confidence": round(m.confidence, 3),
                    "evidence": m.evidence,
                    "transform": m.recommended_transform,
                }
                for m in cr.matches
            ]
            reasoning = cr.reasoning

        results[qid] = {
            "ast_matches": ast_matches,
            "llm_matches": llm_matches,
            "reasoning": reasoning,
            "classified_at": datetime.now(timezone.utc).isoformat(),
        }

        # Progress
        n_ast = len(ast_matches)
        n_llm = len(llm_matches)
        top_ast = ast_matches[0]["id"] if ast_matches else "-"
        top_llm = (
            f"{llm_matches[0]['pathology_id']}({llm_matches[0]['confidence']:.0%})"
            if llm_matches
            else "-"
        )
        console.print(
            f"  [{i}/{len(query_ids)}] {qid}: "
            f"AST={n_ast} (top: {top_ast}), "
            f"LLM={n_llm} (top: {top_llm})"
        )

    elapsed = time.time() - t0

    # Save
    out_path.write_text(json.dumps(results, indent=2))

    # Summary
    total = len(results)
    with_ast = sum(1 for r in results.values() if r.get("ast_matches"))
    with_llm = sum(1 for r in results.values() if r.get("llm_matches"))

    print_success(
        f"Classified {len(query_ids)} queries in {elapsed:.1f}s → {out_path.name}"
    )
    console.print(
        f"  Total: {total} | AST hits: {with_ast} | LLM hits: {with_llm}"
    )

"""qt promote — promote beam candidates to gold examples with human curation.

Usage:
    qt promote postgres_dsb_76 --from candidates.json --ids 1,2,3
    qt promote postgres_dsb_76 --from candidates.json --ids 1,2,3 --interactive
"""

from __future__ import annotations

import json
from pathlib import Path

import click

from ._common import console, engine_from_config, resolve_benchmark


def _show_candidate_detail(candidate: dict, index: int) -> None:
    """Display detailed candidate information for review."""
    console.print(f"\n{'=' * 60}")
    console.print(
        f"[bold cyan]Promoting [{index}]: {candidate['query_id']} "
        f"({candidate['transform']}, {candidate['speedup']:.2f}x)[/bold cyan]"
    )
    console.print(f"{'=' * 60}")

    console.print(f"  Patch ID:  {candidate['patch_id']}")
    console.print(f"  Family:    {candidate['family']}")
    console.print(f"  Status:    {candidate['status']}")
    console.print(f"  Original:  {candidate['original_ms']:.1f}ms")
    console.print(f"  Optimized: {candidate['patch_ms']:.1f}ms")

    if candidate.get("hypothesis"):
        console.print(f"\n  [dim]Hypothesis: {candidate['hypothesis'][:200]}[/dim]")

    # Show SQL diff (abbreviated)
    orig = candidate.get("original_sql", "")
    opt = candidate.get("optimized_sql", "")

    if orig:
        console.print("\n  [bold]Original SQL[/bold] (first 300 chars):")
        console.print(f"  [dim]{orig[:300]}{'...' if len(orig) > 300 else ''}[/dim]")

    if opt:
        console.print("\n  [bold]Optimized SQL[/bold] (first 300 chars):")
        console.print(f"  [green]{opt[:300]}{'...' if len(opt) > 300 else ''}[/green]")

    # Show EXPLAIN diff
    exp_before = candidate.get("explain_before", "")
    exp_after = candidate.get("explain_after", "")

    if exp_before:
        console.print("\n  [bold]EXPLAIN Before[/bold] (first 200 chars):")
        console.print(f"  [dim]{exp_before[:200]}[/dim]")
    if exp_after:
        console.print("\n  [bold]EXPLAIN After[/bold] (first 200 chars):")
        console.print(f"  [green]{exp_after[:200]}[/green]")


def _prompt_insights() -> tuple[str, str, str]:
    """Prompt user for key_insight, when_not_to_use, and input_slice."""
    console.print("\n[bold yellow]Enter curation data:[/bold yellow]")

    key_insight = click.prompt(
        "\n  key_insight (WHY it's faster — engine-level explanation)",
        type=str,
    )

    when_not_to_use = click.prompt(
        "\n  when_not_to_use (failure modes — when does this transform regress?)",
        type=str,
    )

    input_slice = click.prompt(
        "\n  input_slice (describe the SQL pattern in 1-2 sentences)",
        type=str,
    )

    return key_insight, when_not_to_use, input_slice


@click.command()
@click.argument("benchmark")
@click.option(
    "--from",
    "from_path",
    required=True,
    type=click.Path(exists=True),
    help="Path to candidates.json (from qt analyze --export).",
)
@click.option(
    "--ids",
    required=True,
    help="Comma-separated 1-indexed IDs to promote (e.g., 1,2,5).",
)
@click.option(
    "--interactive/--no-interactive",
    default=True,
    show_default=True,
    help="Prompt for key_insight/when_not_to_use interactively.",
)
@click.option(
    "--key-insight",
    default=None,
    help="Provide key_insight non-interactively (applies to all).",
)
@click.option(
    "--when-not-to-use",
    default=None,
    help="Provide when_not_to_use non-interactively (applies to all).",
)
@click.option(
    "--input-slice",
    default=None,
    help="Provide input_slice non-interactively (applies to all).",
)
@click.pass_context
def promote(
    ctx: click.Context,
    benchmark: str,
    from_path: str,
    ids: str,
    interactive: bool,
    key_insight: str | None,
    when_not_to_use: str | None,
    input_slice: str | None,
) -> None:
    """Promote beam candidates to gold examples with human curation."""
    from qt_sql.beam_analyzer import candidates_from_json
    from qt_sql.gold_curator import (
        create_gold_example,
        examples_root,
        write_gold_example,
    )

    bench_dir = resolve_benchmark(benchmark)
    engine = engine_from_config(bench_dir)

    # Load candidates
    data = json.loads(Path(from_path).read_text(encoding="utf-8"))
    all_candidates_raw = data.get("wins", []) + data.get("regressions", [])

    if not all_candidates_raw:
        console.print("[bold red]No candidates found in file.[/bold red]")
        raise SystemExit(1)

    # Parse IDs
    try:
        selected_ids = [int(x.strip()) for x in ids.split(",")]
    except ValueError:
        console.print("[bold red]Invalid --ids format. Use comma-separated numbers.[/bold red]")
        raise SystemExit(1)

    console.print(f"\n[bold cyan]Promoting {len(selected_ids)} candidates from {benchmark}[/bold cyan]")
    console.print(f"  Engine: {engine}")
    console.print(f"  Source: {from_path}")
    console.print(f"  IDs: {selected_ids}")

    promoted = 0
    skipped = 0

    for idx in selected_ids:
        if idx < 1 or idx > len(all_candidates_raw):
            console.print(f"[yellow]  Skipping ID {idx} — out of range (1-{len(all_candidates_raw)})[/yellow]")
            skipped += 1
            continue

        candidate_raw = all_candidates_raw[idx - 1]
        candidates = candidates_from_json([candidate_raw])
        candidate = candidates[0]

        _show_candidate_detail(candidate_raw, idx)

        # Get human insights
        if interactive and not (key_insight and when_not_to_use and input_slice):
            ki, wntu, isl = _prompt_insights()
        else:
            ki = key_insight or "TODO: Add engine-level explanation"
            wntu = when_not_to_use or "TODO: Add failure modes"
            isl = input_slice or "TODO: Describe the SQL pattern"

        # Confirm
        if interactive:
            if not click.confirm("\n  Promote this candidate?", default=True):
                console.print("  [dim]Skipped.[/dim]")
                skipped += 1
                continue

        # Create and write gold example
        gold = create_gold_example(candidate, ki, wntu, isl)
        out_path = write_gold_example(gold, examples_root(), engine)

        if out_path:
            console.print(f"\n  [green]Wrote: {out_path.relative_to(examples_root().parent)}[/green]")
            promoted += 1
        else:
            console.print("  [yellow]Skipped (existing has higher speedup).[/yellow]")
            skipped += 1

    console.print(f"\n{'=' * 60}")
    console.print(f"[bold]Promoted: {promoted}  Skipped: {skipped}[/bold]")
    console.print(f"{'=' * 60}")

    if promoted > 0:
        console.print("\n[bold]Next steps:[/bold]")
        console.print("  qt index               # Rebuild tag index")
        console.print(f"  qt playbook {benchmark}  # Regenerate playbook")
